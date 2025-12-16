from __future__ import annotations

import json
import sqlite3
import sys
import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_DB_PATH = Path("./home-assistant_v2.db")
DEFAULT_STORAGE_DIR = Path("./.storage/")
REGISTRY_FILENAME = "core.entity_registry"


class MergeHistoryError(RuntimeError):
    pass


def _sql_literal(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, (bytes, bytearray, memoryview)):
        return "X'" + bytes(v).hex() + "'"
    s = str(v)
    return "'" + s.replace("'", "''") + "'"


def _inline_sql_params(sql: str, params: tuple[Any, ...]) -> str:
    if not params:
        return sql
    parts = sql.split("?")
    if len(parts) - 1 != len(params):
        raise MergeHistoryError(
            f"SQL placeholder count mismatch: expected {len(parts) - 1}, got {len(params)}"
        )
    out = parts[0]
    for p, tail in zip(params, parts[1:], strict=True):
        out += _sql_literal(p) + tail
    return out


def _print_sql(sql: str, params: tuple[Any, ...]) -> None:
    # Keep it pasteable into sqlite3 console.
    formatted = _inline_sql_params(sql, params).strip()
    for kw in ("INSERT", "SELECT", "FROM", "WHERE"):
        formatted = formatted.replace(f" {kw} ", f"\n{kw} ")
    print("")
    if not formatted.endswith(";"):
        formatted += ";"
    print(formatted)


def _parse_bool_prompt(text: str) -> bool:
    v = text.strip().lower()
    return v in {"y", "yes"}


def _sql_quote_identifier(name: str) -> str:
    if not name or "\x00" in name:
        raise MergeHistoryError(f"Invalid identifier: {name!r}")
    return '"' + name.replace('"', '""') + '"'


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({_sql_quote_identifier(table)})").fetchall()
    return {r[1] for r in rows}


def _start_ts_expr(cols: set[str]) -> str:
    if "start_ts" not in cols:
        raise MergeHistoryError("Statistics table missing start_ts")
    return "CAST(start_ts AS REAL)"


@dataclass(frozen=True)
class StatsSelector:
    id_col: str  # metadata_id
    old_id_value: Any
    new_id_value: Any
    where_old: str
    where_new: str
    params_old: tuple[Any, ...]
    params_new: tuple[Any, ...]


def _resolve_stats_selector(conn: sqlite3.Connection, table: str, old_id: str, new_id: str) -> StatsSelector:
    cols = _columns(conn, table)
    if "metadata_id" not in cols:
        raise MergeHistoryError(
            f"Unsupported schema for {table}: expected metadata_id/start_ts (modern HA recorder schema)"
        )
    if not _table_exists(conn, "statistics_meta"):
        raise MergeHistoryError("metadata_id schema but statistics_meta is missing")
    meta_cols = _columns(conn, "statistics_meta")
    if not {"id", "statistic_id"}.issubset(meta_cols):
        raise MergeHistoryError("statistics_meta missing required columns")

    old_meta = conn.execute(
        "SELECT id FROM statistics_meta WHERE statistic_id = ?",
        (old_id,),
    ).fetchone()
    new_meta = conn.execute(
        "SELECT id FROM statistics_meta WHERE statistic_id = ?",
        (new_id,),
    ).fetchone()
    if old_meta is None:
        raise MergeHistoryError(f"Old statistic_id not found in statistics_meta: {old_id}")
    if new_meta is None:
        raise MergeHistoryError(f"New statistic_id not found in statistics_meta: {new_id}")

    old_meta_id = old_meta[0]
    new_meta_id = new_meta[0]
    return StatsSelector(
        id_col="metadata_id",
        old_id_value=old_meta_id,
        new_id_value=new_meta_id,
        where_old="metadata_id = ?",
        where_new="metadata_id = ?",
        params_old=(old_meta_id,),
        params_new=(new_meta_id,),
    )


@dataclass(frozen=True)
class TablePlan:
    table: str
    old_count: int
    old_start: float | None
    old_end: float | None
    new_count: int
    new_start: float | None
    new_end: float | None


@dataclass(frozen=True)
class PairPlan:
    old_entity_id: str
    new_entity_id: str
    state_class: str
    offset: float
    table_plans: list[TablePlan]
    copy_filters: dict[str, tuple[str, tuple[Any, ...]]]
    overlaps: dict[str, tuple[float, float]]  # table -> (overlap_start_ts, overlap_end_ts)


def _stats_span(conn: sqlite3.Connection, table: str, where_sql: str, params: tuple[Any, ...]) -> tuple[int, float | None, float | None]:
    cols = _columns(conn, table)
    start_epoch_expr = _start_ts_expr(cols)

    row = conn.execute(
        f"SELECT COUNT(*) AS c, MIN({start_epoch_expr}) AS mn, MAX({start_epoch_expr}) AS mx "
        f"FROM {table} WHERE {where_sql}",
        params,
    ).fetchone()
    if row is None:
        return 0, None, None
    return int(row[0] or 0), (None if row[1] is None else float(row[1])), (None if row[2] is None else float(row[2]))


def _summarize_omitted_rows(
    conn: sqlite3.Connection,
    table: str,
    selector: StatsSelector,
    *,
    overlap_start_ts: float,
    overlap_end_ts: float,
) -> tuple[int, list[dict[str, Any]]]:
    cols = _columns(conn, table)
    start_epoch_expr = _start_ts_expr(cols)

    wanted = ["state", "min", "mean", "max", "sum", "last_reset_ts"]
    select_exprs: list[str] = []
    for c in wanted:
        if c in cols:
            select_exprs.append(c)
        else:
            select_exprs.append(f"NULL AS {c}")
    select_sql = ", ".join(select_exprs)

    rows = conn.execute(
        f"SELECT {select_sql} FROM {table} "
        f"WHERE {selector.where_old} AND {start_epoch_expr} >= ? AND {start_epoch_expr} <= ? "
        f"ORDER BY {start_epoch_expr} ASC",
        (*selector.params_old, overlap_start_ts, overlap_end_ts),
    ).fetchall()

    if not rows:
        return 0, []

    unique: list[dict[str, Any]] = []
    seen: set[str] = set()
    for r in rows:
        rep: dict[str, Any] = {
            "state": r[0],
            "min": r[1],
            "mean": r[2],
            "max": r[3],
            "sum": r[4],
            "last_reset_ts": (None if r[5] is None else _fmt_ts(float(r[5]))),
        }
        key = json.dumps(rep, ensure_ascii=False, sort_keys=True, default=str)
        if key in seen:
            continue
        seen.add(key)
        unique.append(rep)

    return len(rows), unique


def _latest_sum(conn: sqlite3.Connection, table: str, selector: StatsSelector) -> float:
    cols = _columns(conn, table)
    if "sum" not in cols:
        raise MergeHistoryError(f"{table} has no sum column")
    start_epoch_expr = _start_ts_expr(cols)
    row = conn.execute(
        f"SELECT sum FROM {table} WHERE {selector.where_old} "
        f"ORDER BY {start_epoch_expr} DESC LIMIT 1",
        selector.params_old,
    ).fetchone()
    if row is None or row[0] is None:
        raise MergeHistoryError(f"No sum rows found for old entity in {table}")
    return float(row[0])


def _earliest_new_sum_state(conn: sqlite3.Connection, table: str, selector: StatsSelector) -> tuple[float, float]:
    cols = _columns(conn, table)
    if "sum" not in cols:
        raise MergeHistoryError(f"{table} has no sum column")
    if "state" not in cols:
        raise MergeHistoryError(f"{table} has no state column")
    start_epoch_expr = _start_ts_expr(cols)
    row = conn.execute(
        f"SELECT sum, state FROM {table} WHERE {selector.where_new} "
        f"ORDER BY {start_epoch_expr} ASC LIMIT 1",
        selector.params_new,
    ).fetchone()
    if row is None or row[0] is None or row[1] is None:
        raise MergeHistoryError(f"No (sum,state) rows found for new entity in {table}")
    return float(row[0]), float(row[1])


def _fmt_ts(ts: float | None) -> str:
    if ts is None:
        return ""
    # Local-time display like the main tool
    import datetime as _dt

    return _dt.datetime.fromtimestamp(ts, tz=_dt.timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _load_entity_registry(storage_dir: Path) -> dict[str, dict[str, Any]]:
    registry_path = storage_dir / REGISTRY_FILENAME
    try:
        doc = json.loads(registry_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise MergeHistoryError(f"Entity registry file not found: {registry_path}") from exc
    except OSError as exc:
        raise MergeHistoryError(f"Failed to read entity registry file: {registry_path} ({exc})") from exc
    except json.JSONDecodeError as exc:
        raise MergeHistoryError(f"Invalid JSON in entity registry file: {registry_path} ({exc})") from exc

    data = doc.get("data")
    if not isinstance(data, dict):
        raise MergeHistoryError(f"Unexpected entity registry JSON shape: {registry_path}")
    entities = data.get("entities")
    if not isinstance(entities, list):
        raise MergeHistoryError(f"Unexpected entity registry JSON shape: {registry_path}")

    out: dict[str, dict[str, Any]] = {}
    for e in entities:
        if isinstance(e, dict):
            eid = e.get("entity_id")
            if isinstance(eid, str) and eid:
                out[eid] = e
    return out


def _get_state_class(entity: dict[str, Any] | None) -> str | None:
    if not entity:
        return None
    caps = entity.get("capabilities")
    if isinstance(caps, dict):
        sc = caps.get("state_class")
        if isinstance(sc, str) and sc:
            return sc
    for key in ("state_class", "original_state_class"):
        v = entity.get(key)
        if isinstance(v, str) and v:
            return v
    return None


def _copy_rows(
    conn: sqlite3.Connection,
    table: str,
    selector: StatsSelector,
    *,
    extra_where_sql: str = "",
    extra_params: tuple[Any, ...] = (),
    dry_run: bool = False,
) -> int:
    cols = _columns(conn, table)

    # Avoid inserting PK
    payload_cols = [
        c
        for c in (
            selector.id_col,
            "start_ts",
            "created_ts",
            "state",
            "sum",
            "last_reset",
            "last_reset_ts",
            "mean",
            "min",
            "max",
        )
        if c in cols
    ]

    if selector.id_col not in cols:
        raise MergeHistoryError(f"{table} missing id column: {selector.id_col}")
    if "start_ts" not in cols:
        raise MergeHistoryError(f"{table} missing start_ts")

    dest_cols = ", ".join(payload_cols)

    select_cols: list[str] = []
    for c in payload_cols:
        if c == selector.id_col:
            select_cols.append("?")
        else:
            select_cols.append(c)
    select_sql = ", ".join(select_cols)

    where_sql = selector.where_old + (" " + extra_where_sql if extra_where_sql else "")

    sql = (
        f"INSERT INTO {table} ({dest_cols}) "
        f"SELECT {select_sql} FROM {table} WHERE {where_sql}"
    )
    params = (selector.new_id_value, *selector.params_old, *extra_params)
    if dry_run:
        _print_sql(sql, params)
        return 0

    cur = conn.execute(sql, params)
    return int(cur.rowcount or 0)


def _update_sums(
    conn: sqlite3.Connection,
    table: str,
    selector: StatsSelector,
    offset: float,
    *,
    dry_run: bool = False,
) -> int:
    cols = _columns(conn, table)
    if "sum" not in cols:
        return 0
    sql = f"UPDATE {table} SET sum = sum + ? WHERE {selector.where_new}"
    params = (offset, *selector.params_new)
    if dry_run:
        _print_sql(sql, params)
        return 0
    cur = conn.execute(sql, params)
    return int(cur.rowcount or 0)


def _apply_pair_plans(conn: sqlite3.Connection, pair_plans: list[PairPlan], *, dry_run: bool) -> int:
    copied_total = 0
    for plan in pair_plans:
        old_entity_id = plan.old_entity_id
        new_entity_id = plan.new_entity_id

        # Update sums first (existing new rows)
        if plan.state_class in {"total", "total_increasing"} and plan.offset != 0.0:
            for table in ("statistics", "statistics_short_term"):
                if not _table_exists(conn, table):
                    continue
                sel = _resolve_stats_selector(conn, table, old_entity_id, new_entity_id)
                _update_sums(conn, table, sel, plan.offset, dry_run=dry_run)

        # Copy old rows into new, possibly skipping overlap
        for table in ("statistics", "statistics_short_term"):
            if not _table_exists(conn, table):
                continue
            sel = _resolve_stats_selector(conn, table, old_entity_id, new_entity_id)
            extra_where_sql, extra_params = plan.copy_filters.get(table, ("", ()))
            copied_total += _copy_rows(
                conn,
                table,
                sel,
                extra_where_sql=extra_where_sql,
                extra_params=extra_params,
                dry_run=dry_run,
            )
    return copied_total


def _plan_pair(
    conn: sqlite3.Connection,
    *,
    entities: dict[str, dict[str, Any]],
    old_entity_id: str,
    new_entity_id: str,
) -> PairPlan:
    old_ent = entities.get(old_entity_id)
    new_ent = entities.get(new_entity_id)
    if old_ent is None:
        raise MergeHistoryError(f"Old entity not found in registry: {old_entity_id}")
    if new_ent is None:
        raise MergeHistoryError(f"New entity not found in registry: {new_entity_id}")

    old_sc = _get_state_class(old_ent)
    new_sc = _get_state_class(new_ent)
    if old_sc != new_sc:
        raise MergeHistoryError(f"State class mismatch: old={old_sc!r} new={new_sc!r}")
    if old_sc not in {"measurement", "total", "total_increasing"}:
        raise MergeHistoryError(f"Unsupported state class: {old_sc!r}")

    @dataclass(frozen=True)
    class _TableInfo:
        table: str
        selector: StatsSelector
        old_count: int
        old_start: float | None
        old_end: float | None
        new_count: int
        new_start: float | None
        new_end: float | None

    table_infos: list[_TableInfo] = []
    overlaps: dict[str, tuple[float, float]] = {}  # table -> (overlap_start, overlap_end)

    for table in ("statistics", "statistics_short_term"):
        if not _table_exists(conn, table):
            continue
        sel = _resolve_stats_selector(conn, table, old_entity_id, new_entity_id)
        old_count, old_start, old_end = _stats_span(conn, table, sel.where_old, sel.params_old)
        new_count, new_start, new_end = _stats_span(conn, table, sel.where_new, sel.params_new)
        table_infos.append(_TableInfo(table, sel, old_count, old_start, old_end, new_count, new_start, new_end))

        if old_end is not None and new_start is not None and old_end >= new_start:
            overlap_end = old_end if new_end is None else min(old_end, new_end)
            overlaps[table] = (float(new_start), float(overlap_end))

    if not table_infos:
        raise MergeHistoryError("No statistics tables found")

    copy_filters: dict[str, tuple[str, tuple[Any, ...]]] = {}
    if overlaps:
        for table, (overlap_start, _overlap_end) in overlaps.items():
            cols = _columns(conn, table)
            start_epoch_expr = _start_ts_expr(cols)
            copy_filters[table] = (f"AND {start_epoch_expr} < ?", (overlap_start,))

    plans: list[TablePlan] = []
    for info in table_infos:
        extra_where_sql, extra_params = copy_filters.get(info.table, ("", ()))
        eff_old_where = info.selector.where_old + (" " + extra_where_sql if extra_where_sql else "")
        eff_old_params = (*info.selector.params_old, *extra_params)
        eff_old_count, eff_old_start, eff_old_end = _stats_span(conn, info.table, eff_old_where, eff_old_params)
        plans.append(
            TablePlan(
                info.table,
                eff_old_count,
                eff_old_start,
                eff_old_end,
                info.new_count,
                info.new_start,
                info.new_end,
            )
        )

    offset = 0.0
    if old_sc in {"total", "total_increasing"}:
        if not _table_exists(conn, "statistics"):
            raise MergeHistoryError("statistics table missing (required for offset calculation)")
        sel_stats = _resolve_stats_selector(conn, "statistics", old_entity_id, new_entity_id)
        last_old_sum = _latest_sum(conn, "statistics", sel_stats)
        first_new_sum, first_new_state = _earliest_new_sum_state(conn, "statistics", sel_stats)
        offset = last_old_sum - first_new_sum + first_new_state

    return PairPlan(
        old_entity_id=old_entity_id,
        new_entity_id=new_entity_id,
        state_class=old_sc,
        offset=offset,
        table_plans=plans,
        copy_filters=copy_filters,
        overlaps=overlaps,
    )


def run_merge_many(
    *,
    pairs: list[tuple[str, str]],
    db_path: Path,
    storage_dir: Path,
    dry_run: bool = False,
) -> None:
    if not pairs:
        raise MergeHistoryError("No entity pairs provided")

    entities = _load_entity_registry(storage_dir)

    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row

        pair_plans: list[PairPlan] = []

        def _pline(line: str = "") -> None:
            if dry_run:
                if line:
                    print(f"-- {line}")
                else:
                    print("--")
            else:
                print(line)

        def _emit_pair_report(old_entity_id: str, new_entity_id: str, plan: PairPlan) -> None:
            header = f"# {old_entity_id} -> {new_entity_id} ({plan.state_class}) #"
            bar = "#" * len(header)
            _pline(bar)
            _pline(header)
            _pline(bar)

            for p in plan.table_plans:
                _pline(f"{p.table.upper()}:")
                _pline(f"  [{_fmt_ts(p.old_start)} - {_fmt_ts(p.old_end)}] copy {p.old_count} rows")
                if plan.state_class in {"total", "total_increasing"}:
                    _pline(
                        f"  [{_fmt_ts(p.new_start)} - {_fmt_ts(p.new_end)}] update {p.new_count} rows (sum += {plan.offset})"
                    )

                if p.table in plan.overlaps:
                    overlap_start, overlap_end = plan.overlaps[p.table]
                    sel = _resolve_stats_selector(conn, p.table, old_entity_id, new_entity_id)
                    omit_count, omit_rows = _summarize_omitted_rows(
                        conn, p.table, sel, overlap_start_ts=overlap_start, overlap_end_ts=overlap_end
                    )
                    if omit_count:
                        _pline(f"  [{_fmt_ts(overlap_start)} - {_fmt_ts(overlap_end)}] ommit {omit_count} rows:")
                        for r in omit_rows:
                            _pline("    " + json.dumps(r, ensure_ascii=False, sort_keys=True, default=str))

            _pline()

        _pline()
        _pline(f"DB: {db_path}")
        _pline()

        for old_entity_id, new_entity_id in pairs:
            plan = _plan_pair(conn, entities=entities, old_entity_id=old_entity_id, new_entity_id=new_entity_id)
            pair_plans.append(plan)

            _emit_pair_report(old_entity_id, new_entity_id, plan)

        if not dry_run:
            answer = input("Apply changes to DB? [y/N] ")
            if not _parse_bool_prompt(answer):
                print("Aborted.")
                return

        if dry_run:
            print("")
            print("BEGIN;")
            _apply_pair_plans(conn, pair_plans, dry_run=True)
            print("")
            print("COMMIT;")
            print("")
            return

        with conn:
            copied_total = _apply_pair_plans(conn, pair_plans, dry_run=False)

        print(f"Done. Copied {copied_total} rows.")

    finally:
        conn.close()


def run_merge(
    *,
    old_entity_id: str,
    new_entity_id: str,
    db_path: Path,
    storage_dir: Path,
    dry_run: bool = False,
) -> None:
    run_merge_many(pairs=[(old_entity_id, new_entity_id)], db_path=db_path, storage_dir=storage_dir, dry_run=dry_run)


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(prog="ha-merge-history.py")
    parser.add_argument("old", help="Old entity id (or base name when using --suffixes)")
    parser.add_argument("new", help="New entity id (or base name when using --suffixes)")
    parser.add_argument(
        "--suffixes",
        help="Comma-separated suffixes to append to both old/new (creates multiple pairs)",
        default=None,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not modify the DB; print the SQL statements that would be executed",
    )
    ns = parser.parse_args(argv)

    old_arg = str(ns.old)
    new_arg = str(ns.new)
    suffixes_raw = ns.suffixes
    if suffixes_raw:
        suffixes = [s.strip() for s in str(suffixes_raw).split(",") if s.strip()]
        if not suffixes:
            print("Error: --suffixes provided but empty", file=sys.stderr)
            return 2
        pairs = [(old_arg + s, new_arg + s) for s in suffixes]
    else:
        pairs = [(old_arg, new_arg)]

    try:
        run_merge_many(
            pairs=pairs,
            db_path=DEFAULT_DB_PATH,
            storage_dir=DEFAULT_STORAGE_DIR,
            dry_run=bool(ns.dry_run),
        )
    except MergeHistoryError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
