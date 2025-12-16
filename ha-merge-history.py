from __future__ import annotations

import json
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


DEFAULT_DB_PATH = Path("./home-assistant_v2.db")
DEFAULT_STORAGE_DIR = Path("./.storage/")
REGISTRY_FILENAME = "core.entity_registry"


class MergeHistoryError(RuntimeError):
    pass


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


def _print_overlapping_old_rows(
    conn: sqlite3.Connection,
    table: str,
    selector: StatsSelector,
    *,
    overlap_start_ts: float,
    overlap_end_ts: float,
) -> None:
    cols = _columns(conn, table)
    start_epoch_expr = _start_ts_expr(cols)
    conn.row_factory = sqlite3.Row
    print(f"Overlap detected in {table}; printing overlapping OLD rows (all columns):")
    print(f"- range: {_fmt_ts(overlap_start_ts)} .. {_fmt_ts(overlap_end_ts)}")

    rows = conn.execute(
        f"SELECT * FROM {table} WHERE {selector.where_old} AND {start_epoch_expr} >= ? AND {start_epoch_expr} <= ? "
        f"ORDER BY {start_epoch_expr} ASC",
        (*selector.params_old, overlap_start_ts, overlap_end_ts),
    ).fetchall()

    print(f"- old overlapping rows: {len(rows)}")
    for r in rows:
        d = dict(r)
        # Emit a stable, readable representation.
        print(json.dumps(d, ensure_ascii=False, sort_keys=True, default=str))


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


def _max_defined(values: Iterable[float | None]) -> float | None:
    vs = [v for v in values if v is not None]
    return max(vs) if vs else None


def _min_defined(values: Iterable[float | None]) -> float | None:
    vs = [v for v in values if v is not None]
    return min(vs) if vs else None


def _copy_rows(
    conn: sqlite3.Connection,
    table: str,
    selector: StatsSelector,
    *,
    extra_where_sql: str = "",
    extra_params: tuple[Any, ...] = (),
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

    cur = conn.execute(
        f"INSERT INTO {table} ({dest_cols}) "
        f"SELECT {select_sql} FROM {table} WHERE {where_sql}",
        (selector.new_id_value, *selector.params_old, *extra_params),
    )
    return int(cur.rowcount or 0)


def _update_sums(conn: sqlite3.Connection, table: str, selector: StatsSelector, offset: float) -> int:
    cols = _columns(conn, table)
    if "sum" not in cols:
        return 0
    cur = conn.execute(
        f"UPDATE {table} SET sum = sum + ? WHERE {selector.where_new}",
        (offset, *selector.params_new),
    )
    return int(cur.rowcount or 0)


def run_merge(*, old_entity_id: str, new_entity_id: str, db_path: Path, storage_dir: Path) -> None:
    entities = _load_entity_registry(storage_dir)
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

    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row

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
        overlaps: list[tuple[str, StatsSelector, float, float]] = []  # (table, selector, new_start, overlap_end)

        for table in ("statistics", "statistics_short_term"):
            if not _table_exists(conn, table):
                continue
            sel = _resolve_stats_selector(conn, table, old_entity_id, new_entity_id)
            old_count, old_start, old_end = _stats_span(conn, table, sel.where_old, sel.params_old)
            new_count, new_start, new_end = _stats_span(conn, table, sel.where_new, sel.params_new)
            table_infos.append(_TableInfo(table, sel, old_count, old_start, old_end, new_count, new_start, new_end))

            if old_end is not None and new_start is not None and old_end >= new_start:
                overlap_end = old_end if new_end is None else min(old_end, new_end)
                overlaps.append((table, sel, float(new_start), float(overlap_end)))

        if not table_infos:
            raise MergeHistoryError("No statistics tables found")

        copy_filters: dict[str, tuple[str, tuple[Any, ...]]] = {}

        if overlaps:
            for table, sel, overlap_start, overlap_end in overlaps:
                _print_overlapping_old_rows(
                    conn,
                    table,
                    sel,
                    overlap_start_ts=overlap_start,
                    overlap_end_ts=overlap_end,
                )

            ans = input("Overlap found. Skip overlapping OLD rows and continue? [y/N] ")
            if not _parse_bool_prompt(ans):
                first_table, _, overlap_start, _ = overlaps[0]
                raise MergeHistoryError(
                    "Precondition failed: old latest stats must precede new earliest stats "
                    f"for {first_table} (overlap at/after {_fmt_ts(overlap_start)})"
                )

            # Skip all old rows at/after the first new timestamp for that table.
            for table, _sel, overlap_start, _overlap_end in overlaps:
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
            # Offset for monotonic sum: shift new sums so they continue from old sum,
            # assuming the new entity's state started from 0.
            # offset = last_old_sum - first_new_sum + first_new_state
            if not _table_exists(conn, "statistics"):
                raise MergeHistoryError("statistics table missing (required for offset calculation)")
            sel_stats = _resolve_stats_selector(conn, "statistics", old_entity_id, new_entity_id)
            last_old_sum = _latest_sum(conn, "statistics", sel_stats)
            first_new_sum, first_new_state = _earliest_new_sum_state(conn, "statistics", sel_stats)
            offset = last_old_sum - first_new_sum + first_new_state

        print("ha-merge-history plan:")
        print(f"- db: {db_path}")
        print(f"- state_class: {old_sc}")

        for p in plans:
            print(f"- {p.table}:")
            print(
                f"  [{_fmt_ts(p.old_start)} - {_fmt_ts(p.old_end)}] copy {p.old_count} rows"
            )
            if old_sc in {"total", "total_increasing"}:
                print(
                    f"  [{_fmt_ts(p.new_start)} - {_fmt_ts(p.new_end)}] update {p.new_count} rows "
                    f"(sum += {offset})"
                )

        answer = input("Proceed to update DB? [y/N] ")
        if not _parse_bool_prompt(answer):
            print("Aborted.")
            return

        with conn:
            # Update sums first (only affects existing new rows)
            if old_sc in {"total", "total_increasing"} and offset != 0.0:
                for table in ("statistics", "statistics_short_term"):
                    if not _table_exists(conn, table):
                        continue
                    sel = _resolve_stats_selector(conn, table, old_entity_id, new_entity_id)
                    _update_sums(conn, table, sel, offset)

            # Copy rows
            copied_total = 0
            for table in ("statistics", "statistics_short_term"):
                if not _table_exists(conn, table):
                    continue
                sel = _resolve_stats_selector(conn, table, old_entity_id, new_entity_id)
                extra_where_sql, extra_params = copy_filters.get(table, ("", ()))
                copied_total += _copy_rows(conn, table, sel, extra_where_sql=extra_where_sql, extra_params=extra_params)

        print(f"Done. Copied {copied_total} rows.")

    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if len(argv) != 2:
        print("usage: ha-merge-history.py OLD_ENTITY_ID NEW_ENTITY_ID", file=sys.stderr)
        print(f"defaults: db={DEFAULT_DB_PATH} storage={DEFAULT_STORAGE_DIR}", file=sys.stderr)
        return 2

    old_entity_id, new_entity_id = argv
    try:
        run_merge(
            old_entity_id=old_entity_id,
            new_entity_id=new_entity_id,
            db_path=DEFAULT_DB_PATH,
            storage_dir=DEFAULT_STORAGE_DIR,
        )
    except MergeHistoryError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
