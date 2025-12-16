import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
import importlib.util
import sys
import contextlib
import io


def _load_merge_module():
    script_path = Path(__file__).resolve().parents[1] / "ha-merge-history.py"
    spec = importlib.util.spec_from_file_location("ha_merge_history_script", script_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class TestMergeHistoryScript(unittest.TestCase):
    def _make_db(self) -> Path:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        tmp.close()
        return Path(tmp.name)

    def _make_storage(self, *, old_id: str, new_id: str, state_class: str) -> Path:
        d = Path(tempfile.mkdtemp())
        storage = d / ".storage"
        storage.mkdir(parents=True, exist_ok=True)
        doc = {
            "data": {
                "entities": [
                    {"entity_id": old_id, "capabilities": {"state_class": state_class}},
                    {"entity_id": new_id, "capabilities": {"state_class": state_class}},
                ]
            }
        }
        (storage / "core.entity_registry").write_text(json.dumps(doc), encoding="utf-8")
        return storage

    def test_legacy_schema_is_rejected(self) -> None:
        merge_module = _load_merge_module()
        run_merge = merge_module.run_merge

        db = self._make_db()
        storage = self._make_storage(old_id="sensor.old", new_id="sensor.new", state_class="total_increasing")

        conn = sqlite3.connect(str(db))
        try:
            conn.execute(
                "CREATE TABLE statistics (statistic_id TEXT, start_ts REAL, state REAL, sum REAL)"
            )
            conn.commit()
        finally:
            conn.close()

        with self.assertRaises(merge_module.MergeHistoryError):
            with contextlib.redirect_stdout(io.StringIO()):
                run_merge(old_entity_id="sensor.old", new_entity_id="sensor.new", db_path=db, storage_dir=storage)

    def test_metadata_schema_total_increasing_updates_and_copies(self) -> None:
        run_merge = _load_merge_module().run_merge

        db = self._make_db()
        storage = self._make_storage(old_id="sensor.old", new_id="sensor.new", state_class="total_increasing")

        conn = sqlite3.connect(str(db))
        try:
            conn.execute("CREATE TABLE statistics_meta (id INTEGER, statistic_id TEXT)")
            conn.execute(
                "CREATE TABLE statistics (metadata_id INTEGER, start_ts REAL, state REAL, sum REAL)"
            )
            conn.execute(
                "CREATE TABLE statistics_short_term (metadata_id INTEGER, start_ts REAL, state REAL, sum REAL)"
            )
            conn.executemany(
                "INSERT INTO statistics_meta(id, statistic_id) VALUES(?,?)",
                [(1, "sensor.old"), (2, "sensor.new")],
            )
            conn.executemany(
                "INSERT INTO statistics(metadata_id, start_ts, state, sum) VALUES(?,?,?,?)",
                [
                    (1, 0.0, 1.0, 10.0),
                    (1, 3600.0, 2.0, 12.0),
                    (2, 7200.0, 3.0, 1.0),
                    (2, 10800.0, 4.0, 2.0),
                ],
            )
            conn.commit()
        finally:
            conn.close()

        import builtins

        orig_input = builtins.input
        builtins.input = lambda *_args, **_kwargs: "y"
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                run_merge(old_entity_id="sensor.old", new_entity_id="sensor.new", db_path=db, storage_dir=storage)
        finally:
            builtins.input = orig_input

        ro = sqlite3.connect(str(db))
        try:
            rows = ro.execute(
                "SELECT start_ts, sum FROM statistics WHERE metadata_id=2 ORDER BY start_ts"
            ).fetchall()
            self.assertEqual([r[0] for r in rows], [0.0, 3600.0, 7200.0, 10800.0])
            self.assertEqual([float(r[1]) for r in rows], [10.0, 12.0, 12.0, 13.0])
        finally:
            ro.close()

    def test_trailing_constant_old_rows_are_skipped_and_precondition_uses_first_of_run(self) -> None:
        run_merge = _load_merge_module().run_merge

        db = self._make_db()
        storage = self._make_storage(old_id="sensor.old", new_id="sensor.new", state_class="total_increasing")

        conn = sqlite3.connect(str(db))
        try:
            conn.execute("CREATE TABLE statistics_meta (id INTEGER, statistic_id TEXT)")
            conn.execute(
                "CREATE TABLE statistics (metadata_id INTEGER, start_ts REAL, state REAL, sum REAL)"
            )
            conn.execute(
                "CREATE TABLE statistics_short_term (metadata_id INTEGER, start_ts REAL, state REAL, sum REAL)"
            )
            conn.executemany(
                "INSERT INTO statistics_meta(id, statistic_id) VALUES(?,?)",
                [(1, "sensor.old"), (2, "sensor.new")],
            )
            # Old has a trailing run of identical sum/state at 3600/7200/10800.
            # New starts at 7200, which would fail the precondition without collapsing the redundant tail.
            conn.executemany(
                "INSERT INTO statistics(metadata_id, start_ts, state, sum) VALUES(?,?,?,?)",
                [
                    (1, 0.0, 1.0, 10.0),
                    (1, 3600.0, 2.0, 12.0),
                    (1, 7200.0, 2.0, 12.0),
                    (1, 10800.0, 2.0, 12.0),
                    (2, 7200.0, 3.0, 1.0),
                    (2, 10800.0, 4.0, 2.0),
                ],
            )
            conn.commit()
        finally:
            conn.close()

        import builtins

        orig_input = builtins.input
        builtins.input = lambda *_args, **_kwargs: "y"
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                run_merge(old_entity_id="sensor.old", new_entity_id="sensor.new", db_path=db, storage_dir=storage)
        finally:
            builtins.input = orig_input

        ro = sqlite3.connect(str(db))
        try:
            rows = ro.execute(
                "SELECT start_ts, sum FROM statistics WHERE metadata_id=2 ORDER BY start_ts"
            ).fetchall()
            # Should only have the two original new rows plus the non-redundant part of old (0 and 3600).
            self.assertEqual([r[0] for r in rows], [0.0, 3600.0, 7200.0, 10800.0])
            self.assertEqual([float(r[1]) for r in rows], [10.0, 12.0, 12.0, 13.0])
        finally:
            ro.close()

    def test_overlap_with_changed_values_is_rejected(self) -> None:
        merge_module = _load_merge_module()
        run_merge = merge_module.run_merge

        db = self._make_db()
        storage = self._make_storage(old_id="sensor.old", new_id="sensor.new", state_class="total_increasing")

        conn = sqlite3.connect(str(db))
        try:
            conn.execute("CREATE TABLE statistics_meta (id INTEGER, statistic_id TEXT)")
            conn.execute(
                "CREATE TABLE statistics (metadata_id INTEGER, start_ts REAL, state REAL, sum REAL)"
            )
            conn.executemany(
                "INSERT INTO statistics_meta(id, statistic_id) VALUES(?,?)",
                [(1, "sensor.old"), (2, "sensor.new")],
            )
            # New starts at 7200. Old overlaps but the overlapping part is NOT constant.
            conn.executemany(
                "INSERT INTO statistics(metadata_id, start_ts, state, sum) VALUES(?,?,?,?)",
                [
                    (1, 0.0, 1.0, 10.0),
                    (1, 3600.0, 2.0, 12.0),
                    (1, 7200.0, 2.0, 12.0),
                    (1, 10800.0, 2.0, 13.0),
                    (2, 7200.0, 3.0, 1.0),
                ],
            )
            conn.commit()
        finally:
            conn.close()

        import builtins

        orig_input = builtins.input
        builtins.input = lambda *_args, **_kwargs: "y"
        try:
            with self.assertRaises(merge_module.MergeHistoryError):
                with contextlib.redirect_stdout(io.StringIO()):
                    run_merge(old_entity_id="sensor.old", new_entity_id="sensor.new", db_path=db, storage_dir=storage)
        finally:
            builtins.input = orig_input

    def test_precondition_is_per_table_not_global(self) -> None:
        run_merge = _load_merge_module().run_merge

        db = self._make_db()
        storage = self._make_storage(old_id="sensor.old", new_id="sensor.new", state_class="total_increasing")

        conn = sqlite3.connect(str(db))
        try:
            conn.execute("CREATE TABLE statistics_meta (id INTEGER, statistic_id TEXT)")
            conn.execute("CREATE TABLE statistics (metadata_id INTEGER, start_ts REAL, state REAL, sum REAL)")
            conn.execute(
                "CREATE TABLE statistics_short_term (metadata_id INTEGER, start_ts REAL, state REAL, sum REAL)"
            )
            conn.executemany(
                "INSERT INTO statistics_meta(id, statistic_id) VALUES(?,?)",
                [(1, "sensor.old"), (2, "sensor.new")],
            )
            # statistics: old ends at 3600, new starts at 7200 (no overlap)
            conn.executemany(
                "INSERT INTO statistics(metadata_id, start_ts, state, sum) VALUES(?,?,?,?)",
                [
                    (1, 0.0, 1.0, 10.0),
                    (1, 3600.0, 2.0, 12.0),
                    (2, 7200.0, 3.0, 1.0),
                ],
            )
            # statistics_short_term: old ends later (10800) but new also starts later (14400)
            conn.executemany(
                "INSERT INTO statistics_short_term(metadata_id, start_ts, state, sum) VALUES(?,?,?,?)",
                [
                    (1, 7200.0, 2.5, 12.5),
                    (1, 10800.0, 2.6, 12.6),
                    (2, 14400.0, 3.0, 2.0),
                ],
            )
            conn.commit()
        finally:
            conn.close()

        import builtins

        orig_input = builtins.input
        builtins.input = lambda *_args, **_kwargs: "y"
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                run_merge(old_entity_id="sensor.old", new_entity_id="sensor.new", db_path=db, storage_dir=storage)
        finally:
            builtins.input = orig_input

        ro = sqlite3.connect(str(db))
        try:
            stats = ro.execute(
                "SELECT start_ts FROM statistics WHERE metadata_id=2 ORDER BY start_ts"
            ).fetchall()
            self.assertEqual([r[0] for r in stats], [0.0, 3600.0, 7200.0])
            st = ro.execute(
                "SELECT start_ts FROM statistics_short_term WHERE metadata_id=2 ORDER BY start_ts"
            ).fetchall()
            self.assertEqual([r[0] for r in st], [7200.0, 10800.0, 14400.0])
        finally:
            ro.close()


if __name__ == "__main__":
    unittest.main()
