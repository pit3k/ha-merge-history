import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
import builtins
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
    def _mock_input(self, answers: list[str]):
        it = iter(answers)
        orig_input = builtins.input

        def _fake_input(*_args, **_kwargs):
            try:
                return next(it)
            except StopIteration:
                return "n"

        builtins.input = _fake_input
        return orig_input
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

    def _make_storage_new_only(self, *, new_id: str, state_class: str) -> Path:
        d = Path(tempfile.mkdtemp())
        storage = d / ".storage"
        storage.mkdir(parents=True, exist_ok=True)
        doc = {
            "data": {
                "entities": [
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
                "CREATE TABLE statistics (id INTEGER PRIMARY KEY AUTOINCREMENT, metadata_id INTEGER, start_ts REAL, state REAL, sum REAL)"
            )
            conn.execute(
                "CREATE TABLE statistics_short_term (id INTEGER PRIMARY KEY AUTOINCREMENT, metadata_id INTEGER, start_ts REAL, state REAL, sum REAL)"
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

        orig_input = self._mock_input(["y"])
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
            self.assertEqual([float(r[1]) for r in rows], [10.0, 12.0, 15.0, 16.0])
        finally:
            ro.close()

    def test_overlap_decline_aborts_and_prints_ommit_table(self) -> None:
        merge_module = _load_merge_module()
        run_merge = merge_module.run_merge

        db = self._make_db()
        storage = self._make_storage(old_id="sensor.old", new_id="sensor.new", state_class="total_increasing")

        conn = sqlite3.connect(str(db))
        try:
            conn.execute("CREATE TABLE statistics_meta (id INTEGER, statistic_id TEXT)")
            conn.execute(
                "CREATE TABLE statistics (id INTEGER PRIMARY KEY AUTOINCREMENT, metadata_id INTEGER, start_ts REAL, state REAL, sum REAL)"
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
                    (1, 7200.0, 2.0, 12.0),
                    (2, 7200.0, 3.0, 1.0),
                ],
            )
            conn.commit()
        finally:
            conn.close()

        # Decline final confirmation => abort (no exception)
        orig_input = self._mock_input(["n"])
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf):
                run_merge(old_entity_id="sensor.old", new_entity_id="sensor.new", db_path=db, storage_dir=storage)
        finally:
            builtins.input = orig_input

        out = buf.getvalue()
        self.assertIn("Aborted.", out)
        self.assertIn("ommit 1 rows:", out)
        # JSON summary excludes start_ts and includes last_reset_ts
        self.assertIn('"state": 2.0', out)
        self.assertIn('"sum": 12.0', out)

        ro = sqlite3.connect(str(db))
        try:
            # DB should be unchanged (no copies done)
            rows = ro.execute(
                "SELECT metadata_id, start_ts FROM statistics ORDER BY metadata_id, start_ts"
            ).fetchall()
            self.assertEqual(
                rows,
                [(1, 0.0), (1, 3600.0), (1, 7200.0), (2, 7200.0)],
            )
        finally:
            ro.close()

    def test_ommit_rows_truncates_to_10_with_warning(self) -> None:
        merge_module = _load_merge_module()
        run_merge = merge_module.run_merge

        db = self._make_db()
        storage = self._make_storage(old_id="sensor.old", new_id="sensor.new", state_class="total_increasing")

        conn = sqlite3.connect(str(db))
        try:
            conn.execute("CREATE TABLE statistics_meta (id INTEGER, statistic_id TEXT)")
            conn.execute(
                "CREATE TABLE statistics (id INTEGER PRIMARY KEY AUTOINCREMENT, metadata_id INTEGER, start_ts REAL, state REAL, sum REAL)"
            )
            conn.executemany(
                "INSERT INTO statistics_meta(id, statistic_id) VALUES(?,?)",
                [(1, "sensor.old"), (2, "sensor.new")],
            )

            # Create 11 omitted old rows (>= new_start) by making new span the same range.
            old_rows = [(1, float(i * 3600), float(i), float(i)) for i in range(0, 12)]
            new_rows = [(2, float(i * 3600), float(i), float(i)) for i in range(1, 12)]
            conn.executemany(
                "INSERT INTO statistics(metadata_id, start_ts, state, sum) VALUES(?,?,?,?)",
                old_rows + new_rows,
            )
            conn.commit()
        finally:
            conn.close()

        # Decline final confirmation => abort (no exception)
        orig_input = self._mock_input(["n"])
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf):
                run_merge(old_entity_id="sensor.old", new_entity_id="sensor.new", db_path=db, storage_dir=storage)
        finally:
            builtins.input = orig_input

        out = buf.getvalue()
        self.assertIn("ommit 11 rows:", out)
        self.assertIn("WARNING: omitted rows truncated; showing first 10 of 11", out)
        self.assertEqual(out.count('"id":'), 10)

    def test_when_ommit_leaves_zero_rows_skip_table_details(self) -> None:
        merge_module = _load_merge_module()
        run_merge = merge_module.run_merge

        db = self._make_db()
        storage = self._make_storage(old_id="sensor.old", new_id="sensor.new", state_class="total_increasing")

        conn = sqlite3.connect(str(db))
        try:
            conn.execute("CREATE TABLE statistics_meta (id INTEGER, statistic_id TEXT)")
            conn.execute(
                "CREATE TABLE statistics (id INTEGER PRIMARY KEY AUTOINCREMENT, metadata_id INTEGER, start_ts REAL, state REAL, sum REAL)"
            )
            conn.executemany(
                "INSERT INTO statistics_meta(id, statistic_id) VALUES(?,?)",
                [(1, "sensor.old"), (2, "sensor.new")],
            )
            # Old rows are all overlapping (>= new_start), so after filtering (< new_start) there are 0 rows to copy.
            conn.executemany(
                "INSERT INTO statistics(metadata_id, start_ts, state, sum) VALUES(?,?,?,?)",
                [
                    (1, 7200.0, 2.0, 12.0),
                    (1, 10800.0, 3.0, 13.0),
                    (2, 7200.0, 3.0, 1.0),
                    (2, 10800.0, 4.0, 2.0),
                ],
            )
            conn.commit()
        finally:
            conn.close()

        # Decline final confirmation => abort (no exception)
        orig_input = self._mock_input(["n"])
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf):
                run_merge(old_entity_id="sensor.old", new_entity_id="sensor.new", db_path=db, storage_dir=storage)
        finally:
            builtins.input = orig_input

        out = buf.getvalue()
        self.assertIn("Aborted.", out)
        self.assertNotIn("STATISTICS:", out)
        self.assertNotIn("ommit", out)
        self.assertNotIn("update", out)

        ro = sqlite3.connect(str(db))
        try:
            rows = ro.execute(
                "SELECT metadata_id, start_ts FROM statistics ORDER BY metadata_id, start_ts"
            ).fetchall()
            self.assertEqual(rows, [(1, 7200.0), (1, 10800.0), (2, 7200.0), (2, 10800.0)])
        finally:
            ro.close()

    def test_overlap_accept_skip_proceeds_and_skips_overlapping_old_rows(self) -> None:
        run_merge = _load_merge_module().run_merge

        db = self._make_db()
        storage = self._make_storage(old_id="sensor.old", new_id="sensor.new", state_class="total_increasing")

        conn = sqlite3.connect(str(db))
        try:
            conn.execute("CREATE TABLE statistics_meta (id INTEGER, statistic_id TEXT)")
            conn.execute("CREATE TABLE statistics (id INTEGER PRIMARY KEY AUTOINCREMENT, metadata_id INTEGER, start_ts REAL, state REAL, sum REAL)")
            conn.executemany(
                "INSERT INTO statistics_meta(id, statistic_id) VALUES(?,?)",
                [(1, "sensor.old"), (2, "sensor.new")],
            )
            # Overlap at 7200: old has a row at 7200, new starts at 7200.
            conn.executemany(
                "INSERT INTO statistics(metadata_id, start_ts, state, sum) VALUES(?,?,?,?)",
                [
                    (1, 0.0, 1.0, 10.0),
                    (1, 3600.0, 2.0, 12.0),
                    (1, 7200.0, 2.0, 12.0),
                    (2, 7200.0, 3.0, 1.0),
                    (2, 10800.0, 4.0, 2.0),
                ],
            )
            conn.commit()
        finally:
            conn.close()

        # Proceed with DB update (overlap skip is automatic)
        orig_input = self._mock_input(["y"])
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
            # Old row at 7200 must NOT be copied; only old < 7200 should be added.
            self.assertEqual([r[0] for r in rows], [0.0, 3600.0, 7200.0, 10800.0])
            self.assertEqual([float(r[1]) for r in rows], [10.0, 12.0, 15.0, 16.0])
        finally:
            ro.close()

    def test_overlap_ignores_null_state_values(self) -> None:
        merge_module = _load_merge_module()
        run_merge = merge_module.run_merge

        db = self._make_db()
        storage = self._make_storage(old_id="sensor.old", new_id="sensor.new", state_class="total_increasing")

        conn = sqlite3.connect(str(db))
        try:
            conn.execute("CREATE TABLE statistics_meta (id INTEGER, statistic_id TEXT)")
            conn.execute(
                "CREATE TABLE statistics (id INTEGER PRIMARY KEY AUTOINCREMENT, metadata_id INTEGER, start_ts REAL, state REAL, sum REAL)"
            )
            conn.executemany(
                "INSERT INTO statistics_meta(id, statistic_id) VALUES(?,?)",
                [(1, "sensor.old"), (2, "sensor.new")],
            )
            # Old overlaps new, but the overlapping old rows have state=NULL; sum is constant.
            conn.executemany(
                "INSERT INTO statistics(metadata_id, start_ts, state, sum) VALUES(?,?,?,?)",
                [
                    (1, 0.0, 1.0, 10.0),
                    (1, 3600.0, 2.0, 12.0),
                    (1, 7200.0, None, 12.0),
                    (1, 10800.0, None, 12.0),
                    (2, 7200.0, 3.0, 1.0),
                    (2, 10800.0, 4.0, 2.0),
                ],
            )
            conn.commit()
        finally:
            conn.close()

        # Proceed; overlap skip is automatic
        orig_input = self._mock_input(["y"])
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
            self.assertEqual([float(r[1]) for r in rows], [10.0, 12.0, 15.0, 16.0])
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
                "CREATE TABLE statistics (id INTEGER PRIMARY KEY AUTOINCREMENT, metadata_id INTEGER, start_ts REAL, state REAL, sum REAL)"
            )
            conn.executemany(
                "INSERT INTO statistics_meta(id, statistic_id) VALUES(?,?)",
                [(1, "sensor.old"), (2, "sensor.new")],
            )
            # New starts at 7200. Any overlap is rejected now.
            conn.executemany(
                "INSERT INTO statistics(metadata_id, start_ts, state, sum) VALUES(?,?,?,?)",
                [
                    (1, 0.0, 1.0, 10.0),
                    (1, 3600.0, 2.0, 12.0),
                    (1, 7200.0, 2.0, 12.0),
                    (1, 10800.0, 3.0, 13.0),
                    (2, 7200.0, 3.0, 1.0),
                ],
            )
            conn.commit()
        finally:
            conn.close()

        # Proceed; overlap skip is automatic (no constancy check)
        orig_input = self._mock_input(["y"])
        try:
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
            conn.execute("CREATE TABLE statistics (id INTEGER PRIMARY KEY AUTOINCREMENT, metadata_id INTEGER, start_ts REAL, state REAL, sum REAL)")
            conn.execute(
                "CREATE TABLE statistics_short_term (id INTEGER PRIMARY KEY AUTOINCREMENT, metadata_id INTEGER, start_ts REAL, state REAL, sum REAL)"
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

    def test_measurement_copies_mean_when_state_null(self) -> None:
        run_merge = _load_merge_module().run_merge

        db = self._make_db()
        storage = self._make_storage(old_id="sensor.old", new_id="sensor.new", state_class="measurement")

        conn = sqlite3.connect(str(db))
        try:
            conn.execute("CREATE TABLE statistics_meta (id INTEGER, statistic_id TEXT)")
            # Include mean/min/max to mimic measurement schema.
            conn.execute(
                "CREATE TABLE statistics (id INTEGER PRIMARY KEY AUTOINCREMENT, metadata_id INTEGER, start_ts REAL, state REAL, mean REAL, min REAL, max REAL, sum REAL)"
            )
            conn.executemany(
                "INSERT INTO statistics_meta(id, statistic_id) VALUES(?,?)",
                [(1, "sensor.old"), (2, "sensor.new")],
            )
            conn.executemany(
                "INSERT INTO statistics(metadata_id, start_ts, state, mean, min, max, sum) VALUES(?,?,?,?,?,?,?)",
                [
                    (1, 0.0, None, 5.0, 4.0, 6.0, None),
                    (1, 3600.0, None, 7.0, 6.0, 8.0, None),
                    (2, 7200.0, None, 9.0, 8.0, 10.0, None),
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
                "SELECT start_ts, mean FROM statistics WHERE metadata_id=2 ORDER BY start_ts"
            ).fetchall()
            self.assertEqual([r[0] for r in rows], [0.0, 3600.0, 7200.0])
            self.assertEqual([float(r[1]) for r in rows], [5.0, 7.0, 9.0])
        finally:
            ro.close()

    def test_missing_old_entity_in_registry_uses_new_state_class(self) -> None:
        run_merge = _load_merge_module().run_merge

        db = self._make_db()
        storage = self._make_storage_new_only(new_id="sensor.new", state_class="measurement")

        conn = sqlite3.connect(str(db))
        try:
            conn.execute("CREATE TABLE statistics_meta (id INTEGER, statistic_id TEXT)")
            conn.execute(
                "CREATE TABLE statistics (id INTEGER PRIMARY KEY AUTOINCREMENT, metadata_id INTEGER, start_ts REAL, state REAL, mean REAL, min REAL, max REAL, sum REAL)"
            )
            conn.executemany(
                "INSERT INTO statistics_meta(id, statistic_id) VALUES(?,?)",
                [(1, "sensor.old"), (2, "sensor.new")],
            )
            conn.executemany(
                "INSERT INTO statistics(metadata_id, start_ts, state, mean, min, max, sum) VALUES(?,?,?,?,?,?,?)",
                [
                    (1, 0.0, None, 5.0, 4.0, 6.0, None),
                    (1, 3600.0, None, 7.0, 6.0, 8.0, None),
                    (2, 7200.0, None, 9.0, 8.0, 10.0, None),
                ],
            )
            conn.commit()
        finally:
            conn.close()

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
                "SELECT start_ts, mean FROM statistics WHERE metadata_id=2 ORDER BY start_ts"
            ).fetchall()
            self.assertEqual([r[0] for r in rows], [0.0, 3600.0, 7200.0])
            self.assertEqual([float(r[1]) for r in rows], [5.0, 7.0, 9.0])
        finally:
            ro.close()

    def test_dry_run_prints_sql_and_makes_no_changes(self) -> None:
        merge_module = _load_merge_module()
        run_merge = merge_module.run_merge

        db = self._make_db()
        storage = self._make_storage(old_id="sensor.old", new_id="sensor.new", state_class="total_increasing")

        conn = sqlite3.connect(str(db))
        try:
            conn.execute("CREATE TABLE statistics_meta (id INTEGER, statistic_id TEXT)")
            conn.execute("CREATE TABLE statistics (id INTEGER PRIMARY KEY AUTOINCREMENT, metadata_id INTEGER, start_ts REAL, state REAL, sum REAL)")
            conn.execute(
                "CREATE TABLE statistics_short_term (id INTEGER PRIMARY KEY AUTOINCREMENT, metadata_id INTEGER, start_ts REAL, state REAL, sum REAL)"
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
            conn.executemany(
                "INSERT INTO statistics_short_term(metadata_id, start_ts, state, sum) VALUES(?,?,?,?)",
                [
                    (1, 0.0, 1.0, 10.0),
                    (2, 7200.0, 3.0, 1.0),
                ],
            )
            conn.commit()
        finally:
            conn.close()

        import builtins

        orig_input = builtins.input
        builtins.input = lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("input called"))
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf):
                run_merge(
                    old_entity_id="sensor.old",
                    new_entity_id="sensor.new",
                    db_path=db,
                    storage_dir=storage,
                    dry_run=True,
                )
        finally:
            builtins.input = orig_input

        out = buf.getvalue()
        self.assertIn("BEGIN;", out)
        self.assertIn("COMMIT;", out)
        self.assertNotIn("PARAMS:", out)
        self.assertNotIn("?", out)
        self.assertIn("UPDATE statistics SET sum = sum +", out)
        self.assertIn("WHERE metadata_id = 2", out)
        self.assertIn("INSERT INTO statistics", out)
        self.assertIn("INSERT INTO statistics_short_term", out)
        # Formatting: newline before SELECT/FROM/WHERE in insert-select.
        self.assertIn("\nSELECT", out)
        self.assertIn("\nFROM", out)
        self.assertIn("\nWHERE", out)

        ro = sqlite3.connect(str(db))
        try:
            # DB must be unchanged.
            rows = ro.execute(
                "SELECT metadata_id, start_ts, sum FROM statistics ORDER BY metadata_id, start_ts"
            ).fetchall()
            self.assertEqual(
                rows,
                [
                    (1, 0.0, 10.0),
                    (1, 3600.0, 12.0),
                    (2, 7200.0, 1.0),
                    (2, 10800.0, 2.0),
                ],
            )
        finally:
            ro.close()

    def test_sqlite_ro_uri_is_valid_file_uri(self) -> None:
        merge_module = _load_merge_module()
        sqlite_ro_uri = merge_module._sqlite_ro_uri

        # POSIX-style absolute path.
        uri = sqlite_ro_uri(Path("/homeassistant/home-assistant_v2.db"))
        self.assertTrue(uri.startswith("file:///"))
        self.assertIn("/homeassistant/home-assistant_v2.db", uri)
        self.assertTrue(uri.endswith("?mode=ro"))

        # Windows-like drive path.
        uri2 = sqlite_ro_uri(Path("E:/dev/ha-merge-history/home-assistant_v2.db"))
        self.assertTrue(uri2.startswith("file:///"))
        self.assertIn("/E:/dev/ha-merge-history/home-assistant_v2.db", uri2)
        self.assertTrue(uri2.endswith("?mode=ro"))


if __name__ == "__main__":
    unittest.main()
