"""Autonomous aggregate export tests using local synthetic typed Parquet."""

from datetime import date
from pathlib import Path
import tempfile
import unittest

import duckdb

from github_analytics.export import export_date, SCHEMAS
from scripts.verify_exports import verify_exports
from github_analytics.publication import PublicationError, publish, published_partition, recover_partition
from tests.test_publication import fixture_writer, fail_at

DAY = date(2025, 6, 1)
NEXT = date(2025, 6, 2)
SPECIAL = 'repo,with"quotes\nand newline'
VALUES = {
    "event_counts": [("PushEvent", 0, False, 3)],
    "daily_volume": [(3,)],
    "top_repositories": [(1, SPECIAL, 2), (2, "z/repo", 1)],
    "top_actors": [(1, "alice", 2), (2, "service[bot]", 1)],
    "rejection_counts": [("outside_date", 2), ("unsupported_event_type", 1)],
}


def write_metrics(root, day, empty=False, overrides=None):
    with duckdb.connect(config={"threads": 2}) as connection:
        for name, fields in SCHEMAS.items():
            schema = {k: v for k, v in fields.items() if k != "event_date"}
            schema, rows = (overrides or {}).get(name, (schema, VALUES[name]))
            connection.execute("CREATE OR REPLACE TABLE fixture (" + ",".join(f"{k} {v}" for k, v in schema.items()) + ")")
            if not empty and rows:
                connection.executemany("INSERT INTO fixture VALUES (" + ",".join("?" for _ in schema) + ")", rows)
            def writer(stage):
                stage.mkdir()
                path = str(stage / "part.parquet").replace("'", "''")
                connection.execute(f"COPY fixture TO '{path}' (FORMAT PARQUET)")
                (stage / "_SUCCESS").touch()
                return connection.execute("SELECT count(*) FROM fixture").fetchone()[0], schema
            publish(root / name, day, writer)


class ExportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.parquet, self.csv, self.database = self.root / "parquet", self.root / "csv", self.root / "metrics.duckdb"

    def tearDown(self):
        self.temporary.cleanup()

    def snapshot(self):
        with duckdb.connect(str(self.database), read_only=True, config={"threads": 2}) as connection:
            return {name: sorted(connection.execute(f"SELECT * FROM {name}").fetchall()) for name in SCHEMAS}

    def test_roundtrip_escaping_types_rerun_empty_and_other_date(self):
        write_metrics(self.parquet, DAY)
        write_metrics(self.parquet, NEXT)
        for day in (DAY, NEXT):
            self.assertEqual(export_date(self.parquet, day, self.csv, self.database),
                             {"event_counts": 1, "daily_volume": 1, "top_repositories": 2, "top_actors": 2, "rejection_counts": 2})
        baseline = verify_exports(self.parquet, self.csv, self.database, DAY)
        snapshot = self.snapshot()
        files = {str(path): path.read_bytes() for path in self.csv.rglob("*.csv")}
        export_date(self.parquet, DAY, self.csv, self.database)
        self.assertEqual(self.snapshot(), snapshot)
        self.assertEqual(verify_exports(self.parquet, self.csv, self.database, DAY), baseline)
        self.assertEqual({str(path): path.read_bytes() for path in self.csv.rglob("*.csv")}, files)
        with duckdb.connect(str(self.database), read_only=True, config={"threads": 2}) as connection:
            row = connection.execute("SELECT event_date,event_hour,is_bot,event_count FROM event_counts WHERE event_date=?", [DAY]).fetchone()
            self.assertEqual(row, (DAY, 0, False, 3))
            self.assertIsInstance(row[0], date)
            self.assertIsInstance(row[2], bool)
        write_metrics(self.parquet, DAY, empty=True)
        export_date(self.parquet, DAY, self.csv, self.database)
        report = verify_exports(self.parquet, self.csv, self.database, DAY)
        self.assertTrue(all(value["rows"] == 0 for value in report["tables"].values()))
        for name, rows in self.snapshot().items():
            self.assertEqual(rows, [row for row in snapshot[name] if row[0] == NEXT])
        for name in ("top_repositories", "top_actors"):
            self.assertEqual((self.csv / f"event_date={NEXT}" / f"{name}.csv").read_bytes(), files[str(self.csv / f"event_date={NEXT}" / f"{name}.csv")])
            self.assertEqual((self.csv / f"event_date={DAY}" / f"{name}.csv").read_text(), ",".join(SCHEMAS[name]) + "\n")

    def test_late_missing_input_rolls_back_five_tables_other_date_and_csv(self):
        write_metrics(self.parquet, DAY)
        write_metrics(self.parquet, NEXT)
        export_date(self.parquet, DAY, self.csv, self.database)
        export_date(self.parquet, NEXT, self.csv, self.database)
        snapshot = self.snapshot()
        files = {str(path): path.read_bytes() for path in self.csv.rglob("*.csv")}
        write_metrics(self.parquet, DAY, overrides={"daily_volume": ({"event_count": "BIGINT"}, [(99,)])})
        Path(published_partition(self.parquet / "rejection_counts", DAY)[0][0]).unlink()
        with self.assertRaises(PublicationError):
            export_date(self.parquet, DAY, self.csv, self.database)
        self.assertEqual(self.snapshot(), snapshot)
        self.assertEqual({str(path): path.read_bytes() for path in self.csv.rglob("*.csv")}, files)
        # Failure in the last target table also rolls back earlier replacements.
        # Restore the tampered file from the still-retained prior generation.
        damaged = self.parquet / "rejection_counts" / f"event_date={DAY}" / "_generations"
        generations = sorted(damaged.iterdir(), key=lambda path: path.stat().st_mtime_ns)
        (generations[-1] / "part.parquet").write_bytes((generations[0] / "part.parquet").read_bytes())
        write_metrics(self.parquet, DAY)
        with duckdb.connect(str(self.database), config={"threads": 2}) as connection:
            connection.execute("ALTER TABLE rejection_counts ADD COLUMN incompatible INTEGER DEFAULT 7")
        snapshot = self.snapshot()
        write_metrics(self.parquet, DAY, overrides={"daily_volume": ({"event_count": "BIGINT"}, [(99,)])})
        with self.assertRaisesRegex(ValueError, "Unexpected target schema"):
            export_date(self.parquet, DAY, self.csv, self.database)
        self.assertEqual(self.snapshot(), snapshot)
        self.assertEqual({str(path): path.read_bytes() for path in self.csv.rglob("*.csv")}, files)

    def test_rejection_metric_invalid_schema_date_null_positive_reason_grain_rolls_back(self):
        write_metrics(self.parquet, DAY)
        write_metrics(self.parquet, NEXT)
        export_date(self.parquet, DAY, self.csv, self.database)
        export_date(self.parquet, NEXT, self.csv, self.database)
        snapshot = self.snapshot()
        files = {str(path): path.read_bytes() for path in self.csv.rglob("*.csv")}
        normal = {"rejection_reason": "VARCHAR", "event_count": "BIGINT"}
        cases = [
            ({"rejection_reason": "VARCHAR", "event_count": "INTEGER"}, [("outside_date", 1)]),
            ({"event_date": "DATE", **normal}, [(NEXT, "outside_date", 1)]),
            (normal, [(None, 1)]), (normal, [("outside_date", None)]),
            (normal, [("", 1)]), (normal, [("unknown_reason", 1)]),
            (normal, [("outside_date ", 1)]), (normal, [("outside_date", 0)]),
            (normal, [("outside_date", -1)]),
            (normal, [("outside_date", 1), ("outside_date", 2)]),
        ]
        for schema, rows in cases:
            with self.subTest(schema=schema, rows=rows):
                write_metrics(self.parquet, DAY, overrides={
                    "daily_volume": ({"event_count": "BIGINT"}, [(99,)]),
                    "rejection_counts": (schema, rows)})
                with self.assertRaises(ValueError):
                    export_date(self.parquet, DAY, self.csv, self.database)
                self.assertEqual(self.snapshot(), snapshot)
                self.assertEqual({str(path): path.read_bytes() for path in self.csv.rglob("*.csv")}, files)

    def test_interrupted_fifth_metric_rolls_back_and_recovery_retry_succeeds(self):
        write_metrics(self.parquet, DAY)
        write_metrics(self.parquet, NEXT)
        export_date(self.parquet, DAY, self.csv, self.database)
        export_date(self.parquet, NEXT, self.csv, self.database)
        snapshot = self.snapshot()
        files = {str(path): path.read_bytes() for path in self.csv.rglob("*.csv")}
        write_metrics(self.parquet, DAY, overrides={"daily_volume": ({"event_count": "BIGINT"}, [(99,)])})
        target = self.parquet / "rejection_counts"
        with self.assertRaises(RuntimeError):
            publish(target, DAY, fixture_writer(9), fail_at("stage_written"))
        with self.assertRaises(PublicationError):
            export_date(self.parquet, DAY, self.csv, self.database)
        with self.assertRaises(PublicationError):
            verify_exports(self.parquet, self.csv, self.database, DAY)
        self.assertEqual(self.snapshot(), snapshot)
        self.assertEqual({str(path): path.read_bytes() for path in self.csv.rglob("*.csv")}, files)
        recover_partition(target, DAY)
        export_date(self.parquet, DAY, self.csv, self.database)
        self.assertEqual(self.snapshot()["rejection_counts"], snapshot["rejection_counts"])
        verify_exports(self.parquet, self.csv, self.database, DAY)

    def test_reason_count_tampering_with_unchanged_sql_total_is_detected(self):
        write_metrics(self.parquet, DAY)
        export_date(self.parquet, DAY, self.csv, self.database)
        with duckdb.connect(str(self.database), config={"threads": 2}) as connection:
            connection.execute("UPDATE rejection_counts SET event_count=3-event_count")
            self.assertEqual(connection.execute("SELECT sum(event_count) FROM rejection_counts").fetchone()[0], 3)
        with self.assertRaisesRegex(AssertionError, "SQL rows differ"):
            verify_exports(self.parquet, self.csv, self.database, DAY)

    def test_invalid_schema_date_null_and_grain_roll_back(self):
        write_metrics(self.parquet, DAY)
        export_date(self.parquet, DAY, self.csv, self.database)
        snapshot = self.snapshot()
        cases = [
            ({"event_count": "INTEGER"}, [(3,)]),
            ({"event_date": "DATE", "event_count": "BIGINT"}, [(NEXT, 3)]),
            ({"event_count": "BIGINT"}, [(None,)]),
            ({"event_count": "BIGINT"}, [(3,), (3,)]),
        ]
        for schema, rows in cases:
            with self.subTest(schema=schema, rows=rows):
                write_metrics(self.parquet, DAY, overrides={"daily_volume": (schema, rows)})
                with self.assertRaises(ValueError):
                    export_date(self.parquet, DAY, self.csv, self.database)
                self.assertEqual(self.snapshot(), snapshot)

    def test_independent_csv_and_duckdb_modes(self):
        write_metrics(self.parquet, DAY)
        export_date(self.parquet, DAY, csv_root=self.csv)
        self.assertFalse(self.database.exists())
        export_date(self.parquet, DAY, database=self.database)
        self.assertEqual(verify_exports(self.parquet, self.csv, self.database, DAY)["tables"]["top_repositories"]["rows"], 2)

    def test_independent_verifier_detects_csv_and_sql_tampering(self):
        write_metrics(self.parquet, DAY)
        export_date(self.parquet, DAY, self.csv, self.database)
        with duckdb.connect(str(self.database), config={"threads": 2}) as connection:
            connection.execute("UPDATE daily_volume SET event_count=99")
        with self.assertRaisesRegex(AssertionError, "SQL rows differ"):
            verify_exports(self.parquet, self.csv, self.database, DAY)
        export_date(self.parquet, DAY, self.csv, self.database)
        (self.csv / f"event_date={DAY}" / "top_actors.csv").write_text("wrong,header\n")
        with self.assertRaisesRegex(AssertionError, "CSV header"):
            verify_exports(self.parquet, self.csv, self.database, DAY)
