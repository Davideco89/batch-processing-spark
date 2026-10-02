"""CLI destinations are coherent; explicit local overrides stay isolated."""

from datetime import date
from unittest import TestCase
from unittest.mock import patch

from github_analytics import export, runner
from github_analytics.paths import RAW_ROOT, PARQUET_ROOT, CSV_ROOT, DATABASE
from scripts.compare_layout import historical_source_uri


class PathTests(TestCase):
    def test_original_provenance_survives_physical_relocation(self):
        original = "file:///old/archive/2025-06-01-0.json.gz"
        recorded = {"samples": {"PushEvent": [{"source_file": original}]},
                    "rejected_samples": [{"source_file": original}]}
        self.assertEqual(historical_source_uri(recorded, original), original)
        with self.assertRaisesRegex(AssertionError, "historical URI metadata"):
            historical_source_uri(recorded, "file:///data/test/relocated/archive.json.gz")

    def test_pipeline_default_and_explicit_paths(self):
        for options, raw, output in (([], RAW_ROOT, PARQUET_ROOT),
                                     (["--raw-root", "/tmp/raw", "--output-root", "/tmp/parquet"], "/tmp/raw", "/tmp/parquet")):
            with self.subTest(options=options), patch("sys.argv", ["pipeline", "--date", "2025-06-01", *options]), patch.object(runner, "create_session") as session, patch.object(runner, "run_stage") as run:
                runner.main("pipeline")
                run.assert_called_once_with(session.return_value, "pipeline", date(2025, 6, 1), raw, output, 10,
                                            from_stage="ingest", shuffle_partitions=None, target_bytes=134217728)

    def test_export_defaults_and_modes_with_explicit_overrides(self):
        cases = [([], PARQUET_ROOT, CSV_ROOT, DATABASE),
                 (["--parquet-root", "/tmp/parquet", "--csv-root", "/tmp/csv", "--database", "/tmp/local.duckdb"], "/tmp/parquet", "/tmp/csv", "/tmp/local.duckdb"),
                 (["--mode", "csv"], PARQUET_ROOT, CSV_ROOT, None),
                 (["--mode", "duckdb"], PARQUET_ROOT, None, DATABASE)]
        for options, parquet, csv, database in cases:
            with self.subTest(options=options), patch("sys.argv", ["export", "--date", "2025-06-01", *options]), patch.object(export, "export_date") as run:
                export.main()
                run.assert_called_once_with(parquet, date(2025, 6, 1), csv, database)
