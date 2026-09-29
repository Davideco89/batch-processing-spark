"""Autonomous integration check; requires the Docker Spark runtime."""

from datetime import date
from pathlib import Path
import tempfile
import unittest

from github_analytics.session import create_session
from github_analytics.storage import write_partitioned


class StorageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # unittest launches Python directly, so it must select a local master.
        from pyspark.sql import SparkSession

        SparkSession.builder.master("local[2]").getOrCreate()
        cls.spark = create_session("github-events-storage-test")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_roundtrip_and_rerun_preserve_other_dates(self):
        first, second = date(2025, 6, 1), date(2025, 6, 2)
        with tempfile.TemporaryDirectory() as directory:
            output = str(Path(directory) / "parquet")
            original = self.spark.createDataFrame(
                [("old", first), ("keep", second)], "value string, event_date date"
            )
            replacement = self.spark.createDataFrame(
                [("new", first)], "value string, event_date date"
            )
            write_partitioned(original, output)
            write_partitioned(replacement, output)
            write_partitioned(replacement, output)
            rows = self.spark.read.parquet(output).collect()
            self.assertEqual({(r.value, r.event_date) for r in rows},
                             {("new", first), ("keep", second)})
            self.assertEqual(len(rows), 2)
            for day in (first, second):
                self.assertTrue(list(
                    (Path(output) / f"event_date={day}").glob("*.parquet")
                ))
            self.assertEqual(self.spark.conf.get("spark.sql.session.timeZone"), "UTC")
