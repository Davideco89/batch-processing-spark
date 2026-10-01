"""Autonomous integration check; requires the Docker Spark runtime."""

from datetime import date
from pathlib import Path
import tempfile
import unittest

from github_analytics.session import create_session
from github_analytics.storage import read_date, read_dates, write_date, write_partitioned
from github_analytics.publication import published_partition


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
            rows = read_dates(self.spark, output).collect()
            self.assertEqual({(r.value, r.event_date) for r in rows},
                             {("new", first), ("keep", second)})
            self.assertEqual(len(rows), 2)
            for day in (first, second):
                self.assertTrue(published_partition(output, day)[0])
            self.assertEqual(self.spark.conf.get("spark.sql.session.timeZone"), "UTC")

    def test_lazy_reader_keeps_immutable_generation_after_replacement_and_empty_rerun(self):
        day = date(2025, 6, 1)
        with tempfile.TemporaryDirectory() as directory:
            original = self.spark.createDataFrame([("old", day)], "value string,event_date date")
            replacement = self.spark.createDataFrame([("new", day)], original.schema)
            write_date(original, directory, day)
            delayed = read_date(self.spark, directory, day)
            old_files = delayed.inputFiles()
            write_date(replacement, directory, day)
            self.assertEqual([row.value for row in delayed.collect()], ["old"])
            self.assertEqual([row.value for row in read_date(self.spark, directory, day).collect()], ["new"])
            self.assertTrue(all(Path(path.replace("file://", "")).exists() for path in old_files))
            write_date(replacement.limit(0), directory, day)
            self.assertEqual(read_date(self.spark, directory, day).count(), 0)
            self.assertEqual([row.value for row in delayed.collect()], ["old"])
            self.assertEqual(read_date(self.spark, directory, day).schema, delayed.schema)
