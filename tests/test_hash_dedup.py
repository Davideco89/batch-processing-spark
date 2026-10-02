"""Exact native-hash fallback with deliberate collisions and distributed equality."""

from datetime import date
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from pyspark.sql import SparkSession, Window, functions as F
from github_analytics.ingest import ingest
from github_analytics.session import create_session
from github_analytics.transform import BUSINESS_FIELDS, deduplicate_valid
from tests.fixture_data import event, write_archive


def legacy_dedup(frame):
    """Frozen pre-optimization implementation for exact comparison/benchmark."""
    group = Window.partitionBy("event_date", "event_id")
    tagged = frame.withColumn("_business", F.to_json(F.struct(*BUSINESS_FIELDS), {"ignoreNullFields": "false"}))
    tagged = (tagged.withColumn("_conflict", F.min("_business").over(group) != F.max("_business").over(group))
              .withColumn("_copy", F.row_number().over(group.orderBy(F.col("source_file").asc_nulls_last()))))
    return (tagged.withColumn("rejection_reason", F.when(F.col("_conflict"), "conflicting_event_id")
                             .when(F.col("_copy") > 1, "duplicate_event_id"))
            .drop("_business", "_conflict", "_copy"))


class HashDedupTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        SparkSession.builder.master("local[2]").getOrCreate()
        cls.spark = create_session("hash-dedup-tests")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_forced_collisions_null_arrays_timestamp_text_and_reordering(self):
        with tempfile.TemporaryDirectory() as directory:
            raw = Path(directory)
            def make(identifier, labels=None, stamp="2025-06-01T00:00:00Z"):
                return event(identifier, "IssuesEvent", "alice", "acme/repo", stamp,
                             {"issue": {"labels": labels}})
            records = [make("same"), make("same"), make("unique"),
                       make("ordered", [{"name": "a"}, {"name": "b"}]),
                       make("ordered", [{"name": "b"}, {"name": "a"}]),
                       make("null", None), make("null", []),
                       make("stamp"), make("stamp", stamp="2025-06-01T01:00:00+01:00")]
            write_archive(raw / "2025-06-01-0.json.gz", records)
            source = ingest(self.spark, raw, date(2025, 6, 1)).withColumn(
                "event_timestamp", F.to_timestamp("created_at"))
            before = legacy_dedup(source)
            for partitions in (2, 8):
                self.spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
                with patch("github_analytics.transform.F.xxhash64", return_value=F.lit(42).cast("long")):
                    after = deduplicate_valid(source.repartition(partitions))
                self.assertEqual(before.schema, after.schema)
                self.assertEqual(before.exceptAll(after).count(), 0)
                self.assertEqual(after.exceptAll(before).count(), 0)
                self.assertEqual(after.filter("rejection_reason='conflicting_event_id'").count(), 6)
                self.assertEqual(after.filter("rejection_reason='duplicate_event_id'").count(), 1)
                self.assertEqual(after.filter("rejection_reason IS NULL").count(), 2)
            # The optimized logical plan must retain a conditional exact check,
            # not serialize a JSON signature unconditionally before grouping.
            plan = deduplicate_valid(source)._jdf.queryExecution().optimizedPlan().toString()
            self.assertIn("CASE WHEN", plan)
            self.assertIn("xxhash64", plan)
            self.assertIn("to_json", plan)
