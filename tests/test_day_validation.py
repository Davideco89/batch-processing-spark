"""Autonomous guarded acquisition and independent distributed day checks."""

from datetime import date
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from pyspark.sql import SparkSession, functions as F
from github_analytics.session import create_session
from github_analytics.runner import run_stage
from github_analytics.storage import write_date
from scripts.acquire_archive import acquire, validate_plan
from scripts.stream_oracle import build_oracle
from scripts.validate_parquet import validate, equal_rows, read_partition, validate_sources
from tests.test_deduplication import duplicate_fixture

DAY = date(2025, 6, 1)


class AcquisitionTests(unittest.TestCase):
    def test_independent_input_coverage_and_hashes_must_match_manifest(self):
        oracle = {"date": str(DAY), "inputs": [{"filename": "2025-06-01-0.json.gz", "compressed_bytes": 3, "sha256": "abc"}]}
        manifest = {"complete": True, "date": str(DAY), "hours": [0], "completed": [
            {"url": "https://data.gharchive.org/2025-06-01-0.json.gz", "compressed_bytes": 3, "sha256": "abc"}]}
        validate_sources(oracle, manifest)
        manifest["completed"][0]["sha256"] = "different"
        with self.assertRaisesRegex(AssertionError, "hashes"):
            validate_sources(oracle, manifest)

    def test_guard_reuse_and_size_mismatch_without_network(self):
        source = {"hour": 0, "url": "https://data.gharchive.org/2025-06-01-0.json.gz", "head_bytes": 3}
        plan = {"date": str(DAY), "sources": [source]}
        validate_plan(plan, DAY, [0], 3, 3)
        with self.assertRaises(ValueError):
            validate_plan(plan, DAY, [0], 2, 3)
        with self.assertRaises(ValueError):
            validate_plan(plan, DAY, [0], 3, 2)
        with self.assertRaises(AssertionError):
            validate_plan(plan, DAY, [0, 1], 3, 6)
        with tempfile.TemporaryDirectory() as directory, patch("scripts.acquire_archive.urlopen") as network:
            path = Path(directory) / "2025-06-01-0.json.gz"
            path.write_bytes(b"abc")
            result = acquire(source, directory, 3)
            self.assertTrue(result["reused"])
            self.assertEqual(result["sha256"], hashlib.sha256(b"abc").hexdigest())
            network.assert_not_called()
            path.write_bytes(b"ab")
            with self.assertRaises(ValueError):
                acquire(source, directory, 3)


class DistributedOracleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        SparkSession.builder.master("local[2]").getOrCreate()
        cls.spark = create_session("distributed-oracle-tests")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_complete_oracle_conflicts_nested_fields_and_multiplicity(self):
        with tempfile.TemporaryDirectory() as directory:
            raw, output, oracle = [Path(directory) / name for name in ("raw", "parquet", "oracle")]
            duplicate_fixture(raw)
            expected = build_oracle(raw, DAY, oracle)
            self.assertEqual((expected["raw_lines"], expected["accepted"], expected["rejected"]), (13, 3, 10))
            self.assertEqual(expected["rejection_reasons"]["duplicate_event_id"], 2)
            self.assertEqual(expected["rejection_reasons"]["conflicting_event_id"], 4)
            reason_rows = [json.loads(line) for line in (oracle / "rejection_counts.json").read_text().splitlines()]
            self.assertEqual({row["rejection_reason"]: row["event_count"] for row in reason_rows}, expected["rejection_reasons"])
            self.assertTrue(all(row["event_date"] == str(DAY) for row in reason_rows))
            rows = [json.loads(line) for line in (oracle / "clean.json").read_text().splitlines()]
            self.assertEqual(next(row for row in rows if row["event_id"] == "4")["pr_number"], 42)
            run_stage(self.spark, "pipeline", DAY, raw, output)
            result = validate(self.spark, output, DAY, oracle=oracle)
            self.assertEqual(result["datasets"]["clean"]["rows"], 3)
            self.assertEqual(result["published_rejection_counts"], expected["rejection_reasons"])
            frame = read_partition(self.spark, output, "clean", DAY)
            with self.assertRaisesRegex(AssertionError, "multiplicity"):
                equal_rows(frame.unionByName(frame.limit(1)), frame, "clean")
            with self.assertRaisesRegex(AssertionError, "multiplicity"):
                equal_rows(frame.withColumn("event_timestamp", F.col("event_timestamp") + F.expr("INTERVAL 1 SECOND")), frame, "clean")
            reasons = read_partition(self.spark, output, "rejection_counts", DAY)
            # Swap two counts while preserving the total: aggregate-only sum
            # checks would miss this corruption, but exact reason checks must not.
            tampered = reasons.withColumn("event_count", F.when(F.col("rejection_reason") == "duplicate_event_id", F.lit(4))
                                          .when(F.col("rejection_reason") == "conflicting_event_id", F.lit(2))
                                          .otherwise(F.col("event_count")).cast("long"))
            write_date(tampered, output / "rejection_counts", DAY)
            with self.assertRaisesRegex(AssertionError, "multiplicity"):
                validate(self.spark, output, DAY)
            with self.assertRaisesRegex(AssertionError, "multiplicity"):
                validate(self.spark, output, DAY, oracle=oracle)
