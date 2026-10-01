"""Autonomous duplicate/conflict checks across files, stages and reruns."""

from collections import Counter
from copy import deepcopy
from datetime import date
from pathlib import Path
import json
import tempfile
import unittest

from pyspark.sql import SparkSession, functions as F
from github_analytics.ingest import ingest
from github_analytics.runner import run_stage
from github_analytics.session import create_session
from github_analytics.storage import read_date, read_dates, write_date
from github_analytics.transform import transform
from scripts.profile_archive import profile, raw_oracle
from tests.fixture_data import event, write_archive

DAY = date(2025, 6, 1)
NEXT_DAY = date(2025, 6, 2)
DATASETS = ("ingested", "clean", "rejected", "event_counts", "daily_volume", "top_repositories", "top_actors")


def duplicate_fixture(raw):
    def make(identifier, kind="PushEvent", actor="alice", payload=None):
        return event(identifier, kind, actor, "acme/alpha", "2025-06-01T00:10:00Z", payload)
    first = [make(1), make(1), make(2), make(3, "WatchEvent"),
             make(4, "PullRequestEvent", payload={"number": 42, "pull_request": {"number": None}}),
             make(5, "IssuesEvent", payload={"issue": {"labels": [{"name": "bug"}, {"name": "help"}]}})]
    invalid_stamp = make(7)
    invalid_stamp["created_at"] = "not-a-timestamp"
    outside = make(8)
    outside["created_at"] = "2025-06-02T00:00:00Z"
    second = [make(1), make(2, actor="bob"), make(3, "WatchEvent", actor=None),
              make(5, "IssuesEvent", payload={"issue": {"labels": [{"name": "help"}, {"name": "bug"}]}})]
    write_archive(raw / "2025-06-01-0.json.gz", first + [invalid_stamp, outside])
    write_archive(raw / "2025-06-01-1.json.gz", second, malformed=True)
    other = event(1, "WatchEvent", "bob", "acme/beta", "2025-06-02T00:00:00Z")
    write_archive(raw / "2025-06-02-0.json.gz", [other, deepcopy(other)])


class DeduplicationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        SparkSession.builder.master("local[2]").getOrCreate()
        cls.spark = create_session("deduplication-tests")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_quality_precedence_conflicts_nested_arrays_and_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            raw = Path(directory)
            duplicate_fixture(raw)
            clean, rejected = transform(ingest(self.spark, raw, DAY), DAY)
            rows = {r.event_id: r for r in clean.collect()}
            self.assertEqual(set(rows), {"1", "3", "4"})
            self.assertEqual(rows["4"].pr_number, 42)
            self.assertEqual(rows["1"].source_file, (raw / "2025-06-01-0.json.gz").as_uri())
            self.assertEqual(Counter(r.rejection_reason for r in rejected.collect()), {
                "duplicate_event_id": 2, "conflicting_event_id": 4, "missing_actor_login": 1,
                "invalid_timestamp": 1, "outside_date": 1, "corrupt_json": 1})
            self.assertEqual(clean.count() + rejected.count(), 13)
            oracle = raw_oracle(raw, DAY)
            self.assertEqual((oracle[1], len(oracle[2]), len(oracle[7])), (13, 3, 10))
            self.assertEqual(next(r for r in oracle[2] if r["event_id"] == "4")["pr_number"], 42)
            # Physical input ordering cannot change selected provenance or rejected multiplicity.
            reversed_clean, reversed_rejected = transform(ingest(self.spark, raw, DAY).repartition(3), DAY)
            self.assertEqual(sorted(clean.toJSON().collect()), sorted(reversed_clean.toJSON().collect()))
            self.assertEqual(sorted(rejected.toJSON().collect()), sorted(reversed_rejected.toJSON().collect()))

    def test_full_oracle_stage_equivalence_rerun_other_date_and_empty_replacement(self):
        with tempfile.TemporaryDirectory() as directory:
            raw, output, separate = [Path(directory) / name for name in ("raw", "output", "separate")]
            duplicate_fixture(raw)
            run_stage(self.spark, "pipeline", DAY, raw, output)
            report = profile(self.spark, raw, output, DAY, 10)
            self.assertEqual((report["accepted"], report["rejected"], report["duplicate_event_ids"]), (3, 10, 0))
            run_stage(self.spark, "pipeline", NEXT_DAY, raw, output)
            snapshot = {name: sorted(read_dates(self.spark, output / name).toJSON().collect()) for name in DATASETS}
            run_stage(self.spark, "pipeline", DAY, raw, output)
            for name in DATASETS:
                self.assertEqual(sorted(read_dates(self.spark, output / name).toJSON().collect()), snapshot[name])
            for stage in ("ingest", "transform", "aggregate"):
                run_stage(self.spark, stage, DAY, raw, separate)
            for name in DATASETS:
                self.assertEqual(sorted(read_date(self.spark, output / name, DAY).toJSON().collect()),
                                 sorted(read_date(self.spark, separate / name, DAY).toJSON().collect()))
            # A timestamp-only rejected-row alteration preserves IDs/reasons/counts;
            # the independent full-row multiset must still detect it.
            saved = read_date(self.spark, output / "rejected", DAY)
            saved = self.spark.createDataFrame(saved.collect(), saved.schema)
            write_date(saved.withColumn("event_timestamp", F.lit(None).cast("timestamp")), output / "rejected", DAY)
            with self.assertRaisesRegex(AssertionError, "Full rejected row multiset differs"):
                profile(self.spark, raw, output, DAY, 10)
            write_date(saved, output / "rejected", DAY)
            # Replace the first date with only conflicting valid copies, producing empty metrics.
            one = event(20, "PushEvent", "alice", "acme/alpha", "2025-06-01T00:00:00Z")
            two = deepcopy(one)
            two["actor"]["login"] = "bob"
            write_archive(raw / "2025-06-01-0.json.gz", [one, two])
            (raw / "2025-06-01-1.json.gz").unlink()
            run_stage(self.spark, "pipeline", DAY, raw, output)
            empty = profile(self.spark, raw, output, DAY, 10)
            self.assertEqual((empty["accepted"], empty["rejected"]), (0, 2))
            for name in ("clean", "event_counts", "daily_volume", "top_repositories", "top_actors"):
                self.assertEqual(read_date(self.spark, output / name, DAY).count(), 0)
            for name in DATASETS:
                self.assertEqual(sorted(read_date(self.spark, output / name, NEXT_DAY).toJSON().collect()),
                                 sorted(row for row in snapshot[name] if json.loads(row)["event_date"] == str(NEXT_DAY)))
