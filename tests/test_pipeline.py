"""Golden-data integration checks using isolated temporary gzip and Parquet."""

from datetime import date
from pathlib import Path
import tempfile
import unittest

from pyspark.sql import SparkSession, functions as F
from github_analytics.aggregate import aggregate
from github_analytics.ingest import ingest
from github_analytics.runner import run_stage
from github_analytics.session import create_session
from github_analytics.storage import read_date, write_date
from github_analytics.transform import transform
from tests.fixture_data import event, write_archive, write_fixture

DAY = date(2025, 6, 1)


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        SparkSession.builder.master("local[2]").getOrCreate()
        cls.spark = create_session("github-events-pipeline-test")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_normalization_validation_and_metrics(self):
        with tempfile.TemporaryDirectory() as directory:
            write_fixture(directory)
            ingested = ingest(self.spark, directory, DAY).cache()
            try:
                self.assertEqual(ingested.count(), 13)
                clean, rejected = transform(ingested, DAY)
                rows = {r.event_id: r for r in clean.collect()}
                self.assertEqual(len(rows), 8)
                self.assertEqual(rows["4"].pr_number, 42)
                self.assertEqual(rows["5"].pr_number, 43)
                self.assertEqual(rows["6"].issue_labels, ["bug", "help wanted"])
                self.assertEqual(rows["6"].issue_number, 7)
                self.assertIsNone(rows["6"].pr_number)
                self.assertIsNone(rows["8"].org_login)
                self.assertEqual(rows["3"].event_hour, 1)
                self.assertEqual(rows["3"].event_date, DAY)
                self.assertTrue(rows["3"].is_bot)
                self.assertFalse(rows["1"].is_bot)
                self.assertEqual({r.event_category for r in rows.values()},
                                 {"content", "collaboration", "passive"})
                self.assertEqual({r.rejection_reason: r["count"] for r in rejected.groupBy("rejection_reason").count().collect()},
                                 {"corrupt_json": 1, "missing_actor_login": 1, "invalid_timestamp": 1,
                                  "unsupported_event_type": 1, "outside_date": 1})
                metrics = aggregate(clean)
                self.assertEqual(metrics["daily_volume"].first().event_count, 8)
                self.assertEqual({(r.event_type, r.event_hour, r.is_bot): r.event_count
                                  for r in metrics["event_counts"].collect()}, {
                    ("PushEvent", 0, False): 2, ("PushEvent", 1, True): 1,
                    ("PullRequestEvent", 1, False): 1, ("PullRequestEvent", 1, True): 1,
                    ("IssuesEvent", 2, False): 1, ("WatchEvent", 2, False): 2})
                self.assertEqual([(r.repo_name, r.event_count, r.rank) for r in metrics["top_repositories"].orderBy("rank").collect()],
                                 [("acme/alpha", 5, 1), ("acme/beta", 3, 2)])
                self.assertEqual([(r.actor_login, r.event_count, r.rank) for r in metrics["top_actors"].orderBy("rank").collect()],
                                 [("alice", 4, 1), ("bob", 2, 2), ("ci[bot]", 2, 3)])
                self.assertEqual(aggregate(clean, 1)["top_actors"].first().actor_login, "alice")
            finally:
                ingested.unpersist()

    def test_timestamp_offsets_and_required_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            records = [event(1, "PushEvent", "Robot", "acme/alpha", "2025-06-02T01:30:00+02:00"),
                       event(2, "WatchEvent", "SERVICE[BOT]", "acme/alpha", "2025-06-01T01:30:00.123Z")]
            for identifier, field, value in [(3, "repo", {"name": " "}),
                                              (4, "id", None), (5, "type", None)]:
                record = event(identifier, "PushEvent", "alice", "acme/alpha", "2025-06-01T00:00:00Z")
                record[field] = value
                records.append(record)
            write_archive(Path(directory) / "2025-06-01-0.json.gz", records)
            clean, rejected = transform(ingest(self.spark, directory, DAY), DAY)
            rows = {r.event_id: r for r in clean.collect()}
            self.assertEqual(rows["1"].event_hour, 23)
            self.assertFalse(rows["1"].is_bot)
            self.assertTrue(rows["2"].is_bot)
            self.assertEqual({r.rejection_reason for r in rejected.collect()},
                             {"missing_repo_name", "missing_event_id", "missing_event_type"})

    def test_chain_independent_stages_rerun_and_empty_replacement(self):
        with tempfile.TemporaryDirectory() as directory:
            raw, output, separate = [Path(directory) / name for name in ("raw", "output", "separate")]
            write_fixture(raw)
            for day in (DAY, date(2025, 6, 2)):
                run_stage(self.spark, "pipeline", day, raw, output)
            names = ["ingested", "clean", "rejected", "event_counts", "daily_volume", "top_repositories", "top_actors"]
            snapshot = {name: sorted(self.spark.read.parquet(str(output / name)).toJSON().collect()) for name in names}
            run_stage(self.spark, "pipeline", DAY, raw, output)
            for name in names:
                self.assertEqual(sorted(self.spark.read.parquet(str(output / name)).toJSON().collect()), snapshot[name], name)
            for stage in ("ingest", "transform", "aggregate"):
                run_stage(self.spark, stage, DAY, raw, separate)
            # source_file is identical because both executions use the same raw fixture.
            for name in names:
                self.assertEqual(sorted(read_date(self.spark, output / name, DAY).toJSON().collect()),
                                 sorted(read_date(self.spark, separate / name, DAY).toJSON().collect()), name)
            # If a rerun has no accepted events, stale rows must disappear.
            invalid = read_date(self.spark, output / "ingested", DAY).filter(F.col("event_id") == "10")
            invalid = self.spark.createDataFrame(invalid.collect(), invalid.schema)
            write_date(invalid, output / "ingested", DAY)
            run_stage(self.spark, "transform", DAY, raw, output)
            run_stage(self.spark, "aggregate", DAY, raw, output)
            for name in ("clean", "event_counts", "daily_volume", "top_repositories", "top_actors"):
                self.assertEqual(read_date(self.spark, output / name, DAY).count(), 0, name)
                self.assertEqual(sorted(read_date(self.spark, output / name, date(2025, 6, 2)).toJSON().collect()),
                                 sorted(r for r in snapshot[name] if '2025-06-02' in r), name)

    def test_missing_input_does_not_create_output(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(FileNotFoundError):
                run_stage(self.spark, "pipeline", DAY, directory, Path(directory) / "output")
            self.assertFalse((Path(directory) / "output").exists())
