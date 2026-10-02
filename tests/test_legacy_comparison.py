"""Legacy reconciliation must not certify input or hide provenance changes."""

from datetime import date
from pathlib import Path
import shutil
import tempfile
import unittest

from pyspark.sql import SparkSession, functions as F

from github_analytics.publication import published_partition
from github_analytics.runner import run_stage
from github_analytics.session import create_session
from github_analytics.storage import read_date, write_date
from scripts.compare_legacy import compare
from scripts.profile_archive import DATASETS
from tests.fixture_data import write_fixture


class LegacyComparisonTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        SparkSession.builder.master("local[2]").getOrCreate()
        cls.spark = create_session("legacy-comparison-tests")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_exact_seven_dataset_comparison_refuses_provenance_change_and_nested_state(self):
        day = date(2025, 6, 1)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw, output, legacy = (root / name for name in ("raw", "published", "legacy"))
            write_fixture(raw)
            run_stage(self.spark, "pipeline", day, raw, output)
            for name in DATASETS:
                if name == "rejection_counts":
                    continue
                target = legacy / name / f"event_date={day}"
                target.mkdir(parents=True)
                for source in published_partition(output / name, day)[0]:
                    shutil.copyfile(source, target / Path(source).name)
                (target / "_SUCCESS").touch()
            result = compare(self.spark, legacy, output, day)
            self.assertEqual(len(result), 7)
            self.assertEqual(result["clean"]["rows"], 8)
            # Equal row counts cannot hide a changed source URI.
            frame = read_date(self.spark, output / "clean", day)
            write_date(frame.withColumn("source_file", F.lit("file:///unrelated/archive.json.gz")),
                       output / "clean", day)
            with self.assertRaisesRegex(AssertionError, "multiplicity"):
                compare(self.spark, legacy, output, day)
            (legacy / "ingested" / f"event_date={day}" / "_generations").mkdir()
            with self.assertRaisesRegex(ValueError, "direct-file"):
                compare(self.spark, legacy, output, day)
