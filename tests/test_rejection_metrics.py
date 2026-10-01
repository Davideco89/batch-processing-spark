"""Native rejection aggregation contracts without external input or services."""

from datetime import date
import unittest

from pyspark.sql import SparkSession, types as T

from github_analytics.aggregate import rejection_counts
from github_analytics.session import create_session

DAY = date(2025, 6, 1)
SCHEMA = T.StructType([T.StructField("event_date", T.DateType()),
                       T.StructField("rejection_reason", T.StringType())])


class RejectionAggregationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        SparkSession.builder.master("local[2]").getOrCreate()
        cls.spark = create_session("rejection-aggregation-tests")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_typed_empty_and_exact_reason_counts(self):
        for rows, expected in [([], {}), ([(DAY, "outside_date"), (DAY, "outside_date"),
                                          (DAY, "duplicate_event_id")],
                                         {"outside_date": 2, "duplicate_event_id": 1})]:
            with self.subTest(rows=rows):
                metric = rejection_counts(self.spark.createDataFrame(rows, SCHEMA), DAY)
                self.assertEqual({f.name: f.dataType.simpleString() for f in metric.schema},
                                 {"event_date": "date", "rejection_reason": "string", "event_count": "bigint"})
                self.assertEqual({r.rejection_reason: r.event_count for r in metric.collect()}, expected)

    def test_invalid_checkpoint_is_not_silently_relabelled(self):
        for row in [(date(2025, 6, 2), "outside_date"), (None, "outside_date"),
                    (DAY, None), (DAY, ""), (DAY, "unknown"), (DAY, "outside_date ")]:
            with self.subTest(row=row), self.assertRaises(ValueError):
                rejection_counts(self.spark.createDataFrame([row], SCHEMA), DAY)
        with self.assertRaises(ValueError):
            rejection_counts(self.spark.createDataFrame([("2025-06-01", "outside_date")],
                                                        "event_date string, rejection_reason string"), DAY)
        with self.assertRaises(ValueError):
            rejection_counts(self.spark.createDataFrame([(DAY, 1)],
                                                        "event_date date, rejection_reason long"), DAY)
