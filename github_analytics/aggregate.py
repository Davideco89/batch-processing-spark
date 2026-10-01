"""Daily activity metrics with deterministic top-N ranking."""

from pyspark.sql import Window, functions as F, types as T
from github_analytics.publication import day_string
from github_analytics.quality import REJECTION_REASONS


def aggregate(frame, top_n=10):
    if top_n < 1:
        raise ValueError("top_n must be positive")
    def counts(*dimensions):
        return frame.groupBy("event_date", *dimensions).agg(F.count("*").alias("event_count"))
    def ranking(entity):
        window = Window.partitionBy("event_date").orderBy(F.desc("event_count"), entity)
        return counts(entity).withColumn("rank", F.row_number().over(window)).filter(F.col("rank") <= top_n)
    return {
        "event_counts": counts("event_type", "event_hour", "is_bot"),
        "daily_volume": counts(),
        "top_repositories": ranking("repo_name"),
        "top_actors": ranking("actor_login"),
    }


def rejection_counts(rejected, event_date):
    """Count rejects by processing date/reason, never by rejected timestamp.

    A bounded existence check protects this reusable function from malformed
    checkpoints. Empty input keeps the same typed schema and yields no rows.
    The one native grouping shuffle is necessary for exact reason counts.
    """
    day = day_string(event_date)
    required = {"event_date": T.DateType(), "rejection_reason": T.StringType()}
    if any(name not in rejected.columns or rejected.schema[name].dataType != kind
           for name, kind in required.items()):
        raise ValueError("Rejected checkpoint requires DateType event_date and string rejection_reason")
    invalid = (F.col("event_date").isNull() | (F.col("event_date") != F.lit(day).cast("date"))
               | F.col("rejection_reason").isNull()
               | ~F.col("rejection_reason").isin(*REJECTION_REASONS))
    if rejected.filter(invalid).limit(1).count():
        raise ValueError("Rejected checkpoint contains an invalid batch date or rejection reason")
    return (rejected.select(F.lit(day).cast("date").alias("event_date"), "rejection_reason")
            .groupBy("event_date", "rejection_reason").agg(F.count("*").alias("event_count")))
