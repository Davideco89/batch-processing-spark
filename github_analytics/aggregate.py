"""Daily activity metrics with deterministic top-N ranking."""

from pyspark.sql import Window, functions as F


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
