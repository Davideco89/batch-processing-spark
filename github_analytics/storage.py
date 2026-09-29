"""Date-partitioned Parquet output for local batch jobs."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def write_partitioned(frame: DataFrame, output: str) -> None:
    """Replace only dates present in this frame; empty input is a no-op.

    Callers must supply validated, non-null event_date values. Concurrent writers
    and removal of an existing date when a rerun has no rows are out of scope.
    """
    (
        frame.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("event_date")
        .parquet(output)
    )


def date_path(root, event_date):
    return f"{str(root).rstrip('/')}/event_date={event_date}"


def write_date(frame, root, event_date):
    """Replace one date, including empty reruns; other dates remain intact.

    Local single-writer output only. Replacement is not transactional across
    datasets; rerun the chain after an interrupted write.
    """
    if frame.filter(F.col("event_date").isNull() |
                    (F.col("event_date") != F.lit(event_date))).limit(1).count():
        raise ValueError("Output contains rows outside the requested date")
    frame.drop("event_date").write.mode("overwrite").parquet(date_path(root, event_date))


def read_date(spark, root, event_date):
    return spark.read.parquet(date_path(root, event_date)).withColumn("event_date", F.lit(event_date))
