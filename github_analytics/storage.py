"""Safe date-partitioned Parquet checkpoints in the local Docker runtime."""

from functools import reduce
from pyspark.sql import functions as F, types as T
from github_analytics.publication import (PublicationError, date_directory,
                                         day_string, publish, published_dates,
                                         published_partition)


def date_path(root, event_date):
    return str(date_directory(root, event_date))


def write_date(frame, root, event_date, fault=None):
    """Validate, stage and publish one date, including schema-carrying empties."""
    day = day_string(event_date)
    if "event_date" not in frame.columns or frame.schema["event_date"].dataType != T.DateType():
        raise ValueError("Output event_date must have DateType")
    if frame.filter(F.col("event_date").isNull() |
                    (F.col("event_date") != F.lit(day).cast("date"))).limit(1).count():
        raise ValueError("Output contains rows outside the requested date")
    physical = frame.drop("event_date")
    expected = {field.name: field.dataType.simpleString() for field in physical.schema}

    def writer(stage):
        physical.write.mode("errorifexists").parquet(str(stage))
        saved = frame.sparkSession.read.parquet(str(stage))
        actual = {field.name: field.dataType.simpleString() for field in saved.schema}
        if actual != expected:
            raise PublicationError("Staged physical schema differs from input")
        # Distributed scalar reduction, not driver-side data collection.
        return saved.count(), actual

    return publish(root, day, writer, fault)


def read_date(spark, root, event_date):
    """Resolve a complete immutable generation; safe for subsequent lazy actions."""
    files, manifest = published_partition(root, event_date)
    frame = (spark.read.option("recursiveFileLookup", "true")
             .option("ignoreMissingFiles", "false")
             .option("ignoreCorruptFiles", "false").parquet(*files))
    if {field.name: field.dataType.simpleString() for field in frame.schema} != manifest["schema"]:
        raise PublicationError("Published physical schema differs from manifest")
    return frame.withColumn("event_date", F.lit(day_string(event_date)).cast("date"))


def read_dates(spark, root):
    """Read only explicitly published dates, never dataset-root/staging globs."""
    frames = [read_date(spark, root, day) for day in published_dates(root)]
    if not frames:
        raise PublicationError("No published dates")
    return reduce(lambda left, right: left.unionByName(right), frames)


def write_partitioned(frame, output):
    """Compatibility helper using the same per-date publication protocol.

    Iterates distinct partition dates, never event rows. No cross-date
    transaction is implied. Empty input is a no-op; write_date replaces a
    known date when its rerun is empty.
    """
    if "event_date" not in frame.columns or frame.schema["event_date"].dataType != T.DateType():
        raise ValueError("Output event_date must have DateType")
    if frame.filter(F.col("event_date").isNull()).limit(1).count():
        raise ValueError("Output contains a null event_date")
    for row in frame.select("event_date").distinct().orderBy("event_date").toLocalIterator():
        write_date(frame.filter(F.col("event_date") == F.lit(row.event_date)), output, row.event_date)
