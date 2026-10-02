"""Comparable repeated native/legacy dedup benchmark on one distributed input.

Input and materialized benchmark results are isolated, never public datasets.
Both algorithms consume the same Parquet and write the same complete schema.
No driver-wide event collection; full multisets are compared with exceptAll.
"""

import argparse
import json
from pathlib import Path
import time

from pyspark.sql import functions as F
from github_analytics.session import create_session
from github_analytics.transform import deduplicate_valid
from tests.test_hash_dedup import legacy_dedup


def synthetic(spark, rows, width):
    source = spark.range(rows, numPartitions=8)
    family = F.when((F.col("id") % 100) < 3, F.concat(F.lit("duplicate-"),
                    F.floor(F.col("id") / 100).cast("string"))).otherwise(F.col("id").cast("string"))
    source = source.withColumn("event_id", family)
    return source.select(
        "event_id", F.lit("IssuesEvent").alias("event_type"),
        F.lit("2025-06-01T00:00:00Z").alias("created_at"),
        F.concat(F.lit("acme/repository-"), (F.crc32("event_id") % 16).cast("string")).alias("repo_name"),
        F.when((F.col("id") % 100 == 2) & (F.floor(F.col("id") / 100) % 3 == 0), "bob")
         .otherwise("alice").alias("actor_login"),
        F.lit(None).cast("string").alias("org_login"),
        F.repeat(F.sha2("event_id", 256), width).alias("payload_action"),
        F.lit(None).cast("long").alias("pr_number"), F.lit(1).cast("long").alias("issue_number"),
        F.array(F.lit("bug"), F.lit("help")).alias("issue_labels"),
        F.lit(None).cast("string").alias("corrupt_record"),
        F.when(F.col("id") % 2 == 0, "file:///synthetic/first-hour.json.gz")
         .otherwise("file:///synthetic/second-hour.json.gz").alias("source_file"),
        F.lit("2025-06-01").cast("date").alias("event_date"),
        F.lit("2025-06-01T00:00:00Z").cast("timestamp").alias("event_timestamp"),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--rows", type=int, default=150000)
    parser.add_argument("--string-blocks", type=int, default=32)
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()
    if args.rows < 1 or args.string_blocks < 1 or args.repetitions < 2:
        parser.error("Positive rows/string blocks and at least two repetitions required")
    root = Path(args.output)
    root.mkdir(parents=True, exist_ok=False)
    spark = create_session("business-dedup-benchmark")
    spark.sparkContext.setLogLevel("WARN")
    results = []
    try:
        synthetic(spark, args.rows, args.string_blocks).write.mode("errorifexists").parquet(str(root / "input"))
        source = spark.read.parquet(str(root / "input"))
        for partitions in (2, 8):
            spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
            # Alternate order to reduce a consistent warm-JVM/filesystem bias.
            for iteration in range(args.repetitions + 1):
                methods = (("legacy", legacy_dedup), ("native", deduplicate_valid))
                if iteration % 2:
                    methods = tuple(reversed(methods))
                for name, operation in methods:
                    label = f"{name}-shuffle-{partitions}-run-{iteration}"
                    spark.sparkContext.setJobGroup(label, label)
                    frame = operation(source)
                    destination = root / label
                    before = time.perf_counter()
                    frame.write.mode("errorifexists").parquet(str(destination))
                    elapsed = time.perf_counter() - before
                    # The event log contains final AQE execution plans for the
                    # write query; retain this DataFrame plan separately as well.
                    (root / (label + "-plan.txt")).write_text(
                        frame._jdf.queryExecution().executedPlan().toString(), encoding="utf-8")
                    spark.sparkContext.setJobGroup(label + "-counts", "post-benchmark output validation")
                    saved = spark.read.parquet(str(destination))
                    counts = {r.rejection_reason or "accepted": r["count"]
                              for r in saved.groupBy("rejection_reason").count().collect()}
                    record = {"label": label, "method": name, "shuffle_partitions": partitions,
                              "warmup": iteration == 0, "iteration": iteration,
                              "wall_seconds": elapsed, "counts": counts,
                              "files": len(list(destination.glob("*.parquet"))),
                              "parquet_bytes": sum(p.stat().st_size for p in destination.glob("*.parquet"))}
                    results.append(record)
                    print("Benchmark " + json.dumps(record, sort_keys=True), flush=True)
                spark.sparkContext.setJobGroup(f"equality-shuffle-{partitions}-run-{iteration}", "distributed exact multiset")
                a = spark.read.parquet(str(root / f"legacy-shuffle-{partitions}-run-{iteration}"))
                b = spark.read.parquet(str(root / f"native-shuffle-{partitions}-run-{iteration}"))
                if a.schema != b.schema or a.exceptAll(b).limit(1).count() or b.exceptAll(a).limit(1).count():
                    raise AssertionError("Legacy/native exact schema or full-row multiset differs")
        two = spark.read.parquet(str(root / "native-shuffle-2-run-1"))
        eight = spark.read.parquet(str(root / "native-shuffle-8-run-1"))
        if two.schema != eight.schema or two.exceptAll(eight).limit(1).count() or eight.exceptAll(two).limit(1).count():
            raise AssertionError("Shuffle 2/8 exact outputs differ")
        report = {"rows": args.rows, "string_blocks": args.string_blocks,
                  "runtime": {"spark": spark.version, "master": spark.sparkContext.master,
                              "aqe": spark.conf.get("spark.sql.adaptive.enabled")},
                  "exact_schema_and_full_row_equality": "PASS", "results": results}
        (root / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
