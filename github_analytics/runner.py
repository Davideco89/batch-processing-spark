"""Shared date-parametrized CLI for individual stages and the full chain."""

from github_analytics.paths import RAW_ROOT, PARQUET_ROOT

import argparse
from datetime import date
from pathlib import Path
from pyspark import StorageLevel

from github_analytics.aggregate import aggregate
from github_analytics.ingest import ingest
from github_analytics.session import create_session
from github_analytics.storage import read_date, write_date
from github_analytics.transform import transform


def run_stage(spark, stage, event_date, raw_root, output_root, top_n=10):
    root = Path(output_root)
    if stage in ("ingest", "pipeline"):
        frame = ingest(spark, raw_root, event_date).cache()
        try:
            write_date(frame, root / "ingested", event_date)
            print(f"Ingested {event_date}: {frame.count()} records", flush=True)
        finally:
            frame.unpersist()
    if stage in ("transform", "pipeline"):
        clean, rejected = transform(read_date(spark, root / "ingested", event_date), event_date)
        clean, rejected = clean.persist(StorageLevel.DISK_ONLY), rejected.persist(StorageLevel.DISK_ONLY)
        try:
            write_date(clean, root / "clean", event_date)
            write_date(rejected, root / "rejected", event_date)
            print(f"Transformed {event_date}: accepted={clean.count()}, rejected={rejected.count()}", flush=True)
        finally:
            clean.unpersist()
            rejected.unpersist()
    if stage in ("aggregate", "pipeline"):
        frame = read_date(spark, root / "clean", event_date).cache()
        try:
            for name, metric in aggregate(frame, top_n).items():
                write_date(metric, root / name, event_date)
            print(f"Aggregated {event_date}: {frame.count()} events", flush=True)
        finally:
            frame.unpersist()


def main(stage):
    parser = argparse.ArgumentParser(description=f"Run GitHub events {stage}.")
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--raw-root", default=RAW_ROOT)
    parser.add_argument("--output-root", default=PARQUET_ROOT)
    parser.add_argument("--top-n", type=int, default=10)
    args = parser.parse_args()
    if args.top_n < 1 or not args.raw_root.strip() or not args.output_root.strip():
        parser.error("Paths must not be empty and --top-n must be positive")
    if Path(args.raw_root).resolve() == Path(args.output_root).resolve():
        parser.error("Input and output directories must differ")
    spark = create_session(f"github-events-{stage}")
    spark.sparkContext.setLogLevel("WARN")
    print(f"Runtime Spark={spark.version}; timeZone={spark.conf.get('spark.sql.session.timeZone')}; "
          f"shuffle_partitions={spark.conf.get('spark.sql.shuffle.partitions')}", flush=True)
    try:
        run_stage(spark, stage, args.date, args.raw_root, args.output_root, args.top_n)
    finally:
        spark.stop()
