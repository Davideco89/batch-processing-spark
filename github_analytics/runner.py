"""Shared date-parametrized CLI for individual stages and the full chain."""

from github_analytics.paths import RAW_ROOT, PARQUET_ROOT

import argparse
from pathlib import Path
from pyspark import StorageLevel

from github_analytics.aggregate import aggregate, rejection_counts
from github_analytics.ingest import ingest
from github_analytics.session import create_session
from github_analytics.storage import read_date, write_date
from github_analytics.transform import transform
from github_analytics.batch import STAGES, date_bounds, run_batch, strict_date
from github_analytics.scaling import configure_stage, DEFAULT_TARGET_BYTES


def run_stage(spark, stage, event_date, raw_root, output_root, top_n=10,
              from_stage="ingest", shuffle_partitions=None, target_bytes=DEFAULT_TARGET_BYTES):
    if stage not in (*STAGES, "pipeline") or from_stage not in STAGES:
        raise ValueError("Unknown stage")
    if top_n < 1:
        raise ValueError("top_n must be positive")
    if stage == "pipeline":
        for name in STAGES[STAGES.index(from_stage):]:
            run_stage(spark, name, event_date, raw_root, output_root, top_n,
                      shuffle_partitions=shuffle_partitions, target_bytes=target_bytes)
        return
    root = Path(output_root)
    configure_stage(spark, stage, event_date, raw_root, output_root,
                    shuffle_partitions, target_bytes)
    if stage == "ingest":
        frame = ingest(spark, raw_root, event_date).cache()
        try:
            write_date(frame, root / "ingested", event_date)
            print(f"Ingested {event_date}: {frame.count()} records", flush=True)
        finally:
            frame.unpersist()
    if stage == "transform":
        clean, rejected = transform(read_date(spark, root / "ingested", event_date), event_date)
        clean, rejected = clean.persist(StorageLevel.DISK_ONLY), rejected.persist(StorageLevel.DISK_ONLY)
        try:
            write_date(clean, root / "clean", event_date)
            write_date(rejected, root / "rejected", event_date)
            print(f"Transformed {event_date}: accepted={clean.count()}, rejected={rejected.count()}", flush=True)
        finally:
            clean.unpersist()
            rejected.unpersist()
    if stage == "aggregate":
        # Resolve both complete restart checkpoints before publishing metrics.
        rejected = read_date(spark, root / "rejected", event_date)
        reasons = rejection_counts(rejected, event_date)
        frame = read_date(spark, root / "clean", event_date).cache()
        try:
            for name, metric in aggregate(frame, top_n).items():
                write_date(metric, root / name, event_date)
            write_date(reasons, root / "rejection_counts", event_date)
            print(f"Aggregated {event_date}: {frame.count()} events", flush=True)
        finally:
            frame.unpersist()


def main(stage, argv=None):
    parser = argparse.ArgumentParser(description=f"Run GitHub events {stage}.")
    parser.add_argument("--date", type=strict_date)
    parser.add_argument("--start-date", type=strict_date)
    parser.add_argument("--end-date", type=strict_date)
    parser.add_argument("--resume-date", type=strict_date)
    parser.add_argument("--from-stage", choices=STAGES, default="ingest",
                        help="Pipeline restart stage for first selected date; later dates run the full chain")
    parser.add_argument("--shuffle-partitions", type=int)
    parser.add_argument("--shuffle-target-bytes", type=int, default=DEFAULT_TARGET_BYTES)
    parser.add_argument("--raw-root", default=RAW_ROOT)
    parser.add_argument("--output-root", default=PARQUET_ROOT)
    parser.add_argument("--top-n", type=int, default=10)
    args = parser.parse_args(argv)
    try:
        start, end = date_bounds(args.date, args.start_date, args.end_date, args.resume_date)
    except ValueError as error:
        parser.error(str(error))
    if stage != "pipeline" and args.from_stage != "ingest":
        parser.error("--from-stage applies only to pipeline; individual jobs already select a stage")
    if args.shuffle_target_bytes < 1 or (args.shuffle_partitions is not None and args.shuffle_partitions < 1):
        parser.error("Shuffle partitions and target bytes must be positive")
    if args.top_n < 1 or not args.raw_root.strip() or not args.output_root.strip():
        parser.error("Paths must not be empty and --top-n must be positive")
    if Path(args.raw_root).resolve() == Path(args.output_root).resolve():
        parser.error("Input and output directories must differ")
    spark = create_session(f"github-events-{stage}")
    spark.sparkContext.setLogLevel("WARN")
    print(f"Runtime Spark={spark.version}; timeZone={spark.conf.get('spark.sql.session.timeZone')}; "
          f"shuffle_partitions={spark.conf.get('spark.sql.shuffle.partitions')}", flush=True)
    try:
        run_batch(spark, stage, start, end, args.raw_root, args.output_root, args.top_n,
                  args.from_stage, args.shuffle_partitions, args.shuffle_target_bytes)
    finally:
        spark.stop()
