"""Isolated bind-mount experiment for actual writer interruption and recovery.

Run with spark-submit in Docker. The host kills the `write` container only
after observing a nonempty staging `_temporary` part file. `commit-crash`
kills its Python driver after pointer replacement. This harness touches only
its own marked synthetic experiment under /data/test/partition-publication.
"""

import argparse
from datetime import date
import json
import os
from pathlib import Path
import signal

from pyspark.sql import functions as F

from github_analytics.export import export_date
from github_analytics.publication import (PublicationError, published_partition,
                                         recover_partition, sha256)
from github_analytics.runner import run_stage
from github_analytics.session import create_session
from github_analytics.storage import read_date, read_dates, write_date
from scripts.profile_archive import DATASETS, profile
from scripts.validate_parquet import validate
from scripts.verify_exports import verify_exports
from scripts.verify_fixture import verify
from tests.fixture_data import write_fixture

DAY, OTHER = date(2025, 6, 1), date(2025, 6, 2)


def other_date_snapshot(root):
    paths = []
    for name in DATASETS:
        paths.extend((root / "parquet" / name / f"event_date={OTHER}").rglob("*"))
    paths.extend((root / "csv" / f"event_date={OTHER}").rglob("*"))
    return {str(path.relative_to(root)): sha256(path) for path in paths if path.is_file()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    parser.add_argument("--mode", required=True, choices=("setup", "write", "probe", "recover", "commit-crash", "verify"))
    args = parser.parse_args()
    root = Path(args.root).resolve()
    allowed = (Path("/data/test/partition-publication"), Path("/data/test/integrated-validation"))
    if not any(path.resolve() in root.parents for path in allowed):
        parser.error("Use a new isolated experiment directory under /data/test/partition-publication or /data/test/integrated-validation")
    marker = root / "_experiment.json"
    if args.mode == "setup":
        if root.exists() and any(root.iterdir()):
            parser.error("Setup requires an empty isolated directory")
        root.mkdir(parents=True, exist_ok=True)
        marker.write_text(json.dumps({"purpose": "synthetic publication interruption", "date": str(DAY)}))
    elif not marker.is_file() or json.loads(marker.read_text()).get("purpose") != "synthetic publication interruption":
        parser.error("Experiment marker missing or invalid")
    parquet, csv, database = root / "parquet", root / "csv", root / "metrics.duckdb"
    target = parquet / "daily_volume"
    spark = create_session("verify-partition-publication")
    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark={spark.version}; Hadoop={spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}; "
          f"master={spark.sparkContext.master}; timezone={spark.conf.get('spark.sql.session.timeZone')}", flush=True)
    try:
        if args.mode == "setup":
            write_fixture(root / "raw")
            for day in (DAY, OTHER):
                run_stage(spark, "pipeline", day, root / "raw", parquet)
                export_date(parquet, day, csv, database)
            report = {"other_date": other_date_snapshot(root), "exports": verify_exports(parquet, csv, database, DAY)}
            (root / "baseline.json").write_text(json.dumps(report, indent=2))
            print("BASELINE_COMPLETE", flush=True)
        elif args.mode == "write":
            # Native generation intentionally exceeds a short experiment. No
            # download, UDF, repartition, collect or whole-data driver memory.
            frame = spark.range(0, 2_000_000_000, numPartitions=2).select(
                (F.col("id") + 1).alias("event_count"), F.lit(DAY).alias("event_date"))
            print("WRITER_STARTING", flush=True)
            write_date(frame, target, DAY)
            raise AssertionError("Host was expected to interrupt the active writer")
        elif args.mode == "commit-crash":
            frame = spark.range(1).select(F.lit(8).cast("long").alias("event_count"), F.lit(DAY).alias("event_date"))
            def hook(phase):
                if phase == "pointer_replaced":
                    print("KILL_AFTER_POINTER_REPLACE", flush=True)
                    os.kill(os.getpid(), signal.SIGKILL)
            write_date(frame, target, DAY, hook)
            raise AssertionError("Driver was expected to be killed")
        elif args.mode == "probe":
            consumers = {
                "Spark date reader": lambda: read_date(spark, target, DAY).count(),
                "Spark dataset reader": lambda: read_dates(spark, target).count(),
                "DuckDB export": lambda: export_date(parquet, DAY, csv, database),
                "export verifier": lambda: verify_exports(parquet, csv, database, DAY),
                "exact Parquet verifier": lambda: validate(spark, parquet, DAY),
                "independent fixture oracle": lambda: profile(spark, root / "raw", parquet, DAY, 10),
                "golden fixture verifier": lambda: verify(spark, parquet),
            }
            result = {}
            for name, consumer in consumers.items():
                try:
                    consumer()
                except PublicationError as error:
                    result[name] = "REJECTED: " + str(error)
                else:
                    raise AssertionError(name + " accepted an interrupted partition")
            assert other_date_snapshot(root) == json.loads((root / "baseline.json").read_text())["other_date"]
            print(json.dumps(result, indent=2), flush=True)
        else:
            if args.mode == "recover":
                print(recover_partition(target, DAY), flush=True)
            baseline = json.loads((root / "baseline.json").read_text())
            assert other_date_snapshot(root) == baseline["other_date"], "Other-date physical bytes changed"
            run_stage(spark, "aggregate", DAY, "/not-used-no-raw", parquet)
            export_date(parquet, DAY, csv, database)
            actual = verify_exports(parquet, csv, database, DAY)
            assert actual == baseline["exports"], "SQL or CSV contents differ after recovery/aggregate restart"
            verify(spark, parquet)
            print(json.dumps({"manifest": published_partition(target, DAY)[1],
                              "other_date_files": len(baseline["other_date"]),
                              "verification": "PASS: recovery, aggregate-only restart, other-date physical bytes, Parquet/SQL/CSV"}, indent=2), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
