"""Isolated daily-range, restart, publication and stage-volume evidence on /data.

Run this orchestration harness with the Docker python3 entrypoint. Its child
CLIs start their own spark-submit JVMs; PYSPARK_SUBMIT_ARGS configures the later
parent session before that JVM starts. The volume-only mode runs directly with
spark-submit and avoids repeating already verified CLI scenarios.
"""

import argparse
from datetime import date
import json
import os
from pathlib import Path
import subprocess
import sys

from github_analytics.batch import run_batch
from github_analytics.export import export_date
from github_analytics.publication import PublicationError, published_partition, recover_partition, sha256
from github_analytics.runner import run_stage
from github_analytics.scaling import checkpoint_volume, configure_stage
from github_analytics.session import create_session
from github_analytics.storage import read_date, read_dates, write_date
from scripts.benchmark_dedup import synthetic
from scripts.profile_archive import profile
from scripts.validate_parquet import validate
from scripts.verify_exports import verify_exports
from scripts.verify_fixture import verify
from tests.fixture_data import event, write_archive, write_fixture

DATASETS = ("ingested", "clean", "rejected", "event_counts", "daily_volume",
            "top_repositories", "top_actors", "rejection_counts")
FIRST, SECOND, THIRD = (date(2025, 6, n) for n in (1, 2, 3))


def snapshot(root, selected_dates):
    return {str(p.relative_to(root)): sha256(p)
            for name in DATASETS for day in selected_dates
            for p in (root / name / f"event_date={day}").rglob("*") if p.is_file()}


def measure_volumes(spark, root):
    records = []
    for rows, width, label in ((2000, 2, "small"), (150000, 32, "wide")):
        destination = root / label
        frame = synthetic(spark, rows, width).drop("event_timestamp")
        write_date(frame, destination / "ingested", FIRST)
        spark.sparkContext.setJobGroup("volume-" + label + "-transform", "transform volume policy workload")
        settings = configure_stage(spark, "transform", FIRST, root / "absent-raw", destination)
        run_stage(spark, "transform", FIRST, root / "absent-raw", destination)
        spark.sparkContext.setJobGroup("volume-" + label + "-aggregate", "aggregate volume policy workload")
        run_stage(spark, "aggregate", FIRST, root / "absent-raw", destination)
        spark.sparkContext.setJobGroup("volume-" + label + "-validation", "post-stage validation and layout")
        record = {"label": label, "input_rows": rows, "policy": settings,
                  "layout": {name: checkpoint_volume(destination / name, FIRST) for name in DATASETS},
                  "counts": {name: read_date(spark, destination / name, FIRST).count() for name in DATASETS}}
        assert record["counts"]["ingested"] == record["counts"]["clean"] + record["counts"]["rejected"]
        if label == "wide":
            assert settings["effective_shuffle_partitions"] > 2, "Wide volume must exercise automatic growth"
        records.append(record)
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    parser.add_argument("--mode", choices=("full", "volume"), default="full")
    parser.add_argument("--skip-volume", action="store_true",
                        help="Reuse separately recorded volume benchmarks during integrated validation")
    args = parser.parse_args()
    root = Path(args.root).resolve()
    allowed = (Path("/data/test/batch-scaling"), Path("/data/test/integrated-validation"))
    if not any(path.resolve() in root.parents for path in allowed) or root.exists():
        parser.error("Use a new isolated directory below /data/test/batch-scaling or /data/test/integrated-validation")
    root.mkdir(parents=True)
    if args.mode == "volume":
        spark = create_session("batch-stage-volume-checks")
        spark.sparkContext.setLogLevel("WARN")
        try:
            records = measure_volumes(spark, root)
            (root / "report.json").write_text(json.dumps({"volumes": records}, indent=2), encoding="utf-8")
            print("PASS: per-stage volume, reconciled counts, layout and separate task job groups", flush=True)
        finally:
            spark.stop()
        return
    # Keep isolated Windows bind paths short: generation UUIDs and Hadoop CRC
    # sidecars add length beyond the visible dataset directory.
    raw, output = root / "raw", root / "pq"
    write_fixture(raw)
    # Missing middle date forces stop before the available third date.
    (raw / "2025-06-02-0.json.gz").unlink()
    write_archive(raw / "2025-06-03-0.json.gz", [event(30, "WatchEvent", "service[BOT]", "acme/third", "2025-06-03T00:00:00Z")])
    results = {}

    def cli(label, options, expected=0):
        command = [sys.executable, "/app/scripts/launch_job.py", "/app/jobs/pipeline.py",
                   "--raw-root", str(raw), "--output-root", str(output)] + options
        completed = subprocess.run(command, capture_output=True, text=True)
        (root / (label + ".stdout.log")).write_text(completed.stdout, encoding="utf-8")
        (root / (label + ".stderr.log")).write_text(completed.stderr, encoding="utf-8")
        print(json.dumps({"label": label, "command": command, "exit": completed.returncode}), flush=True)
        if completed.returncode != expected:
            raise AssertionError(f"{label}: expected exit {expected}, observed {completed.returncode}")
        return completed.stdout

    # Invalid arguments fail before creating a SparkSession or output.
    for label, options in (("invalid-date", ["--date", "20250601"]),
                           ("reversed-range", ["--start-date", "2025-06-03", "--end-date", "2025-06-01"]),
                           ("mixed-selection", ["--date", "2025-06-01", "--start-date", "2025-06-01", "--end-date", "2025-06-03"]),
                           ("invalid-resource", ["--date", "2025-06-01", "--driver-memory", "0g"])):
        stdout = cli(label, options, 2)
        assert "StageSettings" not in stdout
        assert not output.exists()
    cli("range-middle-failure", ["--start-date", "2025-06-01", "--end-date", "2025-06-03"], 1)
    assert (output / "clean" / f"event_date={FIRST}").exists()
    assert not (output / "ingested" / f"event_date={THIRD}").exists()
    write_archive(raw / "2025-06-02-0.json.gz", [event(20, "PushEvent", "robot", "acme/second", "2025-06-02T00:00:00Z")])
    stdout = cli("range-resume", ["--start-date", "2025-06-01", "--end-date", "2025-06-03",
                                   "--resume-date", "2025-06-02", "--master", "local[4]",
                                   "--driver-memory", "768m", "--executor-memory", "768m",
                                   "--conf", "spark.sql.shuffle.partitions=8", "--conf", "spark.sql.adaptive.enabled=false"])
    settings = [json.loads(line[len("StageSettings "):]) for line in stdout.splitlines() if line.startswith("StageSettings ")]
    assert len(settings) == 6
    assert all(s["master"] == "local[4]" and s["driver_heap_max_bytes"] == 768 * 1024 ** 2
               and s["effective_shuffle_partitions"] == 8 and s["aqe_enabled"] == "false" for s in settings)
    results["range_resume_settings"] = settings

    spark = create_session("batch-scaling-storage-checks")
    spark.sparkContext.setLogLevel("WARN")
    try:
        results["runtime"] = {"spark": spark.version, "hadoop": spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion(),
                              "python": sys.version, "java": spark._jvm.java.lang.System.getProperty("java.version"),
                              "uid": os.getuid(), "gid": os.getgid(), "master": spark.sparkContext.master}
        results["initial_counts"] = {str(day): {name: read_date(spark, output / name, day).count() for name in DATASETS}
                                     for day in (FIRST, SECOND, THIRD)}
        results["baseline_oracle"] = profile(spark, raw, output, FIRST, 10)
        export_date(output, FIRST, root / "csv", root / "analytics.duckdb")
        results["export"] = verify_exports(output, root / "csv", root / "analytics.duckdb", FIRST)
        other = snapshot(output, (SECOND, THIRD))
        reasons = read_date(spark, output / "rejection_counts", FIRST)
        def crash(phase):
            if phase == "stage_written":
                raise RuntimeError("Injected incomplete rejection-count publication")
        try:
            write_date(reasons, output / "rejection_counts", FIRST, crash)
            raise AssertionError("Expected publication failure")
        except RuntimeError as error:
            assert "Injected incomplete" in str(error)
        consumers = {
            "Spark date reader": lambda: read_date(spark, output / "rejection_counts", FIRST).count(),
            "Spark dataset reader": lambda: read_dates(spark, output / "rejection_counts").count(),
            "DuckDB export": lambda: export_date(output, FIRST, root / "csv", root / "analytics.duckdb"),
            "export verifier": lambda: verify_exports(output, root / "csv", root / "analytics.duckdb", FIRST),
            "exact Parquet verifier": lambda: validate(spark, output, FIRST),
            "independent raw oracle": lambda: profile(spark, raw, output, FIRST, 10),
            "golden fixture verifier": lambda: verify(spark, output),
        }
        results["partial_output_consumers"] = {}
        for name, consumer in consumers.items():
            try:
                consumer()
                raise AssertionError(f"{name} accepted incomplete publication")
            except PublicationError as error:
                results["partial_output_consumers"][name] = str(error)
        results["recovery"] = recover_partition(output / "rejection_counts", FIRST)
        # Restart the first date's aggregate without raw; third date is still a
        # complete pipeline in a separate destination, proving later-day rules.
        restarted = root / "restart"
        run_stage(spark, "pipeline", SECOND, raw, restarted)
        saved_raw = raw / "2025-06-02-0.json.gz"
        saved_raw.rename(raw / "saved-second-hour.gz")
        run_batch(spark, "pipeline", SECOND, THIRD, raw, restarted, from_stage="aggregate")
        for day in (SECOND, THIRD):
            for name in DATASETS:
                a, b = read_date(spark, output / name, day), read_date(spark, restarted / name, day)
                assert a.schema == b.schema and not a.exceptAll(b).limit(1).count() and not b.exceptAll(a).limit(1).count()
        results["restart_aggregate_without_middle_raw_and_later_full_chain"] = "PASS"
        run_stage(spark, "aggregate", FIRST, root / "absent-raw", output)
        # Empty rerun removes every former row and reason without touching dates.
        write_archive(raw / "2025-06-01-0.json.gz", [])
        (raw / "2025-06-01-1.json.gz").unlink()
        run_stage(spark, "pipeline", FIRST, raw, output)
        results["empty_counts"] = {name: read_date(spark, output / name, FIRST).count() for name in DATASETS}
        assert set(results["empty_counts"].values()) == {0}
        assert snapshot(output, (SECOND, THIRD)) == other
        results["other_date_files_unchanged"] = len(other)
        # Different real serialized inputs: wide business text deliberately
        # exercises policy growth. These are synthetic CPU/volume workloads.
        results["volumes"] = [] if args.skip_volume else measure_volumes(spark, root)
        results["volume_checks_skipped"] = args.skip_volume
        (root / "report.json").write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
        print("PASS: bounded CLI range, fail/resume, effective startup settings, exact restart equivalence, "
              "seven partial consumer gates, recovery, empty rerun and other-date preservation"
              + ("; volume workloads explicitly skipped" if args.skip_volume else "; stage volume workloads verified"), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
