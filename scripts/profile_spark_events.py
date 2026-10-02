"""Summarize observed Spark event-log task, CPU, shuffle and final AQE evidence."""

import argparse
from collections import defaultdict
import json
from pathlib import Path


def summarize(paths):
    metrics = defaultdict(lambda: {"tasks": 0, "executor_cpu_ns": 0, "executor_run_ms": 0,
                                  "shuffle_read_bytes": 0, "shuffle_write_bytes": 0,
                                  "max_task_shuffle_read_bytes": 0, "max_task_shuffle_write_bytes": 0,
                                  "max_task_duration_ms": 0, "max_task_cpu_ns": 0})
    plans = []
    for path in paths:
        # Stage IDs restart at zero in every application. The pinned runtime's
        # uncompressed non-rolling event log is one file per application.
        stages = defaultdict(set)
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                event = json.loads(line)
                kind = event.get("Event", "")
                if kind == "SparkListenerJobStart":
                    group = event.get("Properties", {}).get("spark.jobGroup.id", "ungrouped")
                    for stage in event["Stage IDs"]:
                        stages[stage].add(group)
                elif kind == "SparkListenerTaskEnd" and "Task Metrics" in event:
                    task = event["Task Metrics"]
                    info = event["Task Info"]
                    read = task.get("Shuffle Read Metrics", {})
                    read_bytes = read.get("Remote Bytes Read", 0) + read.get("Local Bytes Read", 0)
                    write_bytes = task.get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0)
                    for group in stages[event["Stage ID"]] or {"ungrouped"}:
                        report = metrics[group]
                        report["tasks"] += 1
                        report["executor_cpu_ns"] += task.get("Executor CPU Time", 0)
                        report["executor_run_ms"] += task.get("Executor Run Time", 0)
                        report["shuffle_read_bytes"] += read_bytes
                        report["shuffle_write_bytes"] += write_bytes
                        report["max_task_shuffle_read_bytes"] = max(report["max_task_shuffle_read_bytes"], read_bytes)
                        report["max_task_shuffle_write_bytes"] = max(report["max_task_shuffle_write_bytes"], write_bytes)
                        report["max_task_cpu_ns"] = max(report["max_task_cpu_ns"], task.get("Executor CPU Time", 0))
                        report["max_task_duration_ms"] = max(report["max_task_duration_ms"], info["Finish Time"] - info["Launch Time"])
                elif kind.endswith("SparkListenerSQLAdaptiveExecutionUpdate"):
                    plans.append({"execution_id": event["executionId"],
                                  "plan": event.get("physicalPlanDescription", "")})
    return {"job_groups": dict(metrics), "adaptive_plan_updates": plans,
            "final_aqe_plan_count": sum("isFinalPlan=true" in p["plan"] for p in plans),
            "note": "CPU is JVM executor CPU time; wall time is separately recorded. "
                    "Task shuffle bytes are compressed transport bytes, not Parquet footer estimates. "
                    "Shared/reused stages may be attributed to multiple job groups."}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-dir", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    paths = [p for p in Path(args.event_dir).rglob("*") if p.is_file() and not p.name.startswith(".")]
    report = summarize(paths)
    Path(args.output).write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"event_logs": len(paths), "groups": len(report["job_groups"]),
                      "final_aqe_plans": report["final_aqe_plan_count"]}))
