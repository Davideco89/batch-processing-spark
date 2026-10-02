"""Dependency-free bounded daily execution and strict parameter validation."""

from datetime import date, timedelta
import re

STAGES = ("ingest", "transform", "aggregate")


def strict_date(value):
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("Expected an ISO date YYYY-MM-DD")
    return date.fromisoformat(value)


def date_bounds(single=None, start=None, end=None, resume=None):
    if single is not None:
        if start is not None or end is not None:
            raise ValueError("--date cannot be combined with --start-date/--end-date")
        start = end = single
    elif start is None or end is None:
        raise ValueError("Provide --date or both --start-date and --end-date")
    if start > end:
        raise ValueError("--start-date must not follow --end-date")
    if resume is not None:
        if not start <= resume <= end:
            raise ValueError("--resume-date must be within the requested interval")
        start = resume
    return start, end


def days(start, end):
    """Yield dates without materializing a range or overflowing date.max."""
    if start > end:
        raise ValueError("Start date must not follow end date")
    current = start
    while True:
        yield current
        if current == end:
            return
        current += timedelta(days=1)


def run_batch(spark, stage, start, end, raw_root, output_root, top_n=10,
              from_stage="ingest", shuffle_partitions=None, target_bytes=128 * 1024 ** 2):
    from github_analytics.runner import run_stage

    for day in days(start, end):
        # A pipeline restart skips completed stages only on the first selected
        # date. Subsequent dates execute the complete chain. Individual jobs
        # intentionally run their chosen stage on every selected date.
        first_stage = from_stage if day == start else "ingest"
        print(f"Batch date={day}; stage={stage}; from_stage={first_stage}", flush=True)
        try:
            run_stage(spark, stage, day, raw_root, output_root, top_n,
                      from_stage=first_stage, shuffle_partitions=shuffle_partitions,
                      target_bytes=target_bytes)
        except Exception:
            resume = f"Resume the same {stage} job explicitly with --resume-date {day}"
            if stage == "pipeline":
                resume += " and --from-stage <ingest|transform|aggregate>"
            print(f"Batch failed date={day}; inspect the failing stage above. "
                  f"{resume}; no later dates were started.", flush=True)
            raise
