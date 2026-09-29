"""Dependency-free command-line configuration."""

import argparse
from dataclasses import dataclass
from datetime import date
from typing import Optional, Sequence


@dataclass(frozen=True)
class JobConfig:
    event_date: date
    output: str


def parse_config(argv: Optional[Sequence[str]] = None) -> JobConfig:
    parser = argparse.ArgumentParser(description="Run the synthetic Spark smoke job.")
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    if not args.output.strip():
        parser.error("--output must not be empty")
    return JobConfig(args.date, args.output)
