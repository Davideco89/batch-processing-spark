"""Run essential CLIs on self-generated data in a fresh temporary directory."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile

from tests.fixture_data import write_fixture


def main():
    with tempfile.TemporaryDirectory(prefix="github-events-fresh-") as directory:
        root = Path(directory)
        raw, parquet, csv, database = root / "raw", root / "parquet", root / "csv", root / "analytics.duckdb"
        write_fixture(raw)
        commands = [
            ["/opt/spark/bin/spark-submit", "--master", "local[2]", "--conf", "spark.sql.shuffle.partitions=8", "/app/jobs/pipeline.py", "--date", "2025-06-01", "--raw-root", str(raw), "--output-root", str(parquet)],
            [sys.executable, "/app/jobs/export.py", "--date", "2025-06-01", "--parquet-root", str(parquet), "--csv-root", str(csv), "--database", str(database)],
            [sys.executable, "/app/scripts/verify_exports.py", "--date", "2025-06-01", "--parquet-root", str(parquet), "--csv-root", str(csv), "--database", str(database), "--report", str(root / "exports.json")],
        ]
        for command in commands:
            print("Running " + json.dumps(command), flush=True)
            completed = subprocess.run(command, check=True, capture_output=True, text=True)
            print(completed.stdout, flush=True)
            print(completed.stderr, flush=True)
            if command[0] == "/opt/spark/bin/spark-submit":
                assert "shuffle_partitions=8" in completed.stdout, "Launcher shuffle configuration was overridden"
        report = json.loads((root / "exports.json").read_text())
        assert report["tables"]["daily_volume"]["rows"] == 1
        assert report["tables"]["rejection_counts"]["rows"] == 5
        print("PASS: fresh generated input, pipeline/export/verifier CLIs, temporary outputs, no inherited data or external services", flush=True)


if __name__ == "__main__":
    main()
