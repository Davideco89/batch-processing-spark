"""Inspect an active publication or explicitly recover a stopped date writer."""

import argparse
from datetime import date
import json

from github_analytics.publication import (current_pointer, date_directory,
                                         published_partition, recover_partition)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-root", required=True)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--action", choices=("inspect", "rollback", "commit"), default="inspect")
    args = parser.parse_args()
    if args.action != "inspect":
        print(recover_partition(args.dataset_root, args.date, args.action), flush=True)
        if current_pointer(date_directory(args.dataset_root, args.date)) is None:
            print("No published partition; rerun the requested date to publish it.", flush=True)
            return
    files, manifest = published_partition(args.dataset_root, args.date)
    print(json.dumps({"files": files, "manifest": manifest}, indent=2), flush=True)


if __name__ == "__main__":
    main()
