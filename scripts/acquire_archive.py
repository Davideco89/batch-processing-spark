"""Acquire declared complete GH Archive hours with guards and streaming hashes.

Completed files are reused; interrupted files restart from byte zero.
"""
import argparse
from datetime import date
import hashlib
import json
from pathlib import Path
import time
from urllib.request import Request, urlopen

from github_analytics.paths import RAW_ROOT, ACQUISITION_MANIFEST

HEADERS = {"User-Agent": "Mozilla/5.0 P005 GHArchive validation"}


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def validate_plan(plan, day, hours, max_file_bytes, max_total_bytes):
    sources = plan["sources"]
    assert plan["date"] == str(day) and [item["hour"] for item in sources] == hours, "Plan coverage differs"
    for source in sources:
        assert source["url"] == f"https://data.gharchive.org/{day}-{source['hour']}.json.gz", "Unexpected URL"
        if not 0 < source["head_bytes"] <= max_file_bytes:
            raise ValueError("Archive exceeds declared compressed size guard")
    if sum(source["head_bytes"] for source in sources) > max_total_bytes:
        raise ValueError("Sample exceeds total compressed size guard")


def acquire(source, root, max_bytes):
    target = Path(root) / source["url"].rsplit("/", 1)[1]
    target.parent.mkdir(parents=True, exist_ok=True)
    reused = target.exists()
    if not reused:
        temporary = target.with_suffix(target.suffix + ".partial")
        for attempt in range(3):
            try:
                total = 0
                with urlopen(Request(source["url"], headers=HEADERS), timeout=60) as response, temporary.open("wb") as stream:
                    while chunk := response.read(1024 * 1024):
                        total += len(chunk)
                        if total > max_bytes or total > source["head_bytes"]:
                            raise ValueError("Download exceeds declared size")
                        stream.write(chunk)
                if total != source["head_bytes"]:
                    raise ValueError("Downloaded bytes differ from HEAD")
                temporary.replace(target)
                break
            except Exception:
                temporary.unlink(missing_ok=True)
                if attempt == 2:
                    raise
                time.sleep(2)
    if target.stat().st_size != source["head_bytes"]:
        raise ValueError("Existing archive size differs from plan")
    return dict(source, compressed_bytes=target.stat().st_size, sha256=file_hash(target), reused=reused)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, default=date(2025, 6, 1))
    parser.add_argument("--hours", nargs="+", type=int, default=[0])
    parser.add_argument("--raw-root", default=RAW_ROOT)
    parser.add_argument("--manifest")
    parser.add_argument("--plan", help="Previously inspected HEAD plan")
    parser.add_argument("--head-only", action="store_true")
    parser.add_argument("--max-file-bytes", type=int, default=128 * 1024 * 1024)
    parser.add_argument("--max-total-bytes", type=int, default=3 * 1024 * 1024 * 1024)
    args = parser.parse_args()
    hours = sorted(set(args.hours))
    if hours != args.hours or any(hour < 0 or hour > 23 for hour in hours):
        parser.error("Hours must be unique, ordered and between 0 and 23")
    if args.plan:
        plan = json.loads(Path(args.plan).read_text())
    else:
        sources = []
        for hour in hours:
            url = f"https://data.gharchive.org/{args.date}-{hour}.json.gz"
            with urlopen(Request(url, headers=HEADERS, method="HEAD"), timeout=60) as response:
                sources.append({"hour": hour, "url": url, "head_bytes": int(response.headers["Content-Length"]),
                                "etag": response.headers.get("ETag"), "last_modified": response.headers.get("Last-Modified")})
        plan = {"date": str(args.date), "sources": sources}
    validate_plan(plan, args.date, hours, args.max_file_bytes, args.max_total_bytes)
    manifest = Path(args.manifest or (ACQUISITION_MANIFEST if args.date == date(2025, 6, 1) and hours == [0]
                    else f"/data/test/gharchive/{args.date}/acquisition.json"))
    manifest.parent.mkdir(parents=True, exist_ok=True)
    result = {"date": str(args.date), "hours": hours, "sources": plan["sources"], "completed": [],
              "maximum_file_bytes": args.max_file_bytes, "maximum_total_bytes": args.max_total_bytes,
              "coverage": "Complete selected hours, without row truncation", "complete": False}
    def save():
        temporary = manifest.with_suffix(".tmp")
        temporary.write_text(json.dumps(result, indent=2) + "\n")
        temporary.replace(manifest)
    save()
    if args.head_only:
        print(json.dumps(result, indent=2), flush=True)
        return
    started = time.monotonic()
    for source in plan["sources"]:
        metadata = acquire(source, args.raw_root, args.max_file_bytes)
        result["completed"].append(metadata)
        save()
        print(f"Completed hour {source['hour']}: bytes={metadata['compressed_bytes']}; sha256={metadata['sha256']}; reused={metadata['reused']}", flush=True)
    result.update(complete=True, compressed_bytes=sum(item["compressed_bytes"] for item in result["completed"]),
                  elapsed_seconds=time.monotonic() - started)
    save()
    print(f"Acquisition complete; manifest={manifest}; seconds={result['elapsed_seconds']:.3f}", flush=True)


if __name__ == "__main__":
    main()
