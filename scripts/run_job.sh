#!/usr/bin/env bash
# No host Python; the image validates resources and invokes spark-submit.
set -euo pipefail
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ $# -lt 1 ]]; then printf 'Usage: run_job.sh <ingest|transform|aggregate|pipeline> [options]\n' >&2; exit 2; fi
stage="$1"; shift
case "$stage" in ingest|transform|aggregate|pipeline) ;; *) printf 'Unknown stage: %s\n' "$stage" >&2; exit 2;; esac
docker compose --project-directory "$project_root" -f "$project_root/compose.yaml" run --rm job "/app/jobs/$stage.py" "$@"
