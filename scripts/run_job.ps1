# No host Python; the image validates resources and invokes spark-submit.
$ErrorActionPreference = 'Stop'
if ($args.Count -lt 1 -or $args[0] -notin @('ingest', 'transform', 'aggregate', 'pipeline')) {
    throw 'Usage: run_job.ps1 <ingest|transform|aggregate|pipeline> [options]'
}
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$Stage = $args[0]
$JobArgs = @($args | Select-Object -Skip 1)
& docker compose --project-directory $ProjectRoot -f (Join-Path $ProjectRoot 'compose.yaml') run --rm job "/app/jobs/$Stage.py" @JobArgs
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
