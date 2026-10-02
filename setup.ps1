# Bootstrap only Docker and autonomous fixtures; never download real archives.
$ErrorActionPreference = 'Stop'
$ProjectRoot = $PSScriptRoot
Push-Location $ProjectRoot
try {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw 'Docker is not installed or is not on PATH.'
    }
    & docker compose version | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Docker Compose v2 is required.' }
    if (-not (Test-Path -LiteralPath 'compose.yaml' -PathType Leaf)) { throw 'Missing compose.yaml.' }
    foreach ($Key in @('LOCAL_UID', 'LOCAL_GID')) {
        $ConfiguredValue = $null
        if (Test-Path -LiteralPath '.env' -PathType Leaf) {
            foreach ($Line in Get-Content -LiteralPath '.env') {
                if ($Line -match "^\s*(export\s+)?${Key}=(.*)$") {
                    $ConfiguredValue = ($Matches[2] -split '[\s#]', 2)[0].Trim('"', "'")
                }
            }
        }
        $Value = [Environment]::GetEnvironmentVariable($Key)
        if ($null -ne $Value) { $ConfiguredValue = $Value }
        if ($null -ne $ConfiguredValue -and $ConfiguredValue -notmatch '^\d+$') {
            throw "$Key must be a nonempty numeric identity."
        }
    }
    function Invoke-Compose {
        & docker compose --project-directory $ProjectRoot -f (Join-Path $ProjectRoot 'compose.yaml') @args
        if ($LASTEXITCODE -ne 0) { throw "Docker Compose failed (exit $LASTEXITCODE): $args" }
    }
    Invoke-Compose config --quiet
    Invoke-Compose build
    if (-not (Test-Path -LiteralPath 'data')) { New-Item -ItemType Directory -Path 'data' | Out-Null }
    try {
        Invoke-Compose run --rm --entrypoint python3 job -c "import os,tempfile; print('Runtime identity:',os.getuid(),os.getgid()); p=tempfile.NamedTemporaryFile(prefix='.setup-write-',dir='/data'); p.write(b'setup probe'); p.flush(); p.close(); print('PASS: data bind is writable; probe removed')"
    } catch {
        throw 'The configured runtime cannot write data/. Set LOCAL_UID/LOCAL_GID for the directory owner in .env or your shell. Existing permissions were not changed.'
    }
    Invoke-Compose run --rm test
    Invoke-Compose run --rm --entrypoint python3 job /app/scripts/check_reproducibility.py
    Write-Host 'PASS: setup, autonomous tests and generated-input CLI checks. No real data downloaded.'
} finally {
    Pop-Location
}
