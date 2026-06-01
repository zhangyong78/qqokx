$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

$python = Join-Path $projectRoot ".venv\Scripts\python.exe"
$scriptPath = Join-Path $projectRoot "scripts\run_multi_coin_market_digest.py"

if (-not (Test-Path $python)) {
    throw "Python executable not found: $python"
}

$output = & $python $scriptPath --send-email 2>&1
$exitCode = $LASTEXITCODE
$output | ForEach-Object { Write-Output $_ }

if ($exitCode -ne 0) {
    throw "BTC analysis command failed with exit code $exitCode"
}

if (-not ($output -contains "email_sent")) {
    throw "Multi-coin digest finished but did not report email_sent"
}
