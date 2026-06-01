$ErrorActionPreference = "Stop"

$runnerPath = Join-Path $PSScriptRoot "send_btc_market_analysis_email.ps1"
$powershellPath = Join-Path $env:SystemRoot "System32\WindowsPowerShell\v1.0\powershell.exe"
$taskPrefix = "QQOKX BTC Analysis Email"
$times = @("08:00", "12:00", "16:00", "20:00")

foreach ($time in $times) {
    $suffix = $time.Replace(":", "")
    $taskName = "$taskPrefix $suffix"
    $taskCommand = "`"$powershellPath`" -NoProfile -ExecutionPolicy Bypass -File `"$runnerPath`""
    schtasks /Create /TN $taskName /SC DAILY /ST $time /TR $taskCommand /F | Out-Null
    Write-Output "registered $taskName at $time"
}
