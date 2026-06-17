$ErrorActionPreference = "Stop"

$runnerPath = Join-Path $PSScriptRoot "send_btc_market_analysis_email.ps1"
$powershellPath = Join-Path $env:SystemRoot "System32\WindowsPowerShell\v1.0\powershell.exe"
$taskPrefix = "QQOKX BTC Analysis Email"
$taskConfigs = @(
    @{ Time = "00:00"; DeliveryMode = "archive_only"; ScheduledReleaseSlot = "08:00"; AnalysisSlot = "00:00" },
    @{ Time = "04:00"; DeliveryMode = "archive_only"; ScheduledReleaseSlot = "08:00"; AnalysisSlot = "04:00" },
    @{ Time = "08:00"; DeliveryMode = "release_pending_and_send"; ScheduledReleaseSlot = "08:00"; AnalysisSlot = "08:00" },
    @{ Time = "12:00"; DeliveryMode = "immediate"; ScheduledReleaseSlot = "08:00"; AnalysisSlot = "12:00" },
    @{ Time = "16:00"; DeliveryMode = "immediate"; ScheduledReleaseSlot = "08:00"; AnalysisSlot = "16:00" },
    @{ Time = "20:00"; DeliveryMode = "immediate"; ScheduledReleaseSlot = "08:00"; AnalysisSlot = "20:00" }
)

foreach ($config in $taskConfigs) {
    $time = [string]$config.Time
    $suffix = $time.Replace(":", "")
    $taskName = "$taskPrefix $suffix"
    $taskCommand = (
        "`"$powershellPath`" -NoProfile -ExecutionPolicy Bypass -File `"$runnerPath`" " +
        "-DeliveryMode `"$($config.DeliveryMode)`" " +
        "-ScheduledReleaseSlot `"$($config.ScheduledReleaseSlot)`" " +
        "-AnalysisSlot `"$($config.AnalysisSlot)`""
    )
    schtasks /Create /TN $taskName /SC DAILY /ST $time /TR $taskCommand /F | Out-Null
    Write-Output "registered $taskName at $time [$($config.DeliveryMode)]"
}
