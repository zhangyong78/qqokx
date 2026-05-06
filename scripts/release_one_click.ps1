param(
    [ValidateSet("patch", "minor", "major")]
    [string]$Bump = "patch",
    [string]$CommitMessage = "",
    [switch]$SkipBuild,
    [switch]$SkipPush,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location -LiteralPath $repoRoot
$utf8Bom = [System.Text.UTF8Encoding]::new($true)

function U([string]$s) {
    return [regex]::Unescape($s)
}

function Get-CurrentVersion {
    $pyproject = Join-Path $repoRoot "pyproject.toml"
    $match = Select-String -Path $pyproject -Pattern '^\s*version\s*=\s*"(?<version>\d+\.\d+\.\d+)"\s*$' | Select-Object -First 1
    if (-not $match) { throw (U '\u65e0\u6cd5\u4ece pyproject.toml \u8bfb\u53d6\u7248\u672c\u53f7\u3002') }
    return [version]$match.Matches[0].Groups['version'].Value
}

function Format-Version([version]$v) {
    return "{0}.{1}.{2:00}" -f $v.Major, $v.Minor, $v.Build
}

function Get-NextVersion([version]$current, [string]$bump) {
    switch ($bump) {
        'patch' { return [version]::new($current.Major, $current.Minor, $current.Build + 1) }
        'minor' { return [version]::new($current.Major, $current.Minor + 1, 0) }
        'major' { return [version]::new($current.Major + 1, 0, 0) }
        default { throw (U '\u672a\u77e5\u7248\u672c\u7ea7\u522b\uff1a') + $bump }
    }
}

function Update-TextFile([string]$Path, [scriptblock]$Updater) {
    if (-not (Test-Path -LiteralPath $Path)) { return }
    $text = [System.IO.File]::ReadAllText($Path)
    $newText = & $Updater $text
    if ($newText -ne $text) {
        [System.IO.File]::WriteAllText($Path, $newText, $utf8Bom)
    }
}

function Get-ChangedFiles {
    $raw = git ls-files -m -o --exclude-standard -z
    if (-not $raw) {
        return @()
    }

    $files = New-Object System.Collections.Generic.List[string]
    $seen = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
    foreach ($path in ($raw -split "`0")) {
        if ([string]::IsNullOrWhiteSpace($path)) { continue }
        $normalized = $path.Trim().Replace('\', '/')
        if ($normalized -match '^(dist/|reports/|\.codex/|__pycache__/)' -or $normalized -like '*.pyc') { continue }
        if ($seen.Add($normalized)) { $files.Add($normalized) }
    }
    return $files
}

function Get-ReleaseTopics([string[]]$Files) {
    $topics = New-Object System.Collections.Generic.List[string]
    $added = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
    function Push-Topic([string]$topic) { if (-not [string]::IsNullOrWhiteSpace($topic) -and $added.Add($topic)) { $topics.Add($topic) } }
    foreach ($file in $Files) {
        switch -Wildcard ($file) {
            'README.md' { Push-Topic (U '\u66f4\u65b0 README \u4e2d\u6587\u66f4\u65b0\u65e5\u5fd7\u4e0e\u7248\u672c\u8bf4\u660e'); continue }
            'pyproject.toml' { Push-Topic (U '\u540c\u6b65\u9879\u76ee\u7248\u672c\u53f7'); continue }
            'okx_quant/__init__.py' { Push-Topic (U '\u540c\u6b65\u5305\u5185\u7248\u672c\u53f7'); continue }
            '??????.md' { Push-Topic (U '\u540c\u6b65\u8f6f\u4ef6\u5f00\u53d1\u6307\u5357\u4e2d\u7684\u7248\u672c\u4e0e\u534f\u4f5c\u8bf4\u660e'); continue }
            '???????.md' { Push-Topic (U '\u66f4\u65b0\u53d1\u7248\u5f85\u6253\u5305\u6e05\u5355'); continue }
            '??????.md' { Push-Topic (U '\u6574\u7406\u53d1\u7248\u534f\u4f5c\u7ea6\u5b9a'); continue }
            '???????.md' { Push-Topic (U '\u6574\u7406\u7ebf\u7a0b\u5de5\u4f5c\u6d41\u6a21\u677f'); continue }
            'okx_quant/engine.py' { Push-Topic (U '\u6536\u53e3\u4ea4\u6613\u5f15\u64ce\u4e0e\u5b9e\u76d8\u63a7\u5236'); continue }
            'okx_quant/enhanced_live_engine.py' { Push-Topic (U '\u6574\u7406\u589e\u5f3a\u5b9e\u76d8\u5f15\u64ce'); continue }
            'okx_quant/ui.py' { Push-Topic (U '\u6536\u53e3\u4e3b\u754c\u9762\u4e0e\u7b56\u7565\u5de5\u4f5c\u53f0'); continue }
            'okx_quant/ui_shell.py' { Push-Topic (U '\u6574\u7406\u4e3b\u754c\u9762\u58f3\u5c42\u4e0e\u5e03\u5c40'); continue }
            'okx_quant/ui_positions.py' { Push-Topic (U '\u4f18\u5316\u6301\u4ed3\u4e0e\u5386\u53f2\u89c6\u56fe'); continue }
            'okx_quant/backtest.py' { Push-Topic (U '\u8c03\u6574\u56de\u6d4b\u903b\u8f91\u4e0e\u53e3\u5f84'); continue }
            'okx_quant/backtest_ui.py' { Push-Topic (U '\u4f18\u5316\u56de\u6d4b\u754c\u9762'); continue }
            'okx_quant/backtest_export.py' { Push-Topic (U '\u5b8c\u5584\u56de\u6d4b\u5bfc\u51fa'); continue }
            'okx_quant/persistence.py' { Push-Topic (U '\u5b8c\u5584\u6301\u4e45\u5316\u4e0e\u914d\u7f6e\u5b58\u50a8'); continue }
            'okx_quant/notifications.py' { Push-Topic (U '\u8865\u9f50\u90ae\u4ef6\u901a\u77e5\u4e0a\u4e0b\u6587'); continue }
            'okx_quant/position_protection.py' { Push-Topic (U '\u4f18\u5316\u6301\u4ed3\u4fdd\u62a4\u901a\u77e5\u4e0e\u6d41\u7a0b'); continue }
            'okx_quant/signal_monitor_ui.py' { Push-Topic (U '\u4f18\u5316\u4fe1\u53f7\u76d1\u63a7\u8054\u52a8'); continue }
            'okx_quant/trader_desk.py' { Push-Topic (U '\u6574\u7406\u4ea4\u6613\u5458\u53f0\u903b\u8f91'); continue }
            'okx_quant/trader_desk_ui.py' { Push-Topic (U '\u4f18\u5316\u4ea4\u6613\u5458\u53f0\u754c\u9762'); continue }
            'okx_quant/strategy_live_chart.py' { Push-Topic (U '\u5b8c\u5584\u7b56\u7565\u5b9e\u76d8\u5b9e\u65f6\u56fe\u8868'); continue }
            'okx_quant/strategy_parameters.py' { Push-Topic (U '\u62bd\u8c61\u7b56\u7565\u53c2\u6570\u914d\u7f6e'); continue }
            'okx_quant/candle_patterns.py' { Push-Topic (U '\u8865\u5145 K \u7ebf\u5f62\u6001\u5206\u6790'); continue }
            'scripts/run_*.py' { Push-Topic (U '\u65b0\u589e\u5206\u6790\u6216\u62a5\u8868\u811a\u672c'); continue }
            'scripts/build_server_package.py' { Push-Topic (U '\u8c03\u6574\u6253\u5305\u811a\u672c'); continue }
            'btc_analysis_alignment_checklist_v1.md' { Push-Topic (U '\u65b0\u589e BTC \u5206\u6790\u5bf9\u9f50\u6e05\u5355'); continue }
        }
    }
    if ($topics.Count -eq 0) { Push-Topic (U '\u6536\u53e3\u5f53\u524d\u5de5\u4f5c\u533a\u6539\u52a8') }
    return $topics.ToArray()
}

function Build-ReleaseSummary([string[]]$Files, [string]$VersionText, [string]$BumpLevel, [string]$CommitMessageText) {
    $topics = Get-ReleaseTopics $Files
    $fileList = ($Files | Select-Object -Unique | Sort-Object) -join '、'
    $summary = @()
    $summary += "### v$VersionText | $(Get-Date -Format 'yyyy-MM-dd') | $($topics -join '、')"
    foreach ($topic in $topics) { $summary += "- $topic" }
    $summary += "- 相关文件：$fileList"
    $summary += "- 本次按 `$BumpLevel` 级别处理，版本已递进到 `v$VersionText`。"
    if (-not [string]::IsNullOrWhiteSpace($CommitMessageText)) { $summary += "- 提交说明：$CommitMessageText" }
    return ($summary -join "`r`n")
}

function Update-Version-Files([string]$oldVersionText, [string]$newVersionText, [string]$releaseSummary) {
    foreach ($rel in @('pyproject.toml','okx_quant/__init__.py','README.md','软件开发指南.md')) {
        Update-TextFile (Join-Path $repoRoot $rel) { param($text) $text.Replace($oldVersionText, $newVersionText) }
    }

    Update-TextFile (Join-Path $repoRoot 'README.md') {
        param($text)
        $updated = $text.Replace(('当前版本：`v' + $oldVersionText + '`'), ('当前版本：`v' + $newVersionText + '`'))
        $header = "## 11. 更新日志"
        $section = @"
### v$newVersionText

$releaseSummary

"@
        if ($updated.Contains("### v$newVersionText")) { return $updated }
        if ($updated.Contains($header)) {
            $parts = $updated.Split(@($header), 2, [System.StringSplitOptions]::None)
            if ($parts.Count -eq 2) {
                return $parts[0] + $header + "`r`n`r`n当前版本：v$newVersionText`r`n`r`n" + $section + $parts[1].TrimStart("`r", "`n")
            }
        }
        return $updated
    }

    Update-TextFile (Join-Path $repoRoot '软件开发指南.md') {
        param($text)
        $updated = $text.Replace(('当前版本：`v' + $oldVersionText + '`'), ('当前版本：`v' + $newVersionText + '`'))
        $updated = $updated.Replace(('当前里程碑版本：`v' + $oldVersionText + '`'), ('当前里程碑版本：`v' + $newVersionText + '`'))
        return $updated
    }

    Update-TextFile (Join-Path $repoRoot '发版待打包清单.md') {
        param($text)
        $dateText = Get-Date -Format 'yyyy-MM-dd'
        $topicLine = ($releaseSummary -split "`r?`n" | Select-String -Pattern '^### ' | Select-Object -First 1).Line
        $topicText = if ($topicLine) { $topicLine -replace '^###\s+v[^|]+\|\s*[^|]+\|\s*', '' } else { '自动一键发版' }
        $entry = @"
### v$newVersionText | $dateText | $topicText
$releaseSummary

"@
        if ($text.Contains("### v$newVersionText |")) { return $text }
        return "# 发版待打包清单`r`n## 已收录`r`n`r`n$entry$($text.TrimStart())"
    }
}

$currentVersion = Get-CurrentVersion
$nextVersion = Get-NextVersion $currentVersion $Bump
$currentVersionText = Format-Version $currentVersion
$nextVersionText = Format-Version $nextVersion
$changedFiles = Get-ChangedFiles
if ($changedFiles.Count -eq 0) { Write-Host (U '\u6ca1\u6709\u68c0\u6d4b\u5230\u53ef\u6536\u53e3\u7684\u6587\u4ef6\u3002'); exit 0 }
$releaseSummary = Build-ReleaseSummary -Files $changedFiles -VersionText $nextVersionText -BumpLevel $Bump -CommitMessageText $CommitMessage

if ($DryRun) {
    Write-Host "DRY RUN v$nextVersionText"
    Write-Host "版本：v$nextVersionText"
    Write-Host "摘要："
    Write-Host $releaseSummary
    Write-Host "文件："
    $changedFiles | ForEach-Object { Write-Host " - $_" }
    exit 0
}

Update-Version-Files -oldVersionText $currentVersionText -newVersionText $nextVersionText -releaseSummary $releaseSummary
if (-not $SkipBuild) { python scripts\build_server_package.py }
git add -u
if ($LASTEXITCODE -ne 0) { throw (U '\u6253\u5305\u6210\u529f\uff0c\u4f46 git add -u \u5931\u8d25\u3002') }
$changedFiles = Get-ChangedFiles
if ($changedFiles.Count -gt 0) {
    $tempList = [System.IO.Path]::GetTempFileName()
    try {
        [System.IO.File]::WriteAllText($tempList, ($changedFiles -join "`0") + "`0", [System.Text.UTF8Encoding]::new($false))
        git add --pathspec-from-file=$tempList --pathspec-file-nul
        if ($LASTEXITCODE -ne 0) { throw (U '\u6253\u5305\u6210\u529f\uff0c\u4f46 git add \u65b0\u6587\u4ef6\u5931\u8d25\u3002') }
    }
    finally {
        Remove-Item -LiteralPath $tempList -ErrorAction SilentlyContinue
    }
}
git diff --cached --quiet
if ($LASTEXITCODE -eq 0) { Write-Host (U '\u6ca1\u6709\u53ef\u63d0\u4ea4\u7684\u6587\u4ef6\u3002'); exit 0 }
if ([string]::IsNullOrWhiteSpace($CommitMessage)) { $CommitMessage = "release: v$nextVersionText automated one-click release" }
git commit -m $CommitMessage
if (-not $SkipPush) { git push origin main }
Write-Host "DONE v$nextVersionText"
