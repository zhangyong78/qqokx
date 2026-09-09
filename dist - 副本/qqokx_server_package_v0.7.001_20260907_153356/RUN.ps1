Set-Location -LiteralPath $PSScriptRoot
if (Get-Command py -ErrorAction SilentlyContinue) {
    py -3 .\main.py
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
    python .\main.py
} else {
    Write-Host 'Python 3.11+ not found in PATH.'
    Write-Host 'Install Python and enable Add Python to PATH, then run again.'
}
