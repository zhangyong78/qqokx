@echo off
setlocal

powershell -ExecutionPolicy Bypass -File "%~dp0release_one_click.ps1" %*
set "exit_code=%errorlevel%"

endlocal & exit /b %exit_code%
