@echo off
setlocal

powershell -ExecutionPolicy Bypass -File "%~dp0release_one_click.ps1" %*
set "exit_code=%errorlevel%"

echo.
echo release_one_click exit_code=%exit_code%
echo Press any key to close this window.
pause >nul

endlocal & exit /b %exit_code%
