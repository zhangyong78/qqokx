@echo off
setlocal

cd /d "%~dp0"
set "QQOKX_DATA_DIR=D:\qqokx_data"
set "MPLCONFIGDIR=%~dp0.mplconfig"

if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" "%~dp0run_main_bootstrap.py" %*
    pause
    exit /b %errorlevel%
)

echo [ERROR] Project virtual environment not found: .venv
pause
exit /b 1
