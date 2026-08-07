@echo off
setlocal

cd /d "%~dp0"
set "QQOKX_DATA_DIR=D:\qqokx_data"
set "MPLCONFIGDIR=%~dp0.mplconfig"

if exist ".venv\Scripts\pythonw.exe" (
    start "QQOKX Workbench" ".venv\Scripts\pythonw.exe" "%~dp0main.py"
    exit /b 0
)

if exist ".venv\Scripts\python.exe" (
    start "QQOKX Workbench" ".venv\Scripts\python.exe" "%~dp0main.py"
    exit /b 0
)

echo [ERROR] Project virtual environment not found: .venv
pause
exit /b 1
