@echo off
cd /d %~dp0
where py >nul 2>&1
if %errorlevel%==0 (
    py -3 main.py
    goto :end
)
where python >nul 2>&1
if %errorlevel%==0 (
    python main.py
    goto :end
)
echo Python 3.11+ not found in PATH.
echo Install Python and enable Add Python to PATH, then run again.
:end
pause
