@echo off
setlocal

cd /d "%~dp0"

if exist "RUN_QT.bat" (
    call "RUN_QT.bat" %*
    exit /b %errorlevel%
)

where py >nul 2>&1
if "%errorlevel%"=="0" (
    py -3-64 run_roll_terminal_qt.pyw %*
    if "%errorlevel%"=="0" (
        exit /b 0
    )
)

where py >nul 2>&1
if "%errorlevel%"=="0" (
    py -3 run_roll_terminal_qt.pyw %*
    if "%errorlevel%"=="0" (
        exit /b 0
    )
)

where pythonw >nul 2>&1
if "%errorlevel%"=="0" (
    pythonw run_roll_terminal_qt.pyw %*
    exit /b %errorlevel%
)

where python >nul 2>&1
if "%errorlevel%"=="0" (
    python run_roll_terminal_qt.pyw %*
    exit /b %errorlevel%
)

echo [ERROR] Python 3 not found in PATH.
pause
exit /b 1
