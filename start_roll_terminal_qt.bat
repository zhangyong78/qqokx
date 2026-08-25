@echo off
setlocal

cd /d "%~dp0"

if exist ".venv\Scripts\python.exe" (
    .venv\Scripts\python.exe -c "import struct; assert struct.calcsize('P') == 8" >nul 2>&1
    if not errorlevel 1 (
        .venv\Scripts\python.exe run_roll_terminal_qt_bootstrap.py %*
        exit /b %errorlevel%
    )
)

if exist ".venv_old\Scripts\python.exe" (
    .venv_old\Scripts\python.exe -c "import struct; assert struct.calcsize('P') == 8" >nul 2>&1
    if not errorlevel 1 (
        .venv_old\Scripts\python.exe run_roll_terminal_qt_bootstrap.py %*
        exit /b %errorlevel%
    )
)

if exist "RUN_QT.bat" (
    call "RUN_QT.bat" %*
    exit /b %errorlevel%
)

set "QT_PYTHON_EXE="
set "QT_PYTHON_LAUNCHER="
set "QT_PYTHON_ARGS="
call :try_path "C:\Program Files\Python313\python.exe"
call :try_path "C:\Program Files\Python313\pythonw.exe"
call :try_path "C:\Program Files\Python312\python.exe"
call :try_path "%LocalAppData%\Programs\Python\Python313\python.exe"
call :try_path "%LocalAppData%\Programs\Python\Python312\python.exe"
for %%V in (3.12 3.13 3.11 3.14) do call :try_py %%V
if not defined QT_PYTHON_EXE if not defined QT_PYTHON_LAUNCHER (
    where python >nul 2>&1
    if not errorlevel 1 (
        python -c "import struct; assert struct.calcsize('P') == 8" >nul 2>&1
        if not errorlevel 1 set "QT_PYTHON_EXE=python"
    )
)
if not defined QT_PYTHON_EXE if not defined QT_PYTHON_LAUNCHER (
    where pythonw >nul 2>&1
    if not errorlevel 1 (
        pythonw -c "import struct; assert struct.calcsize('P') == 8" >nul 2>&1
        if not errorlevel 1 set "QT_PYTHON_EXE=pythonw"
    )
)
if defined QT_PYTHON_EXE (
    echo [QQOKX] Using %QT_PYTHON_EXE%
    "%QT_PYTHON_EXE%" run_roll_terminal_qt_bootstrap.py %*
    exit /b %errorlevel%
)
if defined QT_PYTHON_LAUNCHER (
    echo [QQOKX] Using %QT_PYTHON_LAUNCHER% %QT_PYTHON_ARGS%
    %QT_PYTHON_LAUNCHER% %QT_PYTHON_ARGS% run_roll_terminal_qt_bootstrap.py %*
    exit /b %errorlevel%
)
echo [ERROR] No 64-bit Python 3.11+ was found.
echo Install Python 3.11+ and run start_roll_terminal_qt.bat again.
pause
exit /b 1

:try_py
if defined QT_PYTHON_EXE exit /b 0
if defined QT_PYTHON_LAUNCHER exit /b 0
py -%~1 -c "import struct, PySide6, websockets; assert struct.calcsize('P') == 8" >nul 2>&1
if not errorlevel 1 (
    set "QT_PYTHON_LAUNCHER=py"
    set "QT_PYTHON_ARGS=-%~1"
)
exit /b 0

:try_path
if defined QT_PYTHON_EXE exit /b 0
if defined QT_PYTHON_LAUNCHER exit /b 0
if not exist "%~1" exit /b 0
"%~1" -c "import struct; assert struct.calcsize('P') == 8" >nul 2>&1
if not errorlevel 1 set "QT_PYTHON_EXE=%~1"
exit /b 0
