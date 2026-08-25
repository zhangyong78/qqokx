@echo off
setlocal

cd /d "%~dp0"

if exist ".venv\Scripts\python.exe" (
    .venv\Scripts\python.exe -c "import PySide6" >nul 2>&1
    if not errorlevel 1 (
        .venv\Scripts\python.exe run_roll_terminal_qt.pyw %*
        exit /b %errorlevel%
    )
)

if exist ".venv_old\Scripts\python.exe" (
    .venv_old\Scripts\python.exe -c "import PySide6" >nul 2>&1
    if not errorlevel 1 (
        .venv_old\Scripts\python.exe run_roll_terminal_qt.pyw %*
        exit /b %errorlevel%
    )
)

if exist "RUN_QT.bat" (
    call "RUN_QT.bat" %*
    exit /b %errorlevel%
)

set "QT_PYTHON="
call :try_path "C:\Program Files\Python313\python.exe"
call :try_path "C:\Program Files\Python313\pythonw.exe"
call :try_path "C:\Program Files\Python312\python.exe"
call :try_path "%LocalAppData%\Programs\Python\Python313\python.exe"
call :try_path "%LocalAppData%\Programs\Python\Python312\python.exe"
for %%V in (3.12 3.13 3.11 3.14) do call :try_py %%V
if not defined QT_PYTHON (
    where python >nul 2>&1
    if not errorlevel 1 (
        python -c "import struct, PySide6, websockets; assert struct.calcsize('P') == 8" >nul 2>&1
        if not errorlevel 1 set "QT_PYTHON=python"
    )
)
if not defined QT_PYTHON (
    where pythonw >nul 2>&1
    if not errorlevel 1 (
        pythonw -c "import struct, PySide6, websockets; assert struct.calcsize('P') == 8" >nul 2>&1
        if not errorlevel 1 set "QT_PYTHON=pythonw"
    )
)
if defined QT_PYTHON (
    echo [QQOKX] Using %QT_PYTHON%
    %QT_PYTHON% run_roll_terminal_qt.py %*
    exit /b %errorlevel%
)
echo [ERROR] No 64-bit Python with PySide6 and websockets was found.
echo Install dependencies with: py -3.13 -m pip install -r roll_terminal_qt_requirements.txt
pause
exit /b 1

:try_py
if defined QT_PYTHON exit /b 0
py -%~1 -c "import struct, PySide6, websockets; assert struct.calcsize('P') == 8" >nul 2>&1
if not errorlevel 1 set "QT_PYTHON=py -%~1"
exit /b 0

:try_path
if defined QT_PYTHON exit /b 0
if not exist "%~1" exit /b 0
"%~1" -c "import struct, PySide6, websockets; assert struct.calcsize('P') == 8" >nul 2>&1
if not errorlevel 1 set "QT_PYTHON=%~1"
exit /b 0
