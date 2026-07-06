@echo off
setlocal
cd /d "%~dp0"

where py >nul 2>&1
if %errorlevel%==0 (
    py -3-64 -V >nul 2>&1
    if %errorlevel%==0 (
        py -3-64 run_roll_terminal_qt.pyw %*
        goto :end
    )
)

echo [ERROR] 未检测到可用的 64 位 Python 3.11+，当前可能只安装了 32 位 Python。
echo 请在机器上安装 64 位 Python 3.11+（建议从 python.org 安装），并勾选 Add python.exe to PATH。
echo.
echo 若你已安装 64 位 Python，请用明确路径启动：
echo   "C:\\Users\\Windows\\AppData\\Local\\Programs\\Python\\Python312\\pythonw.exe" run_roll_terminal_qt.pyw
echo.
echo 安装后再执行：
echo   py -3-64 -m pip install -r roll_terminal_qt_requirements.txt
pause

:end
endlocal
