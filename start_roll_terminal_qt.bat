@echo off
cd /d "%~dp0"
where py >nul 2>&1
if %errorlevel%==0 (
    py -3 run_roll_terminal_qt.pyw %*
    goto :end
)
where pythonw >nul 2>&1
if %errorlevel%==0 (
    pythonw run_roll_terminal_qt.pyw %*
    goto :end
)
where python >nul 2>&1
if %errorlevel%==0 (
    start "" python run_roll_terminal_qt.pyw %*
) else (
    echo 未检测到可用的 Python3 可执行文件，请先安装 Python 3.11+ 并配置环境变量
    pause
)
:end
