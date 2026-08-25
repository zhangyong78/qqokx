from pathlib import Path
import argparse
import shutil
import sys
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from okx_quant.app_meta import APP_VERSION


def write_text(path: Path, content: str, *, encoding: str = "utf-8-sig") -> None:
    path.write_text(content, encoding=encoding, newline="\n")


def package_ignore(_src: str, names: list[str]) -> set[str]:
    ignored: set[str] = set()
    for name in names:
        if (
            name == "__pycache__"
            or name.endswith(".pyc")
            or name.endswith(".pyo")
            or ".bak_" in name
        ):
            ignored.add(name)
    return ignored


def build_package(version: str) -> tuple[Path, Path]:
    project_root = PROJECT_ROOT
    dist_root = project_root / "dist"
    package_name = f"qqokx_server_package_v{APP_VERSION}_{version}"
    stage_dir = dist_root / package_name
    zip_base = dist_root / package_name
    zip_path = dist_root / f"{package_name}.zip"

    if stage_dir.exists():
        shutil.rmtree(stage_dir)
    if zip_path.exists():
        zip_path.unlink()

    dist_root.mkdir(parents=True, exist_ok=True)
    stage_dir.mkdir(parents=True, exist_ok=True)

    for file_name in (
        "main.py",
        "pyproject.toml",
        "requirements.txt",
        "roll_terminal_qt_requirements.txt",
        "README.md",
        "发版协作约定.md",
        "软件开发指南.md",
        "线程工作流模板.md",
    ):
        shutil.copy2(project_root / file_name, stage_dir / file_name)

    for file_name in (
        "run_roll_terminal_qt.py",
        "run_roll_terminal_qt_bootstrap.py",
        "run_roll_terminal_qt.pyw",
        "start_roll_terminal_qt.bat",
        "start_roll_terminal_qt_silent.vbs",
        "roll_terminal_qt_README.md",
    ):
        source = project_root / file_name
        if source.exists():
            shutil.copy2(source, stage_dir / file_name)

    shutil.copytree(
        project_root / "okx_quant",
        stage_dir / "okx_quant",
        dirs_exist_ok=True,
        ignore=package_ignore,
    )
    shutil.copytree(
        project_root / "roll_terminal_qt",
        stage_dir / "roll_terminal_qt",
        dirs_exist_ok=True,
        ignore=package_ignore,
    )

    write_text(
        stage_dir / "RUN.bat",
        (
            "@echo off\r\n"
            "cd /d %~dp0\r\n"
            "where py >nul 2>&1\r\n"
            "if %errorlevel%==0 (\r\n"
            "    py -3 main.py\r\n"
            "    goto :end\r\n"
            ")\r\n"
            "where python >nul 2>&1\r\n"
            "if %errorlevel%==0 (\r\n"
            "    python main.py\r\n"
            "    goto :end\r\n"
            ")\r\n"
            "echo Python 3.11+ not found in PATH.\r\n"
            "echo Install Python and enable Add Python to PATH, then run again.\r\n"
            ":end\r\n"
            "pause\r\n"
        ),
        encoding="utf-8",
    )
    write_text(
        stage_dir / "RUN_QT.bat",
        (
            "@echo off\r\n"
            "setlocal EnableExtensions\r\n"
            "cd /d %~dp0\r\n"
            "set \"QT_PYTHON_EXE=\"\r\n"
            "set \"QT_PYTHON_LAUNCHER=\"\r\n"
            "set \"QT_PYTHON_ARGS=\"\r\n"
            "call :try_path \".venv\\Scripts\\python.exe\"\r\n"
            "call :try_path \".venv_old\\Scripts\\python.exe\"\r\n"
            "call :try_path \"C:\\Program Files\\Python313\\python.exe\"\r\n"
            "call :try_path \"C:\\Program Files\\Python313\\pythonw.exe\"\r\n"
            "call :try_path \"C:\\Program Files\\Python312\\python.exe\"\r\n"
            "call :try_path \"%LocalAppData%\\Programs\\Python\\Python313\\python.exe\"\r\n"
            "call :try_path \"%LocalAppData%\\Programs\\Python\\Python312\\python.exe\"\r\n"
            "if not defined QT_PYTHON_EXE if not defined QT_PYTHON_LAUNCHER (\r\n"
            "    for %%V in (3.12 3.13 3.11 3.14) do call :try_py %%V\r\n"
            ")\r\n"
            "if not defined QT_PYTHON_EXE if not defined QT_PYTHON_LAUNCHER (\r\n"
            "    where python >nul 2>&1\r\n"
            "    if not errorlevel 1 (\r\n"
            "    python -c \"import struct; assert struct.calcsize('P') == 8\" >nul 2>&1\r\n"
            "    if not errorlevel 1 set \"QT_PYTHON_EXE=python\"\r\n"
            "    )\r\n"
            ")\r\n"
            "if not defined QT_PYTHON_EXE if not defined QT_PYTHON_LAUNCHER (\r\n"
            "    where pythonw >nul 2>&1\r\n"
            "    if not errorlevel 1 (\r\n"
            "        pythonw -c \"import struct; assert struct.calcsize('P') == 8\" >nul 2>&1\r\n"
            "        if not errorlevel 1 set \"QT_PYTHON_EXE=pythonw\"\r\n"
            "    )\r\n"
            ")\r\n"
            "if defined QT_PYTHON_EXE (\r\n"
            "    echo [QQOKX] Using %QT_PYTHON_EXE%\r\n"
            "    \"%QT_PYTHON_EXE%\" run_roll_terminal_qt_bootstrap.py %*\r\n"
            "    exit /b %errorlevel%\r\n"
            ")\r\n"
            "if defined QT_PYTHON_LAUNCHER (\r\n"
            "    echo [QQOKX] Using %QT_PYTHON_LAUNCHER% %QT_PYTHON_ARGS%\r\n"
            "    %QT_PYTHON_LAUNCHER% %QT_PYTHON_ARGS% run_roll_terminal_qt_bootstrap.py %*\r\n"
            "    exit /b %errorlevel%\r\n"
            ")\r\n"
            "echo [ERROR] No 64-bit Python 3.11+ was found.\r\n"
            "echo Install Python 3.11+ and run RUN_QT.bat again.\r\n"
            "pause\r\n"
            "exit /b 1\r\n"
            ":try_path\r\n"
            "if defined QT_PYTHON_EXE exit /b 0\r\n"
            "if defined QT_PYTHON_LAUNCHER exit /b 0\r\n"
            "if not exist \"%~1\" exit /b 0\r\n"
            "\"%~1\" -c \"import struct; assert struct.calcsize('P') == 8\" >nul 2>&1\r\n"
            "if not errorlevel 1 set \"QT_PYTHON_EXE=%~1\"\r\n"
            "exit /b 0\r\n"
            ":try_py\r\n"
            "if defined QT_PYTHON_EXE exit /b 0\r\n"
            "if defined QT_PYTHON_LAUNCHER exit /b 0\r\n"
            "py -%~1 -c \"import struct; assert struct.calcsize('P') == 8\" >nul 2>&1\r\n"
            "if not errorlevel 1 (\r\n"
            "    set \"QT_PYTHON_LAUNCHER=py\"\r\n"
            "    set \"QT_PYTHON_ARGS=-%~1\"\r\n"
            ")\r\n"
            "exit /b 0\r\n"
        ),
        encoding="utf-8",
    )
    write_text(
        stage_dir / "RUN.ps1",
        (
            "Set-Location -LiteralPath $PSScriptRoot\n"
            "if (Get-Command py -ErrorAction SilentlyContinue) {\n"
            "    py -3 .\\main.py\n"
            "} elseif (Get-Command python -ErrorAction SilentlyContinue) {\n"
            "    python .\\main.py\n"
            "} else {\n"
            "    Write-Host 'Python 3.11+ not found in PATH.'\n"
            "    Write-Host 'Install Python and enable Add Python to PATH, then run again.'\n"
            "}\n"
        ),
        encoding="utf-8",
    )
    write_text(
        stage_dir / "start.sh",
        (
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            "cd \"$(dirname \"$0\")\"\n"
            "python3 main.py\n"
        ),
        encoding="utf-8",
    )
    write_text(
        stage_dir / "DEPLOY.txt",
        (
            f"QQOKX server package v{APP_VERSION}\n\n"
            "1. Runtime\n"
            "- Python 3.11+\n"
            "- Windows: run RUN.bat for the workbench, RUN_QT.bat for Roll Terminal QT\n"
            "- RUN_QT.bat automatically installs missing Qt runtime dependencies using the detected Python\n"
            "- Linux server: this GUI app requires a desktop environment\n"
            "- Linux Tk install example: sudo apt-get install -y python3-tk\n"
            "- Optional custom data dir: `python main.py --data-dir D:\\qqokx_data`\n"
            "- Optional environment override: `QQOKX_DATA_DIR=D:\\qqokx_data`\n\n"
            "2. Included\n"
            "- main.py\n"
            "- run_roll_terminal_qt.py\n"
            "- run_roll_terminal_qt_bootstrap.py\n"
            "- start_roll_terminal_qt.bat\n"
            "- start_roll_terminal_qt_silent.vbs\n"
            "- requirements.txt\n"
            "- roll_terminal_qt_requirements.txt\n"
            "- roll_terminal_qt_README.md\n"
            "- 发版协作约定.md\n"
            "- 软件开发指南.md\n"
            "- 线程工作流模板.md\n"
            "- okx_quant/\n"
            "- roll_terminal_qt/\n"
            "- RUN.bat\n"
            "- RUN_QT.bat\n"
            "- RUN.ps1\n"
            "- start.sh\n\n"
            "3. Runtime data layout\n"
            "- Default shared data dir: sibling `qqokx_data/` next to the code folder\n"
            "- Main subfolders: `config/`, `cache/`, `state/`, `logs/`, `reports/`\n"
            "- First launch will bootstrap legacy `.okx_quant_*`, `logs/`, and `reports/` into the shared data dir\n\n"
            "4. Not included\n"
            "- Shared runtime data directory `../qqokx_data/`\n"
            "- tests/\n"
            "- 临时数据目录\n\n"
            "5. Upgrade notes\n"
            "- Preferred: keep the same sibling `qqokx_data/` and replace only the code package\n"
            "- Or copy the whole `qqokx_data/` directory to the new machine / new version\n"
            "- Enhanced live strategy sessions can be resumed from the preserved data directory after restart\n"
        ),
    )

    shutil.make_archive(str(zip_base), "zip", root_dir=dist_root, base_dir=package_name)
    return stage_dir, zip_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build uploadable QQOKX server package")
    parser.add_argument("--version", default=datetime.now().strftime("%Y%m%d_%H%M%S"))
    args = parser.parse_args()

    stage_dir, zip_path = build_package(args.version)
    print(f"STAGE_DIR={stage_dir}")
    print(f"ZIP_PATH={zip_path}")


if __name__ == "__main__":
    main()
