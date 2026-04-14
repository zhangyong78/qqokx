from __future__ import annotations

import argparse
import shutil
import sys
from datetime import datetime
from pathlib import Path

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

    for file_name in ("main.py", "pyproject.toml", "requirements.txt", "README.md", "软件开发指南.md", "线程工作流模板.md"):
        shutil.copy2(project_root / file_name, stage_dir / file_name)
    shutil.copytree(
        project_root / "okx_quant",
        stage_dir / "okx_quant",
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
            "- Windows server: run RUN.bat or `python main.py`\n"
            "- Linux server: this GUI app requires a desktop environment\n"
            "- Linux Tk install example: sudo apt-get install -y python3-tk\n\n"
            "2. Included\n"
            "- main.py\n"
            "- 软件开发指南.md\n"
            "- 线程工作流模板.md\n"
            "- okx_quant/\n"
            "- RUN.bat\n"
            "- RUN.ps1\n"
            "- start.sh\n\n"
            "3. Not included\n"
            "- .okx_quant_credentials.json\n"
            "- .okx_quant_settings.json\n"
            "- .okx_quant_backtest_history.json\n"
            "- .okx_quant_candle_cache/\n"
            "- reports/\n"
            "- tests/\n\n"
            "4. First run\n"
            "- Re-enter API keys, email settings and local runtime settings on the server.\n"
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
