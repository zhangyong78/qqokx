from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
REQUIREMENTS = ROOT / "roll_terminal_qt_requirements.txt"
ENTRYPOINT = ROOT / "run_roll_terminal_qt.py"


def _dependencies_ready() -> bool:
    return (
        importlib.util.find_spec("PySide6") is not None
        and importlib.util.find_spec("websockets") is not None
    )


def _install_dependencies() -> int:
    print("[QQOKX] Qt dependencies are missing; installing them now...")
    print(f"[QQOKX] Python: {sys.executable}")
    print(f"[QQOKX] Requirements: {REQUIREMENTS}")
    if not REQUIREMENTS.exists():
        print(f"[ERROR] Requirements file was not found: {REQUIREMENTS}", file=sys.stderr)
        return 1

    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-r", str(REQUIREMENTS)],
        cwd=str(ROOT),
        check=False,
    )
    if result.returncode != 0:
        print(
            "[ERROR] Dependency installation failed. "
            "Check the network connection or install the requirements manually.",
            file=sys.stderr,
        )
        return result.returncode or 1
    return 0


def main() -> int:
    if sys.version_info < (3, 11):
        print("[ERROR] Python 3.11 or newer is required.", file=sys.stderr)
        return 1
    if not _dependencies_ready():
        result = _install_dependencies()
        if result != 0:
            return result
        if not _dependencies_ready():
            print("[ERROR] Dependencies are still unavailable after installation.", file=sys.stderr)
            return 1

    if not ENTRYPOINT.exists():
        print(f"[ERROR] Qt entrypoint was not found: {ENTRYPOINT}", file=sys.stderr)
        return 1
    os.execv(sys.executable, [sys.executable, str(ENTRYPOINT), *sys.argv[1:]])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
