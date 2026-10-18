from __future__ import annotations

import importlib
import importlib.util
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
REQUIREMENTS = ROOT / "requirements.txt"
ENTRYPOINT = ROOT / "main.py"


def _dependencies_ready() -> bool:
    return all(importlib.util.find_spec(name) is not None for name in ("numpy", "pandas", "websockets"))


def main() -> int:
    if sys.version_info < (3, 11):
        print("[ERROR] Python 3.11 or newer is required.", file=sys.stderr)
        return 1
    if not ENTRYPOINT.is_file():
        print(f"[ERROR] Workbench entrypoint was not found: {ENTRYPOINT}", file=sys.stderr)
        return 1
    if not _dependencies_ready():
        print(f"[QQOKX] Python: {sys.executable}", flush=True)
        print("[QQOKX] Runtime dependencies are missing; installing requirements...", flush=True)
        if not REQUIREMENTS.is_file():
            print(f"[ERROR] Requirements file was not found: {REQUIREMENTS}", file=sys.stderr)
            return 1
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "-r", str(REQUIREMENTS)],
            cwd=str(ROOT),
            check=False,
        )
        if result.returncode != 0:
            print(
                "[ERROR] Dependency installation failed. Check the network connection and pip output above.",
                file=sys.stderr,
            )
            return result.returncode
        importlib.invalidate_caches()
        if not _dependencies_ready():
            print("[ERROR] Dependencies are still unavailable after installation.", file=sys.stderr)
            return 1
    os.execv(sys.executable, [sys.executable, str(ENTRYPOINT), *sys.argv[1:]])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
