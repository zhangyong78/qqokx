from __future__ import annotations

import argparse
import ctypes
import sys

from okx_quant.app_paths import configure_data_root, data_root


def _enable_windows_dpi_awareness() -> None:
    if sys.platform != "win32":
        return
    try:
        user32 = ctypes.windll.user32
    except Exception:
        return

    # Prefer per-monitor v2 so Windows does not bitmap-scale the whole Tk app.
    for setter in (
        lambda: user32.SetProcessDpiAwarenessContext(ctypes.c_void_p(-4)),
        lambda: ctypes.windll.shcore.SetProcessDpiAwareness(2),
        lambda: user32.SetProcessDPIAware(),
    ):
        try:
            setter()
            return
        except Exception:
            continue


def main() -> None:
    _enable_windows_dpi_awareness()
    parser = argparse.ArgumentParser(description="Run QQOKX desktop app")
    parser.add_argument(
        "--app",
        choices=("workbench", "arbitrage-fast"),
        default="workbench",
        help="Application surface to launch",
    )
    parser.add_argument("--data-dir", help="Path to the shared QQOKX data directory")
    args = parser.parse_args()
    if args.data_dir:
        configure_data_root(args.data_dir)
    else:
        data_root()

    if args.app == "arbitrage-fast":
        from okx_quant.arbitrage_fast_app import run_app
    else:
        from okx_quant.ui import run_app

    run_app()


if __name__ == "__main__":
    main()
