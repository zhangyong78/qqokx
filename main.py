from __future__ import annotations

import argparse

from okx_quant.app_paths import configure_data_root, data_root


def main() -> None:
    parser = argparse.ArgumentParser(description="Run QQOKX desktop app")
    parser.add_argument("--data-dir", help="Path to the shared QQOKX data directory")
    args = parser.parse_args()
    if args.data_dir:
        configure_data_root(args.data_dir)
    else:
        data_root()

    from okx_quant.ui import run_app

    run_app()


if __name__ == "__main__":
    main()
