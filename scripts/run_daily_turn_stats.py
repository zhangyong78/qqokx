from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.pipeline import run_daily_turning_point_research


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily turning-point structure statistics")
    parser.add_argument("--hourly", required=True, help="1H candle file path (.csv or .parquet)")
    parser.add_argument("--daily", help="1D candle file path (.csv or .parquet)")
    parser.add_argument("--output-dir", required=True, help="Directory for samples.csv / summary.csv / heatmap_source.csv")
    parser.add_argument("--symbol", help="Override symbol name in output")
    parser.add_argument("--close-mode", choices=["utc+8", "utc+0", "both"], default="both")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    close_modes = ["utc+8", "utc+0"] if args.close_mode == "both" else [args.close_mode]
    for close_mode in close_modes:
        output_dir = args.output_dir if len(close_modes) == 1 else f"{args.output_dir}/{close_mode}"
        run_daily_turning_point_research(
            hourly_path=args.hourly,
            daily_path=args.daily,
            output_dir=output_dir,
            symbol=args.symbol,
            close_mode=close_mode,
        )


if __name__ == "__main__":
    main()
