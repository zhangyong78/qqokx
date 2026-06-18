from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.last_hour_daily_relationship import run_last_hour_daily_relationship_research


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Research the 7:00-8:00 final hourly bar versus the next 8:00 daily close")
    parser.add_argument("--hourly", help="Optional 1H csv/parquet path. If omitted, load from local candle cache.")
    parser.add_argument("--inst-id", default="BTC-USDT-SWAP", help="Instrument id used for local candle cache loading")
    parser.add_argument("--bar", default="1H", help="Bar name used for local candle cache loading")
    parser.add_argument(
        "--output-dir",
        default=str(ROOT / "reports" / "last_hour_daily_relationship_research"),
        help="Directory for features_last_hour_daily.csv / condition_summary.csv / research_report.md",
    )
    parser.add_argument("--symbol", help="Override symbol in outputs")
    parser.add_argument("--timezone-offset-hours", type=int, default=8, help="Timezone offset applied before session bucketing")
    parser.add_argument("--session-close-hour", type=int, default=8, help="Daily session close hour in local time")
    parser.add_argument("--min-samples-report", type=int, default=30, help="Minimum sample count for highlighted conditions in report")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_last_hour_daily_relationship_research(
        hourly_path=args.hourly,
        output_dir=args.output_dir,
        inst_id=args.inst_id,
        bar=args.bar,
        symbol=args.symbol,
        timezone_offset_hours=args.timezone_offset_hours,
        session_close_hour=args.session_close_hour,
        min_samples_for_report=args.min_samples_report,
    )
    print(f"features rows: {len(result.features)}")
    print(f"summary rows: {len(result.summary)}")
    print(f"output dir: {result.output_dir}")


if __name__ == "__main__":
    main()
