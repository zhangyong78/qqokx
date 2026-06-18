from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.last_hour_daily_relationship_phase2 import run_phase2_research


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase2 research for the 7:00-8:00 final hourly candle")
    parser.add_argument("--hourly", help="Optional 1H csv/parquet path. If omitted, load from local candle cache.")
    parser.add_argument("--inst-id", default="BTC-USDT-SWAP", help="Instrument id used for local candle cache loading")
    parser.add_argument("--bar", default="1H", help="Bar name used for local candle cache loading")
    parser.add_argument(
        "--output-dir",
        default=str(ROOT / "reports" / "last_hour_daily_relationship_research"),
        help="Directory for phase2 output files",
    )
    parser.add_argument("--symbol", help="Override symbol in outputs")
    parser.add_argument("--timezone-offset-hours", type=int, default=8)
    parser.add_argument("--session-close-hour", type=int, default=8)
    parser.add_argument("--cost-r", type=float, default=0.05, help="Round-trip cost measured in R units")
    parser.add_argument("--min-risk-pct", type=float, default=0.001, help="Risk-filter minimum stop distance as pct of entry")
    parser.add_argument("--min-samples", type=int, default=30, help="Minimum samples required for conclusions")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_phase2_research(
        hourly_path=args.hourly,
        output_dir=args.output_dir,
        inst_id=args.inst_id,
        bar=args.bar,
        symbol=args.symbol,
        timezone_offset_hours=args.timezone_offset_hours,
        session_close_hour=args.session_close_hour,
        cost_r=args.cost_r,
        min_risk_pct=args.min_risk_pct,
        min_samples=args.min_samples,
    )
    print(f"phase2 features rows: {len(result.features)}")
    print(f"phase2 summary rows: {len(result.summary)}")
    print(f"output dir: {result.output_dir}")


if __name__ == "__main__":
    main()
