from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.btc_market_analyzer import (
    BtcMarketAnalyzerConfig,
    analyze_btc_market_from_client,
    btc_market_analysis_json,
    save_btc_market_analysis,
    send_btc_market_analysis_email,
)
from okx_quant.market_analysis import MarketAnalysisConfig
from okx_quant.okx_client import OkxRestClient


def main() -> None:
    parser = argparse.ArgumentParser(description="Run BTC market analysis for 1H / 4H / 1D.")
    parser.add_argument("--symbol", default="BTC-USDT-SWAP", help="Instrument id to analyze.")
    parser.add_argument(
        "--timeframe",
        action="append",
        dest="timeframes",
        default=[],
        help="Timeframe to analyze. Can be repeated. Defaults to 1H, 4H, 1D.",
    )
    parser.add_argument(
        "--direction-mode",
        choices=("close_to_close", "candle_body"),
        default="close_to_close",
        help="How bullish/bearish probability streaks are defined.",
    )
    parser.add_argument(
        "--send-email",
        action="store_true",
        help="Send the analysis summary using the existing QQOKX email settings.",
    )
    parser.add_argument(
        "--print-json",
        action="store_true",
        help="Print the JSON payload to stdout after saving the report.",
    )
    args = parser.parse_args()

    timeframes = tuple(args.timeframes or ["1H", "4H", "1D"])
    config = BtcMarketAnalyzerConfig(
        timeframes=timeframes,
        probability_config=MarketAnalysisConfig(direction_mode=args.direction_mode),
    )
    client = OkxRestClient()
    analysis = analyze_btc_market_from_client(client, symbol=args.symbol, config=config)
    output_path = save_btc_market_analysis(analysis)
    print(output_path)

    if args.print_json:
        print(btc_market_analysis_json(analysis))

    if args.send_email:
        delivered = send_btc_market_analysis_email(analysis)
        print("email_sent" if delivered else "email_not_sent")


if __name__ == "__main__":
    main()
