from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.btc_market_analyzer import BtcMarketAnalyzerConfig
from okx_quant.market_analysis import MarketAnalysisConfig
from okx_quant.multi_coin_market_digest import (
    DEFAULT_DIGEST_SYMBOLS,
    analyze_multi_coin_market,
    multi_coin_market_digest_json,
    save_multi_coin_market_digest,
    send_multi_coin_market_email,
)
from okx_quant.okx_client import OkxRestClient


def main() -> None:
    parser = argparse.ArgumentParser(description="Run multi-coin market digest for 1H / 4H / 1D.")
    parser.add_argument(
        "--symbol",
        action="append",
        dest="symbols",
        default=[],
        help="Instrument id to analyze. Can be repeated. Defaults to BTC/ETH/BNB/SOL/DOGE swaps.",
    )
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
    parser.add_argument("--send-email", action="store_true", help="Send the combined digest email.")
    parser.add_argument("--print-json", action="store_true", help="Print the JSON payload to stdout after saving the report.")
    args = parser.parse_args()

    symbols = tuple(args.symbols or DEFAULT_DIGEST_SYMBOLS)
    timeframes = tuple(args.timeframes or ["1H", "4H", "1D"])
    config = BtcMarketAnalyzerConfig(
        timeframes=timeframes,
        probability_config=MarketAnalysisConfig(direction_mode=args.direction_mode),
    )
    client = OkxRestClient()
    digest = analyze_multi_coin_market(client, symbols=symbols, config=config)
    output_path = save_multi_coin_market_digest(digest)
    print(output_path)

    if args.print_json:
        print(multi_coin_market_digest_json(digest))

    if args.send_email:
        delivered = send_multi_coin_market_email(digest, report_path=output_path)
        print("email_sent" if delivered else "email_not_sent")


if __name__ == "__main__":
    main()
