from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.analysis_email_validation import (
    build_email_validation_report_payload,
    load_email_analysis_records,
    save_email_validation_report,
    validate_email_analysis_records,
)
from okx_quant.okx_client import OkxRestClient


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate historical multi-coin analysis emails against later market moves.")
    parser.add_argument(
        "--symbol",
        action="append",
        dest="symbols",
        default=[],
        help="Only validate selected symbols. Can be repeated, e.g. BTC-USDT-SWAP.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Only validate the latest N archived emails. Use 0 for all archives.",
    )
    parser.add_argument(
        "--window-hours",
        nargs="+",
        type=int,
        default=[4, 12, 24, 72],
        help="Replay review windows in hours. Defaults to 4 12 24 72.",
    )
    args = parser.parse_args()

    windows_hours = tuple(hours for hours in args.window_hours if int(hours) > 0)
    records = load_email_analysis_records(symbols=args.symbols, limit=args.limit)
    if not records:
        raise SystemExit("No archived multi-coin email analyses found.")

    client = OkxRestClient()
    results = validate_email_analysis_records(records, client=client, windows_hours=windows_hours)
    payload = build_email_validation_report_payload(results, windows_hours=windows_hours)
    saved = save_email_validation_report(payload)
    print(saved["md"])


if __name__ == "__main__":
    main()
