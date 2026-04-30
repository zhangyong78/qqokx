from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.market_analysis import (
    MarketAnalysisConfig,
    MarketAnalysisReport,
    build_market_analysis_report_from_client,
    save_market_analysis_report,
)
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Run BTC market-analysis factor validation on OKX history.")
    parser.add_argument(
        "--inst-id",
        action="append",
        dest="inst_ids",
        default=[],
        help="Instrument id to validate. Can be repeated. Defaults to BTC-USDT-SWAP and BTC-USDT.",
    )
    parser.add_argument(
        "--bar",
        action="append",
        dest="bars",
        default=[],
        help="Bar interval to validate. Can be repeated. Defaults to 1D and 1W.",
    )
    parser.add_argument(
        "--direction-mode",
        choices=("close_to_close", "candle_body"),
        default="close_to_close",
        help="How bullish/bearish streaks are defined.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Candle history limit. Use 0 to fetch the full available history.",
    )
    args = parser.parse_args()

    inst_ids = args.inst_ids or ["BTC-USDT-SWAP", "BTC-USDT"]
    bars = args.bars or ["1D", "1W"]
    config = MarketAnalysisConfig(direction_mode=args.direction_mode)
    client = OkxRestClient()
    report_dir = analysis_report_dir_path()
    report_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    sections: list[str] = [
        "# BTC Market Analysis Validation",
        "",
        f"- Generated at (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Direction mode: `{args.direction_mode}`",
        f"- Requested instruments: {', '.join(inst_ids)}",
        f"- Requested bars: {', '.join(bars)}",
        f"- Candle limit: {args.limit}",
        "",
        "## Notes",
        "",
        "- This validation uses OKX-accessible history only.",
        "- If the available start date is later than September 1, 2014, the result is not a full-market replica of the external study.",
        "- Adopted factors here mean 'worth keeping as candidates in this codebase', not 'safe for mechanical trading'.",
        "",
    ]

    for inst_id in inst_ids:
        for bar in bars:
            report = build_market_analysis_report_from_client(
                client,
                inst_id,
                bar=bar,
                limit=args.limit,
                config=config,
            )
            json_path = save_market_analysis_report(report)
            sections.extend(build_report_section(report, json_path))

    summary_path = report_dir / f"market_validation_{args.direction_mode}_{timestamp}.md"
    summary_path.write_text("\n".join(sections).rstrip() + "\n", encoding="utf-8")
    print(summary_path)


def build_report_section(report: MarketAnalysisReport, json_path: Path) -> list[str]:
    adopted = [item for item in report.factor_candidates if item.adopt]
    observed = [item for item in report.factor_candidates if not item.adopt]

    lines = [
        f"## {report.inst_id} {report.timeframe}",
        "",
        f"- Coverage: {format_ts(report.period_start_ts)} to {format_ts(report.period_end_ts)}",
        f"- Candles: {report.candle_count}",
        f"- Baseline bullish probability: {format_pct(report.baseline_bullish_probability)}",
        f"- JSON report: `{json_path}`",
        "",
        "### Adopted factor candidates",
        "",
    ]
    if adopted:
        for item in adopted:
            lines.append(
                "- "
                + f"`{item.key}` | samples={item.sample_count} | probability={format_pct(item.probability)}"
                + f" | edge={format_pct(item.edge_vs_reference)} | {item.rationale}"
            )
    else:
        lines.append("- None")

    lines.extend(["", "### Observe only", ""])
    if observed:
        for item in observed:
            lines.append(
                "- "
                + f"`{item.key}` | samples={item.sample_count} | probability={format_pct(item.probability)}"
                + f" | edge={format_pct(item.edge_vs_reference)} | {item.rationale}"
            )
    else:
        lines.append("- None")

    lines.extend(["", "### Key streak stats", ""])
    for item in report.streak_stats:
        lines.append(
            "- "
            + f"{item.streak_label} | samples={item.sample_count} | continuation={format_pct(item.continuation_probability)}"
            + f" | edge={format_pct(item.edge_vs_baseline)} | {item.insight}"
        )

    lines.extend(["", "### Pullback stats", ""])
    for item in report.pullback_stats:
        lines.append(
            "- "
            + f"{item.bucket} | samples={item.sample_count} | down_3d={format_pct(item.continue_down_3d_probability)}"
            + f" | down_5d={format_pct(item.continue_down_5d_probability)} | {item.insight}"
        )

    lines.extend(["", "### Support break stats", ""])
    for item in report.support_break_stats:
        lines.append(
            "- "
            + f"{item.support_status} | samples={item.sample_count} | down_5d={format_pct(item.continue_down_5d_probability)}"
            + f" | {item.insight}"
        )

    lines.extend(["", "### Volatility regime stats", ""])
    for item in report.volatility_stats:
        lines.append(
            "- "
            + f"{item.regime} | samples={item.sample_count} | continuation={format_pct(item.continuation_probability)}"
            + f" | {item.insight}"
        )

    lines.extend(["", "### Active factors now", ""])
    if report.active_factors:
        for item in report.active_factors:
            lines.append(
                "- "
                + f"`{item.key}` | bias={item.direction_bias} | score={format_pct(item.score)} | {item.reason}"
            )
    else:
        lines.append("- None")

    lines.extend(["", "### Notes", ""])
    for note in report.notes:
        lines.append(f"- {note}")
    lines.append("")
    return lines


def format_ts(ts: int | None) -> str:
    if ts is None:
        return "-"
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def format_pct(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return f"{(value * Decimal('100')):.2f}%"


if __name__ == "__main__":
    main()
