from __future__ import annotations

from pathlib import Path

import pandas as pd

from report import eligible_candidates, format_rejection_reason


def write_v3_research_report(
    output_path: Path,
    *,
    config: dict[str, object],
    metadata: dict[str, str],
    daily_frame: pd.DataFrame,
    entry_frame: pd.DataFrame,
    comparison: pd.DataFrame,
    yearly: pd.DataFrame,
    trades: pd.DataFrame,
    entry_strategy_name: str,
) -> None:
    eligible = eligible_candidates(comparison)
    selection_pool = comparison[comparison["strategy_name"].isin(eligible["strategy_name"])] if not eligible.empty else comparison
    best = selection_pool.iloc[0]
    baseline = comparison[comparison["strategy_name"] == "v3_exit_ema21_reclaim"].iloc[0]
    rejected = comparison[~comparison["strategy_name"].isin(selection_pool["strategy_name"])].copy()

    lines = [
        "# V3 Exit Study Report",
        "",
        "## 1. Study Goal",
        "",
        "Keep the best V2 entry framework fixed and compare whether changing only the exit rule can improve expectancy, drawdown, and yearly stability.",
        "",
        "## 2. Fixed Entry Framework",
        "",
        f"- entry_strategy: `{entry_strategy_name}`",
        "- daily regime: daily EMA21 < EMA55, daily close < EMA55, daily volume >= daily VOL_MA20",
        "- entry trigger: 4H dual-bear pullback with 4H volume confirmation",
        "",
        "## 3. Data Source",
        "",
        f"- source: `{metadata['data_source']}`",
        f"- data_root: `{metadata['data_root']}`",
        f"- symbol: `{metadata['symbol']}`",
        f"- daily_timeframe: `{config['v2_daily_timeframe']}`",
        f"- entry_timeframe: `{config['v2_entry_timeframe']}`",
        "",
        "## 4. Data Range",
        "",
        f"- daily first bar: {daily_frame['timestamp'].iloc[0].isoformat()}",
        f"- daily last bar: {daily_frame['timestamp'].iloc[-1].isoformat()}",
        f"- 4H first bar: {entry_frame['timestamp'].iloc[0].isoformat()}",
        f"- 4H last bar: {entry_frame['timestamp'].iloc[-1].isoformat()}",
        "",
        "## 5. Exit Rules Compared",
        "",
        "- `v3_exit_ema21_reclaim`: baseline V2 exit",
        "- `v3_exit_fixed_1_5R`: fixed 1.5R take profit",
        "- `v3_exit_fixed_2R`: fixed 2R take profit",
        "- `v3_exit_ema21_or_2R`: first of EMA21 reclaim or 2R target",
        "- `v3_exit_atr_trail_2ATR`: 2ATR trailing stop",
        "- `v3_exit_atr_trail_1_5ATR_or_2R`: first of 1.5ATR trail or 2R target",
        "",
        "## 6. Strategy Summary",
        "",
        comparison.to_markdown(index=False),
        "",
        "## 7. Yearly Performance",
        "",
        yearly.to_markdown(index=False) if not yearly.empty else "No yearly rows.",
        "",
        "## 8. Findings",
        "",
        f"- best overall exit: {best['strategy_name']}",
        f"- best metrics: profit_factor={best['profit_factor']:.2f}, total_return={best['total_return']:.2%}, max_drawdown={best['max_drawdown']:.2%}, trade_count={int(best['trade_count'])}, average_R={best['average_R']:.2f}",
        f"- baseline metrics: profit_factor={baseline['profit_factor']:.2f}, total_return={baseline['total_return']:.2%}, max_drawdown={baseline['max_drawdown']:.2%}, trade_count={int(baseline['trade_count'])}, average_R={baseline['average_R']:.2f}",
        f"- return delta vs baseline: {(float(best['total_return']) - float(baseline['total_return'])):.2%}",
        f"- drawdown delta vs baseline: {(float(best['max_drawdown']) - float(baseline['max_drawdown'])):.2%}",
        "",
        "## 9. Rejected Or Weak Exit Rules",
        "",
    ]
    if rejected.empty:
        lines.append("- none")
    else:
        for _, row in rejected.iterrows():
            lines.append(f"- {row['strategy_name']}: {format_v3_rejection_reason(row)}")

    lines.extend(
        [
            "",
            "## 10. Recommendation",
            "",
            f"Best exit rule to carry forward: `{best['strategy_name']}`",
            "",
            "Interpretation:",
            "- If the best rule clearly beats baseline, the bottleneck was largely exit design rather than entry quality.",
            "- If the best rule only marginally improves results, then both entry and exit are already close to the ceiling of this setup.",
            "- If no rule is eligible, this 1D + 4H short framework likely needs a different regime filter rather than more exit polishing.",
            "",
            "## 11. Output Notes",
            "",
            f"- total trades exported: {len(trades)}",
            f"- results_dir: `{config['v3_results_dir']}`",
        ]
    )

    output_path.write_text("\n".join(lines), encoding="utf-8")


def format_v3_rejection_reason(row: pd.Series) -> str:
    base_reason = format_rejection_reason(row)
    if base_reason != "did not meet continuation criteria":
        return base_reason
    if float(row["profit_factor"]) <= 1.3:
        return f"profit_factor {float(row['profit_factor']):.2f} <= 1.30 continuation threshold"
    return base_reason
