from __future__ import annotations

from pathlib import Path

import pandas as pd

from report import eligible_candidates, format_rejection_reason


def write_v2_research_report(
    output_path: Path,
    *,
    config: dict[str, object],
    metadata: dict[str, str],
    daily_frame: pd.DataFrame,
    entry_frame: pd.DataFrame,
    comparison: pd.DataFrame,
    ranked: pd.DataFrame,
    yearly: pd.DataFrame,
    trades: pd.DataFrame,
) -> None:
    eligible = eligible_candidates(comparison)
    selection_pool = ranked[ranked["strategy_name"].isin(eligible["strategy_name"])] if not eligible.empty else ranked
    best = selection_pool.iloc[0]
    low_drawdown_pool = eligible if not eligible.empty else comparison
    low_drawdown = low_drawdown_pool.sort_values(["max_drawdown", "profit_factor"], ascending=[False, False]).iloc[0]
    rejected = comparison[~comparison["strategy_name"].isin(selection_pool["strategy_name"])].copy()

    lines = [
        "# V2 Research Report",
        "",
        "## 1. Study Goal",
        "",
        "Use daily bars to define BTC short direction, then use 4H bars to improve entry timing and compare which 4H trigger is most practical.",
        "",
        "## 2. Data Source",
        "",
        f"- source: `{metadata['data_source']}`",
        f"- data_root: `{metadata['data_root']}`",
        f"- symbol: `{metadata['symbol']}`",
        f"- daily_timeframe: `{config['v2_daily_timeframe']}`",
        f"- entry_timeframe: `{config['v2_entry_timeframe']}`",
        "",
        "## 3. Data Range",
        "",
        f"- daily first bar: {daily_frame['timestamp'].iloc[0].isoformat()}",
        f"- daily last bar: {daily_frame['timestamp'].iloc[-1].isoformat()}",
        f"- daily bar count: {len(daily_frame)}",
        f"- 4H first bar: {entry_frame['timestamp'].iloc[0].isoformat()}",
        f"- 4H last bar: {entry_frame['timestamp'].iloc[-1].isoformat()}",
        f"- 4H bar count: {len(entry_frame)}",
        "",
        "## 4. V2 Design",
        "",
        "- daily_filter_core = daily EMA21 < EMA55 and daily close < EMA55",
        "- daily_filter_volume = daily_filter_core plus daily volume >= daily VOL_MA20",
        "- daily_filter_ema55 = daily close < EMA55 and daily EMA55 slope over 5 bars < 0",
        "- 4H entries still trigger on bar close and execute on next 4H open",
        "- stop and fees keep the original project rules",
        "",
        "## 5. Strategy Summary",
        "",
        ranked.to_markdown(index=False),
        "",
        "## 6. Yearly Performance",
        "",
        yearly.to_markdown(index=False) if not yearly.empty else "No yearly rows.",
        "",
        "## 7. Findings",
        "",
        f"- best overall: {best['strategy_name']}",
        f"- best overall metrics: profit_factor={best['profit_factor']:.2f}, total_return={best['total_return']:.2%}, max_drawdown={best['max_drawdown']:.2%}, trade_count={int(best['trade_count'])}",
        f"- best low drawdown: {low_drawdown['strategy_name']} with max_drawdown {low_drawdown['max_drawdown']:.2%}",
        f"- highest return: {comparison.sort_values('total_return', ascending=False).iloc[0]['strategy_name']}",
        "",
        "## 8. Rejected Or Weak Candidates",
        "",
    ]
    if rejected.empty:
        lines.append("- none")
    else:
        for _, row in rejected.iterrows():
            lines.append(f"- {row['strategy_name']}: {format_v2_rejection_reason(row)}")

    lines.extend(
        [
            "",
            "## 9. Recommendation",
            "",
            f"Primary V2 direction: `{best['strategy_name']}`",
            "",
            "Why this one:",
            f"- It scored highest after penalizing low sample size.",
            f"- It kept profit factor above 1.3 with at least 30 trades: {'yes' if int(best['trade_count']) >= 30 and float(best['profit_factor']) > 1.3 and float(best['average_R']) > 0 else 'no'}",
            f"- It produced a drawdown profile of {best['max_drawdown']:.2%}.",
            "",
            "Operational reading:",
            "- Daily bars define whether BTC is already in a bearish regime.",
            "- 4H bars are only used to time the actual short entry once the daily bias is aligned.",
            "- If this V2 still has modest absolute return, the next step should be improving exits before adding more entry complexity.",
            "",
            "## 10. Output Notes",
            "",
            f"- total trades exported: {len(trades)}",
            f"- results_dir: `{config['v2_results_dir']}`",
        ]
    )

    output_path.write_text("\n".join(lines), encoding="utf-8")


def format_v2_rejection_reason(row: pd.Series) -> str:
    base_reason = format_rejection_reason(row)
    if base_reason != "did not meet continuation criteria":
        return base_reason
    if float(row["profit_factor"]) <= 1.3:
        return f"profit_factor {float(row['profit_factor']):.2f} <= 1.30 continuation threshold"
    return base_reason
