from __future__ import annotations

from pathlib import Path

import pandas as pd

from report import eligible_candidates, format_rejection_reason


def write_v4_research_report(
    output_path: Path,
    *,
    config: dict[str, object],
    metadata: dict[str, str],
    daily_frame: pd.DataFrame,
    entry_frame: pd.DataFrame,
    comparison: pd.DataFrame,
    yearly: pd.DataFrame,
    trades: pd.DataFrame,
) -> None:
    eligible = eligible_candidates(comparison)
    selection_pool = comparison[comparison["strategy_name"].isin(eligible["strategy_name"])] if not eligible.empty else comparison
    best = selection_pool.iloc[0]
    baseline = comparison[comparison["strategy_name"] == "v4_a_baseline"].iloc[0]
    rejected = comparison[~comparison["strategy_name"].isin(selection_pool["strategy_name"])].copy()

    lines = [
        "# V4 Regime Filter Report",
        "",
        "## 1. Study Goal",
        "",
        "Keep the best V2 entry and V3 baseline exit fixed, then compare which daily environment filters improve or damage the 1D+4H short framework.",
        "",
        "## 2. Fixed Trade Logic",
        "",
        "- entry trigger: 4H dual-bear pullback with 4H volume confirmation",
        "- exit rule: EMA21 reclaim on 4H close, execute on next 4H open",
        "- the only thing changing in this study is the daily regime gate",
        "",
        "## 3. Data Source",
        "",
        f"- source: `{metadata['data_source']}`",
        f"- data_root: `{metadata['data_root']}`",
        f"- symbol: `{metadata['symbol']}`",
        f"- daily_timeframe: `{config['v2_daily_timeframe']}`",
        f"- entry_timeframe: `{config['v2_entry_timeframe']}`",
        "",
        "## 4. Daily Environment Filters Compared",
        "",
        "- `v4_a_baseline`: daily core bear regime plus daily volume >= VOL_MA20",
        "- `v4_b_close_below_ema21`: baseline plus daily close < EMA21",
        "- `v4_c_ema55_down`: baseline plus daily EMA55 slope < 0",
        "- `v4_d_rsi_rebound`: baseline plus daily RSI inside rebound window",
        "- `v4_e_atr_expansion`: baseline plus daily ATR above rolling 100-bar median",
        "- `v4_f_volume_strong`: baseline plus stronger daily volume expansion",
        "- `v4_g_trend_gap_strong`: baseline plus stronger EMA21/EMA55 separation",
        "- `v4_h_breakdown_and_slope`: baseline plus daily close < previous low and EMA55 slope < 0",
        "",
        "## 5. Data Range",
        "",
        f"- daily first bar: {daily_frame['timestamp'].iloc[0].isoformat()}",
        f"- daily last bar: {daily_frame['timestamp'].iloc[-1].isoformat()}",
        f"- 4H first bar: {entry_frame['timestamp'].iloc[0].isoformat()}",
        f"- 4H last bar: {entry_frame['timestamp'].iloc[-1].isoformat()}",
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
        f"- best regime filter: {best['strategy_name']}",
        f"- best metrics: profit_factor={best['profit_factor']:.2f}, total_return={best['total_return']:.2%}, max_drawdown={best['max_drawdown']:.2%}, trade_count={int(best['trade_count'])}",
        f"- baseline metrics: profit_factor={baseline['profit_factor']:.2f}, total_return={baseline['total_return']:.2%}, max_drawdown={baseline['max_drawdown']:.2%}, trade_count={int(baseline['trade_count'])}",
        f"- return delta vs baseline: {(float(best['total_return']) - float(baseline['total_return'])):.2%}",
        f"- drawdown delta vs baseline: {(float(best['max_drawdown']) - float(baseline['max_drawdown'])):.2%}",
        "",
        "## 9. Rejected Or Weak Filters",
        "",
    ]
    if rejected.empty:
        lines.append("- none")
    else:
        for _, row in rejected.iterrows():
            lines.append(f"- {row['strategy_name']}: {format_v4_rejection_reason(row)}")

    lines.extend(
        [
            "",
            "## 10. Recommendation",
            "",
            f"Best daily regime gate to carry forward: `{best['strategy_name']}`",
            "",
            "Interpretation:",
            "- If the best filter meaningfully beats baseline, daily environment selection is the current edge amplifier.",
            "- If most stricter filters reduce quality, the baseline regime is already close to the useful boundary and over-filtering hurts sample quality.",
            "- If only one or two filters survive while others fail, we should focus V5 on refining just those surviving market states.",
            "",
            "## 11. Output Notes",
            "",
            f"- total trades exported: {len(trades)}",
            f"- results_dir: `{config['v4_results_dir']}`",
        ]
    )

    output_path.write_text("\n".join(lines), encoding="utf-8")


def format_v4_rejection_reason(row: pd.Series) -> str:
    base_reason = format_rejection_reason(row)
    if base_reason != "did not meet continuation criteria":
        return base_reason
    if float(row["profit_factor"]) <= 1.3:
        return f"profit_factor {float(row['profit_factor']):.2f} <= 1.30 continuation threshold"
    return base_reason
