from __future__ import annotations

from pathlib import Path

import pandas as pd

from report import eligible_candidates, format_rejection_reason


def write_v5_research_report(
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
    baseline = comparison[comparison["strategy_name"] == "v5_a_baseline"].iloc[0]
    rejected = comparison[~comparison["strategy_name"].isin(selection_pool["strategy_name"])].copy()

    lines = [
        "# V5 Combo Filter Report",
        "",
        "## 1. Study Goal",
        "",
        "Formalize the strongest V4 single filters and test whether combining them improves robustness without killing sample size.",
        "",
        "## 2. Fixed Trade Logic",
        "",
        "- entry trigger: 4H dual-bear pullback with 4H volume confirmation",
        "- exit rule: EMA21 reclaim on 4H close, execute on next 4H open",
        "- only the daily regime gate changes",
        "",
        "## 3. Data Source",
        "",
        f"- source: `{metadata['data_source']}`",
        f"- data_root: `{metadata['data_root']}`",
        f"- symbol: `{metadata['symbol']}`",
        f"- daily_timeframe: `{config['v2_daily_timeframe']}`",
        f"- entry_timeframe: `{config['v2_entry_timeframe']}`",
        "",
        "## 4. Daily Gates Compared",
        "",
        "- `v5_a_baseline`: baseline V2 daily regime",
        "- `v5_b_close_below_ema21`: best V4 return-oriented single filter",
        "- `v5_c_rsi_rebound`: best V4 quality-oriented single filter",
        "- `v5_d_close_and_rsi`: close below EMA21 plus RSI rebound",
        "- `v5_e_close_rsi_ema55`: close below EMA21 plus RSI rebound plus EMA55 slope down",
        "- `v5_f_close_breakdown`: close below EMA21 plus breakdown-and-slope state",
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
        f"- best combo gate: {best['strategy_name']}",
        f"- best metrics: profit_factor={best['profit_factor']:.2f}, total_return={best['total_return']:.2%}, max_drawdown={best['max_drawdown']:.2%}, trade_count={int(best['trade_count'])}, average_R={best['average_R']:.2f}",
        f"- baseline metrics: profit_factor={baseline['profit_factor']:.2f}, total_return={baseline['total_return']:.2%}, max_drawdown={baseline['max_drawdown']:.2%}, trade_count={int(baseline['trade_count'])}, average_R={baseline['average_R']:.2f}",
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
            lines.append(f"- {row['strategy_name']}: {format_v5_rejection_reason(row)}")

    lines.extend(
        [
            "",
            "## 10. Recommendation",
            "",
            f"Best daily combo gate to carry forward: `{best['strategy_name']}`",
            "",
            "Interpretation:",
            "- If the combo beats both strong single filters, the environment edge likely comes from conditional context rather than one variable alone.",
            "- If the combo loses to singles, the regime logic is probably better kept simple.",
            "- If the combo survives with around 30 or more trades and better PF, it is a credible V6 foundation.",
            "",
            "## 11. Output Notes",
            "",
            f"- total trades exported: {len(trades)}",
            f"- results_dir: `{config['v5_results_dir']}`",
        ]
    )

    output_path.write_text("\n".join(lines), encoding="utf-8")


def format_v5_rejection_reason(row: pd.Series) -> str:
    base_reason = format_rejection_reason(row)
    if base_reason != "did not meet continuation criteria":
        return base_reason
    if float(row["profit_factor"]) <= 1.3:
        return f"profit_factor {float(row['profit_factor']):.2f} <= 1.30 continuation threshold"
    return base_reason
