from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_v7_research_report(
    output_path: Path,
    *,
    config: dict[str, object],
    metadata: dict[str, str],
    comparison: pd.DataFrame,
    split_results: pd.DataFrame,
    robustness: pd.DataFrame,
) -> None:
    baseline = comparison[comparison["strategy_name"] == "v7_a_flat_1_0"].iloc[0]
    best = comparison.iloc[0]

    lines = [
        "# V7 Dynamic Risk Report",
        "",
        "## 1. Study Goal",
        "",
        "Keep the same baseline trade sequence, but scale risk up or down depending on whether the trade appears inside the strongest V5 combo regime.",
        "",
        "## 2. Fixed Trade Logic",
        "",
        f"- baseline entry gate: `{config['v6_baseline_strategy']}`",
        f"- strong regime definition: `{config['v6_candidate_strategy']}`",
        "- exit rule: EMA21 reclaim on 4H close, execute on next 4H open",
        "- occupancy is unchanged; only per-trade risk size changes",
        "",
        "## 3. Data Source",
        "",
        f"- source: `{metadata['data_source']}`",
        f"- data_root: `{metadata['data_root']}`",
        f"- symbol: `{metadata['symbol']}`",
        "",
        "## 4. Full-History Comparison",
        "",
        comparison.to_markdown(index=False),
        "",
        "## 5. Walk-Forward Split Results",
        "",
        split_results.to_markdown(index=False) if not split_results.empty else "No split rows.",
        "",
        "## 6. Test Robustness Summary",
        "",
        robustness.to_markdown(index=False) if not robustness.empty else "No robustness rows.",
        "",
        "## 7. Findings",
        "",
        f"- best risk schedule: {best['strategy_name']}",
        f"- best metrics: total_return={best['total_return']:.2%}, profit_factor={best['profit_factor']:.2f}, max_drawdown={best['max_drawdown']:.2%}, average_R={best['average_R']:.2f}",
        f"- flat baseline metrics: total_return={baseline['total_return']:.2%}, profit_factor={baseline['profit_factor']:.2f}, max_drawdown={baseline['max_drawdown']:.2%}, average_R={baseline['average_R']:.2f}",
        f"- return delta vs flat baseline: {(float(best['total_return']) - float(baseline['total_return'])):.2%}",
        f"- drawdown delta vs flat baseline: {(float(best['max_drawdown']) - float(baseline['max_drawdown'])):.2%}",
        "",
        "## 8. Interpretation",
        "",
        "- This study tests capital allocation, not a new signal set.",
        "- Strong-regime trades are the ones that also satisfy the best V5 combo gate.",
        "- If dynamic sizing improves both full-history and walk-forward quality, capital allocation becomes the next practical edge lever.",
        "",
        "## 9. Recommendation",
        "",
        f"Carry forward: `{choose_v7_recommendation(robustness, best['strategy_name'])}`",
        "",
        "## 10. Output Notes",
        "",
        f"- split_count: {len(split_results['split_label'].unique()) if not split_results.empty else 0}",
        f"- results_dir: `{config['v7_results_dir']}`",
    ]

    output_path.write_text("\n".join(lines), encoding="utf-8")


def choose_v7_recommendation(robustness: pd.DataFrame, fallback_name: str) -> str:
    if robustness.empty:
        return fallback_name
    best = robustness.sort_values(
        ["median_test_profit_factor", "median_test_return"],
        ascending=False,
    ).iloc[0]
    return str(best["strategy_name"])
