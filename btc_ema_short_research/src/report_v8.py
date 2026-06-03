from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_v8_research_report(
    output_path: Path,
    *,
    config: dict[str, object],
    metadata: dict[str, str],
    comparison: pd.DataFrame,
    split_results: pd.DataFrame,
    robustness: pd.DataFrame,
) -> None:
    baseline = comparison[
        (comparison["schedule_name"] == "v8_a_flat_1_0") & (comparison["cost_scenario_name"] == "base_cost")
    ].iloc[0]
    best = comparison.iloc[0]
    stress_winner = robustness.iloc[0] if not robustness.empty else None

    lines = [
        "# V8 Stress Report",
        "",
        "## 1. Study Goal",
        "",
        "Stress-test the dynamic-risk conclusion by combining multiple risk schedules with tougher fee and slippage assumptions.",
        "",
        "## 2. Fixed Trade Logic",
        "",
        f"- baseline entry gate: `{config['v6_baseline_strategy']}`",
        f"- strong regime definition: `{config['v6_candidate_strategy']}`",
        "- exit rule: EMA21 reclaim on 4H close, execute on next 4H open",
        "- only risk sizing and trading costs vary",
        "",
        "## 3. Data Source",
        "",
        f"- source: `{metadata['data_source']}`",
        f"- data_root: `{metadata['data_root']}`",
        f"- symbol: `{metadata['symbol']}`",
        "",
        "## 4. Full-History Scenario Grid",
        "",
        comparison.to_markdown(index=False),
        "",
        "## 5. Walk-Forward Stress Results",
        "",
        split_results.to_markdown(index=False) if not split_results.empty else "No split rows.",
        "",
        "## 6. Schedule Robustness Summary",
        "",
        robustness.to_markdown(index=False) if not robustness.empty else "No robustness rows.",
        "",
        "## 7. Findings",
        "",
        f"- best full-history cell: {best['strategy_name']}",
        f"- best full-history metrics: total_return={best['total_return']:.2%}, profit_factor={best['profit_factor']:.2f}, max_drawdown={best['max_drawdown']:.2%}",
        f"- flat baseline metrics: total_return={baseline['total_return']:.2%}, profit_factor={baseline['profit_factor']:.2f}, max_drawdown={baseline['max_drawdown']:.2%}",
        f"- full-history return delta vs baseline: {(float(best['total_return']) - float(baseline['total_return'])):.2%}",
        f"- full-history drawdown delta vs baseline: {(float(best['max_drawdown']) - float(baseline['max_drawdown'])):.2%}",
    ]

    if stress_winner is not None:
        lines.extend(
            [
                f"- most robust schedule under stress: {stress_winner['schedule_name']}",
                f"- stress median return: {stress_winner['median_stress_test_return']:.2%}",
                f"- stress median profit factor: {stress_winner['median_stress_test_profit_factor']:.2f}",
            ]
        )

    lines.extend(
        [
            "",
            "## 8. Interpretation",
            "",
            "- The robustness summary ranks schedules across all stressed cost scenarios and chronological test windows.",
            "- A schedule that still wins after costs are raised is much more credible for live deployment than a schedule that only wins under original assumptions.",
            "",
            "## 9. Recommendation",
            "",
            f"Carry forward: `{choose_v8_recommendation(robustness, best['schedule_name'])}`",
            "",
            "## 10. Output Notes",
            "",
            f"- cost_scenario_count: {split_results['cost_scenario_name'].nunique() if not split_results.empty else 0}",
            f"- split_count: {split_results['split_label'].nunique() if not split_results.empty else 0}",
            f"- results_dir: `{config['v8_results_dir']}`",
        ]
    )

    output_path.write_text("\n".join(lines), encoding="utf-8")


def choose_v8_recommendation(robustness: pd.DataFrame, fallback_schedule: str) -> str:
    if robustness.empty:
        return fallback_schedule
    return str(robustness.iloc[0]["schedule_name"])
