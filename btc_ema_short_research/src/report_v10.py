from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_v10_research_report(
    output_path: Path,
    *,
    config: dict[str, object],
    metadata: dict[str, str],
    comparison: pd.DataFrame,
    monthly_summary: pd.DataFrame,
    guardrail_summary: pd.DataFrame,
) -> None:
    winner = comparison.iloc[0]
    base_reference = select_row(comparison, "v8_d_strong_1_5_weak_0_5", "v10_a_no_stop")
    flat_reference = select_row(comparison, "v8_a_flat_1_0", "v10_a_no_stop")
    deployable = choose_v10_recommendation(comparison, monthly_summary, guardrail_summary)
    deployable_row = comparison[comparison["strategy_name"] == deployable].iloc[0]

    lines = [
        "# V10 Monthly Guardrail Report",
        "",
        "## 1. Study Goal",
        "",
        "Test whether the current harsh-cost favorite still holds up after adding a more realistic monthly stop-and-stand-down rule.",
        "",
        "## 2. Fixed Trade Logic",
        "",
        f"- baseline entry gate: `{config['v6_baseline_strategy']}`",
        f"- strong regime definition: `{config['v6_candidate_strategy']}`",
        f"- cost scenario: `{config['v10_cost_scenario']}`",
        "- exit rule: EMA21 reclaim on 4H close, execute on next 4H open",
        "- only the risk schedule and monthly stand-down rule vary",
        "",
        "## 3. Data Source",
        "",
        f"- source: `{metadata['data_source']}`",
        f"- data_root: `{metadata['data_root']}`",
        f"- symbol: `{metadata['symbol']}`",
        f"- timeframes: `{metadata['timeframe']}`",
        "",
        "## 4. Full-History Comparison",
        "",
        comparison.to_markdown(index=False),
        "",
        "## 5. Monthly Outcome Summary",
        "",
        monthly_summary.to_markdown(index=False) if not monthly_summary.empty else "No monthly rows.",
        "",
        "## 6. Guardrail Trigger Summary",
        "",
        guardrail_summary.to_markdown(index=False) if not guardrail_summary.empty else "No guardrail rows.",
        "",
        "## 7. Findings",
        "",
        f"- best full-history cell: {winner['strategy_name']} with total_return={winner['total_return']:.2%}, profit_factor={winner['profit_factor']:.2f}, max_drawdown={winner['max_drawdown']:.2%}",
        f"- harsh-cost dynamic base without stop: {base_reference['strategy_name']} with total_return={base_reference['total_return']:.2%}, profit_factor={base_reference['profit_factor']:.2f}, max_drawdown={base_reference['max_drawdown']:.2%}",
        f"- harsh-cost flat baseline without stop: {flat_reference['strategy_name']} with total_return={flat_reference['total_return']:.2%}, profit_factor={flat_reference['profit_factor']:.2f}, max_drawdown={flat_reference['max_drawdown']:.2%}",
        build_recommendation_delta_line(deployable_row, base_reference),
        build_month_guardrail_line(monthly_summary, guardrail_summary, deployable, base_reference["strategy_name"]),
        "",
        "## 8. Recommendation",
        "",
        f"Carry forward: `{deployable}`",
        "",
        "## 9. Interpretation",
        "",
        "- This pass is about live discipline, not signal discovery.",
        "- A monthly stop rule is only worth keeping if it improves the pain profile without giving back too much of the already-thin edge.",
        "",
        "## 10. Output Notes",
        "",
        f"- results_dir: `{config['v10_results_dir']}`",
        f"- tested_schedule_count: {comparison['schedule_name'].nunique() if not comparison.empty else 0}",
        f"- tested_stop_rule_count: {comparison['stop_rule_name'].nunique() if not comparison.empty else 0}",
    ]

    output_path.write_text("\n".join(lines), encoding="utf-8")


def choose_v10_recommendation(
    comparison: pd.DataFrame,
    monthly_summary: pd.DataFrame,
    guardrail_summary: pd.DataFrame,
) -> str:
    merged = comparison.merge(
        monthly_summary[
            ["strategy_name", "worst_period_return", "median_period_return", "positive_period_rate"]
        ].rename(
            columns={
                "worst_period_return": "worst_month_return",
                "median_period_return": "median_month_return",
                "positive_period_rate": "positive_month_rate",
            }
        ),
        on="strategy_name",
        how="left",
    )
    merged = merged.merge(
        guardrail_summary[["strategy_name", "triggered_month_count", "total_skipped_trades"]],
        on="strategy_name",
        how="left",
    )
    merged = merged.sort_values(
        [
            "profit_factor",
            "total_return",
            "worst_month_return",
            "max_drawdown",
            "triggered_month_count",
            "total_skipped_trades",
        ],
        ascending=[False, False, False, False, True, True],
    )
    return str(merged.iloc[0]["strategy_name"])


def select_row(comparison: pd.DataFrame, schedule_name: str, stop_rule_name: str) -> pd.Series:
    return comparison[
        (comparison["schedule_name"] == schedule_name) & (comparison["stop_rule_name"] == stop_rule_name)
    ].iloc[0]


def build_recommendation_delta_line(deployable_row: pd.Series, base_reference: pd.Series) -> str:
    return (
        f"- deployable candidate vs dynamic no-stop: return_delta={(float(deployable_row['total_return']) - float(base_reference['total_return'])):.2%}, "
        f"pf_delta={(float(deployable_row['profit_factor']) - float(base_reference['profit_factor'])):.2f}, "
        f"drawdown_delta={(float(deployable_row['max_drawdown']) - float(base_reference['max_drawdown'])):.2%}"
    )


def build_month_guardrail_line(
    monthly_summary: pd.DataFrame,
    guardrail_summary: pd.DataFrame,
    deployable_name: str,
    base_name: str,
) -> str:
    deployable_month = monthly_summary[monthly_summary["strategy_name"] == deployable_name].iloc[0]
    base_month = monthly_summary[monthly_summary["strategy_name"] == base_name].iloc[0]
    deployable_guard = guardrail_summary[guardrail_summary["strategy_name"] == deployable_name].iloc[0]
    base_guard = guardrail_summary[guardrail_summary["strategy_name"] == base_name].iloc[0]
    return (
        f"- monthly pain profile: worst_month deployable={deployable_month['worst_period_return']:.2%} vs base={base_month['worst_period_return']:.2%}; "
        f"triggered_months deployable={int(deployable_guard['triggered_month_count'])} vs base={int(base_guard['triggered_month_count'])}; "
        f"skipped_trades deployable={int(deployable_guard['total_skipped_trades'])} vs base={int(base_guard['total_skipped_trades'])}"
    )
