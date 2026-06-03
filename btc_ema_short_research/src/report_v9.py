from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_v9_research_report(
    output_path: Path,
    *,
    config: dict[str, object],
    metadata: dict[str, str],
    comparison: pd.DataFrame,
    monthly_summary: pd.DataFrame,
    quarterly_summary: pd.DataFrame,
    streak_pressure: pd.DataFrame,
) -> None:
    best = comparison[comparison["cost_scenario_name"] == "base_cost"].iloc[0]
    base_baseline = select_row(comparison, "v8_a_flat_1_0", "base_cost")
    stress_baseline = select_row(comparison, "v8_a_flat_1_0", "stress_cost_2_0x")
    stress_candidate = select_row(comparison, "v8_d_strong_1_5_weak_0_5", "stress_cost_2_0x")
    recommendation = choose_v9_recommendation(comparison, monthly_summary, quarterly_summary, streak_pressure)

    lines = [
        "# V9 Live Readiness Report",
        "",
        "## 1. Study Goal",
        "",
        "Carry the V8 winner into a more practical live-readiness pass by checking month-by-month, quarter-by-quarter, and loss-streak pressure.",
        "",
        "## 2. Fixed Trade Logic",
        "",
        f"- baseline entry gate: `{config['v6_baseline_strategy']}`",
        f"- strong regime definition: `{config['v6_candidate_strategy']}`",
        "- exit rule: EMA21 reclaim on 4H close, execute on next 4H open",
        "- sizing schedules and cost assumptions come from the V8 shortlist only",
        "",
        "## 3. Data Source",
        "",
        f"- source: `{metadata['data_source']}`",
        f"- data_root: `{metadata['data_root']}`",
        f"- symbol: `{metadata['symbol']}`",
        f"- timeframes: `{metadata['timeframe']}`",
        "",
        "## 4. Full-History Shortlist",
        "",
        comparison.to_markdown(index=False),
        "",
        "## 5. Monthly Stability Summary",
        "",
        monthly_summary.to_markdown(index=False) if not monthly_summary.empty else "No monthly rows.",
        "",
        "## 6. Quarterly Stability Summary",
        "",
        quarterly_summary.to_markdown(index=False) if not quarterly_summary.empty else "No quarterly rows.",
        "",
        "## 7. Loss-Streak Pressure",
        "",
        streak_pressure.to_markdown(index=False) if not streak_pressure.empty else "No streak rows.",
        "",
        "## 8. Findings",
        "",
        f"- best original-cost cell: {best['strategy_name']} with total_return={best['total_return']:.2%}, profit_factor={best['profit_factor']:.2f}, max_drawdown={best['max_drawdown']:.2%}",
        f"- harsh-cost baseline: {stress_baseline['strategy_name']} with total_return={stress_baseline['total_return']:.2%}, profit_factor={stress_baseline['profit_factor']:.2f}, max_drawdown={stress_baseline['max_drawdown']:.2%}",
        f"- harsh-cost dynamic candidate: {stress_candidate['strategy_name']} with total_return={stress_candidate['total_return']:.2%}, profit_factor={stress_candidate['profit_factor']:.2f}, max_drawdown={stress_candidate['max_drawdown']:.2%}",
        f"- harsh-cost return delta vs flat baseline: {(float(stress_candidate['total_return']) - float(stress_baseline['total_return'])):.2%}",
        f"- harsh-cost profit-factor delta vs flat baseline: {(float(stress_candidate['profit_factor']) - float(stress_baseline['profit_factor'])):.2f}",
        f"- harsh-cost drawdown delta vs flat baseline: {(float(stress_candidate['max_drawdown']) - float(stress_baseline['max_drawdown'])):.2%}",
        build_period_finding(monthly_summary, "M", stress_candidate["strategy_name"], stress_baseline["strategy_name"], "month"),
        build_period_finding(quarterly_summary, "Q", stress_candidate["strategy_name"], stress_baseline["strategy_name"], "quarter"),
        build_streak_finding(streak_pressure, stress_candidate["strategy_name"], stress_baseline["strategy_name"]),
        "",
        "## 9. Recommendation",
        "",
        f"Carry forward: `{recommendation}`",
        "",
        "## 10. Interpretation",
        "",
        "- V9 is not trying to find a brand-new signal. It is checking whether the current favorite still looks acceptable when inspected through practical pain points that traders actually experience.",
        "- If a candidate survives harsher cost assumptions while also keeping monthly and quarterly damage controlled, it is a much better live candidate than a version that only looks good in one aggregate equity curve.",
        "",
        "## 11. Output Notes",
        "",
        f"- results_dir: `{config['v9_results_dir']}`",
        f"- shortlist_count: {len(comparison)}",
        f"- month_count_range: {format_period_count_range(monthly_summary)}",
        f"- quarter_count_range: {format_period_count_range(quarterly_summary)}",
        f"- base_cost_baseline_reference: `{base_baseline['strategy_name']}`",
    ]

    output_path.write_text("\n".join(lines), encoding="utf-8")


def choose_v9_recommendation(
    comparison: pd.DataFrame,
    monthly_summary: pd.DataFrame,
    quarterly_summary: pd.DataFrame,
    streak_pressure: pd.DataFrame,
) -> str:
    stress = comparison[comparison["cost_scenario_name"] == "stress_cost_2_0x"].copy()
    if stress.empty:
        return str(comparison.iloc[0]["strategy_name"])

    month = monthly_summary[monthly_summary["period_freq"] == "M"].copy()
    quarter = quarterly_summary[quarterly_summary["period_freq"] == "Q"].copy()

    merged = stress.merge(
        month[["strategy_name", "worst_period_return", "worst_period_drawdown", "positive_period_rate"]].rename(
            columns={
                "worst_period_return": "worst_month_return",
                "worst_period_drawdown": "worst_month_drawdown",
                "positive_period_rate": "positive_month_rate",
            }
        ),
        on="strategy_name",
        how="left",
    )
    merged = merged.merge(
        quarter[["strategy_name", "worst_period_return", "worst_period_drawdown", "positive_period_rate"]].rename(
            columns={
                "worst_period_return": "worst_quarter_return",
                "worst_period_drawdown": "worst_quarter_drawdown",
                "positive_period_rate": "positive_quarter_rate",
            }
        ),
        on="strategy_name",
        how="left",
    )
    merged = merged.merge(
        streak_pressure[
            ["strategy_name", "max_consecutive_losses", "worst_consecutive_loss_sum", "worst_5_trade_pnl", "worst_10_trade_pnl"]
        ].rename(
            columns={
                "max_consecutive_losses": "streak_max_consecutive_losses",
                "worst_consecutive_loss_sum": "streak_worst_consecutive_loss_sum",
            }
        ),
        on="strategy_name",
        how="left",
    )
    merged = merged.sort_values(
        [
            "profit_factor",
            "total_return",
            "worst_month_return",
            "worst_quarter_return",
            "worst_10_trade_pnl",
            "streak_max_consecutive_losses",
        ],
        ascending=[False, False, False, False, False, True],
    )
    return str(merged.iloc[0]["strategy_name"])


def select_row(comparison: pd.DataFrame, schedule_name: str, cost_scenario_name: str) -> pd.Series:
    return comparison[
        (comparison["schedule_name"] == schedule_name) & (comparison["cost_scenario_name"] == cost_scenario_name)
    ].iloc[0]


def build_period_finding(
    summary: pd.DataFrame,
    period_freq: str,
    candidate_name: str,
    baseline_name: str,
    label: str,
) -> str:
    subset = summary[summary["period_freq"] == period_freq]
    candidate = subset[subset["strategy_name"] == candidate_name].iloc[0]
    baseline = subset[subset["strategy_name"] == baseline_name].iloc[0]
    return (
        f"- harsh-cost worst {label}: candidate={candidate['worst_period_return']:.2%} vs baseline={baseline['worst_period_return']:.2%}; "
        f"positive {label} rate: candidate={candidate['positive_period_rate']:.2%} vs baseline={baseline['positive_period_rate']:.2%}"
    )


def build_streak_finding(streak_pressure: pd.DataFrame, candidate_name: str, baseline_name: str) -> str:
    candidate = streak_pressure[streak_pressure["strategy_name"] == candidate_name].iloc[0]
    baseline = streak_pressure[streak_pressure["strategy_name"] == baseline_name].iloc[0]
    return (
        f"- harsh-cost loss streak pressure: candidate max_consecutive_losses={candidate['max_consecutive_losses']}, "
        f"baseline={baseline['max_consecutive_losses']}; worst_10_trade_pnl candidate={candidate['worst_10_trade_pnl']:.2f}, "
        f"baseline={baseline['worst_10_trade_pnl']:.2f}"
    )


def format_period_count_range(summary: pd.DataFrame) -> str:
    if summary.empty:
        return "0"
    return f"{int(summary['period_count'].min())}-{int(summary['period_count'].max())}"
