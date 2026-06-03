from __future__ import annotations

from pathlib import Path

import pandas as pd

MIN_RELIABLE_TRADE_COUNT = 30
MIN_ELIGIBLE_PROFIT_FACTOR = 1.3


def write_research_report(
    output_path: Path,
    *,
    config: dict[str, object],
    metadata: dict[str, str],
    raw_frame: pd.DataFrame,
    comparison: pd.DataFrame,
    ranked: pd.DataFrame,
    yearly: pd.DataFrame,
    trades: pd.DataFrame,
    ambiguities: list[str],
) -> None:
    eligible = eligible_candidates(comparison)
    best_overall = (ranked[ranked["strategy_name"].isin(eligible["strategy_name"])] if not eligible.empty else ranked).iloc[0]
    low_drawdown_pool = eligible if not eligible.empty else comparison
    low_drawdown = low_drawdown_pool.sort_values(["max_drawdown", "profit_factor"], ascending=[False, False]).iloc[0]
    rejected = comparison[
        (comparison["profit_factor"] <= 1.0)
        | (comparison["average_R"] <= 0)
        | (comparison["trade_count"] < 30)
    ].copy()

    lines = [
        "# Research Report",
        "",
        "## 1. Data Source",
        "",
        f"- source: `{metadata['data_source']}`",
        f"- data_root: `{metadata['data_root']}`",
        f"- symbol: `{metadata['symbol']}`",
        f"- timeframe: `{metadata['timeframe']}`",
        "",
        "## 2. Data Range",
        "",
        f"- first bar: {raw_frame['timestamp'].iloc[0].isoformat()}",
        f"- last bar: {raw_frame['timestamp'].iloc[-1].isoformat()}",
        f"- bar count: {len(raw_frame)}",
        "",
        "## 3. Indicator Definitions",
        "",
        "- EMA21 = EMA(close, 21)",
        "- EMA55 = EMA(close, 55)",
        "- ATR14 = Wilder ATR(high, low, close, 14)",
        "- RSI14 = RSI(close, 14)",
        "- VOL_MA20 = SMA(volume, 20)",
        "",
        "## 4. Backtest Assumptions",
        "",
        f"- initial_capital: {config['initial_capital']}",
        f"- risk_per_trade: {config['risk_per_trade']}",
        f"- fee_rate: {config['fee_rate']}",
        f"- slippage_rate: {config['slippage_rate']}",
        "- direction: short_only",
        "- max_open_positions: 1",
        "- compound_interest: true",
        "",
        "## 5. Fees And Slippage",
        "",
        "- short entry fill = open[t+1] * (1 - slippage_rate)",
        "- short exit fill on trend exit = open[t+1] * (1 + slippage_rate)",
        "- short stop exit fill = stop_loss * (1 + slippage_rate)",
        "- fees are charged on both entry and exit notionals",
        "",
        "## 6. Strategy Summary",
        "",
        ranked.to_markdown(index=False),
        "",
        "## 7. Yearly Performance Analysis",
        "",
        yearly.to_markdown(index=False) if not yearly.empty else "No yearly rows.",
        "",
        "## 8. Max Drawdown Analysis",
        "",
        f"- best overall max_drawdown: {best_overall['max_drawdown']:.2%}",
        f"- low drawdown candidate: {low_drawdown['strategy_name']} with max_drawdown {low_drawdown['max_drawdown']:.2%}",
        "",
        "## 9. Concentration Check",
        "",
        "The project uses yearly performance and total trade count to reduce the chance of selecting a strategy that only worked in one isolated period.",
        "",
        "## 10. Worthy V2 Candidates",
        "",
        f"- best overall: {best_overall['strategy_name']}",
        f"- best low drawdown: {low_drawdown['strategy_name']}",
        "",
        "## 11. Strategies Not Recommended To Continue",
        "",
    ]
    if rejected.empty:
        lines.append("- None.")
    else:
        for _, row in rejected.iterrows():
            lines.append(
                f"- {row['strategy_name']}: profit_factor={row['profit_factor']:.2f}, "
                f"average_R={row['average_R']:.2f}, trade_count={int(row['trade_count'])}"
            )

    lines.extend(
        [
            "",
            "## 12. Next Optimization Ideas",
            "",
            "- daily direction plus 4H entry timing",
            "- retest entry logic after a confirmed pullback failure",
            "- compare trend exit against ATR trailing in a dedicated V2 study",
            "",
            "## Ambiguities And Choices",
            "",
        ]
    )
    for item in ambiguities:
        lines.append(f"- {item}")

    lines.extend(
        [
            "",
            "## Required Answers",
            "",
            f"1. EMA21 vs EMA55: {answer_compare(comparison, 'strategy_a_ema21_pullback', 'strategy_b_ema55_pullback')}",
            f"2. Does EMA21 < EMA55 improve quality: {answer_dual_bear(comparison)}",
            f"3. First vs second pullback: {answer_compare(comparison, 'strategy_d_first_pullback', 'strategy_e_second_pullback')}",
            f"4. RSI filter value: {answer_filter_value(comparison, 'strategy_f_dual_bear_rsi', 'strategy_c_dual_bear_pullback')}",
            f"5. Volume filter value: {answer_filter_value(comparison, 'strategy_g_dual_bear_volume', 'strategy_c_dual_bear_pullback')}",
            f"6. Highest return strategy: {comparison.sort_values('total_return', ascending=False).iloc[0]['strategy_name']}",
            f"7. Lowest drawdown strategy: {low_drawdown['strategy_name']}",
            f"8. Most comfortable overall strategy: {best_overall['strategy_name']}",
            f"9. Best V2 candidate: {best_overall['strategy_name']}",
            "10. Daily trend plus 4H entry: recommended as the clearest V2 direction if the chosen daily strategy still lacks trade frequency.",
            "",
            "Final Recommendation:",
            "",
            "Best overall strategy:",
            f"- strategy_name: {best_overall['strategy_name']}",
            f"- reason: profit_factor={best_overall['profit_factor']:.2f}, max_drawdown={best_overall['max_drawdown']:.2%}, trade_count={int(best_overall['trade_count'])}",
            "",
            "Best low-drawdown strategy:",
            f"- strategy_name: {low_drawdown['strategy_name']}",
            f"- reason: max_drawdown={low_drawdown['max_drawdown']:.2%}, profit_factor={low_drawdown['profit_factor']:.2f}",
            "",
            "Rejected strategies:",
        ]
    )
    if rejected.empty:
        lines.append("- none")
    else:
        for _, row in rejected.iterrows():
            lines.append(f"- {row['strategy_name']}: {format_rejection_reason(row)}")

    lines.extend(
        [
            "",
            "Recommended V2 direction:",
            "- direction 1: daily direction plus 4H entry timing",
            "- direction 2: pullback event definition refinement for first and second retests",
            "- direction 3: exit rule comparison between EMA21 reclaim, ATR trail, and fixed R",
        ]
    )

    output_path.write_text("\n".join(lines), encoding="utf-8")


def answer_compare(comparison: pd.DataFrame, left_name: str, right_name: str) -> str:
    left = comparison[comparison["strategy_name"] == left_name].iloc[0]
    right = comparison[comparison["strategy_name"] == right_name].iloc[0]
    unreliable_names = [
        row["strategy_name"]
        for _, row in pd.DataFrame([left, right]).iterrows()
        if int(row["trade_count"]) < MIN_RELIABLE_TRADE_COUNT
    ]
    if unreliable_names:
        detail = ", ".join(
            f"{row['strategy_name']} ({int(row['trade_count'])} trades)"
            for _, row in pd.DataFrame([left, right]).iterrows()
        )
        return f"Insufficient sample to decide reliably. {detail}."
    return left_name if left["score"] >= right["score"] else right_name


def answer_dual_bear(comparison: pd.DataFrame) -> str:
    base = comparison[comparison["strategy_name"].isin(["strategy_a_ema21_pullback", "strategy_c_dual_bear_pullback"])]
    ordered = base.sort_values("score", ascending=False)
    winner = ordered.iloc[0]
    if winner["strategy_name"] == "strategy_c_dual_bear_pullback":
        return "Yes, the dual-bear alignment improved the baseline EMA21 pullback."
    return "No clear improvement over the plain EMA21 pullback baseline."


def answer_filter_value(comparison: pd.DataFrame, filter_name: str, base_name: str) -> str:
    filtered = comparison[comparison["strategy_name"] == filter_name].iloc[0]
    base = comparison[comparison["strategy_name"] == base_name].iloc[0]
    if filtered["score"] > base["score"]:
        return f"Positive for {filter_name}."
    return f"Not better than {base_name}."


def eligible_candidates(comparison: pd.DataFrame) -> pd.DataFrame:
    return comparison[
        (comparison["profit_factor"] > MIN_ELIGIBLE_PROFIT_FACTOR)
        & (comparison["trade_count"] >= MIN_RELIABLE_TRADE_COUNT)
        & (comparison["average_R"] > 0)
    ].copy()


def rejection_reasons(row: pd.Series) -> list[str]:
    reasons: list[str] = []
    if int(row["trade_count"]) < MIN_RELIABLE_TRADE_COUNT:
        reasons.append(f"trade_count {int(row['trade_count'])} < {MIN_RELIABLE_TRADE_COUNT}")
    if float(row["profit_factor"]) <= 1.0:
        reasons.append(f"profit_factor {float(row['profit_factor']):.2f} <= 1.00")
    if float(row["average_R"]) <= 0:
        reasons.append(f"average_R {float(row['average_R']):.2f} <= 0")
    return reasons


def format_rejection_reason(row: pd.Series) -> str:
    reasons = rejection_reasons(row)
    if not reasons:
        return "did not meet continuation criteria"
    return "; ".join(reasons)
