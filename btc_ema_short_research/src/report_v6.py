from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_v6_research_report(
    output_path: Path,
    *,
    config: dict[str, object],
    metadata: dict[str, str],
    comparison: pd.DataFrame,
    split_results: pd.DataFrame,
    robustness: pd.DataFrame,
) -> None:
    baseline = comparison[comparison["strategy_name"] == str(config["v6_baseline_strategy"])].iloc[0]
    candidate = comparison[comparison["strategy_name"] == str(config["v6_candidate_strategy"])].iloc[0]
    test_rows = split_results[split_results["sample_type"] == "test"].copy()

    lines = [
        "# V6 Robustness Report",
        "",
        "## 1. Study Goal",
        "",
        "Validate whether the current best daily combo filter remains stable across chronological out-of-sample windows, instead of relying only on full-history results.",
        "",
        "## 2. Strategies Compared",
        "",
        f"- baseline_strategy: `{config['v6_baseline_strategy']}`",
        f"- candidate_strategy: `{config['v6_candidate_strategy']}`",
        "- entry logic and exit logic are fixed; only robustness across time is being tested.",
        "",
        "## 3. Data Source",
        "",
        f"- source: `{metadata['data_source']}`",
        f"- data_root: `{metadata['data_root']}`",
        f"- symbol: `{metadata['symbol']}`",
        f"- daily_timeframe: `{config['v2_daily_timeframe']}`",
        f"- entry_timeframe: `{config['v2_entry_timeframe']}`",
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
        f"- candidate full-history metrics: total_return={candidate['total_return']:.2%}, profit_factor={candidate['profit_factor']:.2f}, max_drawdown={candidate['max_drawdown']:.2%}, trade_count={int(candidate['trade_count'])}",
        f"- baseline full-history metrics: total_return={baseline['total_return']:.2%}, profit_factor={baseline['profit_factor']:.2f}, max_drawdown={baseline['max_drawdown']:.2%}, trade_count={int(baseline['trade_count'])}",
        f"- candidate test median return: {median_test_return(test_rows, str(config['v6_candidate_strategy'])):.2%}",
        f"- baseline test median return: {median_test_return(test_rows, str(config['v6_baseline_strategy'])):.2%}",
        f"- candidate positive test windows: {positive_test_windows(test_rows, str(config['v6_candidate_strategy']))}",
        f"- baseline positive test windows: {positive_test_windows(test_rows, str(config['v6_baseline_strategy']))}",
        "",
        "## 8. Interpretation",
        "",
        "- Each split resets capital at the split boundary and does not carry positions across train/test boundaries.",
        "- Because the strategy has fixed rules rather than learned parameters, the train period is used as context and the test period is the real robustness check.",
        "- If the candidate keeps similar or better median test performance than baseline, the combo filter is likely genuine rather than a full-history artifact.",
        "",
        "## 9. Recommendation",
        "",
        f"Carry forward: `{choose_recommendation(robustness, str(config['v6_baseline_strategy']), str(config['v6_candidate_strategy']))}`",
        "",
        "## 10. Output Notes",
        "",
        f"- split_count: {len(split_results['split_label'].unique()) if not split_results.empty else 0}",
        f"- results_dir: `{config['v6_results_dir']}`",
    ]

    output_path.write_text("\n".join(lines), encoding="utf-8")


def median_test_return(test_rows: pd.DataFrame, strategy_name: str) -> float:
    rows = test_rows[test_rows["strategy_name"] == strategy_name]
    if rows.empty:
        return 0.0
    return float(rows["total_return"].median())


def positive_test_windows(test_rows: pd.DataFrame, strategy_name: str) -> str:
    rows = test_rows[test_rows["strategy_name"] == strategy_name]
    if rows.empty:
        return "0/0"
    positive = int((rows["total_return"] > 0).sum())
    return f"{positive}/{len(rows)}"


def choose_recommendation(robustness: pd.DataFrame, baseline_name: str, candidate_name: str) -> str:
    if robustness.empty:
        return candidate_name
    indexed = robustness.set_index("strategy_name")
    if baseline_name not in indexed.index or candidate_name not in indexed.index:
        return candidate_name
    baseline = indexed.loc[baseline_name]
    candidate = indexed.loc[candidate_name]
    candidate_better = (
        float(candidate["median_test_return"]) >= float(baseline["median_test_return"])
        and float(candidate["median_test_profit_factor"]) >= float(baseline["median_test_profit_factor"])
    )
    return candidate_name if candidate_better else baseline_name
