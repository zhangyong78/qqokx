from __future__ import annotations

import pandas as pd

from backtester import backtest_strategy, default_exit_rule, settings_from_config
from data_loader import load_config, load_multi_timeframe_data, results_dir
from indicators import add_features
from metrics import rank_strategies, summarize_strategy
from report_v8 import write_v8_research_report
from v4 import align_daily_environment_to_entry_frame, build_daily_environment_state
from v5 import build_v5_signals
from v6 import build_anchored_walkforward_splits, slice_frame_by_time
from v7 import simulate_dynamic_risk_trades, tag_strong_regime_trades
from v8 import apply_cost_scenario, v8_cost_scenarios, v8_risk_schedules


def main() -> None:
    config = load_config()
    frames, metadata = load_multi_timeframe_data(
        config,
        [str(config["v2_daily_timeframe"]), str(config["v2_entry_timeframe"])],
    )
    daily_raw = frames[str(config["v2_daily_timeframe"])]
    entry_raw = frames[str(config["v2_entry_timeframe"])]

    daily_features = add_features(daily_raw)
    entry_features = add_features(entry_raw)
    environment_state = build_daily_environment_state(daily_features, config)
    aligned_entry = align_daily_environment_to_entry_frame(entry_features, environment_state)
    signals = build_v5_signals(aligned_entry, config)

    v8_config = dict(config)
    v8_config["results_dir"] = str(config["v8_results_dir"])
    results_path = results_dir(v8_config)
    base_settings = settings_from_config(config)

    comparison = build_full_history_comparison(aligned_entry, signals, base_settings, config)
    split_results = build_split_results(aligned_entry, signals, base_settings, config)
    robustness = build_robustness_summary(split_results)

    (results_path / "strategy_comparison.csv").write_text(comparison.to_csv(index=False), encoding="utf-8-sig")
    (results_path / "walkforward_splits.csv").write_text(split_results.to_csv(index=False), encoding="utf-8-sig")
    (results_path / "robustness_summary.csv").write_text(robustness.to_csv(index=False), encoding="utf-8-sig")

    write_v8_research_report(
        results_path / "research_report.md",
        config=config,
        metadata=metadata,
        comparison=comparison,
        split_results=split_results,
        robustness=robustness,
    )

    print(f"V8 research complete: {results_path}")


def build_full_history_comparison(frame: pd.DataFrame, signals: dict[str, pd.Series], base_settings, config: dict[str, object]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    baseline_name = str(config["v6_baseline_strategy"])
    strong_name = str(config["v6_candidate_strategy"])
    baseline_signal = signals[baseline_name]
    strong_signal_times = pd.to_datetime(frame.loc[signals[strong_name].fillna(False), "timestamp"], utc=True)

    for cost_scenario in v8_cost_scenarios():
        scenario_settings = apply_cost_scenario(base_settings, cost_scenario)
        base_trades = backtest_strategy(frame, baseline_name, baseline_signal, scenario_settings, exit_rule=default_exit_rule())
        tagged_trades = tag_strong_regime_trades(base_trades, strong_signal_times)
        for schedule in v8_risk_schedules():
            strategy_name = f"{schedule.name}__{cost_scenario.name}"
            simulated = simulate_dynamic_risk_trades(
                tagged_trades,
                initial_capital=scenario_settings.initial_capital,
                base_risk_per_trade=float(config["risk_per_trade"]),
                schedule=schedule,
            )
            summary = summarize_strategy(strategy_name, simulated, frame, scenario_settings.initial_capital)
            summary["schedule_name"] = schedule.name
            summary["cost_scenario_name"] = cost_scenario.name
            summary["fee_rate"] = scenario_settings.fee_rate
            summary["slippage_rate"] = scenario_settings.slippage_rate
            rows.append(summary)

    return rank_strategies(pd.DataFrame(rows))


def build_split_results(frame: pd.DataFrame, signals: dict[str, pd.Series], base_settings, config: dict[str, object]) -> pd.DataFrame:
    baseline_name = str(config["v6_baseline_strategy"])
    strong_name = str(config["v6_candidate_strategy"])
    splits = build_anchored_walkforward_splits(
        frame,
        first_test_year=int(config["v6_first_test_year"]),
        min_train_years=int(config["v6_min_train_years"]),
    )
    rows: list[dict[str, object]] = []

    for split in splits:
        for sample_type, start, end in (
            ("train", split.train_start, split.train_end),
            ("test", split.test_start, split.test_end),
        ):
            frame_slice = slice_frame_by_time(frame, start, end)
            if frame_slice.empty:
                continue
            signal_slice_index = frame_slice.index
            frame_slice_reset = frame_slice.reset_index(drop=True)
            baseline_signal = signals[baseline_name].loc[signal_slice_index].reset_index(drop=True)
            strong_signal_slice = signals[strong_name].loc[signal_slice_index].reset_index(drop=True)
            strong_times = pd.to_datetime(frame_slice_reset.loc[strong_signal_slice.fillna(False), "timestamp"], utc=True)

            for cost_scenario in v8_cost_scenarios():
                scenario_settings = apply_cost_scenario(base_settings, cost_scenario)
                base_trades = backtest_strategy(
                    frame_slice_reset,
                    baseline_name,
                    baseline_signal,
                    scenario_settings,
                    exit_rule=default_exit_rule(),
                )
                tagged = tag_strong_regime_trades(base_trades, strong_times)
                for schedule in v8_risk_schedules():
                    strategy_name = f"{schedule.name}__{cost_scenario.name}"
                    simulated = simulate_dynamic_risk_trades(
                        tagged,
                        initial_capital=scenario_settings.initial_capital,
                        base_risk_per_trade=float(config["risk_per_trade"]),
                        schedule=schedule,
                    )
                    summary = summarize_strategy(strategy_name, simulated, frame_slice_reset, scenario_settings.initial_capital)
                    summary["split_label"] = split.label
                    summary["sample_type"] = sample_type
                    summary["schedule_name"] = schedule.name
                    summary["cost_scenario_name"] = cost_scenario.name
                    rows.append(summary)

    columns = [
        "split_label",
        "sample_type",
        "strategy_name",
        "schedule_name",
        "cost_scenario_name",
        "total_return",
        "annual_return",
        "max_drawdown",
        "profit_factor",
        "trade_count",
        "average_R",
        "win_rate",
        "sharpe",
        "sortino",
        "calmar",
    ]
    if not rows:
        return pd.DataFrame(columns=columns)
    out = pd.DataFrame(rows)
    return out[columns].sort_values(["split_label", "sample_type", "cost_scenario_name", "schedule_name"]).reset_index(drop=True)


def build_robustness_summary(split_results: pd.DataFrame) -> pd.DataFrame:
    test_rows = split_results[split_results["sample_type"] == "test"].copy()
    if test_rows.empty:
        return pd.DataFrame(
            columns=[
                "schedule_name",
                "test_case_count",
                "positive_test_cases",
                "pf_gt1_test_cases",
                "median_test_return_all",
                "median_test_profit_factor_all",
                "median_stress_test_return",
                "median_stress_test_profit_factor",
                "worst_test_drawdown",
            ]
        )

    rows: list[dict[str, object]] = []
    for schedule_name, group in test_rows.groupby("schedule_name", sort=False):
        stress = group[group["cost_scenario_name"] != "base_cost"].copy()
        rows.append(
            {
                "schedule_name": schedule_name,
                "test_case_count": int(len(group)),
                "positive_test_cases": int((group["total_return"] > 0).sum()),
                "pf_gt1_test_cases": int((group["profit_factor"] > 1.0).sum()),
                "median_test_return_all": float(group["total_return"].median()),
                "median_test_profit_factor_all": float(group["profit_factor"].median()),
                "median_stress_test_return": float(stress["total_return"].median()) if not stress.empty else 0.0,
                "median_stress_test_profit_factor": float(stress["profit_factor"].median()) if not stress.empty else 0.0,
                "worst_test_drawdown": float(group["max_drawdown"].min()),
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["median_stress_test_profit_factor", "median_stress_test_return"],
        ascending=False,
    ).reset_index(drop=True)


if __name__ == "__main__":
    main()
