from __future__ import annotations

import pandas as pd

from backtester import backtest_strategy, default_exit_rule, settings_from_config
from data_loader import load_config, load_multi_timeframe_data, results_dir
from indicators import add_features
from metrics import rank_strategies, summarize_strategy
from report_v9 import write_v9_research_report
from v4 import align_daily_environment_to_entry_frame, build_daily_environment_state
from v5 import build_v5_signals
from v7 import simulate_dynamic_risk_trades, tag_strong_regime_trades
from v8 import apply_cost_scenario, v8_cost_scenarios, v8_risk_schedules
from v9 import build_period_performance, build_streak_pressure, summarize_periods, v9_selections


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

    v9_config = dict(config)
    v9_config["results_dir"] = str(config["v9_results_dir"])
    results_path = results_dir(v9_config)
    base_settings = settings_from_config(config)

    comparison, monthly_periods, quarterly_periods, streak_pressure = build_v9_outputs(
        aligned_entry,
        signals,
        base_settings,
        config,
    )
    monthly_summary = summarize_periods(monthly_periods)
    quarterly_summary = summarize_periods(quarterly_periods)

    (results_path / "strategy_comparison.csv").write_text(comparison.to_csv(index=False), encoding="utf-8-sig")
    (results_path / "monthly_periods.csv").write_text(monthly_periods.to_csv(index=False), encoding="utf-8-sig")
    (results_path / "monthly_summary.csv").write_text(monthly_summary.to_csv(index=False), encoding="utf-8-sig")
    (results_path / "quarterly_periods.csv").write_text(quarterly_periods.to_csv(index=False), encoding="utf-8-sig")
    (results_path / "quarterly_summary.csv").write_text(quarterly_summary.to_csv(index=False), encoding="utf-8-sig")
    (results_path / "streak_pressure.csv").write_text(streak_pressure.to_csv(index=False), encoding="utf-8-sig")

    write_v9_research_report(
        results_path / "research_report.md",
        config=config,
        metadata=metadata,
        comparison=comparison,
        monthly_summary=monthly_summary,
        quarterly_summary=quarterly_summary,
        streak_pressure=streak_pressure,
    )

    print(f"V9 research complete: {results_path}")


def build_v9_outputs(
    frame: pd.DataFrame,
    signals: dict[str, pd.Series],
    base_settings,
    config: dict[str, object],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    baseline_name = str(config["v6_baseline_strategy"])
    strong_name = str(config["v6_candidate_strategy"])
    baseline_signal = signals[baseline_name]
    strong_signal_times = pd.to_datetime(frame.loc[signals[strong_name].fillna(False), "timestamp"], utc=True)

    schedule_map = {item.name: item for item in v8_risk_schedules()}
    cost_map = {item.name: item for item in v8_cost_scenarios()}
    selected_scenarios = {item.cost_scenario_name for item in v9_selections()}

    tagged_trades_by_scenario: dict[str, tuple[object, pd.DataFrame]] = {}
    for scenario_name in selected_scenarios:
        scenario = cost_map[scenario_name]
        scenario_settings = apply_cost_scenario(base_settings, scenario)
        baseline_trades = backtest_strategy(
            frame,
            baseline_name,
            baseline_signal,
            scenario_settings,
            exit_rule=default_exit_rule(),
        )
        tagged_trades_by_scenario[scenario_name] = (
            scenario_settings,
            tag_strong_regime_trades(baseline_trades, strong_signal_times),
        )

    comparison_rows: list[dict[str, object]] = []
    monthly_frames: list[pd.DataFrame] = []
    quarterly_frames: list[pd.DataFrame] = []
    streak_frames: list[pd.DataFrame] = []

    for selection in v9_selections():
        schedule = schedule_map[selection.schedule_name]
        scenario_settings, tagged_trades = tagged_trades_by_scenario[selection.cost_scenario_name]
        strategy_name = f"{schedule.name}__{selection.cost_scenario_name}"
        simulated = simulate_dynamic_risk_trades(
            tagged_trades,
            initial_capital=scenario_settings.initial_capital,
            base_risk_per_trade=float(config["risk_per_trade"]),
            schedule=schedule,
        )

        summary = summarize_strategy(strategy_name, simulated, frame, scenario_settings.initial_capital)
        summary["schedule_name"] = schedule.name
        summary["cost_scenario_name"] = selection.cost_scenario_name
        summary["description"] = selection.description
        summary["fee_rate"] = scenario_settings.fee_rate
        summary["slippage_rate"] = scenario_settings.slippage_rate
        comparison_rows.append(summary)

        monthly = build_period_performance(
            simulated,
            initial_capital=scenario_settings.initial_capital,
            freq="M",
            strategy_name=strategy_name,
        )
        monthly["schedule_name"] = schedule.name
        monthly["cost_scenario_name"] = selection.cost_scenario_name
        monthly["description"] = selection.description
        monthly_frames.append(monthly)

        quarterly = build_period_performance(
            simulated,
            initial_capital=scenario_settings.initial_capital,
            freq="Q",
            strategy_name=strategy_name,
        )
        quarterly["schedule_name"] = schedule.name
        quarterly["cost_scenario_name"] = selection.cost_scenario_name
        quarterly["description"] = selection.description
        quarterly_frames.append(quarterly)

        streak = build_streak_pressure(simulated, strategy_name=strategy_name)
        streak["schedule_name"] = schedule.name
        streak["cost_scenario_name"] = selection.cost_scenario_name
        streak["description"] = selection.description
        streak_frames.append(streak)

    comparison = rank_strategies(pd.DataFrame(comparison_rows))
    monthly_periods = pd.concat(monthly_frames, ignore_index=True) if monthly_frames else pd.DataFrame()
    quarterly_periods = pd.concat(quarterly_frames, ignore_index=True) if quarterly_frames else pd.DataFrame()
    streak_pressure = pd.concat(streak_frames, ignore_index=True) if streak_frames else pd.DataFrame()
    return comparison, monthly_periods, quarterly_periods, streak_pressure


if __name__ == "__main__":
    main()
