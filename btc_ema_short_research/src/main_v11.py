from __future__ import annotations

import pandas as pd

from backtester import backtest_strategy, default_exit_rule, settings_from_config
from data_loader import load_config, load_multi_timeframe_data, results_dir
from indicators import add_features
from metrics import rank_strategies, summarize_strategy
from report_v11 import write_v11_reports
from v11 import simulate_loss_throttle_trades, summarize_throttle_usage, v11_loss_throttle_rules, v11_schedule_names
from v4 import align_daily_environment_to_entry_frame, build_daily_environment_state
from v5 import build_v5_signals
from v7 import tag_strong_regime_trades
from v8 import apply_cost_scenario, v8_cost_scenarios, v8_risk_schedules
from v9 import build_streak_pressure


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

    v11_config = dict(config)
    v11_config["results_dir"] = str(config["v11_results_dir"])
    results_path = results_dir(v11_config)
    base_settings = settings_from_config(config)

    comparison, throttle_usage, streak_pressure = build_v11_outputs(
        aligned_entry,
        signals,
        base_settings,
        config,
    )

    (results_path / "strategy_comparison.csv").write_text(comparison.to_csv(index=False), encoding="utf-8-sig")
    (results_path / "throttle_usage.csv").write_text(throttle_usage.to_csv(index=False), encoding="utf-8-sig")
    (results_path / "streak_pressure.csv").write_text(streak_pressure.to_csv(index=False), encoding="utf-8-sig")

    write_v11_reports(
        results_path / "research_report.md",
        results_path / "research_report.html",
        config=config,
        metadata=metadata,
        comparison=comparison,
        throttle_usage=throttle_usage,
        streak_pressure=streak_pressure,
    )

    print(f"V11 research complete: {results_path}")


def build_v11_outputs(
    frame: pd.DataFrame,
    signals: dict[str, pd.Series],
    base_settings,
    config: dict[str, object],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    baseline_name = str(config["v6_baseline_strategy"])
    strong_name = str(config["v6_candidate_strategy"])
    baseline_signal = signals[baseline_name]
    strong_signal_times = pd.to_datetime(frame.loc[signals[strong_name].fillna(False), "timestamp"], utc=True)

    scenario_map = {item.name: item for item in v8_cost_scenarios()}
    schedule_map = {item.name: item for item in v8_risk_schedules()}
    scenario = scenario_map[str(config["v11_cost_scenario"])]
    scenario_settings = apply_cost_scenario(base_settings, scenario)
    baseline_trades = backtest_strategy(
        frame,
        baseline_name,
        baseline_signal,
        scenario_settings,
        exit_rule=default_exit_rule(),
    )
    tagged_trades = tag_strong_regime_trades(baseline_trades, strong_signal_times)

    comparison_rows: list[dict[str, object]] = []
    usage_frames: list[pd.DataFrame] = []
    streak_frames: list[pd.DataFrame] = []

    for schedule_name in v11_schedule_names():
        schedule = schedule_map[schedule_name]
        for rule in v11_loss_throttle_rules():
            strategy_name = f"{schedule.name}__{rule.name}"
            simulated = simulate_loss_throttle_trades(
                tagged_trades,
                initial_capital=scenario_settings.initial_capital,
                base_risk_per_trade=float(config["risk_per_trade"]),
                schedule=schedule,
                throttle_rule=rule,
            )
            simulated["strategy_name"] = strategy_name

            summary = summarize_strategy(strategy_name, simulated, frame, scenario_settings.initial_capital)
            summary["schedule_name"] = schedule.name
            summary["loss_throttle_rule_name"] = rule.name
            summary["cost_scenario_name"] = scenario.name
            summary["description"] = f"{schedule.description} + {rule.description}"
            summary["fee_rate"] = scenario_settings.fee_rate
            summary["slippage_rate"] = scenario_settings.slippage_rate
            comparison_rows.append(summary)

            usage_frames.append(summarize_throttle_usage(simulated))
            streak_frames.append(build_streak_pressure(simulated, strategy_name=strategy_name))

    comparison = rank_strategies(pd.DataFrame(comparison_rows))
    throttle_usage = pd.concat(usage_frames, ignore_index=True) if usage_frames else pd.DataFrame()
    streak_pressure = pd.concat(streak_frames, ignore_index=True) if streak_frames else pd.DataFrame()
    return comparison, throttle_usage, streak_pressure


if __name__ == "__main__":
    main()
