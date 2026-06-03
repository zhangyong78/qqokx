from __future__ import annotations

import pandas as pd

from backtester import backtest_strategy, default_exit_rule, settings_from_config
from data_loader import load_config, load_multi_timeframe_data, results_dir
from indicators import add_features
from metrics import rank_strategies, summarize_strategy
from report_v10 import write_v10_research_report
from v10 import apply_monthly_stop_rule, summarize_guardrail_months, v10_monthly_stop_rules, v10_schedule_names
from v4 import align_daily_environment_to_entry_frame, build_daily_environment_state
from v5 import build_v5_signals
from v7 import simulate_dynamic_risk_trades, tag_strong_regime_trades
from v8 import apply_cost_scenario, v8_cost_scenarios, v8_risk_schedules
from v9 import build_period_performance, summarize_periods


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

    v10_config = dict(config)
    v10_config["results_dir"] = str(config["v10_results_dir"])
    results_path = results_dir(v10_config)
    base_settings = settings_from_config(config)

    comparison, monthly_periods, guardrail_months = build_v10_outputs(
        aligned_entry,
        signals,
        base_settings,
        config,
    )
    monthly_summary = summarize_periods(monthly_periods)
    guardrail_summary = summarize_guardrail_months(guardrail_months)

    (results_path / "strategy_comparison.csv").write_text(comparison.to_csv(index=False), encoding="utf-8-sig")
    (results_path / "monthly_periods.csv").write_text(monthly_periods.to_csv(index=False), encoding="utf-8-sig")
    (results_path / "monthly_summary.csv").write_text(monthly_summary.to_csv(index=False), encoding="utf-8-sig")
    (results_path / "guardrail_months.csv").write_text(guardrail_months.to_csv(index=False), encoding="utf-8-sig")
    (results_path / "guardrail_summary.csv").write_text(guardrail_summary.to_csv(index=False), encoding="utf-8-sig")

    write_v10_research_report(
        results_path / "research_report.md",
        config=config,
        metadata=metadata,
        comparison=comparison,
        monthly_summary=monthly_summary,
        guardrail_summary=guardrail_summary,
    )

    print(f"V10 research complete: {results_path}")


def build_v10_outputs(
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
    scenario = scenario_map[str(config["v10_cost_scenario"])]
    scenario_settings = apply_cost_scenario(base_settings, scenario)
    base_trades = backtest_strategy(
        frame,
        baseline_name,
        baseline_signal,
        scenario_settings,
        exit_rule=default_exit_rule(),
    )
    tagged_trades = tag_strong_regime_trades(base_trades, strong_signal_times)

    comparison_rows: list[dict[str, object]] = []
    monthly_frames: list[pd.DataFrame] = []
    guardrail_frames: list[pd.DataFrame] = []

    for schedule_name in v10_schedule_names():
        schedule = schedule_map[schedule_name]
        simulated = simulate_dynamic_risk_trades(
            tagged_trades,
            initial_capital=scenario_settings.initial_capital,
            base_risk_per_trade=float(config["risk_per_trade"]),
            schedule=schedule,
        )
        for stop_rule in v10_monthly_stop_rules():
            strategy_name = f"{schedule.name}__{stop_rule.name}"
            guarded_trades, guardrail_months = apply_monthly_stop_rule(
                simulated,
                initial_capital=scenario_settings.initial_capital,
                stop_rule=stop_rule,
                strategy_name=strategy_name,
            )
            summary = summarize_strategy(strategy_name, guarded_trades, frame, scenario_settings.initial_capital)
            summary["schedule_name"] = schedule.name
            summary["stop_rule_name"] = stop_rule.name
            summary["cost_scenario_name"] = scenario.name
            summary["description"] = f"{schedule.description} + {stop_rule.description}"
            summary["fee_rate"] = scenario_settings.fee_rate
            summary["slippage_rate"] = scenario_settings.slippage_rate
            summary["guarded_trade_count"] = int(len(guarded_trades))
            comparison_rows.append(summary)

            monthly = build_period_performance(
                guarded_trades,
                initial_capital=scenario_settings.initial_capital,
                freq="M",
                strategy_name=strategy_name,
            )
            monthly["schedule_name"] = schedule.name
            monthly["stop_rule_name"] = stop_rule.name
            monthly["cost_scenario_name"] = scenario.name
            monthly_frames.append(monthly)

            guardrail_months["schedule_name"] = schedule.name
            guardrail_months["cost_scenario_name"] = scenario.name
            guardrail_frames.append(guardrail_months)

    comparison = rank_strategies(pd.DataFrame(comparison_rows))
    monthly_periods = pd.concat(monthly_frames, ignore_index=True) if monthly_frames else pd.DataFrame()
    guardrail_months = pd.concat(guardrail_frames, ignore_index=True) if guardrail_frames else pd.DataFrame()
    return comparison, monthly_periods, guardrail_months


if __name__ == "__main__":
    main()
