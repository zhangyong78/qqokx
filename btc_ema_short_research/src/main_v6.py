from __future__ import annotations

import pandas as pd

from backtester import backtest_strategy, default_exit_rule, settings_from_config
from data_loader import load_config, load_multi_timeframe_data, results_dir
from indicators import add_features
from metrics import rank_strategies, summarize_strategy
from report_v6 import write_v6_research_report
from v4 import align_daily_environment_to_entry_frame, build_daily_environment_state
from v5 import build_v5_signals
from v6 import build_anchored_walkforward_splits, slice_frame_by_time


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

    baseline_name = str(config["v6_baseline_strategy"])
    candidate_name = str(config["v6_candidate_strategy"])
    selected_names = [baseline_name, candidate_name]

    v6_config = dict(config)
    v6_config["results_dir"] = str(config["v6_results_dir"])
    results_path = results_dir(v6_config)
    settings = settings_from_config(config)
    exit_rule = default_exit_rule()

    comparison = build_full_history_comparison(aligned_entry, signals, selected_names, settings, exit_rule)
    split_results = build_split_results(aligned_entry, signals, selected_names, settings, exit_rule, config)
    robustness = build_robustness_summary(split_results)

    (results_path / "strategy_comparison.csv").write_text(comparison.to_csv(index=False), encoding="utf-8-sig")
    (results_path / "walkforward_splits.csv").write_text(split_results.to_csv(index=False), encoding="utf-8-sig")
    (results_path / "robustness_summary.csv").write_text(robustness.to_csv(index=False), encoding="utf-8-sig")

    write_v6_research_report(
        results_path / "research_report.md",
        config=config,
        metadata=metadata,
        comparison=comparison,
        split_results=split_results,
        robustness=robustness,
    )

    print(f"V6 research complete: {results_path}")


def build_full_history_comparison(
    frame: pd.DataFrame,
    signals: dict[str, pd.Series],
    strategy_names: list[str],
    settings,
    exit_rule,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for strategy_name in strategy_names:
        trades = backtest_strategy(frame, strategy_name, signals[strategy_name], settings, exit_rule=exit_rule)
        rows.append(summarize_strategy(strategy_name, trades, frame, settings.initial_capital))
    return rank_strategies(pd.DataFrame(rows))


def build_split_results(
    frame: pd.DataFrame,
    signals: dict[str, pd.Series],
    strategy_names: list[str],
    settings,
    exit_rule,
    config: dict[str, object],
) -> pd.DataFrame:
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
            for strategy_name in strategy_names:
                signal_slice = signals[strategy_name].loc[signal_slice_index].reset_index(drop=True)
                frame_slice_reset = frame_slice.reset_index(drop=True)
                trades = backtest_strategy(frame_slice_reset, strategy_name, signal_slice, settings, exit_rule=exit_rule)
                summary = summarize_strategy(strategy_name, trades, frame_slice_reset, settings.initial_capital)
                summary["split_label"] = split.label
                summary["sample_type"] = sample_type
                summary["train_start"] = split.train_start.isoformat()
                summary["train_end"] = split.train_end.isoformat()
                summary["test_start"] = split.test_start.isoformat()
                summary["test_end"] = split.test_end.isoformat()
                rows.append(summary)

    columns = [
        "split_label",
        "sample_type",
        "strategy_name",
        "train_start",
        "train_end",
        "test_start",
        "test_end",
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
    return out[columns].sort_values(["split_label", "sample_type", "strategy_name"]).reset_index(drop=True)


def build_robustness_summary(split_results: pd.DataFrame) -> pd.DataFrame:
    test_rows = split_results[split_results["sample_type"] == "test"].copy()
    if test_rows.empty:
        return pd.DataFrame(
            columns=[
                "strategy_name",
                "test_window_count",
                "positive_test_windows",
                "pf_gt1_test_windows",
                "median_test_return",
                "median_test_profit_factor",
                "worst_test_drawdown",
                "median_test_trade_count",
            ]
        )

    rows: list[dict[str, object]] = []
    for strategy_name, group in test_rows.groupby("strategy_name", sort=False):
        rows.append(
            {
                "strategy_name": strategy_name,
                "test_window_count": int(len(group)),
                "positive_test_windows": int((group["total_return"] > 0).sum()),
                "pf_gt1_test_windows": int((group["profit_factor"] > 1.0).sum()),
                "median_test_return": float(group["total_return"].median()),
                "median_test_profit_factor": float(group["profit_factor"].median()),
                "worst_test_drawdown": float(group["max_drawdown"].min()),
                "median_test_trade_count": float(group["trade_count"].median()),
            }
        )
    return pd.DataFrame(rows).sort_values(["median_test_profit_factor", "median_test_return"], ascending=False).reset_index(drop=True)


if __name__ == "__main__":
    main()
