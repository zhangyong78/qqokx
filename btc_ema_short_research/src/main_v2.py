from __future__ import annotations

from pathlib import Path

import pandas as pd

from backtester import backtest_strategy, settings_from_config
from data_loader import PROJECT_ROOT, load_config, load_multi_timeframe_data, results_dir
from indicators import add_features
from metrics import build_drawdown_curve, build_equity_curve, build_yearly_performance, rank_strategies, summarize_strategy
from mtf import align_daily_filters_to_entry_frame, build_daily_filter_state, build_v2_signals, v2_strategy_definitions
from plots import save_drawdown_curve_html, save_equity_curve_html, save_trade_chart
from report_v2 import write_v2_research_report


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
    daily_filter_state = build_daily_filter_state(daily_features, config)
    aligned_entry = align_daily_filters_to_entry_frame(entry_features, daily_filter_state)

    save_v2_snapshots(
        daily_raw,
        daily_features,
        entry_raw,
        aligned_entry,
        daily_timeframe=str(config["v2_daily_timeframe"]),
        entry_timeframe=str(config["v2_entry_timeframe"]),
    )

    v2_config = dict(config)
    v2_config["results_dir"] = str(config["v2_results_dir"])
    results_path = results_dir(v2_config)
    settings = settings_from_config(config)
    signals = build_v2_signals(aligned_entry, config)

    strategy_rows: list[dict[str, object]] = []
    yearly_frames: list[pd.DataFrame] = []
    all_trades: list[pd.DataFrame] = []
    equity_curves: dict[str, pd.DataFrame] = {}
    drawdown_curves: dict[str, pd.DataFrame] = {}

    for definition in v2_strategy_definitions():
        trades = backtest_strategy(aligned_entry, definition.name, signals[definition.name], settings)
        summary = summarize_strategy(definition.name, trades, aligned_entry, settings.initial_capital)
        strategy_rows.append(summary)
        yearly_frames.append(build_yearly_performance(definition.name, trades, settings.initial_capital))
        all_trades.append(trades)
        equity_curves[definition.name] = build_equity_curve(aligned_entry, trades, settings.initial_capital)
        drawdown_curves[definition.name] = build_drawdown_curve(equity_curves[definition.name])

    comparison = rank_strategies(pd.DataFrame(strategy_rows))
    yearly = pd.concat(yearly_frames, ignore_index=True) if yearly_frames else pd.DataFrame()
    trades = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()

    (results_path / "strategy_comparison.csv").write_text(comparison.to_csv(index=False), encoding="utf-8-sig")
    (results_path / "yearly_performance.csv").write_text(yearly.to_csv(index=False), encoding="utf-8-sig")
    (results_path / "trades.csv").write_text(trades.to_csv(index=False), encoding="utf-8-sig")

    save_equity_curve_html(equity_curves, results_path / "equity_curve.html")
    save_drawdown_curve_html(drawdown_curves, results_path / "drawdown_curve.html")
    save_v2_trade_charts(aligned_entry, comparison, trades, results_path / "trade_charts", int(config.get("v2_trade_chart_limit", 60)))

    write_v2_research_report(
        results_path / "research_report.md",
        config=config,
        metadata=metadata,
        daily_frame=daily_features,
        entry_frame=aligned_entry,
        comparison=comparison,
        ranked=comparison,
        yearly=yearly,
        trades=trades,
    )

    print(f"V2 research complete: {results_path}")


def save_v2_snapshots(
    daily_raw: pd.DataFrame,
    daily_features: pd.DataFrame,
    entry_raw: pd.DataFrame,
    entry_features: pd.DataFrame,
    *,
    daily_timeframe: str,
    entry_timeframe: str,
) -> None:
    daily_label = daily_timeframe.lower()
    entry_label = entry_timeframe.lower()
    targets = {
        PROJECT_ROOT / "data" / "raw" / f"btcusdt_v2_daily_{daily_label}.csv": daily_raw,
        PROJECT_ROOT / "data" / "processed" / f"btcusdt_v2_daily_{daily_label}_features.csv": daily_features,
        PROJECT_ROOT / "data" / "raw" / f"btcusdt_v2_entry_{entry_label}.csv": entry_raw,
        PROJECT_ROOT / "data" / "processed" / f"btcusdt_v2_entry_{entry_label}_features.csv": entry_features,
    }
    for path, frame in targets.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(path, index=False, encoding="utf-8-sig")


def save_v2_trade_charts(
    frame: pd.DataFrame,
    comparison: pd.DataFrame,
    trades: pd.DataFrame,
    output_dir: Path,
    limit: int,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    if trades.empty or comparison.empty or limit <= 0:
        return

    best_strategy = str(comparison.iloc[0]["strategy_name"])
    candidates = (
        trades[trades["strategy_name"] == best_strategy]
        .sort_values(["R_multiple", "entry_time"], ascending=[False, True])
        .head(limit)
    )
    for _, trade in candidates.iterrows():
        file_name = (
            f"{trade['strategy_name']}_{pd.to_datetime(trade['entry_time']).strftime('%Y%m%d_%H%M')}_"
            f"{pd.to_datetime(trade['exit_time']).strftime('%Y%m%d_%H%M')}_{float(trade['R_multiple']):.2f}.png"
        ).replace(":", "_")
        save_trade_chart(frame, trade, output_dir / file_name)


if __name__ == "__main__":
    main()
