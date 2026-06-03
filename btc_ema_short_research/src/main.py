from __future__ import annotations

from pathlib import Path

import pandas as pd

from backtester import backtest_strategy, settings_from_config
from data_loader import load_config, load_market_data, results_dir, save_processed_snapshot, save_raw_snapshot
from indicators import add_features
from metrics import build_drawdown_curve, build_equity_curve, build_yearly_performance, rank_strategies, summarize_strategy
from plots import save_drawdown_curve_html, save_equity_curve_html, save_trade_chart
from report import write_research_report
from strategies import build_strategy_signals, strategy_definitions


def main() -> None:
    config = load_config()
    raw_frame, metadata = load_market_data(config)
    save_raw_snapshot(raw_frame, config)

    features = add_features(raw_frame)
    save_processed_snapshot(features, config)

    results_path = results_dir(config)
    settings = settings_from_config(config)
    signals = build_strategy_signals(features, config)

    strategy_rows: list[dict[str, object]] = []
    yearly_frames: list[pd.DataFrame] = []
    all_trades: list[pd.DataFrame] = []
    equity_curves: dict[str, pd.DataFrame] = {}
    drawdown_curves: dict[str, pd.DataFrame] = {}

    for definition in strategy_definitions():
        trades = backtest_strategy(features, definition.name, signals[definition.name], settings)
        summary = summarize_strategy(definition.name, trades, features, settings.initial_capital)
        strategy_rows.append(summary)
        yearly_frames.append(build_yearly_performance(definition.name, trades, settings.initial_capital))
        all_trades.append(trades)
        equity_curves[definition.name] = build_equity_curve(features, trades, settings.initial_capital)
        drawdown_curves[definition.name] = build_drawdown_curve(equity_curves[definition.name])

    comparison = rank_strategies(pd.DataFrame(strategy_rows))
    yearly = pd.concat(yearly_frames, ignore_index=True) if yearly_frames else pd.DataFrame()
    trades = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()

    comparison_output = results_path / "strategy_comparison.csv"
    yearly_output = results_path / "yearly_performance.csv"
    trades_output = results_path / "trades.csv"
    equity_output = results_path / "equity_curve.html"
    drawdown_output = results_path / "drawdown_curve.html"
    report_output = results_path / "research_report.md"

    comparison.to_csv(comparison_output, index=False, encoding="utf-8-sig")
    yearly.to_csv(yearly_output, index=False, encoding="utf-8-sig")
    trades.to_csv(trades_output, index=False, encoding="utf-8-sig")

    save_equity_curve_html(equity_curves, equity_output)
    save_drawdown_curve_html(drawdown_curves, drawdown_output)

    trade_chart_dir = results_path / "trade_charts"
    for _, trade in trades.iterrows():
        file_name = (
            f"{trade['strategy_name']}_{pd.to_datetime(trade['entry_time']).strftime('%Y%m%d')}_"
            f"{pd.to_datetime(trade['exit_time']).strftime('%Y%m%d')}_{float(trade['R_multiple']):.2f}.png"
        ).replace(":", "_")
        save_trade_chart(features, trade, trade_chart_dir / file_name)

    ambiguities = [
        "The original task brief used Binance spot daily data, but this implementation uses local OKX daily data by explicit instruction.",
        "First and second pullbacks are counted as discrete new touch events, not every consecutive bar that remains in contact with EMA21.",
        "If the final bar triggers a trend exit without a next open, the engine exits on the same bar close with slippage and records a dedicated exit reason.",
    ]
    write_research_report(
        report_output,
        config=config,
        metadata=metadata,
        raw_frame=raw_frame,
        comparison=comparison,
        ranked=comparison,
        yearly=yearly,
        trades=trades,
        ambiguities=ambiguities,
    )

    print(f"Research complete: {results_path}")


if __name__ == "__main__":
    main()
