from __future__ import annotations

import pandas as pd

from research.btc_1h_ema55_simple_backtest import (
    BacktestConfig,
    StrategyConfig,
    add_signal_columns,
    simulate_trade,
)


def test_pullback_signal_waits_for_retest_failure() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=6, freq="h", tz="UTC"),
            "open": [102.0, 101.0, 100.0, 99.0, 98.5, 99.0],
            "high": [103.0, 102.0, 101.0, 99.4, 99.2, 100.2],
            "low": [101.0, 100.0, 99.0, 97.0, 97.8, 97.5],
            "close": [102.0, 101.0, 100.0, 97.5, 98.7, 98.2],
            "volume": [1, 1, 1, 1, 1, 1],
            "ema55": [101.5, 101.2, 100.8, 100.2, 99.8, 99.4],
            "atr14": [2.0, 2.0, 2.0, 2.0, 2.0, 2.0],
        }
    )
    config = BacktestConfig()
    enriched = add_signal_columns(frame, config)

    assert bool(enriched.loc[3, "direct_signal"])
    assert not bool(enriched.loc[3, "pullback_signal"])
    assert bool(enriched.loc[5, "pullback_signal"])


def test_stop_has_priority_when_same_bar_hits_profit_levels() -> None:
    config = BacktestConfig(
        fee_rate=0.0,
        slippage_rate=0.0,
        initial_capital=10_000.0,
    )
    strategy = StrategyConfig(
        name="pullback_failure_short",
        signal_column="pullback_signal",
        signal_label="Breakdown then pullback failure",
    )
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=4, freq="h", tz="UTC"),
            "open": [100.0, 99.0, 98.0, 97.0],
            "high": [101.0, 100.0, 100.0, 98.0],
            "low": [99.0, 98.0, 96.0, 96.0],
            "close": [99.5, 98.5, 97.0, 97.0],
            "volume": [1, 1, 1, 1],
            "ema55": [100.0, 99.0, 98.5, 98.0],
            "atr14": [1.0, 1.0, 1.0, 1.0],
        }
    )

    trade = simulate_trade(
        frame=frame,
        signal_index=1,
        strategy=strategy,
        config=config,
        current_equity=10_000.0,
    )

    assert trade is not None
    assert trade["exit_reason"] == "stop_loss"
    assert trade["partial_take_profit"] is False
