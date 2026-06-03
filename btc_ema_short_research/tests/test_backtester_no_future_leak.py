from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from backtester import BacktestSettings, backtest_strategy
from strategies import build_strategy_signals


class BacktesterNoFutureLeakTest(unittest.TestCase):
    def test_entry_uses_next_bar_open(self) -> None:
        frame = pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=5, freq="D", tz="UTC"),
                "open": [100.0, 101.0, 102.0, 97.0, 96.0],
                "high": [101.0, 102.0, 103.0, 98.0, 97.0],
                "low": [99.0, 100.0, 101.0, 96.0, 95.0],
                "close": [100.0, 99.0, 103.0, 95.0, 94.0],
                "ema21": [105.0, 105.0, 100.0, 100.0, 100.0],
                "highest_high_10": [110.0, 110.0, 110.0, 110.0, 110.0],
                "atr14": [2.0, 2.0, 2.0, 2.0, 2.0],
            }
        )
        signal = pd.Series([False, True, False, False, False])
        settings = BacktestSettings(
            initial_capital=100000.0,
            risk_per_trade=0.005,
            fee_rate=0.0,
            slippage_rate=0.0002,
            stop_lookback=10,
            stop_atr_multiplier=1.0,
        )

        trades = backtest_strategy(frame, "demo", signal, settings)
        self.assertEqual(len(trades), 1)
        expected_entry = 102.0 * (1.0 - settings.slippage_rate)
        self.assertAlmostEqual(float(trades.iloc[0]["entry_price"]), expected_entry)
        self.assertEqual(pd.Timestamp(trades.iloc[0]["entry_time"]), frame.loc[2, "timestamp"])

    def test_signal_generation_does_not_depend_on_future_bar_values(self) -> None:
        base = pd.DataFrame(
            {
                "close": [100.0, 98.0, 97.0, 96.0],
                "high": [101.0, 100.0, 99.0, 97.0],
                "low": [99.0, 97.0, 96.0, 95.0],
                "low_prev": [None, 99.0, 97.0, 96.0],
                "ema21": [99.0, 99.0, 98.0, 98.0],
                "ema55": [100.0, 100.0, 99.0, 99.0],
                "rsi14": [50.0, 50.0, 50.0, 50.0],
                "vol_ma20": [100.0, 100.0, 100.0, 100.0],
                "volume": [100.0, 100.0, 100.0, 100.0],
            }
        )
        mutated = base.copy()
        mutated.loc[3, ["close", "high", "low", "volume"]] = [999.0, 999.0, 1.0, 9999.0]

        config = {"rsi_filter_threshold": 45.0, "volume_filter_multiplier": 1.0}
        left = build_strategy_signals(base, config)["strategy_c_dual_bear_pullback"]
        right = build_strategy_signals(mutated, config)["strategy_c_dual_bear_pullback"]
        self.assertEqual(bool(left.iloc[2]), bool(right.iloc[2]))


if __name__ == "__main__":
    unittest.main()
