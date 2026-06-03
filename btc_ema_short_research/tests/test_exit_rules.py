from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from backtester import BacktestSettings, ExitRule, backtest_strategy


class ExitRulesTest(unittest.TestCase):
    def test_fixed_r_exit_uses_intrabar_target(self) -> None:
        frame = pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=5, freq="4h", tz="UTC"),
                "open": [100.0, 101.0, 100.0, 90.0, 88.0],
                "high": [101.0, 102.0, 101.0, 95.0, 90.0],
                "low": [99.0, 100.0, 95.0, 75.0, 84.0],
                "close": [100.0, 101.0, 96.0, 80.0, 86.0],
                "ema21": [130.0, 130.0, 130.0, 130.0, 130.0],
                "highest_high_10": [110.0, 110.0, 110.0, 110.0, 110.0],
                "atr14": [2.0, 2.0, 2.0, 2.0, 2.0],
            }
        )
        signal = pd.Series([False, True, False, False, False])
        settings = BacktestSettings(
            initial_capital=100000.0,
            risk_per_trade=0.005,
            fee_rate=0.0,
            slippage_rate=0.0,
            stop_lookback=10,
            stop_atr_multiplier=1.0,
        )
        exit_rule = ExitRule(name="fixed_2r", kind="fixed_r", target_r=2.0)

        trades = backtest_strategy(frame, "demo", signal, settings, exit_rule=exit_rule)

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades.iloc[0]["exit_reason"], "take_profit_2R")
        self.assertEqual(float(trades.iloc[0]["exit_price"]), 76.0)
        self.assertEqual(pd.Timestamp(trades.iloc[0]["exit_time"]), frame.loc[3, "timestamp"])

    def test_atr_trailing_stop_updates_for_next_bar_only(self) -> None:
        frame = pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=4, freq="4h", tz="UTC"),
                "open": [100.0, 101.0, 84.0, 80.0],
                "high": [101.0, 95.0, 83.0, 81.0],
                "low": [99.0, 80.0, 70.0, 76.0],
                "close": [100.0, 85.0, 75.0, 79.0],
                "ema21": [130.0, 130.0, 130.0, 130.0],
                "highest_high_10": [110.0, 110.0, 110.0, 110.0],
                "atr14": [2.0, 2.0, 2.0, 2.0],
            }
        )
        signal = pd.Series([True, False, False, False])
        settings = BacktestSettings(
            initial_capital=100000.0,
            risk_per_trade=0.005,
            fee_rate=0.0,
            slippage_rate=0.0,
            stop_lookback=10,
            stop_atr_multiplier=1.0,
        )
        exit_rule = ExitRule(name="atr_trail_1atr", kind="atr_trail", trail_atr_multiplier=1.0)

        trades = backtest_strategy(frame, "demo", signal, settings, exit_rule=exit_rule)

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades.iloc[0]["exit_reason"], "atr_trailing_stop")
        self.assertEqual(float(trades.iloc[0]["exit_price"]), 82.0)
        self.assertEqual(pd.Timestamp(trades.iloc[0]["exit_time"]), frame.loc[2, "timestamp"])


if __name__ == "__main__":
    unittest.main()
