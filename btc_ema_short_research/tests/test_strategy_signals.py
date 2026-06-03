from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from strategies import build_strategy_signals, pullback_event_signals


class StrategySignalsTest(unittest.TestCase):
    def test_core_signals_and_pullback_counting(self) -> None:
        frame = pd.DataFrame(
            {
                "close": [110.0, 109.0, 95.0, 94.0, 96.0, 93.0, 92.0],
                "high": [111.0, 110.0, 101.0, 100.0, 94.0, 100.0, 93.0],
                "low_prev": [None, 100.0, 100.0, 96.0, 94.0, 96.0, 93.0],
                "ema21": [110.0, 102.0, 100.0, 100.0, 96.0, 99.0, 98.0],
                "ema55": [109.0, 101.0, 101.0, 101.0, 97.0, 100.0, 99.0],
                "rsi14": [50.0, 50.0, 46.0, 46.0, 46.0, 46.0, 46.0],
                "vol_ma20": [100.0] * 7,
                "volume": [100.0, 100.0, 120.0, 90.0, 90.0, 130.0, 80.0],
            }
        )

        first_pullback, second_pullback = pullback_event_signals(frame)
        self.assertTrue(bool(first_pullback.iloc[2]))
        self.assertFalse(bool(first_pullback.iloc[3]))
        self.assertTrue(bool(second_pullback.iloc[5]))

        config = {"rsi_filter_threshold": 45.0, "volume_filter_multiplier": 1.0}
        signals = build_strategy_signals(frame, config)
        self.assertTrue(bool(signals["strategy_a_ema21_pullback"].iloc[2]))
        self.assertTrue(bool(signals["strategy_c_dual_bear_pullback"].iloc[2]))
        self.assertTrue(bool(signals["strategy_d_first_pullback"].iloc[2]))
        self.assertTrue(bool(signals["strategy_e_second_pullback"].iloc[5]))
        self.assertTrue(bool(signals["strategy_f_dual_bear_rsi"].iloc[2]))
        self.assertTrue(bool(signals["strategy_g_dual_bear_volume"].iloc[2]))


if __name__ == "__main__":
    unittest.main()
