from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from v5 import build_v5_signals


class V5ComboTest(unittest.TestCase):
    def test_v5_combo_signals_require_all_conditions(self) -> None:
        frame = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    [
                        "2024-01-03 00:00:00+00:00",
                        "2024-01-03 04:00:00+00:00",
                        "2024-01-03 08:00:00+00:00",
                    ]
                ),
                "open": [100.0, 100.0, 100.0],
                "high": [101.0, 101.0, 101.0],
                "low": [98.0, 98.0, 98.0],
                "close": [99.0, 99.0, 99.0],
                "volume": [120.0, 120.0, 120.0],
                "low_prev": [100.0, 100.0, 100.0],
                "ema21": [100.0, 100.0, 100.0],
                "ema55": [101.0, 101.0, 101.0],
                "rsi14": [50.0, 50.0, 50.0],
                "vol_ma20": [100.0, 100.0, 100.0],
                "ema55_slope_5": [-1.0, -1.0, -1.0],
                "env_baseline": [True, True, True],
                "env_close_below_ema21": [False, True, True],
                "env_rsi_rebound": [True, False, True],
                "env_ema55_down": [True, True, True],
                "env_breakdown_and_slope": [False, True, True],
            }
        )
        config = {"rsi_filter_threshold": 45.0, "volume_filter_multiplier": 1.0}

        signals = build_v5_signals(frame, config)

        self.assertTrue(bool(signals["v5_a_baseline"].iloc[0]))
        self.assertFalse(bool(signals["v5_d_close_and_rsi"].iloc[0]))
        self.assertFalse(bool(signals["v5_d_close_and_rsi"].iloc[1]))
        self.assertTrue(bool(signals["v5_d_close_and_rsi"].iloc[2]))
        self.assertTrue(bool(signals["v5_e_close_rsi_ema55"].iloc[2]))
        self.assertTrue(bool(signals["v5_f_close_breakdown"].iloc[2]))


if __name__ == "__main__":
    unittest.main()
