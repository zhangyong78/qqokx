from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mtf import align_daily_filters_to_entry_frame, build_daily_filter_state, build_v2_signals


class MultiTimeframeTest(unittest.TestCase):
    def test_daily_filter_alignment_uses_only_completed_daily_bars(self) -> None:
        daily = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2024-01-01 00:00:00+00:00", "2024-01-02 00:00:00+00:00"]),
                "close": [99.0, 95.0],
                "volume": [90.0, 120.0],
                "ema21": [100.0, 94.0],
                "ema55": [101.0, 96.0],
                "rsi14": [40.0, 50.0],
                "vol_ma20": [100.0, 100.0],
                "ema55_slope_5": [-1.0, -1.0],
            }
        )
        entry = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    [
                        "2024-01-02 12:00:00+00:00",
                        "2024-01-02 16:00:00+00:00",
                        "2024-01-02 20:00:00+00:00",
                        "2024-01-03 00:00:00+00:00",
                    ]
                ),
                "open": [100.0, 100.0, 100.0, 100.0],
                "high": [101.0, 101.0, 101.0, 101.0],
                "low": [98.0, 98.0, 98.0, 98.0],
                "close": [99.0, 99.0, 99.0, 99.0],
                "volume": [100.0, 100.0, 100.0, 100.0],
                "low_prev": [100.0, 100.0, 100.0, 100.0],
                "ema21": [100.0, 100.0, 100.0, 100.0],
                "ema55": [101.0, 101.0, 101.0, 101.0],
                "rsi14": [50.0, 50.0, 50.0, 50.0],
                "vol_ma20": [100.0, 100.0, 100.0, 100.0],
                "ema55_slope_5": [-1.0, -1.0, -1.0, -1.0],
            }
        )
        config = {"rsi_filter_threshold": 45.0, "volume_filter_multiplier": 1.0}

        state = build_daily_filter_state(daily, config)
        aligned = align_daily_filters_to_entry_frame(entry, state)

        self.assertFalse(bool(aligned.iloc[0]["daily_filter_volume"]))
        self.assertFalse(bool(aligned.iloc[1]["daily_filter_volume"]))
        self.assertTrue(bool(aligned.iloc[2]["daily_filter_volume"]))
        self.assertTrue(bool(aligned.iloc[3]["daily_filter_volume"]))

    def test_v2_signal_is_gated_by_daily_filter(self) -> None:
        frame = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2024-01-03 00:00:00+00:00", "2024-01-03 04:00:00+00:00"]),
                "open": [100.0, 100.0],
                "high": [101.0, 101.0],
                "low": [98.0, 98.0],
                "close": [99.0, 99.0],
                "volume": [120.0, 120.0],
                "low_prev": [100.0, 100.0],
                "ema21": [100.0, 100.0],
                "ema55": [101.0, 101.0],
                "rsi14": [50.0, 50.0],
                "vol_ma20": [100.0, 100.0],
                "ema55_slope_5": [-1.0, -1.0],
                "daily_filter_core": [False, True],
                "daily_filter_rsi": [False, False],
                "daily_filter_volume": [False, True],
                "daily_filter_ema55": [False, True],
            }
        )
        config = {"rsi_filter_threshold": 45.0, "volume_filter_multiplier": 1.0}

        signals = build_v2_signals(frame, config)

        self.assertFalse(bool(signals["v2_d_daily_core_4h_dual_bear_volume"].iloc[0]))
        self.assertTrue(bool(signals["v2_d_daily_core_4h_dual_bear_volume"].iloc[1]))


if __name__ == "__main__":
    unittest.main()
