from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from v4 import align_daily_environment_to_entry_frame, build_daily_environment_state, build_v4_signals


class V4RegimeTest(unittest.TestCase):
    def test_daily_environment_state_builds_expected_flags(self) -> None:
        daily = pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=120, freq="D", tz="UTC"),
                "open": [100.0] * 120,
                "high": [102.0] * 120,
                "low": [95.0] * 120,
                "close": [94.0] * 120,
                "volume": [150.0] * 120,
                "ema21": [96.0] * 120,
                "ema55": [100.0] * 120,
                "atr14": [5.0] * 119 + [8.0],
                "rsi14": [45.0] * 120,
                "vol_ma20": [100.0] * 120,
                "low_prev": [96.0] * 120,
                "ema55_slope_5": [-1.0] * 120,
            }
        )
        config = {
            "volume_filter_multiplier": 1.0,
            "v4_daily_rsi_min": 38.0,
            "v4_daily_rsi_max": 55.0,
            "v4_daily_volume_strong_multiplier": 1.2,
            "v4_daily_trend_gap_min": 0.01,
        }

        state = build_daily_environment_state(daily, config)
        row = state.iloc[-1]

        self.assertTrue(bool(row["env_baseline"]))
        self.assertTrue(bool(row["env_daily_atr_expansion"]))
        self.assertTrue(bool(row["env_volume_strong"]))
        self.assertTrue(bool(row["env_breakdown_and_slope"]))

    def test_v4_signal_is_gated_by_environment(self) -> None:
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
                "env_baseline": [False, True],
                "env_close_below_ema21": [False, True],
                "env_ema55_down": [False, True],
                "env_rsi_rebound": [False, True],
                "env_atr_expansion": [False, True],
                "env_volume_strong": [False, True],
                "env_trend_gap_strong": [False, True],
                "env_breakdown_and_slope": [False, True],
            }
        )
        config = {"rsi_filter_threshold": 45.0, "volume_filter_multiplier": 1.0}

        signals = build_v4_signals(frame, config)

        self.assertFalse(bool(signals["v4_a_baseline"].iloc[0]))
        self.assertTrue(bool(signals["v4_a_baseline"].iloc[1]))
        self.assertTrue(bool(signals["v4_h_breakdown_and_slope"].iloc[1]))

    def test_environment_alignment_uses_completed_daily_bar_only(self) -> None:
        daily = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2024-01-01 00:00:00+00:00", "2024-01-02 00:00:00+00:00"]),
                "close": [94.0, 94.0],
                "volume": [90.0, 150.0],
                "ema21": [96.0, 96.0],
                "ema55": [100.0, 100.0],
                "atr14": [5.0, 5.0],
                "rsi14": [45.0, 45.0],
                "vol_ma20": [100.0, 100.0],
                "low_prev": [96.0, 96.0],
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
                "open": [100.0] * 4,
                "high": [101.0] * 4,
                "low": [98.0] * 4,
                "close": [99.0] * 4,
                "volume": [120.0] * 4,
                "low_prev": [100.0] * 4,
                "ema21": [100.0] * 4,
                "ema55": [101.0] * 4,
                "rsi14": [50.0] * 4,
                "vol_ma20": [100.0] * 4,
                "ema55_slope_5": [-1.0] * 4,
            }
        )
        config = {
            "volume_filter_multiplier": 1.0,
            "v4_daily_rsi_min": 38.0,
            "v4_daily_rsi_max": 55.0,
            "v4_daily_volume_strong_multiplier": 1.2,
            "v4_daily_trend_gap_min": 0.01,
        }

        state = build_daily_environment_state(daily, config)
        aligned = align_daily_environment_to_entry_frame(entry, state)

        self.assertFalse(bool(aligned.iloc[0]["env_baseline"]))
        self.assertFalse(bool(aligned.iloc[1]["env_baseline"]))
        self.assertTrue(bool(aligned.iloc[2]["env_baseline"]))
        self.assertTrue(bool(aligned.iloc[3]["env_baseline"]))


if __name__ == "__main__":
    unittest.main()
