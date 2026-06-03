from __future__ import annotations

import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from indicators import add_features


class IndicatorsTest(unittest.TestCase):
    def test_add_features_keeps_length_and_builds_core_columns(self) -> None:
        count = 120
        frame = pd.DataFrame(
            {
                "timestamp": pd.date_range("2020-01-01", periods=count, freq="D", tz="UTC"),
                "open": np.linspace(100, 160, count),
                "high": np.linspace(101, 161, count),
                "low": np.linspace(99, 159, count),
                "close": np.linspace(100.5, 160.5, count),
                "volume": np.linspace(1000, 5000, count),
                "confirmed": True,
            }
        )

        features = add_features(frame)
        self.assertEqual(len(features), len(frame))
        for column in ("ema21", "ema55", "atr14", "rsi14", "vol_ma20"):
            self.assertIn(column, features.columns)
            self.assertFalse(pd.isna(features[column].iloc[-1]))


if __name__ == "__main__":
    unittest.main()
