from __future__ import annotations

import sys
import unittest
from pathlib import Path


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from backtester import position_size_for_short


class PositionSizingTest(unittest.TestCase):
    def test_position_size_uses_fixed_risk_over_stop_distance(self) -> None:
        position_size, risk_amount = position_size_for_short(
            entry_price=100000.0,
            stop_loss=102000.0,
            current_equity=100000.0,
            risk_per_trade=0.005,
        )
        self.assertAlmostEqual(risk_amount, 500.0)
        self.assertAlmostEqual(position_size, 0.25)


if __name__ == "__main__":
    unittest.main()
