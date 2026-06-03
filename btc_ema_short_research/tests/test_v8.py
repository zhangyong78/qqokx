from __future__ import annotations

import sys
import unittest
from pathlib import Path


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from backtester import BacktestSettings
from v8 import apply_cost_scenario, v8_cost_scenarios, v8_risk_schedules


class V8StressTest(unittest.TestCase):
    def test_apply_cost_scenario_scales_fee_and_slippage(self) -> None:
        settings = BacktestSettings(
            initial_capital=100000.0,
            risk_per_trade=0.005,
            fee_rate=0.0004,
            slippage_rate=0.0002,
            stop_lookback=10,
            stop_atr_multiplier=1.0,
        )
        scenario = v8_cost_scenarios()[2]

        adjusted = apply_cost_scenario(settings, scenario)

        self.assertAlmostEqual(adjusted.fee_rate, 0.0008)
        self.assertAlmostEqual(adjusted.slippage_rate, 0.0004)
        self.assertEqual(adjusted.initial_capital, settings.initial_capital)

    def test_v8_risk_schedules_cover_expected_grid(self) -> None:
        schedules = v8_risk_schedules()
        names = [item.name for item in schedules]

        self.assertIn("v8_a_flat_1_0", names)
        self.assertIn("v8_d_strong_1_5_weak_0_5", names)
        self.assertEqual(len(schedules), 6)
        self.assertTrue(all(item.strong_multiplier >= item.weak_multiplier for item in schedules))


if __name__ == "__main__":
    unittest.main()
