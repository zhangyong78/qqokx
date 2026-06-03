from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from v7 import RiskSchedule, simulate_dynamic_risk_trades, tag_strong_regime_trades


class V7DynamicRiskTest(unittest.TestCase):
    def test_tag_strong_regime_trades_marks_matching_signal_times(self) -> None:
        trades = pd.DataFrame(
            {
                "entry_signal_bar_time": pd.to_datetime(
                    ["2024-01-01 00:00:00+00:00", "2024-01-02 00:00:00+00:00"]
                ),
                "entry_time": pd.to_datetime(["2024-01-01 04:00:00+00:00", "2024-01-02 04:00:00+00:00"]),
            }
        )
        strong_times = pd.to_datetime(["2024-01-02 00:00:00+00:00"])

        tagged = tag_strong_regime_trades(trades, strong_times)

        self.assertFalse(bool(tagged.iloc[0]["is_strong_regime"]))
        self.assertTrue(bool(tagged.iloc[1]["is_strong_regime"]))

    def test_simulate_dynamic_risk_trades_rescales_pnl_by_regime_multiplier(self) -> None:
        trades = pd.DataFrame(
            {
                "entry_time": pd.to_datetime(["2024-01-01 04:00:00+00:00", "2024-01-02 04:00:00+00:00"]),
                "entry_signal_bar_time": pd.to_datetime(["2024-01-01 00:00:00+00:00", "2024-01-02 00:00:00+00:00"]),
                "risk_amount": [500.0, 500.0],
                "R_multiple": [1.0, -1.0],
                "position_size_btc": [1.0, 1.0],
                "notional_value": [1000.0, 1000.0],
                "entry_fee": [1.0, 1.0],
                "exit_fee": [1.0, 1.0],
                "is_strong_regime": [True, False],
            }
        )
        schedule = RiskSchedule(
            name="demo",
            strong_multiplier=1.5,
            weak_multiplier=0.5,
            description="demo",
        )

        simulated = simulate_dynamic_risk_trades(
            trades,
            initial_capital=100000.0,
            base_risk_per_trade=0.005,
            schedule=schedule,
        )

        self.assertAlmostEqual(float(simulated.iloc[0]["risk_amount"]), 750.0)
        self.assertAlmostEqual(float(simulated.iloc[0]["pnl_usdt"]), 750.0)
        self.assertAlmostEqual(float(simulated.iloc[1]["equity_before"]), 100750.0)
        self.assertAlmostEqual(float(simulated.iloc[1]["risk_amount"]), 251.875)
        self.assertAlmostEqual(float(simulated.iloc[1]["pnl_usdt"]), -251.875)


if __name__ == "__main__":
    unittest.main()
