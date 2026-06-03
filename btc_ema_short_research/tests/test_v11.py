from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from v11 import LossThrottleRule, simulate_loss_throttle_trades, summarize_throttle_usage
from v7 import RiskSchedule


class V11TradeGuardrailTest(unittest.TestCase):
    def test_simulate_loss_throttle_trades_reduces_risk_after_trigger(self) -> None:
        trades = pd.DataFrame(
            {
                "entry_time": pd.to_datetime(
                    [
                        "2024-01-01 04:00:00+00:00",
                        "2024-01-02 04:00:00+00:00",
                        "2024-01-03 04:00:00+00:00",
                        "2024-01-04 04:00:00+00:00",
                    ]
                ),
                "entry_signal_bar_time": pd.to_datetime(
                    [
                        "2024-01-01 00:00:00+00:00",
                        "2024-01-02 00:00:00+00:00",
                        "2024-01-03 00:00:00+00:00",
                        "2024-01-04 00:00:00+00:00",
                    ]
                ),
                "risk_amount": [500.0, 500.0, 500.0, 500.0],
                "R_multiple": [-1.0, -1.0, -1.0, 2.0],
                "position_size_btc": [1.0, 1.0, 1.0, 1.0],
                "notional_value": [1000.0, 1000.0, 1000.0, 1000.0],
                "entry_fee": [1.0, 1.0, 1.0, 1.0],
                "exit_fee": [1.0, 1.0, 1.0, 1.0],
                "is_strong_regime": [False, False, False, False],
            }
        )
        schedule = RiskSchedule("flat", 1.0, 1.0, "demo")
        rule = LossThrottleRule("after_3", 3, 0.5, "demo")

        simulated = simulate_loss_throttle_trades(
            trades,
            initial_capital=100000.0,
            base_risk_per_trade=0.005,
            schedule=schedule,
            throttle_rule=rule,
        )

        self.assertFalse(bool(simulated.iloc[2]["throttle_active_before"]))
        self.assertTrue(bool(simulated.iloc[2]["throttle_activation_after_trade"]))
        self.assertTrue(bool(simulated.iloc[3]["throttle_active_before"]))
        self.assertAlmostEqual(float(simulated.iloc[3]["risk_amount"]), 246.26871875)
        self.assertAlmostEqual(float(simulated.iloc[3]["effective_risk_multiplier"]), 0.5)

    def test_summarize_throttle_usage_counts_activations_and_reduced_trades(self) -> None:
        trades = pd.DataFrame(
            {
                "strategy_name": ["demo", "demo", "demo"],
                "risk_schedule_name": ["sched", "sched", "sched"],
                "loss_throttle_rule_name": ["rule", "rule", "rule"],
                "throttle_activation_after_trade": [False, True, False],
                "throttle_active_before": [False, False, True],
                "effective_risk_multiplier": [1.0, 1.0, 0.5],
                "pnl_usdt": [-100.0, -100.0, 200.0],
            }
        )

        summary = summarize_throttle_usage(trades)

        self.assertEqual(int(summary.iloc[0]["activation_count"]), 1)
        self.assertEqual(int(summary.iloc[0]["reduced_risk_trade_count"]), 1)
        self.assertAlmostEqual(float(summary.iloc[0]["reduced_risk_win_rate"]), 1.0)
        self.assertAlmostEqual(float(summary.iloc[0]["average_effective_risk_multiplier"]), 5.0 / 6.0)


if __name__ == "__main__":
    unittest.main()
