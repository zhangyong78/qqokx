from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from v9 import build_period_performance, build_streak_pressure, summarize_periods


class V9LiveReadinessTest(unittest.TestCase):
    def test_build_period_performance_groups_months_and_uses_period_start_for_drawdown(self) -> None:
        trades = pd.DataFrame(
            {
                "entry_time": pd.to_datetime(
                    [
                        "2024-01-05 04:00:00+00:00",
                        "2024-01-20 04:00:00+00:00",
                        "2024-02-03 04:00:00+00:00",
                    ]
                ),
                "exit_time": pd.to_datetime(
                    [
                        "2024-01-10 04:00:00+00:00",
                        "2024-01-25 04:00:00+00:00",
                        "2024-02-08 04:00:00+00:00",
                    ]
                ),
                "pnl_usdt": [100.0, -150.0, 200.0],
                "equity_after": [10100.0, 9950.0, 10150.0],
            }
        )

        periods = build_period_performance(
            trades,
            initial_capital=10000.0,
            freq="M",
            strategy_name="demo",
        )

        self.assertEqual(list(periods["period_label"]), ["2024-01", "2024-02"])
        self.assertAlmostEqual(float(periods.iloc[0]["period_return"]), -0.005)
        self.assertAlmostEqual(float(periods.iloc[0]["max_drawdown"]), -0.014851485148514865)
        self.assertAlmostEqual(float(periods.iloc[1]["period_start_equity"]), 9950.0)
        self.assertAlmostEqual(float(periods.iloc[1]["period_return"]), 200.0 / 9950.0)

    def test_build_streak_pressure_measures_longest_losing_run_and_rolling_windows(self) -> None:
        trades = pd.DataFrame(
            {
                "entry_time": pd.to_datetime(
                    [
                        "2024-01-01 04:00:00+00:00",
                        "2024-01-02 04:00:00+00:00",
                        "2024-01-03 04:00:00+00:00",
                        "2024-01-04 04:00:00+00:00",
                        "2024-01-05 04:00:00+00:00",
                    ]
                ),
                "pnl_usdt": [100.0, -30.0, -40.0, -50.0, 20.0],
            }
        )

        pressure = build_streak_pressure(trades, strategy_name="demo")

        self.assertEqual(int(pressure.iloc[0]["max_consecutive_losses"]), 3)
        self.assertAlmostEqual(float(pressure.iloc[0]["worst_consecutive_loss_sum"]), -120.0)
        self.assertAlmostEqual(float(pressure.iloc[0]["worst_3_trade_pnl"]), -120.0)
        self.assertAlmostEqual(float(pressure.iloc[0]["worst_5_trade_pnl"]), 0.0)
        self.assertAlmostEqual(float(pressure.iloc[0]["worst_10_trade_pnl"]), 0.0)

    def test_summarize_periods_aggregates_positive_rate_and_worst_return(self) -> None:
        periods = pd.DataFrame(
            {
                "strategy_name": ["demo", "demo", "demo"],
                "schedule_name": ["sched", "sched", "sched"],
                "cost_scenario_name": ["base", "base", "base"],
                "period_freq": ["M", "M", "M"],
                "period_return": [0.02, -0.01, 0.03],
                "max_drawdown": [-0.01, -0.03, -0.02],
                "profit_factor": [2.0, 0.8, 3.0],
                "trade_count": [2, 1, 3],
            }
        )

        summary = summarize_periods(periods)

        self.assertEqual(int(summary.iloc[0]["period_count"]), 3)
        self.assertEqual(int(summary.iloc[0]["positive_periods"]), 2)
        self.assertAlmostEqual(float(summary.iloc[0]["positive_period_rate"]), 2.0 / 3.0)
        self.assertAlmostEqual(float(summary.iloc[0]["worst_period_return"]), -0.01)
        self.assertAlmostEqual(float(summary.iloc[0]["median_profit_factor"]), 2.0)


if __name__ == "__main__":
    unittest.main()
