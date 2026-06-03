from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from v10 import MonthlyStopRule, apply_monthly_stop_rule, summarize_guardrail_months


class V10MonthlyGuardrailTest(unittest.TestCase):
    def test_apply_monthly_stop_rule_skips_later_trades_after_threshold_hit(self) -> None:
        trades = pd.DataFrame(
            {
                "entry_time": pd.to_datetime(
                    [
                        "2024-01-01 04:00:00+00:00",
                        "2024-01-10 04:00:00+00:00",
                        "2024-01-20 04:00:00+00:00",
                        "2024-02-05 04:00:00+00:00",
                    ]
                ),
                "exit_time": pd.to_datetime(
                    [
                        "2024-01-03 04:00:00+00:00",
                        "2024-01-12 04:00:00+00:00",
                        "2024-01-22 04:00:00+00:00",
                        "2024-02-07 04:00:00+00:00",
                    ]
                ),
                "equity_before": [100000.0, 99200.0, 98500.0, 98500.0],
                "equity_after": [99200.0, 98500.0, 100000.0, 100500.0],
                "pnl_usdt": [-800.0, -700.0, 1500.0, 500.0],
            }
        )
        stop_rule = MonthlyStopRule("stop_1pct", 0.01, "demo")

        guarded, months = apply_monthly_stop_rule(
            trades,
            initial_capital=100000.0,
            stop_rule=stop_rule,
            strategy_name="demo",
        )

        self.assertEqual(len(guarded), 3)
        self.assertNotIn(pd.Timestamp("2024-01-20 04:00:00+00:00"), set(pd.to_datetime(guarded["entry_time"], utc=True)))
        january = months[months["month_label"] == "2024-01"].iloc[0]
        february = months[months["month_label"] == "2024-02"].iloc[0]
        self.assertTrue(bool(january["triggered"]))
        self.assertEqual(int(january["skipped_trade_count"]), 1)
        self.assertFalse(bool(february["triggered"]))

    def test_summarize_guardrail_months_counts_triggers_and_skips(self) -> None:
        months = pd.DataFrame(
            {
                "strategy_name": ["demo", "demo"],
                "stop_rule_name": ["rule", "rule"],
                "month_label": ["2024-01", "2024-02"],
                "month_start_equity": [100000.0, 99000.0],
                "month_realized_pnl": [-1200.0, 400.0],
                "month_return": [-0.012, 0.00404],
                "executed_trade_count": [2, 1],
                "skipped_trade_count": [1, 0],
                "triggered": [True, False],
                "trigger_exit_time": ["2024-01-12T04:00:00+00:00", ""],
            }
        )

        summary = summarize_guardrail_months(months)

        self.assertEqual(int(summary.iloc[0]["month_count"]), 2)
        self.assertEqual(int(summary.iloc[0]["triggered_month_count"]), 1)
        self.assertEqual(int(summary.iloc[0]["total_skipped_trades"]), 1)
        self.assertAlmostEqual(float(summary.iloc[0]["worst_month_return"]), -0.012)


if __name__ == "__main__":
    unittest.main()
