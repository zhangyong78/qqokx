from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from report import answer_compare, format_rejection_reason


class ReportHelpersTest(unittest.TestCase):
    def test_answer_compare_rejects_low_sample_winner_claims(self) -> None:
        comparison = pd.DataFrame(
            [
                {"strategy_name": "strategy_d_first_pullback", "score": -1.0, "trade_count": 3},
                {"strategy_name": "strategy_e_second_pullback", "score": 2.0, "trade_count": 1},
            ]
        )

        answer = answer_compare(comparison, "strategy_d_first_pullback", "strategy_e_second_pullback")
        self.assertIn("Insufficient sample", answer)
        self.assertIn("3 trades", answer)
        self.assertIn("1 trades", answer)

    def test_format_rejection_reason_lists_specific_failures(self) -> None:
        row = pd.Series(
            {
                "strategy_name": "strategy_d_first_pullback",
                "trade_count": 3,
                "profit_factor": 0.0169,
                "average_R": -0.2399,
            }
        )

        reason = format_rejection_reason(row)
        self.assertIn("trade_count 3 < 30", reason)
        self.assertIn("profit_factor 0.02 <= 1.00", reason)
        self.assertIn("average_R -0.24 <= 0", reason)


if __name__ == "__main__":
    unittest.main()
