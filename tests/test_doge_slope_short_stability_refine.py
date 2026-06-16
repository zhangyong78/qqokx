import unittest
from decimal import Decimal

from scripts.run_doge_slope_short_stability_refine import build_candidates


class DogeSlopeShortStabilityRefineTest(unittest.TestCase):
    def test_build_candidates_focus_on_stability_neighbors(self) -> None:
        candidates = {item.candidate_id: item for item in build_candidates()}

        self.assertEqual(
            set(candidates),
            {
                "final_best",
                "sl2_bear_reentry",
                "sl25",
                "sl25_bear_reentry",
                "sl3",
                "sl3_bear_reentry",
                "ma55_context",
            },
        )

        self.assertEqual(candidates["final_best"].config.atr_stop_multiplier, Decimal("2"))
        self.assertTrue(candidates["final_best"].config.ema55_slope_same_bar_reentry_block)
        self.assertFalse(candidates["final_best"].config.ema55_slope_dynamic_exit_requires_bear_reentry)

        self.assertEqual(candidates["sl25"].config.atr_stop_multiplier, Decimal("2.5"))
        self.assertFalse(candidates["sl25"].config.ema55_slope_dynamic_exit_requires_bear_reentry)

        self.assertEqual(candidates["sl3_bear_reentry"].config.atr_stop_multiplier, Decimal("3"))
        self.assertTrue(candidates["sl3_bear_reentry"].config.ema55_slope_dynamic_exit_requires_bear_reentry)
        self.assertTrue(candidates["sl3_bear_reentry"].config.ema55_slope_dynamic_exit_bear_reentry_break_prev_low)

        self.assertEqual(candidates["ma55_context"].config.ema_period, 55)
        self.assertEqual(candidates["ma55_context"].config.atr_period, 14)


if __name__ == "__main__":
    unittest.main()
