from datetime import datetime, timezone
from decimal import Decimal
from unittest import TestCase

from okx_quant.backtest import BacktestTrade
from scripts.run_doge_slope_short_overfit_validation import _add_months, compute_streak_stats


def _trade(*, exit_ts: int, pnl: str, exit_reason: str) -> BacktestTrade:
    pnl_value = Decimal(pnl)
    return BacktestTrade(
        signal="short",
        entry_index=0,
        exit_index=1,
        entry_ts=exit_ts - 3600_000,
        exit_ts=exit_ts,
        entry_price=Decimal("1"),
        exit_price=Decimal("1"),
        stop_loss=Decimal("1"),
        take_profit=Decimal("1"),
        size=Decimal("1"),
        gross_pnl=pnl_value,
        pnl=pnl_value,
        risk_value=Decimal("100"),
        r_multiple=Decimal("0"),
        exit_reason=exit_reason,
    )


class DogeSlopeShortOverfitValidationTest(TestCase):
    def test_add_months_keeps_month_start(self) -> None:
        self.assertEqual(
            _add_months(datetime(2022, 1, 1, tzinfo=timezone.utc), -18),
            datetime(2020, 7, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(
            _add_months(datetime(2025, 7, 1, tzinfo=timezone.utc), 6),
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

    def test_compute_streak_stats_tracks_loss_and_stop_sequences(self) -> None:
        trades = (
            _trade(exit_ts=1_000, pnl="-10", exit_reason="stop_loss"),
            _trade(exit_ts=2_000, pnl="-5", exit_reason="stop_loss"),
            _trade(exit_ts=3_000, pnl="20", exit_reason="take_profit"),
            _trade(exit_ts=4_000, pnl="-1", exit_reason="slope_turn_positive"),
            _trade(exit_ts=5_000, pnl="-2", exit_reason="stop_loss"),
            _trade(exit_ts=6_000, pnl="-3", exit_reason="stop_loss"),
            _trade(exit_ts=7_000, pnl="-4", exit_reason="stop_loss"),
        )

        stats = compute_streak_stats(trades)

        self.assertEqual(stats.max_loss_streak, 4)
        self.assertEqual(stats.max_stop_streak, 3)
        self.assertEqual(stats.stop_loss_count, 5)
        self.assertEqual(stats.slope_turn_count, 1)
