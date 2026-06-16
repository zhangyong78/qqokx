from decimal import Decimal
from unittest import TestCase

from okx_quant.backtest import BacktestTrade
from scripts.build_best_parameter_bundle import _combined_period_report, _combined_period_rows


def _trade(*, signal: str, entry_ts: int, exit_ts: int, pnl: str) -> BacktestTrade:
    pnl_value = Decimal(pnl)
    return BacktestTrade(
        signal=signal,
        entry_index=0,
        exit_index=1,
        entry_ts=entry_ts,
        exit_ts=exit_ts,
        entry_price=Decimal("100"),
        exit_price=Decimal("101"),
        stop_loss=Decimal("99"),
        take_profit=Decimal("110"),
        size=Decimal("1"),
        gross_pnl=pnl_value,
        pnl=pnl_value,
        risk_value=Decimal("10"),
        r_multiple=Decimal("1"),
        exit_reason="take_profit",
    )


class BestParameterBundlePeriodTablesTest(TestCase):
    def test_combined_period_rows_merge_long_short_and_keep_equity_path(self) -> None:
        long_trades = (
            _trade(signal="long", entry_ts=1735686000000, exit_ts=1735689600000, pnl="100"),
            _trade(signal="long", entry_ts=1738364400000, exit_ts=1738368000000, pnl="-50"),
        )
        short_trades = (
            _trade(signal="short", entry_ts=1735772400000, exit_ts=1735776000000, pnl="30"),
        )

        yearly_rows = _combined_period_rows(long_trades, short_trades, by="year")
        monthly_rows = _combined_period_rows(long_trades, short_trades, by="month")

        self.assertEqual(
            yearly_rows,
            (
                (
                    "2025",
                    2,
                    "50.0000",
                    1,
                    "30.0000",
                    3,
                    "66.67%",
                    "80.0000",
                    "0.80%",
                    "50.0000",
                    "0.49%",
                    "10080.0000",
                ),
            ),
        )
        self.assertEqual(
            monthly_rows,
            (
                (
                    "2025-01",
                    1,
                    "100.0000",
                    1,
                    "30.0000",
                    2,
                    "100.00%",
                    "130.0000",
                    "1.30%",
                    "0.0000",
                    "0.00%",
                    "10130.0000",
                ),
                (
                    "2025-02",
                    1,
                    "-50.0000",
                    0,
                    "0.0000",
                    1,
                    "0.00%",
                    "-50.0000",
                    "-0.49%",
                    "50.0000",
                    "0.49%",
                    "10080.0000",
                ),
            ),
        )

    def test_combined_period_report_uses_merged_equity_curve(self) -> None:
        long_trades = (
            _trade(signal="long", entry_ts=1735686000000, exit_ts=1735689600000, pnl="100"),
            _trade(signal="long", entry_ts=1738364400000, exit_ts=1738368000000, pnl="-50"),
        )
        short_trades = (
            _trade(signal="short", entry_ts=1735772400000, exit_ts=1735776000000, pnl="30"),
        )

        report = _combined_period_report(long_trades, short_trades)

        self.assertEqual(report.total_trades, 3)
        self.assertEqual(str(report.total_pnl), "80")
        self.assertEqual(str(report.ending_equity), "10080")
