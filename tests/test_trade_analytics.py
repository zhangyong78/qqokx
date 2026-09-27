from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase

from okx_quant.account_equity import record_account_equity
from okx_quant.daily_trade_report import REPORT_TIMEZONE, _datetime, build_daily_trade_report, report_to_csv, report_to_html
from okx_quant.persistence import load_account_equity_curve_records
from okx_quant.trade_analytics import completed_trades, cumulative_pnl, daily_pnl, metrics, opening_equity, position_history_trade


def history_item(pnl="10", *, day=1, currency="USDT", kind="SWAP", partial=False):
    closed = datetime(2026, 9, day, 18, tzinfo=REPORT_TIMEZONE)
    opened = closed - timedelta(hours=6)
    return SimpleNamespace(
        inst_id="BTC-USDT-SWAP" if currency == "USDT" else "BTC-USD-260925",
        inst_type=kind, direction="long", pos_side="net", update_time=int(closed.timestamp() * 1000),
        open_avg_price=Decimal("70000"), close_avg_price=Decimal("71000"), close_size=Decimal("1"),
        realized_pnl=Decimal(pnl) if pnl is not None else None, pnl=None, settle_pnl=None,
        fee=Decimal("-1"), funding_fee=Decimal("-.2"),
        raw={"cTime": str(int(opened.timestamp() * 1000)), "posId": str(day), "type": "1" if partial else "2", "ccy": currency},
    )


def trade(pnl="10", **kwargs):
    return position_history_trade(history_item(pnl, **kwargs), profile_name="test", environment="demo")


class TradeAnalyticsTests(TestCase):
    def test_native_currency_is_not_mislabeled_as_usdt(self):
        item = history_item(".01", currency="BTC", kind="FUTURES")
        missing = position_history_trade(item, profile_name="a", environment="live")
        self.assertIsNone(missing.net_pnl)
        self.assertEqual(missing.native_net_pnl, Decimal(".01"))
        valued = position_history_trade(item, profile_name="a", environment="live", usdt_prices={"BTC": "60000"})
        self.assertEqual(valued.net_pnl, Decimal("600"))
        self.assertIn("参考汇率", valued.valuation_note)
        self.assertEqual(valued.pnl_currency, "BTC")

    def test_zero_realized_pnl_is_preserved_and_fees_not_deducted_twice(self):
        item = history_item("0")
        item.pnl = Decimal("1.2")
        row = position_history_trade(item, profile_name="a", environment="demo")
        self.assertEqual(row.net_pnl, Decimal("0"))
        self.assertEqual(row.gross_pnl, Decimal("1.2"))
        item.realized_pnl = None
        row = position_history_trade(item, profile_name="a", environment="demo")
        self.assertEqual(row.net_pnl, Decimal("0"))

    def test_partial_positions_and_opened_in_range_closed_outside_are_excluded(self):
        rows = [trade("10", day=1), trade("900", day=2, partial=True), trade("20", day=3)]
        selected = completed_trades(rows, date(2026, 9, 1), date(2026, 9, 2))
        self.assertEqual(len(selected), 1)
        report = build_daily_trade_report(rows, start_date=date(2026, 9, 1), end_date=date(2026, 9, 2))
        self.assertEqual(sum(row.closed_count for row in report.daily), 1)
        self.assertEqual(sum(row.net_pnl for row in report.daily), Decimal("10"))

    def test_metrics_calendar_curve_and_summary_reconcile(self):
        rows = [trade("10", day=1), trade("20", day=2), trade("-5", day=3), trade("0", day=4), trade(None, day=5)]
        result = metrics(rows)
        self.assertEqual((result.closed_count, result.valued_count), (5, 4))
        self.assertEqual((result.win_count, result.loss_count, result.flat_count), (2, 1, 1))
        self.assertEqual(result.win_rate, Decimal("50"))
        self.assertEqual(result.average_pnl, Decimal("6.25"))
        self.assertEqual(result.payoff_ratio, Decimal("3"))
        self.assertEqual(result.profit_factor, Decimal("6"))
        self.assertEqual(result.max_win_streak, 2)
        self.assertEqual(result.average_seconds, 21600)
        self.assertIsNone(result.average_r)
        self.assertIsNone(daily_pnl(rows)[date(2026, 9, 5)])
        points = cumulative_pnl(rows, date(2026, 9, 1), date(2026, 9, 6))
        self.assertEqual(points[-1][1], result.net_pnl)
        self.assertEqual(len(points), 6)

    def test_risk_mean_only_uses_valid_initial_risk(self):
        rows = [replace(trade("10"), risk_amount=Decimal("5")), replace(trade("-4", day=2), risk_amount=Decimal("2")), trade("500", day=3)]
        result = metrics(rows)
        self.assertEqual(result.risk_count, 2)
        self.assertEqual(result.average_r, 0)

    def test_invalid_numbers_and_timestamps_do_not_create_fake_pnl(self):
        for value in ("NaN", "Infinity", "-Infinity"):
            self.assertIsNone(trade(value).net_pnl)
        for value in (float("nan"), float("inf"), 10 ** 30):
            self.assertIsNone(_datetime(value))

    def test_exports_preserve_zero_and_explain_currency_valuation(self):
        rows = [trade("0"), trade(".1", day=2, currency="BTC", kind="FUTURES")]
        report = build_daily_trade_report(rows, start_date=date(2026, 9, 1), end_date=date(2026, 9, 2))
        import csv
        import io
        output = list(csv.DictReader(io.StringIO(report_to_csv(report))))
        self.assertIn("BTC", report_to_csv(report))
        self.assertIn("缺少 BTC 汇率", report_to_html(report))
        self.assertEqual(len(output), 2)
        self.assertEqual(output[1]["净盈亏"], "0")
        self.assertEqual(output[0]["净盈亏"], "")

    def test_empty_single_sign_and_missing_data_are_not_zero_divisions(self):
        for rows in ([], [trade(None)], [trade("10")], [trade("-10")], [trade("0")]):
            with self.subTest(rows=rows):
                self.assertIsNone(metrics(rows).payoff_ratio)
        self.assertIsNone(metrics([replace(trade(), opened_at=None)]).average_seconds)

    def test_date_bucketing_is_shanghai_even_for_utc_inputs(self):
        row = replace(trade(), opened_at=None, closed_at=datetime(2026, 8, 31, 18, tzinfo=timezone.utc))
        report = build_daily_trade_report([row], start_date=date(2026, 9, 1), end_date=date(2026, 9, 1))
        self.assertEqual(report.daily[0].report_date, date(2026, 9, 1))
        self.assertIn("结算币种", report_to_csv(report))

    def test_hourly_equity_samples_are_isolated_and_do_not_rewrite_same_hour(self):
        account = SimpleNamespace(total_equity=Decimal("100"), details=())
        stamp = datetime(2026, 9, 1, 12, 5, tzinfo=timezone.utc)
        with TemporaryDirectory() as temp:
            root = Path(temp)
            self.assertTrue(record_account_equity("a", "live", account, sampled_at=stamp, base_dir=root))
            self.assertFalse(record_account_equity("a", "live", account, sampled_at=stamp + timedelta(minutes=20), base_dir=root))
            self.assertTrue(record_account_equity("a", "live", account, sampled_at=stamp + timedelta(hours=1), base_dir=root))
            self.assertTrue(record_account_equity("a", "demo", account, sampled_at=stamp, base_dir=root))
            self.assertEqual(len(load_account_equity_curve_records("a", "live", base_dir=root)), 2)
            self.assertEqual(len(load_account_equity_curve_records("a", "demo", base_dir=root)), 1)
            self.assertEqual(load_account_equity_curve_records("b", "live", base_dir=root), [])
            self.assertFalse(record_account_equity("a", "live", SimpleNamespace(total_equity=None), sampled_at=stamp, base_dir=root))

    def test_return_denominator_requires_sample_at_start_not_future_equity(self):
        records = [{"time": "2026-08-31T16:05:00Z", "total_equity": "100"}]
        self.assertIsNone(opening_equity(records, date(2026, 9, 1)))
        records.append({"time": "2026-08-31T15:55:00Z", "total_equity": "80"})
        self.assertEqual(opening_equity(records, date(2026, 9, 1)), Decimal("80"))
