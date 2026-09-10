from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from okx_quant.daily_trade_report import (
    REPORT_TIMEZONE,
    build_daily_trade_report,
    daily_trade_from_strategy_ledger,
    report_to_csv,
    format_report_price,
)
from okx_quant.engine import _live_dynamic_break_even_setting_text
from okx_quant.models import StrategyConfig
from okx_quant.persistence import list_history_cache_scopes, save_history_cache_records


class DailyTradeReportTests(unittest.TestCase):
    def test_dynamic_break_even_log_includes_trigger_r(self) -> None:
        config = StrategyConfig(
            inst_id="DOGE-USDT-SWAP", bar="1H", ema_period=21, atr_period=10,
            atr_stop_multiplier=Decimal("1"), atr_take_multiplier=Decimal("1"),
            order_size=Decimal("1"), trade_mode="live", signal_mode="long_only",
            position_mode="net", environment="live", tp_sl_trigger_type="mark",
            dynamic_break_even_trigger_r=2,
        )
        self.assertEqual(_live_dynamic_break_even_setting_text(config), "nR保本=开启（n=2R）")

    def test_report_price_hides_excess_doge_precision(self) -> None:
        self.assertEqual(format_report_price(Decimal("0.0853998458574181"), "DOGE-USDT-SWAP"), "0.0854")
        self.assertEqual(format_report_price(Decimal("80000.123456789"), "BTC-USDT-SWAP"), "80000.12345679")

    def test_history_cache_scopes_are_discovered_without_credentials(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            save_history_cache_records("positions", "159", "live", [], base_dir=base_dir)
            save_history_cache_records("positions", "moni", "demo", [], base_dir=base_dir)

            self.assertEqual(
                list_history_cache_scopes(base_dir=base_dir),
                [("159", "live"), ("moni", "demo")],
            )

    def test_groups_closed_trades_by_close_date_and_keeps_open_trade(self) -> None:
        records = [
            SimpleNamespace(
                record_id="r1", api_name="ReapAi", environment="live", symbol="BTC-USDT-SWAP",
                strategy_name="EMA", session_id="S1", direction_label="只做多",
                opened_at=datetime(2026, 9, 1, 10, tzinfo=REPORT_TIMEZONE),
                closed_at=datetime(2026, 9, 2, 11, tzinfo=REPORT_TIMEZONE),
                entry_price="100", exit_price="102", size="1", entry_fee="-0.1", exit_fee="-0.1",
                funding_fee="-0.02", gross_pnl="2", net_pnl="1.78", close_reason="止盈",
            ),
            SimpleNamespace(
                record_id="r2", api_name="xhb", environment="live", symbol="ETH-USDT-SWAP",
                strategy_name="EMA", session_id="S2", direction_label="只做多",
                opened_at=datetime(2026, 9, 2, 12, tzinfo=REPORT_TIMEZONE), closed_at=None,
                entry_price="200", exit_price=None, size="2", entry_fee="0", exit_fee=None,
                funding_fee=None, gross_pnl=None, net_pnl=None, close_reason="",
            ),
        ]
        trades = [daily_trade_from_strategy_ledger(item) for item in records]
        report = build_daily_trade_report(
            trades,
            start_date=datetime(2026, 9, 1, tzinfo=REPORT_TIMEZONE).date(),
            end_date=datetime(2026, 9, 2, tzinfo=REPORT_TIMEZONE).date(),
        )
        self.assertEqual(len(report.trades), 2)
        reap_day = next(row for row in report.daily if row.api_name == "ReapAi")
        xhb_day = next(row for row in report.daily if row.api_name == "xhb")
        self.assertEqual(reap_day.closed_count, 1)
        self.assertEqual(reap_day.net_pnl, Decimal("1.78"))
        self.assertEqual(reap_day.open_count, 0)
        self.assertEqual(xhb_day.open_count, 1)
        self.assertIn("平仓日期", report_to_csv(report).splitlines()[0])
        eth_report = build_daily_trade_report(
            trades,
            start_date=datetime(2026, 9, 1, tzinfo=REPORT_TIMEZONE).date(),
            end_date=datetime(2026, 9, 2, tzinfo=REPORT_TIMEZONE).date(),
            asset_filter="ETH",
        )
        self.assertEqual(len(eth_report.trades), 1)
        self.assertEqual(eth_report.trades[0].symbol, "ETH-USDT-SWAP")

    def test_report_normalizes_mixed_naive_and_aware_trade_times(self) -> None:
        record = SimpleNamespace(
            record_id="mixed-1", api_name="ReapAi", environment="live", symbol="BTC-USDT-SWAP",
            strategy_name="EMA", session_id="S1", direction_label="只做多",
            opened_at=datetime(2026, 9, 4, 10), closed_at=datetime(2026, 9, 4, 11),
            entry_price="100", exit_price="102", size="1", entry_fee="0", exit_fee="0",
            funding_fee="0", gross_pnl="2", net_pnl="2", close_reason="止盈",
        )
        naive_trade = replace(
            daily_trade_from_strategy_ledger(record),
            opened_at=datetime(2026, 9, 4, 10),
            closed_at=datetime(2026, 9, 4, 11),
        )
        aware_trade = replace(
            naive_trade,
            opened_at=datetime(2026, 9, 4, 12, tzinfo=REPORT_TIMEZONE),
            closed_at=datetime(2026, 9, 4, 13, tzinfo=REPORT_TIMEZONE),
            trade_key="mixed-2",
        )
        report = build_daily_trade_report(
            [naive_trade, aware_trade],
            start_date=datetime(2026, 9, 4).date(),
            end_date=datetime(2026, 9, 4).date(),
        )
        self.assertEqual(len(report.trades), 2)
        self.assertEqual(report.trades[0].trade_key, "mixed-2")
        self.assertEqual(report.trades[0].closed_at.tzinfo, REPORT_TIMEZONE)


if __name__ == "__main__":
    unittest.main()
