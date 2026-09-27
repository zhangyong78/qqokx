from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from math import sin
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from PySide6.QtCore import QDate, QSignalBlocker, Qt
from PySide6.QtGui import QFont, QFontDatabase
from PySide6.QtTest import QSignalSpy

from tests.qt_test_case import QtWidgetTestCase
from tests.test_trade_analytics import trade
from okx_quant.daily_trade_report import REPORT_TIMEZONE
from roll_terminal_qt.daily_trade_report_window import DailyTradeReportWidget
from roll_terminal_qt.realtime_account_store import AccountRealtimeSnapshot, RealtimeAccountStore
from tests.test_qt_realtime_account_store import _FakeRealtimeClient, _runtime
from roll_terminal_qt.trade_report_service import ReportLoadTask
from roll_terminal_qt.history_service import load_local_position_history_all


def demo_trades():
    values = [260, -185, 440, 820, -370, 125, 210, -86, 630, -110, 90, 460, -265, 830, 590, -325, 170, 460, -198, 780, 95, -310, 610, 150, -225, 370, 520]
    symbols = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP")
    return [replace(trade(str(value), day=index + 1), symbol=symbols[index % 3], direction="long" if index % 2 else "short",
                    risk_amount=Decimal("200"), pnl_ratio=Decimal(value) / 20000) for index, value in enumerate(values)]


def demo_account():
    return SimpleNamespace(total_equity=Decimal("68240.50"), available_equity=Decimal("52430"), unrealized_pnl=Decimal("380.25"),
                           details=tuple(SimpleNamespace(ccy=ccy, equity=Decimal(equity), equity_usd=Decimal(usd))
                                         for ccy, equity, usd in (("BTC", ".5", "36000"), ("USDT", "22000", "22000"), ("ETH", "2", "8400"), ("SOL", "10", "1700"), ("DOGE", "1000", "140.50"))))


class TradeAnalyticsQtTests(QtWidgetTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        font_path = Path("C:/Windows/Fonts/msyh.ttc")
        if font_path.exists():
            QFontDatabase.addApplicationFont(str(font_path))
            cls._app.setFont(QFont("Microsoft YaHei", 9))

    def setUp(self):
        self.store = MagicMock()
        self.store.snapshot_for.return_value = None
        self.manager = MagicMock()
        self.patches = [
            patch("roll_terminal_qt.daily_trade_report_window.get_shared_realtime_account_store", return_value=self.store),
            patch("roll_terminal_qt.daily_trade_report_window.get_history_sync_manager", return_value=self.manager),
            patch("roll_terminal_qt.daily_trade_report_window.load_runtime", return_value=SimpleNamespace(environment="demo")),
        ]
        for item in self.patches:
            item.start()
        self.widget = DailyTradeReportWidget(profile_name="test")
        self.widget._reload_timer.stop()
        self.widget._local_timer.stop()
        with QSignalBlocker(self.widget._start_date), QSignalBlocker(self.widget._end_date):
            self.widget._start_date.setDate(QDate(2026, 9, 1))
            self.widget._end_date.setDate(QDate(2026, 9, 27))
        self.widget._all_trades = demo_trades()
        start = datetime(2026, 9, 1, tzinfo=REPORT_TIMEZONE)
        self.widget._equity_records = [{"time": (start + timedelta(hours=hour)).isoformat(),
                                        "total_equity": str(62000 + hour * 9.64 + sin(hour / 18) * 650)}
                                       for hour in range(27 * 24)]
        self.widget._refresh_asset_options(self.widget._all_trades)
        self.widget._render_loaded_report()

    def tearDown(self):
        self.widget.begin_shutdown()
        self.dispose_widget(self.widget)
        for item in reversed(self.patches):
            item.stop()

    def test_calendar_filters_details_and_contract_metrics(self):
        self.assertEqual([self.widget._tabs.tabText(i) for i in range(6)], ["分析概览", "合约分析", "每日汇总", "品种汇总", "策略汇总", "交易明细"])
        self.widget._overview.calendar.day_clicked.emit(date(2026, 9, 2))
        visible = sum(not self.widget._detail_table.isRowHidden(i) for i in range(self.widget._detail_table.rowCount()))
        self.assertEqual(visible, 1)
        self.assertIs(self.widget._tabs.currentWidget(), self.widget._detail_page)
        self.widget._contracts._select_day(date(2026, 9, 2))
        self.assertEqual(self.widget._contracts.stat_values["平仓仓位数"].text(), "1")
        self.widget._contracts._select_day(None)
        self.assertEqual(self.widget._contracts.stat_values["平仓仓位数"].text(), "27")

    def test_asset_filter_updates_all_report_views_and_numeric_sort(self):
        self.widget._asset_combo.setCurrentText("BTC")
        self.assertEqual(len(self.widget._report.trades), 9)
        self.assertEqual(self.widget._contracts.contract_table.rowCount(), 1)
        self.assertEqual(self.widget._detail_table.rowCount(), 9)
        self.widget._asset_combo.setCurrentIndex(0)
        table = self.widget._contracts.contract_table
        table.sortItems(1, Qt.SortOrder.DescendingOrder)
        values = [table.item(row, 1).value for row in range(table.rowCount())]
        self.assertEqual(values, sorted(values, reverse=True))

    def test_asset_filter_scopes_phase_two_spot_data(self):
        spot = SimpleNamespace(inst_id="BTC-USDT", inst_type="SPOT", side="buy", fill_price=Decimal("100"),
                               fill_size=Decimal("1"), fill_fee=Decimal("0"), fee_currency="USDT", fill_time=1)
        self.widget._fills = [spot]
        self.widget._asset_combo.setCurrentText("ETH")
        self.assertEqual(self.widget._overview.phase2_fills, ())

    def test_profile_change_rejects_old_background_results_and_snapshots(self):
        previous_generation = self.widget._generation
        self.widget.apply_workspace_profile("other")
        self.widget._reload_timer.stop()
        self.widget._on_loaded(previous_generation, {"trades": demo_trades(), "items": [], "equity": []})
        self.widget._reload_timer.stop()
        self.assertEqual(self.widget._all_trades, [])
        snapshot = AccountRealtimeSnapshot("test", "demo", (), (), demo_account(), 1, "rest")
        self.widget._apply_account_snapshot(snapshot)
        self.assertIsNone(self.widget._snapshot)

    def test_missing_return_basis_and_empty_charts_render_without_errors(self):
        self.widget._equity_records = []
        self.widget._all_trades = []
        self.widget._render_loaded_report()
        self.widget._overview.mode.setCurrentIndex(1)
        self.assertEqual(self.widget._overview.return_card.value.text(), "--")
        self.assertIn("缺少", self.widget._overview.pnl_curve.empty.text())
        self.assertEqual(self.widget._contracts.contract_table.rowCount(), 0)

    def test_asset_ring_has_visible_thickness_and_updates(self):
        dashboard = self.widget._overview
        for _ in range(3):
            dashboard.set_account(demo_account(), asset_filter="BTC")
            series = dashboard.pie_chart.series()[0]
            self.assertGreater(series.pieSize(), series.holeSize())
            self.assertEqual(series.count(), 5)
            self.assertAlmostEqual(series.sum(), 68240.5)
            self.assertTrue(series.slices()[0].isExploded())
        dashboard.set_account(None)
        self.assertTrue(dashboard.pie_view.isHidden())

    def test_fresh_rest_snapshot_is_sampled_and_disk_failure_is_nonfatal(self):
        store = RealtimeAccountStore(client=_FakeRealtimeClient())
        spy = QSignalSpy(store._reconcile_completed)
        try:
            with patch("roll_terminal_qt.realtime_account_store.record_account_equity") as sample:
                store._run_reconcile_worker(0, "test", _runtime("sample", "demo"))
                self.assertEqual(sample.call_args.args[:2], ("sample", "demo"))
                self.assertIsNotNone(spy.at(0)[2]["account_updated_at"])
                sample.side_effect = OSError("disk full")
                store._run_reconcile_worker(0, "test", _runtime("sample", "demo"))
                self.assertIn("disk full", spy.at(1)[2]["equity_error"])
                self.assertIn("pending_orders", spy.at(1)[2])
        finally:
            store.stop()

    def test_background_loader_reports_results_errors_and_never_rewrites_cache(self):
        with patch("roll_terminal_qt.trade_report_service.load_local_position_history_all", return_value=[]) as positions, \
             patch("roll_terminal_qt.trade_report_service.load_account_equity_curve_records", return_value=[]):
            task = ReportLoadTask(3, "test", "demo", {})
            spy = QSignalSpy(task.signals.finished)
            task.run()
            positions.assert_called_once_with("test", "demo", persist_collapsed=False)
            self.assertEqual(spy.at(0), [3, {"trades": [], "items": [], "equity": [], "fills": [], "bills": [], "asset_bills": []}])
            positions.side_effect = OSError("cache unavailable")
            task.run()
            self.assertIsInstance(spy.at(1)[1], OSError)
        with patch("roll_terminal_qt.history_service.load_history_cache_records", return_value=[{}]), \
             patch("roll_terminal_qt.history_service._collapse_position_history_records", return_value=[]), \
             patch("roll_terminal_qt.history_service.save_history_cache_records") as save:
            self.assertEqual(load_local_position_history_all("test", "demo", persist_collapsed=False), [])
            save.assert_not_called()

    def test_render_previews_with_explicit_demo_data(self):
        self.widget.resize(1560, 1060)
        self.widget._overview.set_account(demo_account(), updated_at=datetime(2026, 9, 27, 10, tzinfo=REPORT_TIMEZONE))
        self.widget.show()
        self._app.processEvents()
        output = Path("tests_artifacts/trade_analytics")
        output.mkdir(parents=True, exist_ok=True)
        self.assertTrue(self.widget.grab().save(str(output / "overview-demo.png")))
        self.widget._overview.verticalScrollBar().setValue(self.widget._overview.verticalScrollBar().maximum())
        self._app.processEvents()
        self.assertTrue(self.widget.grab().save(str(output / "overview-bottom-demo.png")))
        self.widget._tabs.setCurrentWidget(self.widget._contracts)
        self._app.processEvents()
        self.assertTrue(self.widget.grab().save(str(output / "contracts-demo.png")))
