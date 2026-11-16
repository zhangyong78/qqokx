"""Regression checks for rapid chart switching and close during a slow request."""
import threading
import time
from datetime import datetime
from decimal import Decimal
from unittest.mock import patch

from PySide6.QtCore import QCoreApplication, QEvent, QThread
from PySide6.QtGui import QFont
from shiboken6 import isValid

from tests.qt_test_case import QtWidgetTestCase
from roll_terminal_qt.kline_analysis_window import KlineAnalysisWindow, SecondaryVolatilityDataLoader
from roll_terminal_qt.shape_signal_dialog import ShapeSignalPreviewDialog
from okx_quant.models import Candle


class HeldLoader(QThread):
    def __init__(self):
        super().__init__()
        self.release = threading.Event()

    def run(self):
        self.release.wait(5)


class KlineLoaderLifecycleTests(QtWidgetTestCase):
    def setUp(self):
        self.runtime = patch('roll_terminal_qt.kline_analysis_window.load_runtime', return_value=None)
        self.runtime.start()
        self.addCleanup(self.runtime.stop)

    def drain(self, predicate):
        end = time.monotonic() + 3
        while not predicate() and time.monotonic() < end:
            self._app.processEvents()
            QCoreApplication.sendPostedEvents(None, QEvent.Type.DeferredDelete)
            time.sleep(.005)
        self.assertTrue(predicate())

    def test_trade_marker_menu_and_secondary_symbol_popup_after_switching(self):
        window = KlineAnalysisWindow(preview_mode=True)
        try:
            window.show()
            window._secondary_chart_check.setChecked(True)
            button = window._header_menu_buttons["交易标记"]
            menu = button.menu()
            for period, available in (("4H", False), ("1H", True), ("4H", False), ("1H", True)):
                window._period_combo.setCurrentText(period)
                window._refresh_history_trade_availability()
                menu.popup(button.mapToGlobal(button.rect().bottomLeft()))
                self._app.processEvents()
                self.assertTrue(window._history_trade_group.isVisible())
                self.assertEqual(window._show_history_trades_check.isVisible(), available)
                self.assertEqual(window._history_trade_unavailable_label.isVisible(), not available)
                for control in (window._show_history_trades_check, window._history_trade_direction_combo,
                                window._sync_history_trades_button, window._history_trade_unavailable_label):
                    if control.isVisible():
                        self.assertGreaterEqual(control.width(), control.minimumSizeHint().width())
                        self.assertLessEqual(control.geometry().right(), window._history_trade_group.width())
                menu.hide()
                self._app.processEvents()
            menu_font = QFont(window._history_trade_group.font())
            menu_font.setPointSize(20)
            window._history_trade_group.setFont(menu_font)
            menu.popup(button.mapToGlobal(button.rect().bottomLeft()))
            self._app.processEvents()
            for control in (window._show_history_trades_check, window._history_trade_direction_combo,
                            window._sync_history_trades_button):
                self.assertGreaterEqual(control.width(), control.minimumSizeHint().width())
            menu.hide()
            for combo in (window._secondary_symbol_combo, window._secondary_period_combo):
                font = QFont(combo.font())
                font.setPointSize(20)
                combo.setFont(font)
                combo.showPopup()
                self._app.processEvents()
                longest = max(combo.fontMetrics().horizontalAdvance(combo.itemText(i)) for i in range(combo.count()))
                self.assertGreaterEqual(combo.view().minimumWidth(), longest + 36)
                combo.hidePopup()
        finally:
            window.begin_shutdown()
            self.dispose_widget(window)

    def test_finished_but_not_delivered_loader_cannot_be_replaced(self):
        window = KlineAnalysisWindow(preview_mode=True)
        loader = HeldLoader()  # not running, but still owned pending finished delivery
        window._secondary_volatility_loader = loader
        try:
            for _ in range(20):
                window._load_secondary_data(symbol='BTC-USDT-SWAP')
            self.assertIs(window._secondary_volatility_loader, loader)
            self.assertTrue(window._has_active_loaders())
            self.assertTrue(window._pending_secondary_reload)
        finally:
            window._secondary_volatility_loader = None
            window.close()

    def test_stale_finished_signal_does_not_delete_new_loader(self):
        window = KlineAnalysisWindow(preview_mode=True)
        old, new = HeldLoader(), HeldLoader()
        window._secondary_volatility_loader = new
        old.finished.connect(window._on_secondary_volatility_loader_finished)
        try:
            old.finished.emit()
            self.assertIs(window._secondary_volatility_loader, new)
            QCoreApplication.sendPostedEvents(None, QEvent.Type.DeferredDelete)
            self.assertTrue(isValid(new))
        finally:
            window._secondary_volatility_loader = None
            window.close()

    def test_preview_close_and_escape_wait_for_worker(self):
        event = dict(symbol='BTC-USDT-SWAP', period='1H', candle_ts=1790600000000)
        for close_with_escape in (False, True):
            preview = ShapeSignalPreviewDialog(event=event, main_window=None)
            loader = HeldLoader()
            preview._kline._secondary_volatility_loader = loader
            loader.finished.connect(preview._kline._on_secondary_volatility_loader_finished)
            loader.start()
            try:
                self.assertFalse(preview._kline._rr_monitor_timer.isActive())
                preview.reject() if close_with_escape else preview.close()
                self._app.processEvents()
                QCoreApplication.sendPostedEvents(None, QEvent.Type.DeferredDelete)
                self.assertTrue(isValid(preview))
                self.assertTrue(loader.isRunning())
                self.assertFalse(preview._close_ready)
            finally:
                loader.release.set()
                loader.wait(2000)
            self.drain(lambda: not isValid(preview))

    def test_cache_only_volatility_never_calls_network(self):
        loader = SecondaryVolatilityDataLoader(request_id=1, currency='BTC', period='1H',
                                              limit=50, average_kline=False, local_only=True)
        with patch('roll_terminal_qt.kline_analysis_window._load_cached_deribit_hourly_series', return_value=None), \
             patch('roll_terminal_qt.kline_analysis_window.DeribitRestClient') as client:
            with self.assertRaises(ValueError):
                loader._build_payload()
            client.assert_not_called()

    def test_main_shutdown_waits_for_preview_workers(self):
        window = KlineAnalysisWindow(preview_mode=True)
        preview = ShapeSignalPreviewDialog(
            event=dict(symbol='BTC-USDT-SWAP', period='1H', candle_ts=1790600000000),
            main_window=window, parent=window,
        )
        loader = HeldLoader()
        preview._kline._secondary_volatility_loader = loader
        loader.finished.connect(preview._kline._on_secondary_volatility_loader_finished)
        loader.start()
        completed = []
        try:
            window.begin_shutdown(lambda: completed.append(True))
            self.assertEqual(completed, [])
            self.assertTrue(loader.isRunning())
        finally:
            loader.release.set()
            loader.wait(2000)
        self.drain(lambda: bool(completed))
        preview.close()
        window.close()

    def test_repeated_linked_chart_volatility_switches_settle(self):
        candles = [Candle(1790600000000 + i * 3600000, Decimal(100), Decimal(103),
                          Decimal(99), Decimal(102), Decimal(1), True) for i in range(60)]
        with patch('roll_terminal_qt.kline_analysis_window.load_candle_cache', return_value=candles), \
             patch('roll_terminal_qt.kline_analysis_window._prefer_native_chart_backend', return_value=True), \
             patch('roll_terminal_qt.kline_analysis_window._load_cached_deribit_hourly_series',
                   return_value=('BTC-USDT', candles, candles, datetime.now())), \
             patch.object(KlineAnalysisWindow, '_instrument_for_symbol', return_value=None), \
             patch('roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries', return_value={}):
            window = KlineAnalysisWindow(preview_mode=True)
            window.configure_shape_signal_preview(symbol='BTC-USDT-SWAP', period='1H', candle_ts=candles[-1].ts)
            window.show()
            try:
                for _ in range(20):
                    window._secondary_chart_check.setChecked(True)
                    window._on_secondary_chart_kind_cycle_clicked()
                    window._on_secondary_sync_period_clicked()
                    self._app.processEvents()
                    window._secondary_chart_check.setChecked(False)
                window._secondary_chart_check.setChecked(True)
                window._secondary_chart_kind_mode = 'volatility'
                window._load_data()
                self.drain(lambda: not window._has_active_loaders() and not window._load_request_timer.isActive())
                self.assertIsNotNone(window._secondary_pending_payload)
                self.assertIn('DVOL', window._secondary_pending_payload.stats['source'])
                self.assertIsNone(window._realtime_candle_unsubscribe)
            finally:
                window.close()
                self.drain(lambda: bool(getattr(window, '_shutdown_complete', False)))
