from decimal import Decimal
from unittest.mock import patch

from PySide6.QtCore import QSignalBlocker

from okx_quant.kline_trade_recommendations import KlineTradeIdea
from roll_terminal_qt.kline_trade_recommendation_dialog import KlineTradeRecommendationDialog, _price
from tests.qt_test_case import QtWidgetTestCase
from tests.test_kline_trade_recommendations import NOW, candles, idea


class TradeDialogTest(QtWidgetTestCase):
    def setUp(self):
        self.dialog = KlineTradeRecommendationDialog()

    def tearDown(self):
        self.dispose_widget(self.dialog)

    def test_candidate_has_prices_and_limited_percent_precision(self):
        self.dialog.set_result(idea())
        self.assertIn("ETH-USDT-SWAP", self.dialog._heading.text())
        self.assertNotEqual(self.dialog._values["stop"].text(), "—")
        self.assertRegex(self.dialog._values["volatility"].text(), r"\d+\.\d{2}%$")
        self.assertIn("等待", self.dialog._conditions.text())

    def test_wait_clears_previous_price_plan(self):
        self.dialog.set_result(idea())
        self.dialog.set_result(KlineTradeIdea("SOL-USDT", "1H", NOW, reasons=("等待数据",)))
        for key in ("entry", "stop", "tp2", "tp3", "rr", "volatility"):
            self.assertEqual(self.dialog._values[key].text(), "—")
        self.assertNotIn("ETH", self.dialog._heading.text())

    def test_daily_and_weekly_boundary_is_visible(self):
        for period, boundary in (("1D", "UTC+8 日界"), ("1Dutc", "UTC 日界"), ("1W", "UTC+8 周一开周")):
            self.dialog.set_result(idea(period=period))
            self.assertIn(boundary, self.dialog._context.text())

    def test_small_coin_price_is_not_rounded_to_zero(self):
        self.assertEqual(_price(Decimal("0.0000000001234")), "0.0000000001234")


class KlineTradeButtonTest(QtWidgetTestCase):
    def setUp(self):
        from roll_terminal_qt.kline_analysis_window import KlineAnalysisWindow
        self.Window = KlineAnalysisWindow
        for name, value in (("load_runtime", None), ("load_kline_analysis_workspace_entries", {}),
                            ("_prefer_native_chart_backend", True)):
            patcher = patch("roll_terminal_qt.kline_analysis_window."+name, return_value=value)
            patcher.start()
            self.addCleanup(patcher.stop)
        for method in ("_load_data", "_save_workspace_snapshot"):
            patcher = patch.object(self.Window, method)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.window = self.Window(preview_mode=True)
        self.cache = patch("roll_terminal_qt.kline_analysis_window.load_candle_cache", return_value=candles()).start()
        self.addCleanup(patch.stopall)
        self.clock = patch("roll_terminal_qt.kline_analysis_window.time.time", return_value=NOW/1000).start()

    def tearDown(self):
        if self.window._trade_recommendation_dialog:
            self.window._trade_recommendation_dialog.close()
        self.window.close()
        self._app.processEvents()

    def test_button_analyzes_selected_symbol_and_reuses_dialog_after_switch(self):
        with patch.object(self.Window, "_symbol_for_chart_target", return_value="ETH-USDT-SWAP"), \
             patch.object(self.Window, "_active_period_value", return_value="1H"):
            self.window._trade_recommendation_button.click()
        dialog = self.window._trade_recommendation_dialog
        self.cache.assert_called_once_with("ETH-USDT-SWAP", "1H", limit=250)
        self.assertIn("条件做多", dialog._heading.text())
        dialog.close()
        self.cache.return_value = candles(slope="-0.1")
        with patch.object(self.Window, "_symbol_for_chart_target", return_value="ETH-USDT"), \
             patch.object(self.Window, "_active_period_value", return_value="1H"):
            self.window._trade_recommendation_button.click()
        self.assertIs(self.window._trade_recommendation_dialog, dialog)
        self.assertIn("减仓/观望", dialog._heading.text())
        self.assertEqual(dialog._values["entry"].text(), "—")

    def test_fourth_chart_selection_uses_its_symbol_and_period(self):
        with patch.object(self.Window, "_quad_chart_enabled", return_value=True):
            self.window._active_chart_target = "quaternary"
            with QSignalBlocker(self.window._quaternary_symbol_combo), QSignalBlocker(self.window._quaternary_period_combo):
                self.window._quaternary_symbol_combo.setCurrentText("SOL-USDT-SWAP")
                self.window._quaternary_period_combo.setCurrentText("4H")
            self.cache.return_value = candles("4H")
            self.window._show_trade_recommendation()
        self.cache.assert_called_once_with("SOL-USDT-SWAP", "4H", limit=250)
        self.assertIn("SOL-USDT-SWAP | 4H", self.window._trade_recommendation_dialog._heading.text())

    def test_volatility_view_does_not_analyze_price_cache(self):
        with patch.object(self.Window, "_all_charts_volatility_enabled", return_value=True):
            self.window._show_trade_recommendation()
        self.cache.assert_not_called()
        self.assertIn("波动率指数图", self.window._trade_recommendation_dialog._reason.text())

    def test_failed_cache_load_shows_wait_not_previous_prices(self):
        self.window._show_trade_recommendation()
        self.cache.side_effect = OSError("test")
        self.window._show_trade_recommendation()
        dialog = self.window._trade_recommendation_dialog
        self.assertIn("生成失败", dialog._reason.text())
        self.assertEqual(dialog._values["entry"].text(), "—")
