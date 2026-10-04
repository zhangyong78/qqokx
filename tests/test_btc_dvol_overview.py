from __future__ import annotations

from decimal import Decimal
from unittest import TestCase
from unittest.mock import patch

from okx_quant.contract_recommendations import HOUR_MS
from okx_quant.deribit_client import DeribitVolatilityCandle
from okx_quant.models import Candle
from roll_terminal_qt.btc_dvol_overview import BtcDvolOverviewPanel, EthDvolOverviewPanel, _intraday_axis_range
from tests.qt_test_case import QtWidgetTestCase
from tests.test_contract_recommendations import NOW, prices


MODULE = "roll_terminal_qt.btc_dvol_overview"


class IntradayAxisRangeTest(TestCase):
    def test_4h_axis_ticks_stay_on_four_hour_boundaries(self):
        interval_ms = 4 * HOUR_MS
        first_ts = (1_790_000_000_000 // interval_ms) * interval_ms
        candles = [
            Candle(first_ts + index * interval_ms, Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1"), Decimal("0"), True)
            for index in range(100)
        ]

        start_ms, end_ms, tick_count = _intraday_axis_range(candles, "4H")

        self.assertEqual(tick_count, 4)
        self.assertEqual(start_ms % interval_ms, 0)
        self.assertEqual(end_ms % interval_ms, 0)
        self.assertEqual((end_ms - start_ms) // (tick_count - 1) % interval_ms, 0)
        self.assertLessEqual(candles[0].ts - start_ms, 2 * interval_ms)


class BtcDvolOverviewTest(QtWidgetTestCase):
    def setUp(self):
        self.data = prices()
        self.dvol = [DeribitVolatilityCandle(c.ts, c.open, c.high, c.low, c.close) for c in self.data["1H"]]
        self.cache_patch = patch(MODULE + ".load_candle_cache", side_effect=lambda symbol, period, **kw: self.data.get(period, []))
        self.vol_patch = patch(MODULE + "._load_cached_dvol_hourly", side_effect=lambda asset="BTC": self.dvol)
        self.time_patch = patch(MODULE + ".time.time", return_value=NOW / 1000)
        self.cache_mock = self.cache_patch.start()
        self.vol_patch.start()
        self.clock = self.time_patch.start()
        self.addCleanup(self.cache_patch.stop)
        self.addCleanup(self.vol_patch.stop)
        self.addCleanup(self.time_patch.stop)
        self.panel = BtcDvolOverviewPanel()

    def tearDown(self):
        self.dispose_widget(self.panel)

    def test_recommendation_appears_with_existing_eight_charts_and_time_basis(self):
        self.assertEqual(len(self.panel._charts), 8)
        self.assertIn("顺势回踩做多", self.panel._recommendation_box.title())
        self.assertIn("EMA15", self.panel._recommendation_details.text())
        self.assertIn("已收盘，UTC+8", self.panel._recommendation_main.text())
        self.assertIn("未经收益回测", self.panel._recommendation_details.text())
        self.assertEqual(self.cache_mock.call_count, 4)

    def test_refresh_replaces_direction_and_uses_same_chart_data(self):
        self.data = prices(-1)
        self.panel.refresh()
        self.assertIn("顺势回踩做空", self.panel._recommendation_box.title())
        self.assertEqual(self.panel._recommendation_snapshot.direction, "偏空")

    def test_missing_data_shows_reason_instead_of_old_strategy(self):
        self.data["1H"] = []
        self.panel.refresh()
        self.assertIn("等待数据更新", self.panel._recommendation_box.title())
        self.assertIn("1H", self.panel._recommendation_main.text())
        self.assertIn("不足", self.panel._recommendation_main.text())

    def test_screenshot_mode_keeps_strategy_and_existing_charts(self):
        self.panel.show()
        self._app.processEvents()
        self.panel.set_screenshot_mode(True)
        self.assertFalse(self.panel._toolbar.isVisible())
        self.assertTrue(self.panel._recommendation_box.isVisible())
        self.assertTrue(all(c.isVisible() for c in self.panel._charts))

    def test_freshness_recheck_and_screenshot_discard_stale_recommendation(self):
        self.clock.return_value = (NOW + 4 * HOUR_MS) / 1000
        self.panel.copy_screenshot()
        self.assertTrue(self.panel._recommendation_snapshot.blockers)
        self.assertIn("数据过期", self.panel._recommendation_main.text())
        self.assertFalse(self._app.clipboard().pixmap().isNull())
        self.assertEqual(self.cache_mock.call_count, 4)

    def test_eth_panel_uses_eth_swap_and_dvol_labels(self):
        self.cache_mock.reset_mock()
        panel = EthDvolOverviewPanel()
        try:
            self.assertEqual(panel.windowTitle(), "ETH × DVOL 总览")
            self.assertEqual(len(panel._charts), 8)
            self.assertIn("ETH-USDT-SWAP", panel._recommendation_box.title())
            self.assertTrue(all(call.args[0] == "ETH-USDT-SWAP" for call in self.cache_mock.call_args_list))
            self.assertIn("ETH-DVOL", panel._status.text())
        finally:
            self.dispose_widget(panel)
