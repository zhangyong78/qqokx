from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch
from PySide6.QtCore import QPoint, Qt
from PySide6.QtTest import QTest

from okx_quant.area_shape_monitor import AreaShapeMonitor
from okx_quant.area_shape_trade import AreaShapeConfig, AreaShapePlan
from okx_quant.models import Candle
from roll_terminal_qt.kline_analysis_window import KlineAnalysisWindow, KlineChartPayload
from tests.qt_test_case import QtWidgetTestCase


HOUR = 3_600_000


class AreaShapeKlineUiTests(QtWidgetTestCase):
    def setUp(self) -> None:
        self._original_save = __import__("roll_terminal_qt.kline_analysis_window", fromlist=["save_kline_analysis_workspace_entries"]).save_kline_analysis_workspace_entries
        import roll_terminal_qt.kline_analysis_window as kline_module

        kline_module.save_kline_analysis_workspace_entries = lambda _entries: None
        self._native_patch = patch.object(kline_module, "_prefer_native_chart_backend", return_value=True)
        self._native_patch.start()
        self.window = KlineAnalysisWindow(embedded=True, preview_mode=True)
        self.window._workspace_entries = {}
        self.window._active_profile_name = lambda: "ui-test"
        self.window._active_environment = lambda: "demo"
        self.window._selected_symbol = lambda: "BTC-USDT-SWAP"
        self.window._render_to_chart = lambda _payload: None
        self.window._period_combo.setCurrentText("1H")
        self.rows = [Candle(n * HOUR, Decimal("100"), Decimal("105"), Decimal("95"), Decimal("101"), Decimal("1"), True) for n in range(1, 21)]
        self.window._pending_payload = KlineChartPayload(
            candles=[{"time": c.ts // 1000, "open": float(c.open), "high": float(c.high), "low": float(c.low), "close": float(c.close)} for c in self.rows],
            raw_candles=self.rows, ema_9=[], ema_21=[], ema_55=[], trend_indicator=[], signal_markers=[], box_overlays=[], stats={},
        )
        self.saved: list[list[dict[str, object]]] = []
        self.monitor = AreaShapeMonitor(store_load=lambda: [], store_save=lambda records: self.saved.append(records))
        self.window.set_area_shape_monitor(self.monitor)
        self._kline_module = kline_module

    def tearDown(self) -> None:
        self.window.begin_shutdown()
        self._app.processEvents()
        self.dispose_widget(self.window)
        self._kline_module.save_kline_analysis_workspace_entries = self._original_save
        self._native_patch.stop()

    def test_repeated_one_click_preserves_dragged_prices(self) -> None:
        self.window._create_area_shape_plan()
        lines = self.window._workspace_entry(symbol="BTC-USDT-SWAP", period="1H")["lines"]
        lines[0]["price_a"] = lines[0]["price_b"] = 98.0
        lines[1]["price_a"] = lines[1]["price_b"] = 104.0

        self.window._create_area_shape_plan()

        plan = self.monitor.plans()[0]
        self.assertEqual((plan.support_price, plan.pressure_price), (Decimal("98.0"), Decimal("104.0")))
        self.assertEqual(len(lines), 2)

    def test_new_plan_reuses_last_context_parameters(self) -> None:
        previous = AreaShapePlan.create(
            profile_name="ui-test", environment="demo", symbol="BTC-USDT-SWAP",
            support_price=Decimal("98"), pressure_price=Decimal("104"),
            config=AreaShapeConfig(risk_amount=Decimal("33")),
        )
        previous.status = "paper_closed"
        self.monitor._plans[previous.plan_id] = previous

        self.window._create_area_shape_plan()

        plans = self.monitor.plans()
        self.assertEqual(len(plans), 2)
        new_plan = next(item for item in plans if item.status == "draft")
        self.assertEqual(new_plan.config.risk_amount, Decimal("33"))

    def _render_real_chart(self) -> None:
        self.window._render_to_chart = KlineAnalysisWindow._render_to_chart.__get__(self.window)
        self.window.resize(1400, 850)
        self.window.show()
        self._app.processEvents()
        self.window._render_to_chart(self.window._pending_payload)
        self._app.processEvents()

    def test_one_click_immediately_renders_both_ray_bodies_from_the_visible_midpoint(self) -> None:
        self._render_real_chart()
        chart = self.window._native_chart_view
        left, right = chart.current_x_range()
        expected = min(range(len(self.rows)), key=lambda i: abs(chart._display_times_ms[i] - (left+right)/2))
        self.window._create_area_shape_plan()
        self._app.processEvents()
        lines = self.window._workspace_entry(symbol="BTC-USDT-SWAP", period="1H")["lines"]
        self.assertEqual([line["time_a"] for line in lines], [self.rows[expected].ts//1000]*2)
        self.assertLess(lines[0]["time_a"], self.rows[-1].ts//1000)
        bodies = [s for s in self.window._native_chart.series() if s.name().startswith(("区域支撑", "区域压力"))]
        self.assertEqual(len(bodies), 2)
        self.assertTrue(all(s.count() > 1 for s in bodies))

    def test_one_hour_recent_and_full_views_reserve_one_third_and_refresh_keeps_drag(self) -> None:
        self._render_real_chart()
        chart = self.window._native_chart_view
        for view_mode in (chart.set_recent_view_range, chart.set_full_view_range):
            view_mode()
            left, right = chart.current_x_range()
            ratio = (right-chart._display_times_ms[-1])/(right-left)
            self.assertAlmostEqual(ratio, 1/3, delta=0.04)
        chart.set_recent_view_range()
        chart._pan_by_pixels(-200, 900)
        state = chart.capture_view_state()
        self.window._render_to_chart(self.window._pending_payload)
        self.assertEqual(chart.current_x_range(), (state["start_x"], state["end_x"]))
        self.assertEqual(chart._right_padding_bars, state["right_padding_bars"])

    def test_area_replay_results_render_with_distinct_backtest_labels_and_focus(self) -> None:
        self._render_real_chart()
        chart = self.window._native_chart_view
        entry_ts = self.rows[7].ts
        exit_ts = self.rows[12].ts
        results = [{
            "side": "short", "entry_ts": entry_ts, "exit_ts": exit_ts,
            "entry": Decimal("101"), "exit": Decimal("98"), "r": Decimal("2.0"),
        }, {
            "record_type": "opportunity", "side": "long", "signal_ts": self.rows[15].ts,
            "entry_ts": self.rows[15].ts, "entry": Decimal("100"), "space_r": Decimal("1.5"),
        }, {
            "side": "long", "entry_ts": self.rows[2].ts, "exit_ts": self.rows[4].ts,
            "entry": Decimal("100"), "exit": Decimal("102"), "r": Decimal("0.5"),
        }, {
            "record_type": "opportunity", "side": "short", "signal_ts": self.rows[17].ts,
            "entry_ts": self.rows[17].ts, "entry": Decimal("102"), "space_r": Decimal("2.0"),
        }]

        chart.set_area_shape_replay_trades(results, focus=True)
        self._app.processEvents()

        left, right = chart.current_x_range()
        self.assertLess(left, entry_ts)
        self.assertGreater(right, exit_ts)
        self.assertEqual(chart._area_shape_replay_trades, results)
        image = chart.grab().toImage()
        replay_pixels = 0
        opportunity_pixels = 0
        long_trade_pixels = 0
        short_opportunity_pixels = 0
        for y in range(image.height()):
            for x in range(image.width()):
                color = image.pixelColor(x, y)
                if color.red() > 200 and color.green() < 160 and color.blue() < 160:
                    replay_pixels += 1
                if color.red() < 100 and color.green() >= 150 and color.blue() >= 200:
                    opportunity_pixels += 1
                if color.red() < 150 and color.green() >= 180 and color.blue() < 180:
                    long_trade_pixels += 1
                if color.red() >= 150 and color.green() >= 100 and color.blue() >= 180:
                    short_opportunity_pixels += 1
        self.assertGreater(replay_pixels, 20)
        self.assertGreater(opportunity_pixels, 20)
        self.assertGreater(long_trade_pixels, 20)
        self.assertGreater(short_opportunity_pixels, 20)

    def test_normal_pan_can_create_more_blank_space_even_when_no_gap_remains(self) -> None:
        self._render_real_chart()
        chart = self.window._native_chart_view
        chart._set_right_padding_bars(0)
        chart.set_external_x_range(chart._display_times_ms[0], chart._display_times_ms[-1])
        old_left, old_right = chart.current_x_range()
        chart._pan_by_pixels(-300, 900)
        left, right = chart.current_x_range()
        self.assertGreater(right, old_right)
        self.assertAlmostEqual(right-left, old_right-old_left, delta=2)
        self.assertAlmostEqual((right-chart._display_times_ms[-1])/(right-left), 1/3, delta=0.01)

    def test_dragging_the_blank_area_left_increases_padding_without_timeframe_switch(self) -> None:
        self._render_real_chart()
        chart = self.window._native_chart_view
        plot = chart.chart().plotArea()
        press = QPoint(int(plot.right()-20), int(plot.center().y()))
        before = chart._right_padding_bars
        QTest.mousePress(chart.viewport(), Qt.MouseButton.LeftButton, pos=press)
        QTest.mouseMove(chart.viewport(), QPoint(press.x()-120, press.y()))
        QTest.mouseRelease(chart.viewport(), Qt.MouseButton.LeftButton, pos=QPoint(press.x()-120, press.y()))
        self._app.processEvents()
        self.assertGreater(chart._right_padding_bars, before)

    def test_auto_zone_creation_renders_two_rays_and_reports_detection(self) -> None:
        self._render_real_chart()
        zones = SimpleNamespace(support=SimpleNamespace(price=Decimal(96), touches=3),
            resistance=SimpleNamespace(price=Decimal(104), touches=4), lookback_bars=20)
        with patch.object(self._kline_module, "find_auto_zone_pair", return_value=zones):
            self.window._auto_create_area_shape_plan()
        lines = self.window._workspace_entry(symbol="BTC-USDT-SWAP", period="1H")["lines"]
        self.assertEqual(len(lines), 2)
        self.assertEqual(lines[0]["time_a"], lines[1]["time_a"])
        self.assertLess(lines[0]["time_a"], self.rows[-1].ts//1000)
        bodies = [s for s in self.window._native_chart.series() if s.name().startswith(("自动支撑", "自动压力"))]
        self.assertEqual(len(bodies), 2)
        self.assertIn("3个确认摆动点", self.window._area_shape_status.text())
        with patch.object(self._kline_module, "find_auto_zone_pair") as detect:
            self.window._auto_create_area_shape_plan()
            detect.assert_not_called()
        self.assertEqual(len(self.monitor.plans()), 1)
        self.assertIn("已有区域线", self.window._area_shape_status.text())

    def test_auto_zone_failure_explains_why_without_creating_a_plan(self) -> None:
        with patch.object(self._kline_module, "find_auto_zone_pair", side_effect=ValueError("未找到有效支撑/压力簇")):
            self.window._auto_create_area_shape_plan()
        self.assertEqual(self.monitor.plans(), [])
        self.assertIn("未找到有效支撑/压力簇", self.window._area_shape_status.text())
