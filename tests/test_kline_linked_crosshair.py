from types import SimpleNamespace
from unittest.mock import patch

from PySide6.QtCharts import QChart, QDateTimeAxis, QLineSeries, QValueAxis
from PySide6.QtCore import QDateTime, QEvent, QPointF, Qt
from PySide6.QtGui import QPainter

from roll_terminal_qt.kline_analysis_window import InteractiveKlineChartView, KlineAnalysisWindow
from tests.qt_test_case import QtWidgetTestCase


class LinkedCrosshairTest(QtWidgetTestCase):
    def setUp(self):
        self.views = []
        self.start = 1_760_000_000
        for period, hours in (("1H", 1), ("4H", 4), ("1D", 24), ("1W", 168)):
            self.views.append(self.make_view(period, hours))
        self.window = SimpleNamespace(
            _native_chart_view=self.views[0],
            _secondary_native_chart_view=self.views[1],
            _tertiary_native_chart_view=self.views[2],
            _quaternary_native_chart_view=self.views[3],
            _triple_chart_enabled=lambda: True,
            _quad_chart_enabled=lambda: True,
        )
        for name, view in zip(("primary", "secondary", "tertiary", "quaternary"), self.views):
            view.hoverTimeChanged.connect(
                lambda timestamp, source=name: KlineAnalysisWindow._sync_hover_time_from(
                    self.window, source=source, candle_time=timestamp,
                )
            )

    def make_view(self, period, hours):
        chart = QChart()
        chart.legend().hide()
        view = InteractiveKlineChartView(chart)
        view.setAttribute(Qt.WidgetAttribute.WA_DontShowOnScreen, True)
        view.resize(700, 500)
        axis_x, axis_y = QDateTimeAxis(), QValueAxis()
        chart.addAxis(axis_x, Qt.AlignmentFlag.AlignBottom)
        chart.addAxis(axis_y, Qt.AlignmentFlag.AlignRight)
        candles = [
            {"time": self.start + index * hours * 3600, "open": 82000.0,
             "high": 84000.0, "low": 79000.0, "close": 83000.0, "volume": 10.0}
            for index in range(10)
        ]
        display_times = [candle["time"] * 1000 for candle in candles]
        series = QLineSeries()
        for timestamp in display_times:
            series.append(timestamp, 83000.0)
        chart.addSeries(series)
        series.attachAxis(axis_x)
        series.attachAxis(axis_y)
        view.set_chart_context(
            axis_x=axis_x, axis_y=axis_y, candles=candles, overlay_values=[],
            display_times_ms=display_times, period=period, symbol="BTC-USDT-SWAP",
        )
        axis_x.setRange(QDateTime.fromMSecsSinceEpoch(display_times[0]),
                        QDateTime.fromMSecsSinceEpoch(display_times[-1]))
        axis_y.setRange(78000 if period == "1H" else 77000, 86000)
        view.show()
        self._app.processEvents()
        return view

    def tearDown(self):
        for view in self.views:
            self.dispose_widget(view)

    def move_cursor(self, view, price, index=4):
        plot = view.chart().plotArea()
        view._hover_pos = QPointF(view._x_for_index(index, plot), view._y_for_value(price, plot))
        view._emit_hover_time_from_position(view._hover_pos)
        return int(view._candles[index]["time"])

    def test_source_cursor_price_links_to_all_periods_not_target_close(self):
        timestamp = self.move_cursor(self.views[0], 80000)
        for target in self.views[1:]:
            with self.subTest(period=target._period):
                self.assertEqual(target._external_hover_time, timestamp)
                self.assertAlmostEqual(target._external_hover_price, 80000)
                plot = target.chart().plotArea()
                y = target._external_hover_y(plot)
                self.assertAlmostEqual(target._value_for_y(y, plot), 80000)
                self.assertNotAlmostEqual(y, target._y_for_value(83000, plot))

    def test_same_candle_vertical_moves_update_price_and_reverse_direction_works(self):
        timestamp = self.move_cursor(self.views[0], 80000)
        self.assertEqual(self.move_cursor(self.views[0], 81000), timestamp)
        self.assertAlmostEqual(self.views[1]._external_hover_price, 81000)
        self.views[0]._hover_pos = None
        self.move_cursor(self.views[1], 85000, index=1)
        self.assertAlmostEqual(self.views[0]._external_hover_price, 85000)
        self.assertIsNone(self.views[1]._external_hover_time)
        self.assertIsNone(self.views[1]._external_hover_price)

    def test_painted_price_badge_and_horizontal_anchor_use_source_price(self):
        self.move_cursor(self.views[0], 80000)
        target = self.views[1]
        horizontal_lines = []

        class RecordingPainter(QPainter):
            def drawLine(self, *args):
                if (
                    len(args) == 2 and all(isinstance(point, QPointF) for point in args)
                    and args[0].y() == args[1].y()
                    and self.pen().style() == Qt.PenStyle.DashLine
                ):
                    horizontal_lines.append(args)
                return super().drawLine(*args)

        with (
            patch("roll_terminal_qt.kline_analysis_window.QPainter", RecordingPainter),
            patch.object(target, "_update_hover_overlays", wraps=target._update_hover_overlays) as overlays,
        ):
            target.grab()
        self.assertTrue(overlays.called)
        data = overlays.call_args.kwargs
        self.assertEqual(data["price_text"], "80,000.00")
        expected_y = target._y_for_value(80000, target.chart().plotArea())
        self.assertAlmostEqual(data["anchor"].y(), expected_y)
        self.assertEqual(len(horizontal_lines), 1)
        self.assertAlmostEqual(horizontal_lines[0][0].y(), expected_y)
        self.assertIn("C 83,000.00", "\n".join(data["tooltip_lines"]))
        self.assertEqual(target._price_badge.text(), "80,000.00")

    def test_other_symbol_or_volatility_links_only_time(self):
        self.views[2]._symbol = "ETH-USDT-SWAP"
        self.views[3]._symbol = "BTC-DVOL"
        timestamp = self.move_cursor(self.views[0], 80000)
        for target in self.views[2:]:
            self.assertEqual(target._external_hover_time, timestamp)
            self.assertIsNone(target._external_hover_price)
            target.grab()
            self.assertTrue(target._price_badge.isHidden())

    def test_atr_band_does_not_publish_atr_as_a_price(self):
        source = self.views[0]
        source._show_atr = True
        source._atr_values = [100.0] * len(source._candles)
        plot = source.chart().plotArea()
        top, bottom, *_ = source._atr_overlay_geometry(plot)
        source._hover_pos = QPointF(source._x_for_index(4, plot), (top + bottom) / 2)
        source._emit_hover_time_from_position(source._hover_pos)
        self.assertIsNotNone(self.views[1]._external_hover_time)
        self.assertIsNone(self.views[1]._external_hover_price)

    def test_offscreen_price_does_not_clamp_or_rescale_target(self):
        target = self.views[1]
        target._axis_y.setRange(81000, 86000)
        self.move_cursor(self.views[0], 80000)
        self.assertIsNotNone(target._resolve_external_hover_context())
        self.assertIsNone(target._external_hover_y(target.chart().plotArea()))
        target.grab()
        self.assertTrue(target._price_badge.isHidden())
        self.assertEqual((target._axis_y.min(), target._axis_y.max()), (81000, 86000))

    def test_leaving_source_clears_all_linked_cursors(self):
        self.move_cursor(self.views[0], 80000)
        self.views[0].leaveEvent(QEvent(QEvent.Type.Leave))
        for view in self.views:
            self.assertIsNone(view._external_hover_time)
            self.assertIsNone(view._external_hover_price)

    def test_reset_and_invalid_price_clear_price_state(self):
        target = self.views[1]
        for price in (float("nan"), float("inf"), None):
            target.set_external_hover_time(self.start, price)
            self.assertIsNone(target._external_hover_price)
        target.set_external_hover_time(self.start, 80000)
        target.clear_chart_context()
        self.assertIsNone(target._external_hover_time)
        self.assertIsNone(target._external_hover_price)

    def test_outside_time_range_still_hides_linked_cursor(self):
        target = self.views[1]
        target.set_external_hover_time(self.start - 1, 80000)
        self.assertIsNone(target._resolve_external_hover_context())
