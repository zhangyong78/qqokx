from PySide6.QtCharts import QChart, QDateTimeAxis, QValueAxis
from PySide6.QtCore import QDateTime, Qt

from roll_terminal_qt.kline_analysis_window import InteractiveKlineChartView
from tests.qt_test_case import QtWidgetTestCase


class LatestPriceIndicatorTest(QtWidgetTestCase):
    def setUp(self):
        self.chart = QChart()
        self.view = InteractiveKlineChartView(self.chart)
        self.axis_x = QDateTimeAxis()
        self.axis_y = QValueAxis()
        self.axis_x.setRange(QDateTime.fromMSecsSinceEpoch(0), QDateTime.fromMSecsSinceEpoch(3_600_000))
        self.axis_y.setRange(90, 120)
        self.chart.addAxis(self.axis_x, Qt.AlignmentFlag.AlignBottom)
        self.chart.addAxis(self.axis_y, Qt.AlignmentFlag.AlignRight)
        candles = [
            {"time": 0, "open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": 1.0},
            {"time": 3_600_000, "open": 102.0, "high": 110.0, "low": 100.0, "close": 108.0, "volume": 1.0},
        ]
        self.view.set_chart_context(
            axis_x=self.axis_x,
            axis_y=self.axis_y,
            candles=candles,
            overlay_values=[],
            display_times_ms=[0, 3_600_000],
            period="1H",
            symbol="ETH-USDT-SWAP",
            latest_price=109.5,
        )

    def tearDown(self):
        self.dispose_widget(self.view)

    def test_latest_price_is_paint_only_and_does_not_change_axis_range(self):
        before = (self.axis_y.min(), self.axis_y.max())
        self.assertEqual(self.view._latest_price, 109.5)
        self.view.set_latest_price(111.25)
        self.assertEqual(self.view._latest_price, 111.25)
        self.assertEqual((self.axis_y.min(), self.axis_y.max()), before)

    def test_invalid_latest_price_hides_marker_value_without_touching_chart(self):
        before = (self.axis_y.min(), self.axis_y.max())
        self.view.set_latest_price(float("nan"))
        self.assertIsNone(self.view._latest_price)
        self.assertEqual((self.axis_y.min(), self.axis_y.max()), before)
