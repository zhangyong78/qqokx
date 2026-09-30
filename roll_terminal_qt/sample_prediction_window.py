from __future__ import annotations

from datetime import datetime, timezone, timedelta

from PySide6.QtCharts import QCandlestickSeries, QCandlestickSet, QChart, QChartView, QLineSeries, QValueAxis
from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QPainter, QPen
from PySide6.QtWidgets import (
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from okx_quant.sample_prediction import (
    BTC_WEEKLY_INST_ID,
    BacktestResult,
    PredictionResult,
    load_btc_weekly_candles,
    predict_next_week,
    run_weekly_backtest,
)
from roll_terminal_qt.volatility_prediction_panel import VolatilityPredictionPanel


_SHANGHAI = timezone(timedelta(hours=8))


def _date_text(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, _SHANGHAI).strftime("%Y-%m-%d")


def _percent(value: float) -> str:
    return f"{value * 100:.1f}%"


def _price(value: float) -> str:
    return f"{value:,.2f}"


class SamplePredictionWindow(QMainWindow):
    """Local statistical BTC weekly prediction surface."""

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("AI 样本预测 · BTC 波动率 / 周线")
        self.resize(1420, 900)
        self._candles = []
        self._prediction: PredictionResult | None = None
        self._backtest: BacktestResult | None = None
        self._chart = QChart()
        self._chart.setTitle("BTC-USDT-SWAP · 周线统计预测")
        self._chart.legend().setVisible(True)
        self._chart.setBackgroundVisible(False)
        self._chart_view = QChartView(self._chart)
        self._chart_view.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        self._chart_view.setMinimumHeight(520)

        self._status = QLabel("正在读取本地周线数据…")
        self._status.setWordWrap(True)
        self._status.setObjectName("Subtle")
        self._result = QLabel("")
        self._result.setWordWrap(True)
        self._result.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self._result.setObjectName("PredictionSummary")
        self._backtest_summary = QLabel("尚未运行历史回测。")
        self._backtest_summary.setWordWrap(True)
        self._backtest_summary.setObjectName("Subtle")

        refresh = QPushButton("刷新预测")
        refresh.clicked.connect(self.refresh_prediction)
        backtest = QPushButton("运行历史回测")
        backtest.clicked.connect(self.run_backtest)
        toolbar = QHBoxLayout()
        toolbar.addWidget(refresh)
        toolbar.addWidget(backtest)
        toolbar.addStretch(1)
        toolbar.addWidget(QLabel("仅使用本地已确认 BTC 周线，不调用大模型"))

        self._table = QTableWidget(0, 5)
        self._table.setHorizontalHeaderLabels(("预测周", "阳线概率", "预测方向", "实际方向", "结果"))
        self._table.setMinimumHeight(190)
        self._table.horizontalHeader().setStretchLastSection(True)

        content = QWidget(self)
        layout = QVBoxLayout(content)
        layout.addLayout(toolbar)
        layout.addWidget(self._status)
        layout.addWidget(self._chart_view, 1)
        layout.addWidget(self._result)
        layout.addWidget(self._backtest_summary)
        layout.addWidget(self._table)
        self._tabs = QTabWidget(self)
        self._volatility_panel = VolatilityPredictionPanel(self)
        self._tabs.addTab(self._volatility_panel, "BTC 波动率模型")
        self._tabs.addTab(content, "BTC 周线样本")
        self.setCentralWidget(self._tabs)
        self.setStyleSheet(
            "QLabel#PredictionSummary { background: #eef6ff; border: 1px solid #b9d8f5; "
            "border-radius: 5px; padding: 8px; }"
        )
        self.refresh_prediction()

    def closeEvent(self, event) -> None:  # type: ignore[override]
        if not self._volatility_panel.can_close():
            event.ignore()
            return
        super().closeEvent(event)

    def refresh_prediction(self) -> None:
        try:
            self._candles = load_btc_weekly_candles()
            self._prediction = predict_next_week(self._candles)
            if self._prediction is None:
                raise ValueError("本地周线样本不足，至少需要 24 根已确认周线。")
            self._render_prediction_chart()
            self._render_prediction_summary()
            self._status.setText(
                f"数据：{BTC_WEEKLY_INST_ID} / 1W | 已确认样本 {len(self._candles)} 根 | "
                f"数据截止 {_date_text(self._candles[-1].ts)} | 预测目标 {_date_text(self._prediction.target_ts)}"
            )
        except Exception as exc:
            self._status.setText(f"读取本地周线失败：{exc}")
            self._result.clear()
            self._chart.removeAllSeries()

    def run_backtest(self) -> None:
        try:
            if not self._candles:
                self._candles = load_btc_weekly_candles()
            self._backtest = run_weekly_backtest(self._candles)
            if not self._backtest.sample_count:
                raise ValueError("没有足够的历史样本运行回测。")
            self._backtest_summary.setText(
                f"滚动回测：{self._backtest.sample_count} 次 | 方向命中率 {_percent(self._backtest.accuracy)} | "
                f"Brier 分数 {self._backtest.brier_score:.3f}（越低越好）| "
                f"历史多数基准命中率 {_percent(self._backtest.baseline_accuracy)} / "
                f"Brier {self._backtest.baseline_brier_score:.3f} | "
                f"实际阳线比例 {_percent(self._backtest.bullish_rate)} | "
                f"下一周平均收盘收益 {_percent(self._backtest.average_next_return)}"
            )
            self._table.setRowCount(0)
            for row in list(self._backtest.rows)[-20:][::-1]:
                table_row = self._table.rowCount()
                self._table.insertRow(table_row)
                values = (
                    _date_text(int(row["target_ts"])),
                    _percent(float(row["bullish_probability"])),
                    "阳线" if row["predicted_sign"] == "B" else "阴线",
                    "阳线" if row["actual_sign"] == "B" else "阴线",
                    "命中" if row["correct"] else "未命中",
                )
                for column, value in enumerate(values):
                    self._table.setItem(table_row, column, QTableWidgetItem(value))
        except Exception as exc:
            QMessageBox.warning(self, "历史回测", str(exc))

    def _render_prediction_summary(self) -> None:
        prediction = self._prediction
        if prediction is None:
            return
        direction = "阳线" if prediction.predicted_sign == "B" else "阴线"
        self._result.setText(
            f"下一根预测：{direction} | 阳线概率 {_percent(prediction.bullish_probability)} | "
            f"阴线概率 {_percent(prediction.bearish_probability)} | 相似样本 {prediction.sample_count} 条\n"
            f"统计规则：{prediction.sample_rule} | 当前状态："
            f"{'连阳' if prediction.current_sign == 'B' else '连阴' if prediction.current_sign == 'S' else '平线'} "
            f"{prediction.current_streak} 根 | 最近4周收益 {_percent(prediction.current_four_week_return)}\n"
            f"预测中位 K 线：开 {_price(prediction.open_price)} / 高 {_price(prediction.high_price)} / "
            f"低 {_price(prediction.low_price)} / 收 {_price(prediction.close_price)}\n"
            f"收盘统计区间：{_price(prediction.close_range[0])} 至 {_price(prediction.close_range[1])}；"
            f"高低点区间：{_price(prediction.low_range[0])} 至 {_price(prediction.high_range[1])}"
        )

    def _render_prediction_chart(self) -> None:
        prediction = self._prediction
        if prediction is None or not self._candles:
            return
        self._chart.removeAllSeries()
        for axis in list(self._chart.axes()):
            self._chart.removeAxis(axis)
        display = self._candles[-40:]
        display_start = len(self._candles) - len(display)
        forecast_index = len(display)
        reference_price = float(self._candles[-1].close)
        body_width = 0.72
        caps_width = 0.24
        actual = QCandlestickSeries()
        actual.setName("周线（最右为预测）")
        actual.setIncreasingColor(QColor("#dc2626"))
        actual.setDecreasingColor(QColor("#15803d"))
        actual.setBodyWidth(body_width)
        actual.setCapsWidth(caps_width)
        actual.setBodyOutlineVisible(False)
        min_price = min(self._relative_percent(float(candle.low), reference_price) for candle in display)
        max_price = max(self._relative_percent(float(candle.high), reference_price) for candle in display)
        for index, candle in enumerate(display):
            actual.append(QCandlestickSet(
                self._relative_percent(float(candle.open), reference_price),
                self._relative_percent(float(candle.high), reference_price),
                self._relative_percent(float(candle.low), reference_price),
                self._relative_percent(float(candle.close), reference_price),
                index,
            ))
        forecast_open = self._relative_percent(prediction.open_price, reference_price)
        forecast_high = self._relative_percent(prediction.high_price, reference_price)
        forecast_low = self._relative_percent(prediction.low_price, reference_price)
        forecast_close = self._relative_percent(prediction.close_price, reference_price)
        actual.append(QCandlestickSet(
            forecast_open,
            forecast_high,
            forecast_low,
            forecast_close,
            forecast_index,
        ))
        self._chart.addSeries(actual)

        closes = [float(candle.close) for candle in self._candles]
        ema15 = self._ema_values(closes, 15)
        ma50 = self._ma_values(closes, 50)
        ema_series = QLineSeries()
        ema_series.setName("EMA15")
        ema_pen = QPen(QColor("#f59e0b"))
        ema_pen.setWidth(2)
        ema_series.setPen(ema_pen)
        ma_series = QLineSeries()
        ma_series.setName("MA50")
        ma_pen = QPen(QColor("#7c3aed"))
        ma_pen.setWidth(2)
        ma_series.setPen(ma_pen)
        for chart_index, source_index in enumerate(range(display_start, len(self._candles))):
            if ema15[source_index] is not None:
                ema_series.append(chart_index, self._relative_percent(ema15[source_index], reference_price))
            if ma50[source_index] is not None:
                ma_series.append(chart_index, self._relative_percent(ma50[source_index], reference_price))
        self._chart.addSeries(ema_series)
        self._chart.addSeries(ma_series)

        chart_values = [
            forecast_open,
            forecast_high,
            forecast_low,
            forecast_close,
        ]
        chart_values.extend(
            self._relative_percent(value, reference_price)
            for value in ema15[display_start:]
            if value is not None
        )
        chart_values.extend(
            self._relative_percent(value, reference_price)
            for value in ma50[display_start:]
            if value is not None
        )
        min_price = min(min_price, *chart_values)
        max_price = max(max_price, *chart_values)

        arrow = QLineSeries()
        arrow.setName("未来预测 ↑")
        arrow_pen = QPen(QColor("#ea580c"))
        arrow_pen.setWidth(3)
        arrow.setPen(arrow_pen)
        price_span = max(max_price - min_price, 1.0)
        arrow_tip_y = min(forecast_open, forecast_close) - price_span * 0.025
        arrow_start_y = max(min_price + price_span * 0.04, arrow_tip_y - price_span * 0.16)
        arrow_tip_x = float(forecast_index)
        arrow.append(arrow_tip_x - 0.65, arrow_start_y)
        arrow.append(arrow_tip_x, arrow_tip_y)
        arrow.append(arrow_tip_x - 0.14, arrow_tip_y - price_span * 0.025)
        arrow.append(arrow_tip_x, arrow_tip_y)
        arrow.append(arrow_tip_x + 0.14, arrow_tip_y - price_span * 0.025)
        self._chart.addSeries(arrow)

        x_axis = QValueAxis()
        x_axis.setRange(-0.4, forecast_index + 1.2)
        x_axis.setLabelFormat("%d")
        x_axis.setTitleText("历史周线序号（最右为预测）")
        y_axis = QValueAxis()
        padding = max((max_price - min_price) * 0.06, 1.0)
        y_axis.setRange(min_price - padding, max_price + padding)
        y_axis.setLabelFormat("%.1f%%")
        y_axis.setTitleText("相对最新确认收盘价（%）")
        self._chart.addAxis(x_axis, Qt.AlignmentFlag.AlignBottom)
        self._chart.addAxis(y_axis, Qt.AlignmentFlag.AlignLeft)
        for series in self._chart.series():
            series.attachAxis(x_axis)
            series.attachAxis(y_axis)

    @staticmethod
    def _ema_values(values: list[float], period: int) -> list[float | None]:
        if not values or period <= 0:
            return [None] * len(values)
        alpha = 2.0 / (period + 1.0)
        result: list[float | None] = []
        current = values[0]
        for value in values:
            current = (value * alpha) + (current * (1.0 - alpha))
            result.append(current)
        return result

    @staticmethod
    def _ma_values(values: list[float], period: int) -> list[float | None]:
        result: list[float | None] = []
        for index in range(len(values)):
            if index + 1 < period:
                result.append(None)
                continue
            window = values[index - period + 1 : index + 1]
            result.append(sum(window) / period)
        return result

    @staticmethod
    def _relative_percent(value: float, reference: float) -> float:
        if reference == 0:
            return 0.0
        return (value / reference - 1.0) * 100.0
