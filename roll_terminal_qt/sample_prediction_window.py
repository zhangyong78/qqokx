from __future__ import annotations

from datetime import datetime, timezone, timedelta

from PySide6.QtCharts import (
    QCandlestickSeries,
    QCandlestickSet,
    QChart,
    QChartView,
    QDateTimeAxis,
    QLineSeries,
    QScatterSeries,
    QValueAxis,
)
from PySide6.QtCore import QDateTime, QPointF, QRectF, Qt
from PySide6.QtGui import QColor, QPainter, QPen
from PySide6.QtWidgets import (
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from okx_quant.sample_prediction import (
    BTC_WEEKLY_INST_ID,
    BacktestResult,
    PREDICTION_MIN_CONFIDENCE,
    PREDICTION_MIN_SAMPLE_COUNT,
    PredictionResult,
    WEEK_MS,
    load_btc_weekly_candles,
    predict_next_week,
    run_weekly_backtest,
)
from roll_terminal_qt.volatility_prediction_panel import VolatilityPredictionPanel


_SHANGHAI = timezone(timedelta(hours=8))
_CHART_EDGE_MARGIN_MS = 6 * 60 * 60 * 1000
_PREDICTION_RIGHT_EMPTY_SLOTS = 3


def _date_text(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, _SHANGHAI).strftime("%Y-%m-%d")


def _percent(value: float) -> str:
    return f"{value * 100:.1f}%"


def _price(value: float) -> str:
    return f"{value:,.2f}"


class PredictionCandlestickChartView(QChartView):
    """K-line chart view with a snapped crosshair and OHLC hover card."""

    def __init__(self, chart: QChart, parent: QWidget | None = None) -> None:
        super().__init__(chart, parent)
        self._hover_candles: list[dict[str, object]] = []
        self._hover_pos: QPointF | None = None
        self.setMouseTracking(True)
        self.viewport().setMouseTracking(True)
        self.viewport().setCursor(Qt.CursorShape.CrossCursor)

    def set_hover_candles(self, candles: list[dict[str, object]]) -> None:
        self._hover_candles = candles
        self._hover_pos = None
        self.viewport().update()

    def mouseMoveEvent(self, event) -> None:  # type: ignore[override]
        self._hover_pos = QPointF(event.position())
        self.viewport().update()
        super().mouseMoveEvent(event)

    def leaveEvent(self, event) -> None:  # type: ignore[override]
        self._hover_pos = None
        self.viewport().update()
        super().leaveEvent(event)

    def paintEvent(self, event) -> None:  # type: ignore[override]
        super().paintEvent(event)
        hover_pos = self._hover_pos
        plot = self.chart().plotArea()
        if (
            hover_pos is None
            or not self._hover_candles
            or not plot.contains(hover_pos)
            or plot.width() <= 0
            or plot.height() <= 0
        ):
            return

        chart_value = self.chart().mapToValue(hover_pos)
        target_ts = float(chart_value.x())
        candle = min(self._hover_candles, key=lambda item: abs(float(item["ts"]) - target_ts))
        # The chart deliberately leaves empty slots after the forecast.  Do not
        # snap the cursor to a candle while the pointer is in that blank area.
        if abs(float(candle["ts"]) - target_ts) > WEEK_MS * 0.55:
            return

        candle_ts = float(candle["ts"])
        snapped_x = self.chart().mapToPosition(QPointF(candle_ts, chart_value.y())).x()
        marker_y = self.chart().mapToPosition(
            QPointF(candle_ts, float(candle["chart_close"]))
        ).y()
        is_bullish = float(candle["close"]) >= float(candle["open"])
        candle_color = QColor("#15803d" if is_bullish else "#dc2626")

        painter = QPainter(self.viewport())
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        crosshair_pen = QPen(QColor("#64748b"), 1)
        crosshair_pen.setStyle(Qt.PenStyle.DashLine)
        painter.setPen(crosshair_pen)
        painter.drawLine(QPointF(snapped_x, plot.top()), QPointF(snapped_x, plot.bottom()))
        painter.drawLine(QPointF(plot.left(), hover_pos.y()), QPointF(plot.right(), hover_pos.y()))
        painter.setPen(QPen(candle_color, 2))
        painter.setBrush(QColor("#ffffff"))
        painter.drawEllipse(QPointF(snapped_x, marker_y), 4.0, 4.0)

        label = "预测 K 线" if bool(candle["is_prediction"]) else "已确认 K 线"
        change = (float(candle["close"]) / float(candle["open"]) - 1.0) * 100.0
        lines = (
            f"{label} · {_date_text(int(candle_ts))}",
            f"开 {_price(float(candle['open']))}    高 {_price(float(candle['high']))}",
            f"低 {_price(float(candle['low']))}    收 {_price(float(candle['close']))}",
            f"涨跌 {change:+.2f}%",
        )
        metrics = painter.fontMetrics()
        padding = 8.0
        line_height = float(metrics.height())
        card_width = max(float(metrics.horizontalAdvance(line)) for line in lines) + padding * 2
        card_height = line_height * len(lines) + padding * 2
        card_x = snapped_x + 12.0
        if card_x + card_width > plot.right() - 4.0:
            card_x = snapped_x - card_width - 12.0
        card_x = max(plot.left() + 4.0, card_x)
        card_y = min(max(hover_pos.y() + 12.0, plot.top() + 4.0), plot.bottom() - card_height - 4.0)
        card = QRectF(card_x, card_y, card_width, card_height)
        painter.setPen(QPen(QColor("#334155"), 1))
        painter.setBrush(QColor(15, 23, 42, 238))
        painter.drawRoundedRect(card, 5.0, 5.0)
        painter.setPen(QColor("#f8fafc"))
        for index, line in enumerate(lines):
            painter.drawText(
                QPointF(card.left() + padding, card.top() + padding + line_height * (index + 0.8)),
                line,
            )
        painter.end()


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
        self._chart_view = PredictionCandlestickChartView(self._chart)
        self._chart_view.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        self._chart_view.setMinimumHeight(320)
        self._comparison_chart = QChart()
        self._comparison_chart.setTitle("预测与真实复盘对比")
        self._comparison_chart.legend().setVisible(True)
        self._comparison_chart.setBackgroundVisible(False)
        self._comparison_view = QChartView(self._comparison_chart)
        self._comparison_view.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        self._comparison_view.setMinimumHeight(260)
        self._comparison_view.setVisible(False)
        self._charts_splitter = QSplitter(Qt.Orientation.Vertical)
        self._charts_splitter.addWidget(self._chart_view)
        self._charts_splitter.addWidget(self._comparison_view)
        self._charts_splitter.setStretchFactor(0, 3)
        self._charts_splitter.setStretchFactor(1, 2)
        self._prediction_time_axis: QDateTimeAxis | None = None
        self._comparison_time_axis: QDateTimeAxis | None = None
        self._syncing_time_axes = False

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
        self._comparison_toggle = QPushButton("显示预测对比")
        self._comparison_toggle.clicked.connect(self._toggle_comparison_chart)
        toolbar = QHBoxLayout()
        toolbar.addWidget(refresh)
        toolbar.addWidget(backtest)
        toolbar.addWidget(self._comparison_toggle)
        toolbar.addStretch(1)
        toolbar.addWidget(QLabel("仅使用本地已确认 BTC 周线，不调用大模型"))

        self._table = QTableWidget(0, 5)
        self._table.setHorizontalHeaderLabels(("预测周（最新在上）", "阳线概率", "预测方向", "实际方向", "结果"))
        self._table.setMinimumHeight(190)
        self._table.horizontalHeader().setStretchLastSection(True)

        content = QWidget(self)
        layout = QVBoxLayout(content)
        layout.addLayout(toolbar)
        layout.addWidget(self._status)
        layout.addWidget(self._charts_splitter, 1)
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
            self._backtest = None
            self._render_prediction_chart()
            self._render_comparison_chart()
            self._render_prediction_summary()
            if self._prediction.is_actionable:
                prediction_color = "绿色" if self._prediction.predicted_sign == "B" else "红色"
                prediction_text = (
                    f"预测结果：{'阳线' if self._prediction.predicted_sign == 'B' else '阴线'}（{prediction_color}）"
                )
            else:
                prediction_text = "预测结果：当前不预测"
            self._status.setText(
                f"数据：{BTC_WEEKLY_INST_ID} / 1W | 已确认样本 {len(self._candles)} 根 | "
                f"数据截止 {_date_text(self._candles[-1].ts)} | 预测目标 {_date_text(self._prediction.target_ts)}"
                f" | {prediction_text}"
            )
        except Exception as exc:
            self._status.setText(f"读取本地周线失败：{exc}")
            self._result.clear()
            self._chart.removeAllSeries()
            self._chart_view.set_hover_candles([])
            self._comparison_chart.removeAllSeries()

    def _toggle_comparison_chart(self) -> None:
        visible = self._comparison_view.isHidden()
        self._comparison_view.setVisible(visible)
        self._comparison_toggle.setText("隐藏预测对比" if visible else "显示预测对比")
        if visible:
            self._render_comparison_chart()
            self._charts_splitter.setSizes([max(self.height() * 3 // 5, 320), max(self.height() * 2 // 5, 260)])

    def _sync_prediction_time_axis(self, minimum: QDateTime, maximum: QDateTime) -> None:
        if self._syncing_time_axes:
            return
        self._syncing_time_axes = True
        try:
            if self._prediction_time_axis is not None:
                self._prediction_time_axis.setRange(minimum, maximum)
            if self._comparison_time_axis is not None:
                self._comparison_time_axis.setRange(minimum, maximum)
        finally:
            self._syncing_time_axes = False

    def _on_prediction_time_range_changed(self, minimum: QDateTime, maximum: QDateTime) -> None:
        if self._syncing_time_axes or self._comparison_time_axis is None:
            return
        self._syncing_time_axes = True
        try:
            self._comparison_time_axis.setRange(minimum, maximum)
        finally:
            self._syncing_time_axes = False

    def _on_comparison_time_range_changed(self, minimum: QDateTime, maximum: QDateTime) -> None:
        if self._syncing_time_axes or self._prediction_time_axis is None:
            return
        self._syncing_time_axes = True
        try:
            self._prediction_time_axis.setRange(minimum, maximum)
        finally:
            self._syncing_time_axes = False

    def run_backtest(self) -> None:
        try:
            if not self._candles:
                self._candles = load_btc_weekly_candles()
            self._backtest = run_weekly_backtest(self._candles)
            if not self._backtest.sample_count:
                raise ValueError("没有足够的历史样本运行回测。")
            self._backtest_summary.setText(
                f"滚动回测：{self._backtest.sample_count} 个历史机会 | "
                f"有效预测 {self._backtest.prediction_count} 次（覆盖率 {_percent(self._backtest.coverage)}）| "
                f"有效命中率 {_percent(self._backtest.selective_accuracy)} | "
                f"强制预测命中率 {_percent(self._backtest.accuracy)} | "
                f"过滤规则：置信度≥{_percent(PREDICTION_MIN_CONFIDENCE)} 且相似样本≥{PREDICTION_MIN_SAMPLE_COUNT} | "
                f"Brier 分数 {self._backtest.brier_score:.3f}（越低越好）| "
                f"历史多数基准命中率 {_percent(self._backtest.baseline_accuracy)} / "
                f"Brier {self._backtest.baseline_brier_score:.3f} | "
                f"实际阳线比例 {_percent(self._backtest.bullish_rate)} | "
                f"下一周平均收盘收益 {_percent(self._backtest.average_next_return)}"
            )
            self._render_prediction_chart()
            self._render_comparison_chart()
            self._table.setRowCount(0)
            for row in list(self._backtest.rows)[-20:][::-1]:
                table_row = self._table.rowCount()
                self._table.insertRow(table_row)
                values = (
                    _date_text(int(row["target_ts"])),
                    _percent(float(row["bullish_probability"])),
                    ("阳线" if row["predicted_sign"] == "B" else "阴线")
                    if bool(row["is_actionable"])
                    else "不预测",
                    "阳线" if row["actual_sign"] == "B" else "阴线",
                    ("命中 ✅" if row["correct"] else "未命中 ❌")
                    if bool(row["is_actionable"])
                    else "不预测 —",
                )
                for column, value in enumerate(values):
                    item = QTableWidgetItem(value)
                    if column == 4:
                        if not bool(row["is_actionable"]):
                            item.setBackground(QColor("#f3f4f6"))
                            item.setForeground(QColor("#6b7280"))
                        else:
                            item.setBackground(QColor("#dcfce7" if row["correct"] else "#fee2e2"))
                            item.setForeground(QColor("#166534" if row["correct"] else "#991b1b"))
                    self._table.setItem(table_row, column, item)
        except Exception as exc:
            QMessageBox.warning(self, "历史回测", str(exc))

    def _render_prediction_summary(self) -> None:
        prediction = self._prediction
        if prediction is None:
            return
        direction = "阳线" if prediction.predicted_sign == "B" else "阴线"
        if prediction.is_actionable:
            decision_line = (
                f"下一根预测：{direction} | 置信度 {_percent(prediction.confidence)} | "
                f"阳线概率 {_percent(prediction.bullish_probability)} | "
                f"阴线概率 {_percent(prediction.bearish_probability)}"
            )
        else:
            decision_line = (
                f"当前不预测（{prediction.abstain_reason or '统计优势不足'}）| "
                f"模型倾向：{direction}（仅供参考）| 置信度 {_percent(prediction.confidence)}"
            )
        self._result.setText(
            f"{decision_line} | 相似样本 {prediction.sample_count} 条\n"
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
        self._chart_view.set_hover_candles([])
        for axis in list(self._chart.axes()):
            self._chart.removeAxis(axis)
        self._chart.setTitle(
            "BTC-USDT-SWAP · 周线统计预测"
            if prediction.is_actionable
            else "BTC-USDT-SWAP · 周线统计预测（当前不预测）"
        )
        display = self._candles[-40:]
        display_start = len(self._candles) - len(display)
        reference_price = float(self._candles[-1].close)
        body_width = 0.72
        caps_width = 0.24
        actual = QCandlestickSeries()
        actual.setName("已确认周线")
        actual.setIncreasingColor(QColor("#15803d"))
        actual.setDecreasingColor(QColor("#dc2626"))
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
                candle.ts,
            ))
        forecast_open = self._relative_percent(prediction.open_price, reference_price)
        forecast_high = self._relative_percent(prediction.high_price, reference_price)
        forecast_low = self._relative_percent(prediction.low_price, reference_price)
        forecast_close = self._relative_percent(prediction.close_price, reference_price)
        self._chart.addSeries(actual)

        if prediction.is_actionable:
            forecast_series = QCandlestickSeries()
            direction_text = "阳线" if prediction.predicted_sign == "B" else "阴线"
            direction_color = "#15803d" if prediction.predicted_sign == "B" else "#dc2626"
            color_text = "绿色" if prediction.predicted_sign == "B" else "红色"
            forecast_series.setName(f"预测K线（{color_text}·{direction_text}）")
            # Use the predicted direction color for both candle polarities so
            # the visual encoding reflects the model result, not the median
            # OHLC values used to draw the forecast range.
            forecast_series.setIncreasingColor(QColor(direction_color))
            forecast_series.setDecreasingColor(QColor(direction_color))
            forecast_series.setBodyWidth(body_width)
            forecast_series.setCapsWidth(caps_width)
            forecast_series.setBodyOutlineVisible(True)
            forecast_series.append(QCandlestickSet(
                forecast_open,
                forecast_high,
                forecast_low,
                forecast_close,
                prediction.target_ts,
            ))
            self._chart.addSeries(forecast_series)

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
        for source_index in range(display_start, len(self._candles)):
            if ema15[source_index] is not None:
                ema_series.append(self._candles[source_index].ts, self._relative_percent(ema15[source_index], reference_price))
            if ma50[source_index] is not None:
                ma_series.append(self._candles[source_index].ts, self._relative_percent(ma50[source_index], reference_price))
        self._chart.addSeries(ema_series)
        self._chart.addSeries(ma_series)

        if self._backtest is not None and self._backtest.rows:
            candle_by_ts = {int(candle.ts): candle for candle in self._candles}
            hit_series = QScatterSeries()
            hit_series.setName("回测命中 ✅")
            hit_series.setColor(QColor("#16a34a"))
            hit_series.setMarkerSize(13)
            miss_series = QScatterSeries()
            miss_series.setName("回测未命中 ❌")
            miss_series.setColor(QColor("#dc2626"))
            miss_series.setMarkerSize(13)
            abstain_series = QScatterSeries()
            abstain_series.setName("暂不预测 —")
            abstain_series.setColor(QColor("#9ca3af"))
            abstain_series.setMarkerSize(10)
            for row in self._backtest.rows:
                target_ts = int(row["target_ts"])
                candle = candle_by_ts.get(target_ts)
                if candle is None:
                    continue
                point = (target_ts, self._relative_percent(float(candle.close), reference_price))
                if not bool(row["is_actionable"]):
                    abstain_series.append(*point)
                else:
                    (hit_series if bool(row["correct"]) else miss_series).append(*point)
            if hit_series.count():
                self._chart.addSeries(hit_series)
            if miss_series.count():
                self._chart.addSeries(miss_series)
            if abstain_series.count():
                self._chart.addSeries(abstain_series)

        chart_values = []
        if prediction.is_actionable:
            chart_values.extend((forecast_open, forecast_high, forecast_low, forecast_close))
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

        if prediction.is_actionable:
            arrow = QLineSeries()
            direction_text = "阳线" if prediction.predicted_sign == "B" else "阴线"
            direction_color = "#15803d" if prediction.predicted_sign == "B" else "#dc2626"
            color_text = "绿色" if prediction.predicted_sign == "B" else "红色"
            arrow.setName(f"预测标记（{color_text}·{direction_text}）")
            arrow_pen = QPen(QColor(direction_color))
            arrow_pen.setWidth(4)
            arrow.setPen(arrow_pen)
            price_span = max(max_price - min_price, 1.0)
            arrow_tip_y = min(forecast_open, forecast_close) - price_span * 0.025
            arrow_start_y = max(min_price + price_span * 0.04, arrow_tip_y - price_span * 0.16)
            arrow_tip_x = float(prediction.target_ts)
            # Keep the forecast marker centered on the final candle.  The old
            # diagonal shaft made the target easy to miss when the chart was wide.
            arrow.append(arrow_tip_x, arrow_start_y)
            arrow.append(arrow_tip_x, arrow_tip_y)
            arrow_width = WEEK_MS * 0.02
            arrow.append(arrow_tip_x - arrow_width, arrow_tip_y - price_span * 0.035)
            arrow.append(arrow_tip_x, arrow_tip_y)
            arrow.append(arrow_tip_x + arrow_width, arrow_tip_y - price_span * 0.035)
            self._chart.addSeries(arrow)
        else:
            cross = QLineSeries()
            cross.setName("下一根K线：暂不预测（灰色十字）")
            cross.setPen(QPen(QColor("#6b7280"), 3))
            cross_x = float(prediction.target_ts)
            cross_y = min_price + max((max_price - min_price) * 0.08, 0.8)
            cross_width = WEEK_MS * 0.025
            cross_height = max((max_price - min_price) * 0.025, 0.8)
            cross.append(cross_x - cross_width, cross_y - cross_height)
            cross.append(cross_x + cross_width, cross_y + cross_height)
            cross.append(cross_x, cross_y)
            cross.append(cross_x + cross_width, cross_y - cross_height)
            cross.append(cross_x - cross_width, cross_y + cross_height)
            self._chart.addSeries(cross)

        x_axis = QDateTimeAxis()
        x_axis.setFormat("MM-dd")
        x_axis.setTitleText("时间（北京时间；橙色为当前待验证预测）")
        x_min = int(display[0].ts)
        # Leave several future candle slots visible so the prediction marker
        # is not pressed against the right border of the chart.
        x_max = int(prediction.target_ts + WEEK_MS * _PREDICTION_RIGHT_EMPTY_SLOTS)
        x_axis.setRange(QDateTime.fromMSecsSinceEpoch(x_min), QDateTime.fromMSecsSinceEpoch(x_max))
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
        hover_candles = [
            {
                "ts": candle.ts,
                "open": float(candle.open),
                "high": float(candle.high),
                "low": float(candle.low),
                "close": float(candle.close),
                "chart_close": self._relative_percent(float(candle.close), reference_price),
                "is_prediction": False,
            }
            for candle in display
        ]
        if prediction.is_actionable:
            hover_candles.append(
                {
                    "ts": prediction.target_ts,
                    "open": prediction.open_price,
                    "high": prediction.high_price,
                    "low": prediction.low_price,
                    "close": prediction.close_price,
                    "chart_close": forecast_close,
                    "is_prediction": True,
                }
            )
        self._chart_view.set_hover_candles(hover_candles)
        self._prediction_time_axis = x_axis
        x_axis.rangeChanged.connect(self._on_prediction_time_range_changed)
        if self._comparison_time_axis is not None:
            self._sync_prediction_time_axis(x_axis.min(), x_axis.max())

    def _render_comparison_chart(self) -> None:
        self._comparison_chart.removeAllSeries()
        for axis in list(self._comparison_chart.axes()):
            self._comparison_chart.removeAxis(axis)
        self._comparison_chart.setTitle("预测与真实复盘对比（绿色=命中，红色=未命中）")
        backtest = self._backtest
        if backtest is None or not backtest.rows:
            self._comparison_time_axis = None
            return
        rows = list(backtest.rows)
        probability = QLineSeries()
        probability.setName("阳线概率")
        probability_pen = QPen(QColor("#7c3aed"))
        probability_pen.setWidth(2)
        probability_pen.setStyle(Qt.PenStyle.DashLine)
        probability.setPen(probability_pen)
        predicted = QScatterSeries()
        predicted.setName("预测方向")
        predicted.setColor(QColor("#2563eb"))
        predicted.setMarkerSize(10)
        actual = QLineSeries()
        actual.setName("真实方向")
        actual_pen = QPen(QColor("#111827"))
        actual_pen.setWidth(2)
        actual.setPen(actual_pen)
        hit_series = QScatterSeries()
        hit_series.setName("命中 ✅")
        hit_series.setColor(QColor("#16a34a"))
        hit_series.setMarkerSize(14)
        miss_series = QScatterSeries()
        miss_series.setName("未命中 ❌")
        miss_series.setColor(QColor("#dc2626"))
        miss_series.setMarkerSize(14)
        abstain_series = QScatterSeries()
        abstain_series.setName("暂不预测 —")
        abstain_series.setColor(QColor("#9ca3af"))
        abstain_series.setMarkerSize(10)
        for row in rows:
            target_ts = int(row["target_ts"])
            actual_value = 100.0 if row["actual_sign"] == "B" else 0.0
            probability.append(target_ts, float(row["bullish_probability"]) * 100.0)
            actual.append(target_ts, actual_value)
            if bool(row["is_actionable"]):
                predicted_value = 100.0 if row["predicted_sign"] == "B" else 0.0
                predicted.append(target_ts, predicted_value)
                (hit_series if bool(row["correct"]) else miss_series).append(target_ts, actual_value)
            else:
                abstain_series.append(target_ts, actual_value)
        for series in (probability, predicted, actual, hit_series, miss_series, abstain_series):
            self._comparison_chart.addSeries(series)
        x_axis = QDateTimeAxis()
        x_axis.setFormat("yyyy-MM-dd")
        x_axis.setTitleText("目标周（与上方预测图同步）")
        x_min = int(rows[0]["target_ts"])
        x_max = int(rows[-1]["target_ts"] + _CHART_EDGE_MARGIN_MS)
        x_axis.setRange(QDateTime.fromMSecsSinceEpoch(x_min), QDateTime.fromMSecsSinceEpoch(x_max))
        y_axis = QValueAxis()
        y_axis.setRange(-8.0, 108.0)
        y_axis.setLabelFormat("%d%%")
        y_axis.setTitleText("方向 / 阳线概率")
        self._comparison_chart.addAxis(x_axis, Qt.AlignmentFlag.AlignBottom)
        self._comparison_chart.addAxis(y_axis, Qt.AlignmentFlag.AlignLeft)
        for series in self._comparison_chart.series():
            series.attachAxis(x_axis)
            series.attachAxis(y_axis)
        self._comparison_time_axis = x_axis
        x_axis.rangeChanged.connect(self._on_comparison_time_range_changed)
        if self._prediction_time_axis is not None:
            self._sync_prediction_time_axis(self._prediction_time_axis.min(), self._prediction_time_axis.max())

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
