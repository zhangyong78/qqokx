from __future__ import annotations

from decimal import Decimal

from PySide6.QtCore import QDateTime, QThread, Qt, Signal, Slot
from PySide6.QtGui import QColor, QPainter, QPen
from PySide6.QtWidgets import (
    QCheckBox,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
    QWidget,
)
from PySide6.QtCharts import (
    QCandlestickSeries,
    QCandlestickSet,
    QChart,
    QChartView,
    QDateTimeAxis,
    QLineSeries,
    QValueAxis,
)

from okx_quant.models import Candle
from okx_quant.okx_client import OkxRestClient

from roll_terminal_qt.formatting import fmt_decimal


CHART_BAR_OPTIONS: tuple[tuple[str, str], ...] = (
    ("15分钟", "15m"),
    ("1小时", "1H"),
    ("4小时", "4H"),
    ("日线", "1D"),
)

_BAR_INTERVAL_MS = {
    "15m": 15 * 60 * 1000,
    "1H": 60 * 60 * 1000,
    "4H": 4 * 60 * 60 * 1000,
    "1D": 24 * 60 * 60 * 1000,
}


def _compact_spread_axis_range(candles: list[Candle]) -> tuple[Decimal, Decimal]:
    """Keep the default spread chart readable when one candle has a bad wick.

    The executable spread is represented by the candle body (open/close).
    Cross-market high/low timestamps are not simultaneous, so their theoretical
    difference can create extreme wicks which otherwise flatten every normal bar.
    """
    body_values = [value for candle in candles for value in (candle.open, candle.close)]
    if not body_values:
        return Decimal("-1"), Decimal("1")
    lower = min(body_values)
    upper = max(body_values)
    span = upper - lower
    midpoint = (upper + lower) / Decimal("2")
    padding = max(span * Decimal("0.12"), abs(midpoint) * Decimal("0.002"), Decimal("1"))
    return lower - padding, upper + padding


def _compact_spread_wick_cap(candles: list[Candle]) -> Decimal:
    """Return a robust display cap for cross-market spread candle wicks."""
    if not candles:
        return Decimal("1")
    body_ranges = sorted(abs(candle.close - candle.open) for candle in candles)
    wick_extensions = sorted(
        max(
            candle.high - max(candle.open, candle.close),
            min(candle.open, candle.close) - candle.low,
            Decimal("0"),
        )
        for candle in candles
    )
    median_body = body_ranges[len(body_ranges) // 2]
    typical_wick = wick_extensions[min(len(wick_extensions) - 1, int((len(wick_extensions) - 1) * 0.8))]
    body_values = [value for candle in candles for value in (candle.open, candle.close)]
    body_span = max(body_values) - min(body_values)
    return max(typical_wick * Decimal("1.5"), median_body * Decimal("4"), body_span * Decimal("0.03"), Decimal("1"))


def _compact_spread_wick_bounds(candle: Candle, wick_cap: Decimal) -> tuple[Decimal, Decimal]:
    """Clip only pathological spread wicks while keeping every body intact."""
    body_high = max(candle.open, candle.close)
    body_low = min(candle.open, candle.close)
    return (
        min(max(candle.high, body_high), body_high + wick_cap),
        max(min(candle.low, body_low), body_low - wick_cap),
    )


def _aligned_spread_candles(left_candles: list[Candle], right_candles: list[Candle]) -> list[Candle]:
    left_by_ts = {item.ts: item for item in left_candles}
    right_by_ts = {item.ts: item for item in right_candles}
    spread: list[Candle] = []
    for ts in sorted(left_by_ts.keys() & right_by_ts.keys()):
        left = left_by_ts[ts]
        right = right_by_ts[ts]
        open_price = right.open - left.open
        close_price = right.close - left.close
        high_price = max(
            right.high - left.low,
            right.high - left.high,
            right.close - left.close,
            right.open - left.open,
        )
        low_price = min(
            right.low - left.high,
            right.low - left.low,
            right.close - left.close,
            right.open - left.open,
        )
        spread.append(
            Candle(
                ts=ts,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=Decimal("0"),
                confirmed=left.confirmed and right.confirmed,
            )
        )
    return spread


def _load_pair_candles(
    client: OkxRestClient,
    left_inst_id: str,
    right_inst_id: str,
    bar: str,
    limit: int,
) -> tuple[list[Candle], list[Candle]]:
    """Load both legs from a common time window, including expired futures."""
    left = client.get_candles_history(left_inst_id, bar, limit=limit)
    right = client.get_candles_history(right_inst_id, bar, limit=limit)
    if not left or not right:
        return left, right
    if {item.ts for item in left} & {item.ts for item in right}:
        return left, right

    interval_ms = _BAR_INTERVAL_MS.get(bar)
    if interval_ms is None:
        return left, right

    left_start, left_end = left[0].ts, left[-1].ts
    right_start, right_end = right[0].ts, right[-1].ts
    if left_end < right_start:
        end_ts = left_end
        start_ts = max(0, end_ts - interval_ms * (limit - 1))
        right = client.get_candles_history_range(
            right_inst_id,
            bar,
            start_ts=start_ts,
            end_ts=end_ts,
            limit=limit,
        )
    elif right_end < left_start:
        end_ts = right_end
        start_ts = max(0, end_ts - interval_ms * (limit - 1))
        left = client.get_candles_history_range(
            left_inst_id,
            bar,
            start_ts=start_ts,
            end_ts=end_ts,
            limit=limit,
        )
    else:
        overlap_start = max(left_start, right_start)
        overlap_end = min(left_end, right_end)
        if overlap_start <= overlap_end:
            left = client.get_candles_history_range(
                left_inst_id,
                bar,
                start_ts=overlap_start,
                end_ts=overlap_end,
                limit=limit,
            )
            right = client.get_candles_history_range(
                right_inst_id,
                bar,
                start_ts=overlap_start,
                end_ts=overlap_end,
                limit=limit,
            )
    return left, right


class SpreadChartLoadThread(QThread):
    loaded = Signal(str, str, str, object)
    failed = Signal(str)

    def __init__(self, *, left_inst_id: str, right_inst_id: str, bar: str, limit: int = 240) -> None:
        super().__init__()
        self._left_inst_id = left_inst_id.strip().upper()
        self._right_inst_id = right_inst_id.strip().upper()
        self._bar = bar.strip()
        self._limit = max(60, limit)

    def run(self) -> None:
        client = OkxRestClient()
        try:
            left, right = _load_pair_candles(
                client,
                self._left_inst_id,
                self._right_inst_id,
                self._bar,
                self._limit,
            )
            if not left:
                raise ValueError(f"{self._left_inst_id} 没有获取到K线。")
            if not right:
                raise ValueError(f"{self._right_inst_id} 没有获取到K线。")
            spread = _aligned_spread_candles(left, right)
            if not spread:
                raise ValueError("两条腿的历史 K 线没有共同时间范围，无法计算价差。")
            self.loaded.emit(self._left_inst_id, self._right_inst_id, self._bar, spread)
        except Exception as exc:  # noqa: BLE001
            self.failed.emit(str(exc))
        finally:
            client.close()


class SpreadChartWindow(QMainWindow):
    def __init__(self, parent=None) -> None:  # noqa: ANN001
        super().__init__(parent)
        self.setWindowTitle("价差K线图")
        self.resize(1180, 760)
        self._left_inst_id = ""
        self._right_inst_id = ""
        self._current_bar = "15m"
        self._load_thread: SpreadChartLoadThread | None = None
        self._bar_buttons: dict[str, QPushButton] = {}
        self._candles: list[Candle] = []

        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        header = QHBoxLayout()
        header.setSpacing(8)
        self._title = QLabel("等待选择合约")
        self._title.setObjectName("SectionTitle")
        self._status = QLabel("点击周期按钮即可加载价差K线。")
        self._status.setObjectName("Subtle")
        header.addWidget(self._title, 1)
        header.addWidget(self._status, 2)
        layout.addLayout(header)

        bar_row = QHBoxLayout()
        bar_row.setSpacing(8)
        bar_row.addWidget(QLabel("周期"))
        for text, bar in CHART_BAR_OPTIONS:
            button = QPushButton(text)
            button.setCheckable(True)
            button.clicked.connect(lambda _checked=False, target_bar=bar: self._select_bar(target_bar))
            self._bar_buttons[bar] = button
            bar_row.addWidget(button)
        self._show_raw_extremes_check = QCheckBox("显示原始极值")
        self._show_raw_extremes_check.setToolTip(
            "默认会压缩跨市场不同时间点造成的异常长影线，使正常价差变化更清楚；"
            "勾选后按原始最高/最低价完整显示。"
        )
        self._show_raw_extremes_check.toggled.connect(self._render_current_chart)
        bar_row.addWidget(self._show_raw_extremes_check)
        self._show_close_line_check = QCheckBox("显示收盘线")
        self._show_close_line_check.setToolTip("普通 K 线默认不叠加收盘价连线；需要观察连续价差时可以打开。")
        self._show_close_line_check.toggled.connect(self._render_current_chart)
        bar_row.addWidget(self._show_close_line_check)
        self._reset_view_button = QPushButton("恢复视图")
        self._reset_view_button.clicked.connect(self._reset_chart_view)
        bar_row.addWidget(self._reset_view_button)
        bar_row.addStretch(1)
        layout.addLayout(bar_row)

        self._chart = QChart()
        self._chart.legend().hide()
        self._chart.setBackgroundVisible(False)
        self._chart_view = QChartView(self._chart)
        self._chart_view.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        self._chart_view.setRubberBand(QChartView.RubberBand.RectangleRubberBand)
        layout.addWidget(self._chart_view, 1)

        self._sync_bar_buttons()

    def set_pair(self, left_inst_id: str, right_inst_id: str) -> None:
        self._left_inst_id = left_inst_id.strip().upper()
        self._right_inst_id = right_inst_id.strip().upper()
        self._title.setText(f"{self._left_inst_id} / {self._right_inst_id} 价差K线")
        self._load_current_bar()

    def closeEvent(self, event) -> None:  # noqa: ANN001
        if self._load_thread is not None and self._load_thread.isRunning():
            self._load_thread.requestInterruption()
            self._load_thread.wait(1500)
        super().closeEvent(event)

    @Slot()
    def _load_current_bar(self) -> None:
        if not self._left_inst_id or not self._right_inst_id:
            return
        if self._load_thread is not None and self._load_thread.isRunning():
            return
        self._status.setText(f"正在加载 {self._current_bar} 价差K线...")
        self._load_thread = SpreadChartLoadThread(
            left_inst_id=self._left_inst_id,
            right_inst_id=self._right_inst_id,
            bar=self._current_bar,
        )
        self._load_thread.loaded.connect(self._apply_loaded_chart)
        self._load_thread.failed.connect(self._apply_load_error)
        self._load_thread.finished.connect(self._clear_finished_thread)
        self._load_thread.start()

    def _select_bar(self, bar: str) -> None:
        self._current_bar = bar
        self._sync_bar_buttons()
        self._load_current_bar()

    def _sync_bar_buttons(self) -> None:
        for bar, button in self._bar_buttons.items():
            checked = bar == self._current_bar
            button.blockSignals(True)
            button.setChecked(checked)
            button.blockSignals(False)
            button.setObjectName("Primary" if checked else "")
            button.style().unpolish(button)
            button.style().polish(button)

    @Slot()
    def _reset_chart_view(self) -> None:
        self._chart.zoomReset()
        self._chart_view.repaint()

    @Slot(str, str, str, object)
    def _apply_loaded_chart(self, left_inst_id: str, right_inst_id: str, bar: str, candles: list[Candle]) -> None:
        if left_inst_id != self._left_inst_id or right_inst_id != self._right_inst_id or bar != self._current_bar:
            return
        self._candles = list(candles)
        self._render_current_chart()

    def _render_current_chart(self, _checked: bool | None = None) -> None:
        candles = self._candles
        if not candles:
            return
        self._chart.removeAllSeries()
        for axis in list(self._chart.axes()):
            self._chart.removeAxis(axis)

        candle_series = QCandlestickSeries()
        candle_series.setIncreasingColor(Qt.GlobalColor.red)
        candle_series.setDecreasingColor(Qt.GlobalColor.darkGreen)
        candle_series.setBodyOutlineVisible(True)
        candle_series.setBodyWidth(0.58)
        candle_series.setPen(QPen(QColor("#4b5563"), 1))

        close_series = QLineSeries()
        close_series.setName("中间价差")
        close_series.setPen(QPen(QColor("#0f766e"), 2))

        first_ts = candles[0].ts
        last_ts = candles[-1].ts
        show_raw_extremes = self._show_raw_extremes_check.isChecked()
        show_close_line = self._show_close_line_check.isChecked()
        wick_cap = _compact_spread_wick_cap(candles)
        if show_raw_extremes:
            min_price = min(candle.low for candle in candles)
            max_price = max(candle.high for candle in candles)
        else:
            min_price, max_price = _compact_spread_axis_range(candles)
            compact_bounds = [_compact_spread_wick_bounds(candle, wick_cap) for candle in candles]
            min_price = min(min_price, *(low for _high, low in compact_bounds))
            max_price = max(max_price, *(high for high, _low in compact_bounds))
        clipped_wick_count = 0

        for candle in candles:
            timestamp_ms = int(candle.ts)
            display_high = candle.high
            display_low = candle.low
            if not show_raw_extremes:
                display_high, display_low = _compact_spread_wick_bounds(candle, wick_cap)
                if display_high != candle.high or display_low != candle.low:
                    clipped_wick_count += 1
            candle_set = QCandlestickSet(
                float(candle.open),
                float(display_high),
                float(display_low),
                float(candle.close),
                timestamp_ms,
            )
            candle_series.append(candle_set)
            if show_close_line:
                close_series.append(timestamp_ms, float(candle.close))

        self._chart.addSeries(candle_series)
        if show_close_line:
            self._chart.addSeries(close_series)
        view_label = "原始极值" if show_raw_extremes else "紧凑交易视图"
        if show_close_line:
            view_label += " + 收盘线"
        self._chart.setTitle(
            f"{self._left_inst_id} / {self._right_inst_id} 价差K线 | "
            f"{self._current_bar} | {view_label}"
        )

        axis_x = QDateTimeAxis()
        axis_x.setFormat("MM-dd" if self._current_bar == "1D" else "MM-dd HH:mm")
        axis_x.setTickCount(min(8, max(3, len(candles) // 30 + 2)))
        axis_x.setRange(QDateTime.fromMSecsSinceEpoch(int(first_ts)), QDateTime.fromMSecsSinceEpoch(int(last_ts)))

        axis_y = QValueAxis()
        if show_raw_extremes:
            diff = max_price - min_price
            padding = max(diff * Decimal("0.08"), Decimal("1"))
            min_value = float(min_price - padding)
            max_value = float(max_price + padding)
        else:
            min_value = float(min_price)
            max_value = float(max_price)
        axis_y.setRange(min_value, max_value)
        axis_y.setLabelFormat("%.2f")
        axis_y.setTickCount(6)

        self._chart.addAxis(axis_x, Qt.AlignmentFlag.AlignBottom)
        self._chart.addAxis(axis_y, Qt.AlignmentFlag.AlignLeft)
        candle_series.attachAxis(axis_x)
        candle_series.attachAxis(axis_y)
        close_series.attachAxis(axis_x)
        close_series.attachAxis(axis_y)

        last_close = candles[-1].close
        compact_note = (
            "原始最高/最低价显示中"
            if show_raw_extremes
            else f"紧凑视图，已压缩 {clipped_wick_count} 根异常影线"
        )
        self._status.setText(
            f"{self._current_bar} 已加载 {len(candles)} 根 | "
            f"最新价差 {fmt_decimal(last_close, 2)} | {compact_note}"
        )

    @Slot(str)
    def _apply_load_error(self, message: str) -> None:
        self._status.setText(f"加载失败：{message}")
        QMessageBox.warning(self, "价差K线加载失败", message)

    @Slot()
    def _clear_finished_thread(self) -> None:
        if self._load_thread is not None and not self._load_thread.isRunning():
            self._load_thread = None
