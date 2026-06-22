from __future__ import annotations

from decimal import Decimal

from PySide6.QtCore import QDateTime, QThread, Qt, Signal, Slot
from PySide6.QtGui import QPainter
from PySide6.QtWidgets import (
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
        try:
            client = OkxRestClient()
            left = client.get_candles_history(self._left_inst_id, self._bar, limit=self._limit)
            right = client.get_candles_history(self._right_inst_id, self._bar, limit=self._limit)
            spread = _aligned_spread_candles(left, right)
            if not spread:
                raise ValueError("这两个合约在当前周期没有可对齐的K线数据。")
            self.loaded.emit(self._left_inst_id, self._right_inst_id, self._bar, spread)
        except Exception as exc:  # noqa: BLE001
            self.failed.emit(str(exc))


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
        bar_row.addStretch(1)
        layout.addLayout(bar_row)

        self._chart = QChart()
        self._chart.legend().hide()
        self._chart.setBackgroundVisible(False)
        self._chart_view = QChartView(self._chart)
        self._chart_view.setRenderHint(QPainter.RenderHint.Antialiasing, True)
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

    @Slot(str, str, str, object)
    def _apply_loaded_chart(self, left_inst_id: str, right_inst_id: str, bar: str, candles: list[Candle]) -> None:
        if left_inst_id != self._left_inst_id or right_inst_id != self._right_inst_id or bar != self._current_bar:
            return
        self._chart.removeAllSeries()
        for axis in list(self._chart.axes()):
            self._chart.removeAxis(axis)

        candle_series = QCandlestickSeries()
        candle_series.setIncreasingColor(Qt.GlobalColor.red)
        candle_series.setDecreasingColor(Qt.GlobalColor.darkGreen)
        candle_series.setBodyOutlineVisible(True)

        close_series = QLineSeries()
        close_series.setName("收盘")

        min_price: Decimal | None = None
        max_price: Decimal | None = None
        first_ts = candles[0].ts
        last_ts = candles[-1].ts

        for candle in candles:
            timestamp_ms = int(candle.ts)
            candle_set = QCandlestickSet(
                float(candle.open),
                float(candle.high),
                float(candle.low),
                float(candle.close),
                timestamp_ms,
            )
            candle_series.append(candle_set)
            close_series.append(timestamp_ms, float(candle.close))
            min_price = candle.low if min_price is None else min(min_price, candle.low)
            max_price = candle.high if max_price is None else max(max_price, candle.high)

        self._chart.addSeries(candle_series)
        self._chart.addSeries(close_series)
        self._chart.setTitle(f"{left_inst_id} / {right_inst_id} 价差K线 | {bar}")

        axis_x = QDateTimeAxis()
        axis_x.setFormat("MM-dd HH:mm")
        axis_x.setTickCount(min(8, max(3, len(candles) // 30 + 2)))
        axis_x.setRange(QDateTime.fromMSecsSinceEpoch(int(first_ts)), QDateTime.fromMSecsSinceEpoch(int(last_ts)))

        axis_y = QValueAxis()
        if min_price is None or max_price is None:
            min_value = -1.0
            max_value = 1.0
        else:
            diff = max_price - min_price
            padding = max(diff * Decimal("0.08"), Decimal("1"))
            min_value = float(min_price - padding)
            max_value = float(max_price + padding)
        axis_y.setRange(min_value, max_value)
        axis_y.setLabelFormat("%.2f")

        self._chart.addAxis(axis_x, Qt.AlignmentFlag.AlignBottom)
        self._chart.addAxis(axis_y, Qt.AlignmentFlag.AlignLeft)
        candle_series.attachAxis(axis_x)
        candle_series.attachAxis(axis_y)
        close_series.attachAxis(axis_x)
        close_series.attachAxis(axis_y)

        last_close = candles[-1].close
        self._status.setText(
            f"{bar} 已加载 {len(candles)} 根 | 最新价差收盘 {fmt_decimal(last_close, 2)}"
        )

    @Slot(str)
    def _apply_load_error(self, message: str) -> None:
        self._status.setText(f"加载失败：{message}")
        QMessageBox.warning(self, "价差K线加载失败", message)

    @Slot()
    def _clear_finished_thread(self) -> None:
        if self._load_thread is not None and not self._load_thread.isRunning():
            self._load_thread = None
