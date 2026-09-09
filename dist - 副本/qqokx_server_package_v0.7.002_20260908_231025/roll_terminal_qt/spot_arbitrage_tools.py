from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from PySide6.QtCharts import (
    QCandlestickSeries,
    QCandlestickSet,
    QChart,
    QChartView,
    QDateTimeAxis,
    QLineSeries,
    QValueAxis,
)
from PySide6.QtCore import QDateTime, QThread, QTimer, Qt, Signal, Slot
from PySide6.QtGui import QColor, QPainter, QPen
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFrame,
    QGridLayout,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from okx_quant.arbitrage.arbitrage_manager import ArbitrageManager
from okx_quant.arbitrage.models import ArbitrageOpportunity
from okx_quant.models import Candle
from okx_quant.okx_client import OkxRestClient
from roll_terminal_qt.formatting import fmt_decimal
from roll_terminal_qt.models import ArbitrageOpportunityView


ARBITRAGE_CHART_BAR_OPTIONS: tuple[tuple[str, str], ...] = (
    ("1分钟", "1m"),
    ("5分钟", "5m"),
    ("15分钟", "15m"),
    ("1小时", "1H"),
    ("4小时", "4H"),
    ("1天", "1D"),
)

SCAN_COLUMNS: tuple[tuple[str, str], ...] = (
    ("base", "币种"),
    ("kind", "类型"),
    ("spot", "现货"),
    ("derivative", "衍生品"),
    ("basis_abs", "绝对价差"),
    ("basis", "基差%"),
    ("funding", "资金费年化%"),
    ("fee", "手续费%"),
    ("slippage", "滑点%"),
    ("net", "净年化%"),
    ("days", "到期天数"),
)


@dataclass(frozen=True)
class ArbitrageChartPayload:
    spot_inst_id: str
    derivative_inst_id: str
    bar: str
    spot_candles: list[Candle]
    derivative_candles: list[Candle]
    spread_candles: list[Candle]


def _spread_abs(spot_price: Decimal, derivative_price: Decimal) -> Decimal:
    return derivative_price - spot_price


def build_spot_arbitrage_spread_candles(
    spot_candles: list[Candle],
    derivative_candles: list[Candle],
) -> list[Candle]:
    """Keep the old spot-arbitrage definition: spread = derivative - spot."""
    derivative_by_ts = {item.ts: item for item in derivative_candles}
    spread_candles: list[Candle] = []
    for spot in spot_candles:
        derivative = derivative_by_ts.get(spot.ts)
        if derivative is None:
            continue
        values = (
            _spread_abs(spot.open, derivative.open),
            _spread_abs(spot.high, derivative.high),
            _spread_abs(spot.low, derivative.low),
            _spread_abs(spot.close, derivative.close),
        )
        spread_candles.append(
            Candle(
                ts=spot.ts,
                open=values[0],
                high=max(values),
                low=min(values),
                close=values[3],
                volume=Decimal("0"),
                confirmed=spot.confirmed and derivative.confirmed,
            )
        )
    return spread_candles


def format_scan_opportunity_cells(opportunity: ArbitrageOpportunity) -> dict[str, str]:
    return {
        "base": opportunity.base_ccy,
        "kind": opportunity.pair_kind_label,
        "spot": opportunity.spot_inst_id,
        "derivative": opportunity.derivative_inst_id,
        "basis_abs": fmt_decimal(opportunity.basis_abs, 4),
        "basis": fmt_decimal(opportunity.basis_pct, 4),
        "funding": "-" if opportunity.funding_annual_pct is None else fmt_decimal(opportunity.funding_annual_pct, 4),
        "fee": fmt_decimal(opportunity.fee_round_trip_pct, 4),
        "slippage": fmt_decimal(opportunity.slippage_est_pct, 4),
        "net": fmt_decimal(opportunity.net_annual_pct, 4),
        "days": "-" if opportunity.days_to_expiry is None else str(opportunity.days_to_expiry),
    }


def scan_opportunity_to_view(opportunity: ArbitrageOpportunity) -> ArbitrageOpportunityView:
    return ArbitrageOpportunityView(
        key=f"scan_{opportunity.spot_inst_id}_{opportunity.derivative_inst_id}",
        title=f"{opportunity.base_ccy} {opportunity.pair_kind_label}",
        left_inst_id=opportunity.spot_inst_id,
        right_inst_id=opportunity.derivative_inst_id,
        left_kind="现货",
        right_kind="衍生品",
        template="professional",
        description=(
            f"扫描机会：{opportunity.spot_inst_id} / {opportunity.derivative_inst_id}；"
            f"基差 {fmt_decimal(opportunity.basis_pct)}%，"
            f"净年化 {fmt_decimal(opportunity.net_annual_pct)}%。"
        ),
        is_custom=False,
    )


def _parse_positive_int(value: str, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(str(value).strip())
    except ValueError:
        return default
    return min(max(parsed, minimum), maximum)


def _confirmed_or_all(candles: list[Candle]) -> list[Candle]:
    confirmed = [item for item in candles if item.confirmed]
    return confirmed or list(candles)


def _ema(values: list[Decimal], period: int) -> list[Decimal | None]:
    if not values:
        return []
    multiplier = Decimal("2") / Decimal(period + 1)
    result: list[Decimal | None] = []
    current = values[0]
    for value in values:
        current = (value - current) * multiplier + current
        result.append(current)
    return result


class ArbitrageScanThread(QThread):
    loaded = Signal(object)
    failed = Signal(str)

    def __init__(self, *, include_swap: bool, include_futures: bool) -> None:
        super().__init__()
        self._include_swap = include_swap
        self._include_futures = include_futures

    def run(self) -> None:
        try:
            manager = ArbitrageManager(OkxRestClient())
            rows = manager.scan_opportunities(
                include_swap=self._include_swap,
                include_futures=self._include_futures,
            )
            self.loaded.emit(rows)
        except Exception as exc:  # noqa: BLE001
            self.failed.emit(str(exc))


class ArbitrageChartLoadThread(QThread):
    loaded = Signal(object)
    failed = Signal(str)

    def __init__(self, *, spot_inst_id: str, derivative_inst_id: str, bar: str, limit: int) -> None:
        super().__init__()
        self._spot_inst_id = spot_inst_id.strip().upper()
        self._derivative_inst_id = derivative_inst_id.strip().upper()
        self._bar = bar.strip() or "4H"
        self._limit = max(60, min(int(limit), 800))

    def run(self) -> None:
        try:
            client = OkxRestClient()
            spot = client.get_candles_history(self._spot_inst_id, self._bar, limit=self._limit)
            derivative = client.get_candles_history(self._derivative_inst_id, self._bar, limit=self._limit)
            spread = build_spot_arbitrage_spread_candles(spot, derivative)
            if not spot:
                raise ValueError(f"{self._spot_inst_id} 没有获取到K线。")
            if not derivative:
                raise ValueError(f"{self._derivative_inst_id} 没有获取到K线。")
            if not spread:
                raise ValueError("现货和衍生品在当前周期没有可对齐的K线。")
            self.loaded.emit(
                ArbitrageChartPayload(
                    spot_inst_id=self._spot_inst_id,
                    derivative_inst_id=self._derivative_inst_id,
                    bar=self._bar,
                    spot_candles=spot,
                    derivative_candles=derivative,
                    spread_candles=spread,
                )
            )
        except Exception as exc:  # noqa: BLE001
            self.failed.emit(str(exc))


class ArbitrageCandleChart(QWidget):
    def __init__(self, title: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._chart = QChart()
        self._chart.legend().setVisible(True)
        self._chart.setBackgroundVisible(False)
        self._chart.setTitle(title)
        self._view = QChartView(self._chart)
        self._view.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        self._status = QLabel("等待加载")
        self._status.setObjectName("Subtle")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)
        layout.addWidget(self._status)
        layout.addWidget(self._view, 1)
        self.setMinimumHeight(260)

    def set_candles(self, *, title: str, candles: list[Candle], suffix: str = "") -> None:
        visible = _confirmed_or_all(candles)
        self._chart.removeAllSeries()
        for axis in list(self._chart.axes()):
            self._chart.removeAxis(axis)
        self._chart.setTitle(title)
        if not visible:
            self._status.setText("没有可显示的K线")
            return

        up_series = QCandlestickSeries()
        up_series.setName("上涨")
        up_series.setIncreasingColor(QColor("#16a34a"))
        up_series.setDecreasingColor(QColor("#16a34a"))
        up_series.setBodyOutlineVisible(False)
        down_series = QCandlestickSeries()
        down_series.setName("下跌")
        down_series.setIncreasingColor(QColor("#dc2626"))
        down_series.setDecreasingColor(QColor("#dc2626"))
        down_series.setBodyOutlineVisible(False)

        closes: list[Decimal] = []
        min_price: Decimal | None = None
        max_price: Decimal | None = None
        for candle in visible:
            target = up_series if candle.close >= candle.open else down_series
            target.append(
                QCandlestickSet(
                    float(candle.open),
                    float(candle.high),
                    float(candle.low),
                    float(candle.close),
                    int(candle.ts),
                )
            )
            closes.append(candle.close)
            min_price = candle.low if min_price is None else min(min_price, candle.low)
            max_price = candle.high if max_price is None else max(max_price, candle.high)

        self._chart.addSeries(up_series)
        self._chart.addSeries(down_series)

        ema5 = _ema(closes, 5)
        ema10 = _ema(closes, 10)
        ema20 = _ema(closes, 20)
        for name, color, values in (
            ("EMA5", "#f59e0b", ema5),
            ("EMA10", "#0e7490", ema10),
            ("EMA20", "#7c3aed", ema20),
        ):
            line = QLineSeries()
            line.setName(name)
            pen = QPen(QColor(color))
            pen.setWidthF(1.4)
            line.setPen(pen)
            for candle, value in zip(visible, values):
                if value is not None:
                    line.append(int(candle.ts), float(value))
            self._chart.addSeries(line)

        first_ts = int(visible[0].ts)
        last_ts = int(visible[-1].ts)
        axis_x = QDateTimeAxis()
        axis_x.setFormat("MM-dd HH:mm")
        axis_x.setTickCount(min(8, max(3, len(visible) // 40 + 2)))
        axis_x.setRange(QDateTime.fromMSecsSinceEpoch(first_ts), QDateTime.fromMSecsSinceEpoch(last_ts))

        axis_y = QValueAxis()
        if min_price is None or max_price is None:
            axis_y.setRange(-1.0, 1.0)
        else:
            diff = max_price - min_price
            padding = max(diff * Decimal("0.08"), Decimal("0.0001"))
            axis_y.setRange(float(min_price - padding), float(max_price + padding))
        axis_y.setLabelFormat("%.4f" if suffix == "%" else "%.2f")

        self._chart.addAxis(axis_x, Qt.AlignmentFlag.AlignBottom)
        self._chart.addAxis(axis_y, Qt.AlignmentFlag.AlignLeft)
        for series in self._chart.series():
            series.attachAxis(axis_x)
            series.attachAxis(axis_y)

        latest = visible[-1].close
        latest_time = datetime.fromtimestamp(int(visible[-1].ts) / 1000).strftime("%Y-%m-%d %H:%M")
        self._status.setText(f"最新 {fmt_decimal(latest, 4)}{suffix} | {latest_time} | {len(visible)} 根")


class SpotArbitrageScanWidget(QWidget):
    opportunity_selected = Signal(object)
    chart_requested = Signal(object)

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._rows: list[ArbitrageOpportunity] = []
        self._filtered_rows: list[ArbitrageOpportunity] = []
        self._scan_thread: ArbitrageScanThread | None = None
        self._auto_timer = QTimer(self)
        self._auto_timer.setInterval(5000)
        self._auto_timer.timeout.connect(self.start_scan)
        self._build_ui()

    def close_running_thread(self) -> None:
        if self._scan_thread is not None and self._scan_thread.isRunning():
            self._scan_thread.requestInterruption()
            if not self._scan_thread.wait(1500):
                self._scan_thread.requestInterruption()
                self._scan_thread.wait(800)

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        panel = QFrame()
        panel.setObjectName("Panel")
        controls = QGridLayout(panel)
        controls.setContentsMargins(12, 10, 12, 10)
        controls.setHorizontalSpacing(8)
        controls.setVerticalSpacing(6)

        self._scan_button = QPushButton("立即扫描")
        self._scan_button.setObjectName("Primary")
        self._scan_button.clicked.connect(self.start_scan)
        self._swap_check = QCheckBox("永续")
        self._swap_check.setChecked(True)
        self._futures_check = QCheckBox("交割")
        self._futures_check.setChecked(True)
        self._auto_check = QCheckBox("自动刷新(5s)")
        self._auto_check.toggled.connect(self._on_auto_changed)
        self._base_filter = QComboBox()
        self._base_filter.addItem("全部")
        self._base_filter.currentIndexChanged.connect(lambda _index: self._apply_filter())
        self._search = QLineEdit()
        self._search.setPlaceholderText("搜索币种 / 现货 / 衍生品")
        self._search.textChanged.connect(lambda _text: self._apply_filter())
        self._apply_button = QPushButton("带入终端")
        self._apply_button.clicked.connect(self._emit_selected_opportunity)
        self._chart_button = QPushButton("套利图表")
        self._chart_button.clicked.connect(self._emit_chart_requested)
        self._status = QLabel("等待扫描。")
        self._status.setObjectName("Subtle")

        controls.addWidget(self._scan_button, 0, 0)
        controls.addWidget(self._swap_check, 0, 1)
        controls.addWidget(self._futures_check, 0, 2)
        controls.addWidget(QLabel("币种"), 0, 3)
        controls.addWidget(self._base_filter, 0, 4)
        controls.addWidget(self._auto_check, 0, 5)
        controls.addWidget(self._apply_button, 0, 6)
        controls.addWidget(self._chart_button, 0, 7)
        controls.addWidget(QLabel("筛选"), 1, 0)
        controls.addWidget(self._search, 1, 1, 1, 5)
        controls.addWidget(self._status, 1, 6, 1, 2)
        layout.addWidget(panel)

        self._table = QTableWidget(0, len(SCAN_COLUMNS))
        self._table.setHorizontalHeaderLabels([label for _key, label in SCAN_COLUMNS])
        self._table.verticalHeader().setVisible(False)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self._table.cellDoubleClicked.connect(lambda _row, _column: self._emit_selected_opportunity())
        header = self._table.horizontalHeader()
        for column in range(len(SCAN_COLUMNS)):
            header.setSectionResizeMode(column, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        layout.addWidget(self._table, 1)

    @Slot()
    def start_scan(self) -> None:
        if self._scan_thread is not None and self._scan_thread.isRunning():
            return
        include_swap = self._swap_check.isChecked()
        include_futures = self._futures_check.isChecked()
        if not include_swap and not include_futures:
            QMessageBox.warning(self, "扫描参数错误", "永续和交割至少选择一项。")
            return
        self._status.setText("正在扫描现货套利机会...")
        self._scan_button.setEnabled(False)
        self._scan_thread = ArbitrageScanThread(include_swap=include_swap, include_futures=include_futures)
        self._scan_thread.loaded.connect(self._apply_rows)
        self._scan_thread.failed.connect(self._apply_error)
        self._scan_thread.finished.connect(self._finish_scan)
        self._scan_thread.start()

    @Slot(bool)
    def _on_auto_changed(self, checked: bool) -> None:
        if checked:
            self._auto_timer.start()
            self.start_scan()
        else:
            self._auto_timer.stop()

    @Slot(object)
    def _apply_rows(self, rows: list[ArbitrageOpportunity]) -> None:
        self._rows = sorted(rows, key=lambda item: item.net_annual_pct, reverse=True)
        current_base = self._base_filter.currentText()
        bases = ["全部", *sorted({row.base_ccy for row in self._rows})]
        self._base_filter.blockSignals(True)
        self._base_filter.clear()
        self._base_filter.addItems(bases)
        if current_base in bases:
            self._base_filter.setCurrentText(current_base)
        self._base_filter.blockSignals(False)
        self._apply_filter()
        self._status.setText(f"共 {len(self._rows)} 条机会，按净年化降序。")

    @Slot(str)
    def _apply_error(self, message: str) -> None:
        self._status.setText(f"扫描失败：{message}")
        QMessageBox.warning(self, "机会扫描失败", message)

    @Slot()
    def _finish_scan(self) -> None:
        self._scan_button.setEnabled(True)
        self._scan_thread = None

    def _apply_filter(self) -> None:
        base = self._base_filter.currentText().strip().upper()
        keyword = self._search.text().strip().lower()
        result: list[ArbitrageOpportunity] = []
        for row in self._rows:
            if base and base != "全部" and row.base_ccy.upper() != base:
                continue
            haystack = f"{row.base_ccy} {row.pair_kind_label} {row.spot_inst_id} {row.derivative_inst_id}".lower()
            if keyword and keyword not in haystack:
                continue
            result.append(row)
        self._filtered_rows = result
        self._render_rows()

    def _render_rows(self) -> None:
        self._table.setRowCount(len(self._filtered_rows))
        for row_index, opportunity in enumerate(self._filtered_rows):
            cells = format_scan_opportunity_cells(opportunity)
            for column, (key, _label) in enumerate(SCAN_COLUMNS):
                item = QTableWidgetItem(cells[key])
                if key in {"basis_abs", "basis", "funding", "fee", "slippage", "net", "days"}:
                    item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                if key == "net":
                    item.setForeground(QColor("#16a34a" if opportunity.net_annual_pct >= 0 else "#dc2626"))
                self._table.setItem(row_index, column, item)
        if self._filtered_rows:
            self._table.selectRow(0)

    def _selected_row(self) -> ArbitrageOpportunity | None:
        indexes = self._table.selectionModel().selectedRows()
        if not indexes:
            return None
        row = indexes[0].row()
        if row < 0 or row >= len(self._filtered_rows):
            return None
        return self._filtered_rows[row]

    def _emit_selected_opportunity(self) -> None:
        opportunity = self._selected_row()
        if opportunity is None:
            QMessageBox.information(self, "没有选中机会", "请先在扫描结果中选中一行。")
            return
        self.opportunity_selected.emit(opportunity)

    def _emit_chart_requested(self) -> None:
        opportunity = self._selected_row()
        if opportunity is None:
            QMessageBox.information(self, "没有选中机会", "请先在扫描结果中选中一行。")
            return
        self.chart_requested.emit(opportunity)


class SpotArbitrageChartWidget(QWidget):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._load_thread: ArbitrageChartLoadThread | None = None
        self._build_ui()

    def close_running_thread(self) -> None:
        if self._load_thread is not None and self._load_thread.isRunning():
            self._load_thread.requestInterruption()
            if not self._load_thread.wait(1500):
                self._load_thread.requestInterruption()
                self._load_thread.wait(800)

    def load_from_opportunity(self, opportunity: ArbitrageOpportunity) -> None:
        self._spot_input.setText(opportunity.spot_inst_id)
        self._derivative_input.setText(opportunity.derivative_inst_id)
        self.load_chart()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        panel = QFrame()
        panel.setObjectName("Panel")
        controls = QGridLayout(panel)
        controls.setContentsMargins(12, 10, 12, 10)
        controls.setHorizontalSpacing(8)
        controls.setVerticalSpacing(6)

        self._spot_input = QLineEdit("BTC-USDT")
        self._derivative_input = QLineEdit("BTC-USD-260925")
        self._bar_combo = QComboBox()
        for label, bar in ARBITRAGE_CHART_BAR_OPTIONS:
            self._bar_combo.addItem(label, bar)
        default_index = self._bar_combo.findData("4H")
        if default_index >= 0:
            self._bar_combo.setCurrentIndex(default_index)
        self._limit_input = QLineEdit("300")
        self._load_button = QPushButton("加载图表")
        self._load_button.setObjectName("Primary")
        self._load_button.clicked.connect(self.load_chart)
        self._status = QLabel("从扫描结果带入，或手动填写现货和衍生品合约。")
        self._status.setObjectName("Subtle")

        controls.addWidget(QLabel("现货"), 0, 0)
        controls.addWidget(self._spot_input, 0, 1)
        controls.addWidget(QLabel("衍生品"), 0, 2)
        controls.addWidget(self._derivative_input, 0, 3)
        controls.addWidget(QLabel("周期"), 0, 4)
        controls.addWidget(self._bar_combo, 0, 5)
        controls.addWidget(QLabel("K线数量"), 0, 6)
        controls.addWidget(self._limit_input, 0, 7)
        controls.addWidget(self._load_button, 0, 8)
        controls.addWidget(self._status, 1, 0, 1, 9)
        layout.addWidget(panel)

        chart_host = QFrame()
        chart_host.setObjectName("Panel")
        chart_layout = QVBoxLayout(chart_host)
        chart_layout.setContentsMargins(12, 12, 12, 12)
        chart_layout.setSpacing(12)
        self._spot_chart = ArbitrageCandleChart("现货 K 线")
        self._derivative_chart = ArbitrageCandleChart("衍生品 K 线")
        self._spread_chart = ArbitrageCandleChart("价差 K 线")
        chart_layout.addWidget(self._spot_chart)
        chart_layout.addWidget(self._derivative_chart)
        chart_layout.addWidget(self._spread_chart)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setWidget(chart_host)
        layout.addWidget(scroll, 1)

    @Slot()
    def load_chart(self) -> None:
        spot = self._spot_input.text().strip().upper()
        derivative = self._derivative_input.text().strip().upper()
        if not spot or not derivative:
            QMessageBox.warning(self, "参数错误", "请填写现货和衍生品合约。")
            return
        if spot == derivative:
            QMessageBox.warning(self, "参数错误", "现货和衍生品合约不能相同。")
            return
        if self._load_thread is not None and self._load_thread.isRunning():
            return
        limit = _parse_positive_int(self._limit_input.text(), default=300, minimum=60, maximum=800)
        self._limit_input.setText(str(limit))
        bar = str(self._bar_combo.currentData() or "4H")
        self._status.setText(f"正在加载 {spot} / {derivative} | {bar} | {limit} 根...")
        self._load_button.setEnabled(False)
        self._load_thread = ArbitrageChartLoadThread(
            spot_inst_id=spot,
            derivative_inst_id=derivative,
            bar=bar,
            limit=limit,
        )
        self._load_thread.loaded.connect(self._apply_payload)
        self._load_thread.failed.connect(self._apply_error)
        self._load_thread.finished.connect(self._finish_load)
        self._load_thread.start()

    @Slot(object)
    def _apply_payload(self, payload: ArbitrageChartPayload) -> None:
        self._spot_chart.set_candles(
            title=f"{payload.spot_inst_id} 现货K线 | {payload.bar}",
            candles=payload.spot_candles,
        )
        self._derivative_chart.set_candles(
            title=f"{payload.derivative_inst_id} 衍生品K线 | {payload.bar}",
            candles=payload.derivative_candles,
        )
        self._spread_chart.set_candles(
            title=f"价差K线：{payload.derivative_inst_id} - {payload.spot_inst_id} | {payload.bar}",
            candles=payload.spread_candles,
        )
        self._status.setText(
            f"已加载：现货 {len(_confirmed_or_all(payload.spot_candles))} 根 | "
            f"衍生品 {len(_confirmed_or_all(payload.derivative_candles))} 根 | "
            f"价差 {len(_confirmed_or_all(payload.spread_candles))} 根"
        )

    @Slot(str)
    def _apply_error(self, message: str) -> None:
        self._status.setText(f"加载失败：{message}")
        QMessageBox.warning(self, "套利图表加载失败", message)

    @Slot()
    def _finish_load(self) -> None:
        self._load_button.setEnabled(True)
        self._load_thread = None
