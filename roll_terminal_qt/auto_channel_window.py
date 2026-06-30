from __future__ import annotations

from decimal import Decimal, InvalidOperation

from PySide6.QtCharts import (
    QAreaSeries,
    QCandlestickSeries,
    QCandlestickSet,
    QChart,
    QChartView,
    QDateTimeAxis,
    QLineSeries,
    QScatterSeries,
    QValueAxis,
)
from PySide6.QtCore import QDateTime, QMargins, Qt, Slot
from PySide6.QtGui import QPainter
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFormLayout,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from okx_quant.analysis import (
    BoxDetectionConfig,
    ChannelDetectionConfig,
    PivotDetectionConfig,
    TrendlineDetectionConfig,
    TriangleDetectionConfig,
)
from okx_quant.auto_channel_preview import build_auto_channel_preview_snapshot, sample_auto_channel_candles
from okx_quant.auto_channel_storage import (
    build_auto_channel_snapshot_record,
    deserialize_strategy_live_chart_snapshot,
    load_auto_channel_snapshots,
    save_auto_channel_snapshots,
)
from okx_quant.okx_client import OkxRestClient
from okx_quant.strategy_live_chart import StrategyLiveChartSnapshot, build_auto_channel_live_chart_snapshot

from roll_terminal_qt.formatting import fmt_decimal
from roll_terminal_qt.profile_access import ensure_profile_unlocked, load_profile_snapshots


SOURCE_MODE_OPTIONS: tuple[tuple[str, str], ...] = (
    ("样例 sample", "sample"),
    ("市场行情 market", "market"),
    ("已存快照 saved", "saved"),
)
SAMPLE_OPTIONS: tuple[tuple[str, str], ...] = (
    ("上升通道", "channel"),
    ("箱体震荡", "box"),
)
BAR_OPTIONS: tuple[str, ...] = ("1m", "5m", "15m", "1H", "4H", "1D")


_SHARED_CLIENT: OkxRestClient | None = None


def _shared_client() -> OkxRestClient:
    global _SHARED_CLIENT
    if _SHARED_CLIENT is None:
        _SHARED_CLIENT = OkxRestClient()
    return _SHARED_CLIENT


def _safe_text(value: object) -> str:
    if value is None:
        return "-"
    text = str(value).strip()
    return text or "-"


def _parse_int(raw: str, field_name: str, *, minimum: int = 0) -> int:
    try:
        value = int(str(raw).strip())
    except ValueError as exc:
        raise RuntimeError(f"{field_name} 不是有效整数。") from exc
    if value < minimum:
        raise RuntimeError(f"{field_name} 不能小于 {minimum}。")
    return value


def _parse_decimal(raw: str, field_name: str, *, minimum: Decimal | None = None) -> Decimal:
    try:
        value = Decimal(str(raw).strip())
    except (InvalidOperation, ValueError) as exc:
        raise RuntimeError(f"{field_name} 不是有效数字。") from exc
    if minimum is not None and value < minimum:
        raise RuntimeError(f"{field_name} 不能小于 {minimum}。")
    return value


def _infer_candle_interval_ms(snapshot: StrategyLiveChartSnapshot) -> int:
    if len(snapshot.candles) >= 2:
        deltas = [
            max(1, int(snapshot.candles[index].ts - snapshot.candles[index - 1].ts))
            for index in range(1, len(snapshot.candles))
        ]
        deltas.sort()
        return deltas[len(deltas) // 2]
    return 60_000


def _timestamp_for_index(snapshot: StrategyLiveChartSnapshot, index: int, *, interval_ms: int | None = None) -> int:
    if not snapshot.candles:
        return 0
    if 0 <= index < len(snapshot.candles):
        return int(snapshot.candles[index].ts)
    step = interval_ms or _infer_candle_interval_ms(snapshot)
    last_ts = int(snapshot.candles[-1].ts)
    return last_ts + (max(0, index - len(snapshot.candles) + 1) * step)


class AutoChannelWindow(QMainWindow):
    def __init__(self, parent=None) -> None:  # noqa: ANN001
        super().__init__(parent)
        self.setWindowTitle("自动通道 - Qt")
        self.resize(1540, 940)
        self._client = _shared_client()
        self._snapshot = build_auto_channel_preview_snapshot("channel")
        self._saved_records: list[dict[str, object]] = []
        self._profile_snapshots: dict[str, dict[str, str]] = {}
        self._unlocked_profiles: set[str] = set()
        self._last_profile_name = ""

        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(14)

        layout.addWidget(self._build_header())
        layout.addWidget(self._build_toolbar())

        content = QSplitter(Qt.Orientation.Horizontal)
        content.addWidget(self._build_chart_panel())
        content.addWidget(self._build_side_panel())
        content.setChildrenCollapsible(False)
        content.setStretchFactor(0, 5)
        content.setStretchFactor(1, 1)
        content.setSizes([1280, 320])
        layout.addWidget(content, 1)

        self._refresh_profiles()
        self._load_saved_records()
        self._on_source_changed()
        self._reload_snapshot()

    def _build_header(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Panel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(8)
        title = QLabel("自动通道")
        title.setObjectName("SectionTitle")
        subtitle = QLabel(
            "纯 Qt 版本直接加载样例、真实 K 线和已存快照，支持参数调节、结构重算和结果沉淀。"
        )
        subtitle.setObjectName("Subtle")
        subtitle.setWordWrap(True)
        layout.addWidget(title)
        layout.addWidget(subtitle)
        return panel

    def _build_toolbar(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Guide")
        layout = QGridLayout(panel)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setHorizontalSpacing(12)
        layout.setVerticalSpacing(10)

        self._source_combo = QComboBox()
        for label, value in SOURCE_MODE_OPTIONS:
            self._source_combo.addItem(label, value)
        self._source_combo.currentIndexChanged.connect(self._on_source_changed)

        self._sample_combo = QComboBox()
        for label, value in SAMPLE_OPTIONS:
            self._sample_combo.addItem(label, value)
        self._profile_combo = QComboBox()
        self._profile_combo.currentIndexChanged.connect(self._on_profile_changed)
        self._symbol_edit = QLineEdit("BTC-USDT-SWAP")
        self._bar_combo = QComboBox()
        for item in BAR_OPTIONS:
            self._bar_combo.addItem(item, item)
        self._limit_edit = QLineEdit("240")
        self._saved_combo = QComboBox()

        refresh_button = QPushButton("重算结构")
        refresh_button.clicked.connect(self._reload_snapshot)
        save_button = QPushButton("保存快照")
        save_button.clicked.connect(self._save_snapshot)
        load_button = QPushButton("载入已存")
        load_button.clicked.connect(self._load_selected_saved_snapshot)
        self._status = QLabel("")
        self._status.setObjectName("Subtle")
        self._status.setWordWrap(True)

        layout.addWidget(QLabel("来源"), 0, 0)
        layout.addWidget(self._source_combo, 0, 1)
        layout.addWidget(QLabel("样例"), 0, 2)
        layout.addWidget(self._sample_combo, 0, 3)
        layout.addWidget(QLabel("标的"), 0, 4)
        layout.addWidget(self._symbol_edit, 0, 5)
        layout.addWidget(QLabel("周期"), 0, 6)
        layout.addWidget(self._bar_combo, 0, 7)
        layout.addWidget(QLabel("数量"), 0, 8)
        layout.addWidget(self._limit_edit, 0, 9)
        layout.addWidget(refresh_button, 0, 10)
        layout.addWidget(save_button, 0, 11)
        layout.addWidget(QLabel("已存"), 1, 0)
        layout.addWidget(self._saved_combo, 1, 1, 1, 6)
        layout.addWidget(load_button, 1, 7)
        layout.addWidget(self._status, 1, 8, 1, 4)
        layout.addWidget(QLabel("API Profile"), 2, 0)
        layout.addWidget(self._profile_combo, 2, 1, 1, 3)
        return panel

    def _build_chart_panel(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Panel")
        panel.setMinimumWidth(1120)
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)
        layout.addWidget(QLabel("结构图"))

        self._chart = QChart()
        self._chart.legend().hide()
        self._chart.setBackgroundVisible(False)
        self._chart.setMargins(QMargins(6, 6, 6, 6))
        self._chart_view = QChartView(self._chart)
        self._chart_view.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        self._chart_view.setMinimumHeight(760)
        layout.addWidget(self._chart_view, 1)
        return panel

    def _build_side_panel(self) -> QWidget:
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setMinimumWidth(320)
        scroll.setMaximumWidth(420)
        panel = QFrame()
        panel.setObjectName("Panel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)

        layout.addWidget(QLabel("检测参数"))
        form = QWidget()
        form_layout = QFormLayout(form)
        form_layout.setContentsMargins(0, 0, 0, 0)
        form_layout.setSpacing(10)

        self._pivot_left_edit = QLineEdit("1")
        self._pivot_right_edit = QLineEdit("1")
        self._pivot_atr_period_edit = QLineEdit("3")
        self._pivot_atr_mult_edit = QLineEdit("0")
        self._pivot_min_distance_edit = QLineEdit("1")
        self._channel_anchor_distance_edit = QLineEdit("8")
        self._channel_min_bars_edit = QLineEdit("18")
        self._channel_max_violations_edit = QLineEdit("8")
        self._box_min_bars_edit = QLineEdit("18")
        self._box_touch_edit = QLineEdit("2")
        self._box_max_violations_edit = QLineEdit("8")
        self._trendline_max_edit = QLineEdit("2")
        self._triangle_max_edit = QLineEdit("1")
        self._right_pad_edit = QLineEdit("50")
        self._extend_edit = QLineEdit("50")
        self._show_pivots_check = QCheckBox("显示结构点")
        self._show_pivots_check.setChecked(True)

        form_layout.addRow("pivot left", self._pivot_left_edit)
        form_layout.addRow("pivot right", self._pivot_right_edit)
        form_layout.addRow("ATR period", self._pivot_atr_period_edit)
        form_layout.addRow("ATR mult", self._pivot_atr_mult_edit)
        form_layout.addRow("最小 pivot 间距", self._pivot_min_distance_edit)
        form_layout.addRow("通道锚点间距", self._channel_anchor_distance_edit)
        form_layout.addRow("通道最少 K", self._channel_min_bars_edit)
        form_layout.addRow("通道最大违规", self._channel_max_violations_edit)
        form_layout.addRow("箱体最少 K", self._box_min_bars_edit)
        form_layout.addRow("箱体每侧触点", self._box_touch_edit)
        form_layout.addRow("箱体最大违规", self._box_max_violations_edit)
        form_layout.addRow("趋势线数量", self._trendline_max_edit)
        form_layout.addRow("三角形数量", self._triangle_max_edit)
        form_layout.addRow("右侧补白", self._right_pad_edit)
        form_layout.addRow("延伸 bars", self._extend_edit)
        form_layout.addRow(self._show_pivots_check)
        layout.addWidget(form)

        self._note = QLabel("")
        self._note.setWordWrap(True)
        self._note.setObjectName("Subtle")
        layout.addWidget(self._note)

        self._metrics = QTableWidget(0, 2)
        self._metrics.setHorizontalHeaderLabels(["指标", "值"])
        self._metrics.verticalHeader().setVisible(False)
        self._metrics.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self._metrics)
        scroll.setWidget(panel)
        return scroll

    def _refresh_profiles(self) -> None:
        self._profile_snapshots, selected_profile = load_profile_snapshots()
        self._unlocked_profiles.intersection_update(set(self._profile_snapshots))
        current = self._current_profile_name()
        self._profile_combo.blockSignals(True)
        self._profile_combo.clear()
        for profile_name in self._profile_snapshots:
            self._profile_combo.addItem(profile_name, profile_name)
        self._profile_combo.blockSignals(False)
        if self._profile_combo.count() <= 0:
            return
        target = current or self._last_profile_name or selected_profile
        index = self._profile_combo.findData(target)
        if index < 0:
            index = 0
        self._profile_combo.setCurrentIndex(index)
        self._last_profile_name = self._current_profile_name()

    def _current_profile_name(self) -> str:
        return str(self._profile_combo.currentData() or self._profile_combo.currentText() or "").strip()

    def _ensure_profile_access(self, profile_name: str) -> bool:
        if profile_name.strip() not in self._profile_snapshots:
            self._profile_snapshots, _selected_profile = load_profile_snapshots()
            self._unlocked_profiles.intersection_update(set(self._profile_snapshots))
        return ensure_profile_unlocked(self, profile_name, self._profile_snapshots, self._unlocked_profiles)

    def _restore_profile_selection(self) -> None:
        if not self._last_profile_name:
            return
        index = self._profile_combo.findData(self._last_profile_name)
        if index < 0:
            return
        self._profile_combo.blockSignals(True)
        self._profile_combo.setCurrentIndex(index)
        self._profile_combo.blockSignals(False)

    @Slot()
    def _on_profile_changed(self) -> None:
        selected = self._current_profile_name()
        if not selected:
            return
        self._profile_snapshots, _selected_profile = load_profile_snapshots()
        self._unlocked_profiles.intersection_update(set(self._profile_snapshots))
        if selected != self._last_profile_name and not self._ensure_profile_access(selected):
            self._restore_profile_selection()
            return
        self._last_profile_name = selected
        self._status.setText(f"当前 API Profile: {selected}")

    @Slot()
    def _on_source_changed(self) -> None:
        source = self._source_mode()
        sample_enabled = source == "sample"
        market_enabled = source == "market"
        saved_enabled = source == "saved"
        self._sample_combo.setEnabled(sample_enabled)
        self._symbol_edit.setEnabled(market_enabled)
        self._bar_combo.setEnabled(market_enabled)
        self._limit_edit.setEnabled(market_enabled)
        self._saved_combo.setEnabled(saved_enabled)

    def _source_mode(self) -> str:
        return str(self._source_combo.currentData() or "sample")

    def _load_saved_records(self) -> None:
        self._saved_records = load_auto_channel_snapshots()
        current = str(self._saved_combo.currentData() or "").strip()
        self._saved_combo.clear()
        for record in self._saved_records:
            record_id = str(record.get("record_id", "") or "").strip()
            label = str(record.get("label", "") or record_id)
            self._saved_combo.addItem(label, record_id)
        if current:
            for index in range(self._saved_combo.count()):
                if str(self._saved_combo.itemData(index) or "") == current:
                    self._saved_combo.setCurrentIndex(index)
                    break

    def _build_snapshot_from_form(self) -> StrategyLiveChartSnapshot:
        source = self._source_mode()
        if source == "saved":
            record = self._selected_saved_record()
            if record is None:
                raise RuntimeError("请选择一个已存快照。")
            snapshot_payload = record.get("snapshot")
            if not isinstance(snapshot_payload, dict):
                raise RuntimeError("已存快照内容无效。")
            return deserialize_strategy_live_chart_snapshot(snapshot_payload)

        if source == "sample":
            sample_key = str(self._sample_combo.currentData() or "channel")
            candles = sample_auto_channel_candles(sample_key)
            channel_config, box_config, trendline_config, triangle_config, right_pad_bars, extend_bars = (
                self._analysis_configs_from_form()
            )
            return build_auto_channel_live_chart_snapshot(
                session_id=f"auto-channel:sample:{sample_key}",
                candles=candles,
                channel_config=channel_config,
                box_config=box_config,
                trendline_config=trendline_config,
                triangle_config=triangle_config,
                max_channels=1,
                max_boxes=1,
                max_trendlines=_parse_int(self._trendline_max_edit.text(), "趋势线数量", minimum=0),
                max_triangles=_parse_int(self._triangle_max_edit.text(), "三角形数量", minimum=0),
                show_pivots=self._show_pivots_check.isChecked(),
                right_pad_bars=right_pad_bars,
                channel_extend_bars=extend_bars,
                latest_price=candles[-1].close if candles else None,
                note=f"sample:{sample_key}",
            )

        symbol = self._symbol_edit.text().strip().upper()
        if not symbol:
            raise RuntimeError("请填写标的。")
        profile_name = self._current_profile_name()
        if profile_name and not self._ensure_profile_access(profile_name):
            raise RuntimeError(f"API Profile {profile_name} 尚未解锁。")
        bar = str(self._bar_combo.currentData() or "1H")
        limit = _parse_int(self._limit_edit.text(), "K 线数量", minimum=30)
        candles = self._client.get_candles_history(symbol, bar, limit=limit)
        if not candles:
            raise RuntimeError("没有获取到 K 线。")
        channel_config, box_config, trendline_config, triangle_config, right_pad_bars, extend_bars = (
            self._analysis_configs_from_form()
        )
        return build_auto_channel_live_chart_snapshot(
            session_id=f"auto-channel:{symbol}:{bar}",
            candles=candles,
            channel_config=channel_config,
            box_config=box_config,
            trendline_config=trendline_config,
            triangle_config=triangle_config,
            max_channels=1,
            max_boxes=1,
            max_trendlines=_parse_int(self._trendline_max_edit.text(), "趋势线数量", minimum=0),
            max_triangles=_parse_int(self._triangle_max_edit.text(), "三角形数量", minimum=0),
            show_pivots=self._show_pivots_check.isChecked(),
            right_pad_bars=right_pad_bars,
            channel_extend_bars=extend_bars,
            latest_price=candles[-1].close,
            note=f"{symbol} | {bar}",
        )

    def _analysis_configs_from_form(
        self,
    ) -> tuple[
        ChannelDetectionConfig,
        BoxDetectionConfig,
        TrendlineDetectionConfig,
        TriangleDetectionConfig,
        int,
        int,
    ]:
        pivot = PivotDetectionConfig(
            left_bars=_parse_int(self._pivot_left_edit.text(), "pivot left", minimum=1),
            right_bars=_parse_int(self._pivot_right_edit.text(), "pivot right", minimum=1),
            atr_period=_parse_int(self._pivot_atr_period_edit.text(), "ATR period", minimum=1),
            atr_multiplier=_parse_decimal(self._pivot_atr_mult_edit.text(), "ATR mult", minimum=Decimal("0")),
            min_index_distance=_parse_int(self._pivot_min_distance_edit.text(), "pivot 间距", minimum=0),
        )
        channel_config = ChannelDetectionConfig(
            pivot=pivot,
            min_anchor_distance=_parse_int(self._channel_anchor_distance_edit.text(), "通道锚点间距", minimum=1),
            min_channel_bars=_parse_int(self._channel_min_bars_edit.text(), "通道最少 K", minimum=2),
            max_violations=_parse_int(self._channel_max_violations_edit.text(), "通道最大违规", minimum=0),
        )
        box_config = BoxDetectionConfig(
            pivot=pivot,
            min_box_bars=_parse_int(self._box_min_bars_edit.text(), "箱体最少 K", minimum=2),
            min_touches_per_side=_parse_int(self._box_touch_edit.text(), "箱体每侧触点", minimum=1),
            max_violations=_parse_int(self._box_max_violations_edit.text(), "箱体最大违规", minimum=0),
        )
        trendline_config = TrendlineDetectionConfig(pivot=pivot)
        triangle_config = TriangleDetectionConfig(pivot=pivot)
        right_pad_bars = _parse_int(self._right_pad_edit.text(), "右侧补白", minimum=0)
        extend_bars = _parse_int(self._extend_edit.text(), "延伸 bars", minimum=0)
        return channel_config, box_config, trendline_config, triangle_config, right_pad_bars, extend_bars

    def _selected_saved_record(self) -> dict[str, object] | None:
        record_id = str(self._saved_combo.currentData() or "").strip()
        return next((item for item in self._saved_records if str(item.get("record_id", "")) == record_id), None)

    @Slot()
    def _reload_snapshot(self) -> None:
        try:
            self._snapshot = self._build_snapshot_from_form()
        except Exception as exc:  # noqa: BLE001
            self._show_error("结构重算失败", str(exc))
            return
        self._status.setText(
            f"已完成结构重算：candles={len(self._snapshot.candles)} | "
            f"channels={len(self._snapshot.band_overlays)} | boxes={len(self._snapshot.box_overlays)}"
        )
        self._note.setText(self._snapshot.note or "暂无结构摘要。")
        self._populate_metrics(self._snapshot)
        self._render_snapshot(self._snapshot)

    @Slot()
    def _save_snapshot(self) -> None:
        try:
            if self._source_mode() == "saved":
                source_mode = "saved-reloaded"
            else:
                source_mode = self._source_mode()
            symbol = self._symbol_edit.text().strip().upper() or str(self._sample_combo.currentData() or "sample")
            bar = str(self._bar_combo.currentData() or "1H")
            limit = 0
            if self._source_mode() == "market":
                limit = _parse_int(self._limit_edit.text(), "K 线数量", minimum=30)
            record = build_auto_channel_snapshot_record(
                snapshot=self._snapshot,
                source_mode=source_mode,
                symbol=symbol,
                bar=bar,
                label=f"{symbol} | {bar} | {source_mode}",
                api_profile=self._current_profile_name(),
                candle_limit=limit,
            )
            existing = [item for item in self._saved_records if str(item.get("record_id", "")) != str(record["record_id"])]
            existing.insert(0, record)
            save_auto_channel_snapshots(existing)
            self._load_saved_records()
        except Exception as exc:  # noqa: BLE001
            self._show_error("保存失败", str(exc))
            return
        self._status.setText("自动通道快照已保存。")

    @Slot()
    def _load_selected_saved_snapshot(self) -> None:
        record = self._selected_saved_record()
        if record is None:
            self._show_info("提示", "请先选择一个已存快照。")
            return
        record_profile = str(record.get("api_profile", "") or "").strip()
        if record_profile in self._profile_snapshots:
            if not self._ensure_profile_access(record_profile):
                self._show_error("加载失败", f"API Profile {record_profile} 尚未解锁。")
                self._restore_profile_selection()
                return
            index = self._profile_combo.findData(record_profile)
            if index >= 0:
                self._profile_combo.blockSignals(True)
                self._profile_combo.setCurrentIndex(index)
                self._profile_combo.blockSignals(False)
            self._last_profile_name = record_profile
        self._source_combo.setCurrentIndex(self._source_combo.findData("saved"))
        self._on_source_changed()
        self._reload_snapshot()

    def _show_error(self, title: str, message: str) -> None:
        self._status.setText(message)
        QMessageBox.critical(self, title, message)

    def _show_info(self, title: str, message: str) -> None:
        self._status.setText(message)
        QMessageBox.information(self, title, message)

    def _populate_metrics(self, snapshot: StrategyLiveChartSnapshot) -> None:
        rows = [
            ("K 线根数", str(len(snapshot.candles))),
            ("通道数", str(len(snapshot.band_overlays))),
            ("箱体数", str(len(snapshot.box_overlays))),
            ("趋势线", str(len(snapshot.line_overlays))),
            ("结构点", str(len(snapshot.point_overlays))),
            ("最新价", fmt_decimal(snapshot.latest_price) if snapshot.latest_price is not None else "-"),
        ]
        self._metrics.setRowCount(len(rows))
        for row, (label, value) in enumerate(rows):
            self._metrics.setItem(row, 0, QTableWidgetItem(label))
            self._metrics.setItem(row, 1, QTableWidgetItem(value))

    def _render_snapshot(self, snapshot: StrategyLiveChartSnapshot) -> None:
        self._chart.removeAllSeries()
        for axis in list(self._chart.axes()):
            self._chart.removeAxis(axis)

        if not snapshot.candles:
            return

        interval_ms = _infer_candle_interval_ms(snapshot)
        plot_count = len(snapshot.candles) + max(0, int(snapshot.right_pad_bars))

        candle_series = QCandlestickSeries()
        candle_series.setName("K 线")
        candle_series.setIncreasingColor(Qt.GlobalColor.red)
        candle_series.setDecreasingColor(Qt.GlobalColor.darkGreen)
        candle_series.setBodyOutlineVisible(True)

        close_series = QLineSeries()
        close_series.setName("收盘")
        close_series.setColor(Qt.GlobalColor.black)

        min_price: Decimal | None = None
        max_price: Decimal | None = None
        first_ts = snapshot.candles[0].ts
        last_ts = _timestamp_for_index(snapshot, max(0, plot_count - 1), interval_ms=interval_ms)

        for candle in snapshot.candles:
            timestamp_ms = int(candle.ts)
            candle_series.append(
                QCandlestickSet(
                    float(candle.open),
                    float(candle.high),
                    float(candle.low),
                    float(candle.close),
                    timestamp_ms,
                )
            )
            close_series.append(timestamp_ms, float(candle.close))
            min_price = candle.low if min_price is None else min(min_price, candle.low)
            max_price = candle.high if max_price is None else max(max_price, candle.high)

        self._chart.addSeries(candle_series)
        self._chart.addSeries(close_series)

        for overlay in snapshot.line_overlays:
            series = QLineSeries()
            series.setName(overlay.label)
            overlay_end = max(overlay.line.end_index, len(snapshot.candles) - 1)
            for index in range(max(0, overlay.line.start_index), min(overlay_end + 1, plot_count)):
                series.append(
                    _timestamp_for_index(snapshot, index, interval_ms=interval_ms),
                    float(overlay.line.value_at(index)),
                )
            self._chart.addSeries(series)

        for overlay in snapshot.band_overlays:
            upper = QLineSeries()
            lower = QLineSeries()
            upper.setName(f"{overlay.label} 上轨")
            lower.setName(f"{overlay.label} 下轨")
            for index in range(max(0, overlay.start_index), min(overlay.end_index + 1, plot_count)):
                ts = _timestamp_for_index(snapshot, index, interval_ms=interval_ms)
                upper.append(ts, float(overlay.upper_line.value_at(index)))
                lower.append(ts, float(overlay.lower_line.value_at(index)))
            self._chart.addSeries(upper)
            self._chart.addSeries(lower)
            area = QAreaSeries(upper, lower)
            area.setName(overlay.label)
            area.setOpacity(0.12)
            self._chart.addSeries(area)

        for overlay in snapshot.box_overlays:
            top = QLineSeries()
            bottom = QLineSeries()
            top.setName(f"{overlay.label} 上沿")
            bottom.setName(f"{overlay.label} 下沿")
            for index in range(max(0, overlay.start_index), min(overlay.end_index + 1, plot_count)):
                ts = _timestamp_for_index(snapshot, index, interval_ms=interval_ms)
                top.append(ts, float(overlay.upper))
                bottom.append(ts, float(overlay.lower))
            self._chart.addSeries(top)
            self._chart.addSeries(bottom)
            area = QAreaSeries(top, bottom)
            area.setName(overlay.label)
            area.setOpacity(0.1)
            self._chart.addSeries(area)

        if snapshot.point_overlays:
            scatter = QScatterSeries()
            scatter.setName("结构点")
            scatter.setMarkerSize(8.0)
            for overlay in snapshot.point_overlays:
                if 0 <= overlay.index < len(snapshot.candles):
                    scatter.append(int(snapshot.candles[overlay.index].ts), float(overlay.price))
            self._chart.addSeries(scatter)

        time_axis = QDateTimeAxis()
        time_axis.setFormat("MM-dd HH:mm")
        time_axis.setTickCount(6)
        time_axis.setRange(
            QDateTime.fromMSecsSinceEpoch(int(first_ts)),
            QDateTime.fromMSecsSinceEpoch(int(last_ts)),
        )
        price_axis = QValueAxis()
        price_axis.setLabelFormat("%.4f")
        if min_price is None or max_price is None:
            min_price = Decimal("0")
            max_price = Decimal("1")
        padding = max((max_price - min_price) * Decimal("0.06"), Decimal("0.5"))
        price_axis.setRange(float(min_price - padding), float(max_price + padding))

        self._chart.addAxis(time_axis, Qt.AlignmentFlag.AlignBottom)
        self._chart.addAxis(price_axis, Qt.AlignmentFlag.AlignLeft)
        for series in self._chart.series():
            series.attachAxis(time_axis)
            series.attachAxis(price_axis)
