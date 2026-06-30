from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation

from PySide6.QtCharts import (
    QAreaSeries,
    QCandlestickSeries,
    QCandlestickSet,
    QChart,
    QChartView,
    QDateTimeAxis,
    QLineSeries,
    QValueAxis,
)
from PySide6.QtCore import QDateTime, Qt, Slot
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
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import (
    load_line_trading_desk_annotations_entries,
    save_line_trading_desk_annotations_entries,
)
from roll_terminal_qt.profile_access import ensure_profile_unlocked, load_profile_snapshots


LINE_KIND_OPTIONS: tuple[tuple[str, str], ...] = (
    ("趋势线 line", "line"),
    ("水平线 horizontal", "horizontal"),
    ("止损线 stop", "stop"),
)
RAY_ACTION_OPTIONS: tuple[tuple[str, str], ...] = (
    ("通知 notify", "notify"),
    ("开多 long", "long"),
    ("开空 short", "short"),
)
RR_SIDE_OPTIONS: tuple[tuple[str, str], ...] = (
    ("多头 long", "long"),
    ("空头 short", "short"),
)


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


def _split_annotation_key(key: str) -> tuple[str, str, str]:
    parts = [segment.strip() for segment in key.split("|")]
    if len(parts) >= 3:
        return parts[0] or "-", parts[1] or "-", parts[2] or "-"
    if len(parts) == 2:
        return parts[0] or "-", parts[1] or "-", "-"
    return key.strip() or "-", "-", "-"


def _build_annotation_key(api_name: str, symbol: str, bar: str) -> str:
    return f"{api_name.strip()}|{symbol.strip().upper()}|{bar.strip()}"


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f") if value != value.to_integral() else format(value.quantize(Decimal("1")), "f")


def _parse_decimal(raw: str, field_name: str) -> Decimal:
    try:
        value = Decimal(str(raw).strip())
    except (InvalidOperation, ValueError) as exc:
        raise RuntimeError(f"{field_name} 不是有效数字。") from exc
    return value


def _parse_optional_float(raw: str) -> float | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError as exc:
        raise RuntimeError("Bar 序号必须是数字。") from exc


def _compute_rr_target(side: str, entry_price: Decimal, stop_price: Decimal, r_multiple: Decimal) -> Decimal:
    if r_multiple <= 0:
        raise RuntimeError("R 倍数必须大于 0。")
    if side == "long":
        risk = entry_price - stop_price
        if risk <= 0:
            raise RuntimeError("多头 RR 中，止损价必须低于入场价。")
        return entry_price + (risk * r_multiple)
    risk = stop_price - entry_price
    if risk <= 0:
        raise RuntimeError("空头 RR 中，止损价必须高于入场价。")
    return entry_price - (risk * r_multiple)


class LineTradingQtWindow(QMainWindow):
    def __init__(self, parent=None) -> None:  # noqa: ANN001
        super().__init__(parent)
        self.setWindowTitle("划线交易台 - Qt")
        self.resize(1540, 920)
        self._client = _shared_client()
        self._entries: dict[str, dict[str, object]] = {}
        self._selected_session_key = ""
        self._selected_line_index = -1
        self._selected_rr_index = -1
        self._profile_snapshots: dict[str, dict[str, str]] = {}
        self._unlocked_profiles: set[str] = set()
        self._last_profile_name = ""
        self._session_switch_guard = False

        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(14)

        layout.addWidget(self._build_header())
        layout.addWidget(self._build_session_toolbar())
        layout.addWidget(self._build_metrics_panel())

        content = QSplitter(Qt.Orientation.Horizontal)
        content.addWidget(self._build_session_panel())
        content.addWidget(self._build_line_panel())
        content.addWidget(self._build_rr_panel())
        content.setSizes([640, 480, 420])
        layout.addWidget(content, 1)

        self._refresh_profiles()
        self.refresh_entries()

    def _build_header(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Panel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(8)
        title = QLabel("划线交易台")
        title.setObjectName("SectionTitle")
        subtitle = QLabel(
            "纯 Qt 版本直接管理共享注解文件。会话、射线、RR 区块都可在这里编辑、保存和整理。"
        )
        subtitle.setObjectName("Subtle")
        subtitle.setWordWrap(True)
        layout.addWidget(title)
        layout.addWidget(subtitle)
        return panel

    def _build_session_toolbar(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Guide")
        layout = QGridLayout(panel)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setHorizontalSpacing(12)
        layout.setVerticalSpacing(10)

        self._api_edit = QComboBox()
        self._api_edit.currentIndexChanged.connect(self._on_profile_changed)
        self._symbol_edit = QLineEdit("BTC-USDT-SWAP")
        self._bar_edit = QLineEdit("1H")
        self._status = QLabel("")
        self._status.setObjectName("Subtle")
        self._status.setWordWrap(True)

        load_button = QPushButton("载入/创建会话")
        load_button.clicked.connect(self._load_or_create_session)
        save_button = QPushButton("保存全部")
        save_button.clicked.connect(self._save_entries)
        delete_button = QPushButton("删除会话")
        delete_button.clicked.connect(self._delete_session)
        clear_button = QPushButton("清空未锁定")
        clear_button.clicked.connect(self._clear_unlocked_items)
        refresh_button = QPushButton("刷新")
        refresh_button.clicked.connect(self.refresh_entries)

        layout.addWidget(QLabel("API Profile"), 0, 0)
        layout.addWidget(self._api_edit, 0, 1)
        layout.addWidget(QLabel("标的"), 0, 2)
        layout.addWidget(self._symbol_edit, 0, 3)
        layout.addWidget(QLabel("周期"), 0, 4)
        layout.addWidget(self._bar_edit, 0, 5)
        layout.addWidget(load_button, 0, 6)
        layout.addWidget(save_button, 0, 7)
        layout.addWidget(delete_button, 0, 8)
        layout.addWidget(clear_button, 0, 9)
        layout.addWidget(refresh_button, 0, 10)
        layout.addWidget(self._status, 1, 0, 1, 11)
        return panel

    def _build_metrics_panel(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Panel")
        layout = QHBoxLayout(panel)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(18)
        self._session_metric = QLabel("")
        self._line_metric = QLabel("")
        self._rr_metric = QLabel("")
        self._selected_metric = QLabel("")
        for item in (self._session_metric, self._line_metric, self._rr_metric, self._selected_metric):
            item.setObjectName("GuideText")
            layout.addWidget(item, 1)
        return panel

    def _build_session_panel(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Panel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)
        layout.addWidget(QLabel("会话"))

        self._session_table = QTableWidget(0, 6)
        self._session_table.setHorizontalHeaderLabels(["API", "标的", "周期", "射线", "RR", "锁定"])
        self._session_table.verticalHeader().setVisible(False)
        self._session_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._session_table.itemSelectionChanged.connect(self._on_session_selected)
        layout.addWidget(self._session_table)

        layout.addWidget(QLabel("K 线图"))
        self._chart = QChart()
        self._chart.legend().setVisible(True)
        self._chart.setBackgroundVisible(False)
        self._chart_view = QChartView(self._chart)
        self._chart_view.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        self._chart_view.setMinimumHeight(320)
        layout.addWidget(self._chart_view, 1)

        layout.addWidget(QLabel("原始详情"))
        self._detail_text = QTextEdit()
        self._detail_text.setReadOnly(True)
        self._detail_text.setMinimumHeight(160)
        layout.addWidget(self._detail_text)
        return panel

    def _build_line_panel(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Panel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)

        top = QHBoxLayout()
        top.addWidget(QLabel("射线 / 线段"))
        top.addStretch(1)
        add_button = QPushButton("新增/保存射线")
        add_button.clicked.connect(self._save_line_item)
        remove_button = QPushButton("删除选中射线")
        remove_button.clicked.connect(self._remove_line_item)
        top.addWidget(add_button)
        top.addWidget(remove_button)
        layout.addLayout(top)

        self._line_table = QTableWidget(0, 8)
        self._line_table.setHorizontalHeaderLabels(
            ["类型", "标签", "动作", "A 价", "B 价", "A bar", "B bar", "锁定"]
        )
        self._line_table.verticalHeader().setVisible(False)
        self._line_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._line_table.itemSelectionChanged.connect(self._on_line_selected)
        layout.addWidget(self._line_table, 1)

        form = QWidget()
        form_layout = QFormLayout(form)
        form_layout.setContentsMargins(0, 0, 0, 0)
        form_layout.setSpacing(10)
        self._line_kind_combo = QComboBox()
        for label, value in LINE_KIND_OPTIONS:
            self._line_kind_combo.addItem(label, value)
        self._line_label_edit = QLineEdit()
        self._line_action_combo = QComboBox()
        for label, value in RAY_ACTION_OPTIONS:
            self._line_action_combo.addItem(label, value)
        self._line_price_a_edit = QLineEdit()
        self._line_price_b_edit = QLineEdit()
        self._line_bar_a_edit = QLineEdit()
        self._line_bar_b_edit = QLineEdit()
        self._line_color_edit = QLineEdit("#1d4ed8")
        self._line_locked_check = QCheckBox("锁定")
        self._line_triggered_check = QCheckBox("已触发")

        form_layout.addRow("类型", self._line_kind_combo)
        form_layout.addRow("标签", self._line_label_edit)
        form_layout.addRow("动作", self._line_action_combo)
        form_layout.addRow("价格 A", self._line_price_a_edit)
        form_layout.addRow("价格 B", self._line_price_b_edit)
        form_layout.addRow("Bar A", self._line_bar_a_edit)
        form_layout.addRow("Bar B", self._line_bar_b_edit)
        form_layout.addRow("颜色", self._line_color_edit)
        form_layout.addRow(self._line_locked_check)
        form_layout.addRow(self._line_triggered_check)
        layout.addWidget(form)
        return panel

    def _build_rr_panel(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Panel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)

        top = QHBoxLayout()
        top.addWidget(QLabel("RR 区块"))
        top.addStretch(1)
        add_button = QPushButton("新增/保存 RR")
        add_button.clicked.connect(self._save_rr_item)
        remove_button = QPushButton("删除选中 RR")
        remove_button.clicked.connect(self._remove_rr_item)
        top.addWidget(add_button)
        top.addWidget(remove_button)
        layout.addLayout(top)

        self._rr_table = QTableWidget(0, 7)
        self._rr_table.setHorizontalHeaderLabels(["方向", "入场", "止损", "止盈", "R", "Bar", "锁定"])
        self._rr_table.verticalHeader().setVisible(False)
        self._rr_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._rr_table.itemSelectionChanged.connect(self._on_rr_selected)
        layout.addWidget(self._rr_table, 1)

        form = QWidget()
        form_layout = QFormLayout(form)
        form_layout.setContentsMargins(0, 0, 0, 0)
        form_layout.setSpacing(10)
        self._rr_side_combo = QComboBox()
        for label, value in RR_SIDE_OPTIONS:
            self._rr_side_combo.addItem(label, value)
        self._rr_entry_edit = QLineEdit()
        self._rr_stop_edit = QLineEdit()
        self._rr_r_edit = QLineEdit("2")
        self._rr_bar_edit = QLineEdit("0")
        self._rr_locked_check = QCheckBox("锁定")
        self._rr_preview = QLabel("止盈会按入场、止损和 R 倍数自动计算。")
        self._rr_preview.setObjectName("Subtle")
        self._rr_preview.setWordWrap(True)

        form_layout.addRow("方向", self._rr_side_combo)
        form_layout.addRow("入场价", self._rr_entry_edit)
        form_layout.addRow("止损价", self._rr_stop_edit)
        form_layout.addRow("R 倍数", self._rr_r_edit)
        form_layout.addRow("Bar", self._rr_bar_edit)
        form_layout.addRow(self._rr_locked_check)
        form_layout.addRow(self._rr_preview)
        layout.addWidget(form)
        return panel

    def _refresh_profiles(self) -> None:
        self._profile_snapshots, selected_profile = load_profile_snapshots()
        self._unlocked_profiles.intersection_update(set(self._profile_snapshots))
        current = self._selected_profile_name()
        self._api_edit.blockSignals(True)
        self._api_edit.clear()
        for profile_name in self._profile_snapshots:
            self._api_edit.addItem(profile_name, profile_name)
        self._api_edit.blockSignals(False)
        if self._api_edit.count() <= 0:
            return
        target = current or self._last_profile_name or selected_profile
        index = self._api_edit.findData(target)
        if index < 0:
            index = 0
        self._api_edit.setCurrentIndex(index)
        self._last_profile_name = self._selected_profile_name()

    def _selected_profile_name(self) -> str:
        return str(self._api_edit.currentData() or self._api_edit.currentText() or "").strip()

    def _ensure_profile_access(self, profile_name: str) -> bool:
        if profile_name.strip() not in self._profile_snapshots:
            self._profile_snapshots, _selected_profile = load_profile_snapshots()
            self._unlocked_profiles.intersection_update(set(self._profile_snapshots))
        return ensure_profile_unlocked(self, profile_name, self._profile_snapshots, self._unlocked_profiles)

    def _restore_profile_selection(self) -> None:
        if not self._last_profile_name:
            return
        index = self._api_edit.findData(self._last_profile_name)
        if index < 0:
            return
        self._api_edit.blockSignals(True)
        self._api_edit.setCurrentIndex(index)
        self._api_edit.blockSignals(False)

    def _restore_session_selection(self, session_key: str) -> None:
        self._session_switch_guard = True
        try:
            if not session_key:
                self._session_table.clearSelection()
                return
            for row in range(self._session_table.rowCount()):
                item = self._session_table.item(row, 0)
                key = str(item.data(Qt.ItemDataRole.UserRole) or "").strip() if item is not None else ""
                if key == session_key:
                    self._session_table.selectRow(row)
                    return
            self._session_table.clearSelection()
        finally:
            self._session_switch_guard = False

    @Slot()
    def _on_profile_changed(self) -> None:
        selected = self._selected_profile_name()
        if not selected:
            return
        self._profile_snapshots, _selected_profile = load_profile_snapshots()
        self._unlocked_profiles.intersection_update(set(self._profile_snapshots))
        if selected != self._last_profile_name and not self._ensure_profile_access(selected):
            self._restore_profile_selection()
            return
        self._last_profile_name = selected
        if self._selected_session_key:
            api_name, _symbol, _bar = _split_annotation_key(self._selected_session_key)
            if api_name != selected:
                self._selected_session_key = ""
                self._selected_line_index = -1
                self._selected_rr_index = -1
                self._session_table.clearSelection()
                self._sync_current_session_views()
        self._set_status(f"当前 API Profile: {selected}")

    def _current_session_key(self) -> str:
        api_name = self._selected_profile_name()
        symbol = self._symbol_edit.text().strip().upper()
        bar = self._bar_edit.text().strip()
        if not api_name or not symbol or not bar:
            raise RuntimeError("API、标的、周期都需要填写。")
        return _build_annotation_key(api_name, symbol, bar)

    def _current_entry(self) -> dict[str, object]:
        if not self._selected_session_key:
            raise RuntimeError("请先载入一个会话。")
        entry = self._entries.get(self._selected_session_key)
        if not isinstance(entry, dict):
            entry = {"lines": [], "rr": []}
            self._entries[self._selected_session_key] = entry
        if not isinstance(entry.get("lines"), list):
            entry["lines"] = []
        if not isinstance(entry.get("rr"), list):
            entry["rr"] = []
        return entry

    def _set_status(self, text: str) -> None:
        self._status.setText(text)

    def _show_error(self, title: str, message: str) -> None:
        self._set_status(message)
        QMessageBox.critical(self, title, message)

    def _show_info(self, title: str, message: str) -> None:
        self._set_status(message)
        QMessageBox.information(self, title, message)

    @Slot()
    def refresh_entries(self) -> None:
        self._entries = load_line_trading_desk_annotations_entries()
        session_keys = sorted(self._entries)
        self._session_table.setRowCount(len(session_keys))

        total_lines = 0
        total_rr = 0
        selected_row = -1
        for row, key in enumerate(session_keys):
            entry = self._entries.get(key, {})
            lines = entry.get("lines") if isinstance(entry, dict) else []
            rr_items = entry.get("rr") if isinstance(entry, dict) else []
            line_items = lines if isinstance(lines, list) else []
            rr_list = rr_items if isinstance(rr_items, list) else []
            total_lines += len(line_items)
            total_rr += len(rr_list)
            locked_count = sum(1 for item in line_items if isinstance(item, dict) and bool(item.get("locked", False)))
            locked_count += sum(1 for item in rr_list if isinstance(item, dict) and bool(item.get("locked", False)))
            api_name, symbol, bar = _split_annotation_key(key)
            cells = (api_name, symbol, bar, str(len(line_items)), str(len(rr_list)), str(locked_count))
            for column, value in enumerate(cells):
                item = QTableWidgetItem(value)
                if column == 0:
                    item.setData(Qt.ItemDataRole.UserRole, key)
                self._session_table.setItem(row, column, item)
            if key == self._selected_session_key:
                selected_row = row

        self._session_metric.setText(f"会话：{len(session_keys)}")
        self._line_metric.setText(f"射线：{total_lines}")
        self._rr_metric.setText(f"RR：{total_rr}")
        self._selected_metric.setText(f"当前：{self._selected_session_key or '-'}")

        if selected_row >= 0:
            self._session_table.selectRow(selected_row)
        elif session_keys:
            self._session_table.selectRow(0)
        else:
            self._selected_session_key = ""
            self._detail_text.clear()
            self._line_table.setRowCount(0)
            self._rr_table.setRowCount(0)
        self._sync_current_session_views()

    def _sync_current_session_views(self) -> None:
        if not self._selected_session_key:
            self._detail_text.setPlainText("当前没有选中的会话。")
            self._line_table.setRowCount(0)
            self._rr_table.setRowCount(0)
            self._render_chart([], [], [])
            return
        entry = self._entries.get(self._selected_session_key, {"lines": [], "rr": []})
        self._detail_text.setPlainText(
            json.dumps({"session_key": self._selected_session_key, "entry": entry}, ensure_ascii=False, indent=2)
        )
        line_items = entry.get("lines") if isinstance(entry, dict) else []
        rr_items = entry.get("rr") if isinstance(entry, dict) else []
        self._populate_line_table(line_items)
        self._populate_rr_table(rr_items)
        self._reload_chart()

    @Slot()
    def _on_session_selected(self) -> None:
        if self._session_switch_guard:
            return
        row = self._session_table.currentRow()
        if row < 0:
            return
        item = self._session_table.item(row, 0)
        key = str(item.data(Qt.ItemDataRole.UserRole) or "").strip() if item is not None else ""
        if not key:
            return
        previous_key = self._selected_session_key
        api_name, symbol, bar = _split_annotation_key(key)
        if api_name in self._profile_snapshots:
            if not self._ensure_profile_access(api_name):
                self._restore_session_selection(previous_key)
                return
            index = self._api_edit.findData(api_name)
            if index >= 0:
                self._api_edit.blockSignals(True)
                self._api_edit.setCurrentIndex(index)
                self._api_edit.blockSignals(False)
            self._last_profile_name = api_name
        self._selected_session_key = key
        self._symbol_edit.setText(symbol)
        self._bar_edit.setText(bar)
        self._selected_line_index = -1
        self._selected_rr_index = -1
        self._sync_current_session_views()

    @Slot()
    def _load_or_create_session(self) -> None:
        profile_name = self._selected_profile_name()
        if profile_name and not self._ensure_profile_access(profile_name):
            self._show_error("加载失败", f"API Profile {profile_name} 尚未解锁。")
            self._restore_profile_selection()
            return
        try:
            key = self._current_session_key()
        except Exception as exc:  # noqa: BLE001
            self._show_error("载入失败", str(exc))
            return
        if key not in self._entries:
            self._entries[key] = {"lines": [], "rr": []}
        self._selected_session_key = key
        self._set_status(f"已载入会话：{key}")
        self.refresh_entries()

    @Slot()
    def _save_entries(self) -> None:
        try:
            save_line_trading_desk_annotations_entries(self._entries)
        except Exception as exc:  # noqa: BLE001
            self._show_error("保存失败", str(exc))
            return
        self._set_status("共享注解已保存。")
        self.refresh_entries()

    @Slot()
    def _delete_session(self) -> None:
        if not self._selected_session_key:
            self._show_info("提示", "请先选择一个会话。")
            return
        self._entries.pop(self._selected_session_key, None)
        removed_key = self._selected_session_key
        self._selected_session_key = ""
        self._save_entries()
        self._set_status(f"已删除会话：{removed_key}")

    @Slot()
    def _clear_unlocked_items(self) -> None:
        try:
            entry = self._current_entry()
        except Exception as exc:  # noqa: BLE001
            self._show_error("清理失败", str(exc))
            return
        lines = entry.get("lines", [])
        rr_items = entry.get("rr", [])
        if isinstance(lines, list):
            entry["lines"] = [
                item for item in lines if isinstance(item, dict) and bool(item.get("locked", False))
            ]
        if isinstance(rr_items, list):
            entry["rr"] = [
                item for item in rr_items if isinstance(item, dict) and bool(item.get("locked", False))
            ]
        self._save_entries()
        self._set_status("当前会话里未锁定的射线和 RR 已清空。")

    def _reload_chart(self) -> None:
        if not self._selected_session_key:
            self._render_chart([], [], [])
            return
        api_name, symbol, bar = _split_annotation_key(self._selected_session_key)
        _ = api_name
        try:
            candles = self._client.get_candles_history(symbol, bar, limit=240)
        except Exception as exc:  # noqa: BLE001
            self._set_status(f"K 线加载失败：{exc}")
            self._render_chart([], [], [])
            return
        entry = self._entries.get(self._selected_session_key, {})
        raw_lines = entry.get("lines") if isinstance(entry, dict) else []
        raw_rr = entry.get("rr") if isinstance(entry, dict) else []
        lines = raw_lines if isinstance(raw_lines, list) else []
        rr_items = raw_rr if isinstance(raw_rr, list) else []
        self._render_chart(candles, lines, rr_items)

    def _render_chart(
        self,
        candles: list[object],
        raw_lines: list[object],
        raw_rr: list[object],
    ) -> None:
        self._chart.removeAllSeries()
        for axis in list(self._chart.axes()):
            self._chart.removeAxis(axis)

        if not candles:
            self._chart.setTitle("当前会话暂无 K 线")
            return

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
        first_ts = int(candles[0].ts)
        last_ts = int(candles[-1].ts)
        interval_ms = 60_000
        if len(candles) >= 2:
            interval_ms = max(1, int(candles[1].ts - candles[0].ts))

        for candle in candles:
            ts = int(candle.ts)
            candle_series.append(
                QCandlestickSet(
                    float(candle.open),
                    float(candle.high),
                    float(candle.low),
                    float(candle.close),
                    ts,
                )
            )
            close_series.append(ts, float(candle.close))
            min_price = candle.low if min_price is None else min(min_price, candle.low)
            max_price = candle.high if max_price is None else max(max_price, candle.high)

        self._chart.addSeries(candle_series)
        self._chart.addSeries(close_series)

        def _ts_for_bar(raw_bar: object) -> int:
            try:
                index = max(0, min(int(round(float(raw_bar))), len(candles) - 1))
            except (TypeError, ValueError):
                index = 0
            return int(candles[index].ts)

        for item in raw_lines:
            if not isinstance(item, dict):
                continue
            price_a = item.get("price_a")
            price_b = item.get("price_b", price_a)
            if price_a in {None, ""} or price_b in {None, ""}:
                continue
            try:
                line_price_a = Decimal(str(price_a))
                line_price_b = Decimal(str(price_b))
            except InvalidOperation:
                continue
            min_price = line_price_a if min_price is None else min(min_price, line_price_a, line_price_b)
            max_price = line_price_b if max_price is None else max(max_price, line_price_a, line_price_b)
            series = QLineSeries()
            label = str(item.get("label", "") or item.get("kind", "line"))
            action = str(item.get("desk_ray_action", "notify") or "notify")
            series.setName(f"{label} [{action}]")
            start_ts = _ts_for_bar(item.get("bar_a"))
            end_ts = _ts_for_bar(item.get("bar_b"))
            kind = str(item.get("kind", "line") or "line")
            if kind in {"horizontal", "stop"}:
                end_ts = last_ts + interval_ms * 6
                line_price_b = line_price_a
            series.append(start_ts, float(line_price_a))
            series.append(end_ts, float(line_price_b))
            self._chart.addSeries(series)

        for item in raw_rr:
            if not isinstance(item, dict):
                continue
            try:
                entry_price = Decimal(str(item.get("price_entry")))
                stop_price = Decimal(str(item.get("price_stop")))
                tp_price = Decimal(str(item.get("price_tp")))
            except (InvalidOperation, TypeError):
                continue
            min_price = entry_price if min_price is None else min(min_price, entry_price, stop_price, tp_price)
            max_price = tp_price if max_price is None else max(max_price, entry_price, stop_price, tp_price)
            start_ts = _ts_for_bar(item.get("bar_entry"))
            end_ts = last_ts + interval_ms * 10

            for label, price in (("RR 入场", entry_price), ("RR 止损", stop_price), ("RR 止盈", tp_price)):
                line = QLineSeries()
                line.setName(label)
                line.append(start_ts, float(price))
                line.append(end_ts, float(price))
                self._chart.addSeries(line)

            upper_price = max(entry_price, stop_price)
            lower_price = min(entry_price, stop_price)
            top = QLineSeries()
            bottom = QLineSeries()
            top.append(start_ts, float(upper_price))
            top.append(end_ts, float(upper_price))
            bottom.append(start_ts, float(lower_price))
            bottom.append(end_ts, float(lower_price))
            self._chart.addSeries(top)
            self._chart.addSeries(bottom)
            area = QAreaSeries(top, bottom)
            area.setName("风险区")
            area.setOpacity(0.08)
            self._chart.addSeries(area)

        time_axis = QDateTimeAxis()
        time_axis.setFormat("MM-dd HH:mm")
        time_axis.setTickCount(6)
        time_axis.setRange(
            QDateTime.fromMSecsSinceEpoch(first_ts),
            QDateTime.fromMSecsSinceEpoch(last_ts + interval_ms * 10),
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

    def _populate_line_table(self, raw_lines: object) -> None:
        items = raw_lines if isinstance(raw_lines, list) else []
        self._line_table.setRowCount(len(items))
        selected_row = -1
        for row, payload in enumerate(items):
            line = payload if isinstance(payload, dict) else {}
            cells = (
                _safe_text(line.get("kind")),
                _safe_text(line.get("label")),
                _safe_text(line.get("desk_ray_action")),
                _safe_text(line.get("price_a")),
                _safe_text(line.get("price_b")),
                _safe_text(line.get("bar_a")),
                _safe_text(line.get("bar_b")),
                "是" if bool(line.get("locked", False)) else "否",
            )
            for column, value in enumerate(cells):
                item = QTableWidgetItem(value)
                if column == 0:
                    item.setData(Qt.ItemDataRole.UserRole, row)
                self._line_table.setItem(row, column, item)
            if row == self._selected_line_index:
                selected_row = row
        if selected_row >= 0:
            self._line_table.selectRow(selected_row)

    def _populate_rr_table(self, raw_rr: object) -> None:
        items = raw_rr if isinstance(raw_rr, list) else []
        self._rr_table.setRowCount(len(items))
        selected_row = -1
        for row, payload in enumerate(items):
            rr = payload if isinstance(payload, dict) else {}
            cells = (
                _safe_text(rr.get("side")),
                _safe_text(rr.get("price_entry")),
                _safe_text(rr.get("price_stop")),
                _safe_text(rr.get("price_tp")),
                _safe_text(rr.get("r_multiple")),
                _safe_text(rr.get("bar_entry")),
                "是" if bool(rr.get("locked", False)) else "否",
            )
            for column, value in enumerate(cells):
                item = QTableWidgetItem(value)
                if column == 0:
                    item.setData(Qt.ItemDataRole.UserRole, row)
                self._rr_table.setItem(row, column, item)
            if row == self._selected_rr_index:
                selected_row = row
        if selected_row >= 0:
            self._rr_table.selectRow(selected_row)

    @Slot()
    def _on_line_selected(self) -> None:
        row = self._line_table.currentRow()
        if row < 0:
            self._selected_line_index = -1
            return
        item = self._line_table.item(row, 0)
        index = int(item.data(Qt.ItemDataRole.UserRole)) if item is not None else row
        self._selected_line_index = index
        entry = self._entries.get(self._selected_session_key, {})
        lines = entry.get("lines") if isinstance(entry, dict) else []
        if not isinstance(lines, list) or index < 0 or index >= len(lines):
            return
        payload = lines[index] if isinstance(lines[index], dict) else {}
        _set_combo_data(self._line_kind_combo, str(payload.get("kind", "line")))
        self._line_label_edit.setText(str(payload.get("label", "") or ""))
        _set_combo_data(self._line_action_combo, str(payload.get("desk_ray_action", "notify")))
        self._line_price_a_edit.setText(str(payload.get("price_a", "") or ""))
        self._line_price_b_edit.setText(str(payload.get("price_b", "") or ""))
        self._line_bar_a_edit.setText(str(payload.get("bar_a", "") or ""))
        self._line_bar_b_edit.setText(str(payload.get("bar_b", "") or ""))
        self._line_color_edit.setText(str(payload.get("color", "#1d4ed8") or "#1d4ed8"))
        self._line_locked_check.setChecked(bool(payload.get("locked", False)))
        self._line_triggered_check.setChecked(bool(payload.get("desk_ray_triggered", False)))

    @Slot()
    def _save_line_item(self) -> None:
        try:
            entry = self._current_entry()
            price_a = _parse_decimal(self._line_price_a_edit.text(), "价格 A")
            price_b_raw = self._line_price_b_edit.text().strip()
            price_b = _parse_decimal(price_b_raw, "价格 B") if price_b_raw else price_a
            bar_a = _parse_optional_float(self._line_bar_a_edit.text())
            bar_b = _parse_optional_float(self._line_bar_b_edit.text())
            if bar_a is None:
                raise RuntimeError("Bar A 必须填写。")
            if bar_b is None:
                bar_b = bar_a
            payload = {
                "kind": _combo_data(self._line_kind_combo),
                "x1": 0.0,
                "y1": 0.0,
                "x2": 0.0,
                "y2": 0.0,
                "color": self._line_color_edit.text().strip() or "#1d4ed8",
                "label": self._line_label_edit.text().strip(),
                "desk_ray_action": _combo_data(self._line_action_combo),
                "desk_ray_triggered": self._line_triggered_check.isChecked(),
                "desk_ray_submit_pending": False,
                "desk_ray_last_side": None,
                "locked": self._line_locked_check.isChecked(),
                "bar_a": bar_a,
                "bar_b": bar_b,
                "price_a": _decimal_text(price_a),
                "price_b": _decimal_text(price_b),
            }
            lines = entry.get("lines")
            if not isinstance(lines, list):
                lines = []
                entry["lines"] = lines
            if 0 <= self._selected_line_index < len(lines):
                lines[self._selected_line_index] = payload
            else:
                lines.append(payload)
                self._selected_line_index = len(lines) - 1
        except Exception as exc:  # noqa: BLE001
            self._show_error("保存射线失败", str(exc))
            return
        self._save_entries()
        self._set_status("射线已保存。")

    @Slot()
    def _remove_line_item(self) -> None:
        try:
            entry = self._current_entry()
            lines = entry.get("lines")
            if not isinstance(lines, list) or not (0 <= self._selected_line_index < len(lines)):
                raise RuntimeError("请先选择一条射线。")
            del lines[self._selected_line_index]
            self._selected_line_index = -1
        except Exception as exc:  # noqa: BLE001
            self._show_error("删除射线失败", str(exc))
            return
        self._save_entries()
        self._set_status("射线已删除。")

    @Slot()
    def _on_rr_selected(self) -> None:
        row = self._rr_table.currentRow()
        if row < 0:
            self._selected_rr_index = -1
            return
        item = self._rr_table.item(row, 0)
        index = int(item.data(Qt.ItemDataRole.UserRole)) if item is not None else row
        self._selected_rr_index = index
        entry = self._entries.get(self._selected_session_key, {})
        rr_items = entry.get("rr") if isinstance(entry, dict) else []
        if not isinstance(rr_items, list) or index < 0 or index >= len(rr_items):
            return
        payload = rr_items[index] if isinstance(rr_items[index], dict) else {}
        _set_combo_data(self._rr_side_combo, str(payload.get("side", "long")))
        self._rr_entry_edit.setText(str(payload.get("price_entry", "") or ""))
        self._rr_stop_edit.setText(str(payload.get("price_stop", "") or ""))
        self._rr_r_edit.setText(str(payload.get("r_multiple", "2") or "2"))
        self._rr_bar_edit.setText(str(payload.get("bar_entry", "0") or "0"))
        self._rr_locked_check.setChecked(bool(payload.get("locked", False)))
        self._rr_preview.setText(f"当前止盈：{_safe_text(payload.get('price_tp'))}")

    @Slot()
    def _save_rr_item(self) -> None:
        try:
            entry = self._current_entry()
            side = _combo_data(self._rr_side_combo)
            price_entry = _parse_decimal(self._rr_entry_edit.text(), "入场价")
            price_stop = _parse_decimal(self._rr_stop_edit.text(), "止损价")
            r_multiple = _parse_decimal(self._rr_r_edit.text(), "R 倍数")
            bar_entry = _parse_optional_float(self._rr_bar_edit.text())
            if bar_entry is None:
                raise RuntimeError("Bar 必须填写。")
            price_tp = _compute_rr_target(side, price_entry, price_stop, r_multiple)
            payload = {
                "rr_id": self._existing_rr_id(),
                "side": side,
                "bar_entry": bar_entry,
                "bar_stop": bar_entry,
                "price_entry": _decimal_text(price_entry),
                "price_stop": _decimal_text(price_stop),
                "price_tp": _decimal_text(price_tp),
                "r_multiple": _decimal_text(r_multiple),
                "locked": self._rr_locked_check.isChecked(),
            }
            rr_items = entry.get("rr")
            if not isinstance(rr_items, list):
                rr_items = []
                entry["rr"] = rr_items
            if 0 <= self._selected_rr_index < len(rr_items):
                rr_items[self._selected_rr_index] = payload
            else:
                rr_items.append(payload)
                self._selected_rr_index = len(rr_items) - 1
            self._rr_preview.setText(f"自动止盈：{_decimal_text(price_tp)}")
        except Exception as exc:  # noqa: BLE001
            self._show_error("保存 RR 失败", str(exc))
            return
        self._save_entries()
        self._set_status("RR 区块已保存。")

    def _existing_rr_id(self) -> str:
        entry = self._entries.get(self._selected_session_key, {})
        rr_items = entry.get("rr") if isinstance(entry, dict) else []
        if isinstance(rr_items, list) and 0 <= self._selected_rr_index < len(rr_items):
            payload = rr_items[self._selected_rr_index]
            if isinstance(payload, dict):
                rr_id = str(payload.get("rr_id", "") or "").strip()
                if rr_id:
                    return rr_id
        return f"rr-{len(rr_items) + 1}" if isinstance(rr_items, list) else "rr-1"

    @Slot()
    def _remove_rr_item(self) -> None:
        try:
            entry = self._current_entry()
            rr_items = entry.get("rr")
            if not isinstance(rr_items, list) or not (0 <= self._selected_rr_index < len(rr_items)):
                raise RuntimeError("请先选择一个 RR 区块。")
            del rr_items[self._selected_rr_index]
            self._selected_rr_index = -1
        except Exception as exc:  # noqa: BLE001
            self._show_error("删除 RR 失败", str(exc))
            return
        self._save_entries()
        self._set_status("RR 区块已删除。")


def _combo_data(combo: QComboBox) -> str:
    return str(combo.currentData() or "").strip()


def _set_combo_data(combo: QComboBox, target: str) -> None:
    for index in range(combo.count()):
        if str(combo.itemData(index) or "").strip() == target:
            combo.setCurrentIndex(index)
            return
