from __future__ import annotations

import json
import threading
from dataclasses import replace
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation

from PySide6.QtCore import Qt, Signal, Slot
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFormLayout,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from okx_quant.engine import _dynamic_two_taker_fee_offset_live, determine_order_size, resolve_open_pos_side
from okx_quant.log_utils import append_line_desk_log_line, current_log_timestamp, ensure_log_timestamp
from okx_quant.models import OrderPlan, StrategyConfig
from okx_quant.okx_client import OkxApiError, OkxPosition, OkxTradeOrderItem, OkxRestClient
from okx_quant.persistence import (
    load_notification_snapshot,
    load_line_trading_desk_annotations_entries,
    save_line_trading_desk_annotations_entries,
)
from okx_quant.pricing import format_decimal, snap_to_increment
from roll_terminal_qt.line_trading_account import (
    build_rr_order_intent,
    build_runtime_from_profile_payload,
    position_row_cells,
)
from roll_terminal_qt.line_trading_chart import LineTradingChartView
from roll_terminal_qt.line_trading_core import (
    line_annotation_from_payload,
    line_annotation_to_payload,
    rr_annotation_to_payload,
    rr_annotation_from_payload,
)
from roll_terminal_qt.profile_access import ensure_profile_unlocked, load_profile_snapshots
from roll_terminal_qt.workspace_shell import preferred_profile_name


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


LINE_TRADING_DESK_TOOL_ACTIONS: tuple[tuple[str, str], ...] = (
    ("刷新", "refresh"),
    ("重置视图", "reset"),
    ("区间放大", "zoom_range"),
    ("趋势线", "line"),
    ("水平射线", "horizontal"),
    ("止损线", "stop"),
    ("盈亏比·多", "rr_long"),
    ("盈亏比·空", "rr_short"),
    ("清空线", "clear"),
    ("开多", "open_long"),
    ("开空", "open_short"),
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


def _parse_optional_positive_decimal(raw: str, field_name: str) -> Decimal | None:
    text = str(raw or "").strip()
    if not text:
        return None
    value = _parse_decimal(text, field_name)
    if value <= 0:
        raise RuntimeError(f"{field_name}必须大于0。")
    return value


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
    _ui_callback = Signal(object)

    def __init__(self, parent=None) -> None:  # noqa: ANN001
        super().__init__(parent)
        self.setWindowTitle("划线交易台 - Qt")
        self.resize(1540, 920)
        self._window_closing = False
        self._client = _shared_client()
        self._entries: dict[str, dict[str, object]] = {}
        self._selected_session_key = ""
        self._selected_line_index = -1
        self._selected_rr_index = -1
        self._profile_snapshots: dict[str, dict[str, str]] = {}
        self._unlocked_profiles: set[str] = set()
        self._last_profile_name = ""
        self._session_switch_guard = False
        self._selection_sync_guard = False
        self._visible_positions: list[OkxPosition] = []
        self._visible_pending_orders: list[OkxTradeOrderItem] = []
        self._visible_order_history: list[OkxTradeOrderItem] = []

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
        content.addWidget(self._build_account_panel())
        content.setSizes([620, 430, 380, 440])
        layout.addWidget(content, 1)

        self._ui_callback.connect(self._run_ui_callback)
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

        layout.addWidget(QLabel("API"), 0, 0)
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
        toolbar = QHBoxLayout()
        toolbar.setSpacing(6)
        for label, action in LINE_TRADING_DESK_TOOL_ACTIONS:
            button = QPushButton(label)
            button.setMinimumHeight(24)
            button.clicked.connect(lambda _checked=False, selected_action=action: self._on_chart_tool_action(selected_action))
            toolbar.addWidget(button)
        toolbar.addStretch(1)
        layout.addLayout(toolbar)

        self._chart_view = LineTradingChartView()
        self._chart_view.lineCreated.connect(self._on_chart_line_created)
        self._chart_view.rrCreated.connect(self._on_chart_rr_created)
        self._chart_view.lineSelected.connect(self._on_chart_line_selected)
        self._chart_view.rrSelected.connect(self._on_chart_rr_selected)
        self._chart_view.lineUpdated.connect(self._on_chart_line_updated)
        self._chart_view.rrUpdated.connect(self._on_chart_rr_updated)
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
        submit_limit_button = QPushButton("限价委托")
        submit_limit_button.clicked.connect(lambda: self._submit_rr_order_from_selected("limit"))
        submit_trigger_button = QPushButton("触发价委托")
        submit_trigger_button.clicked.connect(lambda: self._submit_rr_order_from_selected("trigger"))
        self._rr_fee_offset_check = QCheckBox("启用手续费偏移（按2倍Taker手续费留缓冲）")
        self._rr_fee_offset_check.setChecked(True)
        top.addWidget(add_button)
        top.addWidget(remove_button)
        top.addWidget(submit_limit_button)
        top.addWidget(submit_trigger_button)
        top.addWidget(self._rr_fee_offset_check)
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

    def _build_account_panel(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Panel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)

        layout.addWidget(QLabel("执行区"))

        action_row = QHBoxLayout()
        action_row.setSpacing(6)
        flatten_selected_button = QPushButton("市价平仓选中")
        flatten_selected_button.clicked.connect(lambda: self._flatten_selected_position("market"))
        flatten_best_quote_button = QPushButton("挂买一/卖一平仓")
        flatten_best_quote_button.clicked.connect(lambda: self._flatten_selected_position("best_quote"))
        cancel_order_button = QPushButton("撤销委托")
        cancel_order_button.clicked.connect(self._cancel_selected_pending_order)
        for button in (flatten_selected_button, flatten_best_quote_button, cancel_order_button):
            button.setMinimumHeight(24)
            action_row.addWidget(button)
        action_row.addWidget(QLabel("平仓币数"))
        self._position_close_qty_edit = QLineEdit("")
        self._position_close_qty_edit.setPlaceholderText("留空=平全部")
        self._position_close_qty_edit.setMaximumWidth(110)
        action_row.addWidget(self._position_close_qty_edit)
        self._position_close_qty_unit_label = QLabel("张")
        action_row.addWidget(self._position_close_qty_unit_label)
        action_row.addStretch(1)
        layout.addLayout(action_row)

        ray_header = QHBoxLayout()
        ray_header.addWidget(QLabel("射线触发"))
        ray_header.addStretch(1)
        ray_lock_button = QPushButton("切换选中锁定")
        ray_lock_button.clicked.connect(self._toggle_selected_ray_lock)
        ray_delete_button = QPushButton("删除选中")
        ray_delete_button.clicked.connect(self._delete_selected_ray)
        ray_header.addWidget(ray_lock_button)
        ray_header.addWidget(ray_delete_button)
        layout.addLayout(ray_header)
        self._ray_trigger_table = QTableWidget(0, 7)
        self._ray_trigger_table.setHorizontalHeaderLabels(
            ["标签", "动作", "价格A", "价格B", "已触发", "提交中", "锁定"]
        )
        self._ray_trigger_table.verticalHeader().setVisible(False)
        self._ray_trigger_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._ray_trigger_table.itemSelectionChanged.connect(self._on_ray_trigger_selected)
        self._ray_trigger_table.cellDoubleClicked.connect(self._on_ray_trigger_cell_double_clicked)
        layout.addWidget(self._ray_trigger_table, 1)

        rr_header = QHBoxLayout()
        rr_header.addWidget(QLabel("RR 动作"))
        rr_header.addStretch(1)
        rr_lock_button = QPushButton("切换选中锁定")
        rr_lock_button.clicked.connect(self._toggle_selected_rr_lock)
        rr_delete_button = QPushButton("删除选中")
        rr_delete_button.clicked.connect(self._delete_selected_rr)
        rr_header.addWidget(rr_lock_button)
        rr_header.addWidget(rr_delete_button)
        layout.addLayout(rr_header)
        self._rr_action_table = QTableWidget(0, 7)
        self._rr_action_table.setHorizontalHeaderLabels(["ID", "方向", "入场", "止损", "止盈", "R", "锁定"])
        self._rr_action_table.verticalHeader().setVisible(False)
        self._rr_action_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._rr_action_table.itemSelectionChanged.connect(self._on_rr_action_selected)
        self._rr_action_table.cellDoubleClicked.connect(self._on_rr_action_cell_double_clicked)
        layout.addWidget(self._rr_action_table, 1)

        self._account_tabs = QTabWidget()
        self._positions_table = QTableWidget(0, 6)
        self._positions_table.setHorizontalHeaderLabels(["合约", "方向", "持仓", "均价", "标记价", "浮盈亏"])
        self._positions_table.itemSelectionChanged.connect(self._on_position_row_selected)
        self._current_orders_table = QTableWidget(0, 6)
        self._current_orders_table.setHorizontalHeaderLabels(["合约", "方向", "价格", "数量", "状态", "订单ID"])
        self._order_history_table = QTableWidget(0, 6)
        self._order_history_table.setHorizontalHeaderLabels(["时间", "合约", "方向", "价格", "数量", "状态"])
        for table in (self._positions_table, self._current_orders_table, self._order_history_table):
            table.verticalHeader().setVisible(False)
            table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._account_tabs.addTab(self._positions_table, "当前持仓")
        self._account_tabs.addTab(self._current_orders_table, "当前委托")
        self._account_tabs.addTab(self._order_history_table, "历史委托")
        layout.addWidget(self._account_tabs, 1)

        layout.addWidget(QLabel("工作台日志"))
        self._workbench_log = QTextEdit()
        self._workbench_log.setReadOnly(True)
        self._workbench_log.setMinimumHeight(110)
        self._workbench_log.setPlainText("Line trading desk account panel ready. Live trading actions are disabled.")
        layout.addWidget(self._workbench_log)
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
        target = preferred_profile_name(
            list(self._profile_snapshots),
            current=current,
            last=self._last_profile_name,
            selected=selected_profile,
        )
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
        self._set_status(f"当前 API：{selected}")

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

        self._session_switch_guard = True
        try:
            if selected_row >= 0:
                self._session_table.selectRow(selected_row)
            elif session_keys:
                self._selected_session_key = session_keys[0]
                self._session_table.selectRow(0)
            else:
                self._selected_session_key = ""
                self._detail_text.clear()
                self._line_table.setRowCount(0)
                self._rr_table.setRowCount(0)
                self._ray_trigger_table.setRowCount(0)
                self._rr_action_table.setRowCount(0)
                self._positions_table.setRowCount(0)
                self._current_orders_table.setRowCount(0)
                self._order_history_table.setRowCount(0)
        finally:
            self._session_switch_guard = False
        self._sync_current_session_views()

    def _sync_current_session_views(self) -> None:
        if not self._selected_session_key:
            self._detail_text.setPlainText("当前没有选中的会话。")
            self._line_table.setRowCount(0)
            self._rr_table.setRowCount(0)
            self._ray_trigger_table.setRowCount(0)
            self._rr_action_table.setRowCount(0)
            self._positions_table.setRowCount(0)
            self._current_orders_table.setRowCount(0)
            self._order_history_table.setRowCount(0)
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
        self._populate_ray_trigger_table(line_items)
        self._populate_rr_action_table(rr_items)
        self._populate_account_data()
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
            self._show_error("加载失败", f"API {profile_name} 尚未解锁。")
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

    @Slot(str)
    def _on_chart_tool_action(self, action: str) -> None:
        if action == "refresh":
            self._reload_chart()
            return
        if action == "clear":
            self._clear_unlocked_items()
            return
        if action in {"zoom_range", "line", "horizontal", "stop", "rr_long", "rr_short"}:
            self._chart_view.set_tool(action)
            self._set_status(f"图表工具已切换：{action}")
            return
        if action == "reset":
            self._chart_view.set_tool("none")
            self._set_status("图表工具已重置。")
            return
        if action in {"open_long", "open_short"}:
            self._chart_view.set_tool("none")
            self._submit_rr_order_from_selected("limit", side_override="long" if action == "open_long" else "short")
            return
        self._set_status(f"暂不支持的图表动作：{action}")

    @Slot(object)
    def _on_chart_line_created(self, annotation: object) -> None:
        if not self._selected_session_key:
            self._set_status("请先选择会话后再画线。")
            return
        try:
            entry = self._current_entry()
            payload = line_annotation_to_payload(annotation)
        except Exception as exc:  # noqa: BLE001
            self._set_status(f"已忽略图表射线：{exc}")
            return
        lines = entry.get("lines")
        if not isinstance(lines, list):
            lines = []
            entry["lines"] = lines
        lines.append(payload)
        self._selected_line_index = len(lines) - 1
        self._save_entries()
        self._set_status("图表射线已保存。")

    @Slot(object)
    def _on_chart_rr_created(self, annotation: object) -> None:
        if not self._selected_session_key:
            self._set_status("请先选择会话后再绘制 RR。")
            return
        try:
            entry = self._current_entry()
            payload = rr_annotation_to_payload(annotation)
            payload["rr_id"] = self._existing_rr_id()
        except Exception as exc:  # noqa: BLE001
            self._set_status(f"已忽略图表 RR：{exc}")
            return
        rr_items = entry.get("rr")
        if not isinstance(rr_items, list):
            rr_items = []
            entry["rr"] = rr_items
        rr_items.append(payload)
        self._selected_rr_index = len(rr_items) - 1
        self._save_entries()
        self._set_status("图表 RR 已保存。")

    @Slot(int)
    def _on_chart_line_selected(self, index: int) -> None:
        if index < 0:
            return
        self._selected_rr_index = -1
        self._sync_rr_selection_views()
        self._selected_line_index = index
        self._sync_line_selection_views()
        self._on_line_selected()

    @Slot(int)
    def _on_chart_rr_selected(self, index: int) -> None:
        if index < 0:
            return
        self._selected_line_index = -1
        self._sync_line_selection_views()
        self._selected_rr_index = index
        self._sync_rr_selection_views()
        self._on_rr_selected()

    @Slot(int, object)
    def _on_chart_rr_updated(self, index: int, annotation: object) -> None:
        try:
            entry = self._current_entry()
            rr_items = entry.get("rr")
            if not isinstance(rr_items, list) or not (0 <= index < len(rr_items)):
                return
            payload = rr_annotation_to_payload(annotation)
            if isinstance(rr_items[index], dict):
                existing_rr_id = str(rr_items[index].get("rr_id", "") or "").strip()
                if existing_rr_id:
                    payload["rr_id"] = existing_rr_id
            rr_items[index] = payload
            self._selected_rr_index = index
            self._selected_line_index = -1
        except Exception as exc:  # noqa: BLE001
            self._set_status(f"已忽略图表 RR 更新：{exc}")
            return
        self._sync_line_selection_views()
        self._sync_rr_selection_views()
        self._on_rr_selected()
        self._save_entries()
        self._set_status(f"图表 RR 第 {index + 1} 条已更新。")

    @Slot(int, object)
    def _on_chart_line_updated(self, index: int, annotation: object) -> None:
        try:
            entry = self._current_entry()
            lines = entry.get("lines")
            if not isinstance(lines, list) or not (0 <= index < len(lines)):
                return
            payload = line_annotation_to_payload(annotation)
            if isinstance(lines[index], dict):
                existing = lines[index]
                payload["label"] = str(existing.get("label", payload.get("label", "")) or payload.get("label", ""))
                payload["desk_ray_action"] = str(existing.get("desk_ray_action", payload.get("desk_ray_action", "notify")) or "notify")
                payload["desk_ray_triggered"] = bool(existing.get("desk_ray_triggered", payload.get("desk_ray_triggered", False)))
                payload["locked"] = bool(existing.get("locked", payload.get("locked", False)))
                payload["desk_ray_last_side"] = existing.get("desk_ray_last_side", payload.get("desk_ray_last_side"))
            lines[index] = payload
            self._selected_line_index = index
            self._selected_rr_index = -1
        except Exception as exc:  # noqa: BLE001
            self._set_status(f"已忽略图表射线更新：{exc}")
            return
        self._sync_rr_selection_views()
        self._sync_line_selection_views()
        self._on_line_selected()
        self._save_entries()
        self._set_status(f"图表射线第 {index + 1} 条已更新。")

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
        lines = [
            line_annotation_from_payload(item)
            for item in raw_lines
            if isinstance(item, Mapping)
        ]
        rr_items = [
            rr_annotation_from_payload(item)
            for item in raw_rr
            if isinstance(item, Mapping)
        ]
        self._chart_view.set_candles(candles)
        self._chart_view.set_annotations(lines=lines, rr_items=rr_items)
        self._sync_chart_selection()

    def _sync_chart_selection(self) -> None:
        self._chart_view.set_selected_indexes(
            line_index=self._selected_line_index,
            rr_index=self._selected_rr_index,
        )

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
        elif self._line_table.rowCount() > 0:
            self._line_table.clearSelection()

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
        elif self._rr_table.rowCount() > 0:
            self._rr_table.clearSelection()

    def _populate_ray_trigger_table(self, raw_lines: object) -> None:
        items = raw_lines if isinstance(raw_lines, list) else []
        self._ray_trigger_table.setRowCount(len(items))
        selected_row = -1
        for row, payload in enumerate(items):
            line = payload if isinstance(payload, dict) else {}
            cells = (
                _safe_text(line.get("label")),
                _safe_text(line.get("desk_ray_action")),
                _safe_text(line.get("price_a")),
                _safe_text(line.get("price_b")),
                "yes" if bool(line.get("desk_ray_triggered", False)) else "no",
                "yes" if bool(line.get("desk_ray_submit_pending", False)) else "no",
                "yes" if bool(line.get("locked", False)) else "no",
            )
            self._set_table_row(self._ray_trigger_table, row, cells)
            if row == self._selected_line_index:
                selected_row = row
        if selected_row >= 0:
            self._ray_trigger_table.selectRow(selected_row)
        elif self._ray_trigger_table.rowCount() > 0:
            self._ray_trigger_table.clearSelection()

    def _populate_rr_action_table(self, raw_rr: object) -> None:
        items = raw_rr if isinstance(raw_rr, list) else []
        self._rr_action_table.setRowCount(len(items))
        selected_row = -1
        for row, payload in enumerate(items):
            rr = payload if isinstance(payload, dict) else {}
            cells = (
                _safe_text(rr.get("rr_id")),
                _safe_text(rr.get("side")),
                _safe_text(rr.get("price_entry")),
                _safe_text(rr.get("price_stop")),
                _safe_text(rr.get("price_tp")),
                _safe_text(rr.get("r_multiple")),
                "yes" if bool(rr.get("locked", False)) else "no",
            )
            self._set_table_row(self._rr_action_table, row, cells)
            if row == self._selected_rr_index:
                selected_row = row
        if selected_row >= 0:
            self._rr_action_table.selectRow(selected_row)
        elif self._rr_action_table.rowCount() > 0:
            self._rr_action_table.clearSelection()

    def _selected_profile_payload(self) -> dict[str, str]:
        profile_name = self._selected_profile_name()
        if profile_name not in self._profile_snapshots:
            self._profile_snapshots, _selected_profile = load_profile_snapshots()
            self._unlocked_profiles.intersection_update(set(self._profile_snapshots))
        payload = self._profile_snapshots.get(profile_name)
        if not isinstance(payload, dict):
            raise RuntimeError(f"未找到 API Profile {profile_name or '-'}。")
        return payload

    def _build_runtime(self):
        profile_name = self._selected_profile_name()
        if not profile_name:
            raise RuntimeError("请先选择 API Profile。")
        if not self._ensure_profile_access(profile_name):
            raise RuntimeError(f"API Profile {profile_name} 尚未解锁。")
        return build_runtime_from_profile_payload(
            profile_name=profile_name,
            payload=self._selected_profile_payload(),
            notification_snapshot=load_notification_snapshot(),
        )

    def _session_symbol(self) -> str:
        if not self._selected_session_key:
            return self._symbol_edit.text().strip().upper()
        _api_name, symbol, _bar = _split_annotation_key(self._selected_session_key)
        return symbol.strip().upper()

    def _load_account_snapshot(self, runtime, symbol: str) -> tuple[list[OkxPosition], list[OkxTradeOrderItem], list[OkxTradeOrderItem]]:
        positions = [
            item
            for item in self._client.get_positions(runtime.credentials, environment=runtime.environment)
            if str(getattr(item, "inst_id", "") or "").strip().upper() == symbol
        ]
        pending_orders = [
            item
            for item in self._client.get_pending_orders(runtime.credentials, environment=runtime.environment, limit=100)
            if str(getattr(item, "inst_id", "") or "").strip().upper() == symbol
        ]
        order_history = [
            item
            for item in self._client.get_order_history(runtime.credentials, environment=runtime.environment, limit=100)
            if str(getattr(item, "inst_id", "") or "").strip().upper() == symbol
        ]
        return list(positions), list(pending_orders), list(order_history)

    def _apply_account_snapshot(
        self,
        positions: list[OkxPosition],
        pending_orders: list[OkxTradeOrderItem],
        order_history: list[OkxTradeOrderItem],
    ) -> None:
        self._visible_positions = positions
        self._visible_pending_orders = pending_orders
        self._visible_order_history = order_history

        self._positions_table.setRowCount(len(self._visible_positions))
        for row, position in enumerate(self._visible_positions):
            self._set_table_row(self._positions_table, row, tuple(position_row_cells(position)))

        self._current_orders_table.setRowCount(len(self._visible_pending_orders))
        for row, order in enumerate(self._visible_pending_orders):
            self._set_table_row(
                self._current_orders_table,
                row,
                (
                    _safe_text(getattr(order, "inst_id", None)),
                    _safe_text(getattr(order, "side", None)),
                    _safe_text(getattr(order, "price", None)),
                    _safe_text(getattr(order, "size", None)),
                    _safe_text(getattr(order, "state", None)),
                    _safe_text(getattr(order, "order_id", None) or getattr(order, "algo_id", None)),
                ),
            )

        self._order_history_table.setRowCount(len(self._visible_order_history))
        for row, order in enumerate(self._visible_order_history):
            self._set_table_row(
                self._order_history_table,
                row,
                (
                    _safe_text(getattr(order, "update_time", None) or getattr(order, "created_time", None)),
                    _safe_text(getattr(order, "inst_id", None)),
                    _safe_text(getattr(order, "side", None)),
                    _safe_text(getattr(order, "price", None)),
                    _safe_text(getattr(order, "size", None)),
                    _safe_text(getattr(order, "state", None)),
                ),
            )
        self._refresh_position_close_qty_unit()

    def _handle_account_load_error(self, exc: Exception) -> None:
        self._visible_positions = []
        self._visible_pending_orders = []
        self._visible_order_history = []
        self._populate_account_placeholders()
        self._refresh_position_close_qty_unit()
        self._append_workbench_log(f"账户加载失败：{exc}")

    @Slot()
    def _on_position_row_selected(self) -> None:
        self._refresh_position_close_qty_unit()

    @staticmethod
    def _position_currency_from_inst_id(inst_id: str) -> str:
        parts = str(inst_id or "").strip().upper().split("-")
        return parts[0] if parts and parts[0] else "张"

    @staticmethod
    def _infer_inst_type(position: OkxPosition) -> str:
        explicit_type = str(getattr(position, "inst_type", "") or "").strip().upper()
        if explicit_type:
            return explicit_type
        parts = str(getattr(position, "inst_id", "") or "").strip().upper().split("-")
        if len(parts) >= 3:
            return parts[-1]
        return ""

    @staticmethod
    def _quote_currency_from_inst_id(inst_id: str) -> str:
        parts = str(inst_id or "").strip().upper().split("-")
        if len(parts) >= 2:
            return parts[1].strip()
        return ""

    @staticmethod
    def _fallback_position_contract_value(inst_id: str, inst_type: str) -> tuple[Decimal | None, str | None]:
        normalized_type = str(inst_type or "").strip().upper()
        asset = LineTradingQtWindow._position_currency_from_inst_id(inst_id)
        quote = LineTradingQtWindow._quote_currency_from_inst_id(inst_id)
        if not asset:
            return None, None

        if normalized_type == "OPTION" and quote == "USD":
            option_contract_values = {
                "BTC": Decimal("0.01"),
                "ETH": Decimal("0.1"),
            }
            contract_value = option_contract_values.get(asset)
            if contract_value is not None:
                return contract_value, asset

        if normalized_type in {"SWAP", "FUTURES"} and quote == "USD":
            inverse_contract_values = {
                "BTC": Decimal("100"),
            }
            contract_value = inverse_contract_values.get(asset)
            if contract_value is not None:
                return contract_value, "USD"

        if normalized_type in {"SWAP", "FUTURES"} and quote in {"USDT", "USDC"}:
            linear_contract_values = {
                "BTC": Decimal("0.01"),
                "ETH": Decimal("0.1"),
                "BNB": Decimal("0.01"),
                "OKB": Decimal("0.01"),
                "SOL": Decimal("1"),
                "DOGE": Decimal("1000"),
                "XRP": Decimal("100"),
            }
            contract_value = linear_contract_values.get(asset)
            if contract_value is not None:
                return contract_value, asset

        return None, None

    @staticmethod
    def _position_contract_value_snapshot(position: OkxPosition, instrument: object | None) -> tuple[Decimal | None, str | None]:
        inst_type = LineTradingQtWindow._infer_inst_type(position)
        if inst_type == "SPOT":
            return None, None
        if instrument is not None:
            ct_val = getattr(instrument, "ct_val", None)
            ct_val_ccy = str(getattr(instrument, "ct_val_ccy", "") or "").strip().upper()
            if ct_val is not None and ct_val_ccy:
                try:
                    value = Decimal(str(ct_val))
                except Exception:
                    value = None
                if value is not None and value > 0:
                    return value, ct_val_ccy
        return LineTradingQtWindow._fallback_position_contract_value(
            str(getattr(position, "inst_id", "")),
            inst_type,
        )

    @staticmethod
    def _as_decimal(value: object, field_name: str) -> Decimal:
        try:
            return Decimal(str(value))
        except (TypeError, InvalidOperation) as exc:
            raise RuntimeError(f"{field_name}数据异常。") from exc

    @staticmethod
    def _position_contract_multiplier(instrument: object | None) -> Decimal:
        if instrument is None:
            return Decimal("1")
        ct_mult = getattr(instrument, "ct_mult", None)
        if ct_mult is not None:
            try:
                value = Decimal(str(ct_mult))
            except Exception:
                value = None
            if value is not None and value > 0:
                return value
        return Decimal("1")

    @staticmethod
    def _snapshot_position_closeable_contracts(position: OkxPosition) -> Decimal:
        raw_size = getattr(position, "avail_position", None)
        if raw_size is None:
            raw_size = getattr(position, "position", Decimal("0"))
        size = LineTradingQtWindow._as_decimal(raw_size, "可平仓数量")
        if size <= 0:
            return Decimal("0")
        return abs(size)

    def _close_base_per_contract(
        self,
        position: OkxPosition,
        instrument: object,
    ) -> Decimal:
        contract_value, contract_currency = self._position_contract_value_snapshot(position, instrument)
        if contract_value is None or contract_currency is None:
            if LineTradingQtWindow._infer_inst_type(position) == "SPOT":
                return Decimal("1")
            raise RuntimeError(f"{position.inst_id} 缺少平仓合约价值信息，无法按币数换算。")
        base_per_contract = contract_value * self._position_contract_multiplier(instrument)
        if contract_currency in {"USD", "USDT", "USDC"}:
            reference_price = (
                getattr(position, "mark_price", None)
                or getattr(position, "last_price", None)
                or getattr(position, "avg_price", None)
            )
            if reference_price is None or reference_price <= 0:
                raise RuntimeError(f"{position.inst_id} 缺少可用标记价格，无法按币数换算。")
            return base_per_contract / reference_price
        return base_per_contract

    def _convert_close_coin_to_order_size(
        self,
        requested_coin: Decimal,
        *,
        position: OkxPosition,
        instrument: object,
    ) -> Decimal:
        if LineTradingQtWindow._infer_inst_type(position) == "SPOT":
            return requested_coin
        base_per_contract = self._close_base_per_contract(position, instrument)
        if base_per_contract <= 0:
            raise RuntimeError(f"{position.inst_id} 合约换算参数无效。")
        return requested_coin / base_per_contract

    def _selected_position_close_amount(self, position: OkxPosition) -> Decimal:
        instrument = self._get_instrument(position.inst_id)
        size = snap_to_increment(
            self._snapshot_position_closeable_contracts(position),
            instrument.lot_size,
            "down",
        )
        if size < instrument.min_size:
            raise RuntimeError("当前可平仓数量低于最小下单数量。")

        requested_coin = _parse_optional_positive_decimal(
            self._position_close_qty_edit.text(),
            "平仓数量",
        )
        if requested_coin is None:
            return size

        requested_contracts = self._convert_close_coin_to_order_size(
            requested_coin,
            position=position,
            instrument=instrument,
        )
        requested_contracts = snap_to_increment(requested_contracts, instrument.lot_size, "down")
        if requested_contracts <= 0:
            raise RuntimeError("平仓数量换算后不能小于等于0。")
        if requested_contracts > size:
            raise RuntimeError("平仓数量不能超过当前可平仓。")
        if requested_contracts < instrument.min_size:
            raise RuntimeError("平仓数量低于最小下单数量。")
        return requested_contracts

    def _selected_position_close_amount_v2(self, position: OkxPosition) -> Decimal:
        instrument = self._get_instrument(position.inst_id)
        if instrument is None:
            raise RuntimeError(f"{position.inst_id} 缺少合约参数，无法计算平仓数量。")

        lot_size = self._as_decimal(getattr(instrument, "lot_size", None), "合约lot_size")
        min_size = self._as_decimal(getattr(instrument, "min_size", None), "合约最小下单量")
        closeable = self._snapshot_position_closeable_contracts(position)
        size = snap_to_increment(closeable, lot_size, "down")
        if size < min_size:
            raise RuntimeError("当前可平仓数量低于最小下单量。")

        requested_coin = _parse_optional_positive_decimal(
            self._position_close_qty_edit.text(),
            "平仓币数",
        )
        if requested_coin is None:
            return size

        requested_contracts = self._convert_close_coin_to_order_size(
            requested_coin,
            position=position,
            instrument=instrument,
        )
        requested_contracts = snap_to_increment(requested_contracts, lot_size, "down")
        if requested_contracts <= 0:
            raise RuntimeError("平仓币数换算后不能小于等于 0。")
        if requested_contracts > size:
            raise RuntimeError("平仓币数不能超过当前可平仓。")
        if requested_contracts < min_size:
            raise RuntimeError("平仓币数小于最小下单量。")
        return requested_contracts

    def _selected_position_close_amount(self, position: OkxPosition) -> Decimal:
        return self._selected_position_close_amount_v2(position)

    def _refresh_position_close_qty_unit(self) -> None:
        position = self._selected_position()
        if position is None:
            if self._visible_positions:
                position = self._visible_positions[0]
            else:
                self._position_close_qty_unit_label.setText("张")
                return
        self._position_close_qty_unit_label.setText(self._position_currency_from_inst_id(position.inst_id))

    def _populate_account_data(self, *, async_load: bool = True) -> None:
        try:
            runtime = self._build_runtime()
            symbol = self._session_symbol()
        except Exception as exc:  # noqa: BLE001
            self._handle_account_load_error(exc)
            return

        def _worker():
            return self._load_account_snapshot(runtime, symbol)

        def _on_success(snapshot) -> None:
            positions, pending_orders, order_history = snapshot
            self._apply_account_snapshot(positions, pending_orders, order_history)
            self._set_status(f"账户数据已加载：{symbol}")

        if not async_load:
            try:
                snapshot = _worker()
            except Exception as exc:  # noqa: BLE001
                self._handle_account_load_error(exc)
                return
            _on_success(snapshot)
            return

        self._set_status(f"正在加载账户数据：{symbol}")
        self._start_background_action(
            task_name="load-account-data",
            worker=_worker,
            on_success=_on_success,
            on_error=self._handle_account_load_error,
        )

    def _selected_position(self) -> OkxPosition | None:
        row = self._positions_table.currentRow()
        if row < 0 or row >= len(self._visible_positions):
            if len(self._visible_positions) == 1:
                return self._visible_positions[0]
            return None
        return self._visible_positions[row]

    def _selected_pending_order(self) -> OkxTradeOrderItem | None:
        row = self._current_orders_table.currentRow()
        if row < 0 or row >= len(self._visible_pending_orders):
            return None
        return self._visible_pending_orders[row]

    def _selected_rr_payload(self) -> dict[str, object] | None:
        if not self._selected_session_key:
            return None
        entry = self._entries.get(self._selected_session_key, {})
        rr_items = entry.get("rr") if isinstance(entry, dict) else []
        if not isinstance(rr_items, list):
            return None
        if 0 <= self._selected_rr_index < len(rr_items) and isinstance(rr_items[self._selected_rr_index], dict):
            return rr_items[self._selected_rr_index]
        row = self._rr_action_table.currentRow()
        if 0 <= row < len(rr_items) and isinstance(rr_items[row], dict):
            self._selected_rr_index = row
            return rr_items[row]
        return None

    def _prompt_positive_decimal(self, *, title: str, label: str, default_value: str) -> Decimal | None:
        text, accepted = QInputDialog.getText(self, title, label, text=default_value)
        if not accepted:
            return None
        value = _parse_decimal(text, label)
        if value <= 0:
            raise RuntimeError(f"{label} 必须大于 0。")
        return value

    def _get_instrument(self, symbol: str):
        try:
            return self._client.get_instrument(symbol, prefer_cached=True)
        except TypeError:
            return self._client.get_instrument(symbol)

    def _build_rr_strategy_config(self, *, symbol: str, side: str, runtime) -> StrategyConfig:
        return StrategyConfig(
            inst_id=symbol,
            bar=self._bar_edit.text().strip() or "1H",
            ema_period=1,
            atr_period=1,
            atr_stop_multiplier=Decimal("1"),
            atr_take_multiplier=Decimal("1"),
            order_size=Decimal("0"),
            trade_mode=runtime.trade_mode,
            signal_mode="long_only" if side == "long" else "short_only",
            position_mode=runtime.position_mode,
            environment=runtime.environment,
            tp_sl_trigger_type="last",
            strategy_id="qt_line_trading_rr",
            poll_seconds=10.0,
            risk_amount=None,
            trade_inst_id=symbol,
            tp_sl_mode="exchange",
            local_tp_sl_inst_id=symbol,
            entry_side_mode="follow_signal",
            run_mode="trade",
        )

    @Slot()
    def _submit_rr_order_from_selected(self, order_mode: str, *, side_override: str | None = None) -> None:
        try:
            payload = self._selected_rr_payload()
            if payload is None:
                raise RuntimeError("请先选中一个 RR 区块。")
            side = str(payload.get("side", "") or "").strip()
            if side_override is not None and side != side_override:
                raise RuntimeError(f"当前 RR 方向为 {side or '-'}，与提交方向不一致。")
            symbol = self._session_symbol()
            runtime = self._build_runtime()
            risk_usdt = self._prompt_positive_decimal(
                title="风险金",
                label="请输入风险金(USDT)：",
                default_value="100",
            )
            if risk_usdt is None:
                self._set_status("已取消 RR 下单。")
                return
            intent = build_rr_order_intent(
                symbol=symbol,
                side=side,
                entry_price=_parse_decimal(str(payload.get("price_entry", "") or ""), "入场价"),
                stop_price=_parse_decimal(str(payload.get("price_stop", "") or ""), "止损价"),
                take_profit=_parse_decimal(str(payload.get("price_tp", "") or ""), "止盈价"),
                risk_usdt=risk_usdt,
                order_mode=order_mode,
            )
            instrument = self._get_instrument(symbol)
            config = self._build_rr_strategy_config(symbol=symbol, side=side, runtime=runtime)
            take_profit = Decimal(str(intent["take_profit"]))
            if self._rr_fee_offset_check.isChecked():
                fee_offset = _dynamic_two_taker_fee_offset_live(Decimal(str(intent["entry_price"])), enabled=True)
                adjusted_take_profit = take_profit + fee_offset if side == "long" else take_profit - fee_offset
                tick_size = getattr(instrument, "tick_size", None)
                if isinstance(tick_size, Decimal) and tick_size > 0:
                    adjusted_take_profit = snap_to_increment(adjusted_take_profit, tick_size, "nearest")
                if side == "long" and intent["stop_price"] < intent["entry_price"] < adjusted_take_profit:
                    take_profit = adjusted_take_profit
                elif side == "short" and adjusted_take_profit < intent["entry_price"] < intent["stop_price"]:
                    take_profit = adjusted_take_profit
            size = determine_order_size(
                instrument=instrument,
                config=replace(config, risk_amount=risk_usdt),
                entry_price=intent["entry_price"],
                stop_loss=intent["stop_price"],
                risk_price_compatible=True,
            )
            trade_side = "buy" if side == "long" else "sell"
            pos_side = resolve_open_pos_side(config, trade_side)
            plan = OrderPlan(
                inst_id=symbol,
                side=trade_side,
                pos_side=pos_side,
                size=size,
                take_profit=take_profit,
                stop_loss=intent["stop_price"],
                entry_reference=intent["entry_price"],
                atr_value=abs(intent["entry_price"] - intent["stop_price"]),
                signal=side,
                candle_ts=0,
                tp_sl_mode="exchange",
            )
            confirmed = QMessageBox.question(
                self,
                "确认下单",
                (
                    f"方向：{side}\n"
                    f"合约：{symbol}\n"
                    f"入场：{format_decimal(intent['entry_price'])}\n"
                    f"止损：{format_decimal(intent['stop_price'])}\n"
                    f"止盈：{format_decimal(take_profit)}\n"
                    f"风险金：{format_decimal(risk_usdt)} USDT\n"
                    f"数量：{format_decimal(size)}\n"
                    f"模式：{order_mode}\n"
                    f"手续费偏移：{'开启' if self._rr_fee_offset_check.isChecked() else '关闭'}"
                ),
            )
            if confirmed != QMessageBox.StandardButton.Yes:
                self._set_status("已取消 RR 下单。")
                return
        except Exception as exc:  # noqa: BLE001
            self._show_error("下单失败", str(exc))
            return

        self._set_status(f"正在提交 RR 下单：{symbol}")

        def _worker():
            if order_mode == "trigger":
                return self._client.place_trigger_limit_algo_order(runtime.credentials, config, plan)
            return self._client.place_limit_order(runtime.credentials, config, plan)

        def _on_success(result) -> None:
            result_id = result.ord_id or result.cl_ord_id or "-"
            self._append_workbench_log(
                f"RR 下单已提交：{symbol} | 方向={side} | 模式={order_mode} | 数量={format_decimal(size)} | id={result_id}"
            )
            self._set_status(f"RR 下单已提交：{result_id}")
            self._populate_account_data()

        self._start_background_action(
            task_name="submit-rr-order",
            worker=_worker,
            on_success=_on_success,
            on_error=lambda exc: self._show_error("下单失败", str(exc)),
        )

    def _position_direction(self, position: OkxPosition) -> str:
        pos_side = str(position.pos_side or "").strip().lower()
        if pos_side in {"long", "short"}:
            return pos_side
        return "long" if Decimal(str(position.position)) > 0 else "short"

    def _build_flatten_strategy_config(self, position: OkxPosition, runtime) -> StrategyConfig:
        normalized_mgn_mode = str(position.mgn_mode or "").strip().lower()
        trade_mode = normalized_mgn_mode if normalized_mgn_mode in {"cross", "isolated"} else runtime.trade_mode
        direction = self._position_direction(position)
        return StrategyConfig(
            inst_id=position.inst_id,
            bar="1m",
            ema_period=1,
            atr_period=1,
            atr_stop_multiplier=Decimal("1"),
            atr_take_multiplier=Decimal("1"),
            order_size=abs(position.position),
            trade_mode=trade_mode,
            signal_mode="long_only" if direction == "long" else "short_only",
            position_mode="long_short" if str(position.pos_side or "").strip().lower() in {"long", "short"} else "net",
            environment=runtime.environment,
            tp_sl_trigger_type="last",
            strategy_id="qt_line_trading_flatten",
            poll_seconds=10.0,
            risk_amount=None,
            trade_inst_id=position.inst_id,
            tp_sl_mode="local_trade",
            local_tp_sl_inst_id=position.inst_id,
            entry_side_mode="follow_signal",
            run_mode="trade",
        )

    def _resolve_best_quote_flatten_price(self, inst_id: str, *, side: str) -> Decimal:
        instrument = self._get_instrument(inst_id)
        order_book = None
        try:
            order_book = self._client.get_order_book(inst_id, depth=5)
        except Exception:  # noqa: BLE001
            order_book = None
        ticker = self._client.get_ticker(inst_id)
        if side == "buy":
            raw_price = order_book.bids[0][0] if order_book is not None and order_book.bids else ticker.bid
            if raw_price is None or raw_price <= 0:
                raise RuntimeError(f"{inst_id} 当前缺少买一价。")
            return snap_to_increment(raw_price, instrument.tick_size, "down")
        raw_price = order_book.asks[0][0] if order_book is not None and order_book.asks else ticker.ask
        if raw_price is None or raw_price <= 0:
            raise RuntimeError(f"{inst_id} 当前缺少卖一价。")
        return snap_to_increment(raw_price, instrument.tick_size, "up")

    def _flatten_selected_position(self, flatten_mode: str) -> None:
        try:
            position = self._selected_position()
            if position is None:
                raise RuntimeError("请先选中一条持仓。")
            runtime = self._build_runtime()
            config = self._build_flatten_strategy_config(position, runtime)
            close_side = "sell" if self._position_direction(position) == "long" else "buy"
            pos_side = resolve_open_pos_side(config, close_side)
            size = self._selected_position_close_amount_v2(position)
            direction = self._position_direction(position)
            close_side_label = "SELL 卖出平仓" if close_side == "sell" else "BUY 买入平仓"
            dir_label = "多头" if direction == "long" else "空头"
            mode_label = "挂买一/卖一平仓" if flatten_mode == "best_quote" else "市价平仓"
            message = (
                f"合约：{position.inst_id}\n"
                f"方向：{dir_label}\n"
                f"报单方向：{close_side_label}\n"
                f"数量：{format_decimal(size)}\n"
                f"方式：{mode_label}"
            )
            confirmed = QMessageBox.question(self, "确认平仓", message)
            if confirmed != QMessageBox.StandardButton.Yes:
                self._set_status("已取消平仓。")
                return
        except Exception as exc:  # noqa: BLE001
            self._show_error("平仓失败", str(exc))
            return

        self._set_status(f"正在提交平仓委托：{position.inst_id}")

        def _worker():
            if flatten_mode == "best_quote":
                price = self._resolve_best_quote_flatten_price(position.inst_id, side=close_side)
                return self._client.place_simple_order(
                    runtime.credentials,
                    config,
                    inst_id=position.inst_id,
                    side=close_side,
                    size=size,
                    ord_type="limit",
                    pos_side=pos_side,
                    price=price,
                    reduce_only=True,
                )
            return self._client.place_simple_order(
                runtime.credentials,
                config,
                inst_id=position.inst_id,
                side=close_side,
                size=size,
                ord_type="market",
                pos_side=pos_side,
                reduce_only=True,
            )

        def _on_success(result) -> None:
            result_id = result.ord_id or result.cl_ord_id or "-"
            self._append_workbench_log(f"平仓委托已提交：{position.inst_id} | 模式={flatten_mode} | id={result_id}")
            self._set_status(f"平仓委托已提交：{result_id}")
            self._populate_account_data()

        self._start_background_action(
            task_name="flatten-position",
            worker=_worker,
            on_success=_on_success,
            on_error=lambda exc: self._show_error("平仓失败", str(exc)),
        )

    @Slot()
    def _cancel_selected_pending_order(self) -> None:
        try:
            order = self._selected_pending_order()
            if order is None:
                raise RuntimeError("请先选中一条当前委托。")
            runtime = self._build_runtime()
            message = (
                f"合约：{order.inst_id}\n"
                f"方向：{_safe_text(order.side)}\n"
                f"状态：{_safe_text(order.state)}\n"
                f"标识：{_safe_text(order.order_id or order.algo_id)}"
            )
            confirmed = QMessageBox.question(self, "确认撤单", message)
            if confirmed != QMessageBox.StandardButton.Yes:
                self._set_status("已取消撤单。")
                return
        except Exception as exc:  # noqa: BLE001
            self._show_error("撤单失败", str(exc))
            return

        self._set_status(f"正在撤销委托：{order.inst_id}")

        def _worker():
            if getattr(order, "source_kind", "") == "algo":
                return self._client.cancel_algo_order(
                    runtime.credentials,
                    environment=runtime.environment,
                    inst_id=order.inst_id,
                    algo_id=order.algo_id or None,
                    algo_cl_ord_id=order.algo_client_order_id or order.client_order_id or None,
                )
            return self._client.cancel_order_by_id(
                runtime.credentials,
                environment=runtime.environment,
                inst_id=order.inst_id,
                ord_id=order.order_id or None,
                cl_ord_id=order.client_order_id or None,
            )

        def _on_success(result) -> None:
            result_id = result.ord_id or result.cl_ord_id or "-"
            self._append_workbench_log(f"撤单已提交：{order.inst_id} | id={result_id}")
            self._set_status(f"撤单已提交：{result_id}")
            self._populate_account_data()

        self._start_background_action(
            task_name="cancel-order",
            worker=_worker,
            on_success=_on_success,
            on_error=lambda exc: self._show_error("撤单失败", str(exc)),
        )

    def _populate_account_placeholders(self) -> None:
        self._positions_table.setRowCount(1)
        self._set_table_row(
            self._positions_table,
            0,
            ("账户数据", "未加载", "-", "-", "-", "未加载"),
        )
        self._current_orders_table.setRowCount(1)
        self._set_table_row(
            self._current_orders_table,
            0,
            ("委托数据", "未加载", "-", "-", "未加载", "-"),
        )
        self._order_history_table.setRowCount(1)
        self._set_table_row(
            self._order_history_table,
            0,
            ("-", "历史数据", "未加载", "-", "-", "未加载"),
        )

    def _set_table_row(self, table: QTableWidget, row: int, cells: tuple[str, ...]) -> None:
        for column, value in enumerate(cells):
            table.setItem(row, column, QTableWidgetItem(value))

    @Slot()
    def _placeholder_account_action(self, action: str) -> None:
        message = f"{action} is a placeholder in this pass; no live order API was called."
        self._set_status(message)
        self._append_workbench_log(message)

    def _append_workbench_log(self, message: str) -> None:
        raw = str(message or "").strip()
        if not raw:
            return
        symbol = self._session_symbol().strip().upper() or "-"
        line = ensure_log_timestamp(f"{symbol} | {raw}", timestamp=current_log_timestamp())
        append_line_desk_log_line(line)
        self._workbench_log.append(line)
        lines = self._workbench_log.toPlainText().splitlines()
        if len(lines) > 3000:
            self._workbench_log.setPlainText("\n".join(lines[-2500:]))

    @Slot(object)
    def _run_ui_callback(self, callback: object) -> None:
        if self._window_closing:
            return
        if callable(callback):
            callback()

    def closeEvent(self, event) -> None:  # noqa: ANN001
        self._window_closing = True
        super().closeEvent(event)

    def _start_background_action(
        self,
        *,
        task_name: str,
        worker,
        on_success=None,
        on_error=None,
    ) -> None:
        def _runner() -> None:
            try:
                result = worker()
            except Exception as exc:  # noqa: BLE001
                if callable(on_error) and not self._window_closing:
                    try:
                        self._ui_callback.emit(lambda exc=exc: on_error(exc))
                    except RuntimeError:
                        pass
                return
            if callable(on_success) and not self._window_closing:
                try:
                    self._ui_callback.emit(lambda result=result: on_success(result))
                except RuntimeError:
                    pass

        threading.Thread(
            target=_runner,
            name=f"qt-line-trading-{task_name}",
            daemon=True,
        ).start()

    def _sync_line_selection_views(self) -> None:
        self._selection_sync_guard = True
        try:
            if 0 <= self._selected_line_index < self._line_table.rowCount():
                self._line_table.selectRow(self._selected_line_index)
            else:
                self._line_table.clearSelection()
                self._line_table.setCurrentCell(-1, -1)
            if 0 <= self._selected_line_index < self._ray_trigger_table.rowCount():
                self._ray_trigger_table.selectRow(self._selected_line_index)
            else:
                self._ray_trigger_table.clearSelection()
                self._ray_trigger_table.setCurrentCell(-1, -1)
        finally:
            self._selection_sync_guard = False

    def _sync_rr_selection_views(self) -> None:
        self._selection_sync_guard = True
        try:
            if 0 <= self._selected_rr_index < self._rr_table.rowCount():
                self._rr_table.selectRow(self._selected_rr_index)
            else:
                self._rr_table.clearSelection()
                self._rr_table.setCurrentCell(-1, -1)
            if 0 <= self._selected_rr_index < self._rr_action_table.rowCount():
                self._rr_action_table.selectRow(self._selected_rr_index)
            else:
                self._rr_action_table.clearSelection()
                self._rr_action_table.setCurrentCell(-1, -1)
        finally:
            self._selection_sync_guard = False

    @Slot()
    def _on_line_selected(self) -> None:
        if self._selection_sync_guard:
            return
        row = self._line_table.currentRow()
        if row < 0:
            self._selected_line_index = -1
            self._sync_line_selection_views()
            self._sync_chart_selection()
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
        self._sync_line_selection_views()
        self._sync_chart_selection()

    @Slot()
    def _on_ray_trigger_selected(self) -> None:
        if self._selection_sync_guard:
            return
        row = self._ray_trigger_table.currentRow()
        if row < 0:
            self._selected_line_index = -1
            self._sync_line_selection_views()
            return
        self._selected_line_index = row
        self._sync_line_selection_views()
        self._on_line_selected()

    @Slot(int, int)
    def _on_ray_trigger_cell_double_clicked(self, row: int, column: int) -> None:
        if column not in {2, 3}:
            return
        self._selected_line_index = row
        self._sync_line_selection_views()
        try:
            entry = self._current_entry()
            lines = entry.get("lines")
            if not isinstance(lines, list) or not (0 <= row < len(lines)):
                raise RuntimeError("请先选择一条射线。")
            payload = lines[row]
            if not isinstance(payload, dict):
                raise RuntimeError("当前射线数据无效。")
            if bool(payload.get("locked", False)):
                self._show_info("修改射线价格", "已锁定的射线不能改价。")
                return
            if bool(payload.get("desk_ray_triggered", False)) or bool(payload.get("desk_ray_submit_pending", False)):
                self._show_info("修改射线价格", "已触发或提交中的射线不能修改价格。")
                return
            kind = str(payload.get("kind", "line") or "line").strip()
            if kind in {"horizontal", "stop"}:
                target_keys = ("price_a", "price_b")
                default_value = str(payload.get("price_a", "") or payload.get("price_b", "") or "")
                field_name = "水平价格"
            else:
                target_keys = ("price_a",) if column == 2 else ("price_b",)
                default_value = str(payload.get(target_keys[0], "") or "")
                field_name = "价格 A" if column == 2 else "价格 B"
            text, accepted = QInputDialog.getText(self, "修改射线价格", f"请输入{field_name}：", text=default_value)
            if not accepted:
                self._set_status("已取消射线改价。")
                return
            price = _parse_decimal(text, field_name)
            if price <= 0:
                raise RuntimeError(f"{field_name} 必须大于 0。")
            symbol = self._session_symbol().strip().upper()
            instrument = self._get_instrument(symbol) if symbol else None
            tick_size = getattr(instrument, "tick_size", None)
            if isinstance(tick_size, Decimal) and tick_size > 0:
                price = snap_to_increment(price, tick_size, "nearest")
            for key in target_keys:
                payload[key] = _decimal_text(price)
            payload["desk_ray_last_side"] = None
        except Exception as exc:  # noqa: BLE001
            self._show_error("修改射线价格失败", str(exc))
            return
        self._save_entries()
        self._set_status(f"射线第 {row + 1} 条价格已更新。")

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
    def _toggle_selected_ray_lock(self) -> None:
        try:
            entry = self._current_entry()
            lines = entry.get("lines")
            if not isinstance(lines, list) or not (0 <= self._selected_line_index < len(lines)):
                raise RuntimeError("请先选择一条射线。")
            payload = lines[self._selected_line_index]
            if not isinstance(payload, dict):
                raise RuntimeError("当前射线数据无效。")
            payload["locked"] = not bool(payload.get("locked", False))
            state_text = "已锁定" if bool(payload["locked"]) else "已解锁"
        except Exception as exc:  # noqa: BLE001
            self._show_error("切换射线锁定失败", str(exc))
            return
        self._save_entries()
        self._set_status(f"射线第 {self._selected_line_index + 1} 条{state_text}。")

    @Slot()
    def _delete_selected_ray(self) -> None:
        self._remove_line_item()

    @Slot()
    def _on_rr_selected(self) -> None:
        if self._selection_sync_guard:
            return
        row = self._rr_table.currentRow()
        if row < 0:
            self._selected_rr_index = -1
            self._sync_rr_selection_views()
            self._sync_chart_selection()
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
        self._sync_rr_selection_views()
        self._sync_chart_selection()

    @Slot()
    def _on_rr_action_selected(self) -> None:
        if self._selection_sync_guard:
            return
        row = self._rr_action_table.currentRow()
        if row < 0:
            self._selected_rr_index = -1
            self._sync_rr_selection_views()
            return
        self._selected_rr_index = row
        self._sync_rr_selection_views()
        self._on_rr_selected()

    @Slot(int, int)
    def _on_rr_action_cell_double_clicked(self, row: int, column: int) -> None:
        if column not in {2, 3, 4, 5}:
            return
        self._selected_rr_index = row
        self._sync_rr_selection_views()
        try:
            entry = self._current_entry()
            rr_items = entry.get("rr")
            if not isinstance(rr_items, list) or not (0 <= row < len(rr_items)):
                raise RuntimeError("请先选择一个 RR 项。")
            payload = rr_items[row]
            if not isinstance(payload, dict):
                raise RuntimeError("当前 RR 数据无效。")
            field_map = {
                2: ("price_entry", "入场价"),
                3: ("price_stop", "止损价"),
                4: ("price_tp", "TP"),
                5: ("r_multiple", "R"),
            }
            field_key, field_name = field_map[column]
            default_value = str(payload.get(field_key, "") or "")
            text, accepted = QInputDialog.getText(
                self,
                "修改 RR",
                f"请输入{field_name}：",
                text=default_value,
            )
            if not accepted:
                self._set_status("已取消 RR 修改。")
                return
            value = _parse_decimal(text, field_name)
            if value <= 0:
                raise RuntimeError(f"{field_name}必须大于 0。")
            if column == 4:
                payload["price_tp"] = _decimal_text(value)
            else:
                side = str(payload.get("side", "long") or "long").strip().lower()
                entry_price = value if column == 2 else _parse_decimal(str(payload.get("price_entry", "") or ""), "入场价")
                stop_price = value if column == 3 else _parse_decimal(str(payload.get("price_stop", "") or ""), "止损价")
                r_multiple = value if column == 5 else _parse_decimal(str(payload.get("r_multiple", "") or ""), "R")
                price_tp = _compute_rr_target(side, entry_price, stop_price, r_multiple)
                payload["price_entry"] = _decimal_text(entry_price)
                payload["price_stop"] = _decimal_text(stop_price)
                payload["r_multiple"] = _decimal_text(r_multiple)
                payload["price_tp"] = _decimal_text(price_tp)
        except Exception as exc:  # noqa: BLE001
            self._show_error("修改 RR 失败", str(exc))
            return
        self._save_entries()
        self._set_status(f"RR 第 {row + 1} 条已更新。")

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

    @Slot()
    def _toggle_selected_rr_lock(self) -> None:
        try:
            entry = self._current_entry()
            rr_items = entry.get("rr")
            if not isinstance(rr_items, list) or not (0 <= self._selected_rr_index < len(rr_items)):
                raise RuntimeError("请先选择一个 RR 区块。")
            payload = rr_items[self._selected_rr_index]
            if not isinstance(payload, dict):
                raise RuntimeError("当前 RR 数据无效。")
            payload["locked"] = not bool(payload.get("locked", False))
            state_text = "已锁定" if bool(payload["locked"]) else "已解锁"
        except Exception as exc:  # noqa: BLE001
            self._show_error("切换 RR 锁定失败", str(exc))
            return
        self._save_entries()
        self._set_status(f"RR 第 {self._selected_rr_index + 1} 条{state_text}。")

    @Slot()
    def _delete_selected_rr(self) -> None:
        self._remove_rr_item()


def _combo_data(combo: QComboBox) -> str:
    return str(combo.currentData() or "").strip()


def _set_combo_data(combo: QComboBox, target: str) -> None:
    for index in range(combo.count()):
        if str(combo.itemData(index) or "").strip() == target:
            combo.setCurrentIndex(index)
            return
