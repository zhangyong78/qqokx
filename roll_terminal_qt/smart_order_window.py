from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from typing import Literal

from PySide6.QtCore import QTimer, Qt, Slot
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QFormLayout,
    QFrame,
    QGridLayout,
    QHeaderView,
    QHBoxLayout,
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

from okx_quant.models import Credentials, Instrument
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import (
    load_smart_order_favorites_snapshot,
    save_smart_order_favorites_snapshot,
)
from okx_quant.pricing import format_decimal, format_decimal_by_increment, snap_to_increment
from okx_quant.smart_order import (
    ExecutionMode,
    SmartOrderManager,
    SmartOrderRuntimeConfig,
    TriggerDirection,
    resolve_best_quote_price,
)
from roll_terminal_qt.profile_access import ensure_profile_unlocked, load_profile_snapshots


INSTRUMENT_TYPE_OPTIONS: tuple[tuple[str, str], ...] = (
    ("现货 SPOT", "SPOT"),
    ("永续 SWAP", "SWAP"),
    ("期权 OPTION", "OPTION"),
)
MANUAL_ORDER_TYPE_OPTIONS: tuple[tuple[str, str], ...] = (
    ("限价", "limit"),
    ("最优价", "best_quote"),
    ("追价 IOC", "aggressive_ioc"),
    ("IOC", "ioc"),
    ("FOK", "fok"),
    ("Post Only", "post_only"),
)
TRIGGER_SOURCE_OPTIONS: tuple[tuple[str, tuple[str, str]], ...] = (
    ("当前合约最新价", ("current", "last")),
    ("当前合约标记价", ("current", "mark")),
    ("当前合约指数价", ("current", "index")),
    ("自定义标的最新价", ("custom", "last")),
    ("自定义标的标记价", ("custom", "mark")),
    ("自定义标的指数价", ("custom", "index")),
)
TRIGGER_DIRECTION_OPTIONS: tuple[tuple[str, TriggerDirection], ...] = (
    ("上穿触发", "above"),
    ("下穿触发", "below"),
)
EXEC_MODE_OPTIONS: tuple[tuple[str, ExecutionMode], ...] = (
    ("限价", "limit"),
    ("追价 IOC", "aggressive_ioc"),
)
ENVIRONMENT_OPTIONS: tuple[tuple[str, Literal["demo", "live"]], ...] = (
    ("模拟盘 demo", "demo"),
    ("实盘 live", "live"),
)
TRADE_MODE_OPTIONS: tuple[tuple[str, Literal["cross", "isolated"]], ...] = (
    ("全仓 cross", "cross"),
    ("逐仓 isolated", "isolated"),
)
POSITION_MODE_OPTIONS: tuple[tuple[str, Literal["net", "long_short"]], ...] = (
    ("净持仓 net", "net"),
    ("双向持仓 long_short", "long_short"),
)
GRID_CYCLE_OPTIONS: tuple[str, ...] = ("连续", "1", "3", "5", "10")
REFRESH_INTERVAL_MS = 1500
DEFAULT_TASK_HINT = "这里会显示任务运行状态、活动委托和恢复信息。"


_SHARED_CLIENT: OkxRestClient | None = None
_SHARED_MANAGER: SmartOrderManager | None = None


def _shared_client() -> OkxRestClient:
    global _SHARED_CLIENT
    if _SHARED_CLIENT is None:
        _SHARED_CLIENT = OkxRestClient()
    return _SHARED_CLIENT


def _shared_manager() -> SmartOrderManager:
    global _SHARED_MANAGER
    if _SHARED_MANAGER is None:
        _SHARED_MANAGER = SmartOrderManager(_shared_client())
    return _SHARED_MANAGER


def _safe_text(value: object) -> str:
    if value is None:
        return "-"
    text = str(value).strip()
    return text or "-"


def _combo_value(combo: QComboBox) -> str:
    return str(combo.currentData() or "").strip()


def _set_combo_value(combo: QComboBox, target: str) -> None:
    for index in range(combo.count()):
        if str(combo.itemData(index) or "").strip() == target:
            combo.setCurrentIndex(index)
            return


class SmartOrderQtWindow(QMainWindow):
    def __init__(self, parent=None) -> None:  # noqa: ANN001
        super().__init__(parent)
        self._client = _shared_client()
        self._manager = _shared_manager()
        self._favorites = self._load_favorites()
        self._instrument: Instrument | None = self._manager.locked_instrument
        self._selected_task_id = ""
        self._profile_snapshots: dict[str, dict[str, str]] = {}
        self._unlocked_profiles: set[str] = set()
        self._last_profile_name = ""
        self._last_ladder_anchor_price = ""

        self.setWindowTitle("无限下单 - Qt")
        self.resize(1560, 960)

        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(14)

        layout.addWidget(self._build_header())
        layout.addWidget(self._build_runtime_panel())
        layout.addWidget(self._build_metrics_panel())

        content = QSplitter(Qt.Orientation.Horizontal)
        content.addWidget(self._build_left_panel())
        content.addWidget(self._build_right_panel())
        content.setSizes([620, 920])
        layout.addWidget(content, 1)

        self._timer = QTimer(self)
        self._timer.setInterval(REFRESH_INTERVAL_MS)
        self._timer.timeout.connect(self.refresh_view)
        self._timer.start()

        self._refresh_profiles()
        self._refresh_favorite_options()
        if self._instrument is not None:
            self._apply_instrument_to_form(self._instrument)
            try:
                self._manager.ensure_market_snapshot(self._instrument, force=True)
            except Exception:
                pass
        self.refresh_view()

    def closeEvent(self, event) -> None:  # noqa: ANN001
        self._timer.stop()
        super().closeEvent(event)

    def _build_header(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Panel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(8)

        title = QLabel("无限下单")
        title.setObjectName("SectionTitle")
        subtitle = QLabel(
            "纯 Qt 版本直接接入共享任务、收藏、仓位限制和运行状态。"
            "这里不再依赖旧窗口，后续任务和数据都从同一套共享文件继续。"
        )
        subtitle.setObjectName("Subtle")
        subtitle.setWordWrap(True)
        layout.addWidget(title)
        layout.addWidget(subtitle)
        return panel

    def _build_runtime_panel(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Guide")
        layout = QGridLayout(panel)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setHorizontalSpacing(12)
        layout.setVerticalSpacing(10)

        self._profile_combo = QComboBox()
        self._profile_combo.currentIndexChanged.connect(self._on_profile_changed)
        self._environment_combo = QComboBox()
        for label, value in ENVIRONMENT_OPTIONS:
            self._environment_combo.addItem(label, value)
        self._trade_mode_combo = QComboBox()
        for label, value in TRADE_MODE_OPTIONS:
            self._trade_mode_combo.addItem(label, value)
        self._position_mode_combo = QComboBox()
        for label, value in POSITION_MODE_OPTIONS:
            self._position_mode_combo.addItem(label, value)

        refresh_button = QPushButton("刷新状态")
        refresh_button.clicked.connect(self.refresh_view)
        unlock_button = QPushButton("空闲时解锁合约")
        unlock_button.clicked.connect(self._unlock_contract_if_idle)

        self._runtime_summary = QLabel("")
        self._runtime_summary.setObjectName("GuideText")
        self._runtime_summary.setWordWrap(True)
        self._status = QLabel("")
        self._status.setObjectName("Subtle")
        self._status.setWordWrap(True)

        layout.addWidget(QLabel("API Profile"), 0, 0)
        layout.addWidget(self._profile_combo, 0, 1)
        layout.addWidget(QLabel("环境"), 0, 2)
        layout.addWidget(self._environment_combo, 0, 3)
        layout.addWidget(QLabel("交易模式"), 0, 4)
        layout.addWidget(self._trade_mode_combo, 0, 5)
        layout.addWidget(QLabel("持仓模式"), 0, 6)
        layout.addWidget(self._position_mode_combo, 0, 7)
        layout.addWidget(refresh_button, 0, 8)
        layout.addWidget(unlock_button, 0, 9)
        layout.addWidget(self._runtime_summary, 1, 0, 1, 6)
        layout.addWidget(self._status, 1, 6, 1, 4)
        return panel

    def _build_metrics_panel(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Panel")
        layout = QHBoxLayout(panel)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(18)
        self._task_metric = QLabel("")
        self._favorite_metric = QLabel("")
        self._locked_metric = QLabel("")
        self._position_metric = QLabel("")
        for label in (self._task_metric, self._favorite_metric, self._locked_metric, self._position_metric):
            label.setObjectName("GuideText")
            layout.addWidget(label, 1)
        return panel

    def _build_left_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)
        layout.addWidget(self._build_instrument_panel())

        tabs = QTabWidget()
        tabs.addTab(self._build_manual_tab(), "手动下单")
        tabs.addTab(self._build_condition_tab(), "条件单")
        tabs.addTab(self._build_tp_sl_tab(), "止盈止损")
        tabs.addTab(self._build_grid_tab(), "网格")
        tabs.addTab(self._build_position_limit_tab(), "仓位限制")
        layout.addWidget(tabs, 1)
        return panel

    def _build_right_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        ladder_panel = QFrame()
        ladder_panel.setObjectName("Panel")
        ladder_layout = QVBoxLayout(ladder_panel)
        ladder_layout.setContentsMargins(14, 14, 14, 14)
        ladder_layout.setSpacing(10)

        ladder_toolbar = QHBoxLayout()
        ladder_toolbar.addWidget(QLabel("盘口梯子"))
        ladder_toolbar.addStretch(1)
        ladder_toolbar.addWidget(QLabel("价格分组"))
        self._ladder_filter_combo = QComboBox()
        self._ladder_filter_combo.currentIndexChanged.connect(self.refresh_view)
        ladder_toolbar.addWidget(self._ladder_filter_combo)
        ladder_layout.addLayout(ladder_toolbar)

        self._ladder_table = QTableWidget(0, 4)
        self._ladder_table.setHorizontalHeaderLabels(["买量", "价格", "卖量", "活动映射"])
        self._ladder_table.verticalHeader().setVisible(False)
        self._ladder_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._ladder_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._ladder_table.setAlternatingRowColors(True)
        self._ladder_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        ladder_header = self._ladder_table.horizontalHeader()
        ladder_header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        ladder_header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        ladder_header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        ladder_header.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        self._ladder_table.cellClicked.connect(self._on_ladder_cell_clicked)
        ladder_layout.addWidget(self._ladder_table, 1)
        layout.addWidget(ladder_panel, 3)

        task_panel = QFrame()
        task_panel.setObjectName("Panel")
        task_layout = QVBoxLayout(task_panel)
        task_layout.setContentsMargins(14, 14, 14, 14)
        task_layout.setSpacing(10)

        task_toolbar = QHBoxLayout()
        task_toolbar.addWidget(QLabel("任务"))
        task_toolbar.addStretch(1)
        restart_button = QPushButton("重启")
        restart_button.clicked.connect(self._restart_selected_task)
        stop_button = QPushButton("停止")
        stop_button.clicked.connect(self._stop_selected_task)
        stop_all_button = QPushButton("全部停止")
        stop_all_button.clicked.connect(self._stop_all_tasks)
        remove_button = QPushButton("删除")
        remove_button.clicked.connect(self._remove_selected_task)
        for button in (restart_button, stop_button, stop_all_button, remove_button):
            task_toolbar.addWidget(button)
        task_layout.addLayout(task_toolbar)

        self._task_table = QTableWidget(0, 8)
        self._task_table.setHorizontalHeaderLabels(
            ["ID", "类型", "状态", "方向", "数量", "活动价", "循环", "说明"]
        )
        self._task_table.verticalHeader().setVisible(False)
        self._task_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._task_table.itemSelectionChanged.connect(self._sync_task_detail)
        task_layout.addWidget(self._task_table, 1)

        self._task_detail = QTextEdit()
        self._task_detail.setReadOnly(True)
        self._task_detail.setMinimumHeight(120)
        task_layout.addWidget(self._task_detail)
        layout.addWidget(task_panel, 3)

        log_panel = QFrame()
        log_panel.setObjectName("Panel")
        log_layout = QVBoxLayout(log_panel)
        log_layout.setContentsMargins(14, 14, 14, 14)
        log_layout.setSpacing(10)
        log_layout.addWidget(QLabel("日志"))
        self._log_text = QTextEdit()
        self._log_text.setReadOnly(True)
        self._log_text.setMinimumHeight(180)
        log_layout.addWidget(self._log_text, 1)
        layout.addWidget(log_panel, 2)
        return panel

    def _build_instrument_panel(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Panel")
        layout = QGridLayout(panel)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setHorizontalSpacing(12)
        layout.setVerticalSpacing(10)

        self._inst_type_combo = QComboBox()
        for label, value in INSTRUMENT_TYPE_OPTIONS:
            self._inst_type_combo.addItem(label, value)
        self._inst_type_combo.currentIndexChanged.connect(self._refresh_favorite_options)
        self._inst_id_edit = QLineEdit()
        self._favorite_combo = QComboBox()
        self._favorite_combo.currentIndexChanged.connect(self._on_favorite_selected)
        self._instrument_summary = QLabel("请先选择合约。")
        self._instrument_summary.setObjectName("Subtle")
        self._instrument_summary.setWordWrap(True)

        load_button = QPushButton("加载合约")
        load_button.clicked.connect(self._load_instrument)
        add_favorite_button = QPushButton("加入收藏")
        add_favorite_button.clicked.connect(self._add_favorite)
        remove_favorite_button = QPushButton("移除收藏")
        remove_favorite_button.clicked.connect(self._remove_favorite)

        layout.addWidget(QLabel("类型"), 0, 0)
        layout.addWidget(self._inst_type_combo, 0, 1)
        layout.addWidget(QLabel("合约"), 0, 2)
        layout.addWidget(self._inst_id_edit, 0, 3)
        layout.addWidget(load_button, 0, 4)
        layout.addWidget(add_favorite_button, 0, 5)
        layout.addWidget(remove_favorite_button, 0, 6)
        layout.addWidget(QLabel("收藏"), 1, 0)
        layout.addWidget(self._favorite_combo, 1, 1, 1, 3)
        layout.addWidget(self._instrument_summary, 1, 4, 1, 3)
        return panel

    def _build_manual_tab(self) -> QWidget:
        panel = QWidget()
        layout = QFormLayout(panel)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        self._manual_side_combo = QComboBox()
        self._manual_side_combo.addItem("买入 buy", "buy")
        self._manual_side_combo.addItem("卖出 sell", "sell")
        self._manual_order_type_combo = QComboBox()
        for label, value in MANUAL_ORDER_TYPE_OPTIONS:
            self._manual_order_type_combo.addItem(label, value)
        self._manual_price_edit = QLineEdit()
        self._manual_size_edit = QLineEdit("0.01")
        submit_button = QPushButton("提交手动单")
        submit_button.clicked.connect(self._submit_manual_order)

        layout.addRow("方向", self._manual_side_combo)
        layout.addRow("委托类型", self._manual_order_type_combo)
        layout.addRow("价格", self._manual_price_edit)
        layout.addRow("数量", self._manual_size_edit)
        layout.addRow(QLabel(self._quantity_hint_text()))
        layout.addRow(submit_button)
        return panel

    def _build_condition_tab(self) -> QWidget:
        panel = QWidget()
        layout = QFormLayout(panel)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        self._condition_side_combo = QComboBox()
        self._condition_side_combo.addItem("买入 buy", "buy")
        self._condition_side_combo.addItem("卖出 sell", "sell")
        self._condition_trigger_source_combo = QComboBox()
        for label, value in TRIGGER_SOURCE_OPTIONS:
            self._condition_trigger_source_combo.addItem(label, value)
        self._condition_custom_inst_edit = QLineEdit()
        self._condition_direction_combo = QComboBox()
        for label, value in TRIGGER_DIRECTION_OPTIONS:
            self._condition_direction_combo.addItem(label, value)
        self._condition_trigger_price_edit = QLineEdit()
        self._condition_exec_mode_combo = QComboBox()
        for label, value in EXEC_MODE_OPTIONS:
            self._condition_exec_mode_combo.addItem(label, value)
        self._condition_exec_price_edit = QLineEdit()
        self._condition_size_edit = QLineEdit("0.01")
        self._condition_tp_edit = QLineEdit()
        self._condition_sl_edit = QLineEdit()
        submit_button = QPushButton("创建条件单任务")
        submit_button.clicked.connect(self._start_condition_task)

        layout.addRow("方向", self._condition_side_combo)
        layout.addRow("触发源", self._condition_trigger_source_combo)
        layout.addRow("自定义标的", self._condition_custom_inst_edit)
        layout.addRow("触发方向", self._condition_direction_combo)
        layout.addRow("触发价", self._condition_trigger_price_edit)
        layout.addRow("执行方式", self._condition_exec_mode_combo)
        layout.addRow("执行价", self._condition_exec_price_edit)
        layout.addRow("数量", self._condition_size_edit)
        layout.addRow("止盈触发", self._condition_tp_edit)
        layout.addRow("止损触发", self._condition_sl_edit)
        layout.addRow(submit_button)
        return panel

    def _build_tp_sl_tab(self) -> QWidget:
        panel = QWidget()
        layout = QFormLayout(panel)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        self._tp_sl_side_combo = QComboBox()
        self._tp_sl_side_combo.addItem("多仓 long", "long")
        self._tp_sl_side_combo.addItem("空仓 short", "short")
        self._tp_sl_trigger_source_combo = QComboBox()
        for label, value in TRIGGER_SOURCE_OPTIONS:
            self._tp_sl_trigger_source_combo.addItem(label, value)
        self._tp_sl_custom_inst_edit = QLineEdit()
        self._tp_sl_size_edit = QLineEdit("0.01")
        self._tp_sl_tp_edit = QLineEdit()
        self._tp_sl_sl_edit = QLineEdit()
        submit_button = QPushButton("创建止盈止损任务")
        submit_button.clicked.connect(self._start_tp_sl_task)

        layout.addRow("持仓方向", self._tp_sl_side_combo)
        layout.addRow("触发源", self._tp_sl_trigger_source_combo)
        layout.addRow("自定义标的", self._tp_sl_custom_inst_edit)
        layout.addRow("保护数量", self._tp_sl_size_edit)
        layout.addRow("止盈触发", self._tp_sl_tp_edit)
        layout.addRow("止损触发", self._tp_sl_sl_edit)
        layout.addRow(submit_button)
        return panel

    def _build_grid_tab(self) -> QWidget:
        panel = QWidget()
        layout = QFormLayout(panel)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        self._grid_enabled = QCheckBox("开启盘口点击建网格")
        self._grid_size_edit = QLineEdit("0.01")
        self._grid_long_step_edit = QLineEdit("0.005")
        self._grid_short_step_edit = QLineEdit("0.005")
        self._grid_cycle_combo = QComboBox()
        for item in GRID_CYCLE_OPTIONS:
            self._grid_cycle_combo.addItem(item, item)
        hint = QLabel(
            "网格模式开启后，点击右侧盘口的买量列或卖量列，会直接以对应价位创建独立网格任务。"
        )
        hint.setWordWrap(True)
        hint.setObjectName("Subtle")

        layout.addRow(self._grid_enabled)
        layout.addRow("下单数量", self._grid_size_edit)
        layout.addRow("多单步长", self._grid_long_step_edit)
        layout.addRow("空单步长", self._grid_short_step_edit)
        layout.addRow("循环次数", self._grid_cycle_combo)
        layout.addRow(hint)
        return panel

    def _build_position_limit_tab(self) -> QWidget:
        panel = QWidget()
        layout = QFormLayout(panel)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        self._position_limit_enabled = QCheckBox("启用总仓位限制")
        self._position_long_limit_edit = QLineEdit()
        self._position_short_limit_edit = QLineEdit()
        self._position_limit_state = QLabel("未启用总仓位限制。")
        self._position_limit_state.setWordWrap(True)
        self._position_limit_state.setObjectName("Subtle")
        submit_button = QPushButton("应用限制")
        submit_button.clicked.connect(self._apply_position_limits)

        layout.addRow(self._position_limit_enabled)
        layout.addRow("多头上限", self._position_long_limit_edit)
        layout.addRow("空头上限", self._position_short_limit_edit)
        layout.addRow(submit_button)
        layout.addRow(self._position_limit_state)
        return panel

    def _refresh_profiles(self) -> None:
        self._profile_snapshots, selected_profile = load_profile_snapshots()
        self._unlocked_profiles.intersection_update(set(self._profile_snapshots))
        current = self._selected_profile_name()
        self._profile_combo.blockSignals(True)
        self._profile_combo.clear()
        for name in self._profile_snapshots:
            self._profile_combo.addItem(name, name)
        self._profile_combo.blockSignals(False)
        if self._profile_combo.count() > 0:
            target = current or self._last_profile_name or selected_profile
            selected_index = self._profile_combo.findData(target)
            if selected_index < 0:
                selected_index = 0
            self._profile_combo.setCurrentIndex(selected_index)
        self._on_profile_changed()

    def _selected_profile_name(self) -> str:
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

    def _load_favorites(self) -> list[dict[str, str]]:
        snapshot = load_smart_order_favorites_snapshot()
        favorites = snapshot.get("favorites", []) if isinstance(snapshot, dict) else []
        normalized: list[dict[str, str]] = []
        for item in favorites:
            if not isinstance(item, dict):
                continue
            inst_id = str(item.get("inst_id", "")).strip().upper()
            inst_type = str(item.get("inst_type", "")).strip().upper()
            if not inst_id or not inst_type:
                continue
            normalized.append({"inst_id": inst_id, "inst_type": inst_type})
        return normalized

    def _save_favorites(self) -> None:
        save_smart_order_favorites_snapshot(self._favorites)

    def _selected_inst_type(self) -> str:
        return _combo_value(self._inst_type_combo) or "OPTION"

    def _quantity_hint_text(self) -> str:
        if self._instrument is not None and self._instrument.inst_type == "OPTION":
            contract_size, ccy = self._option_contract_coin_size(self._instrument)
            if contract_size is not None and contract_size > 0:
                return f"期权数量按币数输入，当前每张约 {format_decimal(contract_size)} {ccy or '币'}。"
        return "数量沿用交易所原始下单单位；若是期权，会自动按币数换算成张数。"

    def _option_contract_coin_size(self, instrument: Instrument) -> tuple[Decimal | None, str | None]:
        if instrument.inst_type != "OPTION":
            return None, None
        if instrument.ct_val is None or instrument.ct_val <= 0:
            return None, None
        multiplier = instrument.ct_mult if instrument.ct_mult is not None and instrument.ct_mult > 0 else Decimal("1")
        return instrument.ct_val * multiplier, instrument.ct_val_ccy

    def _convert_input_size_to_order_size(self, raw_value: str, field_name: str, instrument: Instrument) -> Decimal:
        size = self._parse_positive_decimal(raw_value, field_name)
        if instrument.inst_type != "OPTION":
            return size
        contract_size, contract_ccy = self._option_contract_coin_size(instrument)
        if contract_size is None or contract_size <= 0:
            return size
        order_size = snap_to_increment(size / contract_size, instrument.lot_size, "down")
        if order_size < instrument.min_size:
            raise RuntimeError(
                f"{field_name} {format_decimal(size)} 币换算后仅 {format_decimal(order_size)} 张，"
                f"小于最小下单量 {format_decimal(instrument.min_size)} 张。"
                f"当前每张约 {format_decimal(contract_size)} {contract_ccy or '币'}。"
            )
        return order_size

    def _convert_display_limit_to_internal(self, raw_value: str, field_name: str) -> Decimal | None:
        value = self._parse_optional_positive_decimal(raw_value, field_name)
        if value is None:
            return None
        if self._instrument is None or self._instrument.inst_type != "OPTION":
            return value
        contract_size, contract_ccy = self._option_contract_coin_size(self._instrument)
        if contract_size is None or contract_size <= 0:
            raise RuntimeError("请先加载期权合约，再设置按币数口径的仓位限制。")
        order_size = snap_to_increment(value / contract_size, self._instrument.lot_size, "down")
        if order_size < self._instrument.min_size:
            raise RuntimeError(
                f"{field_name} {format_decimal(value)} 币换算后仅 {format_decimal(order_size)} 张，"
                f"小于最小下单量 {format_decimal(self._instrument.min_size)} 张。"
                f"当前每张约 {format_decimal(contract_size)} {contract_ccy or '币'}。"
            )
        return order_size

    def _convert_internal_size_to_display(self, size: Decimal | None) -> str:
        if size is None:
            return ""
        if self._instrument is None or self._instrument.inst_type != "OPTION":
            return format_decimal(size)
        contract_size, _ccy = self._option_contract_coin_size(self._instrument)
        if contract_size is None or contract_size <= 0:
            return format_decimal(size)
        return format_decimal(size * contract_size)

    def _filtered_favorites(self) -> list[dict[str, str]]:
        selected_type = self._selected_inst_type()
        return [item for item in self._favorites if item.get("inst_type") == selected_type]

    def _refresh_favorite_options(self) -> None:
        current = str(self._favorite_combo.currentData() or "").strip()
        favorites = self._filtered_favorites()
        self._favorite_combo.blockSignals(True)
        self._favorite_combo.clear()
        for item in favorites:
            self._favorite_combo.addItem(item["inst_id"], item["inst_id"])
        self._favorite_combo.blockSignals(False)
        if favorites:
            wanted = current or (self._instrument.inst_id if self._instrument is not None else "")
            index = max(0, self._favorite_combo.findData(wanted))
            self._favorite_combo.setCurrentIndex(index)

    def _apply_instrument_to_form(self, instrument: Instrument) -> None:
        self._instrument = instrument
        self._last_ladder_anchor_price = ""
        self._inst_id_edit.setText(instrument.inst_id)
        _set_combo_value(self._inst_type_combo, instrument.inst_type)
        self._refresh_ladder_filter_options(instrument)
        self._instrument_summary.setText(
            f"{instrument.inst_id} | 类型={instrument.inst_type} | tick={format_decimal(instrument.tick_size)} | "
            f"lot={format_decimal(instrument.lot_size)} | min={format_decimal(instrument.min_size)}"
        )
        self._refresh_position_limit_inputs()
        self._refresh_favorite_options()

    def _refresh_ladder_filter_options(self, instrument: Instrument) -> None:
        candidates = [
            ("自动", ""),
            (format_decimal_by_increment(instrument.tick_size, instrument.tick_size), format_decimal(instrument.tick_size)),
            (format_decimal(instrument.tick_size * Decimal("10")), format_decimal(instrument.tick_size * Decimal("10"))),
            (format_decimal(instrument.tick_size * Decimal("100")), format_decimal(instrument.tick_size * Decimal("100"))),
        ]
        current = str(self._ladder_filter_combo.currentData() or "").strip()
        self._ladder_filter_combo.clear()
        seen: set[str] = set()
        for label, value in candidates:
            if value in seen:
                continue
            seen.add(value)
            self._ladder_filter_combo.addItem(label, value)
        if current:
            _set_combo_value(self._ladder_filter_combo, current)

    def _build_runtime_from_form(self) -> SmartOrderRuntimeConfig:
        self._profile_snapshots, _selected_profile = load_profile_snapshots()
        self._unlocked_profiles.intersection_update(set(self._profile_snapshots))
        if not self._profile_snapshots:
            raise RuntimeError("未找到 API 配置。")
        profile_name = self._selected_profile_name()
        if not self._ensure_profile_access(profile_name):
            raise RuntimeError(f"API Profile {profile_name or '-'} 尚未解锁。")
        payload = self._profile_snapshots.get(profile_name)
        if not isinstance(payload, dict):
            raise RuntimeError("当前 API Profile 无效。")
        api_key = str(payload.get("api_key", "")).strip()
        secret_key = str(payload.get("secret_key", "")).strip()
        passphrase = str(payload.get("passphrase", "")).strip()
        if not api_key or not secret_key or not passphrase:
            raise RuntimeError(f"{profile_name} 缺少完整 API 凭证。")
        return SmartOrderRuntimeConfig(
            credentials=Credentials(
                api_key=api_key,
                secret_key=secret_key,
                passphrase=passphrase,
                profile_name=profile_name,
            ),
            environment=_combo_value(self._environment_combo) or str(payload.get("environment", "demo") or "demo"),
            trade_mode=_combo_value(self._trade_mode_combo) or "cross",
            position_mode=_combo_value(self._position_mode_combo) or "net",
            credential_profile_name=profile_name,
        )
        snapshot = load_credentials_profiles_snapshot()
        profiles = snapshot.get("profiles", {})
        if not isinstance(profiles, dict):
            raise RuntimeError("未找到 API 配置。")
        profile_name = str(self._profile_combo.currentData() or "").strip()
        payload = profiles.get(profile_name)
        if not isinstance(payload, dict):
            raise RuntimeError("当前 API Profile 无效。")
        api_key = str(payload.get("api_key", "")).strip()
        secret_key = str(payload.get("secret_key", "")).strip()
        passphrase = str(payload.get("passphrase", "")).strip()
        if not api_key or not secret_key or not passphrase:
            raise RuntimeError(f"{profile_name} 缺少完整 API 凭证。")
        return SmartOrderRuntimeConfig(
            credentials=Credentials(
                api_key=api_key,
                secret_key=secret_key,
                passphrase=passphrase,
                profile_name=profile_name,
            ),
            environment=_combo_value(self._environment_combo) or str(payload.get("environment", "demo") or "demo"),
            trade_mode=_combo_value(self._trade_mode_combo) or "cross",
            position_mode=_combo_value(self._position_mode_combo) or "net",
            credential_profile_name=profile_name,
        )

    def _require_instrument(self) -> Instrument:
        if self._instrument is None:
            raise RuntimeError("请先加载一个合约。")
        return self._instrument

    def _runtime_summary_text(self) -> str:
        profile_name = str(self._profile_combo.currentData() or "").strip() or "-"
        environment = _combo_value(self._environment_combo) or "-"
        trade_mode = _combo_value(self._trade_mode_combo) or "-"
        position_mode = _combo_value(self._position_mode_combo) or "-"
        return (
            f"Profile={profile_name} | environment={environment} | trade_mode={trade_mode} | "
            f"position_mode={position_mode}"
        )

    def _set_status(self, text: str) -> None:
        self._status.setText(text)

    def _show_error(self, title: str, message: str) -> None:
        self._set_status(message)
        QMessageBox.critical(self, title, message)

    def _show_info(self, title: str, message: str) -> None:
        self._set_status(message)
        QMessageBox.information(self, title, message)

    def _parse_positive_decimal(self, raw: str, field_name: str) -> Decimal:
        try:
            value = Decimal(raw.strip())
        except (InvalidOperation, AttributeError) as exc:
            raise RuntimeError(f"{field_name} 不是有效数字。") from exc
        if value <= 0:
            raise RuntimeError(f"{field_name} 必须大于 0。")
        return value

    def _parse_optional_positive_decimal(self, raw: str, field_name: str) -> Decimal | None:
        text = str(raw or "").strip()
        if not text:
            return None
        return self._parse_positive_decimal(text, field_name)

    def _parse_side(self, combo: QComboBox) -> Literal["buy", "sell"]:
        side = _combo_value(combo)
        if side not in {"buy", "sell"}:
            raise RuntimeError("方向只能是 buy 或 sell。")
        return side

    def _parse_cycle_mode(self) -> tuple[str, int | None]:
        raw = str(self._grid_cycle_combo.currentData() or "连续")
        if raw == "连续":
            return "continuous", None
        return "counted", int(raw)

    def _manual_pos_side(
        self,
        instrument: Instrument,
        runtime: SmartOrderRuntimeConfig,
        side: Literal["buy", "sell"],
    ) -> str | None:
        if instrument.inst_type in {"SPOT", "OPTION"} or runtime.position_mode != "long_short":
            return None
        return "long" if side == "buy" else "short"

    def _resolve_trigger_source(self, combo: QComboBox, custom_inst_edit: QLineEdit) -> tuple[str, str]:
        payload = combo.currentData()
        if not isinstance(payload, tuple) or len(payload) != 2:
            raise RuntimeError("触发源配置无效。")
        mode, price_type = payload
        if mode == "current":
            return self._require_instrument().inst_id, price_type
        inst_id = custom_inst_edit.text().strip().upper()
        if not inst_id:
            raise RuntimeError("自定义触发模式下，请填写自定义标的。")
        return inst_id, price_type

    def _on_profile_changed(self) -> None:
        selected = self._selected_profile_name()
        if not selected:
            self._runtime_summary.setText(self._runtime_summary_text())
            return
        self._profile_snapshots, _selected_profile = load_profile_snapshots()
        self._unlocked_profiles.intersection_update(set(self._profile_snapshots))
        if selected != self._last_profile_name and not self._ensure_profile_access(selected):
            self._restore_profile_selection()
            return
        payload = self._profile_snapshots.get(selected)
        if isinstance(payload, dict):
            _set_combo_value(self._environment_combo, str(payload.get("environment", "demo") or "demo"))
        self._last_profile_name = selected
        self._runtime_summary.setText(self._runtime_summary_text())
        return
        snapshot = load_credentials_profiles_snapshot()
        profiles = snapshot.get("profiles", {})
        selected = str(self._profile_combo.currentData() or "").strip()
        payload = profiles.get(selected) if isinstance(profiles, dict) else None
        if isinstance(payload, dict):
            _set_combo_value(self._environment_combo, str(payload.get("environment", "demo") or "demo"))
        self._runtime_summary.setText(self._runtime_summary_text())

    @Slot()
    def _unlock_contract_if_idle(self) -> None:
        try:
            self._manager.unlock_contract_if_idle()
        except Exception as exc:  # noqa: BLE001
            self._show_error("解锁失败", str(exc))
            return
        self._instrument = None
        self._instrument_summary.setText("合约已解锁。")
        self.refresh_view()

    @Slot()
    def _on_favorite_selected(self) -> None:
        inst_id = str(self._favorite_combo.currentData() or "").strip().upper()
        if inst_id:
            self._inst_id_edit.setText(inst_id)

    @Slot()
    def _add_favorite(self) -> None:
        inst_id = self._inst_id_edit.text().strip().upper()
        if not inst_id:
            self._show_error("收藏失败", "请先填写合约。")
            return
        try:
            instrument = self._client.get_instrument(inst_id)
        except Exception as exc:  # noqa: BLE001
            self._show_error("收藏失败", str(exc))
            return
        key = (instrument.inst_type, instrument.inst_id)
        if any((item["inst_type"], item["inst_id"]) == key for item in self._favorites):
            self._refresh_favorite_options()
            self._show_info("提示", f"{instrument.inst_id} 已在收藏列表。")
            return
        self._favorites.append({"inst_id": instrument.inst_id, "inst_type": instrument.inst_type})
        self._save_favorites()
        self._refresh_favorite_options()
        self._show_info("提示", f"已加入收藏：{instrument.inst_id}")

    @Slot()
    def _remove_favorite(self) -> None:
        inst_id = str(self._favorite_combo.currentData() or "").strip().upper() or self._inst_id_edit.text().strip().upper()
        selected_type = self._selected_inst_type()
        before = len(self._favorites)
        self._favorites = [
            item for item in self._favorites if not (item["inst_type"] == selected_type and item["inst_id"] == inst_id)
        ]
        if len(self._favorites) == before:
            self._show_info("提示", "当前类型下没有这个收藏。")
            return
        self._save_favorites()
        self._refresh_favorite_options()
        self._show_info("提示", f"已移除收藏：{inst_id}")

    @Slot()
    def _load_instrument(self) -> None:
        inst_id = self._inst_id_edit.text().strip().upper()
        if not inst_id:
            self._show_error("加载失败", "请先填写合约。")
            return
        expected_type = self._selected_inst_type()
        try:
            instrument = self._client.get_instrument(inst_id)
            if instrument.inst_type != expected_type:
                raise RuntimeError(f"{inst_id} 实际类型是 {instrument.inst_type}，和当前选择不一致。")
            self._manager.set_contract(instrument)
            self._manager.ensure_market_snapshot(instrument, force=True)
        except Exception as exc:  # noqa: BLE001
            self._show_error("加载失败", str(exc))
            return
        self._apply_instrument_to_form(instrument)
        self._set_status(f"已加载合约：{instrument.inst_id}")
        self.refresh_view()

    @Slot()
    def _submit_manual_order(self) -> None:
        try:
            instrument = self._require_instrument()
            runtime = self._build_runtime_from_form()
            side = self._parse_side(self._manual_side_combo)
            size = self._convert_input_size_to_order_size(self._manual_size_edit.text(), "数量", instrument)
            self._manager.validate_opening_capacity(instrument=instrument, runtime=runtime, side=side, size=size)
            order_type = _combo_value(self._manual_order_type_combo)
            config = self._manager._build_config(instrument.inst_id, runtime)
            if order_type == "limit":
                price = self._parse_positive_decimal(self._manual_price_edit.text(), "价格")
                self._client.place_simple_order(
                    runtime.credentials,
                    config,
                    inst_id=instrument.inst_id,
                    side=side,
                    size=size,
                    ord_type="limit",
                    pos_side=self._manual_pos_side(instrument, runtime, side),
                    price=price,
                )
            elif order_type == "best_quote":
                ticker, order_book = self._manager.ensure_market_snapshot(instrument, force=True)
                price = resolve_best_quote_price(
                    side=side,
                    ticker=ticker,
                    order_book=order_book,
                    tick_size=instrument.tick_size,
                )
                self._manual_price_edit.setText(format_decimal_by_increment(price, instrument.tick_size))
                self._client.place_simple_order(
                    runtime.credentials,
                    config,
                    inst_id=instrument.inst_id,
                    side=side,
                    size=size,
                    ord_type="limit",
                    pos_side=self._manual_pos_side(instrument, runtime, side),
                    price=price,
                )
            elif order_type == "aggressive_ioc":
                self._client.place_aggressive_limit_order(
                    runtime.credentials,
                    config,
                    instrument,
                    side=side,
                    size=size,
                    pos_side=self._manual_pos_side(instrument, runtime, side),
                )
            else:
                price = self._parse_positive_decimal(self._manual_price_edit.text(), "价格")
                self._client.place_simple_order(
                    runtime.credentials,
                    config,
                    inst_id=instrument.inst_id,
                    side=side,
                    size=size,
                    ord_type=order_type,
                    pos_side=self._manual_pos_side(instrument, runtime, side),
                    price=price,
                )
        except Exception as exc:  # noqa: BLE001
            self._show_error("下单失败", str(exc))
            return
        self._show_info("提示", "委托已提交。")
        self.refresh_view()

    @Slot()
    def _start_condition_task(self) -> None:
        try:
            instrument = self._require_instrument()
            runtime = self._build_runtime_from_form()
            trigger_inst_id, trigger_price_type = self._resolve_trigger_source(
                self._condition_trigger_source_combo,
                self._condition_custom_inst_edit,
            )
            task_id = self._manager.start_condition_task(
                instrument=instrument,
                runtime=runtime,
                side=self._parse_side(self._condition_side_combo),
                size=self._convert_input_size_to_order_size(self._condition_size_edit.text(), "数量", instrument),
                trigger_inst_id=trigger_inst_id,
                trigger_price_type=trigger_price_type,  # type: ignore[arg-type]
                trigger_direction=self._condition_direction_combo.currentData(),  # type: ignore[arg-type]
                trigger_price=self._parse_positive_decimal(self._condition_trigger_price_edit.text(), "触发价"),
                exec_mode=self._condition_exec_mode_combo.currentData(),  # type: ignore[arg-type]
                exec_price=self._parse_optional_positive_decimal(self._condition_exec_price_edit.text(), "执行价"),
                take_profit=self._parse_optional_positive_decimal(self._condition_tp_edit.text(), "止盈触发"),
                stop_loss=self._parse_optional_positive_decimal(self._condition_sl_edit.text(), "止损触发"),
            )
        except Exception as exc:  # noqa: BLE001
            self._show_error("创建条件单失败", str(exc))
            return
        self._show_info("提示", f"条件单任务 {task_id} 已创建。")
        self.refresh_view()

    @Slot()
    def _start_tp_sl_task(self) -> None:
        try:
            instrument = self._require_instrument()
            runtime = self._build_runtime_from_form()
            trigger_inst_id, trigger_price_type = self._resolve_trigger_source(
                self._tp_sl_trigger_source_combo,
                self._tp_sl_custom_inst_edit,
            )
            task_id = self._manager.start_tp_sl_task(
                instrument=instrument,
                runtime=runtime,
                position_side=self._tp_sl_side_combo.currentData(),  # type: ignore[arg-type]
                size=self._convert_input_size_to_order_size(self._tp_sl_size_edit.text(), "保护数量", instrument),
                trigger_inst_id=trigger_inst_id,
                trigger_price_type=trigger_price_type,  # type: ignore[arg-type]
                take_profit=self._parse_optional_positive_decimal(self._tp_sl_tp_edit.text(), "止盈触发"),
                stop_loss=self._parse_optional_positive_decimal(self._tp_sl_sl_edit.text(), "止损触发"),
            )
        except Exception as exc:  # noqa: BLE001
            self._show_error("创建止盈止损失败", str(exc))
            return
        self._show_info("提示", f"止盈止损任务 {task_id} 已创建。")
        self.refresh_view()

    @Slot()
    def _apply_position_limits(self) -> None:
        try:
            enabled = self._position_limit_enabled.isChecked()
            long_limit = self._convert_display_limit_to_internal(self._position_long_limit_edit.text(), "多头上限")
            short_limit = self._convert_display_limit_to_internal(self._position_short_limit_edit.text(), "空头上限")
            if enabled and long_limit is None and short_limit is None:
                raise RuntimeError("启用仓位限制后，至少需要填写一个上限。")
            self._manager.set_position_limits(enabled=enabled, long_limit=long_limit, short_limit=short_limit)
        except Exception as exc:  # noqa: BLE001
            self._show_error("应用限制失败", str(exc))
            return
        self._refresh_position_limit_inputs()
        self._show_info("提示", "仓位限制已更新。")
        self.refresh_view()

    def _refresh_position_limit_inputs(self) -> None:
        enabled, long_limit, short_limit = self._manager.get_position_limit_config()
        self._position_limit_enabled.setChecked(enabled)
        self._position_long_limit_edit.setText(self._convert_internal_size_to_display(long_limit))
        self._position_short_limit_edit.setText(self._convert_internal_size_to_display(short_limit))

    @Slot(int, int)
    def _on_ladder_cell_clicked(self, row: int, column: int) -> None:
        price_item = self._ladder_table.item(row, 1)
        if price_item is None:
            return
        raw_price = price_item.data(Qt.ItemDataRole.UserRole)
        try:
            price = Decimal(str(raw_price))
        except (InvalidOperation, TypeError):
            return
        self._manual_price_edit.setText(str(price_item.text()))
        self._condition_exec_price_edit.setText(str(price_item.text()))
        if not self._grid_enabled.isChecked() or column not in {0, 2}:
            self._set_status(f"已带入价格：{price_item.text()}")
            return
        try:
            instrument = self._require_instrument()
            runtime = self._build_runtime_from_form()
            cycle_mode, cycle_limit = self._parse_cycle_mode()
            task_id = self._manager.start_grid_task(
                instrument=instrument,
                runtime=runtime,
                side="buy" if column == 0 else "sell",
                entry_price=price,
                size=self._convert_input_size_to_order_size(self._grid_size_edit.text(), "网格数量", instrument),
                long_step=self._parse_positive_decimal(self._grid_long_step_edit.text(), "多单步长"),
                short_step=self._parse_positive_decimal(self._grid_short_step_edit.text(), "空单步长"),
                cycle_mode=cycle_mode,  # type: ignore[arg-type]
                cycle_limit=cycle_limit,
            )
        except Exception as exc:  # noqa: BLE001
            self._show_error("创建网格失败", str(exc))
            return
        self._show_info("提示", f"网格任务 {task_id} 已创建。")
        self.refresh_view()

    def _selected_task_id_from_table(self) -> str:
        row = self._task_table.currentRow()
        if row < 0:
            return ""
        item = self._task_table.item(row, 0)
        return str(item.data(Qt.ItemDataRole.UserRole) or item.text() or "").strip() if item is not None else ""

    @Slot()
    def _restart_selected_task(self) -> None:
        task_id = self._selected_task_id_from_table()
        if not task_id:
            self._show_info("提示", "请先选择一个任务。")
            return
        try:
            self._manager.restart_task(task_id, self._build_runtime_from_form())
        except Exception as exc:  # noqa: BLE001
            self._show_error("重启失败", str(exc))
            return
        self._show_info("提示", f"任务 {task_id} 已重启。")
        self.refresh_view()

    @Slot()
    def _stop_selected_task(self) -> None:
        task_id = self._selected_task_id_from_table()
        if not task_id:
            self._show_info("提示", "请先选择一个任务。")
            return
        try:
            self._manager.stop_task(task_id, self._build_runtime_from_form())
        except Exception as exc:  # noqa: BLE001
            self._show_error("停止失败", str(exc))
            return
        self._set_status(f"任务 {task_id} 已请求停止。")
        self.refresh_view()

    @Slot()
    def _stop_all_tasks(self) -> None:
        try:
            self._manager.stop_all(self._build_runtime_from_form())
        except Exception as exc:  # noqa: BLE001
            self._show_error("停止失败", str(exc))
            return
        self._set_status("已请求停止全部任务。")
        self.refresh_view()

    @Slot()
    def _remove_selected_task(self) -> None:
        task_id = self._selected_task_id_from_table()
        if not task_id:
            self._show_info("提示", "请先选择一个任务。")
            return
        try:
            self._manager.remove_task(task_id)
        except Exception as exc:  # noqa: BLE001
            self._show_error("删除失败", str(exc))
            return
        self._selected_task_id = ""
        self._set_status(f"任务 {task_id} 已删除。")
        self.refresh_view()

    @Slot()
    def _sync_task_detail(self) -> None:
        self._selected_task_id = self._selected_task_id_from_table()
        if not self._selected_task_id:
            self._task_detail.setPlainText(DEFAULT_TASK_HINT)
            return
        tasks = self._manager.list_tasks()
        task = next((item for item in tasks if item.task_id == self._selected_task_id), None)
        if task is None:
            self._task_detail.setPlainText(DEFAULT_TASK_HINT)
            return
        payload = {
            "task_id": task.task_id,
            "task_type": task.task_type,
            "inst_id": task.inst_id,
            "trigger_label": task.trigger_label,
            "side": task.side,
            "status": task.status,
            "started_at": task.started_at.isoformat(timespec="seconds"),
            "active_order_price": _safe_text(task.active_order_price),
            "active_order_size": _safe_text(task.active_order_size),
            "completed_cycles": task.completed_cycles,
            "cycle_limit_label": task.cycle_limit_label,
            "last_message": task.last_message,
        }
        self._task_detail.setPlainText(json.dumps(payload, ensure_ascii=False, indent=2))

    @Slot()
    def refresh_view(self) -> None:
        self._runtime_summary.setText(self._runtime_summary_text())
        self._task_metric.setText(f"任务数：{len(self._manager.list_tasks())}")
        self._favorite_metric.setText(f"收藏合约：{len(self._favorites)}")
        self._locked_metric.setText(f"锁定合约：{_safe_text(self._manager.locked_inst_id)}")
        self._refresh_position_limit_state()
        self._populate_tasks()
        self._populate_logs()
        self._populate_ladder()

    def _refresh_position_limit_state(self) -> None:
        self._refresh_position_limit_inputs()
        if self._instrument is None:
            self._position_metric.setText("仓位限制：请先加载合约")
            self._position_limit_state.setText("未加载合约，无法计算当前占用。")
            return
        try:
            runtime = self._build_runtime_from_form()
            state = self._manager.get_position_limit_state(self._instrument, runtime)
        except Exception as exc:  # noqa: BLE001
            self._position_metric.setText("仓位限制：读取失败")
            self._position_limit_state.setText(f"读取失败：{exc}")
            return
        self._position_metric.setText(
            f"多占用：{format_decimal(state.used_long)} | 空占用：{format_decimal(state.used_short)}"
        )
        if not state.enabled:
            self._position_limit_state.setText(
                f"当前未启用总仓位限制。实际多={format_decimal(state.actual_long)}，实际空={format_decimal(state.actual_short)}。"
            )
            return
        self._position_limit_state.setText(
            f"多头：实际 {format_decimal(state.actual_long)} + 预留 {format_decimal(state.reserved_long)}，"
            f"上限 {_safe_text(state.long_limit)}，可用 {_safe_text(state.available_long)}\n"
            f"空头：实际 {format_decimal(state.actual_short)} + 预留 {format_decimal(state.reserved_short)}，"
            f"上限 {_safe_text(state.short_limit)}，可用 {_safe_text(state.available_short)}"
        )

    def _populate_tasks(self) -> None:
        tasks = self._manager.list_tasks()
        self._task_table.setRowCount(len(tasks))
        selected_row = -1
        for row, task in enumerate(tasks):
            active_price = format_decimal(task.active_order_price) if task.active_order_price is not None else "-"
            active_size = format_decimal(task.active_order_size) if task.active_order_size is not None else "-"
            cells = (
                task.task_id,
                task.task_type,
                task.status,
                task.side,
                active_size,
                active_price,
                f"{task.completed_cycles}/{task.cycle_limit_label}",
                task.last_message or task.trigger_label,
            )
            for column, value in enumerate(cells):
                item = QTableWidgetItem(value)
                if column == 0:
                    item.setData(Qt.ItemDataRole.UserRole, task.task_id)
                self._task_table.setItem(row, column, item)
            if task.task_id == self._selected_task_id:
                selected_row = row
        if selected_row >= 0:
            self._task_table.selectRow(selected_row)
        elif tasks:
            self._task_table.selectRow(0)
        else:
            self._task_detail.setPlainText(DEFAULT_TASK_HINT)

    def _populate_logs(self) -> None:
        self._log_text.setPlainText("\n".join(self._manager.list_logs()))
        cursor = self._log_text.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        self._log_text.setTextCursor(cursor)

    def _ladder_price_increment(self) -> Decimal | None:
        if self._instrument is None:
            return None
        raw = str(self._ladder_filter_combo.currentData() or "").strip()
        if not raw:
            return None
        try:
            value = Decimal(raw)
        except InvalidOperation:
            return self._instrument.tick_size
        return max(value, self._instrument.tick_size)

    def _populate_ladder(self) -> None:
        if self._instrument is None:
            self._ladder_table.setRowCount(0)
            return
        try:
            ticker, order_book = self._manager.ensure_market_snapshot(self._instrument, force=False)
            levels = self._manager.build_ladder(
                self._instrument,
                levels_each_side=18,
                price_increment=self._ladder_price_increment(),
            )
        except Exception as exc:  # noqa: BLE001
            self._set_status(f"盘口刷新失败：{exc}")
            self._ladder_table.setRowCount(0)
            return
        bid1 = order_book.bids[0][0] if order_book is not None and order_book.bids else ticker.bid
        ask1 = order_book.asks[0][0] if order_book is not None and order_book.asks else ticker.ask
        latest = ticker.last or ticker.mark or ticker.index
        self._instrument_summary.setText(
            f"{self._instrument.inst_id} | 最新="
            f"{format_decimal_by_increment(latest, self._instrument.tick_size) if latest is not None else '-'}"
            f" | 买一={format_decimal_by_increment(bid1, self._instrument.tick_size) if bid1 is not None else '-'}"
            f" | 卖一={format_decimal_by_increment(ask1, self._instrument.tick_size) if ask1 is not None else '-'}"
            f" | tick={format_decimal(self._instrument.tick_size)} | 最小下单量={format_decimal(self._instrument.min_size)}"
        )
        self._ladder_table.setRowCount(len(levels))
        anchor_row = -1
        for row, level in enumerate(levels):
            tags: list[QTableWidgetItem] = []
            marker_labels: list[str] = []
            if level.is_best_ask:
                marker_labels.append("卖一")
            if level.is_last_price:
                marker_labels.append("最新价")
            if level.is_best_bid:
                marker_labels.append("买一")
            cells = (
                format_decimal(level.buy_working) if level.buy_working is not None else "-",
                format_decimal_by_increment(level.price, self._instrument.tick_size),
                format_decimal(level.sell_working) if level.sell_working is not None else "-",
                " | ".join([*marker_labels, *level.working_labels]),
            )
            for column, value in enumerate(cells):
                item = QTableWidgetItem(value)
                if column == 1:
                    item.setData(Qt.ItemDataRole.UserRole, str(level.price))
                tags.append(item)
                self._ladder_table.setItem(row, column, item)
            color = None
            if level.is_last_price:
                color = Qt.GlobalColor.yellow
            if level.is_best_bid:
                color = Qt.GlobalColor.green
            if level.is_best_ask:
                color = Qt.GlobalColor.red
            if color is not None:
                for item in tags:
                    item.setBackground(color)
            if anchor_row < 0 and (level.is_last_price or level.is_best_bid or level.is_best_ask):
                anchor_row = row
        if anchor_row >= 0:
            price_item = self._ladder_table.item(anchor_row, 1)
            if price_item is not None:
                anchor_price = str(price_item.data(Qt.ItemDataRole.UserRole) or "").strip()
                if anchor_price and anchor_price != self._last_ladder_anchor_price:
                    self._last_ladder_anchor_price = anchor_price
                    self._ladder_table.scrollToItem(
                        price_item,
                        QAbstractItemView.ScrollHint.PositionAtCenter,
                    )
