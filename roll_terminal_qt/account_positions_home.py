from __future__ import annotations

import json
import re
import time
from datetime import datetime, timedelta
from decimal import Decimal
from tkinter import Tk
from typing import Callable

from PySide6.QtCore import QTimer, Qt, Slot
from PySide6.QtGui import QColor, QFont
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QSplitter,
    QTabWidget,
    QTextEdit,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from okx_quant.models import StrategyConfig
from okx_quant.option_roll import is_short_option_position
from okx_quant.option_roll_ui import OptionRollSuggestionWindow
from okx_quant.option_strategy_ui import OptionStrategyCalculatorWindow, _build_option_quote
from okx_quant.okx_client import (
    OkxFillHistoryItem,
    OkxPosition,
    OkxPositionHistoryItem,
    OkxRestClient,
    OkxTradeOrderItem,
)
from okx_quant.persistence import (
    load_account_positions_home_view_prefs,
    load_position_notes_snapshot,
    save_account_positions_home_view_prefs,
    save_position_notes_snapshot,
)
from okx_quant.position_protection import (
    OptionProtectionConfig,
    PositionProtectionManager,
    ProtectionSessionSnapshot,
    derive_position_direction,
    describe_protection_price_logic,
    infer_default_spot_inst_id,
    normalize_spot_inst_id,
)
from okx_quant.ui_shell import (
    _aggregate_position_metrics,
    _asset_group_row_id,
    _build_current_position_note_record,
    _build_group_detail_text,
    _build_group_row_values,
    _build_position_detail_text,
    _bucket_group_row_id,
    _filter_positions,
    _format_margin_mode,
    _format_optional_approx_usdt,
    _format_optional_decimal,
    _format_optional_decimal_fixed,
    _format_optional_integer,
    _format_optional_usdt,
    _format_optional_usdt_precise,
    _format_position_avg_price,
    _format_position_avg_price_usdt,
    _format_position_market_value,
    _format_position_note_summary,
    _format_position_option_component_usdt,
    _format_position_option_price_component,
    _format_position_quote_price,
    _format_position_quote_price_usdt,
    _format_position_realized_pnl,
    _format_position_size,
    _format_position_unrealized_pnl,
    _format_ratio,
    _format_mark_price,
    _format_position_mark_price_usdt,
    _format_option_trade_side_display,
    _format_okx_ms_timestamp,
    _group_positions_for_tree,
    _format_history_side,
    _normalize_position_note_text,
    _option_search_shortcuts,
    _format_trade_order_price,
    _format_trade_order_size,
    _format_trade_order_state,
    _format_trade_order_fee_cell,
    _build_trade_order_detail_text,
    _build_fill_history_detail_text,
    _build_history_position_note_record,
    _format_fill_history_exec_type,
    _format_fill_history_fee_cell,
    _format_fill_history_pnl,
    _format_fill_history_price,
    _format_fill_history_size,
    _format_position_history_fee_cell,
    _format_position_history_pnl,
    _format_position_history_filter_stats,
    _format_position_history_price,
    _format_position_history_size,
    _format_position_history_trade_side,
    _build_position_history_detail_text,
    _position_history_note_key,
    _position_history_note_summary_text,
    _position_delta_value,
    _position_note_current_key,
    _position_realized_pnl_usdt,
    _position_signed_open_value_approx_usdt,
    _position_theta_usdt,
    _position_tree_row_id,
    _position_unrealized_pnl_usdt,
    _reconcile_current_position_note_records,
    _format_protection_order_mode_label,
    _format_protection_order_price_detail,
    _format_protection_trigger_price_type,
    _resolve_protection_order_mode_value,
    _validate_protection_live_price_availability,
    _validate_protection_price_relationship,
    PROTECTION_ORDER_MODE_OPTIONS,
    PROTECTION_TRIGGER_SOURCE_OPTIONS,
)
from roll_terminal_qt.account_service import AccountFeedThread
from roll_terminal_qt.history_service import FillHistoryFeedThread, OrderHistoryFeedThread, PositionHistoryFeedThread
from roll_terminal_qt.order_service import OrderFeedThread, OrderStatusView
from roll_terminal_qt.profile_access import ensure_profile_unlocked, load_profile_snapshots, profile_requires_password
from roll_terminal_qt.runtime import load_runtime, profile_names


POSITION_TYPE_OPTIONS: tuple[tuple[str, str], ...] = (
    ("全部类型", ""),
    ("交割合约 FUTURES", "FUTURES"),
    ("永续 SWAP", "SWAP"),
    ("期权 OPTION", "OPTION"),
)

POSITION_COLUMNS: tuple[tuple[str, str, int, Qt.AlignmentFlag], ...] = (
    ("inst_type", "类型", 72, Qt.AlignmentFlag.AlignCenter),
    ("mgn_mode", "保证金模式", 92, Qt.AlignmentFlag.AlignCenter),
    ("time_value", "时间价值", 88, Qt.AlignmentFlag.AlignRight),
    ("time_value_usdt", "时间≈USDT", 72, Qt.AlignmentFlag.AlignRight),
    ("intrinsic_value", "内在价值", 88, Qt.AlignmentFlag.AlignRight),
    ("intrinsic_usdt", "内在≈USDT", 72, Qt.AlignmentFlag.AlignRight),
    ("bid_price", "买一价", 78, Qt.AlignmentFlag.AlignRight),
    ("bid_usdt", "买一≈USDT", 78, Qt.AlignmentFlag.AlignRight),
    ("ask_price", "卖一价", 78, Qt.AlignmentFlag.AlignRight),
    ("ask_usdt", "卖一≈USDT", 78, Qt.AlignmentFlag.AlignRight),
    ("mark", "标记价", 84, Qt.AlignmentFlag.AlignRight),
    ("mark_usdt", "标记≈USDT", 72, Qt.AlignmentFlag.AlignRight),
    ("avg", "开仓价", 84, Qt.AlignmentFlag.AlignRight),
    ("avg_usdt", "开仓≈USDT", 72, Qt.AlignmentFlag.AlignRight),
    ("open_value_usdt", "开仓价值≈USDT", 116, Qt.AlignmentFlag.AlignRight),
    ("pos", "持仓量", 170, Qt.AlignmentFlag.AlignRight),
    ("option_side", "买购:卖购 | 买沽:卖沽", 170, Qt.AlignmentFlag.AlignCenter),
    ("upl", "浮盈亏", 168, Qt.AlignmentFlag.AlignRight),
    ("upl_usdt", "浮盈≈USDT", 108, Qt.AlignmentFlag.AlignRight),
    ("realized", "已实现盈亏", 118, Qt.AlignmentFlag.AlignRight),
    ("realized_usdt", "已实现≈USDT", 108, Qt.AlignmentFlag.AlignRight),
    ("market_value", "市值", 160, Qt.AlignmentFlag.AlignRight),
    ("liq", "强平价", 92, Qt.AlignmentFlag.AlignRight),
    ("mgn_ratio", "保证金率", 88, Qt.AlignmentFlag.AlignRight),
    ("imr", "初始保证金", 100, Qt.AlignmentFlag.AlignRight),
    ("mmr", "维持保证金", 100, Qt.AlignmentFlag.AlignRight),
    ("delta", "Delta(PA)", 82, Qt.AlignmentFlag.AlignRight),
    ("gamma", "Gamma(PA)", 82, Qt.AlignmentFlag.AlignRight),
    ("vega", "Vega(PA)", 82, Qt.AlignmentFlag.AlignRight),
    ("theta", "Theta(PA)", 108, Qt.AlignmentFlag.AlignRight),
    ("theta_usdt", "Theta≈USDT", 108, Qt.AlignmentFlag.AlignRight),
    ("note", "备注", 200, Qt.AlignmentFlag.AlignLeft),
)

DEFAULT_VISIBLE_COLUMNS: tuple[str, ...] = (
    "inst_type",
    "mgn_mode",
    "mark",
    "mark_usdt",
    "avg",
    "avg_usdt",
    "open_value_usdt",
    "pos",
    "option_side",
    "upl",
    "upl_usdt",
    "realized",
    "market_value",
    "liq",
    "mgn_ratio",
    "imr",
    "mmr",
    "delta",
    "gamma",
    "vega",
    "theta",
    "theta_usdt",
    "note",
)

ORDER_SOURCE_FILTER_OPTIONS: tuple[tuple[str, str], ...] = (
    ("全部来源", ""),
    ("普通委托", "normal"),
    ("算法委托", "algo"),
    ("WS 当前", "ws"),
)

ORDER_STATE_FILTER_OPTIONS: tuple[tuple[str, str], ...] = (
    ("全部状态", ""),
    ("等待中", "live"),
    ("部分成交", "partially_filled"),
    ("已成交", "filled"),
    ("已撤单", "canceled"),
    ("失败", "order_failed"),
)

HISTORY_FILL_SIDE_FILTER_OPTIONS: tuple[tuple[str, str], ...] = (
    ("全部方向", ""),
    ("买入", "buy"),
    ("卖出", "sell"),
    ("多头", "long"),
    ("空头", "short"),
)

HISTORY_MARGIN_MODE_FILTER_OPTIONS: tuple[tuple[str, str], ...] = (
    ("全部模式", ""),
    ("全仓", "cross"),
    ("逐仓", "isolated"),
    ("现金", "cash"),
)


def _history_expiry_filter_matches(inst_id: str, expiry_filter: str) -> bool:
    text = expiry_filter.strip().upper().strip("-")
    if not text:
        return True
    normalized = inst_id.strip().upper().strip("-")
    if normalized.startswith(text):
        return True
    parts = normalized.split("-")
    if len(parts) >= 3 and re.fullmatch(r"\d{6,8}", parts[2] or ""):
        expiry = parts[2]
        family_prefix = f"{parts[0]}-{parts[1]}-{expiry}"
        return expiry.startswith(text) or family_prefix.startswith(text)
    return False


class NoteEditorDialog(QDialog):
    def __init__(self, *, title: str, prompt: str, initial_value: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._result_text: str | None = None
        self.setWindowTitle(title)
        self.resize(520, 320)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(10)
        layout.addWidget(QLabel(prompt))

        self._editor = QTextEdit()
        self._editor.setPlainText(initial_value)
        layout.addWidget(self._editor, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self._accept)
        buttons.rejected.connect(self.reject)
        buttons.button(QDialogButtonBox.StandardButton.Ok).setText("保存")
        buttons.button(QDialogButtonBox.StandardButton.Cancel).setText("取消")
        layout.addWidget(buttons)

    @property
    def result_text(self) -> str | None:
        return self._result_text

    def _accept(self) -> None:
        self._result_text = _normalize_position_note_text(self._editor.toPlainText())
        self.accept()


class AccountOverviewDialog(QDialog):
    def __init__(self, *, summary_text: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("账户信息")
        self.resize(760, 520)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(12)
        title = QLabel("账户持仓概览")
        title.setObjectName("SectionTitle")
        layout.addWidget(title)

        detail = QTextEdit()
        detail.setReadOnly(True)
        detail.setPlainText(summary_text)
        layout.addWidget(detail, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        buttons.rejected.connect(self.reject)
        buttons.button(QDialogButtonBox.StandardButton.Close).setText("关闭")
        layout.addWidget(buttons)


class ColumnSettingsDialog(QDialog):
    def __init__(
        self,
        *,
        column_defs: tuple[tuple[str, str, int, Qt.AlignmentFlag], ...],
        visible_columns: set[str],
        toggle_callback,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("持仓大窗列设置")
        self.resize(560, 520)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(10)
        tip = QLabel("可按区域勾选显示/隐藏列。`合约 / 分组` 为结构列，当前固定显示。")
        tip.setObjectName("Subtle")
        tip.setWordWrap(True)
        layout.addWidget(tip)

        checks = QGridLayout()
        checks.setHorizontalSpacing(16)
        checks.setVerticalSpacing(8)
        for index, (column_id, heading, _width, _alignment) in enumerate(column_defs):
            checkbox = QCheckBox(heading)
            checkbox.setChecked(column_id in visible_columns)
            checkbox.stateChanged.connect(lambda _state, cid=column_id: toggle_callback(cid))
            checks.addWidget(checkbox, index // 2, index % 2)
        wrapper = QWidget()
        wrapper.setLayout(checks)
        layout.addWidget(wrapper, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        buttons.rejected.connect(self.reject)
        buttons.button(QDialogButtonBox.StandardButton.Close).setText("关闭")
        layout.addWidget(buttons)


class LegacyOptionToolsHost:
    def __init__(
        self,
        *,
        parent: QWidget,
        runtime_provider: Callable[[], object | None],
    ) -> None:
        self._parent = parent
        self._runtime_provider = runtime_provider
        self._client = OkxRestClient()
        self._root: Tk | None = None
        self._pump_timer: QTimer | None = None
        self._option_roll_window: OptionRollSuggestionWindow | None = None
        self._option_strategy_window: OptionStrategyCalculatorWindow | None = None

    def shutdown(self) -> None:
        if self._pump_timer is not None:
            self._pump_timer.stop()
            self._pump_timer.deleteLater()
            self._pump_timer = None
        if self._option_roll_window is not None:
            try:
                self._option_roll_window.destroy()
            except Exception:
                pass
            self._option_roll_window = None
        if self._option_strategy_window is not None:
            try:
                self._option_strategy_window.destroy()
            except Exception:
                pass
            self._option_strategy_window = None
        if self._root is not None:
            try:
                self._root.destroy()
            except Exception:
                pass
            self._root = None

    def open_option_roll(
        self,
        *,
        position: OkxPosition,
        instrument: object,
        ticker: object,
        api_name: str,
    ) -> None:
        root = self._ensure_root()
        if root is None:
            raise RuntimeError("Tk 桥接窗口初始化失败。")
        quote = _build_option_quote(instrument, ticker)

        def _send_to_strategy(payload: object) -> None:
            if self._option_strategy_window is None or not self._option_strategy_window.window.winfo_exists():
                self._option_strategy_window = OptionStrategyCalculatorWindow(
                    root,
                    self._client,
                    runtime_provider=self._runtime_provider,
                    logger=None,
                )
            self._option_strategy_window.load_roll_transfer_payload(payload)

        if self._option_roll_window is not None and self._option_roll_window.window.winfo_exists():
            self._option_roll_window.load_position(
                position=position,
                instrument=instrument,
                quote=quote,
                api_name=api_name,
                auto_scan=True,
            )
            self._option_roll_window.show()
            return

        self._option_roll_window = OptionRollSuggestionWindow(
            root,
            self._client,
            position=position,
            instrument=instrument,
            quote=quote,
            api_name=api_name,
            send_to_strategy_callback=_send_to_strategy,
            logger=None,
        )

    def _ensure_root(self) -> Tk | None:
        if self._root is not None:
            return self._root
        try:
            root = Tk()
            root.withdraw()
        except Exception:
            return None
        self._root = root
        self._pump_timer = QTimer(self._parent)
        self._pump_timer.timeout.connect(self._pump_events)
        self._pump_timer.start(40)
        return root

    @Slot()
    def _pump_events(self) -> None:
        if self._root is None:
            return
        try:
            self._root.update_idletasks()
            self._root.update()
        except Exception:
            self.shutdown()


class PositionProtectionDialog(QDialog):
    def __init__(
        self,
        *,
        manager: PositionProtectionManager,
        client: OkxRestClient,
        runtime_provider: Callable[[], object | None],
        selected_option_provider: Callable[[], OkxPosition | None],
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._manager = manager
        self._client = client
        self._runtime_provider = runtime_provider
        self._selected_option_provider = selected_option_provider
        self._selected_position: OkxPosition | None = None
        self._form_position_key = ""
        self._session_ids: list[str] = []
        self._last_fixed_price_memory = {"tp": "", "sl": ""}

        self.setWindowTitle("设置期权保护")
        self.resize(1080, 760)

        self._build_ui()
        self._refresh_from_selection(force=True)
        self._refresh_sessions()

        self._refresh_timer = QTimer(self)
        self._refresh_timer.timeout.connect(self._refresh_sessions)
        self._refresh_timer.timeout.connect(lambda: self._refresh_from_selection(force=False))
        self._refresh_timer.start(1200)

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        top_panel = QFrame()
        top_panel.setObjectName("Panel")
        top_layout = QVBoxLayout(top_panel)
        top_layout.setContentsMargins(12, 12, 12, 12)
        top_layout.setSpacing(10)

        self._title_label = QLabel("请先在当前持仓里选中一条期权仓位。")
        self._title_label.setObjectName("SectionTitle")
        self._title_label.setWordWrap(True)
        self._logic_hint = QLabel("保护逻辑会跟随上方选中的期权仓位。")
        self._logic_hint.setObjectName("Subtle")
        self._logic_hint.setWordWrap(True)
        top_layout.addWidget(self._title_label)
        top_layout.addWidget(self._logic_hint)

        form = QGridLayout()
        form.setHorizontalSpacing(10)
        form.setVerticalSpacing(10)

        self._trigger_combo = QComboBox()
        for label in PROTECTION_TRIGGER_SOURCE_OPTIONS:
            self._trigger_combo.addItem(label)
        self._trigger_combo.currentIndexChanged.connect(self._on_trigger_source_changed)

        self._spot_symbol_edit = QLineEdit()
        self._tp_trigger_edit = QLineEdit()
        self._sl_trigger_edit = QLineEdit()
        self._tp_mode_combo = QComboBox()
        self._sl_mode_combo = QComboBox()
        for label in PROTECTION_ORDER_MODE_OPTIONS:
            self._tp_mode_combo.addItem(label)
            self._sl_mode_combo.addItem(label)
        self._tp_mode_combo.currentIndexChanged.connect(self._refresh_order_mode_widgets)
        self._sl_mode_combo.currentIndexChanged.connect(self._refresh_order_mode_widgets)
        self._tp_price_edit = QLineEdit()
        self._sl_price_edit = QLineEdit()
        self._tp_slippage_edit = QLineEdit("0")
        self._sl_slippage_edit = QLineEdit("0")
        self._poll_seconds_edit = QLineEdit("2")

        form.addWidget(QLabel("触发条件"), 0, 0)
        form.addWidget(self._trigger_combo, 0, 1)
        form.addWidget(QLabel("现货标的"), 0, 2)
        form.addWidget(self._spot_symbol_edit, 0, 3)
        form.addWidget(QLabel("止盈触发价"), 1, 0)
        form.addWidget(self._tp_trigger_edit, 1, 1)
        form.addWidget(QLabel("止损触发价"), 1, 2)
        form.addWidget(self._sl_trigger_edit, 1, 3)
        form.addWidget(QLabel("止盈报单方式"), 2, 0)
        form.addWidget(self._tp_mode_combo, 2, 1)
        form.addWidget(QLabel("止盈报单价格"), 2, 2)
        form.addWidget(self._tp_price_edit, 2, 3)
        form.addWidget(QLabel("止盈滑点"), 3, 0)
        form.addWidget(self._tp_slippage_edit, 3, 1)
        form.addWidget(QLabel("轮询秒数"), 3, 2)
        form.addWidget(self._poll_seconds_edit, 3, 3)
        form.addWidget(QLabel("止损报单方式"), 4, 0)
        form.addWidget(self._sl_mode_combo, 4, 1)
        form.addWidget(QLabel("止损报单价格"), 4, 2)
        form.addWidget(self._sl_price_edit, 4, 3)
        form.addWidget(QLabel("止损滑点"), 5, 0)
        form.addWidget(self._sl_slippage_edit, 5, 1)
        top_layout.addLayout(form)

        action_row = QHBoxLayout()
        start_button = QPushButton("启动保护")
        start_button.clicked.connect(self._start_selected_position_protection)
        stop_button = QPushButton("停止选中任务")
        stop_button.clicked.connect(self._stop_selected_position_protection)
        clear_button = QPushButton("清除已结束")
        clear_button.clicked.connect(self._clear_finished_position_protections)
        close_button = QPushButton("关闭")
        close_button.clicked.connect(self.close)
        action_row.addWidget(start_button)
        action_row.addWidget(stop_button)
        action_row.addWidget(clear_button)
        action_row.addStretch(1)
        action_row.addWidget(close_button)
        top_layout.addLayout(action_row)
        layout.addWidget(top_panel)

        bottom_split = QSplitter(Qt.Orientation.Vertical)

        sessions_panel = QFrame()
        sessions_panel.setObjectName("Panel")
        sessions_layout = QVBoxLayout(sessions_panel)
        sessions_layout.setContentsMargins(10, 10, 10, 10)
        sessions_layout.setSpacing(8)
        self._session_status_label = QLabel("当前没有运行中的期权保护任务。")
        self._session_status_label.setObjectName("Subtle")
        sessions_layout.addWidget(self._session_status_label)
        self._sessions_table = QTableWidget(0, 6)
        self._sessions_table.setHorizontalHeaderLabels(("API", "期权合约", "触发条件", "方向", "状态", "启动时间"))
        self._sessions_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._sessions_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._sessions_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._sessions_table.verticalHeader().setVisible(False)
        self._sessions_table.horizontalHeader().setStretchLastSection(False)
        self._sessions_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        self._sessions_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self._sessions_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        self._sessions_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        self._sessions_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        self._sessions_table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        self._sessions_table.itemSelectionChanged.connect(self._refresh_selected_session_detail)
        sessions_layout.addWidget(self._sessions_table, 1)
        bottom_split.addWidget(sessions_panel)

        detail_panel = QFrame()
        detail_panel.setObjectName("Panel")
        detail_layout = QVBoxLayout(detail_panel)
        detail_layout.setContentsMargins(10, 10, 10, 10)
        detail_layout.setSpacing(8)
        detail_title = QLabel("任务详情")
        detail_title.setObjectName("SectionTitle")
        self._detail_text = QTextEdit()
        self._detail_text.setReadOnly(True)
        detail_layout.addWidget(detail_title)
        detail_layout.addWidget(self._detail_text, 1)
        bottom_split.addWidget(detail_panel)
        bottom_split.setSizes([340, 240])
        layout.addWidget(bottom_split, 1)

    def closeEvent(self, event) -> None:  # type: ignore[override]
        self._refresh_timer.stop()
        super().closeEvent(event)

    def showEvent(self, event) -> None:  # type: ignore[override]
        if not self._refresh_timer.isActive():
            self._refresh_timer.start(1200)
        super().showEvent(event)

    def _current_position(self) -> OkxPosition | None:
        current = self._selected_option_provider()
        if current is not None:
            self._selected_position = current
            return current
        return self._selected_position

    def _refresh_from_selection(self, *, force: bool) -> None:
        position = self._selected_option_provider()
        if position is None and self._selected_position is None:
            self._title_label.setText("请先在当前持仓里选中一条期权仓位。")
            self._logic_hint.setText("保护逻辑会跟随上方选中的期权仓位。")
            return
        if position is None:
            position = self._selected_position
        if position is None:
            return
        self._selected_position = position
        position_key = _position_tree_row_id(position)
        direction = derive_position_direction(position)
        self._title_label.setText(
            f"当前选中：{position.inst_id} | 方向={direction.upper()} | 持仓量={_format_optional_decimal(position.position)} | 开仓均价={_format_optional_decimal(position.avg_price)}"
        )
        if force or position_key != self._form_position_key:
            self._form_position_key = position_key
            self._trigger_combo.setCurrentIndex(0)
            self._spot_symbol_edit.setText(infer_default_spot_inst_id(position.inst_id))
            self._tp_trigger_edit.clear()
            self._sl_trigger_edit.clear()
            self._tp_mode_combo.setCurrentIndex(1 if self._tp_mode_combo.count() > 1 else 0)
            self._sl_mode_combo.setCurrentIndex(1 if self._sl_mode_combo.count() > 1 else 0)
            self._tp_price_edit.clear()
            self._sl_price_edit.clear()
            self._tp_slippage_edit.setText("0")
            self._sl_slippage_edit.setText("0")
            self._poll_seconds_edit.setText("2")
        self._on_trigger_source_changed()
        self._refresh_order_mode_widgets()

    def _on_trigger_source_changed(self) -> None:
        position = self._current_position()
        trigger_source = PROTECTION_TRIGGER_SOURCE_OPTIONS.get(self._trigger_combo.currentText(), "option_mark")
        self._spot_symbol_edit.setEnabled(trigger_source == "spot_last")
        if position is None:
            self._logic_hint.setText("保护逻辑会跟随上方选中的期权仓位。")
            return
        if trigger_source == "option_mark":
            trigger_inst_id = position.inst_id
            trigger_price_type = "mark"
        else:
            trigger_inst_id = normalize_spot_inst_id(self._spot_symbol_edit.text()) or infer_default_spot_inst_id(position.inst_id)
            trigger_price_type = "last"
        self._logic_hint.setText(
            describe_protection_price_logic(
                option_inst_id=position.inst_id,
                direction=derive_position_direction(position),
                trigger_inst_id=trigger_inst_id,
                trigger_price_type=trigger_price_type,
            )
        )

    def _refresh_order_mode_widgets(self) -> None:
        self._sync_order_mode_widgets(mode_label=self._tp_mode_combo.currentText(), price_edit=self._tp_price_edit, slippage_edit=self._tp_slippage_edit, key="tp")
        self._sync_order_mode_widgets(mode_label=self._sl_mode_combo.currentText(), price_edit=self._sl_price_edit, slippage_edit=self._sl_slippage_edit, key="sl")

    def _sync_order_mode_widgets(self, *, mode_label: str, price_edit: QLineEdit, slippage_edit: QLineEdit, key: str) -> None:
        fixed_mode = _resolve_protection_order_mode_value(mode_label) == "fixed_price"
        if fixed_mode:
            if not price_edit.text().strip():
                price_edit.setText(self._last_fixed_price_memory.get(key, ""))
            price_edit.setEnabled(True)
            slippage_edit.setEnabled(False)
        else:
            current_text = price_edit.text().strip()
            if current_text:
                self._last_fixed_price_memory[key] = current_text
            price_edit.clear()
            price_edit.setEnabled(False)
            slippage_edit.setEnabled(True)

    def _start_selected_position_protection(self) -> None:
        runtime = self._runtime_provider()
        position = self._current_position()
        if runtime is None:
            QMessageBox.warning(self, "启动失败", "当前没有可用的 API 运行时。")
            return
        if position is None or position.inst_type != "OPTION":
            QMessageBox.information(self, "提示", "请先在当前持仓中选中一条期权仓位。")
            return
        try:
            protection = self._build_selected_position_protection(position)
            _validate_protection_live_price_availability(self._client, protection, position)
            config = self._build_strategy_config(runtime=runtime, position=position, protection=protection)
            self._manager.start(runtime.credentials, config, protection)
            self._refresh_sessions()
        except Exception as exc:
            QMessageBox.critical(self, "启动保护失败", str(exc))

    def _stop_selected_position_protection(self) -> None:
        session_id = self._selected_session_id()
        if not session_id:
            QMessageBox.information(self, "提示", "请先在下方任务列表里选中一条保护任务。")
            return
        try:
            self._manager.stop(session_id)
            self._refresh_sessions()
        except Exception as exc:
            QMessageBox.critical(self, "停止失败", str(exc))

    def _clear_finished_position_protections(self) -> None:
        cleared = self._manager.clear_finished()
        self._refresh_sessions()
        if cleared <= 0:
            QMessageBox.information(self, "提示", "当前没有可清理的已结束任务。")

    def _build_selected_position_protection(self, position: OkxPosition) -> OptionProtectionConfig:
        trigger_source = PROTECTION_TRIGGER_SOURCE_OPTIONS[self._trigger_combo.currentText()]
        if trigger_source == "option_mark":
            trigger_inst_id = position.inst_id
            trigger_price_type = "mark"
            trigger_label = f"{position.inst_id} 标记价"
        else:
            trigger_inst_id = normalize_spot_inst_id(self._spot_symbol_edit.text())
            if not trigger_inst_id:
                raise ValueError("现货触发模式下，请填写现货标的，例如 BTC-USDT。")
            trigger_instrument = self._client.get_instrument(trigger_inst_id)
            if str(trigger_instrument.inst_type or "").upper() != "SPOT":
                raise ValueError("现货触发模式下，标的必须是现货交易对，例如 BTC-USDT。")
            trigger_price_type = "last"
            trigger_label = f"{trigger_inst_id} 最新价"

        take_profit_trigger = self._parse_optional_positive_decimal(self._tp_trigger_edit.text(), "止盈触发价")
        stop_loss_trigger = self._parse_optional_positive_decimal(self._sl_trigger_edit.text(), "止损触发价")
        if take_profit_trigger is None and stop_loss_trigger is None:
            raise ValueError("止盈触发价和止损触发价至少要填写一个。")

        direction = derive_position_direction(position)
        _validate_protection_price_relationship(
            option_inst_id=position.inst_id,
            direction=direction,
            trigger_inst_id=trigger_inst_id,
            trigger_price_type=trigger_price_type,
            take_profit=take_profit_trigger,
            stop_loss=stop_loss_trigger,
        )

        take_profit_order_mode = PROTECTION_ORDER_MODE_OPTIONS[self._tp_mode_combo.currentText()]
        stop_loss_order_mode = PROTECTION_ORDER_MODE_OPTIONS[self._sl_mode_combo.currentText()]
        take_profit_order_price = self._parse_positive_decimal(self._tp_price_edit.text(), "止盈报单价格") if take_profit_order_mode == "fixed_price" else None
        stop_loss_order_price = self._parse_positive_decimal(self._sl_price_edit.text(), "止损报单价格") if stop_loss_order_mode == "fixed_price" else None
        return OptionProtectionConfig(
            option_inst_id=position.inst_id,
            trigger_inst_id=trigger_inst_id,
            trigger_price_type=trigger_price_type,
            direction=direction,
            pos_side=position.pos_side if position.pos_side and position.pos_side.lower() != "net" else None,
            take_profit_trigger=take_profit_trigger,
            stop_loss_trigger=stop_loss_trigger,
            take_profit_order_mode=take_profit_order_mode,
            take_profit_order_price=take_profit_order_price,
            take_profit_slippage=self._parse_nonnegative_decimal(self._tp_slippage_edit.text(), "止盈滑点"),
            stop_loss_order_mode=stop_loss_order_mode,
            stop_loss_order_price=stop_loss_order_price,
            stop_loss_slippage=self._parse_nonnegative_decimal(self._sl_slippage_edit.text(), "止损滑点"),
            poll_seconds=float(self._parse_positive_decimal(self._poll_seconds_edit.text(), "轮询秒数")),
            trigger_label=trigger_label,
        )

    def _build_strategy_config(self, *, runtime: object, position: OkxPosition, protection: OptionProtectionConfig) -> StrategyConfig:
        position_mode = "long_short" if position.pos_side and position.pos_side.lower() != "net" else "net"
        trade_mode = position.mgn_mode if position.mgn_mode in {"cross", "isolated"} else getattr(runtime, "trade_mode", "cross")
        return StrategyConfig(
            inst_id=protection.trigger_inst_id,
            bar="1H",
            ema_period=1,
            atr_period=1,
            atr_stop_multiplier=Decimal("1"),
            atr_take_multiplier=Decimal("1"),
            order_size=abs(position.position),
            trade_mode=trade_mode,
            signal_mode="long_only" if protection.direction == "long" else "short_only",
            position_mode=position_mode,
            environment=getattr(runtime, "environment", "live"),
            tp_sl_trigger_type=protection.trigger_price_type,
            strategy_id="manual_option_protection",
            poll_seconds=protection.poll_seconds,
            risk_amount=None,
            trade_inst_id=position.inst_id,
            tp_sl_mode="local_trade",
            local_tp_sl_inst_id=protection.trigger_inst_id,
            entry_side_mode="follow_signal",
            run_mode="trade",
        )

    def _selected_session_id(self) -> str:
        row = self._sessions_table.currentRow()
        if row < 0 or row >= len(self._session_ids):
            return ""
        return self._session_ids[row]

    def _refresh_sessions(self) -> None:
        sessions = self._manager.list_sessions()
        selected_before = self._selected_session_id()
        self._session_status_label.setText(f"当前保护任务：{len(sessions)}" if sessions else "当前没有运行中的期权保护任务。")
        self._sessions_table.setRowCount(len(sessions))
        self._session_ids = [item.session_id for item in sessions]
        for row, item in enumerate(sessions):
            values = (
                item.api_name or "-",
                item.option_inst_id,
                item.trigger_label,
                item.direction,
                item.status,
                item.started_at.strftime("%H:%M:%S"),
            )
            for column, value in enumerate(values):
                cell = QTableWidgetItem(str(value))
                if column in {0, 3, 4, 5}:
                    cell.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter))
                self._sessions_table.setItem(row, column, cell)
        target_row = -1
        if selected_before and selected_before in self._session_ids:
            target_row = self._session_ids.index(selected_before)
        elif self._session_ids:
            target_row = 0
        if target_row >= 0:
            self._sessions_table.selectRow(target_row)
        else:
            self._detail_text.setPlainText("请选择一条保护任务查看详情。")
        self._refresh_selected_session_detail()

    def _refresh_selected_session_detail(self) -> None:
        session_id = self._selected_session_id()
        sessions = {item.session_id: item for item in self._manager.list_sessions()}
        session = sessions.get(session_id)
        if session is None:
            self._detail_text.setPlainText("请选择一条保护任务查看详情。")
            return
        self._detail_text.setPlainText(
            "\n".join(
                [
                    f"任务：{session.session_id}",
                    f"API配置：{session.api_name or '-'}",
                    f"期权合约：{session.option_inst_id}",
                    f"触发条件：{session.trigger_label}",
                    f"触发标的：{session.trigger_inst_id}",
                    f"触发价格类型：{_format_protection_trigger_price_type(session.trigger_price_type)}",
                    f"方向：{session.direction}",
                    f"持仓方向：{session.pos_side or '-'}",
                    f"止盈触发：{_format_optional_decimal(session.take_profit_trigger)}",
                    f"止盈报单方式：{_format_protection_order_mode_label(session.take_profit_order_mode)}",
                    f"止盈报单价格：{_format_protection_order_price_detail(session.take_profit_order_mode, session.take_profit_order_price)}",
                    f"止盈滑点：{_format_optional_decimal(session.take_profit_slippage)}",
                    f"止损触发：{_format_optional_decimal(session.stop_loss_trigger)}",
                    f"止损报单方式：{_format_protection_order_mode_label(session.stop_loss_order_mode)}",
                    f"止损报单价格：{_format_protection_order_price_detail(session.stop_loss_order_mode, session.stop_loss_order_price)}",
                    f"止损滑点：{_format_optional_decimal(session.stop_loss_slippage)}",
                    f"轮询秒数：{session.poll_seconds:g}",
                    f"状态：{session.status}",
                    f"启动时间：{session.started_at.strftime('%Y-%m-%d %H:%M:%S')}",
                    "",
                    f"最新状态：{session.last_message}",
                ]
            )
        )

    def _parse_positive_decimal(self, raw: str, field_name: str) -> Decimal:
        try:
            value = Decimal(raw.strip())
        except Exception as exc:
            raise ValueError(f"{field_name} 不是有效数字") from exc
        if value <= 0:
            raise ValueError(f"{field_name} 必须大于 0")
        return value

    def _parse_optional_positive_decimal(self, raw: str, field_name: str) -> Decimal | None:
        cleaned = raw.strip()
        if not cleaned:
            return None
        return self._parse_positive_decimal(cleaned, field_name)

    def _parse_nonnegative_decimal(self, raw: str, field_name: str) -> Decimal:
        cleaned = raw.strip()
        if not cleaned:
            return Decimal("0")
        try:
            value = Decimal(cleaned)
        except Exception as exc:
            raise ValueError(f"{field_name} 不是有效数字") from exc
        if value < 0:
            raise ValueError(f"{field_name} 不能小于 0")
        return value


class AccountPositionsHomeWidget(QWidget):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._runtime = load_runtime("159") or load_runtime()
        self._profile_snapshots: dict[str, dict[str, str]] = {}
        self._unlocked_profiles: set[str] = set()
        self._last_profile_name = self._runtime.credential_profile_name if self._runtime is not None else ""
        self._profile_switch_guard = False
        self._account_feed: AccountFeedThread | None = None
        self._order_feed: OrderFeedThread | None = None
        self._order_history_feed: OrderHistoryFeedThread | None = None
        self._fill_history_feed: FillHistoryFeedThread | None = None
        self._position_history_feed: PositionHistoryFeedThread | None = None

        self._raw_positions: list[OkxPosition] = []
        self._visible_positions: list[OkxPosition] = []
        self._orders: list[OrderStatusView] = []
        self._visible_orders: list[OrderStatusView] = []
        self._order_history_items: list[OkxTradeOrderItem] = []
        self._visible_order_history_items: list[OkxTradeOrderItem] = []
        self._fill_history_items: list[OkxFillHistoryItem] = []
        self._visible_fill_history_items: list[OkxFillHistoryItem] = []
        self._fill_history_instruments: dict[str, object] = {}
        self._fill_history_usdt_prices: dict[str, Decimal] = {}
        self._order_history_usdt_prices: dict[str, Decimal] = {}
        self._position_history_items: list[OkxPositionHistoryItem] = []
        self._visible_position_history_items: list[OkxPositionHistoryItem] = []
        self._position_history_instruments: dict[str, object] = {}
        self._position_history_usdt_prices: dict[str, Decimal] = {}
        self._position_instruments: dict[str, object] = {}
        self._position_tickers: dict[str, object] = {}
        self._upl_usdt_prices: dict[str, Decimal] = {}
        self._position_row_payloads: dict[str, dict[str, object]] = {}
        self._visible_column_ids: set[str] = set(DEFAULT_VISIBLE_COLUMNS)
        self._tree_column_width_overrides: dict[str, int] = {}
        self._expanded_row_keys: set[str] = set()
        self._fill_history_fetch_limit = 100
        self._position_history_fetch_limit = 300
        self._position_history_last_sync_text = "-"
        self._position_history_filter_resetting = False
        self._shared_client = OkxRestClient()
        self._protection_manager = PositionProtectionManager(self._shared_client, lambda _message: None)
        self._protection_dialog: PositionProtectionDialog | None = None
        self._legacy_option_tools = LegacyOptionToolsHost(parent=self, runtime_provider=lambda: self._runtime)

        self._current_notes: dict[str, dict[str, object]] = {}
        self._history_notes: dict[str, dict[str, object]] = {}
        self._load_position_notes()
        self._load_positions_view_prefs()

        self._build_ui()
        self._apply_compact_layout_tuning()
        self._positions_view_prefs_save_timer = QTimer(self)
        self._positions_view_prefs_save_timer.setSingleShot(True)
        self._positions_view_prefs_save_timer.timeout.connect(self._save_positions_view_prefs_now)
        self._position_history_render_timer = QTimer(self)
        self._position_history_render_timer.setSingleShot(True)
        self._position_history_render_timer.timeout.connect(self._render_position_history_table)
        self._refresh_profiles()
        self._populate_profile_combo()

        locked_on_start = bool(
            self._last_profile_name and profile_requires_password(self._last_profile_name, self._profile_snapshots)
        )
        if locked_on_start:
            self._account_status.setText(f"API {self._last_profile_name} 未解锁")
            self._order_status.setText("订单 WS 等待 API 解锁")
            self._summary_label.setText("当前 API 配置已加切换密码，请先解锁后再加载账户持仓。")
        else:
            if self._last_profile_name:
                self._unlocked_profiles.add(self._last_profile_name)
            self._start_private_threads()

    def _stop_position_history_thread(self) -> None:
        thread = self._position_history_feed
        if thread is None:
            return
        thread.stop()
        if thread.isRunning() and not thread.wait(1600):
            thread.terminate()
            thread.wait(1600)
        self._position_history_feed = None

    def _stop_order_history_thread(self) -> None:
        thread = self._order_history_feed
        if thread is None:
            return
        thread.stop()
        if thread.isRunning() and not thread.wait(1600):
            thread.terminate()
            thread.wait(1600)
        self._order_history_feed = None

    def _stop_fill_history_thread(self) -> None:
        thread = self._fill_history_feed
        if thread is None:
            return
        thread.stop()
        if thread.isRunning() and not thread.wait(1600):
            thread.terminate()
            thread.wait(1600)
        self._fill_history_feed = None

    def _start_order_history_refresh(self, *, force_restart: bool = False) -> None:
        if self._runtime is None:
            return
        if force_restart:
            self._stop_order_history_thread()
        elif self._order_history_feed is not None and self._order_history_feed.isRunning():
            return
        self._order_history_feed = OrderHistoryFeedThread(self._runtime, limit=200)
        self._order_history_feed.data_ready.connect(self._apply_order_history_payload)
        self._order_history_feed.status_changed.connect(self._set_order_history_status)
        self._order_history_feed.finished.connect(self._clear_order_history_thread)
        if hasattr(self, "_order_history_summary_label"):
            self._order_history_summary_label.setText("正在同步历史委托...")
        self._order_history_feed.start()

    def _start_fill_history_refresh(self, *, force_restart: bool = False) -> None:
        if self._runtime is None:
            return
        if force_restart:
            self._stop_fill_history_thread()
        elif self._fill_history_feed is not None and self._fill_history_feed.isRunning():
            return
        self._fill_history_feed = FillHistoryFeedThread(self._runtime, limit=self._fill_history_fetch_limit)
        self._fill_history_feed.data_ready.connect(self._apply_fill_history_payload)
        self._fill_history_feed.status_changed.connect(self._set_fill_history_status)
        self._fill_history_feed.finished.connect(self._clear_fill_history_thread)
        if hasattr(self, "_fill_history_summary_label"):
            self._fill_history_summary_label.setText("正在同步历史成交...")
        self._fill_history_feed.start()

    def _start_position_history_refresh(self, *, force_restart: bool = False) -> None:
        if self._runtime is None:
            return
        if force_restart:
            self._stop_position_history_thread()
        elif self._position_history_feed is not None and self._position_history_feed.isRunning():
            return
        self._position_history_feed = PositionHistoryFeedThread(self._runtime, limit=self._position_history_fetch_limit)
        self._position_history_feed.data_ready.connect(self._apply_position_history_payload)
        self._position_history_feed.status_changed.connect(self._set_position_history_status)
        self._position_history_feed.finished.connect(self._clear_position_history_thread)
        self._position_history_summary_label.setText("正在同步历史仓位...")
        self._position_history_feed.start()

    @Slot()
    def _refresh_position_history(self) -> None:
        if not self._ensure_runtime_ready(force_unlock=True):
            return
        self._start_position_history_refresh(force_restart=True)

    @Slot()
    def _clear_position_history_thread(self) -> None:
        self._position_history_feed = None
        return

    @Slot()
    def _refresh_order_history(self) -> None:
        if not self._ensure_runtime_ready(force_unlock=True):
            return
        self._start_order_history_refresh(force_restart=True)

    @Slot()
    def _refresh_fill_history(self) -> None:
        if not self._ensure_runtime_ready(force_unlock=True):
            return
        self._start_fill_history_refresh(force_restart=True)

    @Slot()
    def _clear_order_history_thread(self) -> None:
        self._order_history_feed = None

    @Slot()
    def _clear_fill_history_thread(self) -> None:
        self._fill_history_feed = None
        return

        self._account_feed: AccountFeedThread | None = None
        self._order_feed: OrderFeedThread | None = None
        self._position_history_feed: PositionHistoryFeedThread | None = None

        self._raw_positions: list[OkxPosition] = []
        self._visible_positions: list[OkxPosition] = []
        self._orders: list[OrderStatusView] = []
        self._visible_orders: list[OrderStatusView] = []
        self._position_history_items: list[OkxPositionHistoryItem] = []
        self._position_history_instruments: dict[str, object] = {}
        self._position_history_usdt_prices: dict[str, Decimal] = {}
        self._position_instruments: dict[str, object] = {}
        self._position_tickers: dict[str, object] = {}
        self._upl_usdt_prices: dict[str, Decimal] = {}
        self._position_row_payloads: dict[str, dict[str, object]] = {}
        self._visible_column_ids: set[str] = set(DEFAULT_VISIBLE_COLUMNS)
        self._expanded_row_keys: set[str] = set()
        self._shared_client = OkxRestClient()
        self._protection_manager = PositionProtectionManager(self._shared_client, lambda _message: None)
        self._protection_dialog: PositionProtectionDialog | None = None
        self._legacy_option_tools = LegacyOptionToolsHost(parent=self, runtime_provider=lambda: self._runtime)

        self._current_notes: dict[str, dict[str, object]] = {}
        self._history_notes: dict[str, dict[str, object]] = {}
        self._load_position_notes()

        self._build_ui()
        self._refresh_profiles()
        self._populate_profile_combo()

        locked_on_start = bool(
            self._last_profile_name and profile_requires_password(self._last_profile_name, self._profile_snapshots)
        )
        if locked_on_start:
            self._account_status.setText(f"API {self._last_profile_name} 未解锁")
            self._order_status.setText("委托 WS 等待 API 解锁")
            self._summary_label.setText("当前 API 配置已加切换密码，请先解锁后再加载账户持仓。")
        else:
            if self._last_profile_name:
                self._unlocked_profiles.add(self._last_profile_name)
            self._start_private_threads()

    def shutdown(self) -> None:
        self._save_positions_view_prefs_now()
        self._stop_private_threads()
        self._stop_order_history_thread()
        self._stop_fill_history_thread()
        self._stop_position_history_thread()
        self._protection_manager.stop_all()
        if self._protection_dialog is not None:
            self._protection_dialog.close()
        self._legacy_option_tools.shutdown()

    def refresh_view(self) -> None:
        if not self._ensure_runtime_ready(force_unlock=True):
            return
        self._status_badge.setText("正在刷新...")
        self._start_private_threads(force_restart=True)

    def refresh_view(self) -> None:
        if not self._ensure_runtime_ready(force_unlock=True):
            return
        self._status_badge.setText("正在刷新...")
        self._start_private_threads(force_restart=True)

    @Slot(object)
    def _apply_position_history_payload(self, payload: object) -> None:
        if not isinstance(payload, dict):
            return
        items = payload.get("items")
        instruments = payload.get("instruments")
        usdt_prices = payload.get("usdt_prices")
        self._position_history_items = list(items) if isinstance(items, list) else []
        self._position_history_instruments = dict(instruments) if isinstance(instruments, dict) else {}
        self._position_history_usdt_prices = dict(usdt_prices) if isinstance(usdt_prices, dict) else {}
        self._position_history_last_sync_text = time.strftime("%H:%M:%S")
        self._render_position_history_table()

    def _render_position_history_table(self) -> None:
        if not hasattr(self, "_position_history_table"):
            return
        filtered = self._filtered_position_history_items()
        selected_key = ""
        row = self._position_history_table.currentRow()
        if 0 <= row < len(self._visible_position_history_items):
            selected_key = self._position_history_row_key(self._visible_position_history_items[row])
        self._visible_position_history_items = filtered
        stats_text = _format_position_history_filter_stats(list(enumerate(filtered)), self._position_history_usdt_prices)
        self._position_history_summary_label.setText(
            "\n".join(
                (
                    f"历史仓位: {len(self._position_history_items)} 条 | 最近同步: {self._position_history_last_sync_text} | 当前显示: {len(filtered)}/{len(self._position_history_items)}",
                    f"筛选统计: {stats_text}",
                )
            )
        )
        stats_text = _format_position_history_filter_stats(list(enumerate(filtered)), self._position_history_usdt_prices)
        self._position_history_summary_label.setText(
            "\n".join(
                (
                    f"历史仓位: {len(self._position_history_items)} 条 | 最近同步: {self._position_history_last_sync_text} | 当前显示: {len(filtered)}/{len(self._position_history_items)}",
                    f"筛选统计: {stats_text}",
                )
            )
        )
        self._position_history_table.setRowCount(len(filtered))
        for row, item in enumerate(filtered):
            values = (
                _format_okx_ms_timestamp(item.update_time),
                item.inst_type or "-",
                item.inst_id or "-",
                _format_margin_mode(item.mgn_mode or ""),
                _format_history_side(None, item.pos_side or item.direction),
                _format_position_history_trade_side(item),
                _format_position_history_price(item.open_avg_price, item.inst_id, item.inst_type),
                _format_position_history_price(item.close_avg_price, item.inst_id, item.inst_type),
                _format_position_history_size(item, self._position_history_instruments),
                _format_position_history_fee_cell(item, self._position_history_usdt_prices),
                _format_position_history_pnl(item.pnl, item, usdt_prices=self._position_history_usdt_prices),
                _position_history_note_summary_text(item, self._position_history_note_text(item)),
            )
            self._set_table_row(self._position_history_table, row, values, left_align={2, 11})
        self._restore_table_selection(
            self._position_history_table,
            filtered,
            selected_key,
            self._position_history_row_key,
        )
        self._refresh_position_history_detail()
        self._start_order_history_refresh(force_restart=True)
        self._start_fill_history_refresh(force_restart=True)

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(6, 6, 6, 6)
        layout.setSpacing(6)

        layout.addWidget(self._build_header())
        layout.addWidget(self._build_filter_bar())
        layout.addWidget(self._build_positions_panel(), 1)

    def _apply_compact_layout_tuning(self) -> None:
        self.setStyleSheet(
            """
            QWidget {
                font-size: 11px;
            }
            QFrame#HeaderPanel,
            QFrame#Panel,
            QFrame#Guide {
                background: #ffffff;
                border: 1px solid #d7e0ea;
                border-radius: 7px;
            }
            QFrame#ToolbarBand {
                background: #f8fafc;
                border: 1px solid #e2e8f0;
                border-radius: 6px;
            }
            QLabel#SectionTitle {
                font-size: 12px;
                font-weight: 700;
                color: #0f172a;
            }
            QLabel#Subtle {
                color: #64748b;
            }
            QLabel#Badge {
                color: #075985;
                background: #e0f2fe;
                border: 1px solid #bae6fd;
                border-radius: 6px;
                padding: 2px 8px;
                font-weight: 700;
            }
            QPushButton {
                font-size: 11px;
                padding: 2px 8px;
                min-height: 22px;
                border-radius: 5px;
            }
            QComboBox, QLineEdit {
                font-size: 11px;
                min-height: 22px;
                padding: 1px 6px;
                border-radius: 5px;
            }
            QTabBar::tab {
                font-size: 11px;
                min-height: 22px;
                padding: 3px 10px;
            }
            QTabWidget::pane {
                border: 1px solid #d7e0ea;
                border-radius: 6px;
                top: -1px;
            }
            QHeaderView::section {
                padding: 2px 5px;
                min-height: 20px;
            }
            QTreeWidget, QTableWidget, QTextEdit {
                border-radius: 5px;
            }
            QSplitter::handle {
                background: #dbe3ec;
                height: 5px;
            }
            """
        )
        for table in self.findChildren(QTableWidget):
            table.verticalHeader().setDefaultSectionSize(21)
        for tree in self.findChildren(QTreeWidget):
            tree.setStyleSheet("QTreeView::item { height: 21px; }")

    def _build_header(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("HeaderPanel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(8, 6, 8, 6)
        layout.setSpacing(5)

        top = QHBoxLayout()
        top.setSpacing(6)
        self._status_badge = QLabel("正常")
        self._status_badge.setObjectName("Badge")
        self._account_status = QLabel("持仓读取中...")
        self._account_status.setObjectName("Subtle")
        self._order_status = QLabel("订单WS等待中...")
        self._order_status.setObjectName("Subtle")
        self._summary_label = QLabel("当前没有持仓")
        self._summary_label.setObjectName("Subtle")
        self._summary_label.setWordWrap(False)
        top.addWidget(self._status_badge)
        top.addWidget(self._account_status)
        top.addWidget(self._order_status)
        top.addWidget(self._summary_label, 1)
        top.addStretch(1)
        top.addWidget(QLabel("API配置"))
        self._profile_combo = QComboBox()
        self._profile_combo.currentIndexChanged.connect(self._on_profile_changed)
        self._profile_combo.setMinimumWidth(120)
        top.addWidget(self._profile_combo)
        layout.addLayout(top)

        toolbar = QFrame()
        toolbar.setObjectName("ToolbarBand")
        actions = QHBoxLayout(toolbar)
        actions.setContentsMargins(5, 4, 5, 4)
        actions.setSpacing(4)
        for text, handler, button_attr in (
            ("刷新", self.refresh_view, ""),
            ("账户信息", self._show_account_overview, ""),
            ("展开持仓详情", self._toggle_detail_panel, "_detail_toggle_button"),
            ("折叠历史区域", self._toggle_history_panel, "_history_toggle_button"),
            ("平仓选中", self._show_not_ready_action, ""),
            ("编辑备注", self.edit_selected_position_note, ""),
            ("从选中持仓接管", self._show_not_ready_action, ""),
            ("停止接管", self._show_not_ready_action, ""),
            ("设置期权保护", self._open_position_protection_dialog, ""),
            ("展期建议", self._open_option_roll_window, ""),
            ("列设置", self.open_positions_column_window, ""),
        ):
            button = QPushButton(text)
            button.clicked.connect(handler)
            if button_attr:
                setattr(self, button_attr, button)
            actions.addWidget(button)
        actions.addStretch(1)
        layout.addWidget(toolbar)
        return panel

    def _build_filter_bar(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Guide")
        layout = QGridLayout(panel)
        layout.setContentsMargins(8, 6, 8, 6)
        layout.setHorizontalSpacing(8)
        layout.setVerticalSpacing(5)

        self._type_combo = QComboBox()
        for label, value in POSITION_TYPE_OPTIONS:
            self._type_combo.addItem(label, value)
        self._type_combo.currentIndexChanged.connect(self._apply_filters)

        self._keyword_edit = QLineEdit()
        self._keyword_edit.setPlaceholderText("搜索合约 / 币种 / 备注 / 模式")
        self._keyword_edit.textChanged.connect(self._apply_filters)

        self._filter_hint = QLabel("选中期权后，可一键带入合约或到期前缀。")
        self._filter_hint.setObjectName("Subtle")

        self._apply_contract_button = QPushButton("带入合约")
        self._apply_contract_button.clicked.connect(self.apply_selected_option_to_position_search)
        self._apply_contract_button.setEnabled(False)
        self._apply_expiry_button = QPushButton("带入到期前缀")
        self._apply_expiry_button.clicked.connect(self.apply_selected_option_expiry_prefix_to_position_search)
        self._apply_expiry_button.setEnabled(False)

        apply_button = QPushButton("应用筛选")
        apply_button.clicked.connect(self._apply_filters)
        clear_button = QPushButton("清空筛选")
        clear_button.clicked.connect(self._clear_filters)

        layout.addWidget(QLabel("类型"), 0, 0)
        layout.addWidget(self._type_combo, 0, 1)
        layout.addWidget(QLabel("搜索"), 0, 2)
        layout.addWidget(self._keyword_edit, 0, 3, 1, 4)
        layout.addWidget(self._apply_contract_button, 0, 7)
        layout.addWidget(self._apply_expiry_button, 0, 8)
        layout.addWidget(apply_button, 0, 9)
        layout.addWidget(clear_button, 0, 10)
        layout.addWidget(self._filter_hint, 1, 0, 1, 11)
        layout.setColumnStretch(3, 1)
        return panel

    def _build_positions_panel(self) -> QWidget:
        splitter = QSplitter(Qt.Orientation.Vertical)
        splitter.addWidget(self._build_tree_section())
        splitter.addWidget(self._build_history_tabs_v2())
        splitter.setChildrenCollapsible(False)
        splitter.setSizes([690, 340])
        self._update_panel_toggle_buttons()
        return splitter

    def _build_tree_section(self) -> QWidget:
        wrapper = QWidget()
        layout = QVBoxLayout(wrapper)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        panel = QFrame()
        panel.setObjectName("Panel")
        panel_layout = QVBoxLayout(panel)
        panel_layout.setContentsMargins(8, 7, 8, 7)
        panel_layout.setSpacing(5)

        title_row = QHBoxLayout()
        title = QLabel("当前持仓")
        title.setObjectName("SectionTitle")
        self._positions_hint = QLabel("当前显示 0 条持仓 | 点击任一行查看详情。")
        self._positions_hint.setObjectName("Subtle")
        title_row.addWidget(title)
        title_row.addStretch(1)
        self._expand_toggle_button = QPushButton("展开全部")
        self._expand_toggle_button.clicked.connect(self._toggle_all_positions)
        title_row.addWidget(self._expand_toggle_button)
        title_row.addWidget(self._positions_hint)
        panel_layout.addLayout(title_row)

        self._position_tree = QTreeWidget()
        self._position_tree.setColumnCount(1 + len(POSITION_COLUMNS))
        self._position_tree.setHeaderLabels(["合约 / 分组", *[item[1] for item in POSITION_COLUMNS]])
        self._position_tree.setAlternatingRowColors(True)
        self._position_tree.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._position_tree.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._position_tree.itemSelectionChanged.connect(self._on_position_selected)
        self._position_tree.itemExpanded.connect(self._on_tree_item_expanded)
        self._position_tree.itemCollapsed.connect(self._on_tree_item_collapsed)
        self._position_tree.setRootIsDecorated(True)
        self._position_tree.setUniformRowHeights(True)
        header = self._position_tree.header()
        header.setStretchLastSection(False)
        self._position_tree.setColumnWidth(0, 240)
        for index, (_column_id, _heading, width, _alignment) in enumerate(POSITION_COLUMNS, start=1):
            self._position_tree.setColumnWidth(index, width)
        self._apply_tree_column_width_overrides()
        self._apply_column_visibility()
        header.sectionResized.connect(self._schedule_positions_view_prefs_save)
        panel_layout.addWidget(self._position_tree, 1)
        layout.addWidget(panel, 1)

        self._detail_panel = QFrame()
        self._detail_panel.setObjectName("Panel")
        detail_layout = QVBoxLayout(self._detail_panel)
        detail_layout.setContentsMargins(12, 12, 12, 12)
        detail_layout.setSpacing(8)
        detail_title = QLabel("持仓详情")
        detail_title.setObjectName("SectionTitle")
        self._detail_text = QTextEdit()
        self._detail_text.setReadOnly(True)
        detail_layout.addWidget(detail_title)
        detail_layout.addWidget(self._detail_text, 1)
        self._detail_panel.setVisible(False)
        layout.addWidget(self._detail_panel)
        return wrapper

    def _build_history_tabs(self) -> QWidget:
        self._history_panel = QFrame()
        self._history_panel.setObjectName("Panel")
        layout = QVBoxLayout(self._history_panel)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        self._tabs = QTabWidget()
        self._tabs.addTab(self._build_current_orders_tab(), "当前委托")
        self._tabs.addTab(self._build_placeholder_tab("动态止盈接管", "动态止盈接管区块预留，后续按旧页面完整迁移。"), "动态止盈接管")
        self._tabs.addTab(self._build_placeholder_tab("历史委托", "历史委托区块预留，后续补齐筛选和同步逻辑。"), "历史委托")
        self._tabs.addTab(self._build_placeholder_tab("历史成交", "历史成交区块预留，后续补齐筛选和同步逻辑。"), "历史成交")
        self._tabs.addTab(self._build_position_history_tab(), "历史仓位")
        layout.addWidget(self._tabs, 1)
        return self._history_panel

    def _build_current_orders_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        self._orders_summary_label = QLabel("当前委托尚未读取。")
        self._orders_summary_label.setObjectName("Subtle")
        self._orders_summary_label.setWordWrap(True)
        layout.addWidget(self._orders_summary_label)

        self._orders_table = QTableWidget(0, 11)
        self._orders_table.setHorizontalHeaderLabels(
            ("时间", "合约", "类型", "状态", "方向", "委托类型", "委托价", "委托量", "已成交", "交易模式", "clOrdId")
        )
        self._orders_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._orders_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._orders_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._orders_table.verticalHeader().setVisible(False)
        header = self._orders_table.horizontalHeader()
        header.setStretchLastSection(False)
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(7, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(8, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(9, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(10, QHeaderView.ResizeMode.Stretch)
        self._orders_table.itemSelectionChanged.connect(self._refresh_current_order_detail)
        layout.addWidget(self._orders_table, 1)

        detail_title = QLabel("委托详情")
        detail_title.setObjectName("SectionTitle")
        self._orders_detail = QTextEdit()
        self._orders_detail.setReadOnly(True)
        self._orders_detail.setPlainText("这里会显示选中当前委托的详情。")
        layout.addWidget(detail_title)
        layout.addWidget(self._orders_detail, 1)
        return tab

    def _build_placeholder_tab(self, title_text: str, message: str) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)
        title = QLabel(title_text)
        title.setObjectName("SectionTitle")
        detail = QTextEdit()
        detail.setReadOnly(True)
        detail.setPlainText(message)
        layout.addWidget(title)
        layout.addWidget(detail, 1)
        return tab

    def _build_position_history_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        head = QHBoxLayout()
        self._position_history_summary_label = QLabel("历史仓位尚未读取。")
        self._position_history_summary_label.setObjectName("Subtle")
        self._position_history_summary_label.setWordWrap(True)
        head.addWidget(self._position_history_summary_label, 1)
        refresh_button = QPushButton("同步历史仓位")
        refresh_button.clicked.connect(self._refresh_position_history)
        head.addWidget(refresh_button)
        layout.addLayout(head)

        self._position_history_table = QTableWidget(0, 12)
        self._position_history_table.setHorizontalHeaderLabels(
            ("时间", "类型", "合约", "保证金模式", "持仓模式", "交易方向", "开仓均价", "平仓均价", "平仓数量", "手续费", "盈亏", "备注")
        )
        self._position_history_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._position_history_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._position_history_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._position_history_table.verticalHeader().setVisible(False)
        header = self._position_history_table.horizontalHeader()
        header.setStretchLastSection(False)
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(7, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(8, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(9, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(10, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(11, QHeaderView.ResizeMode.Stretch)
        self._position_history_table.itemSelectionChanged.connect(self._refresh_position_history_detail)
        layout.addWidget(self._position_history_table, 1)

        detail_title = QLabel("历史仓位详情")
        detail_title.setObjectName("SectionTitle")
        self._position_history_detail = QTextEdit()
        self._position_history_detail.setReadOnly(True)
        self._position_history_detail.setPlainText("这里会显示选中历史仓位的详情。")
        layout.addWidget(detail_title)
        layout.addWidget(self._position_history_detail, 1)
        return tab

    def _refresh_profiles(self) -> None:
        snapshots, _selected = load_profile_snapshots()
        self._profile_snapshots = snapshots

    def _populate_profile_combo(self) -> None:
        self._profile_switch_guard = True
        self._profile_combo.clear()
        names = profile_names()
        if names:
            self._profile_combo.addItems(names)
            target = self._last_profile_name or names[0]
            index = self._profile_combo.findText(target)
            self._profile_combo.setCurrentIndex(index if index >= 0 else 0)
        else:
            self._profile_combo.addItem("未配置")
        self._profile_switch_guard = False

    def _ensure_runtime_ready(self, *, force_unlock: bool) -> bool:
        profile_name = self._current_profile_name()
        if not profile_name:
            QMessageBox.warning(self, "无法刷新", "当前未配置可用的 API Profile。")
            return False
        if force_unlock and not ensure_profile_unlocked(self, profile_name, self._profile_snapshots, self._unlocked_profiles):
            return False
        runtime = load_runtime(profile_name)
        if runtime is None:
            QMessageBox.warning(self, "无法刷新", f"API 配置 {profile_name} 不可用，请检查凭证。")
            return False
        self._runtime = runtime
        self._last_profile_name = profile_name
        return True

    def _current_profile_name(self) -> str:
        text = self._profile_combo.currentText().strip()
        return "" if text == "未配置" else text

    def _stop_private_threads(self) -> None:
        for thread in (self._account_feed, self._order_feed):
            if thread is None:
                continue
            thread.stop()
            if thread.isRunning() and not thread.wait(1600):
                thread.terminate()
                thread.wait(1600)
        self._account_feed = None
        self._order_feed = None

    def _start_private_threads(self, *, force_restart: bool = False) -> None:
        if self._runtime is None:
            return
        if force_restart:
            self._stop_private_threads()
        elif self._account_feed is not None and self._account_feed.isRunning():
            return

        self._account_feed = AccountFeedThread(self._runtime)
        self._order_feed = OrderFeedThread(self._runtime)
        self._account_feed.positions_ready.connect(self._apply_positions_summary)
        self._account_feed.payload_ready.connect(self._apply_positions_payload)
        self._account_feed.status_changed.connect(self._set_account_status)
        self._order_feed.orders_ready.connect(self._apply_orders)
        self._order_feed.status_changed.connect(self._set_order_status)
        self._account_feed.start()
        self._order_feed.start()
        self._start_position_history_refresh(force_restart=force_restart)

    def _load_position_notes(self) -> None:
        snapshot = load_position_notes_snapshot()
        current = snapshot.get("current_notes", []) if isinstance(snapshot, dict) else []
        history = snapshot.get("history_notes", []) if isinstance(snapshot, dict) else []
        self._current_notes = {
            str(item.get("record_key", "")).strip(): dict(item)
            for item in current
            if isinstance(item, dict) and str(item.get("record_key", "")).strip()
        }
        self._history_notes = {
            str(item.get("record_key", "")).strip(): dict(item)
            for item in history
            if isinstance(item, dict) and str(item.get("record_key", "")).strip()
        }

    def _save_position_notes(self) -> None:
        save_position_notes_snapshot(
            current_notes=list(self._current_notes.values()),
            history_notes=list(self._history_notes.values()),
        )

    def _note_environment(self) -> str:
        if self._runtime is None:
            return "live"
        return str(self._runtime.environment or "live").strip().lower() or "live"

    def _current_note_text(self, position: OkxPosition) -> str:
        key = _position_note_current_key(self._last_profile_name, self._note_environment(), position)
        record = self._current_notes.get(key)
        return _normalize_position_note_text(record.get("note", "")) if isinstance(record, dict) else ""

    def _current_note_summary(self, position: OkxPosition) -> str:
        return _format_position_note_summary(self._current_note_text(position))

    def _current_note_map(self) -> dict[str, str]:
        return {_position_tree_row_id(item): self._current_note_text(item) for item in self._raw_positions}

    def _selected_payload(self) -> dict[str, object] | None:
        item = self._position_tree.currentItem()
        if item is None:
            return None
        row_key = item.data(0, Qt.ItemDataRole.UserRole)
        if not isinstance(row_key, str):
            return None
        return self._position_row_payloads.get(row_key)

    def _selected_position(self) -> OkxPosition | None:
        payload = self._selected_payload()
        if payload is None or payload.get("kind") != "position":
            return None
        position = payload.get("item")
        return position if isinstance(position, OkxPosition) else None

    def _selected_option_for_shortcut(self) -> OkxPosition | None:
        position = self._selected_position()
        if position is None or position.inst_type != "OPTION":
            return None
        return position

    def _position_history_note_text(self, item: OkxPositionHistoryItem) -> str:
        key = _position_history_note_key(self._last_profile_name, self._note_environment(), item)
        record = self._history_notes.get(key)
        return _normalize_position_note_text(record.get("note", "")) if isinstance(record, dict) else ""

    def _render_position_history_table(self) -> None:
        if not hasattr(self, "_position_history_table"):
            return
        selected_row = self._position_history_table.currentRow()
        selected_key = None
        if 0 <= selected_row < len(self._position_history_items):
            current = self._position_history_items[selected_row]
            selected_key = (current.update_time, current.inst_id, current.pos_side, current.direction)
        self._position_history_table.setRowCount(len(self._position_history_items))
        for row, item in enumerate(self._position_history_items):
            values = (
                _format_okx_ms_timestamp(item.update_time),
                item.inst_type or "-",
                item.inst_id or "-",
                _format_margin_mode(item.mgn_mode or ""),
                _format_history_side(None, item.pos_side or item.direction),
                _format_position_history_trade_side(item),
                _format_position_history_price(item.open_avg_price, item.inst_id, item.inst_type),
                _format_position_history_price(item.close_avg_price, item.inst_id, item.inst_type),
                _format_position_history_size(item, self._position_history_instruments),
                _format_position_history_fee_cell(item, self._position_history_usdt_prices),
                _format_position_history_pnl(item.pnl, item, usdt_prices=self._position_history_usdt_prices),
                _position_history_note_summary_text(item, self._position_history_note_text(item)),
            )
            for column, value in enumerate(values):
                cell = QTableWidgetItem(str(value))
                if column not in {2, 11}:
                    cell.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter))
                self._position_history_table.setItem(row, column, cell)
        self._position_history_summary_label.setText(f"历史仓位：{len(self._position_history_items)} 条")
        target_row = -1
        if selected_key is not None:
            for index, item in enumerate(self._position_history_items):
                key = (item.update_time, item.inst_id, item.pos_side, item.direction)
                if key == selected_key:
                    target_row = index
                    break
        elif self._position_history_items:
            target_row = 0
        if target_row >= 0:
            self._position_history_table.selectRow(target_row)
        else:
            self._position_history_detail.setPlainText("这里会显示选中历史仓位的详情。")
        self._refresh_position_history_detail()

    def _open_position_protection_dialog(self) -> None:
        position = self._selected_option_for_shortcut()
        if position is None:
            QMessageBox.information(self, "设置期权保护", "请先在当前持仓里选中一条期权仓位。")
            return
        if self._protection_dialog is None:
            self._protection_dialog = PositionProtectionDialog(
                manager=self._protection_manager,
                client=self._shared_client,
                runtime_provider=lambda: self._runtime,
                selected_option_provider=self._selected_option_for_shortcut,
                parent=self,
            )
        self._protection_dialog._refresh_from_selection(force=True)
        self._protection_dialog.show()
        self._protection_dialog.raise_()
        self._protection_dialog.activateWindow()

    def _open_option_roll_window(self) -> None:
        position = self._selected_option_for_shortcut()
        if position is None:
            QMessageBox.information(self, "展期建议", "请先在当前持仓中选中一条期权持仓。")
            return
        if not is_short_option_position(position):
            QMessageBox.information(self, "展期建议", "展期建议第一版只支持期权卖出方向持仓。")
            return
        instrument = self._position_instruments.get(position.inst_id)
        if instrument is None:
            try:
                instrument = self._shared_client.get_instrument(position.inst_id)
            except Exception as exc:
                QMessageBox.critical(self, "展期建议", f"读取合约信息失败：{exc}")
                return
        ticker = self._position_tickers.get(position.inst_id)
        if ticker is None:
            try:
                ticker = self._shared_client.get_ticker(position.inst_id)
            except Exception as exc:
                QMessageBox.critical(self, "展期建议", f"读取行情失败：{exc}")
                return
        try:
            self._legacy_option_tools.open_option_roll(
                position=position,
                instrument=instrument,
                ticker=ticker,
                api_name=self._last_profile_name or "",
            )
        except Exception as exc:
            QMessageBox.critical(self, "展期建议", f"打开展期建议失败：{exc}")

    def edit_selected_position_note(self) -> None:
        position = self._selected_position()
        if position is None:
            QMessageBox.information(self, "备注", "请先在当前持仓里选中一条具体持仓。")
            return
        dialog = NoteEditorDialog(
            title="编辑持仓备注",
            prompt=f"为 {position.inst_id} 填写备注。留空后保存会清空当前持仓备注。",
            initial_value=self._current_note_text(position),
            parent=self,
        )
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return
        record_key = _position_note_current_key(self._last_profile_name, self._note_environment(), position)
        if dialog.result_text:
            previous = self._current_notes.get(record_key)
            record = _build_current_position_note_record(
                profile_name=self._last_profile_name,
                environment=self._note_environment(),
                position=position,
                note=dialog.result_text,
                now_ms=int(time.time() * 1000),
                previous=previous,
            )
            if record is not None:
                self._current_notes[record_key] = record
        else:
            self._current_notes.pop(record_key, None)
        self._save_position_notes()
        self._render_positions_tree()

    def open_positions_column_window(self) -> None:
        dialog = ColumnSettingsDialog(
            column_defs=POSITION_COLUMNS,
            visible_columns=set(self._visible_column_ids),
            toggle_callback=self._toggle_column_visibility,
            parent=self,
        )
        dialog.exec()

    def _toggle_column_visibility(self, column_id: str) -> None:
        if column_id in self._visible_column_ids:
            if len(self._visible_column_ids) > 1:
                self._visible_column_ids.remove(column_id)
        else:
            self._visible_column_ids.add(column_id)
        self._apply_column_visibility()
        self._schedule_positions_view_prefs_save()

    def _apply_column_visibility(self) -> None:
        for index, (column_id, _heading, _width, _alignment) in enumerate(POSITION_COLUMNS, start=1):
            self._position_tree.setColumnHidden(index, column_id not in self._visible_column_ids)

    def _load_positions_view_prefs(self) -> None:
        try:
            snapshot = load_account_positions_home_view_prefs()
        except Exception:
            return
        raw_visible_columns = snapshot.get("visible_columns")
        if isinstance(raw_visible_columns, list):
            loaded_visible_columns = {
                str(item).strip()
                for item in raw_visible_columns
                if str(item).strip() in {column_id for column_id, *_rest in POSITION_COLUMNS}
            }
            if loaded_visible_columns:
                self._visible_column_ids = loaded_visible_columns
        raw_tree_column_widths = snapshot.get("tree_column_widths")
        if isinstance(raw_tree_column_widths, dict):
            self._tree_column_width_overrides = {
                str(key).strip(): int(value)
                for key, value in raw_tree_column_widths.items()
                if str(key).strip() and str(value).strip().isdigit() and int(value) > 0
            }

    def _apply_tree_column_width_overrides(self) -> None:
        if not hasattr(self, "_position_tree"):
            return
        label_width = self._tree_column_width_overrides.get("__label__")
        if label_width:
            self._position_tree.setColumnWidth(0, label_width)
        for index, (column_id, _heading, _width, _alignment) in enumerate(POSITION_COLUMNS, start=1):
            width = self._tree_column_width_overrides.get(column_id)
            if width:
                self._position_tree.setColumnWidth(index, width)

    @Slot()
    def _schedule_positions_view_prefs_save(self, *_args: object) -> None:
        if not hasattr(self, "_positions_view_prefs_save_timer"):
            return
        self._positions_view_prefs_save_timer.start(400)

    def _collect_tree_column_widths(self) -> dict[str, int]:
        if not hasattr(self, "_position_tree"):
            return dict(self._tree_column_width_overrides)
        widths = {"__label__": self._position_tree.columnWidth(0)}
        for index, (column_id, _heading, _width, _alignment) in enumerate(POSITION_COLUMNS, start=1):
            widths[column_id] = self._position_tree.columnWidth(index)
        return widths

    @Slot()
    def _save_positions_view_prefs_now(self) -> None:
        if hasattr(self, "_positions_view_prefs_save_timer") and self._positions_view_prefs_save_timer.isActive():
            self._positions_view_prefs_save_timer.stop()
        try:
            save_account_positions_home_view_prefs(
                visible_columns=sorted(self._visible_column_ids),
                tree_column_widths=self._collect_tree_column_widths(),
            )
        except Exception:
            return

    def _visible_position_list(self) -> list[OkxPosition]:
        inst_type = str(self._type_combo.currentData() or "").strip().upper()
        keyword = self._keyword_edit.text()
        return _filter_positions(
            self._raw_positions,
            inst_type=inst_type,
            keyword=keyword,
            note_texts=self._current_note_map(),
        )

    def _render_positions_tree(self) -> None:
        self._visible_positions = self._visible_position_list()
        selected_key = ""
        current = self._position_tree.currentItem()
        if current is not None:
            data = current.data(0, Qt.ItemDataRole.UserRole)
            if isinstance(data, str):
                selected_key = data

        self._position_tree.clear()
        self._position_row_payloads.clear()
        groups = _group_positions_for_tree(self._visible_positions)
        bold_font = QFont()
        bold_font.setBold(True)

        for asset_label, buckets in groups.items():
            asset_id = _asset_group_row_id(asset_label)
            asset_positions = [item for bucket in buckets.values() for item in bucket]
            asset_metrics = _aggregate_position_metrics(asset_positions, self._upl_usdt_prices, self._position_instruments)
            asset_item = self._make_tree_item(
                row_key=asset_id,
                label=f"{asset_label} 风险单元",
                values=_build_group_row_values("组合", asset_metrics),
                kind="group",
                payload_item=asset_positions,
                payload_metrics=asset_metrics,
            )
            asset_item.setFont(0, bold_font)
            self._position_tree.addTopLevelItem(asset_item)
            asset_item.setExpanded(asset_id in self._expanded_row_keys)

            for bucket_label, bucket_positions in buckets.items():
                if bucket_label == "__DIRECT__":
                    for position in bucket_positions:
                        asset_item.addChild(self._build_position_item(position))
                    continue
                bucket_id = _bucket_group_row_id(asset_label, bucket_label)
                bucket_metrics = _aggregate_position_metrics(
                    bucket_positions,
                    self._upl_usdt_prices,
                    self._position_instruments,
                )
                bucket_item = self._make_tree_item(
                    row_key=bucket_id,
                    label=bucket_label,
                    values=_build_group_row_values("分组", bucket_metrics),
                    kind="group",
                    payload_item=bucket_positions,
                    payload_metrics=bucket_metrics,
                )
                bucket_item.setFont(0, bold_font)
                asset_item.addChild(bucket_item)
                bucket_item.setExpanded(bucket_id in self._expanded_row_keys)
                for position in bucket_positions:
                    bucket_item.addChild(self._build_position_item(position))

        self._positions_hint.setText(f"当前显示 {len(self._visible_positions)} 条持仓 | 点击任一行查看详情。")
        self._update_summary_text()
        self._restore_tree_selection(selected_key)
        self._update_filter_shortcuts()
        self._sync_order_watchlist()
        visible_inst_ids = {item.inst_id.strip().upper() for item in self._visible_positions}
        self._visible_orders = [
            item for item in self._orders if not visible_inst_ids or item.inst_id.strip().upper() in visible_inst_ids
        ]
        self._refresh_current_orders_table()
        self._refresh_detail()
        self._update_expand_toggle_button()

    def _make_tree_item(
        self,
        *,
        row_key: str,
        label: str,
        values: tuple[str, ...],
        kind: str,
        payload_item: object,
        payload_metrics: dict[str, object] | None,
    ) -> QTreeWidgetItem:
        item = QTreeWidgetItem([label, *list(values)])
        item.setData(0, Qt.ItemDataRole.UserRole, row_key)
        self._position_row_payloads[row_key] = {
            "kind": kind,
            "label": label,
            "item": payload_item,
            "metrics": payload_metrics,
        }
        for index, (_column_id, _heading, _width, alignment) in enumerate(POSITION_COLUMNS, start=1):
            item.setTextAlignment(index, int(alignment | Qt.AlignmentFlag.AlignVCenter))
        return item

    def _build_position_item(self, position: OkxPosition) -> QTreeWidgetItem:
        row_key = _position_tree_row_id(position)
        label = position.inst_id
        if position.pos_side and position.pos_side.lower() != "net":
            label = f"{label} [{position.pos_side}]"
        values = (
            position.inst_type,
            _format_margin_mode(position.mgn_mode),
            _format_position_option_price_component(position, self._upl_usdt_prices, component="time_value"),
            _format_position_option_component_usdt(position, self._upl_usdt_prices, component="time_value"),
            _format_position_option_price_component(position, self._upl_usdt_prices, component="intrinsic_value"),
            _format_position_option_component_usdt(position, self._upl_usdt_prices, component="intrinsic_value"),
            _format_position_quote_price(position, self._position_instruments, self._position_tickers, side="bid"),
            _format_position_quote_price_usdt(position, self._position_tickers, self._upl_usdt_prices, side="bid"),
            _format_position_quote_price(position, self._position_instruments, self._position_tickers, side="ask"),
            _format_position_quote_price_usdt(position, self._position_tickers, self._upl_usdt_prices, side="ask"),
            _format_mark_price(position),
            _format_position_mark_price_usdt(position, self._upl_usdt_prices),
            _format_position_avg_price(position, self._position_instruments),
            _format_position_avg_price_usdt(position, self._upl_usdt_prices),
            _format_optional_approx_usdt(
                _position_signed_open_value_approx_usdt(position, self._position_instruments, self._upl_usdt_prices)
            ),
            _format_position_size(position, self._position_instruments),
            _format_option_trade_side_display(position),
            _format_position_unrealized_pnl(position),
            _format_optional_usdt(_position_unrealized_pnl_usdt(position, self._upl_usdt_prices)),
            _format_position_realized_pnl(position),
            _format_optional_usdt(_position_realized_pnl_usdt(position, self._upl_usdt_prices)),
            _format_position_market_value(position, self._position_instruments, self._upl_usdt_prices),
            _format_optional_decimal(position.liquidation_price),
            _format_ratio(position.margin_ratio, places=2),
            _format_optional_integer(position.initial_margin),
            _format_optional_integer(position.maintenance_margin),
            _format_optional_decimal_fixed(_position_delta_value(position, self._position_instruments), places=5),
            _format_optional_decimal_fixed(position.gamma, places=5),
            _format_optional_decimal_fixed(position.vega, places=5),
            _format_optional_decimal_fixed(position.theta, places=5),
            _format_optional_usdt_precise(_position_theta_usdt(position, self._upl_usdt_prices), places=2),
            self._current_note_summary(position),
        )
        item = self._make_tree_item(
            row_key=row_key,
            label=label,
            values=values,
            kind="position",
            payload_item=position,
            payload_metrics=None,
        )
        pnl_color = None
        if position.unrealized_pnl is not None:
            pnl_color = QColor("#13803d" if position.unrealized_pnl > 0 else "#c23b3b" if position.unrealized_pnl < 0 else "#1f2937")
        if pnl_color is not None:
            for index in (18, 19, 20, 21):
                item.setForeground(index, pnl_color)
        if str(position.mgn_mode or "").strip().lower() == "cross":
            for index in range(0, self._position_tree.columnCount()):
                item.setBackground(index, QColor("#f4f8ff"))
        elif str(position.mgn_mode or "").strip().lower() == "isolated":
            for index in range(0, self._position_tree.columnCount()):
                item.setBackground(index, QColor("#fff4e5"))
        return item

    def _restore_tree_selection(self, selected_key: str) -> None:
        target_item = None
        if selected_key:
            for item in self._iter_tree_items():
                data = item.data(0, Qt.ItemDataRole.UserRole)
                if data == selected_key:
                    target_item = item
                    break
        if target_item is None:
            target_item = self._position_tree.topLevelItem(0)
        if target_item is not None:
            self._position_tree.setCurrentItem(target_item)

    def _iter_tree_items(self) -> list[QTreeWidgetItem]:
        items: list[QTreeWidgetItem] = []

        def _walk(parent: QTreeWidgetItem) -> None:
            items.append(parent)
            for index in range(parent.childCount()):
                _walk(parent.child(index))

        for index in range(self._position_tree.topLevelItemCount()):
            _walk(self._position_tree.topLevelItem(index))
        return items

    def _group_tree_items(self) -> list[QTreeWidgetItem]:
        result: list[QTreeWidgetItem] = []
        for item in self._iter_tree_items():
            row_key = item.data(0, Qt.ItemDataRole.UserRole)
            payload = self._position_row_payloads.get(row_key) if isinstance(row_key, str) else None
            if isinstance(payload, dict) and payload.get("kind") == "group":
                result.append(item)
        return result

    def _all_group_rows_expanded(self) -> bool:
        group_items = self._group_tree_items()
        return bool(group_items) and all(item.isExpanded() for item in group_items)

    def _update_expand_toggle_button(self) -> None:
        if not hasattr(self, "_expand_toggle_button"):
            return
        self._expand_toggle_button.setText("折叠全部" if self._all_group_rows_expanded() else "展开全部")

    def _toggle_all_positions(self) -> None:
        if self._all_group_rows_expanded():
            self._collapse_all_positions()
            return
        self._expand_all_positions()

    def _expand_all_positions(self) -> None:
        for item in self._group_tree_items():
            row_key = item.data(0, Qt.ItemDataRole.UserRole)
            if not isinstance(row_key, str):
                continue
            self._expanded_row_keys.add(row_key)
            item.setExpanded(True)
        self._update_expand_toggle_button()

    def _collapse_all_positions(self) -> None:
        for item in self._group_tree_items():
            row_key = item.data(0, Qt.ItemDataRole.UserRole)
            if not isinstance(row_key, str):
                continue
            self._expanded_row_keys.discard(row_key)
            item.setExpanded(False)
        self._update_expand_toggle_button()

    def _on_tree_item_expanded(self, item: QTreeWidgetItem) -> None:
        row_key = item.data(0, Qt.ItemDataRole.UserRole)
        if not isinstance(row_key, str):
            return
        payload = self._position_row_payloads.get(row_key)
        if isinstance(payload, dict) and payload.get("kind") == "group":
            self._expanded_row_keys.add(row_key)
            self._update_expand_toggle_button()

    def _on_tree_item_collapsed(self, item: QTreeWidgetItem) -> None:
        row_key = item.data(0, Qt.ItemDataRole.UserRole)
        if not isinstance(row_key, str):
            return
        self._expanded_row_keys.discard(row_key)
        self._update_expand_toggle_button()

    def _update_summary_text(self) -> None:
        total_count = len(self._raw_positions)
        visible_count = len(self._visible_positions)
        parts = [
            f"API配置：{self._last_profile_name or '-'}",
            self._account_status.text(),
        ]
        if total_count:
            text = f"当前仓位（{total_count}）"
            if visible_count != total_count:
                text += f"，当前显示 {visible_count}"
            parts.append(text)
        else:
            parts.append("当前没有持仓")
        keyword = self._keyword_edit.text().strip().upper()
        type_label = self._type_combo.currentText().strip()
        if keyword or type_label != "全部类型":
            parts.append(f"筛选：{type_label if type_label != '全部类型' else ''} {'| ' + keyword if keyword else ''}".strip())
        self._summary_label.setText(" | ".join(part for part in parts if part))

    def _update_filter_shortcuts(self) -> None:
        position = self._selected_option_for_shortcut()
        contract, expiry_prefix = _option_search_shortcuts(position.inst_id if position else "")
        enabled = bool(contract)
        self._apply_contract_button.setEnabled(enabled)
        self._apply_expiry_button.setEnabled(enabled)
        if not enabled:
            self._filter_hint.setText("选中期权后，可一键带入合约或到期前缀。")
            return
        self._filter_hint.setText(f"已选期权：{contract} | 快捷筛选：合约={contract} | 到期前缀={expiry_prefix}")

    def apply_selected_option_to_position_search(self) -> None:
        position = self._selected_option_for_shortcut()
        contract, _expiry_prefix = _option_search_shortcuts(position.inst_id if position else "")
        if not contract:
            QMessageBox.information(self, "快捷筛选", "请先在当前持仓里选中一条期权合约。")
            return
        self._keyword_edit.setText(contract)

    def apply_selected_option_expiry_prefix_to_position_search(self) -> None:
        position = self._selected_option_for_shortcut()
        _contract, expiry_prefix = _option_search_shortcuts(position.inst_id if position else "")
        if not expiry_prefix:
            QMessageBox.information(self, "快捷筛选", "请先在当前持仓里选中一条期权合约。")
            return
        self._keyword_edit.setText(expiry_prefix)

    def _clear_filters(self) -> None:
        self._type_combo.setCurrentIndex(0)
        self._keyword_edit.clear()

    def _sync_order_watchlist(self) -> None:
        if self._order_feed is None:
            return
        self._order_feed.set_watched_inst_ids({item.inst_id for item in self._visible_positions})

    def _refresh_current_orders_table(self) -> None:
        if not hasattr(self, "_orders_table"):
            return
        selected_ord_id = ""
        current_row = self._orders_table.currentRow()
        if 0 <= current_row < len(self._visible_orders):
            selected_ord_id = self._visible_orders[current_row].ord_id
        self._orders_summary_label.setText(
            f"当前委托：{len(self._visible_orders)} 条 | 仅显示当前持仓相关合约。"
        )
        self._orders_table.setRowCount(len(self._visible_orders))
        for row, order in enumerate(self._visible_orders):
            values = (
                _format_okx_ms_timestamp(order.update_time or order.created_time),
                order.inst_id,
                order.inst_type or "-",
                _format_trade_order_state(order.state),
                _format_history_side(order.side or "-", order.pos_side or ""),
                order.ord_type or "-",
                _format_trade_order_price(order.price, order.inst_id, order.inst_type or ""),
                _format_trade_order_size(order.size),
                _format_trade_order_size(order.filled_size),
                order.td_mode or "-",
                order.client_order_id or "-",
            )
            for column, value in enumerate(values):
                item = QTableWidgetItem(str(value))
                if column not in {1, 10}:
                    item.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter))
                self._orders_table.setItem(row, column, item)
        target_row = -1
        if selected_ord_id:
            for index, order in enumerate(self._visible_orders):
                if order.ord_id == selected_ord_id:
                    target_row = index
                    break
        elif self._visible_orders:
            target_row = 0
        if target_row >= 0:
            self._orders_table.selectRow(target_row)
        else:
            self._orders_detail.setPlainText("这里会显示选中当前委托的详情。")
        self._refresh_current_order_detail()

    def _refresh_current_order_detail(self) -> None:
        if not hasattr(self, "_orders_table"):
            return
        row = self._orders_table.currentRow()
        if row < 0 or row >= len(self._visible_orders):
            self._orders_detail.setPlainText("这里会显示选中当前委托的详情。")
            return
        order = self._visible_orders[row]
        lines = [
            f"合约：{order.inst_id or '-'}",
            f"类型：{order.inst_type or '-'}",
            f"状态：{_format_trade_order_state(order.state)}",
            f"方向：{_format_history_side(order.side or '-', order.pos_side or '')}",
            f"交易模式：{order.td_mode or '-'}",
            f"委托类型：{order.ord_type or '-'}",
            f"委托价：{_format_trade_order_price(order.price, order.inst_id, order.inst_type or '')}",
            f"委托量：{_format_trade_order_size(order.size)}",
            f"已成交：{_format_trade_order_size(order.filled_size)}",
            f"成交均价：{_format_trade_order_price(order.avg_price, order.inst_id, order.inst_type or '')}",
            f"更新时间：{_format_okx_ms_timestamp(order.update_time)}",
            f"创建时间：{_format_okx_ms_timestamp(order.created_time)}",
            f"reduceOnly：{'是' if order.reduce_only is True else '否' if order.reduce_only is False else '-'}",
            f"ordId：{order.ord_id or '-'}",
            f"clOrdId：{order.client_order_id or '-'}",
            "",
            "原始 WS 回报：",
            json.dumps(order.raw, ensure_ascii=False, indent=2, sort_keys=True),
        ]
        self._orders_detail.setPlainText("\n".join(lines))

    def _refresh_position_history_detail(self) -> None:
        if not hasattr(self, "_position_history_table"):
            return
        row = self._position_history_table.currentRow()
        if row < 0 or row >= len(self._position_history_items):
            self._position_history_detail.setPlainText("这里会显示选中历史仓位的详情。")
            return
        item = self._position_history_items[row]
        self._position_history_detail.setPlainText(
            _build_position_history_detail_text(
                item,
                self._position_history_usdt_prices,
                self._position_history_instruments,
                note=self._position_history_note_text(item),
            )
        )

    def _refresh_detail(self) -> None:
        payload = self._selected_payload()
        if payload is None:
            self._detail_text.setPlainText("点击任一行查看持仓详情。")
            return
        if payload.get("kind") == "position":
            position = payload.get("item")
            if isinstance(position, OkxPosition):
                self._detail_text.setPlainText(
                    _build_position_detail_text(
                        position,
                        self._upl_usdt_prices,
                        self._position_instruments,
                        note=self._current_note_text(position),
                    )
                )
                return
        label = payload.get("label")
        positions = payload.get("item")
        metrics = payload.get("metrics")
        if isinstance(label, str) and isinstance(positions, list) and isinstance(metrics, dict):
            self._detail_text.setPlainText(
                _build_group_detail_text(
                    label,
                    positions,
                    metrics,
                    self._upl_usdt_prices,
                    self._position_instruments,
                )
            )
            return
        self._detail_text.setPlainText("点击任一行查看持仓详情。")

    def _show_account_overview(self) -> None:
        lines = [
            f"当前 API：{self._last_profile_name or '-'}",
            f"持仓总数：{len(self._raw_positions)}",
            f"当前显示：{len(self._visible_positions)}",
            f"当前委托：{len(self._visible_orders)}",
            f"当前筛选：{self._keyword_edit.text().strip() or '-'}",
        ]
        dialog = AccountOverviewDialog(summary_text="\n".join(lines), parent=self)
        dialog.exec()

    def _toggle_detail_panel(self) -> None:
        visible = not self._detail_panel.isHidden()
        self._detail_panel.setVisible(not visible)
        self._update_panel_toggle_buttons()

    def _toggle_history_panel(self) -> None:
        visible = not self._history_panel.isHidden()
        self._history_panel.setVisible(not visible)
        self._update_panel_toggle_buttons()

    def _update_panel_toggle_buttons(self) -> None:
        if hasattr(self, "_detail_toggle_button") and hasattr(self, "_detail_panel"):
            self._detail_toggle_button.setText("折叠持仓详情" if not self._detail_panel.isHidden() else "展开持仓详情")
        if hasattr(self, "_history_toggle_button") and hasattr(self, "_history_panel"):
            self._history_toggle_button.setText("折叠历史区域" if not self._history_panel.isHidden() else "展开历史区域")

    def _show_not_ready_action(self) -> None:
        QMessageBox.information(self, "迁移中", "这个入口已经预留到主页上，下一步会按旧页面逻辑继续接入。")

    def _apply_filters(self, *_args: object) -> None:
        self._render_positions_tree()

    def _on_profile_changed(self, *_args: object) -> None:
        if self._profile_switch_guard:
            return
        target = self._current_profile_name()
        if not target or target == self._last_profile_name:
            return
        if not ensure_profile_unlocked(self, target, self._profile_snapshots, self._unlocked_profiles):
            self._profile_switch_guard = True
            previous_index = self._profile_combo.findText(self._last_profile_name)
            self._profile_combo.setCurrentIndex(previous_index if previous_index >= 0 else 0)
            self._profile_switch_guard = False
            return
        runtime = load_runtime(target)
        if runtime is None:
            QMessageBox.warning(self, "切换失败", f"API 配置 {target} 不可用，请检查凭证。")
            return
        self._runtime = runtime
        self._last_profile_name = target
        self._start_private_threads(force_restart=True)

    def _on_position_selected(self) -> None:
        self._update_filter_shortcuts()
        self._refresh_detail()

    @Slot(str)
    def _set_account_status(self, text: str) -> None:
        self._account_status.setText(text)
        self._update_summary_text()

    @Slot(str)
    def _set_order_status(self, text: str) -> None:
        self._order_status.setText(text)

    @Slot(str)
    def _set_position_history_status(self, text: str) -> None:
        if hasattr(self, "_position_history_summary_label"):
            self._position_history_summary_label.setText(text)

    @Slot(object)
    def _apply_positions_summary(self, _positions: object) -> None:
        self._status_badge.setText("正常")

    @Slot(object)
    def _apply_positions_payload(self, payload: object) -> None:
        if not isinstance(payload, dict):
            return
        positions = payload.get("positions")
        self._raw_positions = list(positions) if isinstance(positions, list) else []
        instruments = payload.get("position_instruments")
        tickers = payload.get("position_tickers")
        prices = payload.get("upl_usdt_prices")
        self._position_instruments = dict(instruments) if isinstance(instruments, dict) else {}
        self._position_tickers = dict(tickers) if isinstance(tickers, dict) else {}
        self._upl_usdt_prices = dict(prices) if isinstance(prices, dict) else {}
        if self._last_profile_name:
            _reconcile_current_position_note_records(
                self._current_notes,
                profile_name=self._last_profile_name,
                environment=self._note_environment(),
                positions=self._raw_positions,
                now_ms=int(time.time() * 1000),
            )
        self._render_positions_tree()

    @Slot(object)
    def _apply_orders(self, orders: object) -> None:
        self._orders = list(orders) if isinstance(orders, list) else []
        visible_inst_ids = {item.inst_id for item in self._visible_positions}
        self._visible_orders = [
            item for item in self._orders if not visible_inst_ids or item.inst_id.strip().upper() in visible_inst_ids
        ]
        self._refresh_current_orders_table()

    @Slot(object)
    def _apply_position_history_payload(self, payload: object) -> None:
        if not isinstance(payload, dict):
            return
        items = payload.get("items")
        instruments = payload.get("instruments")
        usdt_prices = payload.get("usdt_prices")
        self._position_history_items = list(items) if isinstance(items, list) else []
        self._position_history_instruments = dict(instruments) if isinstance(instruments, dict) else {}
        self._position_history_usdt_prices = dict(usdt_prices) if isinstance(usdt_prices, dict) else {}
        self._position_history_last_sync_text = time.strftime("%H:%M:%S")
        self._render_position_history_table()

    def _build_history_tabs_v2(self) -> QWidget:
        self._history_panel = QFrame()
        self._history_panel.setObjectName("Panel")
        layout = QVBoxLayout(self._history_panel)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        self._tabs = QTabWidget()
        self._tabs.addTab(self._build_current_orders_tab_v2(), "当前委托")
        self._tabs.addTab(self._build_placeholder_tab("动态止盈接管", "动态止盈接管页保留在这里，后续继续按旧版完整迁移。"), "动态止盈接管")
        self._tabs.addTab(self._build_order_history_tab(), "历史委托")
        self._tabs.addTab(self._build_fill_history_tab(), "历史成交")
        self._tabs.addTab(self._build_position_history_tab_v2(), "历史仓位")
        layout.addWidget(self._tabs, 1)
        return self._history_panel

    def _build_current_orders_tab_v2(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        top = QHBoxLayout()
        self._orders_summary_label = QLabel("当前委托尚未读取。")
        self._orders_summary_label.setObjectName("Subtle")
        self._orders_summary_label.setWordWrap(True)
        top.addWidget(self._orders_summary_label, 1)
        for text, handler in (
            ("刷新", self.refresh_view),
            ("从选中条件单接管动态止盈", self._show_not_ready_action),
            ("撤单选中", self._show_not_ready_action),
            ("批量撤当前筛选", self._show_not_ready_action),
        ):
            button = QPushButton(text)
            button.clicked.connect(handler)
            top.addWidget(button)
        layout.addLayout(top)

        filter_row = QGridLayout()
        filter_row.setHorizontalSpacing(8)
        filter_row.setVerticalSpacing(8)
        self._pending_type_combo = QComboBox()
        self._pending_source_combo = QComboBox()
        self._pending_state_combo = QComboBox()
        for label, value in POSITION_TYPE_OPTIONS:
            self._pending_type_combo.addItem(label, value)
        for label, value in ORDER_SOURCE_FILTER_OPTIONS:
            self._pending_source_combo.addItem(label, value)
        for label, value in ORDER_STATE_FILTER_OPTIONS:
            self._pending_state_combo.addItem(label, value)
        self._pending_asset_edit = QLineEdit()
        self._pending_expiry_edit = QLineEdit()
        self._pending_keyword_edit = QLineEdit()
        for widget in (
            self._pending_type_combo,
            self._pending_source_combo,
            self._pending_state_combo,
            self._pending_asset_edit,
            self._pending_expiry_edit,
            self._pending_keyword_edit,
        ):
            if isinstance(widget, QComboBox):
                widget.currentIndexChanged.connect(self._refresh_current_orders_table)
            else:
                widget.textChanged.connect(self._refresh_current_orders_table)
        apply_button = QPushButton("应用筛选")
        apply_button.clicked.connect(self._refresh_current_orders_table)
        clear_button = QPushButton("清空筛选")
        clear_button.clicked.connect(self._clear_pending_order_filters)
        filter_row.addWidget(QLabel("类型"), 0, 0)
        filter_row.addWidget(self._pending_type_combo, 0, 1)
        filter_row.addWidget(QLabel("来源"), 0, 2)
        filter_row.addWidget(self._pending_source_combo, 0, 3)
        filter_row.addWidget(QLabel("状态"), 0, 4)
        filter_row.addWidget(self._pending_state_combo, 0, 5)
        filter_row.addWidget(QLabel("标的"), 0, 6)
        filter_row.addWidget(self._pending_asset_edit, 0, 7)
        filter_row.addWidget(QLabel("到期前缀"), 0, 8)
        filter_row.addWidget(self._pending_expiry_edit, 0, 9)
        filter_row.addWidget(QLabel("搜索"), 0, 10)
        filter_row.addWidget(self._pending_keyword_edit, 0, 11)
        filter_row.addWidget(apply_button, 0, 12)
        filter_row.addWidget(clear_button, 0, 13)
        layout.addLayout(filter_row)

        self._orders_table = self._build_history_table(
            ("时间", "来源", "类型", "合约", "状态", "方向", "委托类型", "委托价", "委托量", "已成交", "手续费", "TP/SL", "订单ID", "clOrdId"),
            stretch_columns={3, 11, 13},
        )
        layout.addWidget(self._orders_table, 1)
        return tab

    def _build_order_history_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        top = QHBoxLayout()
        self._order_history_summary_label = QLabel("历史委托尚未读取。")
        self._order_history_summary_label.setObjectName("Subtle")
        top.addWidget(self._order_history_summary_label, 1)
        sync_button = QPushButton("同步")
        sync_button.clicked.connect(self._refresh_order_history)
        top.addWidget(sync_button)
        layout.addLayout(top)

        filter_row = QGridLayout()
        filter_row.setHorizontalSpacing(8)
        filter_row.setVerticalSpacing(8)
        self._order_history_type_combo = QComboBox()
        self._order_history_source_combo = QComboBox()
        self._order_history_state_combo = QComboBox()
        for label, value in POSITION_TYPE_OPTIONS:
            self._order_history_type_combo.addItem(label, value)
        for label, value in ORDER_SOURCE_FILTER_OPTIONS:
            self._order_history_source_combo.addItem(label, value)
        for label, value in ORDER_STATE_FILTER_OPTIONS:
            self._order_history_state_combo.addItem(label, value)
        self._order_history_asset_edit = QLineEdit()
        self._order_history_expiry_edit = QLineEdit()
        self._order_history_keyword_edit = QLineEdit()
        for widget in (
            self._order_history_type_combo,
            self._order_history_source_combo,
            self._order_history_state_combo,
            self._order_history_asset_edit,
            self._order_history_expiry_edit,
            self._order_history_keyword_edit,
        ):
            if isinstance(widget, QComboBox):
                widget.currentIndexChanged.connect(self._refresh_order_history_table)
            else:
                widget.textChanged.connect(self._refresh_order_history_table)
        order_apply = QPushButton("应用筛选")
        order_apply.clicked.connect(self._refresh_order_history_table)
        order_clear = QPushButton("清空筛选")
        order_clear.clicked.connect(self._clear_order_history_filters)
        filter_row.addWidget(QLabel("类型"), 0, 0)
        filter_row.addWidget(self._order_history_type_combo, 0, 1)
        filter_row.addWidget(QLabel("来源"), 0, 2)
        filter_row.addWidget(self._order_history_source_combo, 0, 3)
        filter_row.addWidget(QLabel("状态"), 0, 4)
        filter_row.addWidget(self._order_history_state_combo, 0, 5)
        filter_row.addWidget(QLabel("标的"), 0, 6)
        filter_row.addWidget(self._order_history_asset_edit, 0, 7)
        filter_row.addWidget(QLabel("到期前缀"), 0, 8)
        filter_row.addWidget(self._order_history_expiry_edit, 0, 9)
        filter_row.addWidget(QLabel("搜索"), 0, 10)
        filter_row.addWidget(self._order_history_keyword_edit, 0, 11)
        filter_row.addWidget(order_apply, 0, 12)
        filter_row.addWidget(order_clear, 0, 13)
        layout.addLayout(filter_row)

        self._order_history_table = self._build_history_table(
            ("时间", "来源", "类型", "合约", "状态", "方向", "委托类型", "委托价", "委托量", "已成交", "手续费", "TP/SL", "订单ID", "clOrdId"),
            stretch_columns={3, 11, 13},
        )
        layout.addWidget(self._order_history_table, 1)
        return tab

    def _build_fill_history_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        top = QHBoxLayout()
        self._fill_history_summary_label = QLabel("历史成交尚未读取。")
        self._fill_history_summary_label.setObjectName("Subtle")
        top.addWidget(self._fill_history_summary_label, 1)
        more_button = QPushButton("增加100条")
        more_button.clicked.connect(self._expand_fill_history_limit)
        top.addWidget(more_button)
        layout.addLayout(top)

        filter_row = QGridLayout()
        filter_row.setHorizontalSpacing(8)
        filter_row.setVerticalSpacing(8)
        self._fill_history_type_combo = QComboBox()
        self._fill_history_side_combo = QComboBox()
        for label, value in POSITION_TYPE_OPTIONS:
            self._fill_history_type_combo.addItem(label, value)
        for label, value in HISTORY_FILL_SIDE_FILTER_OPTIONS:
            self._fill_history_side_combo.addItem(label, value)
        self._fill_history_asset_edit = QLineEdit()
        self._fill_history_expiry_edit = QLineEdit()
        self._fill_history_keyword_edit = QLineEdit()
        for widget in (
            self._fill_history_type_combo,
            self._fill_history_side_combo,
            self._fill_history_asset_edit,
            self._fill_history_expiry_edit,
            self._fill_history_keyword_edit,
        ):
            if isinstance(widget, QComboBox):
                widget.currentIndexChanged.connect(self._refresh_fill_history_table)
            else:
                widget.textChanged.connect(self._refresh_fill_history_table)
        fill_apply = QPushButton("应用筛选")
        fill_apply.clicked.connect(self._refresh_fill_history_table)
        fill_clear = QPushButton("清空筛选")
        fill_clear.clicked.connect(self._clear_fill_history_filters)
        fill_contract = QPushButton("带入合约")
        fill_contract.clicked.connect(self.apply_selected_option_to_fill_history_search)
        fill_expiry = QPushButton("带入到期前缀")
        fill_expiry.clicked.connect(self.apply_selected_option_expiry_prefix_to_fill_history_search)
        filter_row.addWidget(QLabel("类型"), 0, 0)
        filter_row.addWidget(self._fill_history_type_combo, 0, 1)
        filter_row.addWidget(QLabel("方向"), 0, 2)
        filter_row.addWidget(self._fill_history_side_combo, 0, 3)
        filter_row.addWidget(QLabel("标的"), 0, 4)
        filter_row.addWidget(self._fill_history_asset_edit, 0, 5)
        filter_row.addWidget(QLabel("到期前缀"), 0, 6)
        filter_row.addWidget(self._fill_history_expiry_edit, 0, 7)
        filter_row.addWidget(QLabel("搜索"), 0, 8)
        filter_row.addWidget(self._fill_history_keyword_edit, 0, 9)
        filter_row.addWidget(fill_contract, 0, 10)
        filter_row.addWidget(fill_expiry, 0, 11)
        filter_row.addWidget(fill_apply, 0, 12)
        filter_row.addWidget(fill_clear, 0, 13)
        layout.addLayout(filter_row)

        self._fill_history_table = self._build_history_table(
            ("时间", "类型", "合约", "方向", "成交价", "成交量", "手续费", "已实现盈亏", "成交类型"),
            stretch_columns={2},
        )
        layout.addWidget(self._fill_history_table, 1)
        return tab

    def _build_position_history_tab_v2(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        top = QHBoxLayout()
        self._position_history_summary_label = QLabel("历史仓位尚未读取。")
        self._position_history_summary_label.setObjectName("Subtle")
        self._position_history_summary_label.setWordWrap(True)
        top.addWidget(self._position_history_summary_label, 1)
        more_button = QPushButton("增加100条")
        more_button.clicked.connect(self._expand_position_history_limit)
        top.addWidget(more_button)
        edit_button = QPushButton("编辑备注")
        edit_button.clicked.connect(self.edit_selected_position_history_note)
        top.addWidget(edit_button)
        layout.addLayout(top)

        filter_row = QGridLayout()
        filter_row.setHorizontalSpacing(8)
        filter_row.setVerticalSpacing(8)
        self._position_history_type_combo = QComboBox()
        self._position_history_margin_combo = QComboBox()
        for label, value in POSITION_TYPE_OPTIONS:
            self._position_history_type_combo.addItem(label, value)
        for label, value in HISTORY_MARGIN_MODE_FILTER_OPTIONS:
            self._position_history_margin_combo.addItem(label, value)
        self._position_history_asset_edit = QLineEdit()
        self._position_history_expiry_edit = QLineEdit()
        self._position_history_keyword_edit = QLineEdit()
        self._position_history_range_start_edit = QLineEdit()
        self._position_history_range_start_edit.setPlaceholderText("YYYYMMDD")
        self._position_history_range_start_edit.setText(self._default_position_history_start_text())
        self._position_history_range_start_edit.setMaxLength(8)
        self._position_history_range_end_edit = QLineEdit()
        self._position_history_range_end_edit.setPlaceholderText("YYYYMMDD")
        self._position_history_range_end_edit.setText(self._default_position_history_end_text())
        self._position_history_range_end_edit.setMaxLength(8)
        for widget in (
            self._position_history_type_combo,
            self._position_history_margin_combo,
            self._position_history_asset_edit,
            self._position_history_expiry_edit,
            self._position_history_keyword_edit,
        ):
            if isinstance(widget, QComboBox):
                widget.currentIndexChanged.connect(self._schedule_position_history_render)
            else:
                widget.textChanged.connect(self._schedule_position_history_render)
        self._position_history_range_start_edit.editingFinished.connect(self._schedule_position_history_render)
        self._position_history_range_end_edit.editingFinished.connect(self._schedule_position_history_render)
        pos_apply = QPushButton("应用筛选")
        pos_apply.clicked.connect(self._force_position_history_render)
        pos_clear = QPushButton("清空筛选")
        pos_clear.clicked.connect(self._clear_position_history_filters)
        pos_contract = QPushButton("带入合约")
        pos_contract.clicked.connect(self.apply_selected_option_to_position_history_search)
        pos_expiry = QPushButton("带入到期前缀")
        pos_expiry.clicked.connect(self.apply_selected_option_expiry_prefix_to_position_history_search)
        filter_row.addWidget(QLabel("类型"), 0, 0)
        filter_row.addWidget(self._position_history_type_combo, 0, 1)
        filter_row.addWidget(QLabel("保证金模式"), 0, 2)
        filter_row.addWidget(self._position_history_margin_combo, 0, 3)
        filter_row.addWidget(QLabel("标的"), 0, 4)
        filter_row.addWidget(self._position_history_asset_edit, 0, 5)
        filter_row.addWidget(QLabel("到期前缀"), 0, 6)
        filter_row.addWidget(self._position_history_expiry_edit, 0, 7)
        filter_row.addWidget(QLabel("搜索"), 0, 8)
        filter_row.addWidget(self._position_history_keyword_edit, 0, 9)
        filter_row.addWidget(pos_contract, 0, 10)
        filter_row.addWidget(pos_expiry, 0, 11)
        filter_row.addWidget(pos_apply, 0, 12)
        filter_row.addWidget(pos_clear, 0, 13)
        filter_row.addWidget(QLabel("本地开始"), 1, 0)
        filter_row.addWidget(self._position_history_range_start_edit, 1, 1)
        filter_row.addWidget(QLabel("本地结束"), 1, 2)
        filter_row.addWidget(self._position_history_range_end_edit, 1, 3)
        filter_row.addWidget(QLabel("YYYYMMDD 或 YYYY-MM-DD，留空则不过滤"), 1, 4, 1, 10)
        layout.addLayout(filter_row)

        self._position_history_table = self._build_history_table(
            ("时间", "类型", "合约", "保证金模式", "持仓模式", "交易方向", "开仓均价", "平仓均价", "平仓数量", "手续费", "盈亏", "备注"),
            stretch_columns={2, 11},
        )
        layout.addWidget(self._position_history_table, 1)
        self._position_history_summary_label.setMinimumHeight(34)
        return tab

    def _build_history_table(self, headings: tuple[str, ...], *, stretch_columns: set[int]) -> QTableWidget:
        table = QTableWidget(0, len(headings))
        table.setHorizontalHeaderLabels(headings)
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.verticalHeader().setVisible(False)
        header = table.horizontalHeader()
        header.setStretchLastSection(False)
        for index in range(len(headings)):
            header.setSectionResizeMode(
                index,
                QHeaderView.ResizeMode.Stretch if index in stretch_columns else QHeaderView.ResizeMode.ResizeToContents,
            )
        return table

    def _start_private_threads(self, *, force_restart: bool = False) -> None:
        if self._runtime is None:
            return
        if force_restart:
            self._stop_private_threads()
        elif self._account_feed is not None and self._account_feed.isRunning():
            return
        self._account_feed = AccountFeedThread(self._runtime)
        self._order_feed = OrderFeedThread(self._runtime)
        self._account_feed.positions_ready.connect(self._apply_positions_summary)
        self._account_feed.payload_ready.connect(self._apply_positions_payload)
        self._account_feed.status_changed.connect(self._set_account_status)
        self._order_feed.orders_ready.connect(self._apply_orders)
        self._order_feed.status_changed.connect(self._set_order_status)
        self._account_feed.start()
        self._order_feed.start()
        self._start_order_history_refresh(force_restart=force_restart)
        self._start_fill_history_refresh(force_restart=force_restart)
        self._start_position_history_refresh(force_restart=force_restart)

    def _clear_pending_order_filters(self) -> None:
        self._pending_type_combo.setCurrentIndex(0)
        self._pending_source_combo.setCurrentIndex(0)
        self._pending_state_combo.setCurrentIndex(0)
        self._pending_asset_edit.clear()
        self._pending_expiry_edit.clear()
        self._pending_keyword_edit.clear()

    def _clear_order_history_filters(self) -> None:
        self._order_history_type_combo.setCurrentIndex(0)
        self._order_history_source_combo.setCurrentIndex(0)
        self._order_history_state_combo.setCurrentIndex(0)
        self._order_history_asset_edit.clear()
        self._order_history_expiry_edit.clear()
        self._order_history_keyword_edit.clear()

    def _clear_fill_history_filters(self) -> None:
        self._fill_history_type_combo.setCurrentIndex(0)
        self._fill_history_side_combo.setCurrentIndex(0)
        self._fill_history_asset_edit.clear()
        self._fill_history_expiry_edit.clear()
        self._fill_history_keyword_edit.clear()

    def _clear_position_history_filters(self) -> None:
        self._position_history_filter_resetting = True
        self._position_history_type_combo.setCurrentIndex(0)
        self._position_history_margin_combo.setCurrentIndex(0)
        self._position_history_asset_edit.clear()
        self._position_history_expiry_edit.clear()
        self._position_history_keyword_edit.clear()
        self._position_history_range_start_edit.setText(self._default_position_history_start_text())
        self._position_history_range_end_edit.setText(self._default_position_history_end_text())
        self._position_history_filter_resetting = False
        self._force_position_history_render()

    def apply_selected_option_to_fill_history_search(self) -> None:
        inst_id = self._selected_option_inst_id_for_fill_history_shortcut()
        contract, _expiry_prefix = _option_search_shortcuts(inst_id)
        if not contract:
            QMessageBox.information(self, "带入合约", "请先在历史成交里选中一条期权记录，或在当前持仓里选中一条期权持仓。")
            return
        self._fill_history_keyword_edit.setText(contract)
        self._refresh_fill_history_table()

    def apply_selected_option_expiry_prefix_to_fill_history_search(self) -> None:
        inst_id = self._selected_option_inst_id_for_fill_history_shortcut()
        _contract, expiry_prefix = _option_search_shortcuts(inst_id)
        if not expiry_prefix:
            QMessageBox.information(self, "带入到期前缀", "请先在历史成交里选中一条期权记录，或在当前持仓里选中一条期权持仓。")
            return
        self._fill_history_expiry_edit.setText(expiry_prefix)
        self._refresh_fill_history_table()

    def apply_selected_option_to_position_history_search(self) -> None:
        inst_id = self._selected_option_inst_id_for_position_history_shortcut()
        contract, _expiry_prefix = _option_search_shortcuts(inst_id)
        if not contract:
            QMessageBox.information(self, "带入合约", "请先在历史仓位里选中一条期权记录，或在当前持仓里选中一条期权持仓。")
            return
        self._position_history_keyword_edit.setText(contract)
        self._force_position_history_render()

    def apply_selected_option_expiry_prefix_to_position_history_search(self) -> None:
        inst_id = self._selected_option_inst_id_for_position_history_shortcut()
        _contract, expiry_prefix = _option_search_shortcuts(inst_id)
        if not expiry_prefix:
            QMessageBox.information(self, "带入到期前缀", "请先在历史仓位里选中一条期权记录，或在当前持仓里选中一条期权持仓。")
            return
        self._position_history_expiry_edit.setText(expiry_prefix)
        self._force_position_history_render()

    def _selected_option_inst_id_for_fill_history_shortcut(self) -> str:
        row = self._fill_history_table.currentRow() if hasattr(self, "_fill_history_table") else -1
        if 0 <= row < len(self._visible_fill_history_items):
            item = self._visible_fill_history_items[row]
            if (item.inst_type or "").strip().upper() == "OPTION":
                return item.inst_id or ""
        position = self._selected_option_for_shortcut()
        return position.inst_id if position is not None else ""

    def _selected_option_inst_id_for_position_history_shortcut(self) -> str:
        item = self._selected_position_history_item()
        if item is not None and (item.inst_type or "").strip().upper() == "OPTION":
            return item.inst_id or ""
        position = self._selected_option_for_shortcut()
        return position.inst_id if position is not None else ""

    def _expand_fill_history_limit(self) -> None:
        self._fill_history_fetch_limit += 100
        self._refresh_fill_history()

    def _expand_position_history_limit(self) -> None:
        self._position_history_fetch_limit += 100
        self._refresh_position_history()

    def _filtered_current_orders(self) -> list[OrderStatusView]:
        items = list(self._visible_orders)
        inst_type = str(self._pending_type_combo.currentData() or "").strip().upper()
        source_filter = str(self._pending_source_combo.currentData() or "").strip().lower()
        state_filter = str(self._pending_state_combo.currentData() or "").strip().lower()
        asset_filter = self._pending_asset_edit.text().strip().upper()
        expiry_filter = self._pending_expiry_edit.text().strip().upper()
        keyword = self._pending_keyword_edit.text().strip().upper()
        result: list[OrderStatusView] = []
        for item in items:
            if inst_type and (item.inst_type or "").strip().upper() != inst_type:
                continue
            if source_filter and source_filter not in {"ws", "normal"}:
                continue
            if state_filter and (item.state or "").strip().lower() != state_filter:
                continue
            inst_id = (item.inst_id or "").strip().upper()
            if asset_filter and not inst_id.startswith(asset_filter + "-"):
                continue
            if expiry_filter and not _history_expiry_filter_matches(inst_id, expiry_filter):
                continue
            if keyword:
                haystack = " ".join(
                    (
                        inst_id,
                        (item.state or ""),
                        (item.side or ""),
                        (item.pos_side or ""),
                        (item.ord_type or ""),
                        (item.client_order_id or ""),
                    )
                ).upper()
                if keyword not in haystack:
                    continue
            result.append(item)
        return result

    def _refresh_current_orders_table(self) -> None:
        if not hasattr(self, "_orders_table"):
            return
        filtered = self._filtered_current_orders()
        selected_ord_id = ""
        row = self._orders_table.currentRow()
        if 0 <= row < len(filtered):
            selected_ord_id = filtered[row].ord_id
        self._orders_summary_label.setText(f"当前委托：{len(filtered)} 条 | 仅显示当前持仓相关合约。")
        self._orders_table.setRowCount(len(filtered))
        for row, order in enumerate(filtered):
            values = (
                _format_okx_ms_timestamp(order.update_time or order.created_time),
                "WS 当前",
                order.inst_type or "-",
                order.inst_id or "-",
                _format_trade_order_state(order.state),
                _format_history_side(order.side or "-", order.pos_side or ""),
                order.ord_type or "-",
                _format_trade_order_price(order.price, order.inst_id, order.inst_type or ""),
                _format_trade_order_size(order.size),
                _format_trade_order_size(order.filled_size),
                "-",
                "-",
                order.ord_id or "-",
                order.client_order_id or "-",
            )
            self._set_table_row(self._orders_table, row, values, left_align={3, 13})
        self._current_order_rows = filtered
        self._restore_table_selection(self._orders_table, filtered, selected_ord_id, lambda item: item.ord_id or "")
        self._refresh_current_order_detail()

    def _refresh_current_order_detail(self) -> None:
        if not hasattr(self, "_orders_detail"):
            return
        items = getattr(self, "_current_order_rows", [])
        row = self._orders_table.currentRow() if hasattr(self, "_orders_table") else -1
        if row < 0 or row >= len(items):
            if hasattr(self, "_orders_detail"):
                self._orders_detail.setPlainText("这里会显示选中当前委托的详情。")
            return
        order = items[row]
        lines = [
            f"时间：{_format_okx_ms_timestamp(order.update_time or order.created_time)}",
            f"合约：{order.inst_id or '-'}",
            f"类型：{order.inst_type or '-'}",
            f"状态：{_format_trade_order_state(order.state)}",
            f"方向：{_format_history_side(order.side or '-', order.pos_side or '')}",
            f"委托类型：{order.ord_type or '-'}",
            f"委托价：{_format_trade_order_price(order.price, order.inst_id, order.inst_type or '')}",
            f"委托量：{_format_trade_order_size(order.size)}",
            f"已成交：{_format_trade_order_size(order.filled_size)}",
            f"交易模式：{order.td_mode or '-'}",
            f"订单ID：{order.ord_id or '-'}",
            f"clOrdId：{order.client_order_id or '-'}",
            "",
            json.dumps(order.raw, ensure_ascii=False, indent=2, sort_keys=True),
        ]
        self._orders_detail.setPlainText("\n".join(lines))

    def _filtered_order_history_items(self) -> list[OkxTradeOrderItem]:
        inst_type = str(self._order_history_type_combo.currentData() or "").strip().upper()
        source_filter = str(self._order_history_source_combo.currentData() or "").strip().lower()
        state_filter = str(self._order_history_state_combo.currentData() or "").strip().lower()
        asset_filter = self._order_history_asset_edit.text().strip().upper()
        expiry_filter = self._order_history_expiry_edit.text().strip().upper()
        keyword = self._order_history_keyword_edit.text().strip().upper()
        result: list[OkxTradeOrderItem] = []
        for item in self._order_history_items:
            if inst_type and (item.inst_type or "").strip().upper() != inst_type:
                continue
            if source_filter and (item.source_kind or "").strip().lower() != source_filter:
                continue
            if state_filter and (item.state or "").strip().lower() != state_filter:
                continue
            inst_id = (item.inst_id or "").strip().upper()
            if asset_filter and not inst_id.startswith(asset_filter + "-"):
                continue
            if expiry_filter and not _history_expiry_filter_matches(inst_id, expiry_filter):
                continue
            if keyword:
                haystack = " ".join(
                    (
                        inst_id,
                        item.source_label or "",
                        item.state or "",
                        item.side or "",
                        item.pos_side or "",
                        item.ord_type or "",
                        item.client_order_id or "",
                        item.algo_client_order_id or "",
                    )
                ).upper()
                if keyword not in haystack:
                    continue
            result.append(item)
        return result

    def _refresh_order_history_table(self) -> None:
        if not hasattr(self, "_order_history_table"):
            return
        filtered = self._filtered_order_history_items()
        selected_key = ""
        row = self._order_history_table.currentRow()
        if 0 <= row < len(filtered):
            selected_key = filtered[row].order_id or filtered[row].client_order_id or ""
        self._visible_order_history_items = filtered
        self._order_history_summary_label.setText(f"历史委托：当前显示 {len(filtered)}/{len(self._order_history_items)}")
        self._order_history_table.setRowCount(len(filtered))
        for row, item in enumerate(filtered):
            values = (
                _format_okx_ms_timestamp(item.update_time or item.created_time),
                item.source_label or "-",
                item.inst_type or "-",
                item.inst_id or "-",
                _format_trade_order_state(item.state),
                _format_history_side(item.side, item.pos_side),
                item.ord_type or "-",
                _format_trade_order_price(item.price, item.inst_id, item.inst_type),
                _format_trade_order_size(item.size),
                _format_trade_order_size(item.filled_size),
                _format_trade_order_fee_cell(item, self._order_history_usdt_prices),
                "-",
                item.order_id or item.algo_id or "-",
                item.client_order_id or item.algo_client_order_id or "-",
            )
            self._set_table_row(self._order_history_table, row, values, left_align={3, 13})
        self._restore_table_selection(
            self._order_history_table,
            filtered,
            selected_key,
            lambda item: item.order_id or item.client_order_id or "",
        )
        self._refresh_order_history_detail()

    def _refresh_order_history_detail(self) -> None:
        if not hasattr(self, "_order_history_detail"):
            return
        row = self._order_history_table.currentRow() if hasattr(self, "_order_history_table") else -1
        if row < 0 or row >= len(self._visible_order_history_items):
            if hasattr(self, "_order_history_detail"):
                self._order_history_detail.setPlainText("这里会显示选中历史委托的详情。")
            return
        item = self._visible_order_history_items[row]
        text = _build_trade_order_detail_text(item)
        self._order_history_detail.setPlainText(
            "\n\n".join((text, json.dumps(item.raw, ensure_ascii=False, indent=2, sort_keys=True)))
        )

    def _filtered_fill_history_items(self) -> list[OkxFillHistoryItem]:
        inst_type = str(self._fill_history_type_combo.currentData() or "").strip().upper()
        side_filter = str(self._fill_history_side_combo.currentData() or "").strip().lower()
        asset_filter = self._fill_history_asset_edit.text().strip().upper()
        expiry_filter = self._fill_history_expiry_edit.text().strip().upper()
        keyword = self._fill_history_keyword_edit.text().strip().upper()
        result: list[OkxFillHistoryItem] = []
        for item in self._fill_history_items:
            if inst_type and (item.inst_type or "").strip().upper() != inst_type:
                continue
            if side_filter and side_filter not in {(item.side or "").strip().lower(), (item.pos_side or "").strip().lower()}:
                continue
            inst_id = (item.inst_id or "").strip().upper()
            if asset_filter and not inst_id.startswith(asset_filter + "-"):
                continue
            if expiry_filter and not _history_expiry_filter_matches(inst_id, expiry_filter):
                continue
            if keyword:
                haystack = " ".join(
                    (inst_id, item.inst_type or "", item.side or "", item.pos_side or "", item.exec_type or "")
                ).upper()
                if keyword not in haystack:
                    continue
            result.append(item)
        return result

    def _refresh_fill_history_table(self) -> None:
        if not hasattr(self, "_fill_history_table"):
            return
        filtered = self._filtered_fill_history_items()
        selected_key = ""
        row = self._fill_history_table.currentRow()
        if 0 <= row < len(filtered):
            selected_key = filtered[row].trade_id or filtered[row].order_id or ""
        self._visible_fill_history_items = filtered
        self._fill_history_summary_label.setText(f"历史成交：当前显示 {len(filtered)}/{len(self._fill_history_items)}")
        self._fill_history_table.setRowCount(len(filtered))
        for row, item in enumerate(filtered):
            values = (
                _format_okx_ms_timestamp(item.fill_time),
                item.inst_type or "-",
                item.inst_id or "-",
                _format_history_side(item.side, item.pos_side),
                _format_fill_history_price(item),
                _format_fill_history_size(item, self._fill_history_instruments),
                _format_fill_history_fee_cell(item, self._fill_history_usdt_prices),
                _format_fill_history_pnl(item, self._fill_history_usdt_prices),
                _format_fill_history_exec_type(item.exec_type),
            )
            self._set_table_row(self._fill_history_table, row, values, left_align={2})
        self._restore_table_selection(
            self._fill_history_table,
            filtered,
            selected_key,
            lambda item: item.trade_id or item.order_id or "",
        )
        self._refresh_fill_history_detail()

    def _refresh_fill_history_detail(self) -> None:
        if not hasattr(self, "_fill_history_detail"):
            return
        row = self._fill_history_table.currentRow() if hasattr(self, "_fill_history_table") else -1
        if row < 0 or row >= len(self._visible_fill_history_items):
            if hasattr(self, "_fill_history_detail"):
                self._fill_history_detail.setPlainText("这里会显示选中历史成交的详情。")
            return
        item = self._visible_fill_history_items[row]
        text = _build_fill_history_detail_text(item, self._fill_history_instruments)
        self._fill_history_detail.setPlainText(
            "\n\n".join((text, json.dumps(item.raw, ensure_ascii=False, indent=2, sort_keys=True)))
        )

    def _filtered_position_history_items(self) -> list[OkxPositionHistoryItem]:
        inst_type = str(self._position_history_type_combo.currentData() or "").strip().upper()
        margin_mode = str(self._position_history_margin_combo.currentData() or "").strip().lower()
        asset_filter = self._position_history_asset_edit.text().strip().upper()
        expiry_filter = self._position_history_expiry_edit.text().strip().upper()
        keyword = self._position_history_keyword_edit.text().strip().upper()
        start_date = self._parse_history_date(self._position_history_range_start_edit.text())
        end_date = self._parse_history_date(self._position_history_range_end_edit.text(), end_of_day=True)
        result: list[OkxPositionHistoryItem] = []
        for item in self._position_history_items:
            if inst_type and (item.inst_type or "").strip().upper() != inst_type:
                continue
            if margin_mode and (item.mgn_mode or "").strip().lower() != margin_mode:
                continue
            inst_id = (item.inst_id or "").strip().upper()
            if asset_filter and not inst_id.startswith(asset_filter + "-"):
                continue
            if expiry_filter and not _history_expiry_filter_matches(inst_id, expiry_filter):
                continue
            if keyword:
                haystack = " ".join(
                    (
                        inst_id,
                        item.inst_type or "",
                        item.mgn_mode or "",
                        item.pos_side or "",
                        item.direction or "",
                        self._position_history_note_text(item),
                    )
                ).upper()
                if keyword not in haystack:
                    continue
            if item.update_time is not None:
                if start_date is not None and item.update_time < start_date:
                    continue
                if end_date is not None and item.update_time > end_date:
                    continue
            result.append(item)
        return result

    def _render_position_history_table(self) -> None:
        if not hasattr(self, "_position_history_table"):
            return
        filtered = self._filtered_position_history_items()
        selected_key = ""
        row = self._position_history_table.currentRow()
        if 0 <= row < len(self._visible_position_history_items):
            selected_key = self._position_history_row_key(self._visible_position_history_items[row])
        self._visible_position_history_items = filtered
        stats_text = _format_position_history_filter_stats(
            list(enumerate(filtered)),
            self._position_history_usdt_prices,
        )
        self._position_history_summary_label.setText(
            "\n".join(
                (
                    f"历史仓位：{len(self._position_history_items)} 条 | 最近同步：{self._position_history_last_sync_text} | 当前显示：{len(filtered)}/{len(self._position_history_items)}",
                    f"筛选统计：{stats_text}",
                )
            )
        )
        self._position_history_table.setRowCount(len(filtered))
        for row, item in enumerate(filtered):
            values = (
                _format_okx_ms_timestamp(item.update_time),
                item.inst_type or "-",
                item.inst_id or "-",
                _format_margin_mode(item.mgn_mode or ""),
                _format_history_side(None, item.pos_side or item.direction),
                _format_position_history_trade_side(item),
                _format_position_history_price(item.open_avg_price, item.inst_id, item.inst_type),
                _format_position_history_price(item.close_avg_price, item.inst_id, item.inst_type),
                _format_position_history_size(item, self._position_history_instruments),
                _format_position_history_fee_cell(item, self._position_history_usdt_prices),
                _format_position_history_pnl(item.pnl, item, usdt_prices=self._position_history_usdt_prices),
                _position_history_note_summary_text(item, self._position_history_note_text(item)),
            )
            self._set_table_row(self._position_history_table, row, values, left_align={2, 11})
        self._restore_table_selection(
            self._position_history_table,
            filtered,
            selected_key,
            self._position_history_row_key,
        )

    def _refresh_position_history_detail(self) -> None:
        if not hasattr(self, "_position_history_detail"):
            return
        row = self._position_history_table.currentRow() if hasattr(self, "_position_history_table") else -1
        if row < 0 or row >= len(self._visible_position_history_items):
            if hasattr(self, "_position_history_detail"):
                self._position_history_detail.setPlainText("这里会显示选中历史仓位的详情。")
            return
        item = self._visible_position_history_items[row]
        self._position_history_detail.setPlainText(
            _build_position_history_detail_text(
                item,
                self._position_history_usdt_prices,
                self._position_history_instruments,
                note=self._position_history_note_text(item),
            )
        )

    def _selected_position_history_item(self) -> OkxPositionHistoryItem | None:
        row = self._position_history_table.currentRow() if hasattr(self, "_position_history_table") else -1
        if row < 0 or row >= len(self._visible_position_history_items):
            return None
        return self._visible_position_history_items[row]

    def edit_selected_position_history_note(self) -> None:
        item = self._selected_position_history_item()
        if item is None:
            QMessageBox.information(self, "编辑备注", "请先选择一条历史仓位。")
            return
        dialog = NoteEditorDialog(
            title="编辑历史仓位备注",
            prompt=f"为 {item.inst_id} 填写备注。",
            initial_value=self._position_history_note_text(item),
            parent=self,
        )
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return
        record_key = _position_history_note_key(self._last_profile_name, self._note_environment(), item)
        if dialog.result_text:
            record = _build_history_position_note_record(
                profile_name=self._last_profile_name,
                environment=self._note_environment(),
                item=item,
                note=dialog.result_text,
                now_ms=int(time.time() * 1000),
                previous=self._history_notes.get(record_key),
            )
            if record is not None:
                self._history_notes[record_key] = record
        else:
            self._history_notes.pop(record_key, None)
        self._save_position_notes()
        self._render_position_history_table()

    @Slot(str)
    def _set_order_history_status(self, text: str) -> None:
        if hasattr(self, "_order_history_summary_label"):
            self._order_history_summary_label.setText(text)

    @Slot(str)
    def _set_fill_history_status(self, text: str) -> None:
        if hasattr(self, "_fill_history_summary_label"):
            self._fill_history_summary_label.setText(text)

    @Slot(object)
    def _apply_orders(self, orders: object) -> None:
        self._orders = list(orders) if isinstance(orders, list) else []
        visible_inst_ids = {item.inst_id.strip().upper() for item in self._visible_positions}
        self._visible_orders = [
            item for item in self._orders if not visible_inst_ids or item.inst_id.strip().upper() in visible_inst_ids
        ]
        self._refresh_current_orders_table()

    @Slot(object)
    def _apply_order_history_payload(self, payload: object) -> None:
        if not isinstance(payload, dict):
            return
        items = payload.get("items")
        prices = payload.get("usdt_prices")
        self._order_history_items = list(items) if isinstance(items, list) else []
        self._order_history_usdt_prices = dict(prices) if isinstance(prices, dict) else {}
        self._refresh_order_history_table()

    @Slot(object)
    def _apply_fill_history_payload(self, payload: object) -> None:
        if not isinstance(payload, dict):
            return
        items = payload.get("items")
        instruments = payload.get("instruments")
        prices = payload.get("usdt_prices")
        self._fill_history_items = list(items) if isinstance(items, list) else []
        self._fill_history_instruments = dict(instruments) if isinstance(instruments, dict) else {}
        self._fill_history_usdt_prices = dict(prices) if isinstance(prices, dict) else {}
        self._refresh_fill_history_table()

    @Slot(object)
    def _apply_position_history_payload(self, payload: object) -> None:
        if not isinstance(payload, dict):
            return
        items = payload.get("items")
        instruments = payload.get("instruments")
        usdt_prices = payload.get("usdt_prices")
        self._position_history_items = list(items) if isinstance(items, list) else []
        self._position_history_instruments = dict(instruments) if isinstance(instruments, dict) else {}
        self._position_history_usdt_prices = dict(usdt_prices) if isinstance(usdt_prices, dict) else {}
        self._position_history_last_sync_text = time.strftime("%H:%M:%S")
        self._render_position_history_table()

    def _set_table_row(
        self,
        table: QTableWidget,
        row: int,
        values: tuple[str, ...],
        *,
        left_align: set[int] | None = None,
    ) -> None:
        left_align = left_align or set()
        for column, value in enumerate(values):
            cell = QTableWidgetItem(str(value))
            if column in left_align:
                cell.setTextAlignment(int(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter))
            else:
                cell.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter))
            table.setItem(row, column, cell)

    def _restore_table_selection(
        self,
        table: QTableWidget,
        items: list[object],
        selected_key: str,
        key_fn: Callable[[object], str],
    ) -> None:
        target_row = -1
        if selected_key:
            for index, item in enumerate(items):
                if key_fn(item) == selected_key:
                    target_row = index
                    break
        elif items:
            target_row = 0
        if target_row >= 0:
            table.selectRow(target_row)

    def _position_history_row_key(self, item: OkxPositionHistoryItem) -> str:
        return "|".join(
            (
                str(item.update_time or ""),
                item.inst_id or "",
                item.pos_side or "",
                item.direction or "",
                str(item.close_size or ""),
            )
        )

    def _parse_history_date(self, raw: str, *, end_of_day: bool = False) -> int | None:
        text = raw.strip()
        if not text:
            return None
        try:
            normalized = text.replace("-", "").replace("/", "").replace(".", "")
            parsed = time.strptime(normalized, "%Y%m%d")
            base = int(time.mktime(parsed)) * 1000
            return base + (24 * 60 * 60 * 1000 - 1 if end_of_day else 0)
        except Exception:
            return None

    def _default_position_history_start_text(self) -> str:
        return "20260101"

    def _default_position_history_end_text(self) -> str:
        return (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    @Slot()
    def _schedule_position_history_render(self) -> None:
        if self._position_history_filter_resetting:
            return
        if not hasattr(self, "_position_history_render_timer"):
            self._render_position_history_table()
            return
        self._position_history_render_timer.start(120)

    @Slot()
    def _force_position_history_render(self) -> None:
        if hasattr(self, "_position_history_render_timer") and self._position_history_render_timer.isActive():
            self._position_history_render_timer.stop()
        self._render_position_history_table()

    def refresh_view(self) -> None:
        if not self._ensure_runtime_ready(force_unlock=True):
            return
        self._status_badge.setText("正在刷新...")
        self._start_private_threads(force_restart=True)
