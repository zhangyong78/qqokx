from __future__ import annotations

import time
from decimal import Decimal

from PySide6.QtCore import Qt, Slot
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
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTabWidget,
    QTextEdit,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from okx_quant.okx_client import OkxPosition
from okx_quant.persistence import load_position_notes_snapshot, save_position_notes_snapshot
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
    _group_positions_for_tree,
    _normalize_position_note_text,
    _option_search_shortcuts,
    _position_delta_value,
    _position_note_current_key,
    _position_realized_pnl_usdt,
    _position_signed_open_value_approx_usdt,
    _position_theta_usdt,
    _position_tree_row_id,
    _position_unrealized_pnl_usdt,
    _reconcile_current_position_note_records,
)
from roll_terminal_qt.account_service import AccountFeedThread
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
    ("mark", "标记价", 108, Qt.AlignmentFlag.AlignRight),
    ("mark_usdt", "标记≈USDT", 84, Qt.AlignmentFlag.AlignRight),
    ("avg", "开仓价", 108, Qt.AlignmentFlag.AlignRight),
    ("avg_usdt", "开仓≈USDT", 84, Qt.AlignmentFlag.AlignRight),
    ("open_value_usdt", "开仓价值≈USDT", 116, Qt.AlignmentFlag.AlignRight),
    ("pos", "持仓量", 170, Qt.AlignmentFlag.AlignRight),
    ("option_side", "买购:卖购 | 买沽:卖沽", 170, Qt.AlignmentFlag.AlignCenter),
    ("upl", "浮盈亏", 120, Qt.AlignmentFlag.AlignRight),
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


class AccountPositionsHomeWidget(QWidget):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._runtime = load_runtime("2211") or load_runtime()
        self._profile_snapshots: dict[str, dict[str, str]] = {}
        self._unlocked_profiles: set[str] = set()
        self._last_profile_name = self._runtime.credential_profile_name if self._runtime is not None else ""
        self._profile_switch_guard = False

        self._account_feed: AccountFeedThread | None = None
        self._order_feed: OrderFeedThread | None = None

        self._raw_positions: list[OkxPosition] = []
        self._visible_positions: list[OkxPosition] = []
        self._orders: list[OrderStatusView] = []
        self._visible_orders: list[OrderStatusView] = []
        self._position_instruments: dict[str, object] = {}
        self._position_tickers: dict[str, object] = {}
        self._upl_usdt_prices: dict[str, Decimal] = {}
        self._position_row_payloads: dict[str, dict[str, object]] = {}
        self._visible_column_ids: set[str] = set(DEFAULT_VISIBLE_COLUMNS)
        self._expanded_row_keys: set[str] = set()

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
        self._stop_private_threads()

    def refresh_view(self) -> None:
        if not self._ensure_runtime_ready(force_unlock=True):
            return
        self._status_badge.setText("正在刷新...")
        self._start_private_threads(force_restart=True)

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        layout.addWidget(self._build_header())
        layout.addWidget(self._build_filter_bar())
        layout.addWidget(self._build_positions_panel(), 1)

    def _build_header(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Panel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)

        top = QHBoxLayout()
        top.setSpacing(8)
        self._status_badge = QLabel("正常")
        self._status_badge.setObjectName("Badge")
        self._account_status = QLabel("持仓读取中...")
        self._account_status.setObjectName("Subtle")
        self._order_status = QLabel("订单WS等待中...")
        self._order_status.setObjectName("Subtle")
        top.addWidget(self._status_badge)
        top.addWidget(self._account_status)
        top.addWidget(self._order_status)
        top.addStretch(1)
        top.addWidget(QLabel("API配置"))
        self._profile_combo = QComboBox()
        self._profile_combo.currentIndexChanged.connect(self._on_profile_changed)
        self._profile_combo.setMinimumWidth(120)
        top.addWidget(self._profile_combo)
        layout.addLayout(top)

        actions = QHBoxLayout()
        actions.setSpacing(6)
        for text, handler in (
            ("刷新", self.refresh_view),
            ("账户信息", self._show_account_overview),
            ("展开持仓详情", self._toggle_detail_panel),
            ("折叠历史区域", self._toggle_history_panel),
            ("平仓选中", self._show_not_ready_action),
            ("编辑备注", self.edit_selected_position_note),
            ("从选中持仓接管", self._show_not_ready_action),
            ("停止接管", self._show_not_ready_action),
            ("设置期权保护", self._show_not_ready_action),
            ("展期建议", self._show_not_ready_action),
            ("列设置", self.open_positions_column_window),
        ):
            button = QPushButton(text)
            button.clicked.connect(handler)
            actions.addWidget(button)
        actions.addStretch(1)
        layout.addLayout(actions)

        self._summary_label = QLabel("当前没有持仓")
        self._summary_label.setObjectName("Subtle")
        self._summary_label.setWordWrap(True)
        layout.addWidget(self._summary_label)
        return panel

    def _build_filter_bar(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Guide")
        layout = QGridLayout(panel)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setHorizontalSpacing(8)
        layout.setVerticalSpacing(8)

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
        return panel

    def _build_positions_panel(self) -> QWidget:
        splitter = QSplitter(Qt.Orientation.Vertical)
        splitter.addWidget(self._build_tree_section())
        splitter.addWidget(self._build_history_tabs())
        splitter.setSizes([760, 300])
        return splitter

    def _build_tree_section(self) -> QWidget:
        wrapper = QWidget()
        layout = QVBoxLayout(wrapper)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        panel = QFrame()
        panel.setObjectName("Panel")
        panel_layout = QVBoxLayout(panel)
        panel_layout.setContentsMargins(10, 10, 10, 10)
        panel_layout.setSpacing(8)

        title_row = QHBoxLayout()
        title = QLabel("当前持仓")
        title.setObjectName("SectionTitle")
        self._positions_hint = QLabel("当前显示 0 条持仓 | 点击任一行查看详情。")
        self._positions_hint.setObjectName("Subtle")
        title_row.addWidget(title)
        title_row.addStretch(1)
        expand_all_button = QPushButton("展开全部")
        expand_all_button.clicked.connect(self._expand_all_positions)
        title_row.addWidget(expand_all_button)
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
        self._apply_column_visibility()
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
        layout.addWidget(self._detail_panel)
        return wrapper

    def _build_history_tabs(self) -> QWidget:
        self._history_panel = QFrame()
        self._history_panel.setObjectName("Panel")
        layout = QVBoxLayout(self._history_panel)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        self._tabs = QTabWidget()
        self._tabs.addTab(self._build_placeholder_tab("当前委托", "当前委托区块已接入，下面继续显示实时 WS 回报。"), "当前委托")
        self._tabs.addTab(self._build_placeholder_tab("动态止盈接管", "动态止盈接管区块预留，后续按旧页面完整迁移。"), "动态止盈接管")
        self._tabs.addTab(self._build_placeholder_tab("历史委托", "历史委托区块预留，后续补齐筛选和同步逻辑。"), "历史委托")
        self._tabs.addTab(self._build_placeholder_tab("历史成交", "历史成交区块预留，后续补齐筛选和同步逻辑。"), "历史成交")
        self._tabs.addTab(self._build_placeholder_tab("历史仓位", "历史仓位区块预留，后续补齐备注和筛选逻辑。"), "历史仓位")
        layout.addWidget(self._tabs, 1)
        return self._history_panel

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

    def _apply_column_visibility(self) -> None:
        for index, (column_id, _heading, _width, _alignment) in enumerate(POSITION_COLUMNS, start=1):
            self._position_tree.setColumnHidden(index, column_id not in self._visible_column_ids)

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
        self._refresh_detail()

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

    def _expand_all_positions(self) -> None:
        for item in self._iter_tree_items():
            row_key = item.data(0, Qt.ItemDataRole.UserRole)
            payload = self._position_row_payloads.get(row_key) if isinstance(row_key, str) else None
            if not isinstance(payload, dict) or payload.get("kind") != "group":
                continue
            self._expanded_row_keys.add(row_key)
            item.setExpanded(True)

    def _on_tree_item_expanded(self, item: QTreeWidgetItem) -> None:
        row_key = item.data(0, Qt.ItemDataRole.UserRole)
        if not isinstance(row_key, str):
            return
        payload = self._position_row_payloads.get(row_key)
        if isinstance(payload, dict) and payload.get("kind") == "group":
            self._expanded_row_keys.add(row_key)

    def _on_tree_item_collapsed(self, item: QTreeWidgetItem) -> None:
        row_key = item.data(0, Qt.ItemDataRole.UserRole)
        if not isinstance(row_key, str):
            return
        self._expanded_row_keys.discard(row_key)

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
        visible = self._detail_panel.isVisible()
        self._detail_panel.setVisible(not visible)

    def _toggle_history_panel(self) -> None:
        visible = self._history_panel.isVisible()
        self._history_panel.setVisible(not visible)

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
