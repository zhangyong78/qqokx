from __future__ import annotations

from decimal import Decimal

from PySide6.QtCore import Qt, Slot
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFrame,
    QGridLayout,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QLineEdit,
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

from roll_terminal_qt.account_service import AccountFeedThread, FuturesPositionView
from roll_terminal_qt.formatting import fmt_decimal
from roll_terminal_qt.order_service import OrderFeedThread, OrderStatusView
from roll_terminal_qt.profile_access import ensure_profile_unlocked, load_profile_snapshots, profile_requires_password
from roll_terminal_qt.runtime import load_runtime, profile_names


POSITION_TYPE_OPTIONS: tuple[tuple[str, str], ...] = (
    ("全部类型", ""),
    ("交割合约 FUTURES", "FUTURES"),
    ("永续 SWAP", "SWAP"),
)


def _base_ccy(inst_id: str) -> str:
    return (inst_id or "").strip().upper().split("-", 1)[0] or "-"


def _table_text(value: Decimal | None, places: int | None = None) -> str:
    if value is None:
        return "-"
    return fmt_decimal(value, places)


def _spot_text_for_position(position: FuturesPositionView, spot_lookup: dict[str, str]) -> str:
    base_ccy = _base_ccy(position.inst_id)
    return spot_lookup.get(base_ccy, f"{base_ccy}-USDT | 等待账户余额刷新...")


def _position_mode_text(position: FuturesPositionView) -> str:
    parts = [segment.strip() for segment in position.label.split("|")]
    if parts:
        return parts[-1] or "-"
    return "-"


def _position_inst_type_matches(position: FuturesPositionView, inst_type: str) -> bool:
    if not inst_type:
        return True
    return position.inst_type.strip().upper() == inst_type


class AccountOverviewDialog(QDialog):
    def __init__(self, *, summary_text: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("账户概览")
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
        buttons.accepted.connect(self.accept)
        buttons.button(QDialogButtonBox.StandardButton.Close).setText("关闭")
        layout.addWidget(buttons)


class AccountPositionsHomeWidget(QWidget):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._runtime = load_runtime("2211") or load_runtime()
        self._positions: list[FuturesPositionView] = []
        self._orders: list[OrderStatusView] = []
        self._spot_balance_lookup: dict[str, str] = {}
        self._visible_positions: list[FuturesPositionView] = []
        self._visible_orders: list[OrderStatusView] = []
        self._profile_snapshots: dict[str, dict[str, str]] = {}
        self._unlocked_profiles: set[str] = set()
        self._last_profile_name = self._runtime.credential_profile_name if self._runtime is not None else ""
        self._profile_switch_guard = False
        self._account_feed: AccountFeedThread | None = None
        self._order_feed: OrderFeedThread | None = None

        self._build_ui()
        self._refresh_profiles()
        self._populate_profile_combo()

        locked_on_start = bool(
            self._last_profile_name and profile_requires_password(self._last_profile_name, self._profile_snapshots)
        )
        if locked_on_start:
            self._account_status.setText(f"API {self._last_profile_name} 未解锁")
            self._order_status.setText("委托 WS 等待 API 解锁")
            self._status.setText("点击“刷新”或重新选择 API 后解锁")
            self._summary_text.setPlainText("当前 API 配置已加切换密码，请先解锁后再加载账户持仓。")
        else:
            if self._last_profile_name:
                self._unlocked_profiles.add(self._last_profile_name)
            self._start_private_threads()

    def shutdown(self) -> None:
        self._stop_private_threads()

    def refresh_view(self) -> None:
        if not self._ensure_runtime_ready(force_unlock=True):
            return
        self._status.setText("正在刷新账户持仓与委托视图...")
        self._start_private_threads(force_restart=True)

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(14)

        layout.addWidget(self._build_header_panel())
        layout.addWidget(self._build_toolbar_panel())
        layout.addWidget(self._build_metrics_panel())

        content = QSplitter(Qt.Orientation.Vertical)
        content.addWidget(self._build_positions_splitter())
        content.addWidget(self._build_lower_tabs())
        content.setSizes([620, 340])
        layout.addWidget(content, 1)

    def _build_header_panel(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Panel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(20, 18, 20, 18)
        layout.setSpacing(6)

        title = QLabel("账户持仓")
        title.setObjectName("SectionTitle")
        subtitle = QLabel(
            "主页默认聚焦账户持仓、对应现货和当前委托。共享配置与功能模块入口已收进菜单，"
            "这里优先承担本地查看、筛选和分析。"
        )
        subtitle.setObjectName("Subtle")
        subtitle.setWordWrap(True)
        layout.addWidget(title)
        layout.addWidget(subtitle)
        return panel

    def _build_toolbar_panel(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Guide")
        layout = QGridLayout(panel)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setHorizontalSpacing(12)
        layout.setVerticalSpacing(10)

        self._profile_combo = QComboBox()
        self._profile_combo.currentIndexChanged.connect(self._on_profile_changed)

        self._type_combo = QComboBox()
        for label, value in POSITION_TYPE_OPTIONS:
            self._type_combo.addItem(label, value)
        self._type_combo.currentIndexChanged.connect(self._apply_filters)

        self._keyword_edit = QLineEdit()
        self._keyword_edit.setPlaceholderText("搜索合约 / 币种 / 方向 / 模式")
        self._keyword_edit.textChanged.connect(self._apply_filters)

        self._only_available_checkbox = QCheckBox("仅显示可平")
        self._only_available_checkbox.setChecked(True)
        self._only_available_checkbox.stateChanged.connect(self._apply_filters)

        refresh_button = QPushButton("刷新")
        refresh_button.clicked.connect(self.refresh_view)
        account_button = QPushButton("账户信息")
        account_button.clicked.connect(self._show_account_overview)
        detail_button = QPushButton("展开持仓详情")
        detail_button.clicked.connect(self._focus_position_detail)
        history_button = QPushButton("展开历史区块")
        history_button.clicked.connect(self._focus_order_tab)
        clear_button = QPushButton("清空筛选")
        clear_button.clicked.connect(self._clear_filters)

        self._status = QLabel("等待账户数据...")
        self._status.setObjectName("Badge")
        self._account_status = QLabel("持仓读取中...")
        self._account_status.setObjectName("Badge")
        self._order_status = QLabel("委托 WS 等待中...")
        self._order_status.setObjectName("Badge")

        layout.addWidget(QLabel("API"), 0, 0)
        layout.addWidget(self._profile_combo, 0, 1)
        layout.addWidget(QLabel("类型"), 0, 2)
        layout.addWidget(self._type_combo, 0, 3)
        layout.addWidget(QLabel("搜索"), 0, 4)
        layout.addWidget(self._keyword_edit, 0, 5, 1, 2)
        layout.addWidget(self._only_available_checkbox, 0, 7)
        layout.addWidget(refresh_button, 0, 8)
        layout.addWidget(account_button, 0, 9)
        layout.addWidget(detail_button, 0, 10)
        layout.addWidget(history_button, 0, 11)
        layout.addWidget(clear_button, 0, 12)
        layout.addWidget(self._account_status, 1, 0, 1, 4)
        layout.addWidget(self._order_status, 1, 4, 1, 4)
        layout.addWidget(self._status, 1, 8, 1, 5)
        return panel

    def _build_metrics_panel(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Panel")
        layout = QHBoxLayout(panel)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(12)

        self._position_count_metric = self._build_metric_card(layout, "可见持仓")
        self._contract_metric = self._build_metric_card(layout, "可平张数")
        self._base_metric = self._build_metric_card(layout, "折合币数")
        self._spot_metric = self._build_metric_card(layout, "对应现货")
        return panel

    def _build_metric_card(self, parent_layout: QHBoxLayout, title: str) -> QLabel:
        card = QFrame()
        card.setObjectName("StatCard")
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(14, 12, 14, 12)
        card_layout.setSpacing(4)
        title_label = QLabel(title)
        title_label.setObjectName("StatTitle")
        value_label = QLabel("-")
        value_label.setObjectName("Metric")
        card_layout.addWidget(title_label)
        card_layout.addWidget(value_label)
        parent_layout.addWidget(card, 1)
        return value_label

    def _build_positions_splitter(self) -> QWidget:
        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.addWidget(self._build_positions_panel())
        splitter.addWidget(self._build_position_detail_panel())
        splitter.setSizes([980, 520])
        return splitter

    def _build_positions_panel(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Panel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)

        title_row = QHBoxLayout()
        title = QLabel("当前持仓")
        title.setObjectName("SectionTitle")
        title_row.addWidget(title)
        title_row.addStretch(1)
        self._positions_hint = QLabel("点击任一行查看对应现货与委托明细。")
        self._positions_hint.setObjectName("Subtle")
        title_row.addWidget(self._positions_hint)
        layout.addLayout(title_row)

        self._positions_table = QTableWidget(0, 10)
        self._positions_table.setHorizontalHeaderLabels(
            ["合约", "类型", "方向", "可平(张)", "持仓(张)", "1张面值", "名义金额", "折合币数", "对应现货", "模式"]
        )
        self._positions_table.verticalHeader().setVisible(False)
        self._positions_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._positions_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._positions_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._positions_table.setAlternatingRowColors(True)
        self._positions_table.itemSelectionChanged.connect(self._on_position_selected)
        header = self._positions_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(7, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(8, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(9, QHeaderView.ResizeMode.ResizeToContents)
        layout.addWidget(self._positions_table, 1)
        return panel

    def _build_position_detail_panel(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Panel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)

        title = QLabel("持仓详情")
        title.setObjectName("SectionTitle")
        self._summary_text = QTextEdit()
        self._summary_text.setReadOnly(True)
        self._summary_text.setMinimumHeight(220)

        layout.addWidget(title)
        layout.addWidget(self._summary_text, 1)
        return panel

    def _build_lower_tabs(self) -> QWidget:
        self._tabs = QTabWidget()
        self._tabs.addTab(self._build_orders_tab(), "当前委托")
        self._tabs.addTab(
            self._build_placeholder_tab("历史委托", "历史委托区块已预留，后续可直接接入本地缓存与同步逻辑。"),
            "历史委托",
        )
        self._tabs.addTab(
            self._build_placeholder_tab("历史成交", "历史成交区块已预留，后续可补齐成交明细与分析统计。"),
            "历史成交",
        )
        self._tabs.addTab(
            self._build_placeholder_tab("历史仓位", "历史仓位区块已预留，后续可补齐闭仓记录与回溯分析。"),
            "历史仓位",
        )
        self._tabs.addTab(self._build_account_summary_tab(), "账户摘要")
        self._tabs.addTab(self._build_migration_notes_tab(), "迁移说明")
        return self._tabs

    def _build_orders_tab(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Panel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)

        title_row = QHBoxLayout()
        title = QLabel("当前委托")
        title.setObjectName("SectionTitle")
        self._orders_hint = QLabel("当前展示与可见持仓相关的 WS 委托回报。")
        self._orders_hint.setObjectName("Subtle")
        title_row.addWidget(title)
        title_row.addStretch(1)
        title_row.addWidget(self._orders_hint)
        layout.addLayout(title_row)

        self._orders_table = QTableWidget(0, 8)
        self._orders_table.setHorizontalHeaderLabels(
            ["合约", "方向", "委托类型", "状态", "委托价", "成交均价", "委托量", "已成交"]
        )
        self._orders_table.verticalHeader().setVisible(False)
        self._orders_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._orders_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._orders_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._orders_table.setAlternatingRowColors(True)
        self._orders_table.itemSelectionChanged.connect(self._on_order_selected)
        order_header = self._orders_table.horizontalHeader()
        order_header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        order_header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        order_header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        order_header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        order_header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        order_header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        order_header.setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)
        order_header.setSectionResizeMode(7, QHeaderView.ResizeMode.Stretch)

        self._order_detail_text = QTextEdit()
        self._order_detail_text.setReadOnly(True)
        self._order_detail_text.setMinimumHeight(120)

        layout.addWidget(self._orders_table, 1)
        layout.addWidget(self._order_detail_text)
        return panel

    def _build_account_summary_tab(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Panel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)

        title = QLabel("账户摘要")
        title.setObjectName("SectionTitle")
        self._account_summary_text = QTextEdit()
        self._account_summary_text.setReadOnly(True)

        layout.addWidget(title)
        layout.addWidget(self._account_summary_text, 1)
        return panel

    def _build_placeholder_tab(self, title_text: str, message: str) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Panel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)

        title = QLabel(title_text)
        title.setObjectName("SectionTitle")
        detail = QTextEdit()
        detail.setReadOnly(True)
        detail.setPlainText(message)

        layout.addWidget(title)
        layout.addWidget(detail, 1)
        return panel

    def _build_migration_notes_tab(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Panel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)

        title = QLabel("迁移说明")
        title.setObjectName("SectionTitle")
        notes = QTextEdit()
        notes.setReadOnly(True)
        notes.setPlainText(
            "\n".join(
                [
                    "本页现在由 Qt 原生承载，优先解决旧 ZT 页面在高频刷新时偶发卡顿的问题。",
                    "",
                    "当前已迁移：",
                    "1. API 切换与解锁",
                    "2. 衍生品持仓刷新",
                    "3. 对应现货余额映射",
                    "4. 当前委托 WS 联动",
                    "5. 本地筛选、查看和分析入口",
                    "",
                    "后续可继续补齐：",
                    "1. 历史委托 / 历史成交 / 历史仓位",
                    "2. 列设置与自定义视图",
                    "3. 平仓选中、备注、接管等操作按钮",
                    "4. 更细的分组与统计分析",
                ]
            )
        )
        layout.addWidget(title)
        layout.addWidget(notes, 1)
        return panel

    def _refresh_profiles(self) -> None:
        snapshots, _selected = load_profile_snapshots()
        self._profile_snapshots = snapshots

    def _populate_profile_combo(self) -> None:
        names = profile_names()
        self._profile_switch_guard = True
        self._profile_combo.clear()
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
        if force_unlock and not ensure_profile_unlocked(
            self,
            profile_name,
            self._profile_snapshots,
            self._unlocked_profiles,
        ):
            self._account_status.setText(f"API {profile_name} 未解锁")
            self._order_status.setText("委托 WS 等待 API 解锁")
            return False
        runtime = load_runtime(profile_name)
        if runtime is None:
            QMessageBox.warning(self, "无法刷新", f"API 配置 {profile_name} 不可用，请检查凭证。")
            return False
        self._runtime = runtime
        self._last_profile_name = profile_name
        return True

    def _current_profile_name(self) -> str:
        if self._profile_combo.count() <= 0:
            return ""
        text = self._profile_combo.currentText().strip()
        if text == "未配置":
            return ""
        return text

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
            self._account_status.setText("API 配置不可用")
            self._order_status.setText("委托 WS 不可用")
            return
        if force_restart:
            self._stop_private_threads()
        elif self._account_feed is not None and self._account_feed.isRunning():
            return

        self._account_feed = AccountFeedThread(self._runtime)
        self._order_feed = OrderFeedThread(self._runtime)
        self._account_feed.positions_ready.connect(self._apply_positions)
        self._account_feed.spot_balances_ready.connect(self._apply_spot_balances)
        self._account_feed.status_changed.connect(self._set_account_status)
        self._order_feed.orders_ready.connect(self._apply_orders)
        self._order_feed.status_changed.connect(self._set_order_status)
        self._order_feed.set_watched_inst_ids({item.inst_id for item in self._visible_positions or self._positions})
        self._account_feed.start()
        self._order_feed.start()

    def _visible_position_list(self) -> list[FuturesPositionView]:
        inst_type = str(self._type_combo.currentData() or "").strip().upper()
        keyword = self._keyword_edit.text().strip().lower()
        only_available = self._only_available_checkbox.isChecked()
        visible: list[FuturesPositionView] = []
        for position in self._positions:
            if not _position_inst_type_matches(position, inst_type):
                continue
            if only_available and position.available <= 0:
                continue
            if keyword:
                haystack = " | ".join(
                    [
                        position.inst_id,
                        position.inst_type,
                        position.side,
                        position.label,
                        _spot_text_for_position(position, self._spot_balance_lookup),
                    ]
                ).lower()
                if keyword not in haystack:
                    continue
            visible.append(position)
        return visible

    def _refresh_metrics(self) -> None:
        total_contracts = sum((item.available for item in self._visible_positions), Decimal("0"))
        total_base = sum((item.notional_base or Decimal("0") for item in self._visible_positions), Decimal("0"))
        visible_assets = {
            _base_ccy(item.inst_id)
            for item in self._visible_positions
            if _base_ccy(item.inst_id) in self._spot_balance_lookup
        }
        self._position_count_metric.setText(str(len(self._visible_positions)))
        self._contract_metric.setText(f"{fmt_decimal(total_contracts)} 张")
        self._base_metric.setText(f"{fmt_decimal(total_base)} 币")
        self._spot_metric.setText(str(len(visible_assets)))

    def _render_positions_table(self) -> None:
        self._visible_positions = self._visible_position_list()
        current_key = self._selected_position_key()
        self._positions_table.setRowCount(len(self._visible_positions))
        for row, position in enumerate(self._visible_positions):
            values = [
                position.inst_id,
                position.inst_type,
                "空" if position.side == "short" else "多",
                _table_text(position.available),
                _table_text(position.contracts),
                (
                    f"{_table_text(position.contract_value)} {position.contract_value_ccy}"
                    if position.contract_value is not None and position.contract_value_ccy
                    else "-"
                ),
                (
                    f"{_table_text(position.notional_value)} {position.contract_value_ccy}"
                    if position.notional_value is not None and position.contract_value_ccy
                    else "-"
                ),
                (
                    f"{_table_text(position.notional_base)} {_base_ccy(position.inst_id)}"
                    if position.notional_base is not None
                    else "-"
                ),
                _spot_text_for_position(position, self._spot_balance_lookup),
                _position_mode_text(position),
            ]
            for column, value in enumerate(values):
                item = QTableWidgetItem(value)
                if column in {3, 4, 5, 6, 7}:
                    item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                else:
                    item.setTextAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
                if column == 2:
                    item.setForeground(QColor("#c83b55" if position.side == "short" else "#1a7f46"))
                if column == 8:
                    item.setForeground(QColor("#4f46e5"))
                item.setData(Qt.ItemDataRole.UserRole, position.position_key)
                self._positions_table.setItem(row, column, item)

        self._positions_hint.setText(
            f"当前显示 {len(self._visible_positions)} 条持仓 | "
            "点击任一行查看对应现货与委托明细。"
        )
        self._refresh_metrics()
        self._sync_order_watchlist()
        self._restore_position_selection(current_key)
        if not self._visible_positions:
            self._summary_text.setPlainText("当前筛选条件下没有可展示的持仓。")
        self._refresh_account_summary()

    def _selected_position_key(self) -> str:
        row = self._positions_table.currentRow()
        if row < 0 or row >= len(self._visible_positions):
            return ""
        return self._visible_positions[row].position_key

    def _restore_position_selection(self, position_key: str) -> None:
        if not self._visible_positions:
            return
        target_row = 0
        if position_key:
            for index, position in enumerate(self._visible_positions):
                if position.position_key == position_key:
                    target_row = index
                    break
        self._positions_table.selectRow(target_row)
        self._on_position_selected()

    def _sync_order_watchlist(self) -> None:
        if self._order_feed is None:
            return
        self._order_feed.set_watched_inst_ids({item.inst_id for item in self._visible_positions})

    def _render_orders_table(self) -> None:
        current_ord_id = self._selected_order_id()
        visible_inst_ids = {item.inst_id for item in self._visible_positions}
        self._visible_orders = [
            item for item in self._orders if not visible_inst_ids or item.inst_id.strip().upper() in visible_inst_ids
        ]
        self._orders_table.setRowCount(len(self._visible_orders))
        for row, order in enumerate(self._visible_orders):
            values = [
                order.inst_id,
                order.side,
                order.ord_type,
                order.state,
                _table_text(order.price),
                _table_text(order.avg_price),
                _table_text(order.size),
                _table_text(order.filled_size),
            ]
            for column, value in enumerate(values):
                item = QTableWidgetItem(value)
                if column >= 4:
                    item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                else:
                    item.setTextAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
                if column == 1:
                    item.setForeground(QColor("#1a7f46" if order.side == "buy" else "#c83b55"))
                item.setData(Qt.ItemDataRole.UserRole, order.ord_id)
                self._orders_table.setItem(row, column, item)
        self._orders_hint.setText(f"当前显示 {len(self._visible_orders)} 条相关委托。")
        self._restore_order_selection(current_ord_id)
        if not self._visible_orders:
            self._order_detail_text.setPlainText("当前没有与可见持仓相关的 WS 委托回报。")

    def _selected_order_id(self) -> str:
        row = self._orders_table.currentRow()
        if row < 0 or row >= len(self._visible_orders):
            return ""
        return self._visible_orders[row].ord_id

    def _restore_order_selection(self, ord_id: str) -> None:
        if not self._visible_orders:
            return
        target_row = 0
        if ord_id:
            for index, order in enumerate(self._visible_orders):
                if order.ord_id == ord_id:
                    target_row = index
                    break
        self._orders_table.selectRow(target_row)
        self._on_order_selected()

    def _selected_position(self) -> FuturesPositionView | None:
        row = self._positions_table.currentRow()
        if row < 0 or row >= len(self._visible_positions):
            return None
        return self._visible_positions[row]

    def _selected_order(self) -> OrderStatusView | None:
        row = self._orders_table.currentRow()
        if row < 0 or row >= len(self._visible_orders):
            return None
        return self._visible_orders[row]

    def _refresh_position_detail(self) -> None:
        position = self._selected_position()
        if position is None:
            self._summary_text.setPlainText("请先在左侧选择一条持仓。")
            return
        matched_orders = [item for item in self._visible_orders if item.inst_id.strip().upper() == position.inst_id.strip().upper()]
        lines = [
            f"合约：{position.inst_id}",
            f"类型：{position.inst_type}",
            f"方向：{'空' if position.side == 'short' else '多'}",
            f"可平张数：{fmt_decimal(position.available)}",
            f"持仓张数：{fmt_decimal(position.contracts)}",
            f"API 原始可平：{fmt_decimal(position.api_available)}",
            f"API 原始持仓：{fmt_decimal(position.api_contracts)}",
            f"每张面值："
            + (
                f"{fmt_decimal(position.contract_value)} {position.contract_value_ccy}"
                if position.contract_value is not None and position.contract_value_ccy
                else "-"
            ),
            f"名义金额："
            + (
                f"{fmt_decimal(position.notional_value)} {position.contract_value_ccy}"
                if position.notional_value is not None and position.contract_value_ccy
                else "-"
            ),
            f"折合币数："
            + (
                f"{fmt_decimal(position.notional_base)} {_base_ccy(position.inst_id)}"
                if position.notional_base is not None
                else "-"
            ),
            f"对应现货：{_spot_text_for_position(position, self._spot_balance_lookup)}",
            f"账户模式：{_position_mode_text(position)}",
            "",
            f"相关委托：{len(matched_orders)} 条",
            f"持仓标签：{position.label}",
        ]
        self._summary_text.setPlainText("\n".join(lines))

    def _refresh_order_detail(self) -> None:
        order = self._selected_order()
        if order is None:
            self._order_detail_text.setPlainText("请先在上方委托表中选择一条记录。")
            return
        lines = [
            f"合约：{order.inst_id}",
            f"方向：{order.side or '-'}",
            f"委托类型：{order.ord_type or '-'}",
            f"状态：{order.state or '-'}",
            f"委托价：{_table_text(order.price)}",
            f"成交均价：{_table_text(order.avg_price)}",
            f"委托量：{_table_text(order.size)}",
            f"已成交：{_table_text(order.filled_size)}",
            f"订单 ID：{order.ord_id or '-'}",
        ]
        self._order_detail_text.setPlainText("\n".join(lines))

    def _refresh_account_summary(self) -> None:
        total_contracts = sum((item.available for item in self._visible_positions), Decimal("0"))
        total_base = sum((item.notional_base or Decimal("0") for item in self._visible_positions), Decimal("0"))
        futures_count = sum(1 for item in self._visible_positions if item.inst_type == "FUTURES")
        swap_count = sum(1 for item in self._visible_positions if item.inst_type == "SWAP")
        lines = [
            f"当前 API：{self._last_profile_name or '-'}",
            f"可见持仓：{len(self._visible_positions)} 条 | 交割 {futures_count} | 永续 {swap_count}",
            f"可平张数合计：{fmt_decimal(total_contracts)} 张",
            f"折合币数合计：{fmt_decimal(total_base)}",
            f"当前委托：{len(self._visible_orders)} 条",
            "",
            "对应现货余额：",
        ]
        if self._spot_balance_lookup:
            for asset in sorted(self._spot_balance_lookup):
                lines.append(f"- {self._spot_balance_lookup[asset]}")
        else:
            lines.append("- 等待账户余额刷新...")
        self._account_summary_text.setPlainText("\n".join(lines))

    def _show_account_overview(self) -> None:
        dialog = AccountOverviewDialog(summary_text=self._account_summary_text.toPlainText(), parent=self)
        dialog.exec()

    def _focus_position_detail(self) -> None:
        self._summary_text.setFocus(Qt.FocusReason.OtherFocusReason)

    def _focus_order_tab(self) -> None:
        self._tabs.setCurrentIndex(0)
        self._orders_table.setFocus(Qt.FocusReason.OtherFocusReason)

    def _clear_filters(self) -> None:
        self._type_combo.setCurrentIndex(0)
        self._keyword_edit.clear()
        self._only_available_checkbox.setChecked(True)

    def _apply_filters(self, *_args: object) -> None:
        self._render_positions_table()
        self._render_orders_table()

    @Slot()
    def _on_position_selected(self) -> None:
        self._refresh_position_detail()

    @Slot()
    def _on_order_selected(self) -> None:
        self._refresh_order_detail()

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
            self._profile_switch_guard = True
            previous_index = self._profile_combo.findText(self._last_profile_name)
            self._profile_combo.setCurrentIndex(previous_index if previous_index >= 0 else 0)
            self._profile_switch_guard = False
            return
        self._runtime = runtime
        self._last_profile_name = target
        self._status.setText(f"已切换到 API {target}，正在刷新账户持仓...")
        self._start_private_threads(force_restart=True)

    @Slot(str)
    def _set_account_status(self, text: str) -> None:
        self._account_status.setText(text)

    @Slot(str)
    def _set_order_status(self, text: str) -> None:
        self._order_status.setText(text)

    @Slot(object)
    def _apply_positions(self, positions: object) -> None:
        self._positions = list(positions) if isinstance(positions, list) else []
        self._status.setText(f"持仓已刷新：{len(self._positions)} 条")
        self._render_positions_table()
        self._render_orders_table()

    @Slot(object)
    def _apply_spot_balances(self, lookup: object) -> None:
        self._spot_balance_lookup = dict(lookup) if isinstance(lookup, dict) else {}
        self._render_positions_table()
        self._render_orders_table()

    @Slot(object)
    def _apply_orders(self, orders: object) -> None:
        self._orders = list(orders) if isinstance(orders, list) else []
        self._render_orders_table()
        self._refresh_account_summary()
