from __future__ import annotations

from decimal import Decimal, ROUND_DOWN

from PySide6.QtCore import QTimer, Qt, Slot
from PySide6.QtGui import QColor, QFont
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QFrame,
    QGridLayout,
    QHeaderView,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from roll_terminal_qt.account_service import AccountFeedThread, FuturesPositionView
from roll_terminal_qt.execution_service import (
    ExecutionStatus,
    RollExecutionPlan,
    RollExecutionThread,
    parse_nonnegative_int,
    parse_optional_decimal,
    parse_positive_float,
    parse_positive_int,
    parse_roll_qty,
    parse_slippage_percent,
    roll_direction_from_position,
)
from roll_terminal_qt.formatting import fmt_decimal
from roll_terminal_qt.instrument_service import TargetInstrumentThread
from roll_terminal_qt.market_service import MarketFeedThread
from roll_terminal_qt.models import ArbitrageOpportunityView, LegMarket, MarketPairSnapshot
from roll_terminal_qt.opportunity_service import default_opportunities, filter_opportunities
from roll_terminal_qt.order_service import OrderFeedThread, OrderStatusView
from roll_terminal_qt.runtime import load_runtime, profile_names
from roll_terminal_qt.style import APP_STYLE
from okx_quant.persistence import (
    credential_profile_has_switch_password,
    load_credentials_profiles_snapshot,
    verify_profile_switch_password,
)


class OrderBookPanel(QFrame):
    def __init__(self, title: str) -> None:
        super().__init__()
        self.setObjectName("Panel")
        self._title = QLabel(title)
        self._title.setObjectName("SectionTitle")
        self._quote = QLabel("等待行情...")
        self._quote.setObjectName("Subtle")
        self._depth_levels = 5
        self._table = QTableWidget(self._depth_levels * 2, 2)
        self._table.setObjectName("OrderBookTable")
        self._table.setHorizontalHeaderLabels(["价格", "数量"])
        self._table.verticalHeader().setVisible(False)
        self._table.verticalHeader().setDefaultSectionSize(21)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self._table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)
        header = self._table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        header.setMinimumSectionSize(96)
        self._table.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self._table.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        compact_font = QFont()
        compact_font.setPointSize(8)
        self._table.setFont(compact_font)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(6)
        layout.addWidget(self._title)
        layout.addWidget(self._quote)
        layout.addWidget(self._table, 1)
        self._table.setFixedHeight(21 * (self._depth_levels * 2 + 1) + 6)

    def update_leg(self, leg: LegMarket) -> None:
        self._title.setText(f"{leg.inst_id} 盘口 [{leg.source}]")
        self._quote.setText(
            f"最新 {fmt_decimal(leg.last)} | 买一 {fmt_decimal(leg.bid)} | 卖一 {fmt_decimal(leg.ask)}"
        )
        rows = list(reversed(leg.asks[: self._depth_levels])) + list(leg.bids[: self._depth_levels])
        for row_index in range(self._table.rowCount()):
            if row_index >= len(rows):
                self._table.setItem(row_index, 0, QTableWidgetItem(""))
                self._table.setItem(row_index, 1, QTableWidgetItem(""))
                continue
            row = rows[row_index]
            price_item = QTableWidgetItem(fmt_decimal(row.price))
            size_item = QTableWidgetItem(fmt_decimal(row.size))
            price_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            size_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            is_ask = row_index < min(self._depth_levels, len(leg.asks))
            color = QColor("#c83b55" if is_ask else "#1a7f46")
            price_item.setForeground(color)
            size_item.setForeground(color)
            self._table.setItem(row_index, 0, price_item)
            self._table.setItem(row_index, 1, size_item)


class RollTerminalWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("专业套利终端")
        self.resize(1680, 980)
        self._runtime = load_runtime("2211") or load_runtime()
        environment = self._runtime.environment if self._runtime is not None else "live"
        self._feed = MarketFeedThread(environment=environment)
        self._account_feed = AccountFeedThread(self._runtime)
        self._order_feed = OrderFeedThread(self._runtime)
        self._target_thread: TargetInstrumentThread | None = None
        self._execution_thread: RollExecutionThread | None = None
        self._positions: list[FuturesPositionView] = []
        self._latest_snapshot: MarketPairSnapshot | None = None
        self._profile_snapshots: dict[str, dict[str, str]] = {}
        self._unlocked_profiles: set[str] = set()
        self._last_profile_name = self._runtime.credential_profile_name if self._runtime is not None else ""
        self._private_threads_started = False
        self._auto_enabled = False
        self._auto_triggered = False
        self._auto_threshold_value: Decimal | None = None
        self._current_position_key: str = ""
        self._all_opportunities = default_opportunities()
        self._filtered_opportunities = list(self._all_opportunities)
        self._selected_opportunity: ArbitrageOpportunityView | None = None
        self._refresh_profile_snapshots()
        startup_locked = bool(self._last_profile_name and self._profile_requires_password(self._last_profile_name))
        self._feed.snapshot_ready.connect(self._apply_snapshot)
        self._feed.status_changed.connect(self._set_status)
        self._account_feed.positions_ready.connect(self._apply_positions)
        self._account_feed.status_changed.connect(self._set_account_status)
        self._order_feed.orders_ready.connect(self._apply_order_updates)
        self._order_feed.status_changed.connect(self._set_order_status)
        self._build_ui()
        self._feed.start()
        if startup_locked:
            self._account_status.setText(f"API {self._last_profile_name} 未解锁")
            self._order_status.setText("订单WS等待 API 解锁")
            QTimer.singleShot(200, self._unlock_startup_profile)
        else:
            if self._last_profile_name:
                self._unlocked_profiles.add(self._last_profile_name)
            self._start_private_threads()

    def closeEvent(self, event) -> None:  # noqa: ANN001
        if self._execution_thread is not None and self._execution_thread.isRunning():
            QMessageBox.warning(self, "执行中", "移仓执行中，请等待完成后再关闭窗口")
            event.ignore()
            return
        self._stop_runtime_threads()
        if self._target_thread is not None and self._target_thread.isRunning():
            if not self._target_thread.wait(800):
                self._target_thread.terminate()
                self._target_thread.wait(800)
        super().closeEvent(event)

    def _stop_runtime_threads(self) -> None:
        for thread in (self._feed, self._account_feed, self._order_feed):
            if not thread.isRunning():
                continue
            thread.stop()
            if not thread.wait(1500):
                thread.terminate()
                thread.wait(1500)
        self._private_threads_started = False

    def _start_private_threads(self) -> None:
        if self._private_threads_started:
            return
        self._account_feed.start()
        self._order_feed.start()
        self._private_threads_started = True

    def _build_ui(self) -> None:
        root = QWidget()
        main = QVBoxLayout(root)
        main.setContentsMargins(10, 10, 10, 10)
        main.setSpacing(10)

        header = QFrame()
        header.setObjectName("Header")
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(14, 12, 14, 12)
        header_layout.setSpacing(10)
        title_box = QVBoxLayout()
        title_box.setSpacing(2)
        title = QLabel("专业套利终端")
        title.setObjectName("Title")
        subtitle = QLabel("统一承载专业套利、套利开平与交割换月模板的高性能终端。")
        subtitle.setObjectName("Subtitle")
        title_box.addWidget(title)
        title_box.addWidget(subtitle)
        self._status = QLabel("启动中...")
        self._status.setObjectName("Badge")
        self._account_status = QLabel("持仓读取中...")
        self._account_status.setObjectName("Badge")
        self._order_status = QLabel("订单WS等待中...")
        self._order_status.setObjectName("Badge")
        self._api = QComboBox()
        names = profile_names()
        if names:
            self._api.addItems(names)
            if self._runtime is not None:
                index = self._api.findText(self._runtime.credential_profile_name)
                if index >= 0:
                    self._api.setCurrentIndex(index)
        else:
            self._api.addItem("未配置")
        self._api.currentIndexChanged.connect(lambda _index: self._on_api_profile_changed())
        header_layout.addLayout(title_box, 1)
        header_layout.addStretch(1)
        api_label = QLabel("API")
        api_label.setObjectName("Subtle")
        header_layout.addWidget(api_label)
        header_layout.addWidget(self._api)
        header_layout.addWidget(self._account_status)
        header_layout.addWidget(self._order_status)
        header_layout.addWidget(self._status)
        main.addWidget(header)

        content = QWidget()
        content_layout = QHBoxLayout(content)
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.setSpacing(10)

        sidebar = QFrame()
        sidebar.setObjectName("Panel")
        sidebar_layout = QVBoxLayout(sidebar)
        sidebar_layout.setContentsMargins(12, 12, 12, 12)
        sidebar_layout.setSpacing(8)
        sidebar_title = QLabel("套利机会")
        sidebar_title.setObjectName("SectionTitle")
        self._opportunity_search = QLineEdit()
        self._opportunity_search.setPlaceholderText("搜索套利对 / 标的 / 模板")
        self._opportunity_search.textChanged.connect(self._apply_opportunity_filter)
        self._opportunity_list = QListWidget()
        self._opportunity_list.currentRowChanged.connect(self._on_opportunity_selected)
        self._template_badge = QLabel("模板：等待选择")
        self._template_badge.setObjectName("Badge")
        self._opportunity_desc = QLabel("从左侧选择一个套利对后，中心盘口与右侧模板会联动。")
        self._opportunity_desc.setObjectName("Subtle")
        self._opportunity_desc.setWordWrap(True)
        sidebar_layout.addWidget(sidebar_title)
        sidebar_layout.addWidget(self._opportunity_search)
        sidebar_layout.addWidget(self._opportunity_list, 1)
        sidebar_layout.addWidget(self._template_badge)
        sidebar_layout.addWidget(self._opportunity_desc)

        left_column = QVBoxLayout()
        left_column.setSpacing(10)
        right_column = QVBoxLayout()
        right_column.setSpacing(10)

        metrics = QFrame()
        metrics.setObjectName("Panel")
        metrics_layout = QHBoxLayout(metrics)
        metrics_layout.setContentsMargins(12, 12, 12, 12)
        metrics_layout.setSpacing(10)

        spread_card = QFrame()
        spread_card.setObjectName("StatCard")
        spread_layout = QVBoxLayout(spread_card)
        spread_layout.setContentsMargins(12, 10, 12, 10)
        spread_layout.setSpacing(4)
        spread_title = QLabel("当前价差")
        spread_title.setObjectName("StatTitle")
        self._spread = QLabel("绝对价差 - | 价差率 -")
        self._spread.setObjectName("Metric")
        spread_layout.addWidget(spread_title)
        spread_layout.addWidget(self._spread)

        source_card = QFrame()
        source_card.setObjectName("StatCard")
        source_layout = QVBoxLayout(source_card)
        source_layout.setContentsMargins(12, 10, 12, 10)
        source_layout.setSpacing(4)
        source_title = QLabel("行情状态")
        source_title.setObjectName("StatTitle")
        self._source = QLabel("等待盘口...")
        self._source.setObjectName("Subtle")
        self._source.setWordWrap(True)
        source_layout.addWidget(source_title)
        source_layout.addWidget(self._source)

        metrics_layout.addWidget(spread_card, 1)
        metrics_layout.addWidget(source_card, 1)
        left_column.addWidget(metrics)

        books_panel = QFrame()
        books_panel.setObjectName("Panel")
        books_outer = QVBoxLayout(books_panel)
        books_outer.setContentsMargins(12, 12, 12, 12)
        books_outer.setSpacing(8)
        books_title = QLabel("盘口对照")
        books_title.setObjectName("SectionTitle")
        books_outer.addWidget(books_title)
        books = QWidget()
        books_layout = QHBoxLayout(books)
        books_layout.setContentsMargins(0, 0, 0, 0)
        books_layout.setSpacing(12)
        self._left_book = OrderBookPanel("当前交割盘口")
        self._right_book = OrderBookPanel("目标交割盘口")
        books_layout.addWidget(self._left_book, 1)
        books_layout.addWidget(self._right_book, 1)
        books_outer.addWidget(books, 1)
        left_column.addWidget(books_panel, 4)

        positions_panel = QFrame()
        positions_panel.setObjectName("Panel")
        positions_layout = QVBoxLayout(positions_panel)
        positions_layout.setContentsMargins(12, 12, 12, 12)
        positions_layout.setSpacing(8)
        positions_head = QHBoxLayout()
        positions_head.setSpacing(8)
        positions_title = QLabel("交割持仓区")
        positions_title.setObjectName("SectionTitle")
        positions_hint = QLabel("点击任一持仓行，可直接切换为当前交割合约。")
        positions_hint.setObjectName("Subtle")
        positions_head.addWidget(positions_title)
        positions_head.addStretch(1)
        positions_head.addWidget(positions_hint)
        positions_layout.addLayout(positions_head)
        position_focus = QFrame()
        position_focus.setObjectName("StatCard")
        position_focus_layout = QVBoxLayout(position_focus)
        position_focus_layout.setContentsMargins(12, 10, 12, 10)
        position_focus_layout.setSpacing(4)
        position_focus_title = QLabel("当前选中持仓")
        position_focus_title.setObjectName("StatTitle")
        self._position_summary = QLabel("等待持仓...")
        self._position_summary.setObjectName("Metric")
        self._position_summary.setWordWrap(True)
        self._position_action = QLabel("操作方向：等待持仓...")
        self._position_action.setObjectName("Hint")
        self._position_action.setWordWrap(True)
        position_focus_layout.addWidget(position_focus_title)
        position_focus_layout.addWidget(self._position_summary)
        position_focus_layout.addWidget(self._position_action)
        positions_layout.addWidget(position_focus)
        self._positions_table = QTableWidget(0, 4)
        self._positions_table.setHorizontalHeaderLabels(["合约", "方向", "可平(张)", "折合"])
        self._positions_table.verticalHeader().setVisible(False)
        self._positions_table.verticalHeader().setDefaultSectionSize(28)
        self._positions_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._positions_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._positions_table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        positions_header = self._positions_table.horizontalHeader()
        positions_header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        positions_header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        positions_header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        positions_header.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        self._positions_table.setMinimumHeight(220)
        self._positions_table.setMaximumHeight(340)
        self._positions_table.cellClicked.connect(self._on_position_row_clicked)
        positions_layout.addWidget(self._positions_table)
        left_column.addWidget(positions_panel, 2)

        activity_panel = QFrame()
        activity_panel.setObjectName("Panel")
        activity_layout = QVBoxLayout(activity_panel)
        activity_layout.setContentsMargins(12, 12, 12, 12)
        activity_layout.setSpacing(8)
        activity_title = QLabel("执行回报")
        activity_title.setObjectName("SectionTitle")
        activity_layout.addWidget(activity_title)

        self._current = QComboBox()
        self._current.setEditable(True)
        self._current.addItems(["BTC-USD-260626", "BTC-USD-260925"])
        self._current.currentIndexChanged.connect(lambda _index: self._on_current_contract_changed())
        self._target = QComboBox()
        self._target.setEditable(True)
        self._target.addItems(["BTC-USD-260925", "BTC-USD-260703"])
        self._configure_contract_combo(self._current, minimum_width=420, popup_width=860)
        self._configure_contract_combo(self._target, minimum_width=260, popup_width=520)
        self._qty = QLineEdit("10")
        self._qty_label = QLabel("数量(张)")
        self._use_limit_orders = QCheckBox("按限价挂单")
        self._direction_hint = QLabel("准备下单方向：请先选择当前交割持仓")
        self._direction_hint.setObjectName("Hint")
        self._max_slippage = QLineEdit("0.15")
        self._batch_count = QLineEdit("10")
        self._batch_qty = QLineEdit("1")
        self._mode = QComboBox()
        self._mode.addItem("双腿吃单", "dual_taker")
        self._mode.addItem("旧合约挂单/新合约吃单", "old_maker_new_taker")
        self._mode.addItem("新合约挂单/旧合约吃单", "new_maker_old_taker")
        self._mode.addItem("双方挂单/先成后市价", "both_maker_first_taker")
        self._maker_wait = QLineEdit("6")
        self._chase_limit = QLineEdit("3")
        self._current_limit_price = QLineEdit("")
        self._target_limit_price = QLineEdit("")
        self._auto_threshold = QLineEdit("450")
        self._qty.setPlaceholderText("先用 1-2 张测试")
        self._max_slippage.setPlaceholderText("例如 0.15")
        self._batch_count.setPlaceholderText("例如 10")
        self._batch_qty.setPlaceholderText("例如 1")
        self._maker_wait.setPlaceholderText("例如 6")
        self._chase_limit.setPlaceholderText("例如 3")
        self._current_limit_price.setPlaceholderText("可不填")
        self._target_limit_price.setPlaceholderText("可不填")
        self._auto_threshold.setPlaceholderText("例如 450")
        self._current.setToolTip("当前需要回补的交割合约持仓。")
        self._target.setToolTip("移仓后准备开出的目标交割合约。")
        self._qty.setToolTip("按 OKX 页面显示的张数填写。你输入 1，就是按 OKX 的 1 张执行。")
        self._mode.setToolTip("双腿吃单更偏速度；双方挂单/先成后市价用于抢排队价格。")
        self._use_limit_orders.setToolTip(
            "想尽量按指定价格成交、能接受等待或追单时再勾选；追求更快成交可不勾。"
            "若执行方式本身带“挂单”，挂单腿会直接走限价单，这个勾选主要影响“双腿吃单”路径。"
        )
        self._max_slippage.setToolTip("最大允许滑点百分比，例如 0.15 表示 0.15%。")
        self._batch_count.setToolTip("不填写每批张数时，按分批次数拆单。")
        self._batch_qty.setToolTip("填写后优先按每批张数拆单；此时分批次数只作参考，不参与实际拆分。")
        self._maker_wait.setToolTip("挂单等待秒数，超时后按追单设置处理。")
        self._chase_limit.setToolTip("挂单未成交后的追单次数，0 表示不追。")
        self._current_limit_price.setToolTip("旧合约限价。当前为空单时通常表示买入平空价格。")
        self._target_limit_price.setToolTip("目标合约限价。当前为空单时通常表示卖出开空价格。")
        self._auto_threshold.setToolTip(
            "点击“启动自动移仓”后才会开始监控；当目标合约中间价 - 当前合约中间价 >= 阈值时，自动触发一次真实移仓。"
        )
        self._qty.textChanged.connect(lambda _text: self._update_batch_hint())
        self._batch_count.textChanged.connect(lambda _text: self._update_batch_hint())
        self._batch_qty.textChanged.connect(lambda _text: self._update_batch_hint())
        for entry in (
            self._qty,
            self._max_slippage,
            self._batch_count,
            self._batch_qty,
            self._maker_wait,
            self._chase_limit,
            self._current_limit_price,
            self._target_limit_price,
            self._auto_threshold,
        ):
            entry.setMaximumWidth(150)
        switch = QPushButton("切换合约")
        switch.setObjectName("Secondary")
        switch.clicked.connect(self._switch_pair)
        self._execute_button = QPushButton("执行移仓")
        self._execute_button.setObjectName("Primary")
        self._execute_button.setEnabled(False)
        self._execute_button.clicked.connect(self._confirm_and_execute)
        self._start_auto_button = QPushButton("启动自动移仓")
        self._start_auto_button.setObjectName("Secondary")
        self._start_auto_button.clicked.connect(self._start_auto_roll)
        self._stop_auto_button = QPushButton("停止自动移仓")
        self._stop_auto_button.setObjectName("Danger")
        self._stop_auto_button.clicked.connect(self._stop_auto_roll)
        self._stop_auto_button.setEnabled(False)
        self._auto_help = QLabel(
            "自动移仓说明：只有点击“启动自动移仓”后才开始监控。"
            "监控条件是“目标合约中间价 - 当前合约中间价 >= 阈值”；达到阈值后会直接按当前页面参数提交一次真实订单，"
            "本轮监控随即自动停止，不会重复下单。"
        )
        self._auto_help.setObjectName("Hint")
        self._auto_help.setWordWrap(True)
        self._auto_hint = QLabel("自动移仓：填写阈值后启动；达到阈值只触发一次，避免重复下单。")
        self._auto_hint.setObjectName("Hint")
        self._batch_hint = QLabel()
        self._batch_hint.setObjectName("Hint")
        self._batch_hint.setWordWrap(True)

        selected_panel = QFrame()
        selected_panel.setObjectName("Panel")
        selected_layout = QVBoxLayout(selected_panel)
        selected_layout.setContentsMargins(12, 12, 12, 12)
        selected_layout.setSpacing(4)
        selected_title = QLabel("当前策略")
        selected_title.setObjectName("GuideTitle")
        self._selected_pair_title = QLabel("等待选择套利机会")
        self._selected_pair_title.setObjectName("SectionTitle")
        self._selected_pair_legs = QLabel("左腿 - | 右腿 -")
        self._selected_pair_legs.setObjectName("Subtle")
        self._execution_scope_hint = QLabel("当前真实执行能力：交割换月模板。其他专业套利模板先完成看盘与选对骨架。")
        self._execution_scope_hint.setObjectName("Hint")
        self._execution_scope_hint.setWordWrap(True)
        selected_layout.addWidget(selected_title)
        selected_layout.addWidget(self._selected_pair_title)
        selected_layout.addWidget(self._selected_pair_legs)
        selected_layout.addWidget(self._execution_scope_hint)
        right_column.addWidget(selected_panel)

        guide = QFrame()
        guide.setObjectName("Guide")
        guide_layout = QVBoxLayout(guide)
        guide_layout.setContentsMargins(12, 12, 12, 12)
        guide_layout.setSpacing(4)
        guide_title = QLabel("操作引导")
        guide_title.setObjectName("GuideTitle")
        guide_text = QLabel(
            "1. 先选“当前交割”持仓，再选“目标交割”远月合约。\n"
            "2. 数量按 OKX 页面张数填写，建议先 1-2 张试单。\n"
            "3. 追求速度用“双腿吃单”；想尽量控制成交价时再勾“按限价挂单”。若执行方式本身带“挂单”，挂单腿会直接按限价下单。\n"
            "4. 当前为空单时，旧合约应买入平空，目标合约应卖出开空。\n"
            "5. 自动移仓只有点击“启动自动移仓”后才会开启监控；达到阈值会直接下真实单，并自动停止本轮监控。\n"
            "6. 若同时填写“分批次数”和“每批张数”，系统优先按“每批张数”拆单。"
        )
        guide_text.setObjectName("GuideText")
        guide_text.setWordWrap(True)
        guide_layout.addWidget(guide_title)
        guide_layout.addWidget(guide_text)
        right_column.addWidget(guide)

        controls = QFrame()
        controls.setObjectName("Panel")
        controls_layout = QGridLayout(controls)
        controls_layout.setContentsMargins(12, 12, 12, 12)
        controls_layout.setHorizontalSpacing(8)
        controls_layout.setVerticalSpacing(8)
        controls_title = QLabel("下单参数")
        controls_title.setObjectName("SectionTitle")
        controls_layout.addWidget(controls_title, 0, 0, 1, 4)
        controls_layout.addWidget(QLabel("当前交割"), 1, 0)
        controls_layout.addWidget(self._current, 2, 0, 1, 4)
        controls_layout.addWidget(QLabel("目标交割"), 3, 0)
        controls_layout.addWidget(self._target, 4, 0, 1, 3)
        controls_layout.addWidget(switch, 4, 3)
        controls_layout.addWidget(self._qty_label, 5, 0)
        controls_layout.addWidget(self._qty, 6, 0)
        controls_layout.addWidget(QLabel("执行方式"), 5, 1)
        controls_layout.addWidget(self._mode, 6, 1, 1, 3)
        controls_layout.addWidget(self._direction_hint, 7, 0, 1, 4)
        controls_layout.addWidget(self._use_limit_orders, 8, 0, 1, 2)
        controls_layout.addWidget(QLabel("最大滑点(%)"), 9, 0)
        controls_layout.addWidget(self._max_slippage, 10, 0)
        controls_layout.addWidget(QLabel("分批次数"), 9, 1)
        controls_layout.addWidget(self._batch_count, 10, 1)
        controls_layout.addWidget(QLabel("每批张数"), 9, 2)
        controls_layout.addWidget(self._batch_qty, 10, 2)
        controls_layout.addWidget(self._batch_hint, 11, 0, 1, 4)
        controls_layout.addWidget(QLabel("挂单等待(s)"), 12, 0)
        controls_layout.addWidget(self._maker_wait, 13, 0)
        controls_layout.addWidget(QLabel("追单次数"), 12, 1)
        controls_layout.addWidget(self._chase_limit, 13, 1)
        controls_layout.addWidget(QLabel("自动移仓价差≥"), 12, 2)
        controls_layout.addWidget(self._auto_threshold, 13, 2)
        controls_layout.addWidget(QLabel("旧合约限价"), 14, 0)
        controls_layout.addWidget(self._current_limit_price, 15, 0)
        controls_layout.addWidget(QLabel("目标合约限价"), 14, 1)
        controls_layout.addWidget(self._target_limit_price, 15, 1)
        controls_layout.addWidget(self._auto_help, 16, 0, 1, 4)
        controls_layout.addWidget(self._auto_hint, 17, 0, 1, 4)
        controls_layout.addWidget(self._start_auto_button, 18, 0, 1, 2)
        controls_layout.addWidget(self._stop_auto_button, 18, 2)
        controls_layout.addWidget(self._execute_button, 18, 3)
        right_column.addWidget(controls)
        right_column.addStretch(1)

        self._log = QTextEdit()
        self._log.setReadOnly(True)
        self._log.setPlaceholderText("执行日志会实时显示在这里。")
        self._execution_table = QTableWidget(0, 5)
        self._execution_table.setHorizontalHeaderLabels(["阶段", "旧合约", "新合约", "成交", "状态"])
        self._execution_table.verticalHeader().setVisible(False)
        self._execution_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._execution_table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)
        execution_header = self._execution_table.horizontalHeader()
        execution_header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        execution_header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        execution_header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        execution_header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        execution_header.setSectionResizeMode(4, QHeaderView.ResizeMode.Stretch)
        self._order_table = QTableWidget(0, 8)
        self._order_table.setHorizontalHeaderLabels(["合约", "订单号", "方向", "类型", "状态", "价格", "成交均价", "成交/数量"])
        self._order_table.verticalHeader().setVisible(False)
        self._order_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._order_table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)
        order_header = self._order_table.horizontalHeader()
        order_header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        order_header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        order_header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        order_header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        order_header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        order_header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        order_header.setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)
        order_header.setSectionResizeMode(7, QHeaderView.ResizeMode.ResizeToContents)

        execution_title = QLabel("批次执行")
        execution_title.setObjectName("Subtle")
        orders_title = QLabel("委托回报")
        orders_title.setObjectName("Subtle")
        logs_title = QLabel("日志")
        logs_title.setObjectName("Subtle")
        activity_layout.addWidget(execution_title)
        activity_layout.addWidget(self._execution_table, 2)
        activity_layout.addWidget(orders_title)
        activity_layout.addWidget(self._order_table, 2)
        activity_layout.addWidget(logs_title)
        activity_layout.addWidget(self._log, 2)
        left_column.addWidget(activity_panel, 4)

        content_layout.addWidget(sidebar, 3)
        content_layout.addLayout(left_column, 7)
        content_layout.addLayout(right_column, 4)
        main.addWidget(content, 1)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setWidget(root)
        self.setCentralWidget(scroll)
        self._update_batch_hint()
        self._reload_opportunity_list()

    @staticmethod
    def _configure_contract_combo(combo: QComboBox, *, minimum_width: int, popup_width: int) -> None:
        combo.setMinimumWidth(minimum_width)
        combo.setMinimumContentsLength(32)
        combo.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToMinimumContentsLengthWithIcon)
        combo.view().setMinimumWidth(popup_width)

    def _reload_opportunity_list(self) -> None:
        selected_key = self._selected_opportunity.key if self._selected_opportunity is not None else ""
        self._opportunity_list.blockSignals(True)
        self._opportunity_list.clear()
        for item in self._filtered_opportunities:
            row = QListWidgetItem(f"{item.title}\n{item.left_inst_id}  <->  {item.right_inst_id}")
            row.setData(Qt.ItemDataRole.UserRole, item.key)
            row.setToolTip(item.description)
            self._opportunity_list.addItem(row)
        self._opportunity_list.blockSignals(False)
        if not self._filtered_opportunities:
            self._selected_opportunity = None
            self._template_badge.setText("模板：无匹配结果")
            self._opportunity_desc.setText("没有匹配到套利对，请修改搜索关键字。")
            self._selected_pair_title.setText("等待选择套利机会")
            self._selected_pair_legs.setText("左腿 - | 右腿 -")
            return
        selected_index = next(
            (idx for idx, item in enumerate(self._filtered_opportunities) if item.key == selected_key),
            0,
        )
        self._opportunity_list.setCurrentRow(selected_index)
        self._on_opportunity_selected(selected_index)

    @Slot(str)
    def _apply_opportunity_filter(self, text: str) -> None:
        self._filtered_opportunities = filter_opportunities(self._all_opportunities, text)
        self._reload_opportunity_list()

    @Slot(int)
    def _on_opportunity_selected(self, row: int) -> None:
        if row < 0 or row >= len(self._filtered_opportunities):
            return
        item = self._filtered_opportunities[row]
        self._selected_opportunity = item
        template_name = "交割换月模板" if item.template == "roll" else "专业套利模板"
        self._template_badge.setText(f"模板：{template_name}")
        self._opportunity_desc.setText(item.description)
        self._selected_pair_title.setText(item.title)
        self._selected_pair_legs.setText(
            f"左腿：{item.left_inst_id} ({item.left_kind}) | 右腿：{item.right_inst_id} ({item.right_kind})"
        )
        if item.template == "roll":
            self._execution_scope_hint.setText("当前真实执行能力：交割换月模板已接通，可直接用右侧参数执行与自动移仓。")
        else:
            self._execution_scope_hint.setText("当前阶段：该模板已接入专业看盘与选对骨架；双腿通用下单器将在下一阶段接入。")
        self._feed.set_pair(item.left_inst_id, item.right_inst_id)
        self._order_feed.set_watched_inst_ids({item.left_inst_id, item.right_inst_id})
        if item.template == "roll":
            selected_position = self._selected_position()
            if selected_position is None or selected_position.inst_id != item.left_inst_id:
                current_index = next(
                    (
                        idx
                        for idx, position in enumerate(self._positions)
                        if position.inst_id == item.left_inst_id
                    ),
                    -1,
                )
                if current_index >= 0:
                    self._current.setCurrentIndex(current_index)
                else:
                    self._current.setEditText(item.left_inst_id)
            target_index = self._target.findText(item.right_inst_id)
            if target_index >= 0:
                self._target.setCurrentIndex(target_index)
            else:
                self._target.setEditText(item.right_inst_id)
        self._set_status(f"已切换套利对：{item.left_inst_id} / {item.right_inst_id}")
        self._sync_execute_button()

    @Slot()
    def _on_current_contract_changed(self) -> None:
        current_data = self._current.currentData()
        self._current_position_key = str(current_data or "").strip()
        self._refresh_target_candidates()
        self._update_qty_unit_hint()
        position = self._selected_position()
        if position is not None:
            self._position_summary.setText(position.label)
            self._position_action.setText(
                f"操作方向：{roll_direction_from_position(position).summary_text}"
            )
            self._highlight_position_row(position.position_key)

    @Slot(int, int)
    def _on_position_row_clicked(self, row: int, _column: int) -> None:
        item = self._positions_table.item(row, 0)
        if item is None:
            return
        position_key = str(item.data(Qt.ItemDataRole.UserRole) or "").strip()
        if not position_key:
            return
        index = self._current.findData(position_key)
        if index >= 0:
            self._current.setCurrentIndex(index)

    @Slot()
    def _switch_pair(self) -> None:
        selected_position = self._selected_position()
        current = selected_position.inst_id if selected_position is not None else self._current.currentText().strip().upper()
        target = self._target.currentText().strip().upper()
        if not current or not target:
            self._set_status("请选择当前和目标交割合约")
            if hasattr(self, "_source"):
                self._source.setText("等待选择合约...")
            return
        self._set_status(f"正在切换：{current} / {target}")
        if hasattr(self, "_source"):
            self._source.setText(f"切换中：{current} -> {target}")
        self._feed.set_pair(current, target)
        self._order_feed.set_watched_inst_ids({current, target})
        self._sync_execute_button()

    @Slot()
    def _refresh_target_candidates(self) -> None:
        selected_position = self._selected_position()
        current = selected_position.inst_id if selected_position is not None else self._current.currentText().strip().upper()
        if not current:
            return
        if self._target_thread is not None and self._target_thread.isRunning():
            return
        self._target_thread = TargetInstrumentThread(current)
        self._target_thread.targets_ready.connect(self._apply_target_candidates)
        self._target_thread.status_changed.connect(self._set_status)
        self._target_thread.start()

    @Slot(object)
    def _apply_snapshot(self, snapshot: MarketPairSnapshot) -> None:
        self._latest_snapshot = snapshot
        self._left_book.update_leg(snapshot.current)
        self._right_book.update_leg(snapshot.target)
        self._spread.setText(
            f"绝对价差 {fmt_decimal(snapshot.spread_abs)} | 价差率 {fmt_decimal(snapshot.spread_pct, 4)}%"
        )
        self._source.setText(snapshot.status)
        self._status.setText("在线")
        self._maybe_trigger_auto_roll(snapshot)

    def _maybe_trigger_auto_roll(self, snapshot: MarketPairSnapshot) -> None:
        if not self._auto_enabled or self._auto_triggered:
            return
        if self._execution_thread is not None and self._execution_thread.isRunning():
            return
        if self._auto_threshold_value is None or snapshot.spread_abs is None:
            return
        if snapshot.spread_abs < self._auto_threshold_value:
            return
        self._auto_triggered = True
        self._auto_enabled = False
        self._update_auto_controls()
        self._auto_hint.setText("自动移仓已触发：本轮监控已自动关闭，避免重复下单。")
        self._append_log(
            f"自动移仓触发：当前价差 {fmt_decimal(snapshot.spread_abs)} >= {fmt_decimal(self._auto_threshold_value)}"
        )
        plan = self._build_execution_plan()
        if plan is None:
            self._append_log("自动移仓触发失败：执行参数无效。")
            return
        self._start_execution(plan, "自动移仓已触发，开始执行...")

    @Slot(str)
    def _set_status(self, text: str) -> None:
        self._status.setText(text)

    def _refresh_profile_snapshots(self) -> None:
        snapshot = load_credentials_profiles_snapshot()
        profiles = snapshot.get("profiles", {}) if isinstance(snapshot, dict) else {}
        self._profile_snapshots = {
            str(name).strip(): dict(profile)
            for name, profile in profiles.items()
            if str(name).strip() and isinstance(profile, dict)
        } if isinstance(profiles, dict) else {}
        self._unlocked_profiles.intersection_update(set(self._profile_snapshots))

    def _profile_requires_password(self, profile_name: str) -> bool:
        return credential_profile_has_switch_password(self._profile_snapshots.get(profile_name.strip(), {}))

    def _ensure_profile_unlocked(self, profile_name: str) -> bool:
        target = profile_name.strip()
        if not target:
            return False
        if not self._profile_requires_password(target):
            self._unlocked_profiles.add(target)
            return True
        if target in self._unlocked_profiles:
            return True
        password, accepted = QInputDialog.getText(
            self,
            "输入 API 切换密码",
            f"API 配置 {target} 已设置切换密码，请输入后继续：",
            QLineEdit.EchoMode.Password,
        )
        if not accepted:
            return False
        if verify_profile_switch_password(self._profile_snapshots.get(target, {}), password):
            self._unlocked_profiles.add(target)
            return True
        QMessageBox.warning(self, "密码错误", f"API 配置 {target} 的切换密码不正确。")
        return False

    @Slot()
    def _unlock_startup_profile(self) -> None:
        profile_name = self._last_profile_name.strip()
        if not profile_name:
            return
        if self._ensure_profile_unlocked(profile_name):
            self._start_private_threads()
            self._account_status.setText("持仓读取中...")
            self._order_status.setText("订单WS等待中...")
            self._append_log(f"API 已解锁：{profile_name}")
            self._sync_execute_button()
            return
        self._account_status.setText(f"API {profile_name} 未解锁")
        self._order_status.setText("订单WS未启动")
        self._set_status("API 未解锁，请重新选择 API 或重启后输入密码")
        self._sync_execute_button()

    @Slot()
    def _on_api_profile_changed(self) -> None:
        selected = self._api.currentText().strip()
        if not selected or selected == "未配置":
            return
        if selected == self._last_profile_name:
            if self._ensure_profile_unlocked(selected):
                return
            self._restore_api_selection()
            return
        if self._execution_thread is not None and self._execution_thread.isRunning():
            QMessageBox.warning(self, "提示", "移仓执行中，请等待完成后再切换 API。")
            self._restore_api_selection()
            return
        if self._auto_enabled:
            QMessageBox.warning(self, "提示", "请先停止自动移仓，再切换 API。")
            self._restore_api_selection()
            return
        self._refresh_profile_snapshots()
        if not self._ensure_profile_unlocked(selected):
            self._restore_api_selection()
            return
        self._apply_api_profile(selected)

    def _restore_api_selection(self) -> None:
        if not self._last_profile_name:
            return
        index = self._api.findText(self._last_profile_name)
        if index < 0:
            return
        self._api.blockSignals(True)
        self._api.setCurrentIndex(index)
        self._api.blockSignals(False)

    def _apply_api_profile(self, profile_name: str) -> None:
        runtime = load_runtime(profile_name)
        if runtime is None:
            QMessageBox.warning(self, "切换失败", f"API 配置 {profile_name} 不可用，请检查凭证。")
            self._restore_api_selection()
            return
        self._stop_runtime_threads()
        self._runtime = runtime
        self._positions = []
        self._current.blockSignals(True)
        self._current.clear()
        self._current.addItem("正在读取交割持仓...", "")
        self._current.blockSignals(False)
        self._position_summary.setText("正在读取交割持仓...")
        self._source.setText("等待盘口切换...")
        self._order_table.setRowCount(0)
        self._execution_table.setRowCount(0)
        self._account_status.setText("持仓读取中...")
        self._order_status.setText("订单WS等待中...")
        self._feed = MarketFeedThread(environment=runtime.environment)
        self._account_feed = AccountFeedThread(runtime)
        self._order_feed = OrderFeedThread(runtime)
        self._feed.snapshot_ready.connect(self._apply_snapshot)
        self._feed.status_changed.connect(self._set_status)
        self._account_feed.positions_ready.connect(self._apply_positions)
        self._account_feed.status_changed.connect(self._set_account_status)
        self._order_feed.orders_ready.connect(self._apply_order_updates)
        self._order_feed.status_changed.connect(self._set_order_status)
        self._feed.start()
        self._start_private_threads()
        self._last_profile_name = profile_name
        self._unlocked_profiles.add(profile_name)
        self._switch_pair()
        self._sync_execute_button()
        self._append_log(f"API 已切换：{profile_name} | {runtime.environment}")

    @Slot(object)
    def _apply_positions(self, positions: list[FuturesPositionView]) -> None:
        self._positions = positions
        current_data = self._current.currentData()
        current_key = str(current_data or self._current_position_key or "").strip()
        self._current.blockSignals(True)
        self._current.clear()
        for position in positions:
            self._current.addItem(position.label, position.position_key)
        if positions:
            index = next((i for i, item in enumerate(positions) if item.position_key == current_key), 0)
            self._current.setCurrentIndex(index)
            selected_position_key = positions[index].position_key
        else:
            self._current.addItem("未读取到交割持仓", "")
            selected_position_key = ""
        self._positions_table.setRowCount(0)
        for position in positions:
            row = self._positions_table.rowCount()
            self._positions_table.insertRow(row)
            base_ccy = position.inst_id.split("-")[0].strip().upper()
            exposure_text = (
                f"{fmt_decimal(position.notional_base)} {base_ccy}"
                if position.notional_base is not None
                else "-"
            )
            values = [
                position.inst_id,
                "空" if str(position.side).lower() == "short" else "多",
                fmt_decimal(position.available),
                exposure_text,
            ]
            for column, value in enumerate(values):
                item = QTableWidgetItem(str(value))
                if column == 0:
                    item.setData(Qt.ItemDataRole.UserRole, position.position_key)
                if column in {2, 3}:
                    item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                self._positions_table.setItem(row, column, item)
        self._current.blockSignals(False)
        self._current_position_key = selected_position_key
        selected_position = next((item for item in positions if item.position_key == selected_position_key), None)
        if selected_position is not None:
            self._position_summary.setText(selected_position.label)
            self._position_action.setText(
                f"操作方向：{roll_direction_from_position(selected_position).summary_text}"
            )
        else:
            self._position_summary.setText("未读取到交割持仓")
            self._position_action.setText("操作方向：等待持仓...")
        self._highlight_position_row(selected_position_key)
        if selected_position is not None:
            self._refresh_target_candidates()
        self._update_qty_unit_hint()
        self._sync_execute_button()

    @Slot(str, object)
    def _apply_target_candidates(self, current_inst_id: str, targets: list[str]) -> None:
        selected_position = self._selected_position()
        selected_current = selected_position.inst_id if selected_position is not None else self._current.currentText().strip().upper()
        if current_inst_id and selected_current != current_inst_id:
            return
        previous = self._target.currentText().strip().upper()
        self._target.blockSignals(True)
        self._target.clear()
        if targets:
            self._target.addItems(targets)
            index = self._target.findText(previous)
            self._target.setCurrentIndex(index if index >= 0 else 0)
        else:
            self._target.addItem(previous or "BTC-USD-260925")
        self._target.blockSignals(False)
        self._switch_pair()
        self._sync_execute_button()

    @Slot()
    def _confirm_and_execute(self) -> None:
        if self._runtime is None:
            QMessageBox.warning(self, "无法执行", "API 配置不可用")
            return
        plan = self._build_execution_plan()
        if plan is None:
            return
        batch_preview = self._batch_preview_text_for_plan(plan)
        answer = QMessageBox.question(
            self,
            "确认执行移仓",
            (
                f"当前合约：{plan.current.inst_id}\n"
                f"目标合约：{plan.target_inst_id}\n"
                f"数量：{plan.qty} 张（按 OKX 页面张数）\n"
                f"准备下单：{roll_direction_from_position(plan.current).summary_text}\n"
                f"方式：{plan.execution_label}\n\n"
                f"按限价挂单：{'是' if plan.use_limit_orders else '否'}\n"
                f"最大滑点：{fmt_decimal(plan.max_slippage * 100)}%\n"
                f"分批次数：{plan.batch_count}\n"
                f"每批张数：{fmt_decimal(plan.batch_contract_qty) if plan.batch_contract_qty else '-'}\n"
                f"拆单预览：{batch_preview}\n"
                f"挂单等待：{plan.maker_wait_seconds:g}s\n"
                f"追单次数：{plan.chase_limit}\n"
                f"旧合约买入限价：{fmt_decimal(plan.current_limit_price)}\n"
                f"新合约卖出限价：{fmt_decimal(plan.target_limit_price)}\n\n"
                "确认后会向 OKX 提交真实订单。"
            ),
        )
        if answer != QMessageBox.StandardButton.Yes:
            return
        self._start_execution(plan, "开始执行移仓...")

    def _start_execution(self, plan: RollExecutionPlan, log_message: str) -> None:
        if self._execution_thread is not None and self._execution_thread.isRunning():
            self._append_log("已有移仓任务正在执行，本次请求已忽略。")
            return
        self._execute_button.setEnabled(False)
        self._execution_table.setRowCount(0)
        self._append_log(log_message)
        self._execution_thread = RollExecutionThread(runtime=self._runtime, plan=plan)
        self._execution_thread.log.connect(self._append_log)
        self._execution_thread.status.connect(self._apply_execution_status)
        self._execution_thread.finished_with_result.connect(self._handle_execution_result)
        self._execution_thread.start()

    @Slot()
    def _start_auto_roll(self) -> None:
        if self._runtime is None:
            QMessageBox.warning(self, "无法启动", "API 配置不可用")
            return
        plan = self._build_execution_plan()
        if plan is None:
            return
        try:
            threshold = parse_optional_decimal(self._auto_threshold.text(), field_name="自动移仓价差")
        except ValueError as exc:
            QMessageBox.warning(self, "参数错误", str(exc))
            return
        if threshold is None:
            QMessageBox.warning(self, "参数错误", "请填写自动移仓价差阈值")
            return
        answer = QMessageBox.question(
            self,
            "确认启动自动移仓",
            (
                f"当“目标合约中间价 - 当前合约中间价 >= {fmt_decimal(threshold)}”时，将自动提交真实订单。\n"
                f"当前合约：{plan.current.inst_id}\n"
                f"目标合约：{plan.target_inst_id}\n"
                f"数量：{plan.qty} 张（按 OKX 页面张数）\n"
                f"准备下单：{roll_direction_from_position(plan.current).summary_text}\n"
                f"方式：{plan.execution_label}\n"
                f"拆单预览：{self._batch_preview_text_for_plan(plan)}\n\n"
                "启动后达到阈值会自动触发一次，不再二次确认。"
            ),
        )
        if answer != QMessageBox.StandardButton.Yes:
            return
        self._auto_threshold_value = threshold
        self._auto_enabled = True
        self._auto_triggered = False
        self._update_auto_controls()
        self._auto_hint.setText(
            f"自动移仓监控中：目标合约中间价 - 当前合约中间价 >= {fmt_decimal(threshold)} 时触发一次真实订单。"
        )
        self._append_log(
            f"自动移仓已启动：目标合约中间价 - 当前合约中间价 >= {fmt_decimal(threshold)} 时触发。"
        )
        if self._latest_snapshot is not None:
            self._maybe_trigger_auto_roll(self._latest_snapshot)

    @Slot()
    def _stop_auto_roll(self) -> None:
        self._auto_enabled = False
        self._update_auto_controls()
        self._auto_hint.setText("自动移仓已停止：修改参数后可重新启动。")
        self._append_log("自动移仓已停止。")

    def _build_execution_plan(self) -> RollExecutionPlan | None:
        position = self._selected_position()
        if position is None:
            QMessageBox.warning(self, "参数错误", "请先选择当前交割合约持仓")
            return None
        target = self._target.currentText().strip().upper()
        if not target:
            QMessageBox.warning(self, "参数错误", "请先选择目标交割合约")
            return None
        try:
            qty = parse_roll_qty(self._qty.text(), max_qty=position.available)
            max_slippage = parse_slippage_percent(self._max_slippage.text())
            batch_count = parse_positive_int(self._batch_count.text(), field_name="分批次数", default=1)
            batch_contract_qty = parse_optional_decimal(self._batch_qty.text(), field_name="每批张数")
            maker_wait_seconds = parse_positive_float(self._maker_wait.text(), field_name="挂单等待", default=6.0)
            chase_limit = parse_nonnegative_int(self._chase_limit.text(), field_name="追单次数", default=3)
            current_limit_price = parse_optional_decimal(self._current_limit_price.text(), field_name="旧合约买入限价")
            target_limit_price = parse_optional_decimal(self._target_limit_price.text(), field_name="新合约卖出限价")
        except ValueError as exc:
            QMessageBox.warning(self, "参数错误", str(exc))
            return None
        return RollExecutionPlan(
            current=position,
            target_inst_id=target,
            qty=qty,
            execution_label=self._mode.currentText().strip(),
            execution_mode_value=str(self._mode.currentData() or ""),
            max_slippage=max_slippage,
            use_limit_orders=self._use_limit_orders.isChecked(),
            current_limit_price=current_limit_price,
            target_limit_price=target_limit_price,
            batch_count=batch_count,
            batch_contract_qty=batch_contract_qty,
            maker_wait_seconds=maker_wait_seconds,
            chase_limit=chase_limit,
        )

    def _selected_position(self) -> FuturesPositionView | None:
        current_data = self._current.currentData()
        position_key = str(current_data or self._current_position_key or "").strip()
        return next((item for item in self._positions if item.position_key == position_key), None)

    def _highlight_position_row(self, position_key: str) -> None:
        target = position_key.strip()
        self._positions_table.blockSignals(True)
        self._positions_table.clearSelection()
        if target:
            for row in range(self._positions_table.rowCount()):
                item = self._positions_table.item(row, 0)
                if item is None:
                    continue
                row_position_key = str(item.data(Qt.ItemDataRole.UserRole) or "").strip()
                if row_position_key == target:
                    self._positions_table.selectRow(row)
                    break
        self._positions_table.blockSignals(False)

    def _update_qty_unit_hint(self) -> None:
        position = self._selected_position()
        if position is None:
            self._qty_label.setText("数量(张)")
            self._qty.setToolTip("按 OKX 页面显示的张数填写。你输入 1，就是按 OKX 的 1 张执行。")
            self._direction_hint.setText("准备下单方向：请先选择当前交割持仓")
            self._position_action.setText("操作方向：等待持仓...")
            self._update_batch_hint()
            return
        contract_hint = ""
        if position.contract_value is not None and position.contract_value_ccy:
            contract_hint = f"，当前 1 张={fmt_decimal(position.contract_value)} {position.contract_value_ccy}"
        self._qty_label.setText("数量(张)")
        self._qty.setToolTip(
            f"按 OKX 页面显示的张数填写。你输入 1，就是按 OKX 的 1 张执行{contract_hint}。"
        )
        direction_text = roll_direction_from_position(position).summary_text
        self._direction_hint.setText(f"准备下单方向：{direction_text}")
        self._position_action.setText(f"操作方向：{direction_text}")
        self._update_batch_hint()

    def _update_batch_hint(self) -> None:
        self._batch_hint.setText(self._current_batch_preview_text())

    def _current_batch_preview_text(self) -> str:
        position = self._selected_position()
        if position is None:
            return "拆单规则：若同时填写“分批次数”和“每批张数”，系统优先按“每批张数”拆单。"
        try:
            qty = parse_roll_qty(self._qty.text(), max_qty=position.available)
            batch_count = parse_positive_int(self._batch_count.text(), field_name="分批次数", default=1)
            batch_contract_qty = parse_optional_decimal(self._batch_qty.text(), field_name="每批张数")
        except ValueError:
            return "拆单规则：若同时填写“分批次数”和“每批张数”，系统优先按“每批张数”拆单。"
        return self._batch_preview_text(
            qty=qty,
            batch_count=batch_count,
            batch_contract_qty=batch_contract_qty,
            lot_size=position.lot_size,
        )

    def _batch_preview_text_for_plan(self, plan: RollExecutionPlan) -> str:
        return self._batch_preview_text(
            qty=plan.qty,
            batch_count=plan.batch_count,
            batch_contract_qty=plan.batch_contract_qty,
            lot_size=plan.current.lot_size,
        )

    def _batch_preview_text(
        self,
        *,
        qty: Decimal,
        batch_count: int,
        batch_contract_qty: Decimal | None,
        lot_size: Decimal,
    ) -> str:
        batches = self._preview_batches(
            total_qty=qty,
            batch_count=batch_count,
            batch_contract_qty=batch_contract_qty,
            lot_size=lot_size,
        )
        if not batches:
            return "拆单预览暂不可用，请检查数量和批次参数。"
        batch_summary = self._describe_batches(batches)
        if batch_contract_qty is not None:
            return (
                f"拆单预览：已填写每批张数 {fmt_decimal(batch_contract_qty)} 张，系统会优先按每批张数拆单。"
                f"当前 {fmt_decimal(qty)} 张预计执行为 {batch_summary}；分批次数 {batch_count} 此时不生效。"
            )
        return (
            f"拆单预览：当前 {fmt_decimal(qty)} 张预计按分批次数 {batch_count} 拆成 {batch_summary}。"
        )

    def _preview_batches(
        self,
        *,
        total_qty: Decimal,
        batch_count: int,
        batch_contract_qty: Decimal | None,
        lot_size: Decimal,
    ) -> list[Decimal]:
        normalized_total = self._snap_down(total_qty, lot_size)
        if normalized_total <= 0:
            return []
        if batch_contract_qty is not None:
            normalized_batch = self._snap_down(batch_contract_qty, lot_size)
            if normalized_batch <= 0:
                return []
            batches: list[Decimal] = []
            remaining = normalized_total
            while remaining > 0:
                current = self._snap_down(min(normalized_batch, remaining), lot_size)
                if current <= 0:
                    break
                batches.append(current)
                remaining -= current
            return batches
        if batch_count <= 1:
            return [normalized_total]
        base_batch = self._snap_down(normalized_total / Decimal(batch_count), lot_size)
        if base_batch <= 0:
            return [normalized_total]
        batches = []
        remaining = normalized_total
        while len(batches) < batch_count - 1 and remaining - base_batch > 0:
            batches.append(base_batch)
            remaining -= base_batch
        if remaining > 0:
            batches.append(remaining)
        return batches

    @staticmethod
    def _snap_down(value: Decimal, increment: Decimal) -> Decimal:
        if increment <= 0:
            return value
        return (value / increment).to_integral_value(rounding=ROUND_DOWN) * increment

    def _describe_batches(self, batches: list[Decimal]) -> str:
        if len(batches) == 1:
            return f"1 批 {fmt_decimal(batches[0])} 张"
        if all(batch == batches[0] for batch in batches):
            return f"{len(batches)} 批，每批 {fmt_decimal(batches[0])} 张"
        details = " / ".join(
            f"第{index}批 {fmt_decimal(batch)} 张"
            for index, batch in enumerate(batches, start=1)
        )
        return f"{len(batches)} 批：{details}"

    @Slot(object)
    def _handle_execution_result(self, result) -> None:  # noqa: ANN001
        success = bool(getattr(result, "success", False))
        self._set_status("移仓完成" if success else "移仓失败")
        self._sync_execute_button()

    @Slot(object)
    def _apply_execution_status(self, status: ExecutionStatus) -> None:
        row = self._execution_table.rowCount()
        self._execution_table.insertRow(row)
        filled_text = f"{fmt_decimal(status.current_filled)} / {fmt_decimal(status.target_filled)}"
        values = [
            status.phase,
            status.current_inst_id,
            status.target_inst_id,
            filled_text,
            status.message,
        ]
        for column, value in enumerate(values):
            item = QTableWidgetItem(str(value))
            if status.success is True:
                item.setForeground(QColor("#1a7f46"))
            elif status.success is False:
                item.setForeground(QColor("#c83b55"))
            self._execution_table.setItem(row, column, item)
        self._execution_table.scrollToBottom()

    @Slot(object)
    def _apply_order_updates(self, orders: list[OrderStatusView]) -> None:
        self._order_table.setRowCount(0)
        for order in orders[:30]:
            row = self._order_table.rowCount()
            self._order_table.insertRow(row)
            filled = f"{fmt_decimal(order.filled_size)} / {fmt_decimal(order.size)}"
            values = [
                order.inst_id,
                order.ord_id,
                order.side,
                order.ord_type,
                order.state,
                fmt_decimal(order.price),
                fmt_decimal(order.avg_price),
                filled,
            ]
            for column, value in enumerate(values):
                item = QTableWidgetItem(str(value))
                if order.state in {"filled", "partially_filled"}:
                    item.setForeground(QColor("#1a7f46"))
                elif order.state in {"canceled", "cancelled", "failed"}:
                    item.setForeground(QColor("#c83b55"))
                self._order_table.setItem(row, column, item)

    @Slot(str)
    def _set_order_status(self, text: str) -> None:
        self._order_status.setText(text)

    @Slot(str)
    def _append_log(self, text: str) -> None:
        self._log.append(text)
        self._log.verticalScrollBar().setValue(self._log.verticalScrollBar().maximum())

    def _sync_execute_button(self) -> None:
        roll_template_active = (
            self._selected_opportunity is None
            or self._selected_opportunity.template == "roll"
        )
        if self._runtime is None:
            self._execute_button.setEnabled(False)
            self._update_auto_controls()
            return
        if self._execution_thread is not None and self._execution_thread.isRunning():
            self._execute_button.setEnabled(False)
            self._update_auto_controls()
            return
        self._execute_button.setEnabled(
            roll_template_active
            and self._selected_position() is not None
            and bool(self._target.currentText().strip())
        )
        self._update_auto_controls()

    def _update_auto_controls(self) -> None:
        roll_template_active = (
            self._selected_opportunity is None
            or self._selected_opportunity.template == "roll"
        )
        can_start = (
            self._runtime is not None
            and roll_template_active
            and not self._auto_enabled
            and not (self._execution_thread is not None and self._execution_thread.isRunning())
        )
        if hasattr(self, "_start_auto_button"):
            self._start_auto_button.setEnabled(can_start)
        if hasattr(self, "_stop_auto_button"):
            self._stop_auto_button.setEnabled(self._auto_enabled)

    @Slot(str)
    def _set_account_status(self, text: str) -> None:
        self._account_status.setText(text)
        if not self._positions:
            self._position_summary.setText(text)
            self._position_action.setText("操作方向：等待持仓...")


def run() -> int:
    app = QApplication([])
    app.setStyleSheet(APP_STYLE)
    window = RollTerminalWindow()
    window.show()
    return app.exec()
