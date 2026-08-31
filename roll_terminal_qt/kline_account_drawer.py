from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Iterable

from PySide6.QtCore import QThread, QTimer, Qt, Signal
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMessageBox,
    QPushButton,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from okx_quant.okx_client import OkxPosition, OkxTradeOrderItem
from okx_quant.okx_client import OkxRestClient
from roll_terminal_qt.account_positions_home import (
    POSITION_COLUMNS,
    _break_even_taker_fee_rate,
    _position_break_even_price,
)
from okx_quant.ui_shell import (
    _augment_upl_usdt_prices_from_positions,
    _build_position_instrument_map,
    _build_position_ticker_map,
    _build_upl_usdt_price_map,
    _format_margin_mode,
    _format_mark_price,
    _format_option_trade_side_display,
    _format_optional_approx_usdt,
    _format_optional_decimal,
    _format_optional_decimal_fixed,
    _format_optional_integer,
    _format_optional_usdt,
    _format_optional_usdt_precise,
    _format_position_avg_price,
    _format_position_avg_price_usdt,
    _format_position_market_value,
    _format_position_mark_price_usdt,
    _format_position_option_component_usdt,
    _format_position_option_price_component,
    _format_position_quote_price,
    _format_position_quote_price_usdt,
    _format_position_realized_pnl,
    _format_position_size,
    _format_position_unrealized_pnl,
    _format_ratio,
    _position_delta_value,
    _position_realized_pnl_usdt,
    _position_signed_open_value_approx_usdt,
    _position_theta_usdt,
    _position_unrealized_pnl_usdt,
)
from roll_terminal_qt.shared_order_store import SharedOrderSnapshot, get_shared_order_store
from roll_terminal_qt.realtime_account_store import (
    AccountRealtimeSnapshot,
    get_shared_realtime_account_store,
)


_POSITION_COLUMN_IDS = tuple(column_id for column_id, _heading, _width, _alignment in POSITION_COLUMNS)
_POSITION_COLUMN_INDEX = {column_id: index for index, column_id in enumerate(_POSITION_COLUMN_IDS)}


def _position_table_values(
    position: object,
    *,
    upl_usdt_prices: dict[str, object],
    position_instruments: dict[str, object],
    position_tickers: dict[str, object],
) -> list[object]:
    values = (
        getattr(position, "inst_type", "-"),
        _format_margin_mode(getattr(position, "mgn_mode", "")),
        _format_position_option_price_component(position, upl_usdt_prices, component="time_value"),
        _format_position_option_component_usdt(position, upl_usdt_prices, component="time_value"),
        _format_position_option_price_component(position, upl_usdt_prices, component="intrinsic_value"),
        _format_position_option_component_usdt(position, upl_usdt_prices, component="intrinsic_value"),
        _format_position_quote_price(position, position_instruments, position_tickers, side="bid"),
        _format_position_quote_price_usdt(position, position_tickers, upl_usdt_prices, side="bid"),
        _format_position_quote_price(position, position_instruments, position_tickers, side="ask"),
        _format_position_quote_price_usdt(position, position_tickers, upl_usdt_prices, side="ask"),
        _format_mark_price(position),
        _format_position_mark_price_usdt(position, upl_usdt_prices),
        _format_position_avg_price(position, position_instruments),
        _format_position_avg_price_usdt(position, upl_usdt_prices),
        _format_optional_approx_usdt(_position_signed_open_value_approx_usdt(position, position_instruments, upl_usdt_prices)),
        _format_optional_decimal_fixed(
            _position_break_even_price(position, upl_usdt_prices, fee_rate=_break_even_taker_fee_rate({}, inst_type=getattr(position, "inst_type", ""))),
            places=2,
        ),
        _format_position_size(position, position_instruments),
        _format_option_trade_side_display(position),
        _format_position_unrealized_pnl(position),
        _format_optional_usdt(_position_unrealized_pnl_usdt(position, upl_usdt_prices)),
        _format_position_realized_pnl(position),
        _format_optional_usdt(_position_realized_pnl_usdt(position, upl_usdt_prices)),
        _format_position_market_value(position, position_instruments, upl_usdt_prices),
        _format_optional_decimal(getattr(position, "liquidation_price", None)),
        _format_ratio(getattr(position, "margin_ratio", None), places=2),
        _format_optional_integer(getattr(position, "initial_margin", None)),
        _format_optional_integer(getattr(position, "maintenance_margin", None)),
        _format_optional_decimal_fixed(_position_delta_value(position, position_instruments), places=5),
        _format_optional_decimal_fixed(getattr(position, "gamma", None), places=5),
        _format_optional_decimal_fixed(getattr(position, "vega", None), places=5),
        _format_optional_decimal_fixed(getattr(position, "theta", None), places=5),
        _format_optional_usdt_precise(_position_theta_usdt(position, upl_usdt_prices), places=2),
        getattr(position, "note", "-"),
    )
    return [getattr(position, "inst_id", "-"), *values]


@dataclass(frozen=True)
class AccountDrawerSnapshot:
    positions: tuple[OkxPosition, ...] = ()
    orders: tuple[OkxTradeOrderItem, ...] = ()
    order_history: tuple[OkxTradeOrderItem, ...] = ()
    upl_usdt_prices: dict[str, object] | None = None
    position_instruments: dict[str, object] | None = None
    position_tickers: dict[str, object] | None = None


def filter_account_items(items: Iterable[object], *, scope: str, symbol: str) -> list[object]:
    records = list(items)
    if scope == "all":
        return records
    normalized_symbol = symbol.strip().upper()
    return [
        item
        for item in records
        if str(getattr(item, "inst_id", "") or "").strip().upper() == normalized_symbol
    ]


def order_source_kind(order: object) -> str:
    source_kind = str(getattr(order, "source_kind", "") or "").strip().lower()
    algo_id = str(getattr(order, "algo_id", "") or "").strip()
    return "algo" if source_kind == "algo" or algo_id else "normal"


def order_cancel_reference(order: object) -> str:
    if order_source_kind(order) == "algo":
        names = ("algo_id", "algo_client_order_id", "client_order_id")
    else:
        names = ("order_id", "client_order_id")
    for name in names:
        value = str(getattr(order, name, "") or "").strip()
        if value:
            return value
    return ""


def order_display_direction(item: object) -> str:
    side = str(getattr(item, "side", "") or "").strip().lower()
    if side == "buy":
        return "买入"
    if side == "sell":
        return "卖出"
    pos_side = str(getattr(item, "pos_side", "") or "").strip().lower()
    if pos_side == "long":
        return "买入"
    if pos_side == "short":
        return "卖出"
    return side or pos_side


def order_display_price(item: object) -> str:
    trigger_price = getattr(item, "trigger_price", None)
    order_price = getattr(item, "order_price", None)
    price = getattr(item, "price", None)
    if trigger_price is not None:
        trigger_text = str(trigger_price)
        if order_price is None or order_price == trigger_price:
            return f"触发 {trigger_text}"
        return f"触发 {trigger_text}=>{order_price}"
    if order_price is not None and price is not None and order_price != price:
        return f"{price}=>{order_price}"
    return str(price or order_price or "-")


def order_display_tp_sl(item: object) -> str:
    def _segment(label: str, trigger_price: object, order_price: object) -> str | None:
        if trigger_price is None:
            return None
        if order_price is None or order_price == trigger_price:
            return f"{label} {trigger_price}"
        return f"{label} {trigger_price}=>{order_price}"

    parts = [
        segment
        for segment in (
            _segment("TP", getattr(item, "take_profit_trigger_price", None), getattr(item, "take_profit_order_price", None)),
            _segment("SL", getattr(item, "stop_loss_trigger_price", None), getattr(item, "stop_loss_order_price", None)),
        )
        if segment
    ]
    if parts:
        return " / ".join(parts)
    return "-"


def order_display_ids(item: object) -> tuple[str, str]:
    order_id = str(getattr(item, "order_id", "") or getattr(item, "algo_id", "") or "-")
    client_order_id = str(
        getattr(item, "client_order_id", "") or getattr(item, "algo_client_order_id", "") or "-"
    )
    return order_id, client_order_id


class AccountDrawerLoadThread(QThread):
    completed = Signal(int, object)
    failed = Signal(int, str)

    def __init__(self, *, request_generation: int, runtime: object, client: OkxRestClient | None = None) -> None:
        super().__init__()
        self._request_generation = request_generation
        self._runtime = runtime
        self._client = client

    def run(self) -> None:
        try:
            positions_client = self._client or OkxRestClient()
            positions = positions_client.get_positions(
                self._runtime.credentials,
                environment=self._runtime.environment,
            )
        except Exception as exc:
            self.failed.emit(self._request_generation, str(exc))
            return
        upl_usdt_prices: dict[str, object] = {}
        position_instruments: dict[str, object] = {}
        position_tickers: dict[str, object] = {}
        try:
            upl_usdt_prices = _build_upl_usdt_price_map(positions_client, list(positions))
        except Exception:
            pass
        try:
            position_instruments = _build_position_instrument_map(positions_client, list(positions))
        except Exception:
            pass
        try:
            position_tickers = _build_position_ticker_map(positions_client, list(positions))
        except Exception:
            pass
        try:
            upl_usdt_prices = _augment_upl_usdt_prices_from_positions(
                upl_usdt_prices,
                list(positions),
                position_tickers,
            )
        except Exception:
            pass
        self.completed.emit(
            self._request_generation,
            AccountDrawerSnapshot(
                positions=tuple(positions),
                upl_usdt_prices=upl_usdt_prices,
                position_instruments=position_instruments,
                position_tickers=position_tickers,
            ),
        )


class AccountDrawerCancelThread(QThread):
    completed = Signal(object)
    failed = Signal(str)

    def __init__(self, *, runtime: object, order: object, client: OkxRestClient | None = None) -> None:
        super().__init__()
        self._runtime = runtime
        self._order = order
        self._client = client or OkxRestClient()

    def run(self) -> None:
        try:
            if order_source_kind(self._order) == "algo":
                result = self._client.cancel_algo_order(
                    self._runtime.credentials,
                    environment=self._runtime.environment,
                    inst_id=self._order.inst_id,
                    algo_id=getattr(self._order, "algo_id", None) or None,
                    algo_cl_ord_id=getattr(self._order, "algo_client_order_id", None) or None,
                )
            else:
                result = self._client.cancel_order_by_id(
                    self._runtime.credentials,
                    environment=self._runtime.environment,
                    inst_id=self._order.inst_id,
                    ord_id=getattr(self._order, "order_id", None) or None,
                    cl_ord_id=getattr(self._order, "client_order_id", None) or None,
                )
        except Exception as exc:
            self.failed.emit(str(exc))
            return
        self.completed.emit(result)


class KlineAccountDrawer(QWidget):
    collapseRequested = Signal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._runtime: object | None = None
        self._profile_name = ""
        self._environment = ""
        self._symbol = ""
        self._request_generation = 0
        self._refresh_pending = False
        self._active_table_refresh_pending = False
        self._active_table_refresh_timer = QTimer(self)
        self._active_table_refresh_timer.setSingleShot(True)
        self._active_table_refresh_timer.timeout.connect(self._refresh_active_table)
        self._refresh_data_timer = QTimer(self)
        self._refresh_data_timer.setSingleShot(True)
        self._refresh_data_timer.timeout.connect(self.refresh_data)
        self._snapshot = AccountDrawerSnapshot()
        self._visible_orders: list[object] = []
        self._load_thread: AccountDrawerLoadThread | None = None
        self._cancel_thread: AccountDrawerCancelThread | None = None
        self._cancel_in_flight = False
        self._shared_order_store = get_shared_order_store()
        self._shared_order_store.snapshot_changed.connect(self._apply_shared_order_snapshot)
        self._shared_order_store.refresh_failed.connect(self._apply_shared_order_refresh_error)
        self._realtime_store = get_shared_realtime_account_store()
        self._realtime_store.snapshot_ready.connect(self._apply_realtime_snapshot)
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        toolbar = QHBoxLayout()
        toolbar.setSpacing(8)

        self._scope_combo = QComboBox()
        self._scope_combo.addItem("当前交易对", "symbol")
        self._scope_combo.addItem("全部", "all")
        self._scope_combo.currentIndexChanged.connect(self._schedule_active_table_refresh)
        toolbar.addWidget(self._scope_combo, 0)

        self._refresh_button = QPushButton("刷新")
        self._refresh_button.clicked.connect(self.refresh_data)
        toolbar.addWidget(self._refresh_button, 0)

        self._collapse_button = QPushButton("收起")
        self._collapse_button.clicked.connect(self.collapseRequested.emit)
        toolbar.addWidget(self._collapse_button, 0)

        self._status_label = QLabel("未加载")
        self._status_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        toolbar.addWidget(self._status_label, 1)
        layout.addLayout(toolbar)

        self._tabs = QTabWidget()
        self._tabs.currentChanged.connect(self._schedule_active_table_refresh)
        layout.addWidget(self._tabs, 1)

        self._orders_table = self._create_table(
            [
                "合约",
                "来源",
                "方向",
                "类型",
                "价格/触发价",
                "数量",
                "已成交",
                "状态",
                "更新时间",
                "TP/SL",
                "订单ID",
                "clOrdId",
            ]
        )
        self._orders_table.itemSelectionChanged.connect(self._sync_cancel_button_state)
        orders_tab = QWidget()
        orders_layout = QVBoxLayout(orders_tab)
        orders_layout.setContentsMargins(0, 0, 0, 0)
        orders_layout.setSpacing(8)
        orders_layout.addWidget(self._orders_table, 1)

        orders_toolbar = QHBoxLayout()
        self._cancel_button = QPushButton("撤单")
        self._cancel_button.setEnabled(False)
        self._cancel_button.clicked.connect(self._cancel_selected_order)
        orders_toolbar.addStretch(1)
        orders_toolbar.addWidget(self._cancel_button, 0)
        orders_layout.addLayout(orders_toolbar)
        self._tabs.addTab(orders_tab, "当前委托")

        self._positions_table = self._create_table(
            ["合约 / 分组", *[heading for _column_id, heading, _width, _alignment in POSITION_COLUMNS]],
            column_defs=(("contract", "合约 / 分组", 220, Qt.AlignmentFlag.AlignLeft), *POSITION_COLUMNS),
        )
        positions_tab = QWidget()
        positions_layout = QVBoxLayout(positions_tab)
        positions_layout.setContentsMargins(0, 0, 0, 0)
        positions_layout.addWidget(self._positions_table, 1)
        self._tabs.addTab(positions_tab, "当前持仓")

        self._history_orders_table = self._create_table(
            [
                "合约",
                "来源",
                "方向",
                "类型",
                "价格/触发价",
                "数量",
                "已成交",
                "状态",
                "更新时间",
                "TP/SL",
                "订单ID",
                "clOrdId",
            ]
        )
        history_orders_tab = QWidget()
        history_orders_layout = QVBoxLayout(history_orders_tab)
        history_orders_layout.setContentsMargins(0, 0, 0, 0)
        history_orders_layout.addWidget(self._history_orders_table, 1)
        self._tabs.addTab(history_orders_tab, "历史委托")

    def _create_table(
        self,
        headers: list[str],
        *,
        column_defs: tuple[tuple[str, str, int, Qt.AlignmentFlag], ...] | None = None,
    ) -> QTableWidget:
        table = QTableWidget(0, len(headers), self)
        table.setHorizontalHeaderLabels(headers)
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        table.verticalHeader().setVisible(False)
        header = table.horizontalHeader()
        header.setStretchLastSection(False)
        if column_defs is None:
            header.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
            header.setStretchLastSection(True)
        else:
            for index, (_column_id, _heading, width, _alignment) in enumerate(column_defs):
                header.setSectionResizeMode(index, QHeaderView.ResizeMode.Interactive)
                table.setColumnWidth(index, width)
        return table

    def show_tab(self, tab_name: str) -> None:
        self._tabs.setCurrentIndex(0 if tab_name == "orders" else 1)

    def set_context(
        self,
        *,
        runtime: object | None,
        profile_name: str,
        environment: str,
        symbol: str,
        refresh_if_visible: bool = True,
    ) -> None:
        normalized_symbol = symbol.strip().upper()
        changed = (
            runtime is not self._runtime
            or profile_name != self._profile_name
            or environment != self._environment
            or normalized_symbol != self._symbol
        )
        self._runtime = runtime
        self._profile_name = profile_name
        self._environment = environment
        self._symbol = normalized_symbol
        if changed:
            self._load_shared_order_snapshot()
            self._refresh_tables()
        if changed and refresh_if_visible and self.isVisible():
            self.refresh_data()

    def refresh_data(self) -> None:
        if self._runtime is None:
            self._status_label.setText("未连接账户")
            return
        self._shared_order_store.request_refresh(runtime=self._runtime, profile_name=self._profile_name)
        self._request_generation += 1
        if self._load_thread is not None and self._load_thread.isRunning():
            self._refresh_pending = True
            self._status_label.setText("加载中...")
            return
        self._start_load(self._request_generation)

    def shutdown(self, wait_ms: int = 1500) -> bool:
        if self._cancel_thread is not None and self._cancel_thread.isRunning():
            return self._cancel_thread.wait(wait_ms)
        if self._load_thread is not None and self._load_thread.isRunning():
            return self._load_thread.wait(wait_ms)
        return True

    def _clear_load_thread(self) -> None:
        thread = self._load_thread
        self._load_thread = None
        if thread is not None:
            thread.deleteLater()
        if self._refresh_pending and self._runtime is not None:
            self._refresh_pending = False
            self._start_load(self._request_generation)

    def _apply_snapshot(self, generation: int, snapshot: AccountDrawerSnapshot) -> None:
        if generation != self._request_generation:
            return
        self._snapshot = snapshot
        self._status_label.setText(f"委托 {len(snapshot.orders)} | 持仓 {len(snapshot.positions)}")
        self._refresh_tables()

    def _apply_load_error(self, generation: int, message: str) -> None:
        if generation != self._request_generation:
            return
        self._status_label.setText(f"加载失败: {message}")

    def _refresh_tables(self) -> None:
        scope = str(self._scope_combo.currentData() or "symbol")
        filtered_orders = filter_account_items(self._snapshot.orders, scope=scope, symbol=self._symbol)
        filtered_positions = filter_account_items(self._snapshot.positions, scope=scope, symbol=self._symbol)
        filtered_order_history = filter_account_items(self._snapshot.order_history, scope=scope, symbol=self._symbol)
        self._populate_orders_table(filtered_orders)
        self._populate_positions_table(filtered_positions)
        self._populate_history_orders_table(filtered_order_history)
        self._sync_cancel_button_state()

    def _schedule_active_table_refresh(self, *_args: object) -> None:
        """Coalesce visible-tab table work so showing the drawer can paint first."""
        if not self.isVisible() or self._active_table_refresh_pending:
            return
        self._active_table_refresh_pending = True
        self._active_table_refresh_timer.start(0)

    def schedule_refresh_data(self) -> None:
        """Queue a refresh with drawer-owned lifetime protection."""
        if self._runtime is None:
            return
        self._refresh_data_timer.start(0)

    def _refresh_active_table(self) -> None:
        self._active_table_refresh_pending = False
        if not self.isVisible():
            return
        scope = str(self._scope_combo.currentData() or "symbol")
        current_tab = self._tabs.currentIndex()
        if current_tab == 0:
            self._populate_orders_table(
                filter_account_items(self._snapshot.orders, scope=scope, symbol=self._symbol)
            )
            self._sync_cancel_button_state()
            return
        if current_tab == 1:
            self._populate_positions_table(
                filter_account_items(self._snapshot.positions, scope=scope, symbol=self._symbol)
            )
            self._cancel_button.setEnabled(False)
            return
        self._populate_history_orders_table(
            filter_account_items(self._snapshot.order_history, scope=scope, symbol=self._symbol)
        )
        self._cancel_button.setEnabled(False)

    def _populate_orders_table(self, orders: list[object]) -> None:
        self._visible_orders = list(orders)
        self._orders_table.setRowCount(len(orders))
        for row, order in enumerate(orders):
            values = [
                getattr(order, "inst_id", ""),
                getattr(order, "source_label", "") or order_source_kind(order),
                order_display_direction(order),
                getattr(order, "ord_type", ""),
                order_display_price(order),
                getattr(order, "size", ""),
                getattr(order, "filled_size", ""),
                getattr(order, "state", ""),
                getattr(order, "update_time", None) or getattr(order, "created_time", None) or "",
                order_display_tp_sl(order),
                order_display_ids(order)[0],
                order_display_ids(order)[1],
            ]
            for column, value in enumerate(values):
                item = QTableWidgetItem(self._format_value(value))
                self._orders_table.setItem(row, column, item)

    def _populate_positions_table(self, positions: list[object]) -> None:
        self._positions_table.setRowCount(len(positions))
        upl_usdt_prices = dict(self._snapshot.upl_usdt_prices or {})
        position_instruments = dict(self._snapshot.position_instruments or {})
        position_tickers = dict(self._snapshot.position_tickers or {})
        for row, position in enumerate(positions):
            values = _position_table_values(
                position,
                upl_usdt_prices=upl_usdt_prices,
                position_instruments=position_instruments,
                position_tickers=position_tickers,
            )
            for column, value in enumerate(values):
                item = QTableWidgetItem(self._format_value(value))
                self._positions_table.setItem(row, column, item)
            pnl = getattr(position, "unrealized_pnl", None)
            if pnl is not None:
                try:
                    pnl_color = QColor("#13803d" if pnl > 0 else "#c23b3b" if pnl < 0 else "#1f2937")
                    for column_id in ("upl", "upl_usdt", "market_value"):
                        column = 1 + _POSITION_COLUMN_INDEX[column_id]
                        self._positions_table.item(row, column).setForeground(pnl_color)
                except (TypeError, ValueError):
                    pass

    def _populate_history_orders_table(self, orders: list[object]) -> None:
        self._history_orders_table.setRowCount(len(orders))
        for row, order in enumerate(orders):
            values = [
                getattr(order, "inst_id", ""),
                getattr(order, "source_label", "") or order_source_kind(order),
                order_display_direction(order),
                getattr(order, "ord_type", ""),
                order_display_price(order),
                getattr(order, "size", ""),
                getattr(order, "filled_size", ""),
                getattr(order, "state", ""),
                getattr(order, "update_time", None) or getattr(order, "created_time", None) or "",
                order_display_tp_sl(order),
                order_display_ids(order)[0],
                order_display_ids(order)[1],
            ]
            for column, value in enumerate(values):
                item = QTableWidgetItem(self._format_value(value))
                self._history_orders_table.setItem(row, column, item)

    @staticmethod
    def _format_value(value: object) -> str:
        if value is None:
            return ""
        return str(value)

    def _start_load(self, generation: int) -> None:
        self._status_label.setText("加载中...")
        self._load_thread = AccountDrawerLoadThread(
            request_generation=generation,
            runtime=self._runtime,
        )
        self._load_thread.completed.connect(self._apply_snapshot)
        self._load_thread.failed.connect(self._apply_load_error)
        self._load_thread.finished.connect(self._clear_load_thread)
        self._load_thread.start()

    def _selected_order(self) -> object | None:
        row = self._orders_table.currentRow()
        if row < 0 or row >= len(self._visible_orders):
            return None
        return self._visible_orders[row]

    def _sync_cancel_button_state(self) -> None:
        enabled = (
            not self._cancel_in_flight
            and self._runtime is not None
            and self._selected_order() is not None
            and bool(order_cancel_reference(self._selected_order()))
        )
        self._cancel_button.setEnabled(enabled)

    def _cancel_selected_order(self) -> None:
        if self._runtime is None or self._cancel_in_flight:
            return
        order = self._selected_order()
        if order is None:
            return
        reference = order_cancel_reference(order)
        if not reference:
            self._status_label.setText("缺少可撤销订单标识")
            return
        answer = QMessageBox.question(
            self,
            "确认撤单",
            (
                f"确认撤销 {getattr(order, 'inst_id', '')} "
                f"{getattr(order, 'side', '') or getattr(order, 'pos_side', '')} "
                f"{getattr(order, 'ord_type', '')} {reference} ?"
            ).strip(),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if answer != QMessageBox.StandardButton.Yes:
            return
        self._cancel_in_flight = True
        self._cancel_button.setEnabled(False)
        self._status_label.setText("撤单中...")
        self._cancel_thread = AccountDrawerCancelThread(runtime=self._runtime, order=order)
        self._cancel_thread.completed.connect(self._handle_cancel_completed)
        self._cancel_thread.failed.connect(self._handle_cancel_failed)
        self._cancel_thread.finished.connect(self._clear_cancel_thread)
        self._cancel_thread.start()

    def _handle_cancel_completed(self, _result: object) -> None:
        self._cancel_in_flight = False
        self._status_label.setText("撤单成功")
        self._sync_cancel_button_state()
        self.refresh_data()

    def _handle_cancel_failed(self, message: str) -> None:
        self._cancel_in_flight = False
        self._status_label.setText(f"撤单失败: {message}")
        self._sync_cancel_button_state()

    def _clear_cancel_thread(self) -> None:
        thread = self._cancel_thread
        self._cancel_thread = None
        if thread is not None:
            thread.deleteLater()


def _shared_set_context(
    self: KlineAccountDrawer,
    *,
    runtime: object | None,
    profile_name: str,
    environment: str,
    symbol: str,
    refresh_if_visible: bool = True,
) -> None:
    normalized_symbol = symbol.strip().upper()
    changed = (
        runtime is not self._runtime
        or profile_name != self._profile_name
        or environment != self._environment
        or normalized_symbol != self._symbol
    )
    self._runtime = runtime
    self._profile_name = profile_name
    self._environment = environment
    self._symbol = normalized_symbol
    if changed:
        self._load_shared_order_snapshot()
        if runtime is not None:
            self._realtime_store.start_if_needed(runtime)
        realtime_snapshot = self._realtime_store.snapshot_for(
            profile_name=self._profile_name,
            environment=self._environment,
        )
        if realtime_snapshot is not None:
            self._apply_realtime_snapshot(realtime_snapshot)
        # The hidden drawer only needs the data snapshot.  Building all three
        # wide tables here delays the next click on the holdings button.
        self._schedule_active_table_refresh()
    if changed and refresh_if_visible and self.isVisible():
        self.refresh_data()


def _shared_refresh_data(self: KlineAccountDrawer) -> None:
    if self._runtime is None:
        self._status_label.setText("未连接账户")
        return
    # The visible tab is filled from the cached snapshot on the next event
    # loop pass, so the drawer itself can become visible without waiting for
    # any table construction or network reconciliation.
    self._schedule_active_table_refresh()
    self._shared_order_store.request_refresh(runtime=self._runtime, profile_name=self._profile_name)
    self._realtime_store.start_if_needed(self._runtime)
    self._realtime_store.request_reconcile("drawer")
    self._status_label.setText("同步持仓中...")


def _shared_apply_snapshot(self: KlineAccountDrawer, generation: int, snapshot: AccountDrawerSnapshot) -> None:
    if generation != self._request_generation:
        return
    self._snapshot = replace(
        self._snapshot,
        positions=tuple(snapshot.positions),
        upl_usdt_prices=dict(snapshot.upl_usdt_prices or {}),
        position_instruments=dict(snapshot.position_instruments or {}),
        position_tickers=dict(snapshot.position_tickers or {}),
    )
    self._status_label.setText(f"委托 {len(self._snapshot.orders)} | 持仓 {len(self._snapshot.positions)}")
    self._schedule_active_table_refresh()


def _shared_load_shared_order_snapshot(self: KlineAccountDrawer) -> None:
    snapshot = self._shared_order_store.snapshot_for(
        profile_name=self._profile_name,
        environment=self._environment,
    )
    self._snapshot = replace(
        self._snapshot,
        orders=tuple(snapshot.current_order_items),
        order_history=tuple(snapshot.history_orders),
    )


def _shared_apply_shared_order_snapshot(
    self: KlineAccountDrawer,
    profile_name: str,
    environment: str,
    snapshot: object,
) -> None:
    if profile_name != self._profile_name or environment != self._environment:
        return
    if not isinstance(snapshot, SharedOrderSnapshot):
        return
    self._snapshot = replace(
        self._snapshot,
        orders=tuple(snapshot.current_order_items),
        order_history=tuple(snapshot.history_orders),
    )
    self._status_label.setText(f"委托 {len(self._snapshot.orders)} | 持仓 {len(self._snapshot.positions)}")
    self._schedule_active_table_refresh()


def _shared_apply_realtime_snapshot(self: KlineAccountDrawer, snapshot: object) -> None:
    if not isinstance(snapshot, AccountRealtimeSnapshot):
        return
    if snapshot.profile_name != self._profile_name or snapshot.environment != self._environment:
        return
    self._snapshot = replace(
        self._snapshot,
        positions=tuple(snapshot.positions),
        position_instruments=dict(snapshot.position_instruments),
        position_tickers=dict(snapshot.position_tickers),
        upl_usdt_prices=dict(snapshot.upl_usdt_prices),
    )
    self._status_label.setText(f"委托 {len(self._snapshot.orders)} | 持仓 {len(self._snapshot.positions)}")
    # Avoid constructing any wide tables while the drawer is collapsed; when
    # visible, coalesce updates and repaint the selected tab only.
    self._schedule_active_table_refresh()


def _shared_apply_shared_order_refresh_error(
    self: KlineAccountDrawer,
    profile_name: str,
    environment: str,
    message: str,
) -> None:
    if profile_name != self._profile_name or environment != self._environment:
        return
    self._status_label.setText(f"委托刷新失败: {message}")


KlineAccountDrawer.set_context = _shared_set_context
KlineAccountDrawer.refresh_data = _shared_refresh_data
KlineAccountDrawer._apply_snapshot = _shared_apply_snapshot
KlineAccountDrawer._load_shared_order_snapshot = _shared_load_shared_order_snapshot
KlineAccountDrawer._apply_shared_order_snapshot = _shared_apply_shared_order_snapshot
KlineAccountDrawer._apply_shared_order_refresh_error = _shared_apply_shared_order_refresh_error
KlineAccountDrawer._apply_realtime_snapshot = _shared_apply_realtime_snapshot
