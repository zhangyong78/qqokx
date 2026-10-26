from __future__ import annotations

from time import perf_counter
import threading

from dataclasses import dataclass, replace
from decimal import Decimal
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
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from okx_quant.okx_client import OkxFillHistoryItem, OkxPosition, OkxPositionHistoryItem, OkxTradeOrderItem
from okx_quant.okx_client import OkxRestClient
from okx_quant.pricing import format_decimal
from okx_quant.pending_order_chase import chase_unavailable_reason, get_pending_order_chase_service, order_reference
from roll_terminal_qt.account_positions_home import (
    POSITION_COLUMNS,
    _break_even_taker_fee_rate,
    _format_fill_history_exec_type,
    _format_fill_history_fee_cell,
    _format_fill_history_pnl,
    _format_fill_history_price,
    _format_fill_history_size_with_contracts,
    _format_history_side,
    _format_position_history_fee_cell,
    _format_position_history_pnl,
    _format_position_history_price,
    _position_break_even_price,
    _position_history_holding_time_text,
    _position_history_open_time_text,
    _position_history_raw_size_text,
    _position_history_status_text,
)
from roll_terminal_qt.history_service import FillHistoryFeedThread, PositionHistoryFeedThread
from okx_quant.ui_shell import (
    _augment_upl_usdt_prices_from_positions,
    _build_position_instrument_map,
    _build_position_ticker_map,
    _build_upl_usdt_price_map,
    _format_margin_mode,
    _format_mark_price,
    _format_option_trade_side_display,
    _format_okx_ms_timestamp,
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
    _format_position_contracts,
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
from okx_quant.log_utils import append_log_line
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
        _format_position_contracts(position),
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


def order_display_coin_size(item: object, instruments: dict[str, object] | None = None) -> str:
    """Show the raw order size as an approximate base-coin amount.

    OKX derivative orders use ``sz`` in contracts (张), while spot orders use
    the base-coin quantity directly.  The instrument metadata is therefore
    required for a reliable conversion; when it is unavailable we show ``-``
    rather than guessing a contract value.
    """
    size = getattr(item, "size", None)
    try:
        quantity = Decimal(str(size))
    except (TypeError, ValueError, ArithmeticError):
        return "-"
    if quantity < 0:
        quantity = abs(quantity)
    inst_id = str(getattr(item, "inst_id", "") or "").strip().upper()
    inst_type = str(getattr(item, "inst_type", "") or "").strip().upper()
    if not inst_id or quantity == 0:
        return "-"
    instrument = dict(instruments or {}).get(inst_id)
    base_currency = inst_id.split("-", 1)[0] or "-"
    if inst_type in {"SPOT", "MARGIN"}:
        return f"{format_decimal(quantity)} {base_currency}"
    if instrument is None:
        return "-"
    try:
        contract_value = Decimal(str(getattr(instrument, "ct_val", None)))
        multiplier_raw = getattr(instrument, "ct_mult", None)
        multiplier = Decimal(str(multiplier_raw)) if multiplier_raw not in (None, "") else Decimal("1")
    except (TypeError, ValueError, ArithmeticError):
        return "-"
    if contract_value <= 0 or multiplier <= 0:
        return "-"
    notional = quantity * contract_value * multiplier
    value_currency = str(getattr(instrument, "ct_val_ccy", "") or "").strip().upper()
    if value_currency in {"USD", "USDT", "USDC"}:
        reference_price = None
        for field_name in ("actual_price", "order_price", "price", "trigger_price"):
            raw_price = getattr(item, field_name, None)
            try:
                candidate = Decimal(str(raw_price))
            except (TypeError, ValueError, ArithmeticError):
                continue
            if candidate > 0:
                reference_price = candidate
                break
        if reference_price is None:
            return "-"
        notional /= reference_price
        value_currency = base_currency
    if not value_currency:
        value_currency = base_currency
    return f"{format_decimal(notional)} {value_currency}"


def _order_table_values(order: object, instruments: dict[str, object] | None = None) -> list[object]:
    return [
        getattr(order, "inst_id", ""),
        getattr(order, "source_label", "") or order_source_kind(order),
        order_display_direction(order),
        getattr(order, "ord_type", ""),
        order_display_price(order),
        order_display_coin_size(order, instruments),
        getattr(order, "size", ""),
        getattr(order, "filled_size", ""),
        "待交易所确认" if getattr(order, "state", "") == "pending_confirmation" else getattr(order, "state", ""),
        _format_okx_ms_timestamp(getattr(order, "update_time", None) or getattr(order, "created_time", None)),
        order_display_tp_sl(order),
        order_display_ids(order)[0],
        order_display_ids(order)[1],
    ]


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


class AccountDrawerChaseThread(QThread):
    updated = Signal(object)
    failed = Signal(str)

    def __init__(self, *, runtime: object, order: object, context: dict[str, object], interval: int,
                 service=None, client_factory=OkxRestClient) -> None:
        super().__init__()
        self.runtime, self.order, self.context = runtime, order, context
        self.order_id = order_reference(order)
        self.profile_name = str(getattr(runtime, "credential_profile_name", "") or "")
        self.environment = str(runtime.environment)
        self.service = service or get_pending_order_chase_service()
        self.client_factory = client_factory
        self.interval = max(1, interval)
        self.key = ""
        self.stopping = threading.Event()

    def stop(self) -> None:
        self.stopping.set()
        if self.key:
            try:
                self.service.stop(self.key)
            except Exception as exc:
                self.failed.emit(str(exc))

    def run(self) -> None:
        client = None
        try:
            client = self.client_factory()
            if self.stopping.is_set():
                return
            self.key = self.service.start(client=client, runtime=self.runtime, order=self.order, context=self.context)
            self.updated.emit(self.service.snapshot(self.key))
            while not self.stopping.is_set():
                task = self.service.step(client=client, runtime=self.runtime, key=self.key)
                self.updated.emit(task)
                if not task.get("active") or self.stopping.wait(self.interval):
                    break
        except Exception as exc:
            if self.key:
                try:
                    self.service.stop(self.key, f"自动追单已暂停：{exc}")
                except Exception:
                    pass
            self.failed.emit(str(exc))
        finally:
            if self.key:
                try:
                    task = self.service.snapshot(self.key)
                    if task.get("active"):
                        self.service.stop(self.key)
                    self.updated.emit(self.service.snapshot(self.key))
                except Exception:
                    pass
            if client is not None:
                client.close()


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
        self._fill_history_items: tuple[OkxFillHistoryItem, ...] = ()
        self._fill_history_instruments: dict[str, object] = {}
        self._fill_history_usdt_prices: dict[str, Decimal] = {}
        self._position_history_items: tuple[OkxPositionHistoryItem, ...] = ()
        self._position_history_instruments: dict[str, object] = {}
        self._position_history_usdt_prices: dict[str, Decimal] = {}
        self._history_generation = 0
        self._fill_history_loaded = False
        self._position_history_loaded = False
        self._fill_history_thread: FillHistoryFeedThread | None = None
        self._position_history_thread: PositionHistoryFeedThread | None = None
        self._retired_history_threads: list[QThread] = []
        self._visible_orders: list[object] = []
        self._rendered_submission_ids: set[str] = set()
        self._load_thread: AccountDrawerLoadThread | None = None
        self._cancel_thread: AccountDrawerCancelThread | None = None
        self._cancel_in_flight = False
        self._chase_thread: AccountDrawerChaseThread | None = None
        self._chase_shutting_down = False
        self.chase_context_provider = None
        # Current positions are the urgent source of truth.  Defer the shared
        # current-order refresh until the REST position check has completed.
        self._orders_refresh_waiting_for_realtime = False
        self._orders_refresh_started_after_realtime = False
        self._orders_refresh_deferred_timer = QTimer(self)
        self._orders_refresh_deferred_timer.setSingleShot(True)
        self._orders_refresh_deferred_timer.timeout.connect(self._start_deferred_order_refresh)
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
                "币数量",
                "委托张数",
                "成交张数",
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
        self._chase_button = QPushButton("自动追单")
        self._chase_button.setEnabled(False)
        self._chase_button.clicked.connect(self._start_selected_order_chase)
        orders_toolbar.addWidget(self._chase_button)
        self._stop_chase_button = QPushButton("停止追单")
        self._stop_chase_button.setEnabled(False)
        self._stop_chase_button.clicked.connect(self._stop_order_chase)
        orders_toolbar.addWidget(self._stop_chase_button)
        orders_toolbar.addWidget(QLabel("间隔（秒）"))
        self._chase_interval = QSpinBox()
        self._chase_interval.setRange(1, 60)
        self._chase_interval.setValue(2)
        orders_toolbar.addWidget(self._chase_interval)
        self._cancel_button = QPushButton("撤单")
        self._cancel_button.setEnabled(False)
        self._cancel_button.clicked.connect(self._cancel_selected_order)
        orders_toolbar.addStretch(1)
        orders_toolbar.addWidget(self._cancel_button, 0)
        orders_layout.addLayout(orders_toolbar)
        self._chase_status_label = QLabel("选中普通限价委托后可追单：买单跟买一、卖单跟卖一；开仓按止损和原风险预算调数量，平仓只改价格。")
        self._chase_status_label.setWordWrap(True)
        orders_layout.addWidget(self._chase_status_label)
        self._tabs.addTab(orders_tab, "当前委托")
        self._tabs.currentChanged.connect(self._sync_chase_button_state)

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
                "币数量",
                "委托张数",
                "成交张数",
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

        self._history_fills_table = self._create_table(
            ["成交时间", "类型", "合约", "方向", "成交价", "成交数量（币 / 张）", "手续费", "已实现盈亏", "成交类别"]
        )
        history_fills_tab = QWidget()
        history_fills_layout = QVBoxLayout(history_fills_tab)
        history_fills_layout.setContentsMargins(0, 0, 0, 0)
        history_fills_layout.addWidget(self._history_fills_table, 1)
        self._tabs.addTab(history_fills_tab, "历史成交")

        self._history_positions_table = self._create_table(
            [
                "更新时间", "开仓时间", "持仓时长", "合约", "方向", "开仓均价", "平仓均价",
                "开仓数量（币 / 张）", "平仓数量（币 / 张）", "手续费", "已实现盈亏", "状态",
            ]
        )
        history_positions_tab = QWidget()
        history_positions_layout = QVBoxLayout(history_positions_tab)
        history_positions_layout.setContentsMargins(0, 0, 0, 0)
        history_positions_layout.addWidget(self._history_positions_table, 1)
        self._tabs.addTab(history_positions_tab, "历史仓位")

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
        account_changed = (
            runtime is not self._runtime
            or profile_name != self._profile_name
            or environment != self._environment
        )
        changed = (
            account_changed
            or normalized_symbol != self._symbol
        )
        if account_changed:
            self._reset_history_context()
        self._runtime = runtime
        self._profile_name = profile_name
        self._environment = environment
        self._symbol = normalized_symbol
        if changed:
            self._load_shared_order_snapshot()
            self._refresh_tables()
        if changed and refresh_if_visible and self.isVisible():
            self.refresh_data(refresh_history=account_changed)

    def _reset_history_context(self) -> None:
        self._history_generation += 1
        self._fill_history_loaded = False
        self._position_history_loaded = False
        self._fill_history_items = ()
        self._fill_history_instruments.clear()
        self._fill_history_usdt_prices.clear()
        self._position_history_items = ()
        self._position_history_instruments.clear()
        self._position_history_usdt_prices.clear()
        for attr_name in ("_fill_history_thread", "_position_history_thread"):
            thread = getattr(self, attr_name)
            if thread is None:
                continue
            if thread.isRunning():
                self._retired_history_threads.append(thread)
                thread.finished.connect(lambda finished=thread: self._forget_retired_history_thread(finished))
                thread.stop()
            else:
                thread.deleteLater()
            setattr(self, attr_name, None)

    def _forget_retired_history_thread(self, thread: QThread) -> None:
        if thread in self._retired_history_threads:
            self._retired_history_threads.remove(thread)
        thread.deleteLater()

    def refresh_data(self, _checked: bool = False, *, refresh_history: bool = True) -> None:
        if self._runtime is None:
            self._status_label.setText("未连接账户")
            return
        self._shared_order_store.request_refresh(runtime=self._runtime, profile_name=self._profile_name)
        if refresh_history and self._tabs.currentIndex() == 3:
            self._ensure_fill_history_loaded(force=True)
        elif refresh_history and self._tabs.currentIndex() == 4:
            self._ensure_position_history_loaded(force=True)
        self._request_generation += 1
        if self._load_thread is not None and self._load_thread.isRunning():
            self._refresh_pending = True
            self._status_label.setText("加载中...")
            return
        self._start_load(self._request_generation)

    def shutdown(self, wait_ms: int = 1500) -> bool:
        self._chase_shutting_down = True
        if self._chase_thread is not None and self._chase_thread.isRunning():
            self._chase_thread.stop()
            if not self._chase_thread.wait(wait_ms):
                return False
        history_threads = [
            thread
            for thread in (self._fill_history_thread, self._position_history_thread, *self._retired_history_threads)
            if thread is not None and thread.isRunning()
        ]
        for thread in history_threads:
            if hasattr(thread, "stop"):
                thread.stop()
        if self._cancel_thread is not None and self._cancel_thread.isRunning():
            return self._cancel_thread.wait(wait_ms)
        if self._load_thread is not None and self._load_thread.isRunning():
            if not self._load_thread.wait(wait_ms):
                return False
        return all(thread.wait(wait_ms) for thread in history_threads)

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
        self._cancel_button.setEnabled(False)
        if current_tab == 2:
            self._populate_history_orders_table(
                filter_account_items(self._snapshot.order_history, scope=scope, symbol=self._symbol)
            )
        elif current_tab == 3:
            self._populate_fill_history_table(
                filter_account_items(self._fill_history_items, scope=scope, symbol=self._symbol)
            )
            self._ensure_fill_history_loaded()
        elif current_tab == 4:
            self._populate_position_history_table(
                filter_account_items(self._position_history_items, scope=scope, symbol=self._symbol)
            )
            self._ensure_position_history_loaded()

    def _populate_orders_table(self, orders: list[object]) -> None:
        previous = self._selected_order()
        previous_id = order_reference(previous) if previous is not None else ""
        self._visible_orders = list(orders)
        self._orders_table.setRowCount(len(orders))
        instruments = dict(self._snapshot.position_instruments or {})
        for row, order in enumerate(orders):
            values = _order_table_values(order, instruments)
            for column, value in enumerate(values):
                item = QTableWidgetItem(self._format_value(value))
                if self._chase_thread is not None and self._chase_thread.isRunning() and order_reference(order) == self._chase_thread.order_id:
                    item.setBackground(QColor("#fff4cc"))
                self._orders_table.setItem(row, column, item)
            if previous_id and order_reference(order) == previous_id:
                self._orders_table.selectRow(row)
        pending_ids: set[str] = set()
        for order in orders:
            raw = getattr(order, "raw", {})
            received_at = raw.get("_ack_received_at") if isinstance(raw, dict) else None
            order_id = str(getattr(order, "order_id", "") or "")
            if received_at is not None and order_id:
                pending_ids.add(order_id)
                if order_id not in self._rendered_submission_ids:
                    append_log_line(f"[rr_latency] table_ready profile={self._profile_name} environment={self._environment} order={order_id} ack_to_table_ms={(perf_counter() - received_at) * 1000:.1f}")
        self._rendered_submission_ids = pending_ids

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
        instruments = dict(self._snapshot.position_instruments or {})
        for row, order in enumerate(orders):
            values = _order_table_values(order, instruments)
            for column, value in enumerate(values):
                item = QTableWidgetItem(self._format_value(value))
                self._history_orders_table.setItem(row, column, item)

    def _populate_fill_history_table(self, fills: list[object]) -> None:
        table = self._history_fills_table
        table.setRowCount(len(fills))
        for row, fill in enumerate(fills):
            values = (
                _format_okx_ms_timestamp(getattr(fill, "fill_time", None)),
                getattr(fill, "inst_type", "-") or "-",
                getattr(fill, "inst_id", "-") or "-",
                _format_history_side(getattr(fill, "side", None), getattr(fill, "pos_side", None)),
                _format_fill_history_price(fill),
                _format_fill_history_size_with_contracts(fill, self._fill_history_instruments),
                _format_fill_history_fee_cell(fill, self._fill_history_usdt_prices),
                _format_fill_history_pnl(fill, self._fill_history_usdt_prices),
                _format_fill_history_exec_type(getattr(fill, "exec_type", None)),
            )
            for column, value in enumerate(values):
                item = QTableWidgetItem(self._format_value(value))
                if column not in {2, 5}:
                    item.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter))
                table.setItem(row, column, item)

    def _populate_position_history_table(self, positions: list[object]) -> None:
        table = self._history_positions_table
        table.setRowCount(len(positions))
        for row, position in enumerate(positions):
            raw_direction = str(
                getattr(position, "direction", None)
                or (getattr(position, "pos_side", None) if getattr(position, "pos_side", None) != "net" else "")
                or ""
            ).strip().lower()
            direction_text = {"long": "多头", "short": "空头", "buy": "多头", "sell": "空头"}.get(
                raw_direction, raw_direction or "-"
            )
            values = (
                _format_okx_ms_timestamp(getattr(position, "update_time", None)),
                _position_history_open_time_text(position),
                _position_history_holding_time_text(position),
                getattr(position, "inst_id", "-") or "-",
                direction_text,
                _format_position_history_price(
                    getattr(position, "open_avg_price", None),
                    getattr(position, "inst_id", ""),
                    getattr(position, "inst_type", ""),
                    instrument=self._position_history_instruments.get(getattr(position, "inst_id", "")),
                    usdt_prices=self._position_history_usdt_prices,
                ),
                _format_position_history_price(
                    getattr(position, "close_avg_price", None),
                    getattr(position, "inst_id", ""),
                    getattr(position, "inst_type", ""),
                    instrument=self._position_history_instruments.get(getattr(position, "inst_id", "")),
                    usdt_prices=self._position_history_usdt_prices,
                ),
                _position_history_raw_size_text(
                    position,
                    self._position_history_instruments,
                    "openMaxPos",
                    "maxPos",
                    "openPos",
                ),
                _position_history_raw_size_text(
                    position,
                    self._position_history_instruments,
                    "closeTotalPos",
                    "closePos",
                    "closeSz",
                ),
                _format_position_history_fee_cell(position, self._position_history_usdt_prices),
                _format_position_history_pnl(
                    getattr(position, "realized_pnl", None),
                    position,
                    with_sign=True,
                    usdt_prices=self._position_history_usdt_prices,
                ),
                _position_history_status_text(position),
            )
            for column, value in enumerate(values):
                item = QTableWidgetItem(self._format_value(value))
                if column not in {3, 7, 8}:
                    item.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter))
                table.setItem(row, column, item)

    def _ensure_fill_history_loaded(self, *, force: bool = False) -> None:
        if self._runtime is None or (self._fill_history_loaded and not force):
            return
        if self._fill_history_thread is not None and self._fill_history_thread.isRunning():
            if not force:
                return
            old_thread = self._fill_history_thread
            self._retired_history_threads.append(old_thread)
            old_thread.finished.connect(lambda finished=old_thread: self._forget_retired_history_thread(finished))
            old_thread.stop()
        generation = self._history_generation
        thread = FillHistoryFeedThread(self._runtime, limit=100)
        self._fill_history_thread = thread
        thread.data_ready.connect(lambda payload, token=generation: self._apply_fill_history_payload(token, payload))
        thread.status_changed.connect(lambda message, token=generation: self._apply_history_status(token, message))
        thread.finished.connect(lambda finished=thread: self._clear_fill_history_thread(finished))
        self._fill_history_loaded = False
        thread.start()

    def _ensure_position_history_loaded(self, *, force: bool = False) -> None:
        if self._runtime is None or (self._position_history_loaded and not force):
            return
        if self._position_history_thread is not None and self._position_history_thread.isRunning():
            if not force:
                return
            old_thread = self._position_history_thread
            self._retired_history_threads.append(old_thread)
            old_thread.finished.connect(lambda finished=old_thread: self._forget_retired_history_thread(finished))
            old_thread.stop()
        generation = self._history_generation
        thread = PositionHistoryFeedThread(self._runtime, limit=1000)
        self._position_history_thread = thread
        thread.data_ready.connect(
            lambda payload, token=generation: self._apply_position_history_payload(token, payload)
        )
        thread.status_changed.connect(lambda message, token=generation: self._apply_history_status(token, message))
        thread.finished.connect(lambda finished=thread: self._clear_position_history_thread(finished))
        self._position_history_loaded = False
        thread.start()

    def _apply_history_status(self, generation: int, message: str) -> None:
        if generation == self._history_generation:
            self._status_label.setText(message)

    def _apply_fill_history_payload(self, generation: int, payload: object) -> None:
        if generation != self._history_generation or not isinstance(payload, dict):
            return
        self._fill_history_items = tuple(payload.get("items", ()))
        self._fill_history_instruments = dict(payload.get("instruments", {}) or {})
        self._fill_history_usdt_prices = dict(payload.get("usdt_prices", {}) or {})
        self._fill_history_loaded = True
        if self._tabs.currentIndex() == 3:
            scope = str(self._scope_combo.currentData() or "symbol")
            self._populate_fill_history_table(
                filter_account_items(self._fill_history_items, scope=scope, symbol=self._symbol)
            )

    def _apply_position_history_payload(self, generation: int, payload: object) -> None:
        if generation != self._history_generation or not isinstance(payload, dict):
            return
        self._position_history_items = tuple(payload.get("items", ()))
        self._position_history_instruments = dict(payload.get("instruments", {}) or {})
        self._position_history_usdt_prices = dict(payload.get("usdt_prices", {}) or {})
        self._position_history_loaded = True
        if self._tabs.currentIndex() == 4:
            scope = str(self._scope_combo.currentData() or "symbol")
            self._populate_position_history_table(
                filter_account_items(self._position_history_items, scope=scope, symbol=self._symbol)
            )

    def _clear_fill_history_thread(self, thread: FillHistoryFeedThread) -> None:
        if self._fill_history_thread is thread:
            self._fill_history_thread = None
        thread.deleteLater()

    def _clear_position_history_thread(self, thread: PositionHistoryFeedThread) -> None:
        if self._position_history_thread is thread:
            self._position_history_thread = None
        thread.deleteLater()

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
        self._sync_chase_button_state()

    def _sync_chase_button_state(self, *_args: object) -> None:
        if not hasattr(self, "_chase_button"):
            return
        busy = self._chase_thread is not None and self._chase_thread.isRunning()
        order = self._selected_order()
        reason = chase_unavailable_reason(order) if order is not None else "请先选中一条普通限价委托"
        self._chase_button.setEnabled(self._runtime is not None and not self._chase_shutting_down and not busy and not self._cancel_in_flight
                                      and self._tabs.currentIndex() == 0 and not reason)
        self._chase_button.setToolTip(reason or "买单跟买一、卖单跟卖一；不更改止盈止损触发价")
        self._stop_chase_button.setEnabled(busy)
        self._chase_interval.setEnabled(not busy)

    def blocks_order_reconcile(self, order_id: str, profile: str, environment: str) -> bool:
        worker = self._chase_thread
        return bool(worker is not None and worker.isRunning() and not worker.key and worker.order_id == order_id
                    and worker.profile_name == profile and worker.environment == environment)

    def _start_selected_order_chase(self) -> None:
        order = self._selected_order()
        if self._chase_shutting_down or self._runtime is None or order is None or (self._chase_thread and self._chase_thread.isRunning()):
            return
        try:
            reason = chase_unavailable_reason(order)
            if reason:
                raise ValueError(reason)
            context = self.chase_context_provider(order) if callable(self.chase_context_provider) else {}
            thread = AccountDrawerChaseThread(runtime=self._runtime, order=order, context=context,
                                              interval=self._chase_interval.value())
        except Exception as exc:
            self._chase_status_label.setText(str(exc))
            return
        self._chase_thread = thread
        thread.updated.connect(self._apply_chase_update)
        thread.failed.connect(lambda message, worker=thread: self._apply_chase_failure(worker, message))
        thread.finished.connect(lambda finished=thread: self._clear_chase_thread(finished))
        self._chase_status_label.setText("正在核对原订单、账户和开平仓性质…")
        thread.start()
        self._sync_chase_button_state()

    def _apply_chase_failure(self, worker: AccountDrawerChaseThread, message: str) -> None:
        if worker.profile_name == self._profile_name and worker.environment == self._environment:
            self._chase_status_label.setText(f"追单已暂停：{message}")

    def _apply_chase_update(self, task: object) -> None:
        if not isinstance(task, dict) or task.get("environment") != self._environment:
            return
        if task.get("profile_name") and task.get("profile_name") != self._profile_name:
            return
        mode = "开仓" if task.get("mode") == "open" else "平仓"
        budget = f" | 风险预算 {task['budget']} U | 止损 {task['stop']}" if task.get("mode") == "open" else " | 只改价格，数量不变"
        self._chase_status_label.setText(f"{task.get('inst_id', '')} {mode}：{task.get('message', '')}{budget}")
        self._sync_chase_button_state()
        self._schedule_active_table_refresh()

    def _stop_order_chase(self) -> None:
        if self._chase_thread is not None:
            self._chase_status_label.setText("正在停止追单；等待在途请求核实，交易所委托保留。")
            self._chase_thread.stop()

    def _clear_chase_thread(self, thread: AccountDrawerChaseThread) -> None:
        if self._chase_thread is thread:
            self._chase_thread = None
        thread.deleteLater()
        self._sync_chase_button_state()
        self._schedule_active_table_refresh()
        if self._runtime is not None and not self._chase_shutting_down:
            self._shared_order_store.request_refresh(runtime=self._runtime, profile_name=self._profile_name)

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
        if self._chase_thread is not None and order_reference(order) == self._chase_thread.order_id:
            self._chase_thread.stop()
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
    if self._chase_thread is not None and (runtime is not self._runtime or profile_name != self._profile_name or environment != self._environment):
        self._chase_thread.stop()
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
        self._orders_refresh_deferred_timer.stop()
        self._orders_refresh_waiting_for_realtime = False
        self._orders_refresh_started_after_realtime = False
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
    self._orders_refresh_deferred_timer.stop()
    self._orders_refresh_waiting_for_realtime = False
    self._orders_refresh_started_after_realtime = True
    self._shared_order_store.request_refresh(runtime=self._runtime, profile_name=self._profile_name)
    self._realtime_store.start_if_needed(self._runtime)
    self._realtime_store.request_reconcile("drawer")
    self._status_label.setText("正在同步当前委托和持仓…")


def _start_deferred_order_refresh(self: KlineAccountDrawer) -> None:
    if not self._orders_refresh_waiting_for_realtime or self._orders_refresh_started_after_realtime:
        return
    # The timer is only a safety wake-up.  Never start the lower-priority
    # current-order refresh while the urgent REST account check is in flight.
    if bool(getattr(self._realtime_store, "_reconcile_in_flight", False)):
        self._orders_refresh_deferred_timer.start(5_000)
        return
    self._orders_refresh_deferred_timer.stop()
    self._orders_refresh_waiting_for_realtime = False
    self._orders_refresh_started_after_realtime = True
    if self._runtime is None:
        return
    self._shared_order_store.request_refresh(runtime=self._runtime, profile_name=self._profile_name)
    self._status_label.setText(
        f"现有持仓已检查，正在同步当前委托（{len(self._snapshot.positions)} 条持仓）..."
    )


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
    known_order_instruments = dict(self._snapshot.position_instruments or {})
    missing_order_instruments = {
        str(getattr(order, "inst_id", "") or "").strip().upper()
        for order in snapshot.current_order_items
        if str(getattr(order, "inst_id", "") or "").strip().upper()
        and str(getattr(order, "inst_id", "") or "").strip().upper()
        not in known_order_instruments
    }
    if missing_order_instruments and self._runtime is not None:
        # A new order may exist before the next account reconciliation and
        # may have no matching open position.  Ask the shared account store
        # for one bounded reconciliation so its public instrument metadata is
        # available for the conversion column.
        self._realtime_store.request_reconcile("order instrument")
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
    # These are incremental observations (including terminal orders), not an
    # authoritative replacement for all current orders.
    self._shared_order_store.publish_order_updates(
        profile_name=snapshot.profile_name, environment=snapshot.environment, orders=list(snapshot.orders),
    )
    self._status_label.setText(f"委托 {len(self._snapshot.orders)} | 持仓 {len(self._snapshot.positions)}")
    # Avoid constructing any wide tables while the drawer is collapsed; when
    # visible, coalesce updates and repaint the selected tab only.
    self._schedule_active_table_refresh()
    if snapshot.source == "rest":
        _start_deferred_order_refresh(self)


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
KlineAccountDrawer._start_deferred_order_refresh = _start_deferred_order_refresh
