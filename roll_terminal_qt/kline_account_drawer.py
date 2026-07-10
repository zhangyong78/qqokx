from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from PySide6.QtCore import QThread, Qt, Signal
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from okx_quant.okx_client import OkxPosition, OkxTradeOrderItem
from okx_quant.okx_client import OkxRestClient


@dataclass(frozen=True)
class AccountDrawerSnapshot:
    positions: tuple[OkxPosition, ...] = ()
    orders: tuple[OkxTradeOrderItem, ...] = ()


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


class AccountDrawerLoadThread(QThread):
    completed = Signal(int, object)
    failed = Signal(int, str)

    def __init__(self, *, request_generation: int, runtime: object, client: OkxRestClient | None = None) -> None:
        super().__init__()
        self._request_generation = request_generation
        self._runtime = runtime
        self._client = client or OkxRestClient()

    def run(self) -> None:
        try:
            positions = self._client.get_positions(
                self._runtime.credentials,
                environment=self._runtime.environment,
            )
            orders = self._client.get_pending_orders(
                self._runtime.credentials,
                environment=self._runtime.environment,
                limit=100,
                include_algo=True,
            )
        except Exception as exc:
            self.failed.emit(self._request_generation, str(exc))
            return
        self.completed.emit(
            self._request_generation,
            AccountDrawerSnapshot(tuple(positions), tuple(orders)),
        )


class KlineAccountDrawer(QWidget):
    collapseRequested = Signal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._runtime: object | None = None
        self._profile_name = ""
        self._environment = ""
        self._symbol = ""
        self._request_generation = 0
        self._snapshot = AccountDrawerSnapshot()
        self._load_thread: AccountDrawerLoadThread | None = None
        self._cancel_thread: QThread | None = None
        self._cancel_in_flight = False
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
        self._scope_combo.currentIndexChanged.connect(self._refresh_tables)
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
                "标识",
            ]
        )
        orders_tab = QWidget()
        orders_layout = QVBoxLayout(orders_tab)
        orders_layout.setContentsMargins(0, 0, 0, 0)
        orders_layout.setSpacing(8)
        orders_layout.addWidget(self._orders_table, 1)

        orders_toolbar = QHBoxLayout()
        self._cancel_button = QPushButton("撤单")
        self._cancel_button.setEnabled(False)
        orders_toolbar.addStretch(1)
        orders_toolbar.addWidget(self._cancel_button, 0)
        orders_layout.addLayout(orders_toolbar)
        self._tabs.addTab(orders_tab, "当前委托")

        self._positions_table = self._create_table(
            ["合约", "方向", "持仓量", "可平量", "开仓均价", "标记价", "未实现盈亏", "保证金模式", "持仓模式"]
        )
        positions_tab = QWidget()
        positions_layout = QVBoxLayout(positions_tab)
        positions_layout.setContentsMargins(0, 0, 0, 0)
        positions_layout.addWidget(self._positions_table, 1)
        self._tabs.addTab(positions_tab, "当前持仓")

    def _create_table(self, headers: list[str]) -> QTableWidget:
        table = QTableWidget(0, len(headers), self)
        table.setHorizontalHeaderLabels(headers)
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        table.verticalHeader().setVisible(False)
        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        table.horizontalHeader().setStretchLastSection(True)
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
            self._refresh_tables()
        if changed and refresh_if_visible and self.isVisible():
            self.refresh_data()

    def refresh_data(self) -> None:
        if self._runtime is None:
            self._status_label.setText("未连接账户")
            return
        if self._load_thread is not None and self._load_thread.isRunning():
            return
        self._request_generation += 1
        self._status_label.setText("加载中...")
        self._load_thread = AccountDrawerLoadThread(
            request_generation=self._request_generation,
            runtime=self._runtime,
        )
        self._load_thread.completed.connect(self._apply_snapshot)
        self._load_thread.failed.connect(self._apply_load_error)
        self._load_thread.finished.connect(self._clear_load_thread)
        self._load_thread.start()

    def shutdown(self, wait_ms: int = 1500) -> bool:
        if self._load_thread is not None and self._load_thread.isRunning():
            return self._load_thread.wait(wait_ms)
        return True

    def _clear_load_thread(self) -> None:
        thread = self._load_thread
        self._load_thread = None
        if thread is not None:
            thread.deleteLater()

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
        self._populate_orders_table(filtered_orders)
        self._populate_positions_table(filtered_positions)

    def _populate_orders_table(self, orders: list[object]) -> None:
        self._orders_table.setRowCount(len(orders))
        for row, order in enumerate(orders):
            values = [
                getattr(order, "inst_id", ""),
                getattr(order, "source_label", "") or order_source_kind(order),
                getattr(order, "side", "") or getattr(order, "pos_side", ""),
                getattr(order, "ord_type", ""),
                getattr(order, "trigger_price", None) or getattr(order, "price", None) or "",
                getattr(order, "size", ""),
                getattr(order, "filled_size", ""),
                getattr(order, "state", ""),
                getattr(order, "update_time", None) or getattr(order, "created_time", None) or "",
                order_cancel_reference(order),
            ]
            for column, value in enumerate(values):
                self._orders_table.setItem(row, column, QTableWidgetItem(self._format_value(value)))

    def _populate_positions_table(self, positions: list[object]) -> None:
        self._positions_table.setRowCount(len(positions))
        for row, position in enumerate(positions):
            values = [
                getattr(position, "inst_id", ""),
                getattr(position, "pos_side", ""),
                getattr(position, "position", ""),
                getattr(position, "avail_position", ""),
                getattr(position, "avg_price", ""),
                getattr(position, "mark_price", ""),
                getattr(position, "unrealized_pnl", ""),
                getattr(position, "mgn_mode", ""),
                getattr(position, "pos_side", ""),
            ]
            for column, value in enumerate(values):
                self._positions_table.setItem(row, column, QTableWidgetItem(self._format_value(value)))

    @staticmethod
    def _format_value(value: object) -> str:
        if value is None:
            return ""
        return str(value)
