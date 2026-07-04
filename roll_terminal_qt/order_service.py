from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal

from PySide6.QtCore import QThread, Signal

from okx_quant.arbitrage.models import ArbitrageTradeRuntime
from okx_quant.okx_client import OkxOrderStatus, OkxRestClient, OkxTradeOrderItem


@dataclass(frozen=True)
class OrderStatusView:
    inst_id: str
    inst_type: str
    ord_id: str
    side: str
    pos_side: str
    td_mode: str
    ord_type: str
    state: str
    price: Decimal | None
    avg_price: Decimal | None
    size: Decimal | None
    filled_size: Decimal | None
    created_time: int | None
    update_time: int | None
    client_order_id: str
    reduce_only: bool | None
    raw: dict[str, object]


class OrderFeedThread(QThread):
    orders_ready = Signal(object)
    status_changed = Signal(str)

    def __init__(self, runtime: ArbitrageTradeRuntime | None) -> None:
        super().__init__()
        self._runtime = runtime
        self._client = OkxRestClient()
        self._running = True
        self._watched_inst_ids: set[str] = set()

    def set_watched_inst_ids(self, inst_ids: set[str]) -> None:
        self._watched_inst_ids = {item.strip().upper() for item in inst_ids if item and item.strip()}

    def stop(self) -> None:
        self._running = False

    def run(self) -> None:
        while self._running:
            if self._runtime is None:
                self.status_changed.emit("订单WS不可用")
                time.sleep(1.0)
                continue
            try:
                payload = self._client.get_cached_private_order_statuses(
                    self._runtime.credentials,
                    environment=self._runtime.environment,
                    limit=80,
                )
                ws_version = 0
                ws_statuses: list[OkxOrderStatus] = []
                if payload is not None:
                    ws_version, ws_statuses = payload
                pending_orders = self._client.get_pending_orders(
                    self._runtime.credentials,
                    environment=self._runtime.environment,
                    limit=80,
                    include_algo=True,
                )
                views = self._merge_order_views(ws_statuses, pending_orders)
                self.orders_ready.emit(views)
                if payload is None:
                    self.status_changed.emit(f"订单REST pending | 相关 {len(views)}")
                else:
                    self.status_changed.emit(f"订单WS v{ws_version} + REST | 相关 {len(views)}")
                time.sleep(0.35)
            except Exception as exc:  # noqa: BLE001
                self.status_changed.emit(f"订单WS异常：{exc}")
                time.sleep(1.0)

    def _merge_order_views(
        self,
        ws_statuses: list[OkxOrderStatus],
        pending_orders: list[OkxTradeOrderItem],
    ) -> list[OrderStatusView]:
        merged: list[OrderStatusView] = []
        seen_keys: set[tuple[str, str]] = set()
        for status in ws_statuses:
            if not self._is_relevant(status):
                continue
            view = self._to_view(status)
            key = self._view_identity(view)
            if key is not None and key in seen_keys:
                continue
            if key is not None:
                seen_keys.add(key)
            merged.append(view)
        for item in pending_orders:
            if not self._is_relevant_trade_order(item):
                continue
            view = self._trade_order_to_view(item)
            key = self._view_identity(view)
            if key is not None and key in seen_keys:
                continue
            if key is not None:
                seen_keys.add(key)
            merged.append(view)
        merged.sort(key=lambda item: item.update_time or item.created_time or 0, reverse=True)
        return merged

    def _is_relevant(self, status: OkxOrderStatus) -> bool:
        if not self._watched_inst_ids:
            return True
        inst_id = str(status.raw.get("instId") or "").strip().upper()
        return inst_id in self._watched_inst_ids

    def _is_relevant_trade_order(self, item: OkxTradeOrderItem) -> bool:
        if not self._watched_inst_ids:
            return True
        inst_id = str(item.inst_id or "").strip().upper()
        return inst_id in self._watched_inst_ids

    def _to_view(self, status: OkxOrderStatus) -> OrderStatusView:
        raw = status.raw if isinstance(status.raw, dict) else {}
        raw_view = dict(raw)
        raw_view["_feed_source"] = "ws"
        raw_view["_source_kind"] = (
            "algo"
            if str(raw.get("algoId") or raw.get("algoClOrdId") or "").strip()
            else "normal"
        )
        return OrderStatusView(
            inst_id=str(raw.get("instId") or ""),
            inst_type=str(raw.get("instType") or ""),
            ord_id=status.ord_id,
            side=str(status.side or ""),
            pos_side=str(raw.get("posSide") or ""),
            td_mode=str(raw.get("tdMode") or ""),
            ord_type=str(status.ord_type or ""),
            state=status.state,
            price=status.price,
            avg_price=status.avg_price,
            size=status.size,
            filled_size=status.filled_size,
            created_time=_parse_int_like(raw.get("cTime")),
            update_time=_parse_int_like(raw.get("uTime")),
            client_order_id=str(raw.get("clOrdId") or ""),
            reduce_only=_parse_optional_bool(raw.get("reduceOnly")),
            raw=raw_view,
        )

    def _trade_order_to_view(self, item: OkxTradeOrderItem) -> OrderStatusView:
        raw = dict(item.raw) if isinstance(item.raw, dict) else {}
        raw["_feed_source"] = "rest_pending"
        raw["_source_kind"] = str(item.source_kind or "").strip().lower() or "normal"
        return OrderStatusView(
            inst_id=str(item.inst_id or ""),
            inst_type=str(item.inst_type or ""),
            ord_id=str(item.order_id or item.algo_id or ""),
            side=str(item.side or ""),
            pos_side=str(item.pos_side or ""),
            td_mode=str(item.td_mode or ""),
            ord_type=str(item.ord_type or ""),
            state=str(item.state or ""),
            price=item.price,
            avg_price=item.avg_price,
            size=item.size,
            filled_size=item.filled_size,
            created_time=item.created_time,
            update_time=item.update_time,
            client_order_id=str(item.client_order_id or item.algo_client_order_id or ""),
            reduce_only=item.reduce_only,
            raw=raw,
        )

    @staticmethod
    def _view_identity(view: OrderStatusView) -> tuple[str, str] | None:
        ord_id = str(view.ord_id or "").strip()
        if ord_id:
            return ("ord_id", ord_id)
        cl_ord_id = str(view.client_order_id or "").strip()
        if cl_ord_id:
            return ("cl_ord_id", cl_ord_id)
        inst_id = str(view.inst_id or "").strip().upper()
        update_time = str(view.update_time or view.created_time or "").strip()
        if inst_id and update_time:
            return ("inst_time", f"{inst_id}:{update_time}")
        return None


def _parse_int_like(value: object) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except Exception:
        return None


def _parse_optional_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None
