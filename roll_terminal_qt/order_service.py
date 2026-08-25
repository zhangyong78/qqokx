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


def _order_view_source_kind(order: OrderStatusView) -> str:
    raw = order.raw if isinstance(order.raw, dict) else {}
    return str(raw.get("_source_kind") or "").strip().lower() or "normal"


def _order_view_source_label(order: OrderStatusView) -> str:
    raw = order.raw if isinstance(order.raw, dict) else {}
    feed_source = str(raw.get("_feed_source") or "").strip().lower()
    source_kind = _order_view_source_kind(order)
    if feed_source == "rest_pending" and source_kind == "algo":
        return "REST 算法"
    if feed_source == "rest_pending":
        return "REST 普通"
    if feed_source == "ws" and source_kind == "algo":
        return "WS 算法"
    if feed_source == "ws":
        return "WS 当前"
    if source_kind == "algo":
        return "算法委托"
    return "普通委托"


def _order_view_raw_decimal(raw: dict[str, object], *keys: str) -> Decimal | None:
    for key in keys:
        value = raw.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        try:
            return Decimal(text)
        except Exception:
            continue
    return None


def _order_view_extract_tp_sl_fields(raw: dict[str, object]) -> dict[str, Decimal | str | None]:
    take_profit_trigger_price = _order_view_raw_decimal(raw, "tpTriggerPx", "takeProfitTriggerPrice")
    take_profit_order_price = _order_view_raw_decimal(raw, "tpOrdPx", "takeProfitOrdPx")
    take_profit_trigger_price_type = raw.get("tpTriggerPxType") or raw.get("takeProfitTriggerPxType")
    stop_loss_trigger_price = _order_view_raw_decimal(raw, "slTriggerPx", "stopLossTriggerPrice")
    stop_loss_order_price = _order_view_raw_decimal(raw, "slOrdPx", "stopLossOrdPx")
    stop_loss_trigger_price_type = raw.get("slTriggerPxType") or raw.get("stopLossTriggerPxType")
    attach_algo_orders = raw.get("attachAlgoOrds")
    if isinstance(attach_algo_orders, list):
        for item in attach_algo_orders:
            if not isinstance(item, dict):
                continue
            if take_profit_trigger_price is None:
                take_profit_trigger_price = _order_view_raw_decimal(item, "tpTriggerPx", "takeProfitTriggerPrice")
            if take_profit_order_price is None:
                take_profit_order_price = _order_view_raw_decimal(item, "tpOrdPx", "takeProfitOrdPx")
            if take_profit_trigger_price_type is None:
                take_profit_trigger_price_type = item.get("tpTriggerPxType") or item.get("takeProfitTriggerPxType")
            if stop_loss_trigger_price is None:
                stop_loss_trigger_price = _order_view_raw_decimal(item, "slTriggerPx", "stopLossTriggerPrice")
            if stop_loss_order_price is None:
                stop_loss_order_price = _order_view_raw_decimal(item, "slOrdPx", "stopLossOrdPx")
            if stop_loss_trigger_price_type is None:
                stop_loss_trigger_price_type = item.get("slTriggerPxType") or item.get("stopLossTriggerPxType")
    return {
        "take_profit_trigger_price": take_profit_trigger_price,
        "take_profit_order_price": take_profit_order_price,
        "take_profit_trigger_price_type": (
            str(take_profit_trigger_price_type).strip() if take_profit_trigger_price_type is not None else None
        ),
        "stop_loss_trigger_price": stop_loss_trigger_price,
        "stop_loss_order_price": stop_loss_order_price,
        "stop_loss_trigger_price_type": (
            str(stop_loss_trigger_price_type).strip() if stop_loss_trigger_price_type is not None else None
        ),
    }


def order_view_to_trade_order_item(order: OrderStatusView) -> OkxTradeOrderItem:
    raw = dict(order.raw) if isinstance(order.raw, dict) else {}
    algo_id = str(raw.get("algoId") or "").strip() or None
    algo_cl_ord_id = str(raw.get("algoClOrdId") or "").strip() or None
    tp_sl = _order_view_extract_tp_sl_fields(raw)
    return OkxTradeOrderItem(
        source_kind=_order_view_source_kind(order),
        source_label=_order_view_source_label(order),
        created_time=order.created_time,
        update_time=order.update_time,
        inst_id=order.inst_id,
        inst_type=order.inst_type,
        side=order.side or None,
        pos_side=order.pos_side or None,
        td_mode=order.td_mode or None,
        ord_type=order.ord_type or None,
        state=order.state or None,
        price=order.price,
        size=order.size,
        filled_size=order.filled_size,
        avg_price=order.avg_price,
        order_id=order.ord_id or None,
        algo_id=algo_id,
        client_order_id=order.client_order_id or None,
        algo_client_order_id=algo_cl_ord_id or (order.client_order_id or None),
        pnl=_order_view_raw_decimal(raw, "pnl"),
        fee=_order_view_raw_decimal(raw, "fee", "actualFee", "fillFee"),
        fee_currency=(
            str(raw.get("feeCcy") or raw.get("actualFeeCcy") or raw.get("fillFeeCcy") or "").strip() or None
        ),
        reduce_only=order.reduce_only,
        trigger_price=_order_view_raw_decimal(raw, "triggerPx", "triggerPrice"),
        trigger_price_type=(str(raw.get("triggerPxType") or raw.get("triggerPriceType") or "").strip() or None),
        order_price=_order_view_raw_decimal(raw, "orderPx") or order.price,
        actual_price=_order_view_raw_decimal(raw, "actualPx", "avgPx", "fillPx") or order.avg_price,
        actual_size=_order_view_raw_decimal(raw, "actualSz", "accFillSz", "fillSz") or order.filled_size,
        actual_side=(str(raw.get("actualSide") or "").strip() or order.side or None),
        take_profit_trigger_price=tp_sl["take_profit_trigger_price"],
        take_profit_order_price=tp_sl["take_profit_order_price"],
        take_profit_trigger_price_type=tp_sl["take_profit_trigger_price_type"],
        stop_loss_trigger_price=tp_sl["stop_loss_trigger_price"],
        stop_loss_order_price=tp_sl["stop_loss_order_price"],
        stop_loss_trigger_price_type=tp_sl["stop_loss_trigger_price_type"],
        raw=raw,
    )


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
        try:
            self._client.close()
        except Exception:
            pass

    def run(self) -> None:
        while self._running:
            if self._runtime is None:
                self.status_changed.emit("订单WS不可用")
                time.sleep(1.0)
                continue
            try:
                status_text, views = load_current_order_views(
                    self._runtime,
                    client=self._client,
                    limit=80,
                )
                self.orders_ready.emit(views)
                self.status_changed.emit(status_text)
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


def load_current_order_views(
    runtime: ArbitrageTradeRuntime | None,
    *,
    client: OkxRestClient | None = None,
    limit: int = 80,
) -> tuple[str, list[OrderStatusView]]:
    if runtime is None:
        raise RuntimeError("订单WS不可用")
    rest_client = client or OkxRestClient()
    payload = rest_client.get_cached_private_order_statuses(
        runtime.credentials,
        environment=runtime.environment,
        limit=limit,
    )
    ws_version = 0
    ws_statuses: list[OkxOrderStatus] = []
    if payload is not None:
        ws_version, ws_statuses = payload
    pending_orders = rest_client.get_pending_orders(
        runtime.credentials,
        environment=runtime.environment,
        limit=limit,
        include_algo=True,
    )
    feed = OrderFeedThread(runtime)
    views = feed._merge_order_views(ws_statuses, pending_orders)
    if payload is None:
        return (f"订单REST pending | 相关 {len(views)}", views)
    return (f"订单WS v{ws_version} + REST | 相关 {len(views)}", views)
