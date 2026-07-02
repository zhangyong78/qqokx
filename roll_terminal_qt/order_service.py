from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal

from PySide6.QtCore import QThread, Signal

from okx_quant.arbitrage.models import ArbitrageTradeRuntime
from okx_quant.okx_client import OkxOrderStatus, OkxRestClient


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
                if payload is None:
                    self.status_changed.emit("等待订单WS...")
                    time.sleep(0.5)
                    continue
                version, statuses = payload
                views = [self._to_view(item) for item in statuses if self._is_relevant(item)]
                self.orders_ready.emit(views)
                self.status_changed.emit(f"订单WS v{version} | 相关 {len(views)}")
                time.sleep(0.35)
            except Exception as exc:  # noqa: BLE001
                self.status_changed.emit(f"订单WS异常：{exc}")
                time.sleep(1.0)

    def _is_relevant(self, status: OkxOrderStatus) -> bool:
        if not self._watched_inst_ids:
            return True
        inst_id = str(status.raw.get("instId") or "").strip().upper()
        return inst_id in self._watched_inst_ids

    def _to_view(self, status: OkxOrderStatus) -> OrderStatusView:
        raw = status.raw if isinstance(status.raw, dict) else {}
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
            raw=dict(raw),
        )


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
