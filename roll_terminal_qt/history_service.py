from __future__ import annotations

from decimal import Decimal

from PySide6.QtCore import QThread, Signal

from okx_quant.arbitrage.models import ArbitrageTradeRuntime
from okx_quant.models import Instrument
from okx_quant.okx_client import (
    OkxFillHistoryItem,
    OkxPositionHistoryItem,
    OkxRestClient,
    OkxTradeOrderItem,
)
from okx_quant.persistence import load_history_cache_records, save_history_cache_records
from okx_quant.ui_shell import (
    _build_usdt_price_snapshot,
    _collapse_position_history_records,
    _infer_fill_history_pnl_currency,
    _infer_position_history_pnl_currency,
    _fill_item_from_cache,
    _merge_history_cache_records,
    _order_item_from_cache,
    _position_history_item_from_cache,
    _serialize_history_item,
)


_ORDER_HISTORY_DEDUP_FIELDS = (
    "source_kind", "order_id", "algo_id", "client_order_id",
    "algo_client_order_id", "inst_id", "created_time",
)
_FILL_HISTORY_DEDUP_FIELDS = (
    "trade_id", "order_id", "inst_id", "fill_time", "side", "fill_size", "fill_price",
)


def load_cached_order_history(profile_name: str, environment: str, limit: int) -> list[OkxTradeOrderItem]:
    records = load_history_cache_records("orders", profile_name, environment)
    items = [item for record in records if (item := _order_item_from_cache(record)) is not None]
    items.sort(key=lambda item: item.update_time or item.created_time or 0, reverse=True)
    return items[:limit]


def merge_order_history_cache(
    *, profile_name: str, environment: str, remote_items: list[OkxTradeOrderItem], limit: int,
) -> list[OkxTradeOrderItem]:
    records = _merge_history_cache_records(
        local_records=load_history_cache_records("orders", profile_name, environment),
        remote_records=[_serialize_history_item(item) for item in remote_items],
        dedup_fields=_ORDER_HISTORY_DEDUP_FIELDS,
    )
    save_history_cache_records("orders", profile_name, environment, records)
    items = [item for record in records if (item := _order_item_from_cache(record)) is not None]
    items.sort(key=lambda item: item.update_time or item.created_time or 0, reverse=True)
    return items[:limit]


def _load_cached_fill_history(profile_name: str, environment: str, limit: int) -> list[OkxFillHistoryItem]:
    records = load_history_cache_records("fills", profile_name, environment)
    items = [item for record in records if (item := _fill_item_from_cache(record)) is not None]
    items.sort(key=lambda item: item.fill_time or 0, reverse=True)
    return items[:limit]


def _merge_fill_history_cache(
    *, profile_name: str, environment: str, remote_items: list[OkxFillHistoryItem], limit: int,
) -> list[OkxFillHistoryItem]:
    records = _merge_history_cache_records(
        local_records=load_history_cache_records("fills", profile_name, environment),
        remote_records=[_serialize_history_item(item) for item in remote_items],
        dedup_fields=_FILL_HISTORY_DEDUP_FIELDS,
    )
    save_history_cache_records("fills", profile_name, environment, records)
    items = [item for record in records if (item := _fill_item_from_cache(record)) is not None]
    items.sort(key=lambda item: item.fill_time or 0, reverse=True)
    return items[:limit]


class PositionHistoryFeedThread(QThread):
    data_ready = Signal(object)
    status_changed = Signal(str)

    def __init__(self, runtime: ArbitrageTradeRuntime | None, *, limit: int = 120) -> None:
        super().__init__()
        self._runtime = runtime
        self._limit = max(20, int(limit))
        self._client = OkxRestClient()
        self._running = True

    def stop(self) -> None:
        self._running = False

    def run(self) -> None:
        if self._runtime is None:
            self.status_changed.emit("历史仓位不可用")
            return
        profile_name = str(getattr(self._runtime, "credential_profile_name", "") or "").strip()
        environment = str(getattr(self._runtime, "environment", "") or "").strip()
        cached_items = self._load_local_position_history(profile_name=profile_name, environment=environment)
        if cached_items and self._running:
            self.data_ready.emit({"items": cached_items, "instruments": {}, "usdt_prices": {}})
            self.status_changed.emit(f"历史仓位 {len(cached_items)} 条 | 本地缓存")
        try:
            remote_items = self._client.get_positions_history(
                self._runtime.credentials,
                environment=environment,
                limit=self._limit,
            )
            if not self._running:
                return
            items = self._merge_position_history_cache(
                profile_name=profile_name,
                environment=environment,
                remote_items=remote_items,
            )
            instruments = self._build_instrument_map(items)
            usdt_prices = self._build_usdt_prices(items)
            self.data_ready.emit(
                {
                    "items": items,
                    "instruments": instruments,
                    "usdt_prices": usdt_prices,
                }
            )
            self.status_changed.emit(f"历史仓位 {len(items)} 条")
        except Exception as exc:
            if cached_items:
                self.status_changed.emit(f"历史仓位 {len(cached_items)} 条 | 本地缓存（后台同步失败：{exc}）")
                return
            self.status_changed.emit(f"历史仓位读取异常：{exc}")

    def _load_local_position_history(self, *, profile_name: str, environment: str) -> list[OkxPositionHistoryItem]:
        local_records = load_history_cache_records("positions", profile_name, environment)
        collapsed_records = _collapse_position_history_records(local_records)
        if collapsed_records != local_records:
            save_history_cache_records("positions", profile_name, environment, collapsed_records)
        parsed_items = [
            item
            for record in collapsed_records
            if isinstance(record, dict) and (item := _position_history_item_from_cache(record)) is not None
        ]
        parsed_items.sort(key=lambda item: item.update_time or 0, reverse=True)
        return parsed_items[: self._limit]

    def _merge_position_history_cache(
        self,
        *,
        profile_name: str,
        environment: str,
        remote_items: list[OkxPositionHistoryItem],
    ) -> list[OkxPositionHistoryItem]:
        local_records = load_history_cache_records("positions", profile_name, environment)
        merged_records = _merge_history_cache_records(
            local_records=local_records,
            remote_records=[_serialize_history_item(item) for item in remote_items],
            dedup_fields=("update_time", "inst_id", "pos_side", "direction", "close_size", "close_avg_price"),
        )
        collapsed_records = _collapse_position_history_records(merged_records)
        save_history_cache_records("positions", profile_name, environment, collapsed_records)
        parsed_items = [
            item
            for record in collapsed_records
            if isinstance(record, dict) and (item := _position_history_item_from_cache(record)) is not None
        ]
        parsed_items.sort(key=lambda item: item.update_time or 0, reverse=True)
        return parsed_items[: self._limit]

    def _build_instrument_map(self, items: list[OkxPositionHistoryItem]) -> dict[str, Instrument]:
        result: dict[str, Instrument] = {}
        for inst_id in sorted({item.inst_id for item in items if item.inst_id}):
            try:
                result[inst_id] = self._client.get_instrument(inst_id, prefer_cached=True)
            except Exception:
                continue
        return result

    def _build_usdt_prices(self, items: list[OkxPositionHistoryItem]) -> dict[str, Decimal]:
        currencies: set[str] = set()
        for item in items:
            if item.pnl is not None:
                currencies.add(_infer_position_history_pnl_currency(item))
            if item.realized_pnl is not None:
                currencies.add(_infer_position_history_pnl_currency(item))
            if item.fee is not None and item.fee_currency:
                currencies.add(str(item.fee_currency).strip().upper())
        return _build_usdt_price_snapshot(self._client, currencies) if currencies else {}


class OrderHistoryFeedThread(QThread):
    data_ready = Signal(object)
    status_changed = Signal(str)

    def __init__(self, runtime: ArbitrageTradeRuntime | None, *, limit: int = 200) -> None:
        super().__init__()
        self._runtime = runtime
        self._limit = max(20, int(limit))
        self._client = OkxRestClient()
        self._running = True

    def stop(self) -> None:
        self._running = False

    def run(self) -> None:
        if self._runtime is None:
            self.status_changed.emit("历史委托不可用")
            return
        profile_name = str(getattr(self._runtime, "credential_profile_name", "") or "").strip()
        environment = str(getattr(self._runtime, "environment", "") or "").strip()
        cached_items = load_cached_order_history(profile_name, environment, self._limit)
        if cached_items and self._running:
            self.data_ready.emit({"items": cached_items, "usdt_prices": {}})
            self.status_changed.emit(f"历史委托 {len(cached_items)} 条 | 本地缓存")
        try:
            remote_items = self._client.get_order_history(
                self._runtime.credentials,
                environment=environment,
                limit=self._limit,
                include_algo=True,
            )
            if not self._running:
                return
            items = merge_order_history_cache(
                profile_name=profile_name,
                environment=environment,
                remote_items=remote_items,
                limit=self._limit,
            )
            self.data_ready.emit(
                {
                    "items": items,
                    "usdt_prices": self._build_order_usdt_prices(items),
                }
            )
            self.status_changed.emit(f"历史委托 {len(items)} 条")
        except Exception as exc:
            if cached_items:
                self.status_changed.emit(f"历史委托 {len(cached_items)} 条 | 本地缓存（后台同步失败：{exc}）")
                return
            self.status_changed.emit(f"历史委托读取异常：{exc}")

    def _build_order_usdt_prices(self, items: list[OkxTradeOrderItem]) -> dict[str, Decimal]:
        currencies = {
            str(item.fee_currency).strip().upper()
            for item in items
            if item.fee is not None and str(item.fee_currency or "").strip()
        }
        return _build_usdt_price_snapshot(self._client, currencies) if currencies else {}


class FillHistoryFeedThread(QThread):
    data_ready = Signal(object)
    status_changed = Signal(str)

    def __init__(self, runtime: ArbitrageTradeRuntime | None, *, limit: int = 100) -> None:
        super().__init__()
        self._runtime = runtime
        self._limit = max(20, int(limit))
        self._client = OkxRestClient()
        self._running = True

    def stop(self) -> None:
        self._running = False

    def run(self) -> None:
        if self._runtime is None:
            self.status_changed.emit("历史成交不可用")
            return
        profile_name = str(getattr(self._runtime, "credential_profile_name", "") or "").strip()
        environment = str(getattr(self._runtime, "environment", "") or "").strip()
        cached_items = _load_cached_fill_history(profile_name, environment, self._limit)
        if cached_items and self._running:
            self.data_ready.emit({"items": cached_items, "instruments": {}, "usdt_prices": {}})
            self.status_changed.emit(f"历史成交 {len(cached_items)} 条 | 本地缓存")
        try:
            remote_items = self._client.get_fills_history(
                self._runtime.credentials,
                environment=environment,
                limit=self._limit,
            )
            if not self._running:
                return
            items = _merge_fill_history_cache(
                profile_name=profile_name,
                environment=environment,
                remote_items=remote_items,
                limit=self._limit,
            )
            self.data_ready.emit(
                {
                    "items": items,
                    "instruments": self._build_instrument_map(items),
                    "usdt_prices": self._build_usdt_prices(items),
                }
            )
            self.status_changed.emit(f"历史成交 {len(items)} 条")
        except Exception as exc:
            if cached_items:
                self.status_changed.emit(f"历史成交 {len(cached_items)} 条 | 本地缓存（后台同步失败：{exc}）")
                return
            self.status_changed.emit(f"历史成交读取异常：{exc}")

    def _build_instrument_map(self, items: list[OkxFillHistoryItem]) -> dict[str, Instrument]:
        result: dict[str, Instrument] = {}
        for inst_id in sorted({item.inst_id for item in items if item.inst_id}):
            try:
                result[inst_id] = self._client.get_instrument(inst_id, prefer_cached=True)
            except Exception:
                continue
        return result

    def _build_usdt_prices(self, items: list[OkxFillHistoryItem]) -> dict[str, Decimal]:
        currencies: set[str] = set()
        for item in items:
            if item.pnl is not None:
                currencies.add(_infer_fill_history_pnl_currency(item))
            if item.fill_fee is not None and item.fee_currency:
                currencies.add(str(item.fee_currency).strip().upper())
        return _build_usdt_price_snapshot(self._client, currencies) if currencies else {}
