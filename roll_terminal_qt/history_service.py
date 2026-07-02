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
    _merge_history_cache_records,
    _position_history_item_from_cache,
    _serialize_history_item,
)


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
            items = self._load_local_position_history(profile_name=profile_name, environment=environment)
            if items:
                if not self._running:
                    return
                instruments = self._build_instrument_map(items)
                usdt_prices = self._build_usdt_prices(items)
                self.data_ready.emit(
                    {
                        "items": items,
                        "instruments": instruments,
                        "usdt_prices": usdt_prices,
                    }
                )
                self.status_changed.emit(f"历史仓位 {len(items)} 条 | 本地缓存")
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
        try:
            items = self._client.get_order_history(
                self._runtime.credentials,
                environment=self._runtime.environment,
                limit=self._limit,
            )
            if not self._running:
                return
            self.data_ready.emit(
                {
                    "items": items,
                    "usdt_prices": self._build_order_usdt_prices(items),
                }
            )
            self.status_changed.emit(f"历史委托 {len(items)} 条")
        except Exception as exc:
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
        try:
            items = self._client.get_fills_history(
                self._runtime.credentials,
                environment=self._runtime.environment,
                limit=self._limit,
            )
            if not self._running:
                return
            self.data_ready.emit(
                {
                    "items": items,
                    "instruments": self._build_instrument_map(items),
                    "usdt_prices": self._build_usdt_prices(items),
                }
            )
            self.status_changed.emit(f"历史成交 {len(items)} 条")
        except Exception as exc:
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
