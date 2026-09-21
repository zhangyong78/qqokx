from __future__ import annotations

import time
from decimal import Decimal
from typing import Iterable

from PySide6.QtCore import QObject, QThread, Signal

from okx_quant.arbitrage.models import ArbitrageTradeRuntime
from okx_quant.models import Instrument
from okx_quant.okx_client import (
    OkxFillHistoryItem,
    OkxPositionHistoryItem,
    OkxRestClient,
    OkxTradeOrderItem,
)
from okx_quant.persistence import (
    load_history_sync_state,
    save_history_sync_state,
)
from roll_terminal_qt.history_service import (
    _build_usdt_price_snapshot,
    _infer_fill_history_pnl_currency,
    _infer_position_history_pnl_currency,
    _merge_fill_history_cache,
    _load_cached_fill_history,
    load_local_fill_history,
    load_local_order_history,
    load_local_position_history_all,
    merge_order_history_cache,
    merge_position_history_cache,
)


HISTORY_SYNC_SOURCES = ("fills", "orders", "positions")
HISTORY_SYNC_SOURCE_LABELS = {"fills": "历史成交", "orders": "历史委托", "positions": "历史仓位"}


class HistorySyncThread(QThread):
    payload_ready = Signal(str, object)
    progress = Signal(str, str)
    failed = Signal(str, str)
    completed = Signal(object)

    def __init__(
        self,
        runtime: ArbitrageTradeRuntime,
        *,
        profile_name: str,
        sources: tuple[str, ...],
        deep: bool,
        display_limits: dict[str, int],
    ) -> None:
        super().__init__()
        self._runtime = runtime
        self._profile_name = str(profile_name or "").strip()
        self._environment = str(getattr(runtime, "environment", "") or "").strip()
        self._sources = tuple(source for source in sources if source in HISTORY_SYNC_SOURCES)
        self._deep = bool(deep)
        self._display_limits = {
            source: max(20, int(display_limits.get(source, 100)))
            for source in HISTORY_SYNC_SOURCES
        }
        self._running = True
        self._client: OkxRestClient | None = None

    def stop(self) -> None:
        self._running = False
        client = self._client
        if client is not None:
            try:
                client.close()
            except Exception:
                pass

    def run(self) -> None:
        client = OkxRestClient()
        self._client = client
        completed: list[str] = []
        try:
            for source in self._sources:
                if not self._running:
                    break
                try:
                    self._emit_cached(source)
                except Exception as exc:  # noqa: BLE001
                    # A damaged local cache must not terminate the worker before
                    # it gets a chance to repair the cache from OKX.
                    if self._running:
                        self.failed.emit(source, f"读取本地缓存失败，将继续同步远端数据：{exc}")
                try:
                    self._sync_source(source, client)
                    completed.append(source)
                except Exception as exc:  # noqa: BLE001
                    if self._running:
                        self.failed.emit(source, str(exc))
            self.completed.emit({"requested": self._sources, "completed": tuple(completed)})
        finally:
            self._client = None
            try:
                client.close()
            except Exception:
                pass

    def _emit_cached(self, source: str) -> None:
        if source == "fills":
            items = _load_cached_fill_history(
                self._profile_name,
                self._environment,
                self._display_limits[source],
            )
        elif source == "orders":
            items = load_local_order_history(self._profile_name, self._environment)[: self._display_limits[source]]
        else:
            items = load_local_position_history_all(self._profile_name, self._environment)[: self._display_limits[source]]
        if items:
            self.payload_ready.emit(source, {"items": items, "cached": True})
            self.progress.emit(source, f"{HISTORY_SYNC_SOURCE_LABELS[source]}已显示本地缓存 {len(items)} 条，正在检查最新数据...")

    def _sync_source(self, source: str, client: OkxRestClient) -> None:
        state = load_history_sync_state(self._profile_name, self._environment)
        source_state = state.get("sources", {}) if isinstance(state, dict) else {}
        has_cursor = isinstance(source_state, dict) and isinstance(source_state.get(source), dict)
        display_limit = self._display_limits[source]
        if self._deep:
            remote_limit = max(display_limit, {"fills": 100, "orders": 200, "positions": 300}[source])
        elif has_cursor:
            remote_limit = {"fills": 20, "orders": 20, "positions": 30}[source]
        else:
            remote_limit = min(display_limit, {"fills": 100, "orders": 100, "positions": 100}[source])
        mode_text = "深度检查" if self._deep else ("增量检查" if has_cursor else "首次检查")
        self.progress.emit(source, f"{mode_text}{HISTORY_SYNC_SOURCE_LABELS[source]}，最多读取 {remote_limit} 条...")

        if source == "fills":
            remote_items = client.get_fills_history(
                self._runtime.credentials,
                environment=self._environment,
                limit=remote_limit,
                progress_callback=lambda text: self.progress.emit(source, text),
            )
            items = _merge_fill_history_cache(
                profile_name=self._profile_name,
                environment=self._environment,
                remote_items=remote_items,
                limit=display_limit,
            )
            payload = {
                "items": items,
                "instruments": self._build_fill_instrument_map(client, items),
                "usdt_prices": self._build_fill_usdt_prices(client, items),
            }
        elif source == "orders":
            remote_items = client.get_order_history(
                self._runtime.credentials,
                environment=self._environment,
                limit=remote_limit,
                include_algo=self._deep,
            )
            items = merge_order_history_cache(
                profile_name=self._profile_name,
                environment=self._environment,
                remote_items=remote_items,
                limit=display_limit,
            )
            payload = {
                "items": items,
                "usdt_prices": self._build_order_usdt_prices(client, items),
            }
        else:
            remote_items = client.get_positions_history(
                self._runtime.credentials,
                environment=self._environment,
                limit=remote_limit,
            )
            items = merge_position_history_cache(
                profile_name=self._profile_name,
                environment=self._environment,
                remote_items=remote_items,
                limit=display_limit,
            )
            payload = {
                "items": items,
                "instruments": self._build_position_instrument_map(client, items),
                "usdt_prices": self._build_position_usdt_prices(client, items),
            }
        self._save_cursor(source, items)
        self.payload_ready.emit(source, payload)
        self.progress.emit(source, f"{HISTORY_SYNC_SOURCE_LABELS[source]}同步完成，本地显示 {len(items)} 条")

    def _save_cursor(self, source: str, items: list[object]) -> None:
        if not items:
            return
        latest = items[0]
        if source == "fills":
            latest_time = getattr(latest, "fill_time", None)
            latest_id = getattr(latest, "trade_id", None) or getattr(latest, "order_id", None)
        elif source == "orders":
            latest_time = getattr(latest, "update_time", None) or getattr(latest, "created_time", None)
            latest_id = getattr(latest, "order_id", None) or getattr(latest, "client_order_id", None)
        else:
            latest_time = getattr(latest, "update_time", None)
            raw = getattr(latest, "raw", {})
            latest_id = raw.get("posId") if isinstance(raw, dict) else None
        state = load_history_sync_state(self._profile_name, self._environment)
        sources = state.get("sources") if isinstance(state.get("sources"), dict) else {}
        sources[source] = {
            "last_time": int(latest_time or 0),
            "last_id": str(latest_id or ""),
            "last_sync_epoch": int(time.time()),
            "local_count": len(items),
        }
        save_history_sync_state(
            self._profile_name,
            self._environment,
            {"version": 1, "sources": sources},
        )

    @staticmethod
    def _build_fill_instrument_map(client: OkxRestClient, items: list[OkxFillHistoryItem]) -> dict[str, Instrument]:
        result: dict[str, Instrument] = {}
        for inst_id in sorted({item.inst_id for item in items if item.inst_id}):
            try:
                result[inst_id] = client.get_instrument(inst_id, prefer_cached=True)
            except Exception:
                continue
        return result

    @staticmethod
    def _build_fill_usdt_prices(client: OkxRestClient, items: list[OkxFillHistoryItem]) -> dict[str, Decimal]:
        currencies: set[str] = set()
        for item in items:
            if item.pnl is not None:
                currencies.add(_infer_fill_history_pnl_currency(item))
            if item.fill_fee is not None and item.fee_currency:
                currencies.add(str(item.fee_currency).strip().upper())
        return _build_usdt_price_snapshot(client, currencies) if currencies else {}

    @staticmethod
    def _build_order_usdt_prices(client: OkxRestClient, items: list[OkxTradeOrderItem]) -> dict[str, Decimal]:
        currencies = {
            str(item.fee_currency).strip().upper()
            for item in items
            if item.fee is not None and str(item.fee_currency or "").strip()
        }
        return _build_usdt_price_snapshot(client, currencies) if currencies else {}

    @staticmethod
    def _build_position_instrument_map(client: OkxRestClient, items: list[OkxPositionHistoryItem]) -> dict[str, Instrument]:
        result: dict[str, Instrument] = {}
        for inst_id in sorted({item.inst_id for item in items if item.inst_id}):
            try:
                result[inst_id] = client.get_instrument(inst_id, prefer_cached=True)
            except Exception:
                continue
        return result

    @staticmethod
    def _build_position_usdt_prices(client: OkxRestClient, items: list[OkxPositionHistoryItem]) -> dict[str, Decimal]:
        currencies: set[str] = set()
        for item in items:
            if str(item.inst_type or "").strip().upper() == "OPTION" and item.inst_id:
                currencies.add(item.inst_id.split("-", 1)[0].strip().upper())
            if item.pnl is not None:
                currencies.add(_infer_position_history_pnl_currency(item))
            if item.realized_pnl is not None:
                currencies.add(_infer_position_history_pnl_currency(item))
            if item.fee is not None and item.fee_currency:
                currencies.add(str(item.fee_currency).strip().upper())
        return _build_usdt_price_snapshot(client, currencies) if currencies else {}


class HistorySyncManager(QObject):
    payload_ready = Signal(str, str, str, object)
    status_changed = Signal(str, str, str, str)
    sync_started = Signal(str, str, bool)
    sync_finished = Signal(str, str, object)
    sync_failed = Signal(str, str, str, str)

    def __init__(self) -> None:
        super().__init__()
        self._threads: dict[tuple[str, str], HistorySyncThread] = {}

    def is_running(self, *, profile_name: str, environment: str) -> bool:
        thread = self._threads.get((str(profile_name or "").strip(), str(environment or "").strip()))
        return bool(thread is not None and thread.isRunning())

    def request_sync(
        self,
        *,
        runtime: ArbitrageTradeRuntime | None,
        profile_name: str,
        sources: Iterable[str] = HISTORY_SYNC_SOURCES,
        deep: bool = False,
        display_limits: dict[str, int] | None = None,
    ) -> bool:
        if runtime is None:
            return False
        key = (str(profile_name or "").strip(), str(getattr(runtime, "environment", "") or "").strip())
        if not key[0] or self.is_running(profile_name=key[0], environment=key[1]):
            return False
        normalized_sources = tuple(dict.fromkeys(str(source).strip().lower() for source in sources))
        normalized_sources = tuple(source for source in normalized_sources if source in HISTORY_SYNC_SOURCES)
        if not normalized_sources:
            return False
        thread = HistorySyncThread(
            runtime,
            profile_name=key[0],
            sources=normalized_sources,
            deep=deep,
            display_limits=display_limits or {"fills": 100, "orders": 200, "positions": 300},
        )
        thread.payload_ready.connect(
            lambda source, payload, key=key: self.payload_ready.emit(key[0], key[1], source, payload)
        )
        thread.progress.connect(
            lambda source, message, key=key: self.status_changed.emit(key[0], key[1], source, message)
        )
        thread.failed.connect(
            lambda source, message, key=key: self.sync_failed.emit(key[0], key[1], source, message)
        )
        thread.completed.connect(
            lambda completed, key=key: self.sync_finished.emit(key[0], key[1], completed)
        )
        thread.finished.connect(lambda key=key, thread=thread: self._clear_thread(key, thread))
        self._threads[key] = thread
        self.sync_started.emit(key[0], key[1], bool(deep))
        thread.start()
        return True

    def stop(self, *, profile_name: str, environment: str) -> None:
        thread = self._threads.get((str(profile_name or "").strip(), str(environment or "").strip()))
        if thread is not None:
            thread.stop()

    def shutdown(self) -> None:
        threads = list(self._threads.values())
        for thread in threads:
            thread.stop()
        for thread in threads:
            if thread.isRunning():
                thread.wait(2500)

    def _clear_thread(self, key: tuple[str, str], thread: HistorySyncThread) -> None:
        if self._threads.get(key) is not thread:
            return
        self._threads.pop(key, None)
        thread.deleteLater()


_HISTORY_SYNC_MANAGER: HistorySyncManager | None = None


def get_history_sync_manager() -> HistorySyncManager:
    global _HISTORY_SYNC_MANAGER
    if _HISTORY_SYNC_MANAGER is None:
        _HISTORY_SYNC_MANAGER = HistorySyncManager()
    return _HISTORY_SYNC_MANAGER
