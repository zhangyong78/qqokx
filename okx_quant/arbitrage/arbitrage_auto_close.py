from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Callable

from okx_quant.arbitrage.arbitrage_executor import ArbitrageCloseRequest, ArbitrageCloseResult, ArbitrageExecutor
from okx_quant.arbitrage.basis_calculator import mid_price
from okx_quant.arbitrage.models import ArbitrageTradeRuntime
from okx_quant.arbitrage.position_ledger import find_ledger_entry, load_open_ledger_entries
from okx_quant.okx_client import OkxRestClient

Logger = Callable[[str], None]
SessionUpdateCallback = Callable[[object | None], None]
MonitorPollSeconds = 2.0
PublicWsWaitSliceSeconds = 0.5


@dataclass
class ArbitrageAutoCloseSession:
    request: ArbitrageCloseRequest
    runtime: ArbitrageTradeRuntime
    close_trigger_mode: str
    close_spread_pct_min: Decimal | None
    close_spread_abs_min: Decimal | None
    entry_id: str | None
    status: str = "监控中"
    last_spread_pct: Decimal | None = None
    last_spread_abs: Decimal | None = None
    triggered: bool = False
    result: ArbitrageCloseResult | None = None


class ArbitrageAutoCloseService:
    def __init__(
        self,
        client: OkxRestClient,
        *,
        executor: ArbitrageExecutor | None = None,
        logger: Logger | None = None,
        status_callback: SessionUpdateCallback | None = None,
    ) -> None:
        self._client = client
        self._executor = executor or ArbitrageExecutor(client, logger=logger)
        self._logger = logger or (lambda _message: None)
        self._status_callback = status_callback or (lambda _session: None)
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._session: ArbitrageAutoCloseSession | None = None

    def set_status_callback(self, callback: SessionUpdateCallback | None) -> None:
        self._status_callback = callback or (lambda _session: None)

    def _notify_session_update(self, session: ArbitrageAutoCloseSession | None = None) -> None:
        callback = self._status_callback
        if session is None:
            session = self.session
        try:
            callback(session)
        except Exception:
            pass

    def _get_live_ticker_with_version(self, inst_id: str, *, environment: str):
        ensure_watch = getattr(self._client, "ensure_public_ws_market_watch", None)
        if callable(ensure_watch):
            try:
                ensure_watch(inst_id, environment=environment)
            except Exception:
                pass
        get_cached = getattr(self._client, "get_cached_public_ticker", None)
        if callable(get_cached):
            try:
                payload = get_cached(inst_id, environment=environment)
            except Exception:
                payload = None
            if payload is not None:
                version, ticker = payload
                return version, ticker
        return None, self._client.get_ticker(inst_id)

    def _wait_public_market_update_or_sleep(
        self,
        *,
        inst_ids: tuple[str, ...],
        environment: str,
        after_version: int | None,
    ) -> None:
        deadline = time.monotonic() + MonitorPollSeconds
        if after_version is not None and after_version > 0:
            while not self._stop_event.is_set():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return
                wait_seconds = min(remaining, PublicWsWaitSliceSeconds)
                try:
                    version = self._client.wait_public_market_update(
                        inst_ids,
                        environment=environment,
                        after_version=after_version,
                        timeout=wait_seconds,
                    )
                except Exception:
                    version = None
                if version is not None:
                    return
            return
        self._stop_event.wait(MonitorPollSeconds)

    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._thread is not None and self._thread.is_alive()

    @property
    def session(self) -> ArbitrageAutoCloseSession | None:
        with self._lock:
            return self._session

    def start(
        self,
        *,
        request: ArbitrageCloseRequest,
        runtime: ArbitrageTradeRuntime,
        close_trigger_mode: str,
        close_spread_pct_min: Decimal | None,
        close_spread_abs_min: Decimal | None,
        entry_id: str | None = None,
    ) -> None:
        with self._lock:
            if self.is_running:
                raise RuntimeError("已有自动平仓任务在运行，请先停止。")
            self._stop_event.clear()
            self._session = ArbitrageAutoCloseSession(
                request=request,
                runtime=runtime,
                close_trigger_mode=close_trigger_mode,
                close_spread_pct_min=close_spread_pct_min,
                close_spread_abs_min=close_spread_abs_min,
                entry_id=entry_id,
            )
            self._thread = threading.Thread(target=self._run, name="arbitrage-auto-close", daemon=True)
            self._thread.start()
            session = self._session
        self._notify_session_update(session)

    def stop(self) -> None:
        self._stop_event.set()
        with self._lock:
            if self._session is not None and not self._session.triggered:
                self._session.status = "已停止"
                self._notify_session_update(self._session)
            thread = self._thread
        if thread is not None:
            thread.join(timeout=MonitorPollSeconds + 1.0)

    def _run(self) -> None:
        self._logger("自动平仓监控已启动。")
        while not self._stop_event.is_set():
            session = self.session
            if session is None:
                return
            try:
                spread_pct, spread_abs, market_version, inst_ids = self._refresh_spread_details(session)
                should_trigger = False
                if session.close_trigger_mode == "spread_abs":
                    should_trigger = spread_abs is not None and session.close_spread_abs_min is not None and spread_abs <= session.close_spread_abs_min
                else:
                    should_trigger = spread_pct is not None and session.close_spread_pct_min is not None and spread_pct <= session.close_spread_pct_min
                if should_trigger:
                    session.status = "条件满足，正在平仓…"
                    self._notify_session_update(session)
                    session.triggered = True
                    if session.close_trigger_mode == "spread_abs":
                        self._logger(
                            f"触发自动平仓：当前绝对价差 {spread_abs:.6f} <= {session.close_spread_abs_min}"
                        )
                    else:
                        self._logger(
                            f"触发自动平仓：当前价差率 {spread_pct:.4f}% <= {session.close_spread_pct_min}%"
                        )
                    result = self._executor.close_cash_and_carry(session.request, runtime=session.runtime)
                    session.result = result
                    session.status = "已完成" if result.success else f"失败：{result.message}"
                    self._logger(session.status)
                    self._notify_session_update(session)
                    return
            except Exception as exc:
                session.status = f"异常：{exc}"
                self._logger(f"自动平仓监控异常：{exc}")
                self._notify_session_update(session)
                return
            self._wait_public_market_update_or_sleep(
                inst_ids=inst_ids,
                environment=session.runtime.environment,
                after_version=market_version,
            )
        session = self.session
        if session is not None and not session.triggered:
            session.status = "已停止"
            self._notify_session_update(session)

    def _refresh_spread(
        self,
        session: ArbitrageAutoCloseSession,
    ) -> tuple[Decimal | None, Decimal | None]:
        spread_pct, spread_abs, _market_version, _inst_ids = self._refresh_spread_details(session)
        return spread_pct, spread_abs

    def _refresh_spread_details(
        self,
        session: ArbitrageAutoCloseSession,
    ) -> tuple[Decimal | None, Decimal | None, int | None, tuple[str, ...]]:
        entry = self._resolve_target_entry(session.entry_id)
        if entry is None:
            session.status = "没有可平仓持仓"
            self._notify_session_update(session)
            return None, None, None, ()
        inst_ids = (entry.spot_inst_id, entry.derivative_inst_id)
        spot_version, spot_ticker = self._get_live_ticker_with_version(
            entry.spot_inst_id,
            environment=session.runtime.environment,
        )
        deriv_version, deriv_ticker = self._get_live_ticker_with_version(
            entry.derivative_inst_id,
            environment=session.runtime.environment,
        )
        spot_mid = mid_price(spot_ticker.bid, spot_ticker.ask)
        deriv_mid = mid_price(deriv_ticker.bid, deriv_ticker.ask)
        if spot_mid is None or deriv_mid is None or spot_mid <= 0:
            session.status = "等待有效报价…"
            self._notify_session_update(session)
            return None, None, None, inst_ids
        spread_abs = deriv_mid - spot_mid
        spread_pct = (deriv_mid - spot_mid) / spot_mid * Decimal("100")
        session.last_spread_pct = spread_pct
        session.last_spread_abs = spread_abs
        session.status = f"监控中 | 价差率 {spread_pct:.4f}% | 绝对价差 {spread_abs:.6f}"
        self._notify_session_update(session)
        versions = [version for version in (spot_version, deriv_version) if version is not None]
        return spread_pct, spread_abs, (max(versions) if versions else None), inst_ids

    def _resolve_target_entry(self, entry_id: str | None):
        if entry_id:
            entry = find_ledger_entry(entry_id)
            return entry if entry is not None and entry.close_mode == "open" else None
        open_entries = load_open_ledger_entries()
        return open_entries[0] if open_entries else None
