from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Callable

from okx_quant.arbitrage.arbitrage_executor import ArbitrageExecutor, ArbitrageOpenRequest, ArbitrageOpenResult
from okx_quant.arbitrage.basis_calculator import mid_price
from okx_quant.arbitrage.models import ArbitrageTradeRuntime
from okx_quant.okx_client import OkxRestClient

Logger = Callable[[str], None]
SessionUpdateCallback = Callable[[object | None], None]
MonitorPollSeconds = 2.0
PublicWsWaitSliceSeconds = 0.5


@dataclass
class ArbitrageAutoOpenSession:
    request: ArbitrageOpenRequest
    runtime: ArbitrageTradeRuntime
    status: str = "监控中"
    last_spread_pct: Decimal | None = None
    last_spread_abs: Decimal | None = None
    last_spot_mid: Decimal | None = None
    last_deriv_mid: Decimal | None = None
    triggered: bool = False
    result: ArbitrageOpenResult | None = None


class ArbitrageAutoOpenService:
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
        self._session: ArbitrageAutoOpenSession | None = None

    def set_status_callback(self, callback: SessionUpdateCallback | None) -> None:
        self._status_callback = callback or (lambda _session: None)

    def _notify_session_update(self, session: ArbitrageAutoOpenSession | None = None) -> None:
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
    def session(self) -> ArbitrageAutoOpenSession | None:
        with self._lock:
            return self._session

    def start(self, request: ArbitrageOpenRequest, runtime: ArbitrageTradeRuntime) -> None:
        with self._lock:
            if self.is_running:
                raise RuntimeError("已有自动开仓任务在运行，请先停止。")
            self._stop_event.clear()
            self._session = ArbitrageAutoOpenSession(request=request, runtime=runtime)
            self._thread = threading.Thread(target=self._run, name="arbitrage-auto-open", daemon=True)
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
        self._logger("自动开仓监控已启动。")
        while not self._stop_event.is_set():
            session = self.session
            if session is None:
                return
            try:
                should_trigger, market_version = self._refresh_quote(session)
                if should_trigger:
                    session.status = "条件满足，正在开仓…"
                    self._notify_session_update(session)
                    spread_text = (
                        f"价差率 {session.last_spread_pct:.4f}% | 绝对价差 {session.last_spread_abs:.6f}"
                        if session.last_spread_pct is not None and session.last_spread_abs is not None
                        else f"价差率 {session.last_spread_pct!s}%"
                    )
                    self._logger(
                        f"触发自动开仓：当前{spread_text} "
                        f"(现货 {session.last_spot_mid!s} / 衍生品 {session.last_deriv_mid!s})"
                    )
                    session.triggered = True
                    result = self._executor.open_cash_and_carry(session.request, runtime=session.runtime)
                    session.result = result
                    session.status = "已完成" if result.success else f"失败：{result.message}"
                    self._logger(session.status)
                    self._notify_session_update(session)
                    return
            except Exception as exc:
                session.status = f"异常：{exc}"
                self._logger(f"自动开仓监控异常：{exc}")
                self._notify_session_update(session)
                return
            self._wait_public_market_update_or_sleep(
                inst_ids=(session.request.spot_inst_id, session.request.derivative_inst_id),
                environment=session.runtime.environment,
                after_version=market_version,
            )
        session = self.session
        if session is not None and not session.triggered:
            session.status = "已停止"
            self._notify_session_update(session)

    def _refresh_quote(self, session: ArbitrageAutoOpenSession) -> tuple[bool, int | None]:
        runtime = getattr(session, "runtime", None)
        environment = getattr(runtime, "environment", "demo")
        spot_version, spot_ticker = self._get_live_ticker_with_version(
            session.request.spot_inst_id,
            environment=environment,
        )
        deriv_version, deriv_ticker = self._get_live_ticker_with_version(
            session.request.derivative_inst_id,
            environment=environment,
        )
        spot_mid = mid_price(spot_ticker.bid, spot_ticker.ask)
        deriv_mid = mid_price(deriv_ticker.bid, deriv_ticker.ask)
        if spot_mid is None or deriv_mid is None or spot_mid <= 0:
            session.status = "等待有效报价…"
            self._notify_session_update(session)
            return False, None
        spread_abs = deriv_mid - spot_mid
        spread_pct = (deriv_mid - spot_mid) / spot_mid * Decimal("100")
        session.last_spot_mid = spot_mid
        session.last_deriv_mid = deriv_mid
        session.last_spread_pct = spread_pct
        session.last_spread_abs = spread_abs
        session.status = f"监控中 | 价差率 {spread_pct:.4f}% | 绝对价差 {spread_abs:.6f}"
        self._notify_session_update(session)
        request = session.request
        versions = [version for version in (spot_version, deriv_version) if version is not None]
        market_version = max(versions) if versions else None

        if request.trigger_mode == "spread":
            if request.open_spread_pct_max is None:
                return False, market_version
            return spread_pct <= request.open_spread_pct_max, market_version
        if request.trigger_mode == "spread_abs":
            if request.open_spread_abs_max is None:
                return False, market_version
            return spread_abs >= request.open_spread_abs_max, market_version

        spot_ok = True
        deriv_ok = True
        if request.spot_limit_price is not None and request.spot_limit_price > 0:
            ask = spot_ticker.ask
            spot_ok = ask is not None and ask > 0 and ask <= request.spot_limit_price
        if request.derivative_limit_price is not None and request.derivative_limit_price > 0:
            bid = deriv_ticker.bid
            deriv_ok = bid is not None and bid > 0 and bid >= request.derivative_limit_price
        return spot_ok and deriv_ok, market_version

    def _should_trigger(self, session: ArbitrageAutoOpenSession) -> bool:
        should_trigger, _market_version = self._refresh_quote(session)
        return should_trigger
