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
MonitorPollSeconds = 2.0


@dataclass
class ArbitrageAutoCloseSession:
    request: ArbitrageCloseRequest
    runtime: ArbitrageTradeRuntime
    close_spread_pct_min: Decimal
    entry_id: str | None
    status: str = "监控中"
    last_spread_pct: Decimal | None = None
    triggered: bool = False
    result: ArbitrageCloseResult | None = None


class ArbitrageAutoCloseService:
    def __init__(
        self,
        client: OkxRestClient,
        *,
        executor: ArbitrageExecutor | None = None,
        logger: Logger | None = None,
    ) -> None:
        self._client = client
        self._executor = executor or ArbitrageExecutor(client, logger=logger)
        self._logger = logger or (lambda _message: None)
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._session: ArbitrageAutoCloseSession | None = None

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
        close_spread_pct_min: Decimal,
        entry_id: str | None = None,
    ) -> None:
        with self._lock:
            if self.is_running:
                raise RuntimeError("已有自动平仓任务在运行，请先停止。")
            self._stop_event.clear()
            self._session = ArbitrageAutoCloseSession(
                request=request,
                runtime=runtime,
                close_spread_pct_min=close_spread_pct_min,
                entry_id=entry_id,
            )
            self._thread = threading.Thread(target=self._run, name="arbitrage-auto-close", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        with self._lock:
            if self._session is not None and not self._session.triggered:
                self._session.status = "已停止"
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
                spread_pct = self._refresh_spread(session)
                if spread_pct is not None and spread_pct >= session.close_spread_pct_min:
                    session.status = "条件满足，正在平仓…"
                    session.triggered = True
                    self._logger(f"触发自动平仓：当前价差 {spread_pct:.4f}% >= {session.close_spread_pct_min}%")
                    result = self._executor.close_cash_and_carry(session.request, runtime=session.runtime)
                    session.result = result
                    session.status = "已完成" if result.success else f"失败：{result.message}"
                    self._logger(session.status)
                    return
            except Exception as exc:
                session.status = f"异常：{exc}"
                self._logger(f"自动平仓监控异常：{exc}")
                return
            time.sleep(MonitorPollSeconds)
        session = self.session
        if session is not None and not session.triggered:
            session.status = "已停止"

    def _refresh_spread(self, session: ArbitrageAutoCloseSession) -> Decimal | None:
        entry = self._resolve_target_entry(session.entry_id)
        if entry is None:
            session.status = "没有可平仓持仓"
            return None
        spot_ticker = self._client.get_ticker(entry.spot_inst_id)
        deriv_ticker = self._client.get_ticker(entry.derivative_inst_id)
        spot_mid = mid_price(spot_ticker.bid, spot_ticker.ask)
        deriv_mid = mid_price(deriv_ticker.bid, deriv_ticker.ask)
        if spot_mid is None or deriv_mid is None or spot_mid <= 0:
            session.status = "等待有效报价…"
            return None
        spread_pct = (deriv_mid - spot_mid) / spot_mid * Decimal("100")
        session.last_spread_pct = spread_pct
        session.status = f"监控中 | 价差 {spread_pct:.4f}%"
        return spread_pct

    def _resolve_target_entry(self, entry_id: str | None):
        if entry_id:
            entry = find_ledger_entry(entry_id)
            return entry if entry is not None and entry.close_mode == "open" else None
        open_entries = load_open_ledger_entries()
        return open_entries[0] if open_entries else None
