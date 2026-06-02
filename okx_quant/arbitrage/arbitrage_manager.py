from __future__ import annotations

from decimal import Decimal
from typing import Callable

from okx_quant.arbitrage.arbitrage_auto_close import ArbitrageAutoCloseService
from okx_quant.arbitrage.arbitrage_auto_open import ArbitrageAutoOpenService
from okx_quant.arbitrage.arbitrage_executor import (
    ArbitrageCloseRequest,
    ArbitrageCloseResult,
    ArbitrageExecutor,
    ArbitrageOpenRequest,
    ArbitrageOpenResult,
    ArbitrageRollRequest,
    ArbitrageRollResult,
)
from okx_quant.arbitrage.arbitrage_scanner import ArbitrageScanner
from okx_quant.arbitrage.models import (
    ArbitrageOpportunity,
    ArbitrageRuntimeConfig,
    ArbitrageSizePreview,
    ArbitrageTradeRuntime,
    SizeUnit,
)
from okx_quant.arbitrage.position_ledger import load_ledger_entries, load_open_ledger_entries
from okx_quant.arbitrage.size_converter import preview_arbitrage_size
from okx_quant.models import Instrument
from okx_quant.okx_client import OkxApiError, OkxRestClient

Logger = Callable[[str], None]


class ArbitrageManager:
    def __init__(
        self,
        client: OkxRestClient,
        *,
        config: ArbitrageRuntimeConfig | None = None,
        logger: Logger | None = None,
    ) -> None:
        self._client = client
        self._config = config or ArbitrageRuntimeConfig()
        self._logger = logger or (lambda _message: None)
        self._scanner = ArbitrageScanner(client, config=self._config)
        self._executor = ArbitrageExecutor(client, logger=self._logger)
        self._auto_open = ArbitrageAutoOpenService(client, executor=self._executor, logger=self._logger)
        self._auto_close = ArbitrageAutoCloseService(client, executor=self._executor, logger=self._logger)
        self._last_opportunities: list[ArbitrageOpportunity] = []

    @property
    def config(self) -> ArbitrageRuntimeConfig:
        return self._config

    @property
    def auto_open(self) -> ArbitrageAutoOpenService:
        return self._auto_open

    @property
    def auto_close(self) -> ArbitrageAutoCloseService:
        return self._auto_close

    @property
    def last_opportunities(self) -> tuple[ArbitrageOpportunity, ...]:
        return tuple(self._last_opportunities)

    def scan_opportunities(
        self,
        *,
        include_swap: bool = True,
        include_futures: bool = True,
    ) -> list[ArbitrageOpportunity]:
        self._logger("套利扫描：开始拉取现货/永续/交割行情…")
        rows = self._scanner.scan(include_swap=include_swap, include_futures=include_futures)
        self._last_opportunities = rows
        self._logger(f"套利扫描：完成，共 {len(rows)} 条机会。")
        return rows

    def load_ledger(self):
        return load_ledger_entries()

    def load_open_ledger(self):
        return load_open_ledger_entries()

    def preview_size(
        self,
        *,
        base_ccy: str,
        derivative_inst_id: str,
        size: Decimal,
        unit: SizeUnit,
    ) -> ArbitrageSizePreview:
        spot_inst_id = f"{base_ccy}-USDT"
        spot_inst = self._client.get_instrument(spot_inst_id)
        swap_inst = self._client.get_instrument(derivative_inst_id)
        ticker = self._client.get_ticker(spot_inst_id)
        from okx_quant.arbitrage.basis_calculator import mid_price

        spot_mid = mid_price(ticker.bid, ticker.ask)
        if spot_mid is None:
            raise OkxApiError(f"无法获取 {spot_inst_id} 的有效中间价")
        return preview_arbitrage_size(
            size=size,
            unit=unit,
            spot_mid=spot_mid,
            spot_instrument=spot_inst,
            swap_instrument=swap_inst,
        )

    def open_now(self, request: ArbitrageOpenRequest, *, runtime: ArbitrageTradeRuntime) -> ArbitrageOpenResult:
        return self._executor.open_cash_and_carry(request, runtime=runtime)

    def close_now(self, request: ArbitrageCloseRequest, *, runtime: ArbitrageTradeRuntime) -> ArbitrageCloseResult:
        return self._executor.close_cash_and_carry(request, runtime=runtime)

    def roll_now(self, request: ArbitrageRollRequest, *, runtime: ArbitrageTradeRuntime) -> ArbitrageRollResult:
        return self._executor.roll_cash_and_carry(request, runtime=runtime)

    def start_auto_open(self, request: ArbitrageOpenRequest, *, runtime: ArbitrageTradeRuntime) -> None:
        self._auto_open.start(request, runtime)

    def stop_auto_open(self) -> None:
        self._auto_open.stop()

    def start_auto_close(
        self,
        *,
        request: ArbitrageCloseRequest,
        runtime: ArbitrageTradeRuntime,
        close_trigger_mode: str,
        close_spread_pct_min: Decimal | None,
        close_spread_abs_min: Decimal | None,
        entry_id: str | None = None,
    ) -> None:
        self._auto_close.start(
            request=request,
            runtime=runtime,
            close_trigger_mode=close_trigger_mode,
            close_spread_pct_min=close_spread_pct_min,
            close_spread_abs_min=close_spread_abs_min,
            entry_id=entry_id,
        )

    def stop_auto_close(self) -> None:
        self._auto_close.stop()

    def get_instrument(self, inst_id: str) -> Instrument:
        return self._client.get_instrument(inst_id)
