from __future__ import annotations

from decimal import Decimal
from typing import Callable, TYPE_CHECKING, TypeVar

from okx_quant.models import Credentials, Instrument, StrategyConfig
from okx_quant.okx_client import OkxApiError, OkxOrderStatus, OkxPosition, OkxTradeOrderItem

if TYPE_CHECKING:
    from okx_quant.engine import StrategyEngine

T = TypeVar("T")


class EngineRetryPolicy:
    def __init__(self, engine: StrategyEngine) -> None:
        self._engine = engine

    def call_okx_read_with_retry(self, label: str, fn: Callable[[], T]) -> T:
        from okx_quant import engine as engine_module

        engine = self._engine
        last_exc: OkxApiError | None = None
        max_attempts, base_delay, max_delay = engine_module.get_okx_read_retry_config()
        for attempt in range(1, max_attempts + 1):
            try:
                return fn()
            except Exception as exc:
                okx_exc = engine_module._coerce_okx_read_exception(exc)
                if okx_exc is None:
                    raise
                last_exc = okx_exc
                detail = str(okx_exc).strip() or f"code={okx_exc.code or '-'}"
                if (
                    not engine_module._is_transient_okx_error(okx_exc)
                    or attempt >= max_attempts
                    or engine._stop_event.is_set()
                ):
                    engine._logger(f"OKX 读取失败 | 操作={label} | {detail}")
                    raise okx_exc
                engine._logger(
                    " | ".join(
                        [
                            "OKX 读取异常，准备重试",
                            f"操作={label}",
                            f"第{attempt}/{max_attempts}次",
                            detail,
                        ]
                    )
                )
                delay_seconds = min(base_delay * attempt, max_delay)
                engine._stop_event.wait(delay_seconds)
        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"OKX 读取失败：{label}")

    def get_instrument(self, inst_id: str) -> Instrument:
        return self.call_okx_read_with_retry(
            f"读取标的 {inst_id}",
            lambda: self._engine._client.get_instrument(inst_id),
        )

    def get_candles(self, inst_id: str, bar: str, *, limit: int) -> list:
        return self.call_okx_read_with_retry(
            f"读取K线 {inst_id} {bar}",
            lambda: self._engine._client.get_candles(inst_id, bar, limit=limit),
        )

    def get_order(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_id: str,
        ord_id: str | None = None,
        cl_ord_id: str | None = None,
    ) -> OkxOrderStatus:
        key = ord_id or cl_ord_id or "-"
        return self.call_okx_read_with_retry(
            f"读取订单状态 {inst_id} {key}",
            lambda: self._engine._client.get_order(
                credentials,
                config,
                inst_id=inst_id,
                ord_id=ord_id,
                cl_ord_id=cl_ord_id,
            ),
        )

    def get_trigger_price(self, inst_id: str, price_type: str) -> Decimal:
        """与 `OkxRestClient.get_trigger_price` 对齐：`mark` 在 ticker 缺字段时会回退到 public mark-price。"""
        pt = (price_type or "last").strip().lower()
        if pt in {"bid", "ask"}:
            # 客户端 `get_trigger_price` 未封装 bid/ask，仍走 ticker 单次读取
            def _read_ba() -> Decimal:
                ticker = self._engine._client.get_ticker(inst_id)
                raw = ticker.bid if pt == "bid" else ticker.ask
                if raw is None:
                    raise OkxApiError(f"OKX 未返回有效触发价：{inst_id} type={price_type}")
                return raw

            return self.call_okx_read_with_retry(f"读取触发价格 {inst_id} {price_type}", _read_ba)
        return self.call_okx_read_with_retry(
            f"读取触发价格 {inst_id} {price_type}",
            lambda: self._engine._client.get_trigger_price(inst_id, pt),  # type: ignore[arg-type]
        )

    def get_pending_orders(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_types: tuple[str, ...],
        limit: int,
    ) -> list[OkxTradeOrderItem]:
        return self.call_okx_read_with_retry(
            "读取当前委托",
            lambda: self._engine._client.get_pending_orders(
                credentials,
                environment=config.environment,
                inst_types=inst_types,
                limit=limit,
            ),
        )

    def get_positions(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_type: str | None = None,
    ) -> list[OkxPosition]:
        return self.call_okx_read_with_retry(
            "读取持仓",
            lambda: self._engine._client.get_positions(
                credentials,
                environment=config.environment,
                inst_type=inst_type,
            ),
        )
