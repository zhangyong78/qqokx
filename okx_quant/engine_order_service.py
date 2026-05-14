from __future__ import annotations

from datetime import datetime
from decimal import Decimal
import inspect
from typing import TYPE_CHECKING, Callable, Literal

from okx_quant.models import Credentials, Instrument, StrategyConfig
from okx_quant.okx_client import OkxApiError, OkxOrderResult

if TYPE_CHECKING:
    from okx_quant.engine import StrategyEngine


class EngineOrderService:
    def __init__(self, engine: StrategyEngine) -> None:
        self._engine = engine

    def next_client_order_id(self, *, role: str) -> str:
        engine = self._engine
        engine._order_ref_counter += 1
        session_token = "".join(ch for ch in engine._session_id.lower() if ch.isascii() and ch.isalnum())[:4] or "sess"
        strategy_token = "".join(ch for ch in engine._strategy_name.lower() if ch.isascii() and ch.isalnum())[:4] or "stg"
        role_token = "".join(ch for ch in role.lower() if ch.isascii() and ch.isalnum())[:3] or "ord"
        timestamp = datetime.utcnow().strftime("%m%d%H%M%S%f")[:-3]
        suffix = f"{engine._order_ref_counter % 100:02d}"
        return f"{session_token}{strategy_token}{role_token}{timestamp}{suffix}"[:32]

    def submit_order_with_recovery(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_id: str,
        cl_ord_id: str,
        label: str,
        submit_fn: Callable[[], OkxOrderResult],
    ) -> OkxOrderResult:
        from okx_quant import engine as engine_module

        engine = self._engine
        try:
            return submit_fn()
        except OkxApiError as exc:
            if not engine_module._is_transient_okx_error(exc):
                detail = str(exc).strip() or f"code={exc.code or '-'}"
                raise RuntimeError(
                    engine._build_okx_write_failure_message(
                        label=label,
                        inst_id=inst_id,
                        cl_ord_id=cl_ord_id,
                        detail=detail,
                        code=exc.code,
                    )
                ) from exc
            detail = str(exc).strip() or f"code={exc.code or '-'}"
            engine._logger(
                " | ".join(
                    [
                        "OKX 下单请求响应异常，开始回查订单状态",
                        f"操作={label}",
                        f"标的={inst_id}",
                        f"clOrdId={cl_ord_id}",
                        detail,
                    ]
                )
            )
            recovered = engine._recover_submitted_order_result(
                credentials,
                config,
                inst_id=inst_id,
                cl_ord_id=cl_ord_id,
                label=label,
            )
            if recovered is not None:
                engine._logger(
                    " | ".join(
                        [
                            "OKX 下单响应丢失，但回查确认委托已落地",
                            f"操作={label}",
                            f"标的={inst_id}",
                            f"ordId={recovered.ord_id or '-'}",
                            f"clOrdId={cl_ord_id}",
                        ]
                    )
                )
                return recovered

            if engine._stop_event.is_set():
                raise RuntimeError(
                    f"OKX {label}请求中断，且回查未确认订单状态 | clOrdId={cl_ord_id}"
                ) from exc

            engine._logger(
                " | ".join(
                    [
                        "OKX 下单回查未确认，准备使用同一 clOrdId 补发一次",
                        f"操作={label}",
                        f"标的={inst_id}",
                        f"clOrdId={cl_ord_id}",
                    ]
                )
            )
            try:
                return submit_fn()
            except OkxApiError as retry_exc:
                recovered = engine._recover_submitted_order_result(
                    credentials,
                    config,
                    inst_id=inst_id,
                    cl_ord_id=cl_ord_id,
                    label=label,
                )
                if recovered is not None:
                    engine._logger(
                        " | ".join(
                            [
                                "OKX 下单补发响应异常，但回查确认委托已落地",
                                f"操作={label}",
                                f"标的={inst_id}",
                                f"ordId={recovered.ord_id or '-'}",
                                f"clOrdId={cl_ord_id}",
                            ]
                        )
                    )
                    return recovered
                detail = str(retry_exc).strip() or f"code={retry_exc.code or '-'}"
                raise RuntimeError(
                    engine._build_okx_write_failure_message(
                        label=f"{label}失败且回查未确认订单状态",
                        inst_id=inst_id,
                        cl_ord_id=cl_ord_id,
                        detail=detail,
                        code=retry_exc.code,
                    )
                ) from retry_exc

    def place_entry_order(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        trade_instrument: Instrument,
        side: Literal["buy", "sell"],
        size: Decimal,
        pos_side: Literal["long", "short"] | None,
        *,
        cl_ord_id: str | None = None,
        label: str = "开仓报单",
    ) -> OkxOrderResult:
        engine = self._engine
        resolved_cl_ord_id = cl_ord_id or engine._next_client_order_id(role="entry")
        if trade_instrument.inst_type == "OPTION":
            return engine._submit_order_with_recovery(
                credentials,
                config,
                inst_id=trade_instrument.inst_id,
                cl_ord_id=resolved_cl_ord_id,
                label=label,
                submit_fn=lambda: engine._client.place_aggressive_limit_order(
                    credentials,
                    config,
                    trade_instrument,
                    side=side,
                    size=size,
                    pos_side=pos_side,
                    cl_ord_id=resolved_cl_ord_id,
                ),
            )
        return engine._submit_order_with_recovery(
            credentials,
            config,
            inst_id=trade_instrument.inst_id,
            cl_ord_id=resolved_cl_ord_id,
            label=label,
            submit_fn=lambda: self._place_simple_order_compat(
                credentials,
                config,
                inst_id=trade_instrument.inst_id,
                side=side,
                size=size,
                ord_type="market",
                pos_side=pos_side,
                cl_ord_id=resolved_cl_ord_id,
                reduce_only=label == "平仓报单",
            ),
        )

    def place_exit_order(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        trade_instrument: Instrument,
        side: Literal["buy", "sell"],
        size: Decimal,
        pos_side: Literal["long", "short"] | None,
    ) -> OkxOrderResult:
        engine = self._engine
        return engine._place_entry_order(
            credentials,
            config,
            trade_instrument,
            side,
            size,
            pos_side,
            cl_ord_id=engine._next_client_order_id(role="exit"),
            label="平仓报单",
        )

    def _place_simple_order_compat(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_id: str,
        side: str,
        size: Decimal,
        ord_type: str,
        pos_side: Literal["long", "short"] | None,
        cl_ord_id: str,
        reduce_only: bool,
    ) -> OkxOrderResult:
        place_simple_order = self._engine._client.place_simple_order
        kwargs = {
            "inst_id": inst_id,
            "side": side,
            "size": size,
            "ord_type": ord_type,
            "pos_side": pos_side,
            "cl_ord_id": cl_ord_id,
        }
        try:
            parameters = inspect.signature(place_simple_order).parameters
        except (TypeError, ValueError):
            parameters = {}
        if "reduce_only" in parameters:
            kwargs["reduce_only"] = reduce_only
        return place_simple_order(credentials, config, **kwargs)
