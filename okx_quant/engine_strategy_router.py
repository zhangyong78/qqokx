from __future__ import annotations

from typing import TYPE_CHECKING

from okx_quant.models import Credentials, StrategyConfig
from okx_quant.strategy_runtime_registry import get_strategy_runtime_profile

if TYPE_CHECKING:
    from okx_quant.engine import StrategyEngine


class EngineStrategyRouter:
    def __init__(self, engine: StrategyEngine) -> None:
        self._engine = engine

    def run(self, credentials: Credentials, config: StrategyConfig) -> None:
        from okx_quant import engine as engine_module

        engine = self._engine
        try:
            profile = get_strategy_runtime_profile(config.strategy_id)
            signal_instrument = engine._get_instrument_with_retry(config.inst_id)
            if signal_instrument.state.lower() != "live":
                raise RuntimeError(f"{signal_instrument.inst_id} 当前不可交易，状态：{signal_instrument.state}")

            if config.run_mode == "signal_only":
                getattr(engine, profile.signal_only_handler)(config, signal_instrument)
                return

            trade_inst_id = engine_module.resolve_trade_inst_id(config)
            trade_instrument = engine._get_instrument_with_retry(trade_inst_id)
            if trade_instrument.state.lower() != "live":
                raise RuntimeError(f"{trade_instrument.inst_id} 当前不可交易，状态：{trade_instrument.state}")
            if trade_instrument.inst_type == "SPOT":
                raise RuntimeError("当前版本只支持永续或期权下单，现货暂时仅支持作为触发价格来源")

            engine_module.validate_entry_side_mode_support(config)
            if profile.supports_exchange_trade and engine_module.can_use_exchange_managed_orders(
                config,
                signal_instrument,
                trade_instrument,
            ):
                exchange_instrument = signal_instrument
                if profile.exchange_trade_instrument_role == "trade":
                    exchange_instrument = trade_instrument
                getattr(engine, profile.exchange_trade_handler)(credentials, config, exchange_instrument)
            else:
                getattr(engine, profile.local_trade_handler)(
                    credentials,
                    config,
                    signal_instrument,
                    trade_instrument,
                )
        except Exception as exc:
            engine._notify_error(config, str(exc))
            engine._logger(f"策略停止，原因：{exc}")
        finally:
            engine._stop_event.set()
