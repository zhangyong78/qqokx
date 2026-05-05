from __future__ import annotations

from typing import TYPE_CHECKING

from okx_quant.models import Credentials, StrategyConfig
from okx_quant.strategy_catalog import (
    STRATEGY_EMA5_EMA8_ID,
    is_dynamic_strategy_id,
    is_ema_atr_breakout_strategy,
)

if TYPE_CHECKING:
    from okx_quant.engine import StrategyEngine


class EngineStrategyRouter:
    def __init__(self, engine: StrategyEngine) -> None:
        self._engine = engine

    def run(self, credentials: Credentials, config: StrategyConfig) -> None:
        from okx_quant import engine as engine_module

        engine = self._engine
        try:
            signal_instrument = engine._get_instrument_with_retry(config.inst_id)
            if signal_instrument.state.lower() != "live":
                raise RuntimeError(f"{signal_instrument.inst_id} 当前不可交易，状态：{signal_instrument.state}")

            if config.run_mode == "signal_only":
                if is_dynamic_strategy_id(config.strategy_id):
                    engine._run_dynamic_signal_only_v2(config, signal_instrument)
                elif is_ema_atr_breakout_strategy(config.strategy_id):
                    engine._run_cross_signal_only(config, signal_instrument)
                elif config.strategy_id == STRATEGY_EMA5_EMA8_ID:
                    engine._run_ema5_ema8_signal_only(config, signal_instrument)
                else:
                    raise RuntimeError(f"未知策略：{config.strategy_id}")

                return

            trade_inst_id = engine_module.resolve_trade_inst_id(config)
            trade_instrument = engine._get_instrument_with_retry(trade_inst_id)
            if trade_instrument.state.lower() != "live":
                raise RuntimeError(f"{trade_instrument.inst_id} 当前不可交易，状态：{trade_instrument.state}")
            if trade_instrument.inst_type == "SPOT":
                raise RuntimeError("当前版本只支持永续或期权下单，现货暂时仅支持作为触发价格来源")

            engine_module.validate_entry_side_mode_support(config)
            if is_dynamic_strategy_id(config.strategy_id):
                if engine_module.can_use_exchange_managed_orders(config, signal_instrument, trade_instrument):
                    engine._run_dynamic_exchange_strategy(credentials, config, trade_instrument)
                else:
                    engine._run_dynamic_local_strategy_v2(credentials, config, signal_instrument, trade_instrument)
            elif is_ema_atr_breakout_strategy(config.strategy_id):
                if engine_module.can_use_exchange_managed_orders(config, signal_instrument, trade_instrument):
                    engine._run_cross_exchange_strategy(credentials, config, signal_instrument)
                else:
                    engine._run_cross_local_strategy(credentials, config, signal_instrument, trade_instrument)
            elif config.strategy_id == STRATEGY_EMA5_EMA8_ID:
                engine._run_ema5_ema8_local_strategy(credentials, config, signal_instrument, trade_instrument)
            else:
                raise RuntimeError(f"未知策略：{config.strategy_id}")
        except Exception as exc:
            engine._notify_error(config, str(exc))
            engine._logger(f"策略停止，原因：{exc}")
        finally:
            engine._stop_event.set()
