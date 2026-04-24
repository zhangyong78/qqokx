from __future__ import annotations

from okx_quant.models import StrategyConfig


def is_strategy_pool_config(config: StrategyConfig) -> bool:
    return bool(config.backtest_profile_id)


def strategy_pool_profile_name(config: StrategyConfig) -> str:
    if config.backtest_profile_name:
        return config.backtest_profile_name
    if config.backtest_profile_id:
        return config.backtest_profile_id
    return "未命名候选"
