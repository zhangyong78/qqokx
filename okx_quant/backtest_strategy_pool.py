from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal

from okx_quant.models import StrategyConfig
from okx_quant.strategy_catalog import is_slot_handoff_strategy_id


@dataclass(frozen=True)
class BacktestStrategyPoolPreset:
    profile_id: str
    name: str
    summary: str
    ema_period: int
    trend_ema_period: int
    atr_period: int
    atr_stop_multiplier: Decimal
    atr_take_multiplier: Decimal


SLOT_HANDOFF_5M_PRESETS: tuple[BacktestStrategyPoolPreset, ...] = (
    BacktestStrategyPoolPreset(
        profile_id="01_fast_breakout",
        name="快突破 9/21",
        summary="偏进攻，优先抓 5 分钟第一段加速，止盈也更快。",
        ema_period=9,
        trend_ema_period=21,
        atr_period=7,
        atr_stop_multiplier=Decimal("0.8"),
        atr_take_multiplier=Decimal("1.6"),
    ),
    BacktestStrategyPoolPreset(
        profile_id="02_momentum_drive",
        name="动量 13/34",
        summary="保留进攻性，同时比 9/21 更稳一点，适合连续趋势。",
        ema_period=13,
        trend_ema_period=34,
        atr_period=7,
        atr_stop_multiplier=Decimal("1.0"),
        atr_take_multiplier=Decimal("1.8"),
    ),
    BacktestStrategyPoolPreset(
        profile_id="03_balanced_trend",
        name="均衡 21/55",
        summary="过滤更稳，适合主升主跌段，是默认基准候选。",
        ema_period=21,
        trend_ema_period=55,
        atr_period=10,
        atr_stop_multiplier=Decimal("1.0"),
        atr_take_multiplier=Decimal("2.0"),
    ),
    BacktestStrategyPoolPreset(
        profile_id="04_slow_filter",
        name="慢过滤 21/89",
        summary="进一步削弱震荡噪音，信号更少但更偏向主趋势延续。",
        ema_period=21,
        trend_ema_period=89,
        atr_period=14,
        atr_stop_multiplier=Decimal("1.2"),
        atr_take_multiplier=Decimal("2.4"),
    ),
    BacktestStrategyPoolPreset(
        profile_id="05_fast_reaction",
        name="快反 8/34",
        summary="更激进，适合波动放大时快速参与，容错依赖人工池。",
        ema_period=8,
        trend_ema_period=34,
        atr_period=5,
        atr_stop_multiplier=Decimal("0.7"),
        atr_take_multiplier=Decimal("1.4"),
    ),
    BacktestStrategyPoolPreset(
        profile_id="06_deep_trend",
        name="深趋势 34/89",
        summary="只抓更明显的 5 分钟趋势段，适合过滤假突破。",
        ema_period=34,
        trend_ema_period=89,
        atr_period=14,
        atr_stop_multiplier=Decimal("1.0"),
        atr_take_multiplier=Decimal("2.6"),
    ),
)


def is_strategy_pool_config(config: StrategyConfig) -> bool:
    return is_slot_handoff_strategy_id(config.strategy_id) and bool(config.backtest_profile_id)


def strategy_pool_profile_name(config: StrategyConfig) -> str:
    if config.backtest_profile_name:
        return config.backtest_profile_name
    if config.backtest_profile_id:
        return config.backtest_profile_id
    return "未命名候选"


def build_slot_handoff_strategy_pool_configs(base_config: StrategyConfig) -> list[StrategyConfig]:
    if not is_slot_handoff_strategy_id(base_config.strategy_id):
        return []

    configs: list[StrategyConfig] = []
    for preset in SLOT_HANDOFF_5M_PRESETS:
        configs.append(
            replace(
                base_config,
                bar="5m",
                ema_period=preset.ema_period,
                trend_ema_period=preset.trend_ema_period,
                big_ema_period=0,
                atr_period=preset.atr_period,
                atr_stop_multiplier=preset.atr_stop_multiplier,
                atr_take_multiplier=preset.atr_take_multiplier,
                backtest_profile_id=preset.profile_id,
                backtest_profile_name=preset.name,
                backtest_profile_summary=preset.summary,
            )
        )
    return configs
