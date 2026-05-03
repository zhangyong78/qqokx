from __future__ import annotations

from dataclasses import dataclass

STRATEGY_DYNAMIC_ID = "ema_dynamic_order"
STRATEGY_DYNAMIC_LONG_ID = "ema_dynamic_order_long"
STRATEGY_DYNAMIC_SHORT_ID = "ema_dynamic_order_short"
STRATEGY_CROSS_ID = "ema_cross_market"
STRATEGY_EMA5_EMA8_ID = "ema5_ema8_cross_stop"


@dataclass(frozen=True)
class StrategyDefinition:
    strategy_id: str
    name: str
    summary: str
    rule_description: str
    parameter_hint: str
    default_signal_label: str
    allowed_signal_labels: tuple[str, ...]
    supports_trade: bool = True
    supports_signal_only: bool = False
    supports_backtest: bool = True
    supports_batch_observe: bool = False
    supports_trader_desk: bool = False


ALL_STRATEGY_DEFINITIONS: tuple[StrategyDefinition, ...] = (
    StrategyDefinition(
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        name="EMA 动态委托做多",
        summary="顺着 EMA 趋势方向挂单做多，适合趋势延续型短中线实验。",
        rule_description=(
            "当快 EMA 位于趋势 EMA 上方，且已收盘 K 线仍然站在趋势 EMA 上方时，"
            "下一根按挂单参考 EMA 尝试做多。"
        ),
        parameter_hint=(
            "可调参数主要是 EMA 周期、ATR 止盈止损倍数、挂单参考 EMA、"
            "动态止盈与时间保本。"
        ),
        default_signal_label="只做多",
        allowed_signal_labels=("只做多",),
        supports_signal_only=True,
        supports_batch_observe=True,
        supports_trader_desk=True,
    ),
    StrategyDefinition(
        strategy_id=STRATEGY_DYNAMIC_SHORT_ID,
        name="EMA 动态委托做空",
        summary="顺着 EMA 趋势方向挂单做空，适合趋势延续型短中线实验。",
        rule_description=(
            "当快 EMA 位于趋势 EMA 下方，且已收盘 K 线仍然压在趋势 EMA 下方时，"
            "下一根按挂单参考 EMA 尝试做空。"
        ),
        parameter_hint=(
            "可调参数主要是 EMA 周期、ATR 止盈止损倍数、挂单参考 EMA、"
            "动态止盈与时间保本。"
        ),
        default_signal_label="只做空",
        allowed_signal_labels=("只做空",),
        supports_signal_only=True,
        supports_batch_observe=True,
        supports_trader_desk=True,
    ),
    StrategyDefinition(
        strategy_id=STRATEGY_CROSS_ID,
        name="EMA 穿越策略",
        summary="按穿越参考EMA的突破入场，配合 ATR 止盈止损，适合作为基础突破策略。",
        rule_description=(
            "收盘价向上突破穿越参考 EMA 时做多，"
            "止损按参考EMA减ATR，止盈按 ATR 倍数。"
        ),
        parameter_hint="优先关注穿越参考EMA周期、ATR周期与止盈止损倍数。",
        default_signal_label="双向",
        allowed_signal_labels=("双向", "只做多", "只做空"),
        supports_signal_only=True,
        supports_batch_observe=True,
        supports_trader_desk=True,
    ),
    StrategyDefinition(
        strategy_id=STRATEGY_EMA5_EMA8_ID,
        name="EMA5/8 穿越止损",
        summary="固定 4H 节奏的 EMA5/EMA8 穿越策略，带大周期过滤。",
        rule_description=(
            "使用 EMA5 和 EMA8 的穿越作为入场信号，同时参考 EMA55/EMA233 的大趋势方向，"
            "离场采用动态止损。"
        ),
        parameter_hint="该策略固定 4H 周期，适合做更稳一点的节奏型趋势实验。",
        default_signal_label="双向",
        allowed_signal_labels=("双向", "只做多", "只做空"),
        supports_signal_only=True,
        supports_batch_observe=True,
        supports_trader_desk=True,
    ),
    StrategyDefinition(
        strategy_id=STRATEGY_DYNAMIC_ID,
        name="EMA 动态委托",
        summary="通用 EMA 动态委托策略入口，通常由做多/做空两个方向版本承接。",
        rule_description="支持双向 EMA 动态委托逻辑，主要保留给内部通用逻辑与兼容入口使用。",
        parameter_hint="如果只是日常使用，优先直接选做多或做空版本。",
        default_signal_label="双向",
        allowed_signal_labels=("双向", "只做多", "只做空"),
        supports_signal_only=True,
        supports_backtest=False,
    ),
)

VISIBLE_STRATEGY_DEFINITIONS: tuple[StrategyDefinition, ...] = tuple(
    item for item in ALL_STRATEGY_DEFINITIONS if item.strategy_id != STRATEGY_DYNAMIC_ID
)

BACKTEST_STRATEGY_DEFINITIONS: tuple[StrategyDefinition, ...] = tuple(
    item for item in VISIBLE_STRATEGY_DEFINITIONS if item.supports_backtest
)

STRATEGY_DEFINITIONS: tuple[StrategyDefinition, ...] = VISIBLE_STRATEGY_DEFINITIONS


def get_strategy_definition(strategy_id: str) -> StrategyDefinition:
    for item in ALL_STRATEGY_DEFINITIONS:
        if item.strategy_id == strategy_id:
            return item
    raise KeyError(f"未知策略：{strategy_id}")


def is_dynamic_strategy_id(strategy_id: str) -> bool:
    return strategy_id in {
        STRATEGY_DYNAMIC_ID,
        STRATEGY_DYNAMIC_LONG_ID,
        STRATEGY_DYNAMIC_SHORT_ID,
    }


def supports_signal_only(strategy_id: str) -> bool:
    return get_strategy_definition(strategy_id).supports_signal_only


def supports_trader_desk(strategy_id: str) -> bool:
    return get_strategy_definition(strategy_id).supports_trader_desk


def signal_observer_strategy_definitions() -> tuple[StrategyDefinition, ...]:
    return tuple(item for item in STRATEGY_DEFINITIONS if item.supports_signal_only and item.supports_batch_observe)


def trader_desk_strategy_definitions() -> tuple[StrategyDefinition, ...]:
    return tuple(item for item in STRATEGY_DEFINITIONS if item.supports_trader_desk)


def resolve_dynamic_signal_mode(strategy_id: str, signal_mode: str) -> str:
    if strategy_id == STRATEGY_DYNAMIC_LONG_ID:
        return "long_only"
    if strategy_id == STRATEGY_DYNAMIC_SHORT_ID:
        return "short_only"
    return signal_mode
