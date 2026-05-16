from __future__ import annotations

from dataclasses import dataclass

STRATEGY_DYNAMIC_ID = "ema_dynamic_order"
STRATEGY_DYNAMIC_LONG_ID = "ema_dynamic_order_long"
STRATEGY_DYNAMIC_SHORT_ID = "ema_dynamic_order_short"
STRATEGY_DYNAMIC_MTF_LONG_ID = "ema_dynamic_mtf_long"
STRATEGY_DYNAMIC_MTF_SHORT_ID = "ema_dynamic_mtf_short"
# 仅兼容旧持久化 / 旧脚本，不在策略列表中展示
STRATEGY_CROSS_ID = "ema_cross_market"
STRATEGY_EMA_BREAKOUT_LONG_ID = "ema_breakout_long"
STRATEGY_EMA_BREAKDOWN_SHORT_ID = "ema_breakdown_short"
STRATEGY_EMA5_EMA8_ID = "ema5_ema8_cross_stop"
STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID = "adaptive_ema_rail_long"


def is_ema_atr_breakout_strategy(strategy_id: str) -> bool:
    """EMA 突破/跌破类：参考 EMA 突破或跌破 + 小/中周期 EMA 过滤，共用 EmaAtrStrategy 与回测引擎。"""
    return strategy_id in {
        STRATEGY_EMA_BREAKOUT_LONG_ID,
        STRATEGY_EMA_BREAKDOWN_SHORT_ID,
        STRATEGY_CROSS_ID,
    }


def is_adaptive_ema_rail_strategy(strategy_id: str) -> bool:
    return strategy_id == STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID


def is_dynamic_mtf_strategy_id(strategy_id: str) -> bool:
    return strategy_id in {
        STRATEGY_DYNAMIC_MTF_LONG_ID,
        STRATEGY_DYNAMIC_MTF_SHORT_ID,
    }


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
            "流程：先确认 EMA 小周期在趋势 EMA 上方，且已收盘 K 线收盘价仍站在趋势 EMA 上方；"
            "条件成立后，不直接追价，而是按上一根已收盘 K 线的挂单参考 EMA 挂多头限价单。"
            "若本根 K 线未成交，则下一根 K 线确认后撤掉旧单，再按最新参考 EMA 重挂；"
            "成交后转入止盈止损监控，本轮持仓结束后继续等待下一次多头信号。"
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
            "流程：先确认 EMA 小周期在趋势 EMA 下方，且已收盘 K 线收盘价仍压在趋势 EMA 下方；"
            "条件成立后，不直接追空，而是按上一根已收盘 K 线的挂单参考 EMA 挂空头限价单。"
            "若本根 K 线未成交，则下一根 K 线确认后撤掉旧单，再按最新参考 EMA 重挂；"
            "成交后转入止盈止损监控，本轮持仓结束后继续等待下一次空头信号。"
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
        strategy_id=STRATEGY_DYNAMIC_MTF_LONG_ID,
        name="EMA 动态委托-多周期多头",
        summary="高周期 EMA 趋势只负责过滤方向，低周期继续按 EMA 动态委托做多。",
        rule_description=(
            "流程：先用高周期 EMA 快慢线确认大方向，只有高周期快线在慢线上方时，"
            "才允许低周期按 EMA 动态委托做多。低周期一旦满足多头趋势条件，"
            "就按挂单参考 EMA 挂多头限价单；若未成交，下一根 K 线确认后撤旧挂新。"
            "成交后的止盈止损、动态上移、每波最多开仓次数与继续等待下一次信号的逻辑，"
            "沿用 EMA 动态委托做多。"
        ),
        parameter_hint="关注低周期入场 EMA、ATR 风控、高周期快慢 EMA 与高周期反转处理。",
        default_signal_label="只做多",
        allowed_signal_labels=("只做多",),
        supports_trade=True,
        supports_signal_only=True,
        supports_backtest=True,
    ),
    StrategyDefinition(
        strategy_id=STRATEGY_DYNAMIC_MTF_SHORT_ID,
        name="EMA 动态委托-多周期空头",
        summary="高周期 EMA 趋势只负责过滤方向，低周期继续按 EMA 动态委托做空。",
        rule_description=(
            "流程：先用高周期 EMA 快慢线确认大方向，只有高周期快线在慢线下方时，"
            "才允许低周期按 EMA 动态委托做空。低周期一旦满足空头趋势条件，"
            "就按挂单参考 EMA 挂空头限价单；若未成交，下一根 K 线确认后撤旧挂新。"
            "成交后的止盈止损、动态上移、每波最多开仓次数与继续等待下一次信号的逻辑，"
            "沿用 EMA 动态委托做空。"
        ),
        parameter_hint="关注低周期入场 EMA、ATR 风控、高周期快慢 EMA 与高周期反转处理。",
        default_signal_label="只做空",
        allowed_signal_labels=("只做空",),
        supports_trade=True,
        supports_signal_only=True,
        supports_backtest=True,
    ),
    StrategyDefinition(
        strategy_id=STRATEGY_EMA_BREAKOUT_LONG_ID,
        name="EMA 突破做多策略",
        summary="收盘价向上突破参考 EMA 时做多，且仅当 EMA(小周期) 在 EMA(中周期) 上方；ATR 止盈止损。",
        rule_description=(
            "流程：先等待最近一根已收盘 K 线收盘价向上突破参考 EMA，且 EMA 小周期仍在 EMA 中周期上方；"
            "条件成立后立即按市价开多，不走回调挂单。开仓后若为固定止盈模式，则止盈止损一并交给 OKX；"
            "若为动态止盈模式，则先只挂初始止损，后续按 2R/3R 等规则上移止损。"
            "本轮仓位结束后，本次策略线程收口，不是继续在当前会话里等待下一次信号。"
        ),
        parameter_hint="关注小/中周期 EMA、突破参考 EMA 周期、ATR 与止盈止损倍数。",
        default_signal_label="只做多",
        allowed_signal_labels=("只做多",),
        supports_signal_only=True,
        supports_batch_observe=True,
        supports_trader_desk=True,
    ),
    StrategyDefinition(
        strategy_id=STRATEGY_EMA_BREAKDOWN_SHORT_ID,
        name="EMA 跌破做空策略",
        summary="收盘价向下跌破参考 EMA 时做空，且仅当 EMA(小周期) 在 EMA(中周期) 下方；ATR 止盈止损。",
        rule_description=(
            "流程：先等待最近一根已收盘 K 线收盘价向下跌破参考 EMA，且 EMA 小周期仍在 EMA 中周期下方；"
            "条件成立后立即按市价开空，不走反弹挂单。开仓后若为固定止盈模式，则止盈止损一并交给 OKX；"
            "若为动态止盈模式，则先只挂初始止损，后续按 2R/3R 等规则下移止损。"
            "本轮仓位结束后，本次策略线程收口，不是继续在当前会话里等待下一次信号。"
        ),
        parameter_hint="关注小/中周期 EMA、突破参考 EMA 周期、ATR 与止盈止损倍数。",
        default_signal_label="只做空",
        allowed_signal_labels=("只做空",),
        supports_signal_only=True,
        supports_batch_observe=True,
        supports_trader_desk=True,
    ),
    StrategyDefinition(
        strategy_id=STRATEGY_CROSS_ID,
        name="EMA 突破/跌破（旧版）",
        summary="兼容旧配置；请改用「EMA 突破做多策略」或「EMA 跌破做空策略」。",
        rule_description=(
            "流程与新版突破做多 / 跌破做空一致：等待已收盘 K 线突破或跌破参考 EMA，"
            "并通过 EMA 小周期与中周期的方向过滤后，立即按市价进场；"
            "固定止盈模式会一次性挂好止盈止损，动态止盈模式则先挂初始止损再动态上移。"
            "本入口仅保留给旧持久化与旧脚本兼容使用。"
        ),
        parameter_hint="请迁移到新策略入口；本入口仅用于读取旧持久化。",
        default_signal_label="只做多",
        allowed_signal_labels=("只做多", "只做空"),
        supports_signal_only=True,
        supports_batch_observe=True,
        supports_trader_desk=True,
    ),
    StrategyDefinition(
        strategy_id=STRATEGY_EMA5_EMA8_ID,
        name="EMA5/8 穿越止损",
        summary="固定 4H 节奏的 EMA5/EMA8 穿越策略，带大周期过滤。",
        rule_description=(
            "流程：固定使用 4H 节奏，先看 EMA55/EMA233 的大趋势过滤方向，"
            "再用 EMA5 与 EMA8 的穿越生成入场信号。信号成立后按策略定义进场，"
            "离场不走固定止盈，而是交给动态止损规则持续跟踪，直到保护价被触发或仓位结束。"
            "本轮结束后再回到等待下一次 EMA5/8 穿越信号。"
        ),
        parameter_hint="该策略固定 4H 周期，适合做更稳一点的节奏型趋势实验。",
        default_signal_label="双向",
        allowed_signal_labels=("双向", "只做多", "只做空"),
        supports_signal_only=True,
        supports_batch_observe=True,
        supports_trader_desk=True,
    ),
    StrategyDefinition(
        strategy_id=STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID,
        name="Adaptive EMA Rail 做多",
        summary="自动寻找当前被市场尊重的支撑型 EMA，并在轨道确认后按回踩委托做多。",
        rule_description=(
            "流程：先用 EMA200 斜率与高低点结构判断大趋势，再对候选 EMA 逐条计算 Respect Score，"
            "选出当前最被市场尊重的主导轨道。只有当主导轨道完成至少两次有效反弹后，"
            "才允许按该主导 EMA 挂多头回踩委托。后续重点观察轨道是否继续有效、回踩是否成交，"
            "以及轨道失效后是否需要切换到新的主导 EMA。"
        ),
        parameter_hint="V1 固定候选 EMA 集合与 Respect Score 默认阈值，重点验证轨道识别和回测表现。",
        default_signal_label="只做多",
        allowed_signal_labels=("只做多",),
        supports_trade=False,
        supports_signal_only=False,
        supports_backtest=True,
    ),
    StrategyDefinition(
        strategy_id=STRATEGY_DYNAMIC_ID,
        name="EMA 动态委托",
        summary="通用 EMA 动态委托策略入口，通常由做多/做空两个方向版本承接。",
        rule_description=(
            "通用流程与 EMA 动态委托做多 / 做空相同：先判断趋势方向是否成立，"
            "再按挂单参考 EMA 挂限价单，未成交则每根新 K 线撤旧挂新，"
            "成交后进入止盈止损监控，本轮持仓结束后继续等待下一次信号。"
            "该入口主要保留给内部通用逻辑与兼容场景使用。"
        ),
        parameter_hint="如果只是日常使用，优先直接选做多或做空版本。",
        default_signal_label="双向",
        allowed_signal_labels=("双向", "只做多", "只做空"),
        supports_signal_only=True,
        supports_backtest=False,
    ),
)

_STRATEGY_IDS_HIDDEN_FROM_LAUNCHER: frozenset[str] = frozenset(
    {STRATEGY_DYNAMIC_ID, STRATEGY_CROSS_ID, STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID}
)
_STRATEGY_IDS_HIDDEN_FROM_BACKTEST: frozenset[str] = frozenset({STRATEGY_DYNAMIC_ID, STRATEGY_CROSS_ID})

VISIBLE_STRATEGY_DEFINITIONS: tuple[StrategyDefinition, ...] = tuple(
    item for item in ALL_STRATEGY_DEFINITIONS if item.strategy_id not in _STRATEGY_IDS_HIDDEN_FROM_LAUNCHER
)

BACKTEST_STRATEGY_DEFINITIONS: tuple[StrategyDefinition, ...] = tuple(
    item
    for item in ALL_STRATEGY_DEFINITIONS
    if item.supports_backtest and item.strategy_id not in _STRATEGY_IDS_HIDDEN_FROM_BACKTEST
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
    if strategy_id == STRATEGY_DYNAMIC_MTF_LONG_ID:
        return "long_only"
    if strategy_id == STRATEGY_DYNAMIC_MTF_SHORT_ID:
        return "short_only"
    if strategy_id == STRATEGY_EMA_BREAKOUT_LONG_ID:
        return "long_only"
    if strategy_id == STRATEGY_EMA_BREAKDOWN_SHORT_ID:
        return "short_only"
    if strategy_id == STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID:
        return "long_only"
    return signal_mode
