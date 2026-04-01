from __future__ import annotations

from dataclasses import dataclass


STRATEGY_DYNAMIC_ID = "ema_dynamic_order"
STRATEGY_CROSS_ID = "ema_cross_market"
STRATEGY_EMA5_EMA8_ID = "ema5_ema8_cross_stop"


@dataclass(frozen=True)
class StrategyDefinition:
    strategy_id: str
    name: str
    summary: str
    rule_description: str
    parameter_hint: str
    allowed_signal_labels: tuple[str, ...]
    default_signal_label: str


STRATEGY_DEFINITIONS: tuple[StrategyDefinition, ...] = (
    StrategyDefinition(
        strategy_id=STRATEGY_DYNAMIC_ID,
        name="EMA 动态委托",
        summary="围绕 EMA 做趋势回调或反弹的计划委托单，适合持续跟单式挂单。",
        rule_description=(
            "启动后立即以上一根已收盘 K 线的 EMA 作为开仓价挂限价单；"
            "每一根新 K 线确认后，先撤掉旧挂单，再按最新的上一根 EMA 重新挂单。"
            "做多要求最近一根收盘仍在 EMA 上方，做空要求最近一根收盘仍在 EMA 下方。"
        ),
        parameter_hint=(
            "数量由风险金自动计算：开仓数量 = 风险金 / abs(开仓价格 - 止损价格)。"
            "适合上升趋势回调做多、下降趋势反弹做空。"
        ),
        allowed_signal_labels=("只做多", "只做空"),
        default_signal_label="只做多",
    ),
    StrategyDefinition(
        strategy_id=STRATEGY_CROSS_ID,
        name="EMA 穿越市价",
        summary="等待最新已收盘 K 线有效穿越 EMA，信号确认后直接市价进场。",
        rule_description=(
            "最近一根已收盘 K 线上穿 EMA 做多，下穿 EMA 做空。"
            "一旦出现新信号，就按照当时的参考价格和 ATR 计算止盈止损，并立即市价下单。"
        ),
        parameter_hint=(
            "止损按信号 K 线极值加减 1ATR 计算：做多用信号 K 线最低价减 1ATR，"
            "做空用信号 K 线最高价加 1ATR。数量同样支持风险金自动计算。"
        ),
        allowed_signal_labels=("双向", "只做多", "只做空"),
        default_signal_label="双向",
    ),
    StrategyDefinition(
        strategy_id=STRATEGY_EMA5_EMA8_ID,
        name="4H EMA5/EMA8 金叉死叉",
        summary="固定用 4 小时 EMA5 与 EMA8 的金叉死叉做开单，止损线跟随 EMA8。",
        rule_description=(
            "只使用 4 小时已收盘 K 线。EMA5 上穿 EMA8 视为金叉做多，"
            "EMA5 下穿 EMA8 视为死叉做空。开仓后不设固定止盈，"
            "而是持续监控最新 4 小时 EMA8：做多跌破 EMA8 止损，做空站回 EMA8 上方止损。"
        ),
        parameter_hint=(
            "固定使用 EMA5 / EMA8 与 4 小时周期。风险金默认 100，"
            "仓位按‘风险金 / abs(开仓价 - EMA8止损线)’自动计算。"
        ),
        allowed_signal_labels=("双向", "只做多", "只做空"),
        default_signal_label="双向",
    ),
)


def get_strategy_definition(strategy_id: str) -> StrategyDefinition:
    for item in STRATEGY_DEFINITIONS:
        if item.strategy_id == strategy_id:
            return item
    raise KeyError(f"Unknown strategy_id: {strategy_id}")
