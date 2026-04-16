from __future__ import annotations

from dataclasses import dataclass

STRATEGY_DYNAMIC_ID = "ema_dynamic_order"
STRATEGY_DYNAMIC_LONG_ID = "ema_dynamic_order_long"
STRATEGY_DYNAMIC_SHORT_ID = "ema_dynamic_order_short"
STRATEGY_CROSS_ID = "ema_cross_market"
STRATEGY_EMA5_EMA8_ID = "ema5_ema8_cross_stop"
STRATEGY_SLOT_LONG_ID = "ema_slot_handoff_long"
STRATEGY_SLOT_SHORT_ID = "ema_slot_handoff_short"


@dataclass(frozen=True)
class StrategyDefinition:
    strategy_id: str
    name: str
    summary: str
    rule_description: str
    parameter_hint: str
    default_signal_label: str
    allowed_signal_labels: tuple[str, ...]


ALL_STRATEGY_DEFINITIONS: tuple[StrategyDefinition, ...] = (
    StrategyDefinition(
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        name="EMA 动态委托-多头",
        summary="只做多头。趋势成立后，每根新 K 线都会按挂单参考 EMA 重算回调委托价。",
        rule_description=(
            "做多条件：EMA 小周期高于 EMA 中周期，且收盘价位于 EMA 中周期上方。"
            " 每根新 K 线确认后，上一个委托自动失效，再按最新已收盘 K 线的挂单参考 EMA 重挂下一根回调委托。"
            " 支持每波趋势最多开仓次数限制，并可在固定止盈和动态止盈之间切换。"
        ),
        parameter_hint=(
            "EMA 小周期决定信号强弱，EMA 中周期决定趋势过滤，挂单参考 EMA 决定回调委托价格；填 0 表示跟随 EMA 小周期。"
            " 每波趋势最多开仓次数填 0 表示不限。"
            " 动态止盈达到 2R 后先移动到保本加双向手续费，之后按 nR 锁定 (n-1)R。"
        ),
        default_signal_label="只做多",
        allowed_signal_labels=("只做多",),
    ),
    StrategyDefinition(
        strategy_id=STRATEGY_DYNAMIC_SHORT_ID,
        name="EMA 动态委托-空头",
        summary="只做空头。趋势成立后，每根新 K 线都会按挂单参考 EMA 重算反弹委托价。",
        rule_description=(
            "做空条件：EMA 小周期低于 EMA 中周期，且收盘价位于 EMA 中周期下方。"
            " 每根新 K 线确认后，上一个委托自动失效，再按最新已收盘 K 线的挂单参考 EMA 重挂下一根反弹委托。"
            " 支持每波趋势最多开仓次数限制，并可在固定止盈和动态止盈之间切换。"
        ),
        parameter_hint=(
            "EMA 小周期决定信号强弱，EMA 中周期决定趋势过滤，挂单参考 EMA 决定反弹委托价格；填 0 表示跟随 EMA 小周期。"
            " 每波趋势最多开仓次数填 0 表示不限。"
            " 动态止盈达到 2R 后先移动到保本加双向手续费，之后按 nR 锁定 (n-1)R。"
        ),
        default_signal_label="只做空",
        allowed_signal_labels=("只做空",),
    ),
    StrategyDefinition(
        strategy_id=STRATEGY_CROSS_ID,
        name="EMA 穿越市价",
        summary="EMA 方向确认后按市价进场，适合快速验证趋势跟随。",
        rule_description=(
            "做多：收盘价上穿 EMA 小周期。"
            " 做空：收盘价下穿 EMA 小周期。"
            " 趋势过滤、固定止盈和固定止损按参数执行。"
        ),
        parameter_hint="适合快速验证趋势跟随，不使用每波开仓次数限制，也不支持动态止盈。",
        default_signal_label="只做多",
        allowed_signal_labels=("只做多", "只做空"),
    ),
    StrategyDefinition(
        strategy_id=STRATEGY_EMA5_EMA8_ID,
        name="EMA5/8 穿越止损",
        summary="EMA5 与 EMA8 配合 EMA55、EMA233 过滤，适合强趋势测试。",
        rule_description=(
            "做多：EMA5 上穿 EMA8，且价格位于 EMA55 和 EMA233 上方。"
            " 做空：EMA5 下穿 EMA8，且价格位于 EMA55 和 EMA233 下方。"
        ),
        parameter_hint="这套策略继续使用大 EMA 周期，不受每波开仓次数和动态止盈参数影响。",
        default_signal_label="只做多",
        allowed_signal_labels=("只做多", "只做空"),
    ),
    StrategyDefinition(
        strategy_id=STRATEGY_SLOT_LONG_ID,
        name="EMA 槽位接管做多",
        summary="5m 趋势突破做多，盈利自动止盈或盈利失效退出，亏损单移交人工池继续占用槽位。",
        rule_description=(
            "做多条件：收盘价上穿 EMA 小周期，且收盘价位于 EMA 中周期上方。"
            " 开仓后按 ATR 计算理论止损和止盈，但自动系统只执行止盈；"
            " 若收盘重新跌回 EMA 小周期下方，则盈利单自动平仓，亏损单转入人工池。"
        ),
        parameter_hint=(
            "固定数量代表单个槽位大小；“每波最多开仓次数”在本策略中表示最大槽位数。"
            " 人工池仓位继续占用槽位，只有手动处理或回测结束才会释放。"
        ),
        default_signal_label="只做多",
        allowed_signal_labels=("只做多",),
    ),
    StrategyDefinition(
        strategy_id=STRATEGY_SLOT_SHORT_ID,
        name="EMA 槽位接管做空",
        summary="5m 趋势跌破做空，盈利自动止盈或盈利失效退出，亏损单移交人工池继续占用槽位。",
        rule_description=(
            "做空条件：收盘价下穿 EMA 小周期，且收盘价位于 EMA 中周期下方。"
            " 开仓后按 ATR 计算理论止损和止盈，但自动系统只执行止盈；"
            " 若收盘重新站回 EMA 小周期上方，则盈利单自动平仓，亏损单转入人工池。"
        ),
        parameter_hint=(
            "固定数量代表单个槽位大小；“每波最多开仓次数”在本策略中表示最大槽位数。"
            " 人工池仓位继续占用槽位，只有手动处理或回测结束才会释放。"
        ),
        default_signal_label="只做空",
        allowed_signal_labels=("只做空",),
    ),
    StrategyDefinition(
        strategy_id=STRATEGY_DYNAMIC_ID,
        name="EMA 动态委托（兼容旧版）",
        summary="仅用于兼容旧配置和旧回测记录。新任务建议改用多头版或空头版。",
        rule_description="旧版兼容入口，内部仍按方向参数运行，不建议继续用于新建任务。",
        parameter_hint="如需新实验，优先使用“EMA 动态委托-多头”或“EMA 动态委托-空头”。",
        default_signal_label="只做多",
        allowed_signal_labels=("只做多", "只做空"),
    ),
)

BACKTEST_ONLY_STRATEGY_IDS = {
    STRATEGY_SLOT_LONG_ID,
    STRATEGY_SLOT_SHORT_ID,
}

VISIBLE_STRATEGY_DEFINITIONS: tuple[StrategyDefinition, ...] = tuple(
    item for item in ALL_STRATEGY_DEFINITIONS if item.strategy_id != STRATEGY_DYNAMIC_ID
)

BACKTEST_STRATEGY_DEFINITIONS: tuple[StrategyDefinition, ...] = VISIBLE_STRATEGY_DEFINITIONS

STRATEGY_DEFINITIONS: tuple[StrategyDefinition, ...] = tuple(
    item for item in VISIBLE_STRATEGY_DEFINITIONS if item.strategy_id not in BACKTEST_ONLY_STRATEGY_IDS
)


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


def is_slot_handoff_strategy_id(strategy_id: str) -> bool:
    return strategy_id in {
        STRATEGY_SLOT_LONG_ID,
        STRATEGY_SLOT_SHORT_ID,
    }


def resolve_dynamic_signal_mode(strategy_id: str, signal_mode: str) -> str:
    if strategy_id == STRATEGY_DYNAMIC_LONG_ID:
        return "long_only"
    if strategy_id == STRATEGY_DYNAMIC_SHORT_ID:
        return "short_only"
    return signal_mode
