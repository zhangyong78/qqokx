from __future__ import annotations

from decimal import Decimal

from okx_quant.enhanced_models import (
    ChildSignalConfig,
    ChildSignalLabProfile,
    ExecutionPlaybookConfig,
    ParentStrategyConfig,
    SignalSource,
)
from okx_quant.enhanced_registry import EnhancedStrategyRegistry
from okx_quant.enhanced_signal_engine import (
    close_crosses_above_ema,
    close_crosses_above_lookback_high,
    close_crosses_below_lookback_low,
    pullback_reclaim_above_ema_after_run,
)


PARENT_STRATEGY_ID = "spot_enhancement_36"
PARENT_STRATEGY_NAME = "现货增强三十六计"

SEED_SIGNAL_IDS = (
    "seed_ma_breakout_long",
    "seed_pullback_reclaim_long",
    "seed_range_breakdown_short",
    "seed_range_breakout_long",
)


def register_seed_strategy_package(
    registry: EnhancedStrategyRegistry,
    *,
    underlying_family: str = "BTC-USD",
    spot_inst_id: str = "BTC-USDT",
    signal_bar: str = "5m",
    parent_strategy_id: str = PARENT_STRATEGY_ID,
    parent_strategy_name: str = PARENT_STRATEGY_NAME,
) -> None:
    registry.register_parent_strategy(
        ParentStrategyConfig(
            strategy_id=parent_strategy_id,
            strategy_name=parent_strategy_name,
            base_bar="5m",
        )
    )

    registry.register_playbook(
        ExecutionPlaybookConfig(
            playbook_id="seed_swap_long",
            playbook_name="永续做多",
            action="SWAP_LONG",
            underlying_family=underlying_family,
            sizing_mode="fixed_slot",
            slot_size=Decimal("1"),
            lab_hold_bars=2,
        )
    )
    registry.register_playbook(
        ExecutionPlaybookConfig(
            playbook_id="seed_swap_short",
            playbook_name="永续做空",
            action="SWAP_SHORT",
            underlying_family=underlying_family,
            sizing_mode="fixed_slot",
            slot_size=Decimal("1"),
            lab_hold_bars=2,
        )
    )

    registry.register_child_signal(
        parent_strategy_id,
        ChildSignalConfig(
            signal_id="seed_ma_breakout_long",
            signal_name="第01计_均线突破",
            source=SignalSource(market="SPOT", inst_id=spot_inst_id, bar=signal_bar),
            underlying_family=underlying_family,
            direction_bias="long",
            trigger_rule_id="seed_rule_ma_breakout_long",
            invalidation_rule_id="seed_rule_never",
            evidence_template_id="seed_default",
        ),
    )
    registry.register_child_signal(
        parent_strategy_id,
        ChildSignalConfig(
            signal_id="seed_pullback_reclaim_long",
            signal_name="第02计_连续上涨后回调承接",
            source=SignalSource(market="SPOT", inst_id=spot_inst_id, bar=signal_bar),
            underlying_family=underlying_family,
            direction_bias="long",
            trigger_rule_id="seed_rule_pullback_reclaim_long",
            invalidation_rule_id="seed_rule_never",
            evidence_template_id="seed_default",
        ),
    )
    registry.register_child_signal(
        parent_strategy_id,
        ChildSignalConfig(
            signal_id="seed_range_breakdown_short",
            signal_name="第03计_小区间跌破",
            source=SignalSource(market="SPOT", inst_id=spot_inst_id, bar=signal_bar),
            underlying_family=underlying_family,
            direction_bias="short",
            trigger_rule_id="seed_rule_range_breakdown_short",
            invalidation_rule_id="seed_rule_never",
            evidence_template_id="seed_default",
        ),
    )
    registry.register_child_signal(
        parent_strategy_id,
        ChildSignalConfig(
            signal_id="seed_range_breakout_long",
            signal_name="第04计_小区间突破",
            source=SignalSource(market="SPOT", inst_id=spot_inst_id, bar=signal_bar),
            underlying_family=underlying_family,
            direction_bias="long",
            trigger_rule_id="seed_rule_range_breakout_long",
            invalidation_rule_id="seed_rule_never",
            evidence_template_id="seed_default",
        ),
    )

    registry.register_signal_lab_profile(
        ChildSignalLabProfile(
            signal_id="seed_ma_breakout_long",
            profile_name="突破顺势_轻止损",
            exit_mode="tp_sl_handoff",
            max_hold_bars=18,
            stop_loss_pct=Decimal("0.0045"),
            take_profit_pct=Decimal("0.0090"),
            stop_hit_mode="handoff_manual",
            notes="用于趋势突破后的第一波顺势尝试。",
        )
    )
    registry.register_signal_lab_profile(
        ChildSignalLabProfile(
            signal_id="seed_pullback_reclaim_long",
            profile_name="回调承接_快进快出",
            exit_mode="tp_sl_handoff",
            max_hold_bars=12,
            stop_loss_pct=Decimal("0.0035"),
            take_profit_pct=Decimal("0.0080"),
            stop_hit_mode="handoff_manual",
            notes="更偏短促回抽，持有时间比突破类更短。",
        )
    )
    registry.register_signal_lab_profile(
        ChildSignalLabProfile(
            signal_id="seed_range_breakdown_short",
            profile_name="区间跌破_空头扩张",
            exit_mode="tp_sl_handoff",
            max_hold_bars=24,
            stop_loss_pct=Decimal("0.0040"),
            take_profit_pct=Decimal("0.0100"),
            stop_hit_mode="handoff_manual",
            notes="空头跌破允许给更长时间释放趋势。",
        )
    )
    registry.register_signal_lab_profile(
        ChildSignalLabProfile(
            signal_id="seed_range_breakout_long",
            profile_name="区间突破_标准1比2",
            exit_mode="tp_sl_handoff",
            max_hold_bars=18,
            stop_loss_pct=Decimal("0.0050"),
            take_profit_pct=Decimal("0.0100"),
            stop_hit_mode="handoff_manual",
            notes="保持标准 1:2 结构，便于和其他子策略横向对比。",
        )
    )

    registry.register_trigger_rule(
        "seed_rule_ma_breakout_long",
        lambda candles, index: close_crosses_above_ema(candles, index, 21),
    )
    registry.register_trigger_rule(
        "seed_rule_pullback_reclaim_long",
        lambda candles, index: pullback_reclaim_above_ema_after_run(
            candles,
            index,
            ema_period=21,
            run_bars=4,
            minimum_up_closes=3,
        ),
    )
    registry.register_trigger_rule(
        "seed_rule_range_breakdown_short",
        lambda candles, index: close_crosses_below_lookback_low(candles, index, 6),
    )
    registry.register_trigger_rule(
        "seed_rule_range_breakout_long",
        lambda candles, index: close_crosses_above_lookback_high(candles, index, 6),
    )
    registry.register_invalidation_rule("seed_rule_never", lambda candles, index: None)

    registry.bind_signal("seed_ma_breakout_long", playbook_ids=["seed_swap_long"])
    registry.bind_signal("seed_pullback_reclaim_long", playbook_ids=["seed_swap_long"])
    registry.bind_signal("seed_range_breakdown_short", playbook_ids=["seed_swap_short"])
    registry.bind_signal("seed_range_breakout_long", playbook_ids=["seed_swap_long"])
