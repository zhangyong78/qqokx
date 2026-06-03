from __future__ import annotations

from backtester import ExitRule


def v3_exit_rules() -> list[ExitRule]:
    return [
        ExitRule(name="v3_exit_ema21_reclaim", kind="ema21_reclaim"),
        ExitRule(name="v3_exit_fixed_1_5R", kind="fixed_r", target_r=1.5),
        ExitRule(name="v3_exit_fixed_2R", kind="fixed_r", target_r=2.0),
        ExitRule(name="v3_exit_ema21_or_2R", kind="ema21_or_fixed_r", target_r=2.0),
        ExitRule(name="v3_exit_atr_trail_2ATR", kind="atr_trail", trail_atr_multiplier=2.0),
        ExitRule(name="v3_exit_atr_trail_1_5ATR_or_2R", kind="atr_trail_or_fixed_r", target_r=2.0, trail_atr_multiplier=1.5),
    ]
