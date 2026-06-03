from __future__ import annotations

from dataclasses import dataclass, replace

from backtester import BacktestSettings
from v7 import RiskSchedule


@dataclass(frozen=True)
class CostScenario:
    name: str
    fee_multiplier: float
    slippage_multiplier: float
    description: str


def v8_cost_scenarios() -> list[CostScenario]:
    return [
        CostScenario("base_cost", 1.0, 1.0, "Original fee and slippage assumptions"),
        CostScenario("stress_cost_1_5x", 1.5, 1.5, "Fee and slippage both multiplied by 1.5x"),
        CostScenario("stress_cost_2_0x", 2.0, 2.0, "Fee and slippage both multiplied by 2.0x"),
    ]


def v8_risk_schedules() -> list[RiskSchedule]:
    return [
        RiskSchedule("v8_a_flat_1_0", 1.0, 1.0, "Flat baseline risk on every trade"),
        RiskSchedule("v8_b_strong_1_0_weak_0_5", 1.0, 0.5, "Full risk on strong regime, half risk on weak regime"),
        RiskSchedule("v8_c_strong_1_25_weak_0_5", 1.25, 0.5, "1.25x strong risk, half weak risk"),
        RiskSchedule("v8_d_strong_1_5_weak_0_5", 1.5, 0.5, "1.5x strong risk, half weak risk"),
        RiskSchedule("v8_e_strong_1_25_weak_0_75", 1.25, 0.75, "1.25x strong risk, 0.75x weak risk"),
        RiskSchedule("v8_f_strong_1_5_weak_0_75", 1.5, 0.75, "1.5x strong risk, 0.75x weak risk"),
    ]


def apply_cost_scenario(settings: BacktestSettings, scenario: CostScenario) -> BacktestSettings:
    return replace(
        settings,
        fee_rate=settings.fee_rate * scenario.fee_multiplier,
        slippage_rate=settings.slippage_rate * scenario.slippage_multiplier,
    )
