from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class StrategyDefinition:
    name: str
    description: str


def strategy_definitions() -> list[StrategyDefinition]:
    return [
        StrategyDefinition("strategy_a_ema21_pullback", "EMA21 pullback short"),
        StrategyDefinition("strategy_b_ema55_pullback", "EMA55 downtrend pullback short"),
        StrategyDefinition("strategy_c_dual_bear_pullback", "EMA21 < EMA55 bear alignment pullback to EMA21"),
        StrategyDefinition("strategy_d_first_pullback", "First EMA21 pullback after EMA21 cross below EMA55"),
        StrategyDefinition("strategy_e_second_pullback", "Second EMA21 pullback after EMA21 cross below EMA55"),
        StrategyDefinition("strategy_f_dual_bear_rsi", "Strategy C plus RSI filter"),
        StrategyDefinition("strategy_g_dual_bear_volume", "Strategy C plus volume filter"),
    ]


def build_strategy_signals(frame: pd.DataFrame, config: dict[str, object]) -> dict[str, pd.Series]:
    turn_weak = frame["close"] < frame["low_prev"]
    touch_ema21 = frame["high"] >= frame["ema21"]
    touch_ema55 = frame["high"] >= frame["ema55"]
    dual_bear = (frame["ema21"] < frame["ema55"]) & (frame["close"] < frame["ema55"])
    ema55_down = frame["ema55"] < frame["ema55"].shift(5)
    rsi_filter = frame["rsi14"] >= float(config["rsi_filter_threshold"])
    volume_filter = frame["volume"] >= (frame["vol_ma20"] * float(config["volume_filter_multiplier"]))

    first_pullback, second_pullback = pullback_event_signals(frame)

    return {
        "strategy_a_ema21_pullback": (frame["close"] < frame["ema21"]) & touch_ema21 & turn_weak,
        "strategy_b_ema55_pullback": (frame["close"] < frame["ema55"]) & ema55_down & touch_ema55 & turn_weak,
        "strategy_c_dual_bear_pullback": dual_bear & touch_ema21 & turn_weak,
        "strategy_d_first_pullback": first_pullback & turn_weak,
        "strategy_e_second_pullback": second_pullback & turn_weak,
        "strategy_f_dual_bear_rsi": dual_bear & touch_ema21 & turn_weak & rsi_filter,
        "strategy_g_dual_bear_volume": dual_bear & touch_ema21 & turn_weak & volume_filter,
    }


def pullback_event_signals(frame: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    first = pd.Series(False, index=frame.index)
    second = pd.Series(False, index=frame.index)
    active = False
    pullback_count = 0
    previous_touch = False

    for idx in frame.index:
        ema21 = frame.at[idx, "ema21"]
        ema55 = frame.at[idx, "ema55"]
        if pd.isna(ema21) or pd.isna(ema55):
            active = False
            pullback_count = 0
            previous_touch = False
            continue

        if idx > 0:
            prev_ema21 = frame.at[idx - 1, "ema21"]
            prev_ema55 = frame.at[idx - 1, "ema55"]
            crossed_under = pd.notna(prev_ema21) and pd.notna(prev_ema55) and prev_ema21 >= prev_ema55 and ema21 < ema55
        else:
            crossed_under = False

        if crossed_under:
            active = True
            pullback_count = 0
            previous_touch = False

        if ema21 >= ema55:
            active = False
            pullback_count = 0
            previous_touch = False
            continue

        touch = bool(frame.at[idx, "high"] >= ema21) if active else False
        new_pullback = active and touch and not previous_touch
        if new_pullback:
            pullback_count += 1
            if pullback_count == 1:
                first.at[idx] = True
            elif pullback_count == 2:
                second.at[idx] = True
        previous_touch = touch if active else False

    return first, second
