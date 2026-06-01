from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

import pandas as pd

from okx_quant.ma55_slope_regime import (
    FLAT_THRESHOLD,
    REGIME_LABELS,
    SIGNAL_META,
    add_indicators,
    build_frame,
    classify_regime,
    enrich_line,
    linear_regression_slope,
)

StrategyMode = Literal["options_spread", "futures_trend", "observe"]
ActionBias = Literal["long", "short", "neutral", "sell_call_spread", "sell_put_spread"]
SignalGrade = Literal["A", "B", "S", "none"]

TAU_CHOP = 4
DIST_CHOP_ATR = 0.75
SHORT_CALL_OTM_ATR = Decimal("0.6")
SHORT_PUT_OTM_ATR = Decimal("0.6")
SPREAD_WIDTH_ATR = Decimal("1.0")
STOP_ATR = Decimal("1.5")
HOLD_HOURS = 8
TAKE_PROFIT_CREDIT_PCT = Decimal("0.5")
MAX_DTE_DAYS = 2


@dataclass(frozen=True)
class Ma55StrategyConfig:
    tau_chop: int = TAU_CHOP
    dist_chop_atr: float = DIST_CHOP_ATR
    flat_threshold: float = FLAT_THRESHOLD
    short_otm_atr: Decimal = SHORT_CALL_OTM_ATR
    spread_width_atr: Decimal = SPREAD_WIDTH_ATR
    stop_atr: Decimal = STOP_ATR
    hold_hours: int = HOLD_HOURS
    take_profit_credit_pct: Decimal = TAKE_PROFIT_CREDIT_PCT
    max_dte_days: int = MAX_DTE_DAYS


@dataclass(frozen=True)
class Ma55StrategyDecision:
    timestamp: str
    close: float
    ma55: float
    atr14: float
    slope_ratio: float | None
    dist_atr: float | None
    run_length: int
    slope_regime: str
    slope_regime_label: str
    market_regime: str
    market_regime_label: str
    signal_grade: SignalGrade
    signal_name: str
    action: ActionBias
    mode: StrategyMode
    size_multiplier: float
    reason: str
    strike_short_call: float | None
    strike_long_call: float | None
    strike_short_put: float | None
    strike_long_put: float | None
    stop_price: float | None
    take_profit_note: str
    filters_passed: tuple[str, ...]
    filters_failed: tuple[str, ...]


def classify_market_regime(
    *,
    slope_ratio: float | None,
    dist_atr: float | None,
    run_length: int,
    config: Ma55StrategyConfig,
) -> str:
    if pd.isna(slope_ratio) or pd.isna(dist_atr):
        return "warmup"
    short_run = run_length <= config.tau_chop
    flat_slope = abs(float(slope_ratio)) < config.flat_threshold
    near_line = abs(float(dist_atr)) < config.dist_chop_atr
    if short_run or (flat_slope and near_line):
        return "consolidation"
    if float(dist_atr) > 0 and float(slope_ratio) > 0:
        return "uptrend"
    if float(dist_atr) < 0 and float(slope_ratio) < 0:
        return "downtrend"
    if float(dist_atr) > 0:
        return "uptrend"
    if float(dist_atr) < 0:
        return "downtrend"
    return "consolidation"


def evaluate_ma55_strategy_row(
    row: pd.Series,
    *,
    prev_regime: str | None,
    ema_regime: str | None = None,
    config: Ma55StrategyConfig | None = None,
) -> Ma55StrategyDecision:
    config = config or Ma55StrategyConfig()
    slope_regime = str(row.get("regime", "warming_up"))
    slope_ratio = float(row["slope_ratio"]) if pd.notna(row.get("slope_ratio")) else None
    dist_atr = float(row["dist_atr"]) if pd.notna(row.get("dist_atr")) else None
    run_length = int(row.get("run_length", 0))
    close = float(row["close"])
    ma55 = float(row["sma55"])
    atr14 = float(row["atr14"]) if pd.notna(row.get("atr14")) else float("nan")

    market_regime = classify_market_regime(
        slope_ratio=slope_ratio,
        dist_atr=dist_atr,
        run_length=run_length,
        config=config,
    )
    market_label = {
        "uptrend": "上升趋势",
        "downtrend": "下降趋势",
        "consolidation": "盘整震荡",
        "warmup": "预热",
    }.get(market_regime, market_regime)

    passed: list[str] = []
    failed: list[str] = []
    grade: SignalGrade = "none"
    signal_name = "无信号"
    action: ActionBias = "neutral"
    mode: StrategyMode = "observe"
    size = 0.0
    reason = "当前无符合规则的 MA55 入场。"
    short_call = long_call = short_put = long_put = stop_price = None
    take_profit_note = f"持有 {config.hold_hours}H 或权利金收 {config.take_profit_credit_pct * 100:.0f}% 平仓。"

    if slope_regime == "warming_up" or pd.isna(atr14):
        return _decision(
            row,
            slope_regime,
            market_regime,
            market_label,
            grade,
            signal_name,
            action,
            mode,
            size,
            reason,
            short_call,
            long_call,
            short_put,
            long_put,
            stop_price,
            take_profit_note,
            passed,
            failed,
        )

    is_new_signal = prev_regime != slope_regime
    above = close > ma55
    below = close < ma55

    if market_regime == "consolidation":
        failed.append("三态=盘整")
        reason = "MA55 处于盘整：run-length 短或贴线且斜率走平。按统计不做方向单；期权仅观察，不新开仓。"
        return _decision(
            row,
            slope_regime,
            market_regime,
            market_label,
            grade,
            signal_name,
            action,
            mode,
            size,
            reason,
            short_call,
            long_call,
            short_put,
            long_put,
            stop_price,
            take_profit_note,
            passed,
            failed,
        )

    if slope_regime == "bear_start" and is_new_signal:
        signal_name = "MA55 转空"
        if below:
            passed.append("收盘<MA55")
        else:
            failed.append("收盘未<MA55")
        if market_regime != "uptrend":
            passed.append("非上升三态")
        else:
            failed.append("仍处于上升三态")
        if ema_regime == "bear_start":
            grade, size = "S", 1.5
            passed.append("EMA55 同根转空")
        elif not failed:
            grade, size = "A", 1.0
        if not failed:
            action = "sell_call_spread"
            mode = "options_spread"
            short_call, long_call = _call_spread_strikes(close, atr14, config)
            stop_price = close + float(config.stop_atr) * atr14
            reason = "MA55 斜率由≥0 下穿 <0，4H 区间成功率约 81%。卖 1-2 DTE Call Spread。"
        else:
            reason = "转空信号出现，但过滤未全过：" + "；".join(failed)

    elif slope_regime == "bull_start" and is_new_signal:
        signal_name = "MA55 转多"
        if above:
            passed.append("收盘>MA55")
        else:
            failed.append("收盘未>MA55")
        if market_regime != "downtrend":
            passed.append("非下降三态")
        else:
            failed.append("仍处于下降三态")
        if ema_regime == "bull_start":
            grade, size = "S", 1.5
            passed.append("EMA55 同根转多")
        elif not failed:
            grade, size = "A", 1.0
        if not failed:
            action = "sell_put_spread"
            mode = "options_spread"
            short_put, long_put = _put_spread_strikes(close, atr14, config)
            stop_price = close - float(config.stop_atr) * atr14
            reason = "MA55 斜率由≤0 上穿 >0，4H 区间成功率约 78%。卖 1-2 DTE Put Spread。"
        else:
            reason = "转多信号出现，但过滤未全过：" + "；".join(failed)

    elif slope_regime == "bull_fade" and is_new_signal:
        signal_name = "MA55 多头衰竭"
        grade, size = "B", 0.5
        if slope_ratio is not None and slope_ratio > 0:
            passed.append("斜率仍>0")
        else:
            failed.append("斜率未>0")
        if not failed:
            action = "sell_call_spread"
            mode = "options_spread"
            short_call, long_call = _call_spread_strikes(close, atr14, config)
            stop_price = close + float(config.stop_atr) * atr14
            reason = "MA55 斜率仍正但连续减速，4H 区间成功率约 79%。轻仓卖 Call Spread。"
        else:
            reason = "多头衰竭未通过过滤。"

    elif slope_regime == "bear_fade" and is_new_signal:
        signal_name = "MA55 空头衰竭"
        grade, size = "B", 0.5
        if slope_ratio is not None and slope_ratio < 0:
            passed.append("斜率仍<0")
        else:
            failed.append("斜率未<0")
        if not failed:
            action = "sell_put_spread"
            mode = "options_spread"
            short_put, long_put = _put_spread_strikes(close, atr14, config)
            stop_price = close - float(config.stop_atr) * atr14
            reason = "MA55 斜率仍负但连续回升，4H 区间成功率约 79%。轻仓卖 Put Spread。"
        else:
            reason = "空头衰竭未通过过滤。"

    elif market_regime == "uptrend" and run_length > config.tau_chop and slope_regime == "bull_run":
        signal_name = "上升波段回调"
        action = "long"
        mode = "futures_trend"
        grade = "B"
        size = 0.5
        stop_price = ma55 - float(config.stop_atr) * atr14
        passed.append("上升三态")
        passed.append(f"run>{config.tau_chop}")
        reason = "已确认上升波段（非刚穿线）。可在 MA55 附近挂多，止损 = MA55 - 1.5 ATR。"

    elif market_regime == "downtrend" and run_length > config.tau_chop and slope_regime == "bear_run":
        signal_name = "下降波段反弹"
        action = "short"
        mode = "futures_trend"
        grade = "B"
        size = 0.5
        stop_price = ma55 + float(config.stop_atr) * atr14
        passed.append("下降三态")
        passed.append(f"run>{config.tau_chop}")
        reason = "已确认下降波段。可在 MA55 附近挂空，止损 = MA55 + 1.5 ATR。"

    return _decision(
        row,
        slope_regime,
        market_regime,
        market_label,
        grade,
        signal_name,
        action,
        mode,
        size,
        reason,
        short_call,
        long_call,
        short_put,
        long_put,
        stop_price,
        take_profit_note,
        passed,
        failed,
    )


def evaluate_ma55_strategy_from_candles(candles: list[object], *, config: Ma55StrategyConfig | None = None) -> Ma55StrategyDecision:
    config = config or Ma55StrategyConfig()
    df = build_frame(candles)
    add_indicators(df)
    ma = enrich_line(df, "sma55")
    ema = enrich_line(df, "ema55")

    side = (ma["close"] > ma["sma55"]).astype(int)
    change = side.diff().fillna(0).ne(0)
    ma["side_id"] = change.cumsum()
    ma["run_length"] = ma.groupby("side_id")["close"].transform("count")
    ma["dist_atr"] = (ma["close"] - ma["sma55"]) / ma["atr14"]

    latest = ma.iloc[-1]
    prev_regime = str(ma.iloc[-2]["regime"]) if len(ma) >= 2 else None
    ema_regime = str(ema.iloc[-1]["regime"]) if len(ema) >= 1 else None
    return evaluate_ma55_strategy_row(latest, prev_regime=prev_regime, ema_regime=ema_regime, config=config)


def _call_spread_strikes(close: float, atr14: float, config: Ma55StrategyConfig) -> tuple[float, float]:
    short_k = _ceil_to_step(close + float(config.short_otm_atr) * atr14)
    long_k = _ceil_to_step(close + float(config.short_otm_atr + config.spread_width_atr) * atr14)
    return short_k, long_k


def _put_spread_strikes(close: float, atr14: float, config: Ma55StrategyConfig) -> tuple[float, float]:
    short_k = _floor_to_step(close - float(config.short_otm_atr) * atr14)
    long_k = _floor_to_step(close - float(config.short_otm_atr + config.spread_width_atr) * atr14)
    return short_k, long_k


def _ceil_to_step(value: float, step: float = 100.0) -> float:
    import math

    return float(math.ceil(value / step) * step)


def _floor_to_step(value: float, step: float = 100.0) -> float:
    import math

    return float(math.floor(value / step) * step)


def _decision(
    row: pd.Series,
    slope_regime: str,
    market_regime: str,
    market_label: str,
    grade: SignalGrade,
    signal_name: str,
    action: ActionBias,
    mode: StrategyMode,
    size: float,
    reason: str,
    short_call: float | None,
    long_call: float | None,
    short_put: float | None,
    long_put: float | None,
    stop_price: float | None,
    take_profit_note: str,
    passed: list[str],
    failed: list[str],
) -> Ma55StrategyDecision:
    slope_ratio = float(row["slope_ratio"]) if pd.notna(row.get("slope_ratio")) else None
    dist_atr = float(row["dist_atr"]) if pd.notna(row.get("dist_atr")) else None
    return Ma55StrategyDecision(
        timestamp=str(row["timestamp"]),
        close=float(row["close"]),
        ma55=float(row["sma55"]),
        atr14=float(row["atr14"]) if pd.notna(row.get("atr14")) else float("nan"),
        slope_ratio=slope_ratio,
        dist_atr=dist_atr,
        run_length=int(row.get("run_length", 0)),
        slope_regime=slope_regime,
        slope_regime_label=REGIME_LABELS.get(slope_regime, slope_regime),
        market_regime=market_regime,
        market_regime_label=market_label,
        signal_grade=grade,
        signal_name=signal_name,
        action=action,
        mode=mode,
        size_multiplier=size,
        reason=reason,
        strike_short_call=short_call,
        strike_long_call=long_call,
        strike_short_put=short_put,
        strike_long_put=long_put,
        stop_price=stop_price,
        take_profit_note=take_profit_note,
        filters_passed=tuple(passed),
        filters_failed=tuple(failed),
    )


def decision_to_dict(decision: Ma55StrategyDecision) -> dict[str, object]:
    return {
        "timestamp": decision.timestamp,
        "close": decision.close,
        "ma55": decision.ma55,
        "atr14": decision.atr14,
        "slope_ratio": decision.slope_ratio,
        "dist_atr": decision.dist_atr,
        "run_length": decision.run_length,
        "slope_regime": decision.slope_regime,
        "slope_regime_label": decision.slope_regime_label,
        "market_regime": decision.market_regime,
        "market_regime_label": decision.market_regime_label,
        "signal_grade": decision.signal_grade,
        "signal_name": decision.signal_name,
        "action": decision.action,
        "mode": decision.mode,
        "size_multiplier": decision.size_multiplier,
        "reason": decision.reason,
        "strikes": {
            "short_call": decision.strike_short_call,
            "long_call": decision.strike_long_call,
            "short_put": decision.strike_short_put,
            "long_put": decision.strike_long_put,
        },
        "stop_price": decision.stop_price,
        "take_profit_note": decision.take_profit_note,
        "filters_passed": list(decision.filters_passed),
        "filters_failed": list(decision.filters_failed),
    }
