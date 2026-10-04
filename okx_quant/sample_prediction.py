from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from statistics import mean, median

from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import Candle


WEEK_MS = 7 * 24 * 60 * 60 * 1000
BTC_WEEKLY_INST_ID = "BTC-USDT-SWAP"
PREDICTION_MIN_CONFIDENCE = 0.60
PREDICTION_MIN_SAMPLE_COUNT = 20


@dataclass(frozen=True)
class PredictionResult:
    as_of_ts: int
    target_ts: int
    current_sign: str
    current_streak: int
    sample_count: int
    bullish_probability: float
    bearish_probability: float
    predicted_sign: str
    sample_rule: str
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    high_range: tuple[float, float]
    low_range: tuple[float, float]
    close_range: tuple[float, float]
    current_four_week_return: float
    current_volatility: float
    confidence: float = 0.0
    is_actionable: bool = True
    abstain_reason: str | None = None


@dataclass(frozen=True)
class BacktestResult:
    sample_count: int
    accuracy: float
    brier_score: float
    baseline_accuracy: float
    baseline_brier_score: float
    bullish_rate: float
    average_next_return: float
    rows: tuple[dict[str, object], ...]
    prediction_count: int = 0
    coverage: float = 0.0
    selective_accuracy: float = 0.0


def load_btc_weekly_candles() -> list[Candle]:
    """Load confirmed BTC weekly candles from the local cache only."""
    return [
        candle
        for candle in load_candle_cache(BTC_WEEKLY_INST_ID, "1W", limit=None)
        if candle.confirmed
    ]


def _sign(candle: Candle) -> str:
    if candle.close > candle.open:
        return "B"
    if candle.close < candle.open:
        return "S"
    return "N"


def _streak(candles: list[Candle], index: int) -> int:
    sign = _sign(candles[index])
    if sign == "N":
        return 0
    value = 1
    cursor = index - 1
    while cursor >= 0 and _sign(candles[cursor]) == sign:
        value += 1
        cursor -= 1
    return value


def _state(candles: list[Candle], index: int) -> tuple[str, int, float, float]:
    current = candles[index]
    returns = [
        float(candles[i].close / candles[i - 1].close - Decimal("1"))
        for i in range(max(1, index - 3), index + 1)
    ]
    four_week_return = float(
        current.close / candles[max(0, index - 3)].close - Decimal("1")
    )
    volatility = mean(abs(value) for value in returns) if returns else 0.0
    return _sign(current), _streak(candles, index), four_week_return, volatility


def _percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * min(max(fraction, 0.0), 1.0)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * weight


def _candidate_indexes(candles: list[Candle], current_index: int) -> tuple[list[int], str]:
    current_sign, current_streak, _return, _volatility = _state(candles, current_index)
    if current_sign == "N":
        candidates = [i for i in range(12, current_index) if _sign(candles[i]) != "N"]
        return candidates, "全部非平线样本"
    bucket = min(current_streak, 4)
    exact = [
        i
        for i in range(12, current_index)
        if _sign(candles[i]) == current_sign and min(_streak(candles, i), 4) == bucket
    ]
    if len(exact) >= 8:
        return exact, f"同方向、同连线长度区间（{bucket}+）"
    same_direction = [i for i in range(12, current_index) if _sign(candles[i]) == current_sign]
    if len(same_direction) >= 8:
        return same_direction, "同方向样本（连线样本不足，已放宽）"
    return list(range(12, current_index)), "全部历史样本（相似样本不足，已放宽）"


def _predict_from_history(candles: list[Candle], current_index: int) -> PredictionResult | None:
    if current_index < 24 or current_index >= len(candles):
        return None
    current = candles[current_index]
    candidates, rule = _candidate_indexes(candles, current_index)
    usable = [i for i in candidates if i + 1 < len(candles)]
    if not usable:
        return None
    outcomes = [_sign(candles[i + 1]) for i in usable]
    bullish = sum(outcome == "B" for outcome in outcomes)
    bearish = sum(outcome == "S" for outcome in outcomes)
    base = float(current.close)
    open_changes = [float(candles[i + 1].open / candles[i].close - Decimal("1")) for i in usable]
    high_changes = [float(candles[i + 1].high / candles[i].close - Decimal("1")) for i in usable]
    low_changes = [float(candles[i + 1].low / candles[i].close - Decimal("1")) for i in usable]
    close_changes = [float(candles[i + 1].close / candles[i].close - Decimal("1")) for i in usable]

    open_price = base * (1 + _percentile(open_changes, 0.50))
    close_price = base * (1 + _percentile(close_changes, 0.50))
    high_price = max(open_price, close_price, base * (1 + _percentile(high_changes, 0.50)))
    low_price = min(open_price, close_price, base * (1 + _percentile(low_changes, 0.50)))
    four_week_return = _state(candles, current_index)[2]
    volatility = _state(candles, current_index)[3]
    confidence = max(bullish / len(usable), bearish / len(usable))
    is_actionable = (
        confidence >= PREDICTION_MIN_CONFIDENCE
        and len(usable) >= PREDICTION_MIN_SAMPLE_COUNT
    )
    abstain_reason: str | None = None
    if not is_actionable:
        reasons: list[str] = []
        if confidence < PREDICTION_MIN_CONFIDENCE:
            reasons.append(f"置信度 {confidence:.1%} 低于 {PREDICTION_MIN_CONFIDENCE:.0%}")
        if len(usable) < PREDICTION_MIN_SAMPLE_COUNT:
            reasons.append(f"相似样本 {len(usable)} 条少于 {PREDICTION_MIN_SAMPLE_COUNT} 条")
        abstain_reason = "；".join(reasons)
    return PredictionResult(
        as_of_ts=current.ts,
        target_ts=current.ts + WEEK_MS,
        current_sign=_sign(current),
        current_streak=_streak(candles, current_index),
        sample_count=len(usable),
        bullish_probability=bullish / len(usable),
        bearish_probability=bearish / len(usable),
        predicted_sign="B" if bullish >= bearish else "S",
        sample_rule=rule,
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        close_price=close_price,
        high_range=(base * (1 + _percentile(high_changes, 0.25)), base * (1 + _percentile(high_changes, 0.75))),
        low_range=(base * (1 + _percentile(low_changes, 0.25)), base * (1 + _percentile(low_changes, 0.75))),
        close_range=(base * (1 + _percentile(close_changes, 0.25)), base * (1 + _percentile(close_changes, 0.75))),
        current_four_week_return=four_week_return,
        current_volatility=volatility,
        confidence=confidence,
        is_actionable=is_actionable,
        abstain_reason=abstain_reason,
    )


def predict_next_week(candles: list[Candle]) -> PredictionResult | None:
    confirmed = [candle for candle in candles if candle.confirmed]
    return _predict_from_history(confirmed, len(confirmed) - 1) if confirmed else None


def run_weekly_backtest(candles: list[Candle], *, minimum_history: int = 48) -> BacktestResult:
    confirmed = [candle for candle in candles if candle.confirmed]
    rows: list[dict[str, object]] = []
    for current_index in range(minimum_history, len(confirmed) - 1):
        prediction = _predict_from_history(confirmed, current_index)
        if prediction is None:
            continue
        actual = confirmed[current_index + 1]
        actual_sign = _sign(actual)
        actual_bull = 1.0 if actual_sign == "B" else 0.0
        next_return = float(actual.close / confirmed[current_index].close - Decimal("1"))
        prior_signs = [_sign(item) for item in confirmed[: current_index + 1]]
        prior_directional = [sign for sign in prior_signs if sign in {"B", "S"}]
        baseline_probability = prior_directional.count("B") / len(prior_directional)
        rows.append(
            {
                "as_of_ts": confirmed[current_index].ts,
                "target_ts": actual.ts,
                "bullish_probability": prediction.bullish_probability,
                "predicted_sign": prediction.predicted_sign,
                "actual_sign": actual_sign,
                "raw_correct": prediction.predicted_sign == actual_sign,
                "correct": prediction.is_actionable and prediction.predicted_sign == actual_sign,
                "is_actionable": prediction.is_actionable,
                "prediction_status": "PREDICT" if prediction.is_actionable else "ABSTAIN",
                "confidence": prediction.confidence,
                "sample_count": prediction.sample_count,
                "brier": (prediction.bullish_probability - actual_bull) ** 2,
                "baseline_probability": baseline_probability,
                "next_return": next_return,
            }
        )
    if not rows:
        return BacktestResult(0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, ())
    baseline_correct = []
    baseline_brier = []
    for row in rows:
        probability = float(row["baseline_probability"])
        actual_bull = 1.0 if row["actual_sign"] == "B" else 0.0
        baseline_correct.append((probability >= 0.5) == bool(actual_bull))
        baseline_brier.append((probability - actual_bull) ** 2)
    actionable_rows = [row for row in rows if bool(row["is_actionable"])]
    selective_accuracy = (
        sum(bool(row["correct"]) for row in actionable_rows) / len(actionable_rows)
        if actionable_rows
        else 0.0
    )
    return BacktestResult(
        sample_count=len(rows),
        accuracy=sum(bool(row["raw_correct"]) for row in rows) / len(rows),
        brier_score=mean(float(row["brier"]) for row in rows),
        baseline_accuracy=mean(baseline_correct),
        baseline_brier_score=mean(baseline_brier),
        bullish_rate=mean(float(row["actual_sign"] == "B") for row in rows),
        average_next_return=mean(float(row["next_return"]) for row in rows),
        rows=tuple(rows),
        prediction_count=len(actionable_rows),
        coverage=len(actionable_rows) / len(rows),
        selective_accuracy=selective_accuracy,
    )
