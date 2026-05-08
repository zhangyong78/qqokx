from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from okx_quant.models import Candle


ZERO = Decimal("0")


@dataclass(frozen=True)
class ReplayWindowResult:
    hours: int
    end_ts: int | None
    close_price: Decimal | None
    return_pct: Decimal | None


@dataclass(frozen=True)
class ReplayValidation:
    status: str
    timeframe: str
    entry_price: Decimal | None
    analysis_candle_ts: int | None
    verdict: str
    max_favorable_excursion_pct: Decimal | None
    max_adverse_excursion_pct: Decimal | None
    windows: tuple[ReplayWindowResult, ...]


def build_replay_validation(
    *,
    direction: str,
    timeframe: str,
    entry_price: Decimal | None,
    analysis_candle_ts: int | None,
    future_candles: list[Candle],
    timeframe_ms: int,
    windows_hours: tuple[int, ...] = (4, 12, 24),
) -> ReplayValidation:
    ordered = sorted((item for item in future_candles if item.confirmed), key=lambda item: item.ts)
    if entry_price is None or entry_price <= ZERO or analysis_candle_ts is None:
        return ReplayValidation(
            status="skipped",
            timeframe=timeframe,
            entry_price=entry_price,
            analysis_candle_ts=analysis_candle_ts,
            verdict="insufficient_context",
            max_favorable_excursion_pct=None,
            max_adverse_excursion_pct=None,
            windows=(),
        )
    if not ordered:
        return ReplayValidation(
            status="pending",
            timeframe=timeframe,
            entry_price=entry_price,
            analysis_candle_ts=analysis_candle_ts,
            verdict="pending",
            max_favorable_excursion_pct=None,
            max_adverse_excursion_pct=None,
            windows=(),
        )

    normalized_direction = str(direction or "").strip().lower()
    if normalized_direction == "long":
        mfe = max((((item.high - entry_price) / entry_price) * Decimal("100")) for item in ordered)
        mae = min((((item.low - entry_price) / entry_price) * Decimal("100")) for item in ordered)
    elif normalized_direction == "short":
        mfe = max((((entry_price - item.low) / entry_price) * Decimal("100")) for item in ordered)
        mae = min((((entry_price - item.high) / entry_price) * Decimal("100")) for item in ordered)
    else:
        mfe = None
        mae = None

    window_results: list[ReplayWindowResult] = []
    for hours in windows_hours:
        if hours <= 0:
            continue
        end_ts = analysis_candle_ts + (hours * 3_600_000)
        chosen = _latest_candle_before_or_at(ordered, end_ts)
        if chosen is None:
            window_results.append(ReplayWindowResult(hours=hours, end_ts=None, close_price=None, return_pct=None))
            continue
        window_results.append(
            ReplayWindowResult(
                hours=hours,
                end_ts=chosen.ts,
                close_price=chosen.close,
                return_pct=_directional_return_pct(normalized_direction, entry_price, chosen.close),
            )
        )

    verdict = _resolve_verdict(normalized_direction, window_results, mfe, mae)
    return ReplayValidation(
        status="completed",
        timeframe=timeframe,
        entry_price=entry_price,
        analysis_candle_ts=analysis_candle_ts,
        verdict=verdict,
        max_favorable_excursion_pct=mfe,
        max_adverse_excursion_pct=mae,
        windows=tuple(window_results),
    )


def replay_validation_payload(validation: ReplayValidation | None) -> dict[str, object] | None:
    if validation is None:
        return None
    return {
        "status": validation.status,
        "timeframe": validation.timeframe,
        "entry_price": _decimal_text(validation.entry_price),
        "analysis_candle_ts": validation.analysis_candle_ts,
        "verdict": validation.verdict,
        "review_windows": [f"{item.hours}H" for item in validation.windows],
        "max_favorable_excursion_pct": _decimal_text(validation.max_favorable_excursion_pct),
        "max_adverse_excursion_pct": _decimal_text(validation.max_adverse_excursion_pct),
        "windows": [
            {
                "hours": item.hours,
                "end_ts": item.end_ts,
                "close_price": _decimal_text(item.close_price),
                "return_pct": _decimal_text(item.return_pct),
            }
            for item in validation.windows
        ],
    }


def _latest_candle_before_or_at(candles: list[Candle], end_ts: int) -> Candle | None:
    chosen: Candle | None = None
    for candle in candles:
        if candle.ts <= end_ts:
            chosen = candle
        else:
            break
    return chosen


def _directional_return_pct(direction: str, entry_price: Decimal, close_price: Decimal) -> Decimal:
    if direction == "short":
        return ((entry_price - close_price) / entry_price) * Decimal("100")
    return ((close_price - entry_price) / entry_price) * Decimal("100")


def _resolve_verdict(
    direction: str,
    windows: list[ReplayWindowResult],
    mfe: Decimal | None,
    mae: Decimal | None,
) -> str:
    completed = [item for item in windows if item.return_pct is not None]
    if not completed:
        return "pending"
    if direction not in {"long", "short"}:
        return "observe"
    positive_count = sum(1 for item in completed if item.return_pct is not None and item.return_pct > ZERO)
    strong_positive = any(item.return_pct is not None and item.return_pct >= Decimal("1") for item in completed)
    if positive_count == len(completed) and strong_positive:
        return "effective"
    if positive_count > 0:
        if mfe is not None and mae is not None and mfe > abs(mae):
            return "partially_effective"
        return "mixed"
    return "invalid"


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")
