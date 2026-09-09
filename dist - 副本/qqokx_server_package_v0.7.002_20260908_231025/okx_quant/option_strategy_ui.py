from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
import math
from pathlib import Path

from okx_quant.deribit_client import DeribitVolatilityCandle
from okx_quant.models import Candle, Instrument
from okx_quant.okx_client import OkxPosition, OkxTicker
from okx_quant.option_strategy import (
    OptionQuote,
    StrategyLegDefinition,
    parse_option_contract,
)
from okx_quant.persistence import (
    deribit_volatility_cache_file_path,
)
from okx_quant.pricing import decimal_places_for_increment, format_decimal, format_decimal_by_increment, format_decimal_fixed


BAR_OPTIONS = ["1m", "3m", "5m", "15m", "1H", "4H"]
DEFAULT_OPTION_FAMILY_OPTIONS = ("BTC-USD", "ETH-USD", "BTC-USDT", "ETH-USDT")
MAX_OPTION_COMBO_CANDLES = 2000
DERIBIT_OPTION_CHART_BAR_TO_RESOLUTION = {
    "1m": "3600",
    "3m": "3600",
    "5m": "3600",
    "15m": "3600",
    "1H": "3600",
    "4H": "14400",
    "1D": "86400",
}


def _build_option_quote(instrument: Instrument, ticker: OkxTicker | None) -> OptionQuote:
    return OptionQuote(
        instrument=instrument,
        mark_price=ticker.mark if ticker is not None else None,
        bid_price=ticker.bid if ticker is not None else None,
        ask_price=ticker.ask if ticker is not None else None,
        last_price=ticker.last if ticker is not None else None,
        index_price=ticker.index if ticker is not None else None,
    )


def _instrument_option_family(instrument: Instrument) -> str | None:
    fam = (instrument.inst_family or "").strip().upper()
    if fam:
        return fam
    try:
        return parse_option_contract(instrument.inst_id).inst_family
    except ValueError:
        return None


def _filter_option_instruments_by_family(family: str, instruments: list[Instrument]) -> list[Instrument]:
    """OKX 用 uly 拉期权时可能混进币本位与 U 本位；只保留与所选 instFamily 一致的合约。"""
    normalized = family.strip().upper()
    if not normalized:
        return []
    return [item for item in instruments if _instrument_option_family(item) == normalized]


def _filter_option_tickers_by_family(family: str, tickers: list[OkxTicker]) -> list[OkxTicker]:
    normalized = family.strip().upper()
    if not normalized:
        return []
    out: list[OkxTicker] = []
    for ticker in tickers:
        try:
            if parse_option_contract(ticker.inst_id).inst_family == normalized:
                out.append(ticker)
        except ValueError:
            continue
    return out


def _spot_usdt_inst_id(inst_family: str | None) -> str | None:
    if not inst_family:
        return None
    base = inst_family.strip().upper().split("-", 1)[0]
    if not base or base == "USDT":
        return None
    return f"{base}-USDT"


def _filter_option_positions(
    positions: list[OkxPosition],
    *,
    family: str,
    expiry_code: str | None = None,
) -> list[OkxPosition]:
    normalized_family = family.strip().upper()
    normalized_expiry = expiry_code.strip() if expiry_code else None
    filtered: list[OkxPosition] = []
    for position in positions:
        try:
            parsed = parse_option_contract(position.inst_id)
        except Exception:
            continue
        if parsed.inst_family != normalized_family:
            continue
        if normalized_expiry and parsed.expiry_code != normalized_expiry:
            continue
        filtered.append(position)
    filtered.sort(
        key=lambda item: (
            parse_option_contract(item.inst_id).expiry_code,
            parse_option_contract(item.inst_id).strike,
            parse_option_contract(item.inst_id).option_type,
            item.inst_id,
        )
    )
    return filtered


def _position_side_and_quantity(position: OkxPosition) -> tuple[str, Decimal]:
    pos_side = position.pos_side.strip().lower()
    if pos_side == "short":
        return "sell", abs(position.position)
    if pos_side == "long":
        return "buy", abs(position.position)
    return ("buy", position.position) if position.position >= 0 else ("sell", abs(position.position))


def _native_display_currency(
    legs: list[StrategyLegDefinition],
    instrument_map: dict[str, Instrument],
) -> str:
    for leg in legs:
        instrument = instrument_map.get(leg.inst_id)
        if instrument is not None and instrument.ct_val_ccy:
            return instrument.ct_val_ccy.upper()
    for instrument in instrument_map.values():
        if instrument.ct_val_ccy:
            return instrument.ct_val_ccy.upper()
    return "结算币"


def _strategy_leg_quote_currency(inst_id: str, instrument_map: dict[str, Instrument]) -> str:
    instrument = instrument_map.get(inst_id)
    if instrument is not None and instrument.ct_val_ccy:
        return instrument.ct_val_ccy.strip().upper()
    try:
        parsed = parse_option_contract(inst_id)
    except ValueError:
        return ""
    family_parts = parsed.inst_family.split("-", 1)
    return family_parts[0].strip().upper() if family_parts else ""


def _format_price(value: Decimal | None, tick_size: Decimal | None) -> str:
    if value is None:
        return "-"
    if tick_size is None:
        return format_decimal(value)
    places = decimal_places_for_increment(tick_size)
    if places is None:
        return format_decimal(value)
    if places <= 8:
        return format_decimal_by_increment(value, tick_size)
    return format_decimal_fixed(value, min(places, 10))


def _axis_values(min_value: Decimal, max_value: Decimal, *, steps: int) -> list[Decimal]:
    if steps <= 0:
        return [min_value, max_value]
    interval = (max_value - min_value) / Decimal(steps)
    return [min_value + (interval * Decimal(index)) for index in range(steps + 1)]


def _format_axis_value(value: Decimal) -> str:
    magnitude = abs(value)
    if magnitude >= 1000:
        return format_decimal_fixed(value, 2)
    if magnitude >= 1:
        return format_decimal_fixed(value, 4)
    return format_decimal_fixed(value, 6)


def _format_compact_number(value: Decimal | int | float | None) -> str:
    if value is None:
        return "-"
    if not isinstance(value, Decimal):
        try:
            value = Decimal(str(value))
        except Exception:
            return "-"
    return format_decimal_fixed(value, 4).rstrip("0").rstrip(".") or "0"


def _format_signed_percent(value: Decimal) -> str:
    prefix = "+" if value > 0 else ""
    return f"{prefix}{format_decimal_fixed(value, 1)}%"


def _index_markers(length: int, *, target_count: int) -> list[int]:
    if length <= 0:
        return []
    if length <= target_count:
        return list(range(length))
    step = max((length - 1) // max(target_count - 1, 1), 1)
    values = list(range(0, length, step))
    if values[-1] != length - 1:
        values.append(length - 1)
    return values


def _nearest_linear_index(x: float, left: float, right: float, length: int) -> int:
    if length <= 1:
        return 0
    span = max(right - left, 1.0)
    ratio = (x - left) / span
    index = int(round(ratio * (length - 1)))
    return max(0, min(length - 1, index))


def _nearest_candle_index(x: float, left: float, candle_step: float, length: int) -> int:
    if length <= 1:
        return 0
    effective_step = max(candle_step, 1.0)
    index = int(round((x - left - (effective_step / 2)) / effective_step))
    return max(0, min(length - 1, index))


def _format_chart_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000).strftime("%m-%d %H:%M")


def _option_strategy_deribit_cache_file_path() -> Path:
    return deribit_volatility_cache_file_path()


def _load_deribit_option_chart_cache_payload() -> dict:
    cache_path = _option_strategy_deribit_cache_file_path()
    if not cache_path.exists():
        return {}
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _load_deribit_hourly_series_from_cache(currency: str) -> list[DeribitVolatilityCandle]:
    payload = _load_deribit_option_chart_cache_payload()
    item = payload.get(f"{currency.strip().upper()}|hourly_base")
    if not isinstance(item, dict):
        return []
    candles: list[DeribitVolatilityCandle] = []
    for raw in item.get("volatility_hourly", []):
        if not isinstance(raw, dict):
            continue
        try:
            candles.append(
                DeribitVolatilityCandle(
                    ts=int(raw["ts"]),
                    open=Decimal(str(raw["open"])),
                    high=Decimal(str(raw["high"])),
                    low=Decimal(str(raw["low"])),
                    close=Decimal(str(raw["close"])),
                )
            )
        except Exception:
            continue
    candles.sort(key=lambda item: item.ts)
    return candles


def _aggregate_deribit_option_chart_candles(
    candles: list[DeribitVolatilityCandle],
    resolution_ms: int,
) -> list[DeribitVolatilityCandle]:
    if not candles:
        return []
    grouped: list[list[DeribitVolatilityCandle]] = []
    current_bucket: int | None = None
    current_group: list[DeribitVolatilityCandle] = []
    for candle in candles:
        bucket = candle.ts // resolution_ms
        if current_bucket is None or bucket != current_bucket:
            if current_group:
                grouped.append(current_group)
            current_bucket = bucket
            current_group = [candle]
        else:
            current_group.append(candle)
    if current_group:
        grouped.append(current_group)

    aggregated: list[DeribitVolatilityCandle] = []
    for group in grouped:
        aggregated.append(
            DeribitVolatilityCandle(
                ts=group[0].ts,
                open=group[0].open,
                high=max(item.high for item in group),
                low=min(item.low for item in group),
                close=group[-1].close,
            )
        )
    return aggregated


def _build_deribit_option_chart_candles(
    hourly_candles: list[DeribitVolatilityCandle],
    *,
    bar: str,
    requested_limit: int,
) -> tuple[list[Candle], str, str]:
    normalized_bar = bar.strip().upper()
    selected = list(hourly_candles)
    resolution_note = ""
    if normalized_bar == "4H":
        selected = _aggregate_deribit_option_chart_candles(selected, 14_400_000)
        resolution_label = "4小时"
    elif normalized_bar in {"1D", "1DAY"}:
        selected = _aggregate_deribit_option_chart_candles(selected, 86_400_000)
        resolution_label = "日线"
        resolution_note = "Deribit 最小粒度为1小时，日线由小时线合成"
    else:
        resolution_label = "1小时"
        if normalized_bar not in {"1H", ""}:
            resolution_note = "Deribit 最小周期为1小时，当前按1小时指数显示"

    if requested_limit > 0 and selected:
        selected = selected[-requested_limit:]
    chart_candles = [
        Candle(
            ts=item.ts,
            open=item.open,
            high=item.high,
            low=item.low,
            close=item.close,
            volume=Decimal("0"),
            confirmed=True,
        )
        for item in selected
    ]
    return chart_candles, resolution_label, resolution_note


def _load_deribit_option_chart_candles(
    currency: str,
    *,
    bar: str,
    requested_limit: int,
) -> tuple[list[Candle], str, str]:
    hourly_candles = _load_deribit_hourly_series_from_cache(currency)
    if not hourly_candles:
        nb = bar.strip().upper()
        if nb == "4H":
            resolution_label = "4小时"
            resolution_note = ""
        elif nb in {"1D", "1DAY"}:
            resolution_label = "日线"
            resolution_note = "Deribit 最小粒度为1小时，日线由小时线合成"
        else:
            resolution_label = "1小时"
            resolution_note = "" if nb in {"1H", ""} else "Deribit 最小周期为1小时，当前按1小时指数显示"
        return [], resolution_label, resolution_note
    return _build_deribit_option_chart_candles(
        hourly_candles,
        bar=bar,
        requested_limit=requested_limit,
    )


def _annualization_factor_for_bar(bar: str) -> float:
    normalized = bar.strip()
    periods_per_year = {
        "1m": 365 * 24 * 60,
        "3m": (365 * 24 * 60) / 3,
        "5m": (365 * 24 * 60) / 5,
        "15m": (365 * 24 * 60) / 15,
        "1H": 365 * 24,
        "4H": 365 * 6,
    }.get(normalized)
    if periods_per_year is None:
        return 0.0
    return math.sqrt(periods_per_year)


def _build_volatility_candles_from_reference(
    reference_candles: list[Candle],
    *,
    bar: str,
    lookback: int = 20,
) -> list[Candle]:
    confirmed = [item for item in reference_candles if item.confirmed]
    if len(confirmed) < lookback + 1:
        return []

    annualization = _annualization_factor_for_bar(bar)
    if annualization <= 0:
        return []

    volatility_candles: list[Candle] = []
    previous_close_vol: float | None = None
    for index in range(lookback, len(confirmed)):
        closes = [float(item.close) for item in confirmed[index - lookback : index + 1]]
        if any(value <= 0 for value in closes):
            continue
        returns = [math.log(closes[offset] / closes[offset - 1]) for offset in range(1, len(closes))]
        if not returns:
            continue
        mean_return = sum(returns) / len(returns)
        variance = sum((value - mean_return) ** 2 for value in returns) / len(returns)
        close_vol = math.sqrt(max(variance, 0.0)) * annualization * 100.0
        open_vol = previous_close_vol if previous_close_vol is not None else close_vol
        high_vol = max(open_vol, close_vol)
        low_vol = min(open_vol, close_vol)
        candle = confirmed[index]
        volatility_candles.append(
            Candle(
                ts=candle.ts,
                open=Decimal(str(open_vol)),
                high=Decimal(str(high_vol)),
                low=Decimal(str(low_vol)),
                close=Decimal(str(close_vol)),
                volume=Decimal("0"),
                confirmed=candle.confirmed,
            )
        )
        previous_close_vol = close_vol
    return volatility_candles


def _align_overlay_candles(
    combo_candles: list[Candle],
    volatility_candles: list[Candle],
) -> list[tuple[Candle, Candle]]:
    if not combo_candles or not volatility_candles:
        return []
    volatility_by_ts = {item.ts: item for item in volatility_candles}
    aligned: list[tuple[Candle, Candle]] = []
    for combo_candle in combo_candles:
        volatility_candle = volatility_by_ts.get(combo_candle.ts)
        if volatility_candle is not None:
            aligned.append((combo_candle, volatility_candle))
    return aligned


def _align_overlay_three_series(
    combo_candles: list[Candle],
    volatility_candles: list[Candle],
    spot_candles: list[Candle],
) -> list[tuple[Candle, Candle, Candle]]:
    if not combo_candles or not volatility_candles or not spot_candles:
        return []
    volatility_by_ts = {item.ts: item for item in volatility_candles}
    spot_by_ts = {item.ts: item for item in spot_candles}
    aligned: list[tuple[Candle, Candle, Candle]] = []
    for combo_candle in combo_candles:
        volatility_candle = volatility_by_ts.get(combo_candle.ts)
        spot_candle = spot_by_ts.get(combo_candle.ts)
        if volatility_candle is not None and spot_candle is not None:
            aligned.append((combo_candle, volatility_candle, spot_candle))
    return aligned


def _normalized_kline_view(
    length: int,
    start_index: int,
    visible_count: int | None,
    *,
    auto_full: bool,
    min_visible: int = 30,
) -> tuple[int, int]:
    if length <= 0:
        return 0, 0
    if auto_full or visible_count is None or visible_count >= length:
        return 0, length
    effective_min = min(length, max(min_visible, 1))
    visible = max(effective_min, min(length, visible_count))
    start = max(0, min(start_index, max(length - visible, 0)))
    return start, visible


def _zoom_kline_view(
    length: int,
    start_index: int,
    visible_count: int,
    *,
    focus_ratio: float,
    zoom_in: bool,
    min_visible: int = 30,
) -> tuple[int, int]:
    if length <= 0:
        return 0, 0
    focus_ratio = min(1.0, max(0.0, focus_ratio))
    current_visible = max(1, min(length, visible_count))
    effective_min = min(length, max(min_visible, 1))
    next_visible = max(effective_min, int(round(current_visible * (0.8 if zoom_in else 1.25))))
    next_visible = max(effective_min, min(length, next_visible))
    focus_index = start_index + int(round((current_visible - 1) * focus_ratio))
    next_start = focus_index - int(round((next_visible - 1) * focus_ratio))
    next_start = max(0, min(next_start, max(length - next_visible, 0)))
    return next_start, next_visible


def _pan_kline_view(length: int, start_index: int, visible_count: int, *, delta_items: int) -> tuple[int, int]:
    if length <= 0:
        return 0, 0
    visible = max(1, min(length, visible_count))
    next_start = start_index + delta_items
    next_start = max(0, min(next_start, max(length - visible, 0)))
    return next_start, visible
