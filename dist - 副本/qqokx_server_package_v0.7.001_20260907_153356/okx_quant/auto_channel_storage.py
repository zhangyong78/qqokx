from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from okx_quant.app_paths import state_dir_path
from okx_quant.models import Candle
from okx_quant.strategy_live_chart import (
    StrategyLiveChartBandOverlay,
    StrategyLiveChartBoxOverlay,
    StrategyLiveChartLineOverlay,
    StrategyLiveChartPointOverlay,
    StrategyLiveChartSnapshot,
)
from okx_quant.analysis.structure_models import PriceLine


AUTO_CHANNEL_SNAPSHOTS_FILE_NAME = "auto_channel_snapshots.json"


def auto_channel_snapshots_file_path(base_dir: Path | None = None) -> Path:
    if base_dir is not None:
        return Path(base_dir) / AUTO_CHANNEL_SNAPSHOTS_FILE_NAME
    return state_dir_path() / AUTO_CHANNEL_SNAPSHOTS_FILE_NAME


def load_auto_channel_snapshots(path: Path | None = None) -> list[dict[str, object]]:
    target = path or auto_channel_snapshots_file_path()
    if not target.exists():
        return []
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return []
    records = payload.get("records") if isinstance(payload, dict) else []
    if not isinstance(records, list):
        return []
    normalized: list[dict[str, object]] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        record_id = str(item.get("record_id", "") or "").strip()
        snapshot_payload = item.get("snapshot")
        if not record_id or not isinstance(snapshot_payload, dict):
            continue
        normalized.append(dict(item))
    normalized.sort(key=lambda item: str(item.get("saved_at", "")), reverse=True)
    return normalized


def save_auto_channel_snapshots(records: list[dict[str, object]], path: Path | None = None) -> Path:
    target = path or auto_channel_snapshots_file_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "records": list(records),
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    temp_path = target.with_suffix(target.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(target)
    return target


def build_auto_channel_snapshot_record(
    *,
    snapshot: StrategyLiveChartSnapshot,
    source_mode: str,
    symbol: str,
    bar: str,
    label: str,
    api_profile: str = "",
    candle_limit: int = 0,
) -> dict[str, object]:
    last_candle_ts = snapshot.candles[-1].ts if snapshot.candles else None
    display_label = label.strip() or f"{symbol} | {bar} | {_format_candle_ts(last_candle_ts)}"
    return {
        "record_id": uuid4().hex,
        "label": display_label,
        "source_mode": source_mode,
        "symbol": symbol,
        "bar": bar,
        "api_profile": api_profile,
        "candle_limit": int(candle_limit),
        "last_candle_ts": last_candle_ts,
        "saved_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "snapshot": serialize_strategy_live_chart_snapshot(snapshot),
    }


def _format_candle_ts(value: int | None) -> str:
    if value is None:
        return "-"
    try:
        return datetime.fromtimestamp(int(value) / 1000).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(value)


def serialize_strategy_live_chart_snapshot(snapshot: StrategyLiveChartSnapshot) -> dict[str, object]:
    return {
        "session_id": snapshot.session_id,
        "candles": [_serialize_candle(item) for item in snapshot.candles],
        "line_overlays": [_serialize_line_overlay(item) for item in snapshot.line_overlays],
        "band_overlays": [_serialize_band_overlay(item) for item in snapshot.band_overlays],
        "box_overlays": [_serialize_box_overlay(item) for item in snapshot.box_overlays],
        "point_overlays": [_serialize_point_overlay(item) for item in snapshot.point_overlays],
        "latest_price": _decimal_text(snapshot.latest_price),
        "latest_candle_confirmed": bool(snapshot.latest_candle_confirmed),
        "note": snapshot.note,
        "series_plot_end_index": snapshot.series_plot_end_index,
        "price_display_tick": _decimal_text(snapshot.price_display_tick),
        "right_pad_bars": int(snapshot.right_pad_bars),
    }


def deserialize_strategy_live_chart_snapshot(payload: dict[str, object]) -> StrategyLiveChartSnapshot:
    candles = tuple(_deserialize_candle(item) for item in payload.get("candles", []) or [])
    latest_candle_time = None
    if candles:
        latest_candle_time = datetime.fromtimestamp(candles[-1].ts / 1000)
    latest_price = _parse_decimal(payload.get("latest_price"))
    if latest_price is None and candles:
        latest_price = candles[-1].close
    return StrategyLiveChartSnapshot(
        session_id=str(payload.get("session_id", "") or "auto-channel:saved"),
        candles=candles,
        line_overlays=tuple(_deserialize_line_overlay(item) for item in payload.get("line_overlays", []) or []),
        band_overlays=tuple(_deserialize_band_overlay(item) for item in payload.get("band_overlays", []) or []),
        box_overlays=tuple(_deserialize_box_overlay(item) for item in payload.get("box_overlays", []) or []),
        point_overlays=tuple(_deserialize_point_overlay(item) for item in payload.get("point_overlays", []) or []),
        latest_price=latest_price,
        latest_candle_time=latest_candle_time,
        latest_candle_confirmed=bool(payload.get("latest_candle_confirmed", True)),
        note=str(payload.get("note", "") or ""),
        series_plot_end_index=_parse_optional_int(payload.get("series_plot_end_index")),
        price_display_tick=_parse_decimal(payload.get("price_display_tick")),
        right_pad_bars=max(0, _parse_optional_int(payload.get("right_pad_bars")) or 0),
    )


def _serialize_candle(candle: Candle) -> dict[str, object]:
    return {
        "ts": candle.ts,
        "open": _decimal_text(candle.open),
        "high": _decimal_text(candle.high),
        "low": _decimal_text(candle.low),
        "close": _decimal_text(candle.close),
        "volume": _decimal_text(candle.volume),
        "confirmed": candle.confirmed,
    }


def _deserialize_candle(payload: object) -> Candle:
    item = payload if isinstance(payload, dict) else {}
    return Candle(
        ts=int(item.get("ts", 0) or 0),
        open=_parse_decimal(item.get("open")) or Decimal("0"),
        high=_parse_decimal(item.get("high")) or Decimal("0"),
        low=_parse_decimal(item.get("low")) or Decimal("0"),
        close=_parse_decimal(item.get("close")) or Decimal("0"),
        volume=_parse_decimal(item.get("volume")) or Decimal("0"),
        confirmed=bool(item.get("confirmed", True)),
    )


def _serialize_price_line(line: PriceLine) -> dict[str, object]:
    return {
        "start_index": line.start_index,
        "start_price": _decimal_text(line.start_price),
        "end_index": line.end_index,
        "end_price": _decimal_text(line.end_price),
    }


def _deserialize_price_line(payload: object) -> PriceLine:
    item = payload if isinstance(payload, dict) else {}
    return PriceLine(
        start_index=int(item.get("start_index", 0) or 0),
        start_price=_parse_decimal(item.get("start_price")) or Decimal("0"),
        end_index=int(item.get("end_index", 0) or 0),
        end_price=_parse_decimal(item.get("end_price")) or Decimal("0"),
    )


def _serialize_line_overlay(overlay: StrategyLiveChartLineOverlay) -> dict[str, object]:
    return {
        "key": overlay.key,
        "label": overlay.label,
        "line": _serialize_price_line(overlay.line),
        "color": overlay.color,
        "dash": list(overlay.dash),
        "width": overlay.width,
    }


def _deserialize_line_overlay(payload: object) -> StrategyLiveChartLineOverlay:
    item = payload if isinstance(payload, dict) else {}
    return StrategyLiveChartLineOverlay(
        key=str(item.get("key", "") or ""),
        label=str(item.get("label", "") or ""),
        line=_deserialize_price_line(item.get("line")),
        color=str(item.get("color", "#2563eb") or "#2563eb"),
        dash=tuple(int(part) for part in item.get("dash", []) or []),
        width=int(item.get("width", 1) or 1),
    )


def _serialize_band_overlay(overlay: StrategyLiveChartBandOverlay) -> dict[str, object]:
    return {
        "key": overlay.key,
        "label": overlay.label,
        "start_index": overlay.start_index,
        "end_index": overlay.end_index,
        "upper_line": _serialize_price_line(overlay.upper_line),
        "lower_line": _serialize_price_line(overlay.lower_line),
        "outline": overlay.outline,
        "fill": overlay.fill,
        "stipple": overlay.stipple,
    }


def _deserialize_band_overlay(payload: object) -> StrategyLiveChartBandOverlay:
    item = payload if isinstance(payload, dict) else {}
    return StrategyLiveChartBandOverlay(
        key=str(item.get("key", "") or ""),
        label=str(item.get("label", "") or ""),
        start_index=int(item.get("start_index", 0) or 0),
        end_index=int(item.get("end_index", 0) or 0),
        upper_line=_deserialize_price_line(item.get("upper_line")),
        lower_line=_deserialize_price_line(item.get("lower_line")),
        outline=str(item.get("outline", "#2563eb") or "#2563eb"),
        fill=str(item.get("fill", "#dbeafe") or "#dbeafe"),
        stipple=str(item.get("stipple", "gray25") or "gray25"),
    )


def _serialize_box_overlay(overlay: StrategyLiveChartBoxOverlay) -> dict[str, object]:
    return {
        "key": overlay.key,
        "label": overlay.label,
        "start_index": overlay.start_index,
        "end_index": overlay.end_index,
        "upper": _decimal_text(overlay.upper),
        "lower": _decimal_text(overlay.lower),
        "outline": overlay.outline,
        "fill": overlay.fill,
        "stipple": overlay.stipple,
    }


def _deserialize_box_overlay(payload: object) -> StrategyLiveChartBoxOverlay:
    item = payload if isinstance(payload, dict) else {}
    return StrategyLiveChartBoxOverlay(
        key=str(item.get("key", "") or ""),
        label=str(item.get("label", "") or ""),
        start_index=int(item.get("start_index", 0) or 0),
        end_index=int(item.get("end_index", 0) or 0),
        upper=_parse_decimal(item.get("upper")) or Decimal("0"),
        lower=_parse_decimal(item.get("lower")) or Decimal("0"),
        outline=str(item.get("outline", "#a855f7") or "#a855f7"),
        fill=str(item.get("fill", "#f3e8ff") or "#f3e8ff"),
        stipple=str(item.get("stipple", "gray25") or "gray25"),
    )


def _serialize_point_overlay(overlay: StrategyLiveChartPointOverlay) -> dict[str, object]:
    return {
        "key": overlay.key,
        "label": overlay.label,
        "index": overlay.index,
        "price": _decimal_text(overlay.price),
        "color": overlay.color,
        "radius": overlay.radius,
    }


def _deserialize_point_overlay(payload: object) -> StrategyLiveChartPointOverlay:
    item = payload if isinstance(payload, dict) else {}
    return StrategyLiveChartPointOverlay(
        key=str(item.get("key", "") or ""),
        label=str(item.get("label", "") or ""),
        index=int(item.get("index", 0) or 0),
        price=_parse_decimal(item.get("price")) or Decimal("0"),
        color=str(item.get("color", "#16a34a") or "#16a34a"),
        radius=float(item.get("radius", 4.0) or 4.0),
    )


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _parse_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except Exception:
        return None


def _parse_optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None
