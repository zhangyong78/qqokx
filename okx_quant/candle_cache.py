from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from decimal import Decimal

from okx_quant.models import Candle
from okx_quant.persistence import candle_cache_dir_path as _candle_cache_dir_path


DEFAULT_CANDLE_CACHE_CAPACITY = 12000
_SAFE_COMPONENT_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")


def candle_cache_dir_path(base_dir: Path | None = None) -> Path:
    return _candle_cache_dir_path(base_dir)


def candle_cache_file_path(inst_id: str, bar: str, base_dir: Path | None = None) -> Path:
    safe_inst_id = _SAFE_COMPONENT_PATTERN.sub("_", inst_id.strip().upper())
    safe_bar = _SAFE_COMPONENT_PATTERN.sub("_", bar.strip())
    return candle_cache_dir_path(base_dir) / f"{safe_inst_id}__{safe_bar}.json"


def merge_candles(*groups: list[Candle], max_records: int | None = None) -> list[Candle]:
    merged: dict[int, Candle] = {}
    for candles in groups:
        for candle in candles:
            merged[candle.ts] = candle
    ordered = [merged[ts] for ts in sorted(merged)]
    if max_records is not None and len(ordered) > max_records:
        ordered = ordered[-max_records:]
    return ordered


def load_candle_cache(
    inst_id: str,
    bar: str,
    *,
    limit: int | None = None,
    base_dir: Path | None = None,
) -> list[Candle]:
    target = candle_cache_file_path(inst_id, bar, base_dir)
    if not target.exists():
        return []
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = payload.get("candles") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []

    candles: list[Candle] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        try:
            candles.append(
                Candle(
                    ts=int(item["ts"]),
                    open=Decimal(str(item["open"])),
                    high=Decimal(str(item["high"])),
                    low=Decimal(str(item["low"])),
                    close=Decimal(str(item["close"])),
                    volume=Decimal(str(item.get("volume", "0"))),
                    confirmed=bool(item.get("confirmed", True)),
                )
            )
        except Exception:
            continue
    merged = merge_candles(candles)
    if limit is not None:
        return merged[-max(0, limit) :]
    return merged


def save_candle_cache(
    inst_id: str,
    bar: str,
    candles: list[Candle],
    *,
    max_records: int = DEFAULT_CANDLE_CACHE_CAPACITY,
    base_dir: Path | None = None,
) -> Path:
    target = candle_cache_file_path(inst_id, bar, base_dir)
    target.parent.mkdir(parents=True, exist_ok=True)
    merged = merge_candles(candles, max_records=max_records)
    payload = {
        "inst_id": inst_id,
        "bar": bar,
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "candles": [
            {
                "ts": candle.ts,
                "open": str(candle.open),
                "high": str(candle.high),
                "low": str(candle.low),
                "close": str(candle.close),
                "volume": str(candle.volume),
                "confirmed": candle.confirmed,
            }
            for candle in merged
        ],
    }
    temp_path = target.with_suffix(target.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(target)
    return target
