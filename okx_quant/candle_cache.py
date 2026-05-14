from __future__ import annotations

import re
from pathlib import Path

from okx_quant.candle_store import (
    get_candles,
    get_candles_before,
    migrate_json_cache_file,
    upsert_candles,
)
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
    _migrate_legacy_json_if_needed(inst_id, bar, base_dir)
    return get_candles(inst_id, bar, limit=limit, base_dir=base_dir)


def load_candle_cache_range(
    inst_id: str,
    bar: str,
    *,
    start_ts: int,
    end_ts: int,
    limit: int | None = None,
    preload_count: int = 0,
    base_dir: Path | None = None,
) -> list[Candle]:
    _migrate_legacy_json_if_needed(inst_id, bar, base_dir)
    selected = get_candles(
        inst_id,
        bar,
        start_ts=start_ts,
        end_ts=end_ts,
        limit=limit,
        base_dir=base_dir,
    )
    preload = get_candles_before(
        inst_id,
        bar,
        before_ts=start_ts,
        limit=max(0, preload_count),
        base_dir=base_dir,
    )
    return merge_candles(preload, selected)


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
    upsert_candles(inst_id, bar, merged, base_dir=base_dir)
    return target


def _migrate_legacy_json_if_needed(inst_id: str, bar: str, base_dir: Path | None) -> None:
    target = candle_cache_file_path(inst_id, bar, base_dir)
    if target.exists():
        migrate_json_cache_file(inst_id, bar, target, base_dir=base_dir)
