"""Versioned storage for local area-shape plans; never stores credentials."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from okx_quant.app_paths import state_dir_path
from okx_quant.persistence import _write_json_atomic


AREA_SHAPE_PLANS_FILE_NAME = "area_shape_trade_plans.json"
AREA_SHAPE_STORE_VERSION = 1


def area_shape_plans_file_path() -> Path:
    return state_dir_path() / AREA_SHAPE_PLANS_FILE_NAME


def load_area_shape_plans(path: Path | None = None) -> list[dict[str, object]]:
    target = path or area_shape_plans_file_path()
    if not target.exists():
        return []
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"区域计划文件无法读取或JSON损坏：{exc}") from exc
    if not isinstance(payload, dict) or int(payload.get("version", 0) or 0) != AREA_SHAPE_STORE_VERSION:
        raise ValueError("区域计划存储版本未知；文件已保留，需人工核对后才能继续")
    records = payload.get("plans")
    if not isinstance(records, list) or any(not isinstance(item, dict) for item in records):
        raise ValueError("区域计划记录结构无效；文件已保留，需人工核对后才能继续")
    return [dict(item) for item in records]


def save_area_shape_plans(plans: list[dict[str, object]], path: Path | None = None) -> Path:
    target = path or area_shape_plans_file_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    # Enforce the no-execution invariant even if a caller passes a malformed payload.
    safe_plans: list[dict[str, object]] = []
    for item in plans:
        if not isinstance(item, dict):
            continue
        record = dict(item)
        record["execution_enabled"] = False
        safe_plans.append(record)
    return _write_json_atomic(target, {"version": AREA_SHAPE_STORE_VERSION, "plans": safe_plans})

