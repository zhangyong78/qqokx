"""Versioned execution-only persistence for area-shape demo sessions."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from okx_quant.app_paths import state_dir_path
from okx_quant.persistence import _write_json_atomic


AREA_SHAPE_EXECUTION_STORE_VERSION = 1
AREA_SHAPE_EXECUTION_FILE_NAME = "area_shape_demo_execution.json"


def area_shape_execution_file_path() -> Path:
    return state_dir_path() / AREA_SHAPE_EXECUTION_FILE_NAME


def load_area_shape_execution_records(path: Path | None = None) -> list[dict[str, Any]]:
    target = path or area_shape_execution_file_path()
    if not target.exists():
        return []
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"区域demo执行状态无法读取：{exc}") from exc
    if not isinstance(payload, dict) or payload.get("version") != AREA_SHAPE_EXECUTION_STORE_VERSION:
        raise ValueError("区域demo执行状态schema未知；保留原文件并停止所有写入")
    records = payload.get("sessions")
    if not isinstance(records, list) or any(not isinstance(item, dict) for item in records):
        raise ValueError("区域demo执行状态结构无效；保留原文件并停止所有写入")
    return [dict(item) for item in records]


def save_area_shape_execution_records(records: list[dict[str, Any]], path: Path | None = None) -> Path:
    target = path or area_shape_execution_file_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    safe: list[dict[str, Any]] = []
    for item in records:
        if not isinstance(item, dict):
            raise ValueError("区域demo执行记录必须是对象")
        record = dict(item)
        # Secret material is never allowed into this separate execution ledger.
        for key in ("api_key", "secret_key", "passphrase", "secret", "apiSecret"):
            record.pop(key, None)
        record["mode"] = "demo_exchange"
        safe.append(record)
    return _write_json_atomic(target, {"version": AREA_SHAPE_EXECUTION_STORE_VERSION, "sessions": safe})
