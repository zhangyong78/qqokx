from __future__ import annotations

import pytest

from okx_quant.area_shape_monitor import AreaShapeMonitor
from okx_quant.area_shape_store import load_area_shape_plans, save_area_shape_plans


def test_unknown_store_version_is_reported_and_never_overwritten(tmp_path) -> None:
    path = tmp_path / "area_shape_trade_plans.json"
    original = '{"version": 88, "plans": [{"sentinel": "keep"}]}\n'
    path.write_text(original, encoding="utf-8")

    with pytest.raises(ValueError, match="版本未知"):
        load_area_shape_plans(path)

    monitor = AreaShapeMonitor(store_load=lambda: load_area_shape_plans(path), store_save=lambda plans: save_area_shape_plans(plans, path))
    assert monitor._load_errors
    with pytest.raises(RuntimeError, match="保护原文件"):
        monitor._persist_locked()
    assert path.read_text(encoding="utf-8") == original


def test_corrupt_json_is_visible_and_preserved(tmp_path) -> None:
    path = tmp_path / "area_shape_trade_plans.json"
    original = "{not-json"
    path.write_text(original, encoding="utf-8")
    monitor = AreaShapeMonitor(store_load=lambda: load_area_shape_plans(path))

    assert any("JSON损坏" in item for item in monitor._load_errors)
    with pytest.raises(RuntimeError):
        monitor._persist_locked()
    assert path.read_text(encoding="utf-8") == original
