from __future__ import annotations

import json
import os
import shutil
import threading
from datetime import datetime, timezone
from pathlib import Path


APP_DATA_DIR_ENV_VAR = "QQOKX_DATA_DIR"
DEFAULT_DATA_DIR_NAME = "qqokx_data"
DATA_LAYOUT_VERSION = 1
DATA_LAYOUT_FILE_NAME = ".qqokx_data_layout.json"

_DATA_ROOT_OVERRIDE: Path | None = None
_DATA_ROOT_LOCK = threading.Lock()
_INITIALIZED_ROOTS: set[Path] = set()


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def default_data_root() -> Path:
    return project_root().parent / DEFAULT_DATA_DIR_NAME


def configure_data_root(path: str | Path | None) -> Path:
    global _DATA_ROOT_OVERRIDE
    if path is None:
        _DATA_ROOT_OVERRIDE = None
    else:
        _DATA_ROOT_OVERRIDE = Path(path).expanduser().resolve()
    return data_root()


def configured_data_root() -> Path | None:
    return _DATA_ROOT_OVERRIDE


def data_root() -> Path:
    candidate = _DATA_ROOT_OVERRIDE
    if candidate is None:
        raw = os.environ.get(APP_DATA_DIR_ENV_VAR, "").strip()
        candidate = Path(raw).expanduser() if raw else default_data_root()
    resolved = candidate.resolve()
    _ensure_data_root_initialized(resolved)
    return resolved


def config_dir_path() -> Path:
    return _config_dir_for(data_root())


def cache_dir_path() -> Path:
    return _cache_dir_for(data_root())


def state_dir_path() -> Path:
    return _state_dir_for(data_root())


def logs_dir_path() -> Path:
    return _logs_dir_for(data_root())


def reports_dir_path() -> Path:
    return _reports_dir_for(data_root())


def live_strategy_sessions_dir_path() -> Path:
    return reports_dir_path() / "live_strategy_sessions"


def analysis_reports_dir_path() -> Path:
    return reports_dir_path() / "analysis"


def deribit_reports_dir_path() -> Path:
    return reports_dir_path() / "deribit"


def _config_dir_for(root: Path) -> Path:
    return root / "config"


def _cache_dir_for(root: Path) -> Path:
    return root / "cache"


def _state_dir_for(root: Path) -> Path:
    return root / "state"


def _logs_dir_for(root: Path) -> Path:
    return root / "logs"


def _reports_dir_for(root: Path) -> Path:
    return root / "reports"


def _data_layout_file_path(root: Path) -> Path:
    return root / DATA_LAYOUT_FILE_NAME


def _ensure_data_root_initialized(root: Path) -> None:
    with _DATA_ROOT_LOCK:
        if root in _INITIALIZED_ROOTS:
            return
        root.mkdir(parents=True, exist_ok=True)
        for path in (
            _config_dir_for(root),
            _cache_dir_for(root),
            _state_dir_for(root),
            _logs_dir_for(root),
            _reports_dir_for(root),
        ):
            path.mkdir(parents=True, exist_ok=True)
        _bootstrap_from_legacy_layout(root)
        _write_data_layout_file(root)
        _INITIALIZED_ROOTS.add(root)


def _bootstrap_from_legacy_layout(root: Path) -> None:
    layout_file = _data_layout_file_path(root)
    if layout_file.exists():
        return
    source_root = project_root()
    if root == source_root:
        return
    for source, target in _legacy_data_mappings(source_root, root):
        _copy_legacy_path(source, target)


def _legacy_data_mappings(source_root: Path, target_root: Path) -> tuple[tuple[Path, Path], ...]:
    return (
        (source_root / ".okx_quant_credentials.json", _config_dir_for(target_root) / "credentials.json"),
        (source_root / ".okx_quant_settings.json", _config_dir_for(target_root) / "settings.json"),
        (
            source_root / ".okx_quant_enhanced_strategy_runtime.json",
            _config_dir_for(target_root) / "enhanced_strategy_runtime.json",
        ),
        (source_root / ".okx_quant_backtest_history.json", _state_dir_for(target_root) / "backtest_history.json"),
        (source_root / ".okx_quant_strategy_history.json", _state_dir_for(target_root) / "strategy_history.json"),
        (source_root / ".okx_quant_smart_order_tasks.json", _state_dir_for(target_root) / "smart_order_tasks.json"),
        (
            source_root / ".okx_quant_smart_order_favorites.json",
            _state_dir_for(target_root) / "smart_order_favorites.json",
        ),
        (source_root / ".okx_quant_option_strategies.json", _state_dir_for(target_root) / "option_strategies.json"),
        (
            source_root / ".okx_quant_deribit_volatility_cache.json",
            _cache_dir_for(target_root) / "deribit_volatility_cache.json",
        ),
        (source_root / ".okx_quant_candle_cache", _cache_dir_for(target_root) / "candle_cache"),
        (source_root / "logs", _logs_dir_for(target_root)),
        (source_root / "reports", _reports_dir_for(target_root)),
    )


def _copy_legacy_path(source: Path, target: Path) -> None:
    if not source.exists():
        return
    if source.is_dir():
        _copy_legacy_dir(source, target)
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        return
    shutil.copy2(source, target)


def _copy_legacy_dir(source: Path, target: Path) -> None:
    target.mkdir(parents=True, exist_ok=True)
    for child in source.iterdir():
        destination = target / child.name
        if child.is_dir():
            _copy_legacy_dir(child, destination)
        elif not destination.exists():
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(child, destination)


def _write_data_layout_file(root: Path) -> None:
    payload = {
        "layout_version": DATA_LAYOUT_VERSION,
        "data_root": str(root),
        "project_root": str(project_root()),
        "env_var": APP_DATA_DIR_ENV_VAR,
        "default_data_dir_name": DEFAULT_DATA_DIR_NAME,
        "written_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    _data_layout_file_path(root).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
