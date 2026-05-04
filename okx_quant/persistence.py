from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from okx_quant.app_paths import cache_dir_path, config_dir_path, reports_dir_path, state_dir_path


CREDENTIALS_FILE_NAME = "credentials.json"
SETTINGS_FILE_NAME = "settings.json"
BACKTEST_HISTORY_FILE_NAME = "backtest_history.json"
BACKTEST_CANDLE_CACHE_DIR_NAME = "candle_cache"
BACKTEST_REPORT_EXPORT_DIR_NAME = "backtest_exports"
ANALYSIS_REPORT_DIR_NAME = "analysis"
DERIBIT_REPORT_EXPORT_DIR_NAME = "deribit"
LIVE_STRATEGY_SESSIONS_DIR_NAME = "live_strategy_sessions"
DERIBIT_VOLATILITY_CACHE_FILE_NAME = "deribit_volatility_cache.json"
STRATEGY_HISTORY_FILE_NAME = "strategy_history.json"
STRATEGY_TRADE_LEDGER_FILE_NAME = "strategy_trade_ledger.json"
RECOVERABLE_STRATEGY_SESSIONS_FILE_NAME = "recoverable_strategy_sessions.json"
SMART_ORDER_TASKS_FILE_NAME = "smart_order_tasks.json"
SMART_ORDER_FAVORITES_FILE_NAME = "smart_order_favorites.json"
OPTION_STRATEGIES_FILE_NAME = "option_strategies.json"
SIGNAL_OBSERVER_TEMPLATES_FILE_NAME = "signal_observer_templates.json"
SIGNAL_OBSERVER_PRESETS_FILE_NAME = "signal_observer_presets.json"
TRADER_DESK_FILE_NAME = "trader_desk.json"
POSITION_NOTES_FILE_NAME = "position_notes.json"
STRATEGY_PARAMETER_GLOBAL_DEFAULTS_FILE_NAME = "strategy_parameter_global_defaults.json"
STRATEGY_PARAMETER_DRAFTS_FILE_NAME = "strategy_parameter_drafts.json"
LINE_TRADING_DESK_ANNOTATIONS_FILE_NAME = "line_trading_desk_annotations.json"
HISTORY_CACHE_DIR_NAME = "history"
HISTORY_ORDER_FILE_NAME = "order_history.json"
HISTORY_FILLS_FILE_NAME = "fills_history.json"
HISTORY_POSITIONS_FILE_NAME = "position_history.json"
DEFAULT_CREDENTIAL_PROFILE_NAME = "api1"
PROFILE_ENVIRONMENTS = {"demo", "live"}


def credentials_file_path(base_dir: Path | None = None) -> Path:
    return Path(base_dir) / CREDENTIALS_FILE_NAME if base_dir is not None else config_dir_path() / CREDENTIALS_FILE_NAME


def settings_file_path(base_dir: Path | None = None) -> Path:
    return Path(base_dir) / SETTINGS_FILE_NAME if base_dir is not None else config_dir_path() / SETTINGS_FILE_NAME

def backtest_history_file_path(base_dir: Path | None = None) -> Path:
    return Path(base_dir) / BACKTEST_HISTORY_FILE_NAME if base_dir is not None else state_dir_path() / BACKTEST_HISTORY_FILE_NAME


def candle_cache_dir_path(base_dir: Path | None = None) -> Path:
    return Path(base_dir) / BACKTEST_CANDLE_CACHE_DIR_NAME if base_dir is not None else cache_dir_path() / BACKTEST_CANDLE_CACHE_DIR_NAME


def backtest_report_export_dir_path(base_dir: Path | None = None) -> Path:
    if base_dir is not None:
        return Path(base_dir) / "reports" / BACKTEST_REPORT_EXPORT_DIR_NAME
    return reports_dir_path() / BACKTEST_REPORT_EXPORT_DIR_NAME


def analysis_report_dir_path(base_dir: Path | None = None) -> Path:
    if base_dir is not None:
        return Path(base_dir) / "reports" / ANALYSIS_REPORT_DIR_NAME
    return reports_dir_path() / ANALYSIS_REPORT_DIR_NAME


def deribit_report_export_dir_path(base_dir: Path | None = None) -> Path:
    if base_dir is not None:
        return Path(base_dir) / "reports" / DERIBIT_REPORT_EXPORT_DIR_NAME
    return reports_dir_path() / DERIBIT_REPORT_EXPORT_DIR_NAME


def live_strategy_sessions_dir_path(base_dir: Path | None = None) -> Path:
    if base_dir is not None:
        return Path(base_dir) / "reports" / LIVE_STRATEGY_SESSIONS_DIR_NAME
    return reports_dir_path() / LIVE_STRATEGY_SESSIONS_DIR_NAME


def deribit_volatility_cache_file_path(base_dir: Path | None = None) -> Path:
    if base_dir is not None:
        return Path(base_dir) / DERIBIT_VOLATILITY_CACHE_FILE_NAME
    return cache_dir_path() / DERIBIT_VOLATILITY_CACHE_FILE_NAME


def strategy_history_file_path(base_dir: Path | None = None) -> Path:
    return Path(base_dir) / STRATEGY_HISTORY_FILE_NAME if base_dir is not None else state_dir_path() / STRATEGY_HISTORY_FILE_NAME


def strategy_trade_ledger_file_path(base_dir: Path | None = None) -> Path:
    if base_dir is not None:
        return Path(base_dir) / STRATEGY_TRADE_LEDGER_FILE_NAME
    return state_dir_path() / STRATEGY_TRADE_LEDGER_FILE_NAME


def recoverable_strategy_sessions_file_path(base_dir: Path | None = None) -> Path:
    if base_dir is not None:
        return Path(base_dir) / RECOVERABLE_STRATEGY_SESSIONS_FILE_NAME
    return state_dir_path() / RECOVERABLE_STRATEGY_SESSIONS_FILE_NAME


def smart_order_tasks_file_path(base_dir: Path | None = None) -> Path:
    return Path(base_dir) / SMART_ORDER_TASKS_FILE_NAME if base_dir is not None else state_dir_path() / SMART_ORDER_TASKS_FILE_NAME


def smart_order_favorites_file_path(base_dir: Path | None = None) -> Path:
    return Path(base_dir) / SMART_ORDER_FAVORITES_FILE_NAME if base_dir is not None else state_dir_path() / SMART_ORDER_FAVORITES_FILE_NAME


def option_strategies_file_path(base_dir: Path | None = None) -> Path:
    return Path(base_dir) / OPTION_STRATEGIES_FILE_NAME if base_dir is not None else state_dir_path() / OPTION_STRATEGIES_FILE_NAME


def signal_observer_templates_file_path(base_dir: Path | None = None) -> Path:
    return Path(base_dir) / SIGNAL_OBSERVER_TEMPLATES_FILE_NAME if base_dir is not None else state_dir_path() / SIGNAL_OBSERVER_TEMPLATES_FILE_NAME


def signal_observer_presets_file_path(base_dir: Path | None = None) -> Path:
    return Path(base_dir) / SIGNAL_OBSERVER_PRESETS_FILE_NAME if base_dir is not None else state_dir_path() / SIGNAL_OBSERVER_PRESETS_FILE_NAME


def trader_desk_file_path(base_dir: Path | None = None) -> Path:
    return Path(base_dir) / TRADER_DESK_FILE_NAME if base_dir is not None else state_dir_path() / TRADER_DESK_FILE_NAME


def position_notes_file_path(base_dir: Path | None = None) -> Path:
    return Path(base_dir) / POSITION_NOTES_FILE_NAME if base_dir is not None else state_dir_path() / POSITION_NOTES_FILE_NAME


def strategy_parameter_global_defaults_file_path(base_dir: Path | None = None) -> Path:
    if base_dir is not None:
        return Path(base_dir) / STRATEGY_PARAMETER_GLOBAL_DEFAULTS_FILE_NAME
    return state_dir_path() / STRATEGY_PARAMETER_GLOBAL_DEFAULTS_FILE_NAME


def strategy_parameter_drafts_file_path(base_dir: Path | None = None) -> Path:
    if base_dir is not None:
        return Path(base_dir) / STRATEGY_PARAMETER_DRAFTS_FILE_NAME
    return state_dir_path() / STRATEGY_PARAMETER_DRAFTS_FILE_NAME


def line_trading_desk_annotations_file_path(base_dir: Path | None = None) -> Path:
    if base_dir is not None:
        return Path(base_dir) / LINE_TRADING_DESK_ANNOTATIONS_FILE_NAME
    return state_dir_path() / LINE_TRADING_DESK_ANNOTATIONS_FILE_NAME


def load_line_trading_desk_annotations_entries(path: Path | None = None) -> dict[str, dict[str, object]]:
    """返回 { \"api|INST|bar\": {\"lines\": [...], \"rr\": [...]} }。条目为浅拷贝 dict，调用方可就地修改。"""
    target = path or line_trading_desk_annotations_file_path()
    if not target.exists():
        return {}
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    raw = payload.get("entries")
    if not isinstance(raw, dict):
        return {}
    out: dict[str, dict[str, object]] = {}
    for k, v in raw.items():
        key = str(k).strip()
        if not key or not isinstance(v, dict):
            continue
        out[key] = dict(v)
    return out


def save_line_trading_desk_annotations_entries(entries: dict[str, dict[str, object]], path: Path | None = None) -> Path:
    """整文件写入；entries 为 api|标的|周期 → {lines, rr}。"""
    target = path or line_trading_desk_annotations_file_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "entries": dict(entries),
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    temp_path = target.with_suffix(target.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(target)
    return target


def _normalize_history_profile_name(profile_name: object) -> str:
    normalized = str(profile_name or "").strip()
    return normalized or DEFAULT_CREDENTIAL_PROFILE_NAME


def _normalize_history_environment(environment: object) -> str:
    normalized = str(environment or "").strip().lower()
    return normalized if normalized in PROFILE_ENVIRONMENTS else "demo"


def history_cache_dir_path(
    profile_name: str,
    environment: str,
    *,
    base_dir: Path | None = None,
) -> Path:
    root = Path(base_dir) if base_dir is not None else state_dir_path()
    return root / HISTORY_CACHE_DIR_NAME / _normalize_history_profile_name(profile_name) / _normalize_history_environment(environment)


def history_cache_file_path(
    history_kind: str,
    profile_name: str,
    environment: str,
    *,
    base_dir: Path | None = None,
) -> Path:
    file_name_by_kind = {
        "orders": HISTORY_ORDER_FILE_NAME,
        "fills": HISTORY_FILLS_FILE_NAME,
        "positions": HISTORY_POSITIONS_FILE_NAME,
    }
    file_name = file_name_by_kind.get(str(history_kind).strip().lower())
    if not file_name:
        raise ValueError(f"Unsupported history cache kind: {history_kind}")
    return history_cache_dir_path(profile_name, environment, base_dir=base_dir) / file_name


def load_history_cache_records(
    history_kind: str,
    profile_name: str,
    environment: str,
    *,
    base_dir: Path | None = None,
) -> list[dict[str, object]]:
    target = history_cache_file_path(history_kind, profile_name, environment, base_dir=base_dir)
    if not target.exists():
        return []
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return []
    records = payload.get("records") if isinstance(payload, dict) else None
    if not isinstance(records, list):
        return []
    return [item for item in records if isinstance(item, dict)]


def save_history_cache_records(
    history_kind: str,
    profile_name: str,
    environment: str,
    records: list[dict[str, object]],
    *,
    base_dir: Path | None = None,
) -> Path:
    target = history_cache_file_path(history_kind, profile_name, environment, base_dir=base_dir)
    target.parent.mkdir(parents=True, exist_ok=True)
    normalized_records = [item for item in records if isinstance(item, dict)]
    payload = {
        "records": normalized_records,
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    temp_path = target.with_suffix(target.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(target)
    return target


def _normalize_strategy_parameter_record(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        return {}
    return {str(key): value for key, value in payload.items() if str(key).strip()}


def load_strategy_parameter_global_defaults(path: Path | None = None) -> dict[str, object]:
    target = path or strategy_parameter_global_defaults_file_path()
    if not target.exists():
        return {"values": {}}
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return {"values": {}}
    values = payload.get("values") if isinstance(payload, dict) else {}
    return {"values": _normalize_strategy_parameter_record(values)}


def save_strategy_parameter_global_defaults(values: dict[str, object], path: Path | None = None) -> Path:
    target = path or strategy_parameter_global_defaults_file_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "values": _normalize_strategy_parameter_record(values),
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    temp_path = target.with_suffix(target.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(target)
    return target


def load_strategy_parameter_drafts(path: Path | None = None) -> dict[str, object]:
    target = path or strategy_parameter_drafts_file_path()
    if not target.exists():
        return {"launcher": {}, "backtest": {}, "observer": {}}
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return {"launcher": {}, "backtest": {}, "observer": {}}
    if not isinstance(payload, dict):
        return {"launcher": {}, "backtest": {}, "observer": {}}
    normalized: dict[str, object] = {}
    for scope in ("launcher", "backtest", "observer"):
        scope_payload = payload.get(scope)
        if not isinstance(scope_payload, dict):
            normalized[scope] = {}
            continue
        normalized[scope] = {
            str(strategy_id): _normalize_strategy_parameter_record(values)
            for strategy_id, values in scope_payload.items()
            if str(strategy_id).strip()
        }
    return normalized


def save_strategy_parameter_drafts(snapshot: dict[str, object], path: Path | None = None) -> Path:
    target = path or strategy_parameter_drafts_file_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, object] = {}
    for scope in ("launcher", "backtest", "observer"):
        scope_payload = snapshot.get(scope, {}) if isinstance(snapshot, dict) else {}
        if not isinstance(scope_payload, dict):
            payload[scope] = {}
            continue
        payload[scope] = {
            str(strategy_id): _normalize_strategy_parameter_record(values)
            for strategy_id, values in scope_payload.items()
            if str(strategy_id).strip()
        }
    payload["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    temp_path = target.with_suffix(target.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(target)
    return target


def _empty_credentials_snapshot() -> dict[str, str]:
    return {
        "api_key": "",
        "secret_key": "",
        "passphrase": "",
        "environment": "",
    }


def _normalize_credentials_environment(payload: object) -> str:
    if not isinstance(payload, dict):
        return ""
    environment = str(payload.get("environment", "")).strip().lower()
    if environment in PROFILE_ENVIRONMENTS:
        return environment
    environment_label = str(payload.get("environment_label", "")).strip().lower()
    if environment_label.endswith("live"):
        return "live"
    if environment_label.endswith("demo"):
        return "demo"
    return ""


def _normalize_credentials_profile(payload: object) -> dict[str, str]:
    if not isinstance(payload, dict):
        return _empty_credentials_snapshot()
    return {
        "api_key": str(payload.get("api_key", "")),
        "secret_key": str(payload.get("secret_key", "")),
        "passphrase": str(payload.get("passphrase", "")),
        "environment": _normalize_credentials_environment(payload),
    }


def load_credentials_profiles_snapshot(path: Path | None = None) -> dict[str, object]:
    target = path or credentials_file_path()
    if not target.exists():
        return {
            "selected_profile": DEFAULT_CREDENTIAL_PROFILE_NAME,
            "profiles": {
                DEFAULT_CREDENTIAL_PROFILE_NAME: _empty_credentials_snapshot(),
            },
        }

    payload = json.loads(target.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("profiles"), dict):
        profiles = {
            str(name): _normalize_credentials_profile(profile)
            for name, profile in payload["profiles"].items()
            if str(name).strip()
        }
        if not profiles:
            profiles = {
                DEFAULT_CREDENTIAL_PROFILE_NAME: _empty_credentials_snapshot(),
            }
        selected_profile = str(payload.get("selected_profile", "")).strip()
        if selected_profile not in profiles:
            selected_profile = next(iter(profiles))
        return {
            "selected_profile": selected_profile,
            "profiles": profiles,
        }

    # Backward-compatible migration from the legacy single-profile format.
    legacy_profile = _normalize_credentials_profile(payload)
    return {
        "selected_profile": DEFAULT_CREDENTIAL_PROFILE_NAME,
        "profiles": {
            DEFAULT_CREDENTIAL_PROFILE_NAME: legacy_profile,
        },
    }


def save_credentials_profiles_snapshot(
    *,
    selected_profile: str,
    profiles: dict[str, dict[str, str]],
    path: Path | None = None,
) -> Path:
    target = path or credentials_file_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    normalized_profiles = {
        str(name).strip(): _normalize_credentials_profile(profile)
        for name, profile in profiles.items()
        if str(name).strip()
    }
    if not normalized_profiles:
        normalized_profiles = {
            DEFAULT_CREDENTIAL_PROFILE_NAME: _empty_credentials_snapshot(),
        }

    selected = selected_profile.strip() or next(iter(normalized_profiles))
    if selected not in normalized_profiles:
        selected = next(iter(normalized_profiles))

    payload = {
        "selected_profile": selected,
        "profiles": normalized_profiles,
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    temp_path = target.with_suffix(target.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(target)
    return target


def load_credentials_snapshot(path: Path | None = None, profile_name: str | None = None) -> dict[str, str]:
    snapshot = load_credentials_profiles_snapshot(path)
    profiles = snapshot["profiles"]
    if not isinstance(profiles, dict):
        return _empty_credentials_snapshot()
    selected = profile_name or str(snapshot["selected_profile"])
    profile = profiles.get(selected)
    if not isinstance(profile, dict):
        profile = next(iter(profiles.values()), _empty_credentials_snapshot())
    return _normalize_credentials_profile(profile)


def save_credentials_snapshot(
    api_key: str,
    secret_key: str,
    passphrase: str,
    path: Path | None = None,
    *,
    profile_name: str | None = None,
    select_profile: bool = True,
) -> Path:
    snapshot = load_credentials_profiles_snapshot(path)
    profiles = snapshot["profiles"]
    if not isinstance(profiles, dict):
        profiles = {}
    target_profile = (profile_name or str(snapshot["selected_profile"])).strip() or DEFAULT_CREDENTIAL_PROFILE_NAME
    profiles[target_profile] = {
        "api_key": api_key.strip(),
        "secret_key": secret_key.strip(),
        "passphrase": passphrase.strip(),
    }
    selected_profile = target_profile if select_profile else str(snapshot["selected_profile"])
    return save_credentials_profiles_snapshot(
        selected_profile=selected_profile,
        profiles=profiles,
        path=path,
    )


def load_notification_snapshot(path: Path | None = None) -> dict[str, object]:
    target = path or settings_file_path()
    if not target.exists():
        return {
            "environment_label": "模拟盘 demo",
            "trade_mode_label": "全仓 cross",
            "position_mode_label": "净持仓 net",
            "trigger_type_label": "标记价格 mark",
            "enabled": False,
            "smtp_host": "",
            "smtp_port": 465,
            "smtp_username": "",
            "smtp_password": "",
            "sender_email": "",
            "recipient_emails": "",
            "use_ssl": True,
            "notify_trade_fills": True,
            "notify_signals": True,
            "notify_errors": True,
        }

    payload = json.loads(target.read_text(encoding="utf-8"))
    return {
        "environment_label": str(payload.get("environment_label", "模拟盘 demo")),
        "trade_mode_label": str(payload.get("trade_mode_label", "全仓 cross")),
        "position_mode_label": str(payload.get("position_mode_label", "净持仓 net")),
        "trigger_type_label": str(payload.get("trigger_type_label", "标记价格 mark")),
        "enabled": bool(payload.get("enabled", False)),
        "smtp_host": str(payload.get("smtp_host", "")),
        "smtp_port": int(payload.get("smtp_port", 465)),
        "smtp_username": str(payload.get("smtp_username", "")),
        "smtp_password": str(payload.get("smtp_password", "")),
        "sender_email": str(payload.get("sender_email", "")),
        "recipient_emails": str(payload.get("recipient_emails", "")),
        "use_ssl": bool(payload.get("use_ssl", True)),
        "notify_trade_fills": bool(payload.get("notify_trade_fills", True)),
        "notify_signals": bool(payload.get("notify_signals", True)),
        "notify_errors": bool(payload.get("notify_errors", True)),
    }


def save_notification_snapshot(
    *,
    environment_label: str,
    trade_mode_label: str,
    position_mode_label: str,
    trigger_type_label: str,
    enabled: bool,
    smtp_host: str,
    smtp_port: int,
    smtp_username: str,
    smtp_password: str,
    sender_email: str,
    recipient_emails: str,
    use_ssl: bool,
    notify_trade_fills: bool,
    notify_signals: bool,
    notify_errors: bool,
    path: Path | None = None,
) -> Path:
    target = path or settings_file_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "environment_label": environment_label,
        "trade_mode_label": trade_mode_label,
        "position_mode_label": position_mode_label,
        "trigger_type_label": trigger_type_label,
        "enabled": enabled,
        "smtp_host": smtp_host.strip(),
        "smtp_port": int(smtp_port),
        "smtp_username": smtp_username.strip(),
        "smtp_password": smtp_password.strip(),
        "sender_email": sender_email.strip(),
        "recipient_emails": recipient_emails.strip(),
        "use_ssl": use_ssl,
        "notify_trade_fills": notify_trade_fills,
        "notify_signals": notify_signals,
        "notify_errors": notify_errors,
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    temp_path = target.with_suffix(target.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(target)
    return target


def load_smart_order_tasks_snapshot(path: Path | None = None) -> dict[str, object]:
    target = path or smart_order_tasks_file_path()
    if not target.exists():
        return {
            "task_counter": 0,
            "locked_inst_id": None,
            "locked_instrument": None,
            "position_limit_enabled": False,
            "long_position_limit": None,
            "short_position_limit": None,
            "tasks": [],
        }
    payload = json.loads(target.read_text(encoding="utf-8"))
    tasks = payload.get("tasks")
    if not isinstance(tasks, list):
        tasks = []
    return {
        "task_counter": int(payload.get("task_counter", 0)),
        "locked_inst_id": payload.get("locked_inst_id"),
        "locked_instrument": payload.get("locked_instrument"),
        "position_limit_enabled": bool(payload.get("position_limit_enabled", False)),
        "long_position_limit": payload.get("long_position_limit"),
        "short_position_limit": payload.get("short_position_limit"),
        "tasks": tasks,
    }


def save_smart_order_tasks_snapshot(
    *,
    task_counter: int,
    locked_inst_id: str | None,
    locked_instrument: dict[str, object] | None,
    position_limit_enabled: bool,
    long_position_limit: str | None,
    short_position_limit: str | None,
    tasks: list[dict[str, object]],
    path: Path | None = None,
) -> Path:
    target = path or smart_order_tasks_file_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "task_counter": int(task_counter),
        "locked_inst_id": locked_inst_id,
        "locked_instrument": locked_instrument,
        "position_limit_enabled": bool(position_limit_enabled),
        "long_position_limit": long_position_limit,
        "short_position_limit": short_position_limit,
        "tasks": tasks,
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    temp_path = target.with_suffix(target.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(target)
    return target


def _normalize_smart_order_favorite(item: object) -> dict[str, str] | None:
    if not isinstance(item, dict):
        return None
    inst_id = str(item.get("inst_id", "")).strip().upper()
    inst_type = str(item.get("inst_type", "")).strip().upper()
    if not inst_id or not inst_type:
        return None
    return {
        "inst_id": inst_id,
        "inst_type": inst_type,
    }


def load_smart_order_favorites_snapshot(path: Path | None = None) -> dict[str, object]:
    target = path or smart_order_favorites_file_path()
    if not target.exists():
        return {"favorites": []}
    payload = json.loads(target.read_text(encoding="utf-8"))
    raw_items = payload.get("favorites")
    if not isinstance(raw_items, list):
        raw_items = []
    favorites: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in raw_items:
        normalized = _normalize_smart_order_favorite(item)
        if normalized is None:
            continue
        key = (normalized["inst_type"], normalized["inst_id"])
        if key in seen:
            continue
        seen.add(key)
        favorites.append(normalized)
    favorites.sort(key=lambda item: (item["inst_type"], item["inst_id"]))
    return {"favorites": favorites}


def save_smart_order_favorites_snapshot(
    favorites: list[dict[str, str]],
    path: Path | None = None,
) -> Path:
    target = path or smart_order_favorites_file_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    normalized_favorites: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in favorites:
        normalized = _normalize_smart_order_favorite(item)
        if normalized is None:
            continue
        key = (normalized["inst_type"], normalized["inst_id"])
        if key in seen:
            continue
        seen.add(key)
        normalized_favorites.append(normalized)
    normalized_favorites.sort(key=lambda item: (item["inst_type"], item["inst_id"]))
    payload = {
        "favorites": normalized_favorites,
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    temp_path = target.with_suffix(target.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(target)
    return target


def _normalize_strategy_history_record(item: object) -> dict[str, object] | None:
    if not isinstance(item, dict):
        return None
    record_id = str(item.get("record_id", "")).strip()
    strategy_name = str(item.get("strategy_name", "")).strip()
    started_at = str(item.get("started_at", "")).strip()
    if not record_id or not strategy_name or not started_at:
        return None
    raw_config_snapshot = item.get("config_snapshot")
    config_snapshot = raw_config_snapshot if isinstance(raw_config_snapshot, dict) else {}
    stopped_at = str(item.get("stopped_at", "")).strip()
    updated_at = str(item.get("updated_at", "")).strip()
    try:
        trade_count = max(0, int(item.get("trade_count", 0) or 0))
    except (TypeError, ValueError):
        trade_count = 0
    try:
        win_count = max(0, int(item.get("win_count", 0) or 0))
    except (TypeError, ValueError):
        win_count = 0
    return {
        "record_id": record_id,
        "session_id": str(item.get("session_id", "")).strip(),
        "api_name": str(item.get("api_name", "")).strip(),
        "strategy_id": str(item.get("strategy_id", "")).strip(),
        "strategy_name": strategy_name,
        "symbol": str(item.get("symbol", "")).strip(),
        "direction_label": str(item.get("direction_label", "")).strip(),
        "run_mode_label": str(item.get("run_mode_label", "")).strip(),
        "status": str(item.get("status", "已停止")).strip() or "已停止",
        "started_at": started_at,
        "stopped_at": stopped_at or None,
        "ended_reason": str(item.get("ended_reason", "")).strip(),
        "log_file_path": str(item.get("log_file_path", "")).strip(),
        "updated_at": updated_at or None,
        "config_snapshot": config_snapshot,
        "trade_count": trade_count,
        "win_count": win_count,
        "gross_pnl_total": str(item.get("gross_pnl_total", "")).strip() or "0",
        "fee_total": str(item.get("fee_total", "")).strip() or "0",
        "funding_total": str(item.get("funding_total", "")).strip() or "0",
        "net_pnl_total": str(item.get("net_pnl_total", "")).strip() or "0",
        "last_close_reason": str(item.get("last_close_reason", "")).strip(),
    }


def load_strategy_history_snapshot(path: Path | None = None) -> dict[str, object]:
    target = path or strategy_history_file_path()
    if not target.exists():
        return {"records": []}
    payload = json.loads(target.read_text(encoding="utf-8"))
    raw_records = payload.get("records")
    if not isinstance(raw_records, list):
        raw_records = []
    records = [
        normalized for item in raw_records if (normalized := _normalize_strategy_history_record(item)) is not None
    ]
    records.sort(key=lambda item: (str(item["started_at"]), str(item["record_id"])), reverse=True)
    return {"records": records}


def save_strategy_history_snapshot(
    records: list[dict[str, object]],
    path: Path | None = None,
) -> Path:
    target = path or strategy_history_file_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    normalized_records = [
        item for record in records if (item := _normalize_strategy_history_record(record)) is not None
    ]
    normalized_records.sort(key=lambda item: (str(item["started_at"]), str(item["record_id"])), reverse=True)
    payload = {
        "records": normalized_records,
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    temp_path = target.with_suffix(target.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(target)
    return target


def _normalize_strategy_trade_ledger_record(item: object) -> dict[str, object] | None:
    if not isinstance(item, dict):
        return None
    record_id = str(item.get("record_id", "")).strip()
    session_id = str(item.get("session_id", "")).strip()
    strategy_name = str(item.get("strategy_name", "")).strip()
    closed_at = str(item.get("closed_at", "")).strip()
    if not record_id or not session_id or not strategy_name or not closed_at:
        return None
    return {
        "record_id": record_id,
        "history_record_id": str(item.get("history_record_id", "")).strip(),
        "session_id": session_id,
        "api_name": str(item.get("api_name", "")).strip(),
        "strategy_id": str(item.get("strategy_id", "")).strip(),
        "strategy_name": strategy_name,
        "symbol": str(item.get("symbol", "")).strip(),
        "direction_label": str(item.get("direction_label", "")).strip(),
        "run_mode_label": str(item.get("run_mode_label", "")).strip(),
        "environment": str(item.get("environment", "")).strip(),
        "signal_bar_at": str(item.get("signal_bar_at", "")).strip() or None,
        "opened_at": str(item.get("opened_at", "")).strip() or None,
        "closed_at": closed_at,
        "entry_order_id": str(item.get("entry_order_id", "")).strip(),
        "entry_client_order_id": str(item.get("entry_client_order_id", "")).strip(),
        "exit_order_id": str(item.get("exit_order_id", "")).strip(),
        "protective_algo_id": str(item.get("protective_algo_id", "")).strip(),
        "protective_algo_cl_ord_id": str(item.get("protective_algo_cl_ord_id", "")).strip(),
        "entry_price": str(item.get("entry_price", "")).strip() or None,
        "exit_price": str(item.get("exit_price", "")).strip() or None,
        "size": str(item.get("size", "")).strip() or None,
        "entry_fee": str(item.get("entry_fee", "")).strip() or None,
        "exit_fee": str(item.get("exit_fee", "")).strip() or None,
        "funding_fee": str(item.get("funding_fee", "")).strip() or None,
        "gross_pnl": str(item.get("gross_pnl", "")).strip() or None,
        "net_pnl": str(item.get("net_pnl", "")).strip() or None,
        "close_reason": str(item.get("close_reason", "")).strip(),
        "reason_confidence": str(item.get("reason_confidence", "")).strip() or "low",
        "summary_note": str(item.get("summary_note", "")).strip(),
        "updated_at": str(item.get("updated_at", "")).strip() or None,
    }


def load_strategy_trade_ledger_snapshot(path: Path | None = None) -> dict[str, object]:
    target = path or strategy_trade_ledger_file_path()
    if not target.exists():
        return {"records": []}
    payload = json.loads(target.read_text(encoding="utf-8"))
    raw_records = payload.get("records")
    if not isinstance(raw_records, list):
        raw_records = []
    records = [
        normalized
        for item in raw_records
        if (normalized := _normalize_strategy_trade_ledger_record(item)) is not None
    ]
    records.sort(key=lambda item: (str(item["closed_at"]), str(item["record_id"])), reverse=True)
    return {"records": records}


def save_strategy_trade_ledger_snapshot(
    records: list[dict[str, object]],
    path: Path | None = None,
) -> Path:
    target = path or strategy_trade_ledger_file_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    normalized_records = [
        item for record in records if (item := _normalize_strategy_trade_ledger_record(record)) is not None
    ]
    normalized_records.sort(key=lambda item: (str(item["closed_at"]), str(item["record_id"])), reverse=True)
    payload = {
        "records": normalized_records,
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    temp_path = target.with_suffix(target.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(target)
    return target


def _normalize_recoverable_strategy_session_record(item: object) -> dict[str, object] | None:
    if not isinstance(item, dict):
        return None
    session_id = str(item.get("session_id", "")).strip()
    strategy_id = str(item.get("strategy_id", "")).strip()
    strategy_name = str(item.get("strategy_name", "")).strip()
    started_at = str(item.get("started_at", "")).strip()
    recovery_root_dir = str(item.get("recovery_root_dir", "")).strip()
    raw_config_snapshot = item.get("config_snapshot")
    config_snapshot = raw_config_snapshot if isinstance(raw_config_snapshot, dict) else {}
    if not session_id or not strategy_id or not strategy_name or not started_at or not recovery_root_dir:
        return None
    updated_at = str(item.get("updated_at", "")).strip()
    return {
        "session_id": session_id,
        "api_name": str(item.get("api_name", "")).strip(),
        "strategy_id": strategy_id,
        "strategy_name": strategy_name,
        "symbol": str(item.get("symbol", "")).strip(),
        "direction_label": str(item.get("direction_label", "")).strip(),
        "run_mode_label": str(item.get("run_mode_label", "")).strip(),
        "started_at": started_at,
        "history_record_id": str(item.get("history_record_id", "")).strip(),
        "log_file_path": str(item.get("log_file_path", "")).strip(),
        "recovery_root_dir": recovery_root_dir,
        "config_snapshot": config_snapshot,
        "updated_at": updated_at or None,
    }


def load_recoverable_strategy_sessions_snapshot(path: Path | None = None) -> dict[str, object]:
    target = path or recoverable_strategy_sessions_file_path()
    if not target.exists():
        return {"sessions": []}
    payload = json.loads(target.read_text(encoding="utf-8"))
    raw_sessions = payload.get("sessions")
    if not isinstance(raw_sessions, list):
        raw_sessions = []
    sessions = [
        normalized
        for item in raw_sessions
        if (normalized := _normalize_recoverable_strategy_session_record(item)) is not None
    ]
    sessions.sort(key=lambda item: (str(item["started_at"]), str(item["session_id"])), reverse=True)
    return {"sessions": sessions}


def save_recoverable_strategy_sessions_snapshot(
    sessions: list[dict[str, object]],
    path: Path | None = None,
) -> Path:
    target = path or recoverable_strategy_sessions_file_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    normalized_sessions = [
        item
        for session in sessions
        if (item := _normalize_recoverable_strategy_session_record(session)) is not None
    ]
    normalized_sessions.sort(key=lambda item: (str(item["started_at"]), str(item["session_id"])), reverse=True)
    payload = {
        "sessions": normalized_sessions,
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    temp_path = target.with_suffix(target.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(target)
    return target


def _normalize_position_note_text(value: object) -> str:
    if value is None:
        return ""
    lines = str(value).replace("\r\n", "\n").replace("\r", "\n").split("\n")
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    return "\n".join(line.rstrip() for line in lines)


def _normalize_position_note_environment(value: object) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in PROFILE_ENVIRONMENTS else ""


def _normalize_position_note_int(value: object, *, minimum: int | None = None) -> int | None:
    if value in {None, ""}:
        return None
    try:
        normalized = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    if minimum is not None and normalized < minimum:
        return minimum
    return normalized


def _normalize_position_current_note_record(item: object) -> dict[str, object] | None:
    if not isinstance(item, dict):
        return None
    record_key = str(item.get("record_key", "")).strip()
    note = _normalize_position_note_text(item.get("note", ""))
    if not record_key or not note:
        return None
    raw_linked_history_keys = item.get("linked_history_keys")
    linked_history_keys = (
        [str(value).strip() for value in raw_linked_history_keys if str(value).strip()]
        if isinstance(raw_linked_history_keys, list)
        else []
    )
    return {
        "record_key": record_key,
        "profile_name": str(item.get("profile_name", "")).strip(),
        "environment": _normalize_position_note_environment(item.get("environment")),
        "inst_id": str(item.get("inst_id", "")).strip().upper(),
        "pos_side": str(item.get("pos_side", "")).strip().lower(),
        "mgn_mode": str(item.get("mgn_mode", "")).strip().lower(),
        "note": note,
        "activated_at_ms": _normalize_position_note_int(item.get("activated_at_ms"), minimum=0),
        "updated_at_ms": _normalize_position_note_int(item.get("updated_at_ms"), minimum=0),
        "missing_success_count": _normalize_position_note_int(item.get("missing_success_count"), minimum=0) or 0,
        "missing_started_at_ms": _normalize_position_note_int(item.get("missing_started_at_ms"), minimum=0),
        "linked_history_keys": linked_history_keys,
    }


def _normalize_position_history_note_record(item: object) -> dict[str, object] | None:
    if not isinstance(item, dict):
        return None
    record_key = str(item.get("record_key", "")).strip()
    note = _normalize_position_note_text(item.get("note", ""))
    if not record_key or not note:
        return None
    return {
        "record_key": record_key,
        "profile_name": str(item.get("profile_name", "")).strip(),
        "environment": _normalize_position_note_environment(item.get("environment")),
        "inst_id": str(item.get("inst_id", "")).strip().upper(),
        "update_time": _normalize_position_note_int(item.get("update_time"), minimum=0),
        "mgn_mode": str(item.get("mgn_mode", "")).strip().lower(),
        "pos_side": str(item.get("pos_side", "")).strip().lower(),
        "direction": str(item.get("direction", "")).strip().lower(),
        "close_size": str(item.get("close_size", "")).strip(),
        "close_avg_price": str(item.get("close_avg_price", "")).strip(),
        "note": note,
        "source_current_key": str(item.get("source_current_key", "")).strip(),
        "updated_at_ms": _normalize_position_note_int(item.get("updated_at_ms"), minimum=0),
    }


def load_position_notes_snapshot(path: Path | None = None) -> dict[str, object]:
    target = path or position_notes_file_path()
    if not target.exists():
        return {"current_notes": [], "history_notes": []}
    payload = json.loads(target.read_text(encoding="utf-8"))
    raw_current_notes = payload.get("current_notes")
    raw_history_notes = payload.get("history_notes")
    if not isinstance(raw_current_notes, list):
        raw_current_notes = []
    if not isinstance(raw_history_notes, list):
        raw_history_notes = []
    current_notes = [
        normalized
        for item in raw_current_notes
        if (normalized := _normalize_position_current_note_record(item)) is not None
    ]
    history_notes = [
        normalized
        for item in raw_history_notes
        if (normalized := _normalize_position_history_note_record(item)) is not None
    ]
    current_notes.sort(key=lambda item: str(item["record_key"]))
    history_notes.sort(key=lambda item: (int(item.get("update_time") or 0), str(item["record_key"])), reverse=True)
    return {
        "current_notes": current_notes,
        "history_notes": history_notes,
    }


def save_position_notes_snapshot(
    *,
    current_notes: list[dict[str, object]],
    history_notes: list[dict[str, object]],
    path: Path | None = None,
) -> Path:
    target = path or position_notes_file_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    normalized_current_notes = [
        item
        for record in current_notes
        if (item := _normalize_position_current_note_record(record)) is not None
    ]
    normalized_history_notes = [
        item
        for record in history_notes
        if (item := _normalize_position_history_note_record(record)) is not None
    ]
    normalized_current_notes.sort(key=lambda item: str(item["record_key"]))
    normalized_history_notes.sort(key=lambda item: (int(item.get("update_time") or 0), str(item["record_key"])), reverse=True)
    payload = {
        "current_notes": normalized_current_notes,
        "history_notes": normalized_history_notes,
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    temp_path = target.with_suffix(target.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(target)
    return target


def _normalize_option_strategy_leg(item: object) -> dict[str, object] | None:
    if not isinstance(item, dict):
        return None
    alias = str(item.get("alias", "")).strip()
    inst_id = str(item.get("inst_id", "")).strip().upper()
    side = str(item.get("side", "buy")).strip().lower()
    quantity = str(item.get("quantity", "1")).strip()
    premium = str(item.get("premium", "")).strip()
    delta = str(item.get("delta", "")).strip()
    gamma = str(item.get("gamma", "")).strip()
    theta = str(item.get("theta", "")).strip()
    vega = str(item.get("vega", "")).strip()
    enabled = bool(item.get("enabled", True))
    if not alias or not inst_id or side not in {"buy", "sell"}:
        return None
    return {
        "alias": alias,
        "inst_id": inst_id,
        "side": side,
        "quantity": quantity or "1",
        "premium": premium,
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "enabled": enabled,
    }


def _normalize_option_strategy_record(item: object) -> dict[str, object] | None:
    if not isinstance(item, dict):
        return None
    name = str(item.get("name", "")).strip()
    if not name:
        return None
    raw_legs = item.get("legs")
    if not isinstance(raw_legs, list):
        raw_legs = []
    legs = [normalized for raw in raw_legs if (normalized := _normalize_option_strategy_leg(raw)) is not None]
    combo_chart_mode = str(item.get("combo_chart_mode", "price")).strip().lower() or "price"
    candle_limit = str(item.get("candle_limit", "1000")).strip() or "1000"
    # 历史版本默认是 600，这里视为旧默认值，自动迁移到 2000。
    if candle_limit == "600":
        candle_limit = "1000"
    return {
        "name": name,
        "option_family": str(item.get("option_family", "")).strip().upper(),
        "expiry_code": str(item.get("expiry_code", "")).strip(),
        "bar": str(item.get("bar", "1H")).strip() or "1H",
        "candle_limit": candle_limit,
        "chart_display_ccy": str(item.get("chart_display_ccy", "结算币")).strip() or "结算币",
        "combo_chart_mode": "pnl" if combo_chart_mode == "pnl" else "price",
        "formula": str(item.get("formula", "")).strip(),
        "legs": legs,
    }


def load_option_strategies_snapshot(path: Path | None = None) -> dict[str, object]:
    target = path or option_strategies_file_path()
    if not target.exists():
        return {"strategies": []}
    payload = json.loads(target.read_text(encoding="utf-8"))
    raw_items = payload.get("strategies")
    if not isinstance(raw_items, list):
        raw_items = []
    strategies = [
        normalized for item in raw_items if (normalized := _normalize_option_strategy_record(item)) is not None
    ]
    strategies.sort(key=lambda item: str(item["name"]))
    return {"strategies": strategies}


def save_option_strategies_snapshot(
    strategies: list[dict[str, object]],
    path: Path | None = None,
) -> Path:
    target = path or option_strategies_file_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    normalized = [
        item for strategy in strategies if (item := _normalize_option_strategy_record(strategy)) is not None
    ]
    normalized.sort(key=lambda item: str(item["name"]))
    payload = {
        "strategies": normalized,
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    temp_path = target.with_suffix(target.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(target)
    return target
