from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


CREDENTIALS_FILE_NAME = ".okx_quant_credentials.json"
SETTINGS_FILE_NAME = ".okx_quant_settings.json"
BACKTEST_HISTORY_FILE_NAME = ".okx_quant_backtest_history.json"
BACKTEST_CANDLE_CACHE_DIR_NAME = ".okx_quant_candle_cache"
BACKTEST_REPORT_EXPORT_DIR_NAME = "backtest_exports"
SMART_ORDER_TASKS_FILE_NAME = ".okx_quant_smart_order_tasks.json"
SMART_ORDER_FAVORITES_FILE_NAME = ".okx_quant_smart_order_favorites.json"
OPTION_STRATEGIES_FILE_NAME = ".okx_quant_option_strategies.json"
DEFAULT_CREDENTIAL_PROFILE_NAME = "api1"


def credentials_file_path(base_dir: Path | None = None) -> Path:
    if base_dir is None:
        base_dir = Path(__file__).resolve().parent.parent
    return Path(base_dir) / CREDENTIALS_FILE_NAME


def settings_file_path(base_dir: Path | None = None) -> Path:
    if base_dir is None:
        base_dir = Path(__file__).resolve().parent.parent
    return Path(base_dir) / SETTINGS_FILE_NAME


def backtest_history_file_path(base_dir: Path | None = None) -> Path:
    if base_dir is None:
        base_dir = Path(__file__).resolve().parent.parent
    return Path(base_dir) / BACKTEST_HISTORY_FILE_NAME


def candle_cache_dir_path(base_dir: Path | None = None) -> Path:
    if base_dir is None:
        base_dir = Path(__file__).resolve().parent.parent
    return Path(base_dir) / BACKTEST_CANDLE_CACHE_DIR_NAME


def backtest_report_export_dir_path(base_dir: Path | None = None) -> Path:
    if base_dir is None:
        base_dir = Path(__file__).resolve().parent.parent
    return Path(base_dir) / "reports" / BACKTEST_REPORT_EXPORT_DIR_NAME


def smart_order_tasks_file_path(base_dir: Path | None = None) -> Path:
    if base_dir is None:
        base_dir = Path(__file__).resolve().parent.parent
    return Path(base_dir) / SMART_ORDER_TASKS_FILE_NAME


def smart_order_favorites_file_path(base_dir: Path | None = None) -> Path:
    if base_dir is None:
        base_dir = Path(__file__).resolve().parent.parent
    return Path(base_dir) / SMART_ORDER_FAVORITES_FILE_NAME


def option_strategies_file_path(base_dir: Path | None = None) -> Path:
    if base_dir is None:
        base_dir = Path(__file__).resolve().parent.parent
    return Path(base_dir) / OPTION_STRATEGIES_FILE_NAME


def _empty_credentials_snapshot() -> dict[str, str]:
    return {
        "api_key": "",
        "secret_key": "",
        "passphrase": "",
    }


def _normalize_credentials_profile(payload: object) -> dict[str, str]:
    if not isinstance(payload, dict):
        return _empty_credentials_snapshot()
    return {
        "api_key": str(payload.get("api_key", "")),
        "secret_key": str(payload.get("secret_key", "")),
        "passphrase": str(payload.get("passphrase", "")),
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


def _normalize_option_strategy_leg(item: object) -> dict[str, object] | None:
    if not isinstance(item, dict):
        return None
    alias = str(item.get("alias", "")).strip()
    inst_id = str(item.get("inst_id", "")).strip().upper()
    side = str(item.get("side", "buy")).strip().lower()
    quantity = str(item.get("quantity", "1")).strip()
    premium = str(item.get("premium", "")).strip()
    enabled = bool(item.get("enabled", True))
    if not alias or not inst_id or side not in {"buy", "sell"}:
        return None
    return {
        "alias": alias,
        "inst_id": inst_id,
        "side": side,
        "quantity": quantity or "1",
        "premium": premium,
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
    return {
        "name": name,
        "option_family": str(item.get("option_family", "")).strip().upper(),
        "expiry_code": str(item.get("expiry_code", "")).strip(),
        "bar": str(item.get("bar", "15m")).strip() or "15m",
        "candle_limit": str(item.get("candle_limit", "600")).strip() or "600",
        "chart_display_ccy": str(item.get("chart_display_ccy", "USDT")).strip() or "USDT",
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
