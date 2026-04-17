from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from okx_quant.enhanced_models import ChildSignalLabProfile
from okx_quant.enhanced_registry import EnhancedStrategyRegistry


RUNTIME_CONFIG_VERSION = 1
PROFILE_DECIMAL_FIELDS = {
    "stop_loss_pct",
    "take_profit_pct",
    "fee_rate",
    "slippage_rate",
    "funding_rate_per_8h",
}
PROFILE_INT_FIELDS = {
    "fixed_hold_bars",
    "max_hold_bars",
}
PROFILE_BOOL_FIELDS = {
    "close_on_timeout_if_profitable",
}
PROFILE_TEXT_FIELDS = {
    "profile_name",
    "exit_mode",
    "stop_hit_mode",
    "notes",
}


def load_runtime_store(path: Path | str) -> dict[str, object]:
    target = Path(path)
    if not target.exists():
        return {
            "version": RUNTIME_CONFIG_VERSION,
            "strategies": {},
        }
    payload = json.loads(target.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("runtime config file must contain a JSON object")
    strategies = payload.get("strategies")
    if strategies is None:
        payload["strategies"] = {}
    elif not isinstance(strategies, dict):
        raise TypeError("runtime config `strategies` must be a JSON object")
    if "version" not in payload:
        payload["version"] = RUNTIME_CONFIG_VERSION
    return payload


def save_runtime_store(path: Path | str, payload: dict[str, object]) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def get_strategy_runtime_payload(
    store_payload: dict[str, object],
    parent_strategy_id: str,
) -> dict[str, object] | None:
    strategies = store_payload.get("strategies", {})
    if not isinstance(strategies, dict):
        raise TypeError("runtime config `strategies` must be a JSON object")
    strategy_payload = strategies.get(parent_strategy_id)
    if strategy_payload is None:
        return None
    if not isinstance(strategy_payload, dict):
        raise TypeError(f"runtime config for {parent_strategy_id!r} must be a JSON object")
    return strategy_payload


def build_strategy_runtime_payload(
    registry: EnhancedStrategyRegistry,
    *,
    parent_strategy_id: str,
) -> dict[str, object]:
    parent = registry.get_parent_strategy(parent_strategy_id)
    signals_payload: dict[str, object] = {}
    for signal in registry.list_child_signals(parent_strategy_id, enabled_only=False):
        profile = registry.get_signal_lab_profile(signal.signal_id)
        playbooks = registry.list_playbooks_for_signal(signal.signal_id, enabled_only=False)
        signals_payload[signal.signal_id] = {
            "signal_id": signal.signal_id,
            "signal_name": signal.signal_name,
            "enabled": signal.enabled,
            "underlying_family": signal.underlying_family,
            "source_market": signal.source.market,
            "source_inst_id": signal.source.inst_id,
            "source_bar": signal.source.bar,
            "direction_bias": signal.direction_bias,
            "evidence_template_id": signal.evidence_template_id,
            "notes": signal.notes,
            "playbooks": [
                {
                    "playbook_id": item.playbook_id,
                    "playbook_name": item.playbook_name,
                    "action": item.action,
                    "enabled": item.enabled,
                }
                for item in playbooks
            ],
            "lab_profile": None if profile is None else serialize_lab_profile(profile),
        }
    return {
        "parent_strategy_id": parent.strategy_id,
        "parent_strategy_name": parent.strategy_name,
        "updated_at": _now_iso(),
        "signals": signals_payload,
    }


def apply_strategy_runtime_payload(
    registry: EnhancedStrategyRegistry,
    *,
    parent_strategy_id: str,
    strategy_payload: dict[str, object],
) -> dict[str, object]:
    signals_payload = strategy_payload.get("signals", {})
    if not isinstance(signals_payload, dict):
        raise TypeError("strategy runtime payload `signals` must be a JSON object")

    enabled_signals: list[str] = []
    disabled_signals: list[str] = []
    cleared_lab_profiles: list[str] = []
    replaced_lab_profiles: dict[str, dict[str, object]] = {}
    skipped_unknown_signals: list[str] = []

    known_signals = {
        item.signal_id
        for item in registry.list_child_signals(parent_strategy_id, enabled_only=False)
    }
    for signal_id, raw_signal_payload in signals_payload.items():
        if signal_id not in known_signals:
            skipped_unknown_signals.append(signal_id)
            continue
        if not isinstance(raw_signal_payload, dict):
            raise TypeError(f"runtime signal payload for {signal_id!r} must be a JSON object")

        if "enabled" in raw_signal_payload and raw_signal_payload["enabled"] is not None:
            enabled = _coerce_bool(raw_signal_payload["enabled"])
            registry.set_child_signal_enabled(signal_id, enabled)
            (enabled_signals if enabled else disabled_signals).append(signal_id)

        if "lab_profile" in raw_signal_payload:
            raw_profile = raw_signal_payload["lab_profile"]
            if raw_profile is None:
                registry.replace_signal_lab_profile(signal_id, None)
                cleared_lab_profiles.append(signal_id)
            else:
                profile = deserialize_lab_profile(signal_id, raw_profile)
                applied = registry.replace_signal_lab_profile(signal_id, profile)
                if applied is not None:
                    replaced_lab_profiles[signal_id] = serialize_lab_profile(applied)

    return {
        "loaded_parent_strategy_id": parent_strategy_id,
        "enabled_signals": enabled_signals,
        "disabled_signals": disabled_signals,
        "cleared_lab_profiles": cleared_lab_profiles,
        "replaced_lab_profiles": replaced_lab_profiles,
        "skipped_unknown_signals": skipped_unknown_signals,
    }


def write_strategy_runtime_payload(
    path: Path | str,
    *,
    strategy_payload: dict[str, object],
) -> Path:
    target = Path(path)
    store = load_runtime_store(target)
    strategies = store.setdefault("strategies", {})
    if not isinstance(strategies, dict):
        raise TypeError("runtime config `strategies` must be a JSON object")
    parent_strategy_id = strategy_payload.get("parent_strategy_id")
    if not isinstance(parent_strategy_id, str) or not parent_strategy_id.strip():
        raise ValueError("strategy payload must include a non-empty `parent_strategy_id`")
    strategies[parent_strategy_id] = strategy_payload
    store["version"] = RUNTIME_CONFIG_VERSION
    return save_runtime_store(target, store)


def serialize_lab_profile(profile: ChildSignalLabProfile) -> dict[str, object]:
    normalized = profile.normalized()
    return {
        "signal_id": normalized.signal_id,
        "profile_name": normalized.profile_name,
        "exit_mode": normalized.exit_mode,
        "fixed_hold_bars": normalized.fixed_hold_bars,
        "max_hold_bars": normalized.max_hold_bars,
        "stop_loss_pct": None if normalized.stop_loss_pct is None else str(normalized.stop_loss_pct),
        "take_profit_pct": None if normalized.take_profit_pct is None else str(normalized.take_profit_pct),
        "fee_rate": None if normalized.fee_rate is None else str(normalized.fee_rate),
        "slippage_rate": None if normalized.slippage_rate is None else str(normalized.slippage_rate),
        "funding_rate_per_8h": (
            None if normalized.funding_rate_per_8h is None else str(normalized.funding_rate_per_8h)
        ),
        "stop_hit_mode": normalized.stop_hit_mode,
        "close_on_timeout_if_profitable": normalized.close_on_timeout_if_profitable,
        "notes": normalized.notes,
    }


def deserialize_lab_profile(signal_id: str, raw_payload: object) -> ChildSignalLabProfile:
    if not isinstance(raw_payload, dict):
        raise TypeError(f"lab profile for {signal_id!r} must be a JSON object")
    normalized = normalize_profile_payload(raw_payload)
    return ChildSignalLabProfile(signal_id=signal_id, **normalized).normalized()


def normalize_profile_payload(raw_payload: dict[str, object]) -> dict[str, object]:
    normalized: dict[str, object] = {}
    for key, value in raw_payload.items():
        if key == "signal_id":
            continue
        if key in PROFILE_DECIMAL_FIELDS:
            normalized[key] = None if value is None else Decimal(str(value))
            continue
        if key in PROFILE_INT_FIELDS:
            normalized[key] = None if value is None else max(int(value), 0)
            continue
        if key in PROFILE_BOOL_FIELDS:
            normalized[key] = None if value is None else _coerce_bool(value)
            continue
        if key in PROFILE_TEXT_FIELDS:
            normalized[key] = "" if value is None else str(value)
            continue
        raise KeyError(f"unsupported lab profile field in runtime config: {key}")
    return normalized


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    raise ValueError(f"unsupported bool value: {value!r}")


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
