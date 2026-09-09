from __future__ import annotations

from okx_quant.arbitrage.models import ArbitrageTradeRuntime
from okx_quant.models import Credentials, Instrument
from okx_quant.okx_client import infer_inst_type
from okx_quant.persistence import DEFAULT_CREDENTIAL_PROFILE_NAME

_TRADE_MODE = {"cross", "isolated"}
_POSITION_MODE = {"net", "long_short"}


def _normalize_trade_mode(value: object, *, fallback: str = "cross") -> str:
    text = str(value or "").strip().lower()
    normalized_fallback = str(fallback or "").strip().lower()
    if normalized_fallback not in _TRADE_MODE:
        normalized_fallback = ""
    if text in {"cross", "isolated", "cash"}:
        return "cross" if text == "cash" else text
    if "cross" in text or "全仓" in str(value or ""):
        return "cross"
    if "isolated" in text or "逐仓" in str(value or ""):
        return "isolated"
    return normalized_fallback


def _normalize_position_mode(value: object, *, fallback: str = "net") -> str:
    text = str(value or "").strip().lower()
    lowered = str(value or "")
    normalized_fallback = str(fallback or "").strip().lower()
    if normalized_fallback not in _POSITION_MODE:
        normalized_fallback = ""
    if text in {"net", "long_short"}:
        return text
    if "net" in text or "净持仓" in lowered:
        return "net"
    if "long_short" in text or "long/short" in text or "long short" in text or "long-short" in text or "双向" in lowered:
        return "long_short"
    return normalized_fallback


def future_family_key(inst_id: str) -> str | None:
    normalized = inst_id.strip().upper()
    if infer_inst_type(normalized) != "FUTURES":
        return None
    parts = [part for part in normalized.split("-") if part]
    if len(parts) < 3:
        return None
    expiry = parts[-1]
    if len(expiry) != 6 or not expiry.isdigit():
        return None
    return "-".join(parts[:-1])


def future_expiry_code(inst_id: str) -> str | None:
    normalized = inst_id.strip().upper()
    if infer_inst_type(normalized) != "FUTURES":
        return None
    parts = [part for part in normalized.split("-") if part]
    if len(parts) < 3:
        return None
    expiry = parts[-1]
    if len(expiry) != 6 or not expiry.isdigit():
        return None
    return expiry


def roll_target_future_candidates(current_inst_id: str, instruments: list[Instrument]) -> list[str]:
    current_family = future_family_key(current_inst_id)
    current_expiry = future_expiry_code(current_inst_id)
    if current_family is None or current_expiry is None:
        return []
    candidates: list[str] = []
    for instrument in instruments:
        inst_id = instrument.inst_id.strip().upper()
        if inst_id == current_inst_id.strip().upper():
            continue
        if instrument.state and instrument.state.lower() not in {"live", "test"}:
            continue
        if future_family_key(inst_id) != current_family:
            continue
        expiry = future_expiry_code(inst_id)
        if expiry is None or expiry <= current_expiry:
            continue
        candidates.append(inst_id)
    candidates.sort(key=lambda item: (future_expiry_code(item) or "", item))
    return candidates


def credential_profile_environment(profile_snapshot: dict[str, str] | None, *, fallback: str = "demo") -> str:
    environment = str((profile_snapshot or {}).get("environment", "") or "").strip().lower()
    if environment in {"demo", "live"}:
        return environment
    return fallback if fallback in {"demo", "live"} else "demo"


def build_runtime_for_profile(
    profile_name: str,
    *,
    profile_snapshot: dict[str, str] | None,
    fallback_runtime: ArbitrageTradeRuntime | None,
    trade_mode: str | None = None,
    position_mode: str | None = None,
) -> ArbitrageTradeRuntime | None:
    target_profile = profile_name.strip() or (
        fallback_runtime.credential_profile_name.strip() if fallback_runtime is not None else DEFAULT_CREDENTIAL_PROFILE_NAME
    )
    target_profile = target_profile or DEFAULT_CREDENTIAL_PROFILE_NAME
    snapshot = profile_snapshot or {}
    api_key = str(snapshot.get("api_key", "") or "").strip()
    secret_key = str(snapshot.get("secret_key", "") or "").strip()
    passphrase = str(snapshot.get("passphrase", "") or "").strip()
    if not api_key or not secret_key or not passphrase:
        if fallback_runtime is None:
            return None
        fallback_profile = fallback_runtime.credential_profile_name.strip() or target_profile
        if fallback_profile != target_profile:
            return None
        return ArbitrageTradeRuntime(
            credentials=Credentials(
                fallback_runtime.credentials.api_key,
                fallback_runtime.credentials.secret_key,
                fallback_runtime.credentials.passphrase,
                profile_name=target_profile,
            ),
            environment=fallback_runtime.environment,
            trade_mode=fallback_runtime.trade_mode,
            position_mode=fallback_runtime.position_mode,
            credential_profile_name=target_profile,
        )
    fallback_environment = fallback_runtime.environment if fallback_runtime is not None else "demo"
    environment = credential_profile_environment(snapshot, fallback=fallback_environment)
    resolved_trade_mode = (
        _normalize_trade_mode(trade_mode, fallback="")
        or _normalize_trade_mode(snapshot.get("trade_mode"), fallback="")
        or _normalize_trade_mode(snapshot.get("trade_mode_label"), fallback="")
        or _normalize_trade_mode(snapshot.get("mgn_mode"), fallback="")
    )
    resolved_position_mode = (
        _normalize_position_mode(position_mode, fallback="")
        or _normalize_position_mode(snapshot.get("position_mode"), fallback="")
        or _normalize_position_mode(snapshot.get("position_mode_label"), fallback="")
    )
    if not resolved_trade_mode:
        resolved_trade_mode = fallback_runtime.trade_mode if fallback_runtime is not None else "cross"
    if not resolved_position_mode:
        resolved_position_mode = fallback_runtime.position_mode if fallback_runtime is not None else "net"
    return ArbitrageTradeRuntime(
        credentials=Credentials(
            api_key,
            secret_key,
            passphrase,
            profile_name=target_profile,
        ),
        environment=environment,
        trade_mode=resolved_trade_mode,
        position_mode=resolved_position_mode,
        credential_profile_name=target_profile,
    )
