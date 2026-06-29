from __future__ import annotations

from okx_quant.arbitrage.models import ArbitrageTradeRuntime
from okx_quant.models import Credentials, Instrument
from okx_quant.okx_client import infer_inst_type
from okx_quant.persistence import DEFAULT_CREDENTIAL_PROFILE_NAME


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
    return ArbitrageTradeRuntime(
        credentials=Credentials(
            api_key,
            secret_key,
            passphrase,
            profile_name=target_profile,
        ),
        environment=environment,
        trade_mode=fallback_runtime.trade_mode if fallback_runtime is not None else "cross",
        position_mode=fallback_runtime.position_mode if fallback_runtime is not None else "net",
        credential_profile_name=target_profile,
    )
