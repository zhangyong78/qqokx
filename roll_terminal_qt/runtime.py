from __future__ import annotations

from okx_quant.arbitrage.models import ArbitrageTradeRuntime
from okx_quant.arbitrage_ui import _build_runtime_for_profile
from okx_quant.persistence import load_credentials_profiles_snapshot


def load_runtime(profile_name: str | None = None) -> ArbitrageTradeRuntime | None:
    snapshot = load_credentials_profiles_snapshot()
    profiles = snapshot.get("profiles", {}) if isinstance(snapshot, dict) else {}
    if not isinstance(profiles, dict) or not profiles:
        return None
    selected = str(profile_name or snapshot.get("selected_profile") or "").strip()
    if selected not in profiles:
        selected = next(iter(profiles))
    profile = profiles.get(selected)
    if not isinstance(profile, dict):
        return None
    return _build_runtime_for_profile(selected, profile_snapshot=profile, fallback_runtime=None)


def profile_names() -> list[str]:
    snapshot = load_credentials_profiles_snapshot()
    profiles = snapshot.get("profiles", {}) if isinstance(snapshot, dict) else {}
    if not isinstance(profiles, dict):
        return []
    return [str(name) for name in profiles.keys() if str(name).strip()]

