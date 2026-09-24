from __future__ import annotations

from okx_quant.arbitrage.launch_support import build_runtime_for_profile
from okx_quant.arbitrage.models import ArbitrageTradeRuntime
from okx_quant.persistence import load_credentials_profiles_snapshot


def load_runtime(profile_name: str | None = None) -> ArbitrageTradeRuntime | None:
    snapshot = load_credentials_profiles_snapshot()
    profiles = snapshot.get("profiles", {}) if isinstance(snapshot, dict) else {}
    if not isinstance(profiles, dict) or not profiles:
        return None
    requested = str(profile_name or "").strip()
    # A caller that explicitly names a profile must never silently fall back to
    # another account.  In particular, a typo must not turn a demo operation
    # into a request against the first (possibly live) profile.
    if requested:
        if requested not in profiles:
            return None
        selected = requested
    else:
        selected = str(snapshot.get("selected_profile") or "").strip()
        if selected not in profiles:
            selected = next(iter(profiles))
    profile = profiles.get(selected)
    if not isinstance(profile, dict):
        return None
    return build_runtime_for_profile(
        selected,
        profile_snapshot=profile,
        fallback_runtime=None,
    )


def profile_names() -> list[str]:
    snapshot = load_credentials_profiles_snapshot()
    profiles = snapshot.get("profiles", {}) if isinstance(snapshot, dict) else {}
    if not isinstance(profiles, dict):
        return []
    return [str(name) for name in profiles.keys() if str(name).strip()]
