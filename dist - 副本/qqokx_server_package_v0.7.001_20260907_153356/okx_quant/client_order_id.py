from __future__ import annotations

import hashlib
import re
import uuid


# Keep the legacy clOrdId prefix and send the same 16-character value through
# OKX's broker `tag` field.  Supporting both forms keeps old broker attribution
# compatible while following the current broker API contract.
CUSTOM_ORDER_ID_PREFIX = "9b182c653bfd49BC"
OKX_BROKER_TAG = CUSTOM_ORDER_ID_PREFIX
OKX_CLIENT_ORDER_ID_MAX_LENGTH = 32
_ALPHANUMERIC = re.compile(r"[^A-Za-z0-9]")


def with_custom_order_id_prefix(value: str) -> str:
    """Return an OKX-safe client order ID carrying the rebate prefix.

    The prefix occupies half of OKX's 32-character limit.  For longer legacy
    IDs retain both their source marker (the first four characters) and their
    changing tail, rather than merely truncating one end and risking collisions.
    """
    suffix = _ALPHANUMERIC.sub("", str(value or "").strip())
    if not suffix:
        raise ValueError("custom order ID 不能为空")
    if suffix.startswith(CUSTOM_ORDER_ID_PREFIX):
        return suffix[:OKX_CLIENT_ORDER_ID_MAX_LENGTH]

    available = OKX_CLIENT_ORDER_ID_MAX_LENGTH - len(CUSTOM_ORDER_ID_PREFIX)
    if len(suffix) <= available:
        return f"{CUSTOM_ORDER_ID_PREFIX}{suffix}"
    source_length = min(4, available)
    return f"{CUSTOM_ORDER_ID_PREFIX}{suffix[:source_length]}{suffix[-(available - source_length):]}"


def new_custom_order_id(source: str = "ord") -> str:
    """Create a unique referral-attributed ID for an order path without one."""
    return with_custom_order_id_prefix(f"{source}{uuid.uuid4().hex}")


def strategy_order_identity(session_id: str, strategy_name: str) -> str:
    """Return a stable compact identity used to attribute fills after restart."""
    normalized = f"{str(session_id).strip().lower()}\0{str(strategy_name).strip().lower()}"
    return hashlib.blake2s(normalized.encode("utf-8"), digest_size=3).hexdigest()


def new_strategy_order_id(*, session_id: str, strategy_name: str, role: str) -> str:
    """Create a restart-safe strategy ID while retaining session/role attribution."""
    role_token = _ALPHANUMERIC.sub("", str(role or "").lower())[:3] or "ord"
    suffix = f"{strategy_order_identity(session_id, strategy_name)}{role_token}{uuid.uuid4().hex[:7]}"
    return with_custom_order_id_prefix(suffix)
