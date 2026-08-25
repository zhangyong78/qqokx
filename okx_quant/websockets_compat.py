"""Compatibility helpers for the websockets 14/15 client API."""

from __future__ import annotations

from typing import Any

try:
    import websockets
except Exception:  # noqa: BLE001
    websockets = None

try:
    from websockets.legacy.client import connect as _legacy_connect
except Exception:  # noqa: BLE001
    _legacy_connect = None


def connect_okx_websocket(url: str, *, headers: dict[str, str] | None = None, **kwargs: Any):
    """Create an OKX WebSocket context safely across supported versions."""
    if websockets is None:
        raise RuntimeError("websockets dependency is unavailable")
    if _legacy_connect is not None:
        legacy_kwargs = dict(kwargs)
        if headers:
            legacy_kwargs["extra_headers"] = headers
        return _legacy_connect(url, **legacy_kwargs)
    modern_kwargs = dict(kwargs)
    if headers:
        modern_kwargs["additional_headers"] = headers
    return websockets.connect(url, **modern_kwargs)
