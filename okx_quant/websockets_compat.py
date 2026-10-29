"""Proxy-aware WebSocket connections and bounded OKX heartbeat waits."""

from __future__ import annotations

import asyncio
import inspect
from typing import Any

from okx_quant.network_proxy import runtime_network_proxy_settings

try:
    import websockets
except Exception:  # noqa: BLE001
    websockets = None

try:
    from websockets.legacy.client import connect as _legacy_connect
except Exception:  # noqa: BLE001
    _legacy_connect = None

try:
    from websockets.asyncio.client import connect as _modern_connect
    from websockets.asyncio.client import ClientConnection as _ClientConnection
except Exception:  # noqa: BLE001
    _modern_connect = None
    _ClientConnection = None


if _ClientConnection is not None:
    class _SafeClientConnection(_ClientConnection):
        def connection_lost(self, exc: Exception | None) -> None:
            # An HTTP proxy's TLS handshake can fail before connection_made.
            if not hasattr(self, "recv_messages"):
                self.protocol.receive_eof()
                self.set_recv_exc(exc)
                if not self.connection_lost_waiter.done():
                    self.connection_lost_waiter.set_result(None)
                return
            super().connection_lost(exc)
else:
    _SafeClientConnection = None

_modern_proxy_supported = _modern_connect is not None and "proxy" in inspect.signature(_modern_connect).parameters


def connect_okx_websocket(url: str, *, headers: dict[str, str] | None = None, **kwargs: Any):
    """Create an OKX WebSocket context safely across supported versions."""
    if websockets is None:
        raise RuntimeError("websockets dependency is unavailable")
    settings = runtime_network_proxy_settings()
    proxy = True if settings.mode == "system" else (settings.url if settings.mode == "custom" else None)
    if _modern_proxy_supported:
        modern_kwargs = dict(kwargs)
        modern_kwargs.setdefault("create_connection", _SafeClientConnection)
        if headers:
            modern_kwargs["additional_headers"] = headers
        # True makes the library re-read system / environment proxies on each
        # connection attempt, including after the local proxy has restarted.
        modern_kwargs["proxy"] = proxy
        return _modern_connect(url, **modern_kwargs)
    if settings.mode == "custom":
        raise RuntimeError("自定义 HTTP 代理需要 websockets >= 15，请使用 QQOKX 项目启动脚本。")
    if _legacy_connect is not None:
        legacy_kwargs = dict(kwargs)
        if headers:
            legacy_kwargs["extra_headers"] = headers
        return _legacy_connect(url, **legacy_kwargs)
    modern_kwargs = dict(kwargs)
    if headers:
        modern_kwargs["additional_headers"] = headers
    return websockets.connect(url, **modern_kwargs)


async def receive_okx_websocket(socket: Any, *, idle_timeout: float = 20, pong_timeout: float = 10) -> Any:
    """Detect half-open tunnels with OKX's application-level ping / pong."""
    try:
        return await asyncio.wait_for(socket.recv(), timeout=idle_timeout)
    except asyncio.TimeoutError:
        await asyncio.wait_for(socket.send("ping"), timeout=pong_timeout)
        return await asyncio.wait_for(socket.recv(), timeout=pong_timeout)


async def wait_for_websocket_retry(stop_event: Any, seconds: float) -> None:
    """Back off without delaying shutdown for the entire reconnect interval."""
    deadline = asyncio.get_running_loop().time() + seconds
    while not stop_event.is_set():
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return
        await asyncio.sleep(min(0.25, remaining))
