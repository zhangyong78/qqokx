"""Apply QQOKX proxy settings to the shared urllib HTTP transport."""

from __future__ import annotations

import base64
import threading
from urllib import request
from urllib.parse import unquote, urlsplit

from okx_quant.network_proxy import runtime_network_proxy_settings


_proxy_lock = threading.Lock()
_proxy_settings: tuple[str, dict[str, str]] | None = None


class _ExplicitHttpProxyHandler(request.ProxyHandler):
    """An explicit app proxy must not be bypassed by system NO_PROXY rules."""

    def proxy_open(self, req, proxy, type):
        parsed = urlsplit(proxy)
        if parsed.username is not None:
            credentials = f"{unquote(parsed.username)}:{unquote(parsed.password or '')}"
            encoded = base64.b64encode(credentials.encode()).decode("ascii")
            req.add_header("Proxy-Authorization", "Basic " + encoded)
        req.set_proxy(parsed.netloc.rsplit("@", 1)[-1], "http")
        return None


def refresh_http_proxy_settings() -> None:
    """Rebuild the opener when effective proxy settings change.

    This never sends or retries a request. System proxy changes are picked up
    by the next request; the app's selected mode stays fixed until restart.
    """
    global _proxy_settings
    with _proxy_lock:
        settings = runtime_network_proxy_settings()
        if settings.mode == "system":
            proxies = request.getproxies()
        elif settings.mode == "custom":
            proxies = {"http": settings.url, "https": settings.url}
        else:
            proxies = {}
        snapshot = (settings.mode, proxies)
        if snapshot == _proxy_settings:
            return
        handler = _ExplicitHttpProxyHandler if settings.mode == "custom" else request.ProxyHandler
        opener = request.build_opener(handler(proxies))
        request.install_opener(opener)
        _proxy_settings = (settings.mode, dict(proxies))
