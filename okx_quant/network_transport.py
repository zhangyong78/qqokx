"""Keep urllib's cached HTTP transport aligned with current system proxies."""

from __future__ import annotations

import threading
from urllib import request


_proxy_lock = threading.Lock()
_proxy_settings: dict[str, str] | None = None


def refresh_http_proxy_settings() -> None:
    """Rebuild the opener only when environment / Windows proxy settings change.

    This never sends or retries a request. In-flight requests keep their opener;
    the next request picks up the new proxy configuration.
    """
    global _proxy_settings
    with _proxy_lock:
        proxies = request.getproxies()
        if proxies == _proxy_settings:
            return
        opener = request.build_opener(request.ProxyHandler(proxies))
        request.install_opener(opener)
        _proxy_settings = dict(proxies)
