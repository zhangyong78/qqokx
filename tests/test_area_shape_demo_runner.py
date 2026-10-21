"""Network retry policy must never repeat a simulated trading mutation."""

from types import SimpleNamespace
from urllib.error import HTTPError, URLError
from unittest.mock import patch

import pytest

from okx_quant.okx_client import OkxApiError, OkxRestClient
from scripts.run_area_shape_demo_e2e import DemoClient


@pytest.mark.parametrize("method,authentication_error,retries", [
    ("GET", False, True), ("POST", False, False), ("GET", True, False),
])
def test_demo_runner_only_retries_readonly_network_failures(method, authentication_error, retries):
    events = []
    client = DemoClient(SimpleNamespace(emit=lambda kind, **kwargs: events.append((kind, kwargs))))
    cause = (HTTPError("https://example.invalid", 401, "unauthorized", {}, None)
             if authentication_error else URLError(TimeoutError("synthetic TLS timeout")))
    failure = OkxApiError("synthetic request failure")
    failure.__cause__ = cause
    try:
        with patch.object(OkxRestClient, "_request", side_effect=[failure, {"data": []}]) as request:
            with patch("scripts.run_area_shape_demo_e2e.time.sleep"):
                if retries:
                    assert client._request(method, "/api/v5/trade/order", auth=True, simulated=True) == {"data": []}
                else:
                    with pytest.raises(OkxApiError):
                        client._request(method, "/api/v5/trade/order", auth=True, simulated=True)
        assert request.call_count == (2 if retries else 1)
        assert sum(kind == "readonly_network_retry" for kind, _ in events) == int(retries)
    finally:
        client.close()
