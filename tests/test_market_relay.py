from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
import pytest

from okx_quant import market_relay as relay
from okx_quant import market_relay_protocol as protocol
from okx_quant.market_relay_server import Server
from okx_quant.okx_client import OkxApiError, OkxRestClient
from okx_quant.models import Credentials

TOKEN = "a" * 43
PATH = "/api/v5/market/ticker"
QUERY = "instId=BTC-USDT"


@pytest.fixture(autouse=True)
def isolated_config(tmp_path, monkeypatch):
    monkeypatch.setattr(relay, "config_path", lambda: tmp_path / "market_relay.json")
    relay.reset_runtime_for_tests()
    yield
    relay.reset_runtime_for_tests()


def enabled_settings():
    return relay.Settings(True, "https://relay.example", TOKEN)


def test_default_off_and_config_validation_encryption_and_restart_freeze(monkeypatch):
    assert relay.load() == relay.Settings()
    with pytest.raises(ValueError):
        relay.validate(True, "http://relay.example", TOKEN)
    with pytest.raises(ValueError):
        relay.validate(True, "https://relay.example", "short")
    relay.save(enabled_settings())
    saved = json.loads(relay.config_path().read_text(encoding="utf-8"))
    assert saved["token"] != TOKEN
    assert TOKEN not in relay.config_path().read_text(encoding="utf-8")
    assert relay.runtime() == relay.Settings()  # first save preserves process-start settings
    relay.reset_runtime_for_tests()
    assert relay.runtime() == enabled_settings()
    relay.save(relay.Settings(False, "", ""))
    assert relay.runtime() == enabled_settings()  # saves apply on restart
    relay.reset_runtime_for_tests()
    assert relay.runtime() == relay.Settings(False, "", "")


def test_corrupt_config_is_not_silently_disabled(monkeypatch):
    relay.config_path().write_text("{bad", encoding="utf-8")
    with pytest.raises(ValueError, match="配置读取失败"):
        relay.runtime()


def test_execute_direct_success_business_error_and_private_never_relay(monkeypatch):
    calls = []
    monkeypatch.setattr(relay, "relay", lambda *a: calls.append(a) or {"code": "0", "data": []})
    expected = {"code": "51000", "msg": "bad request", "data": []}
    assert relay.execute(PATH, QUERY, lambda timeout: expected) == expected
    assert calls == []
    # The REST adapter excludes private requests before calling execute; verify
    # that boundary through OkxRestClient._request below.
    client = OkxRestClient()
    monkeypatch.setattr(relay, "runtime", lambda: enabled_settings())
    monkeypatch.setattr("okx_quant.okx_client.refresh_http_proxy_settings", lambda: None)
    monkeypatch.setattr("okx_quant.okx_client.request.urlopen", lambda *a, **k: (_ for _ in ()).throw(urllib.error.URLError("offline")))
    with pytest.raises(OkxApiError):
        client._request("GET", "/api/v5/account/balance", auth=True,
                        credentials=Credentials("test-key", "test-secret", "test-pass"))
    with pytest.raises(OkxApiError):
        client._request("POST", "/api/v5/trade/order", body={"instId": "BTC-USDT"},
                        auth=True, credentials=Credentials("test-key", "test-secret", "test-pass"))
    assert calls == []


def test_execute_network_failure_cooldown_and_recovery_probe(monkeypatch):
    relay_calls = []
    monkeypatch.setattr(relay, "relay", lambda p, q: relay_calls.append((p, q)) or {"code": "0", "data": [1]})
    def failed(timeout):
        assert timeout == 4.0
        raise urllib.error.URLError("offline")
    assert relay.execute(PATH, QUERY, failed)["data"] == [1]
    assert len(relay_calls) == 1
    assert relay.execute(PATH, QUERY, lambda _: pytest.fail("cooldown should relay"))["data"] == [1]
    assert len(relay_calls) == 2
    circuit = relay._circuits[relay.config_path()]
    circuit.failed_until = time.monotonic() - 1
    assert relay.execute(PATH, QUERY, lambda _: {"code": "0", "data": [2]})["data"] == [2]
    assert circuit.failed_until == 0 and not circuit.probing
    assert relay.execute(PATH, QUERY, lambda _: {"code": "0", "data": [3]})["data"] == [3]


def test_probe_exception_releases_single_probe_state(monkeypatch):
    monkeypatch.setattr(relay, "relay", lambda *a: {"code": "0", "data": []})
    circuit = relay._Circuit(failed_until=time.monotonic() - 1)
    relay._circuits[relay.config_path()] = circuit
    with pytest.raises(RuntimeError):
        relay.execute(PATH, QUERY, lambda _: (_ for _ in ()).throw(RuntimeError("business")))
    assert not circuit.probing


def test_only_one_direct_recovery_probe_runs_at_a_time(monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    relay_calls = []
    monkeypatch.setattr(relay, "relay", lambda *args: relay_calls.append(args) or {"code": "0", "data": ["relay"]})
    circuit = relay._Circuit(failed_until=time.monotonic() - 1)
    relay._circuits[relay.config_path()] = circuit
    result = []

    def probe_direct(timeout):
        entered.set()
        assert release.wait(3)
        return {"code": "0", "data": ["direct"]}

    worker = threading.Thread(target=lambda: result.append(relay.execute(PATH, QUERY, probe_direct)))
    worker.start()
    assert entered.wait(2)
    other = relay.execute(PATH, QUERY, lambda _: pytest.fail("only one probe may call direct"))
    assert other["data"] == ["relay"]
    assert len(relay_calls) == 1
    release.set()
    worker.join(3)
    assert not worker.is_alive()
    assert result[0]["data"] == ["direct"]
    assert not circuit.probing and circuit.failed_until == 0


def test_endpoint_query_validation_covers_pagination_mark_index_and_bounds():
    assert protocol.validate("/api/v5/market/history-candles", "instId=BTC-USDT&bar=1m&after=100&limit=300")["after"] == "100"
    assert protocol.validate("/api/v5/market/history-mark-price-candles", "instId=BTC-USDT-SWAP&bar=1H&before=100&limit=100")
    assert protocol.validate("/api/v5/market/history-index-candles", "instId=BTC-USDT&bar=1Dutc&limit=100")
    for path, query in ((PATH, "instId=BTC-USDT&after=1"),
                        ("/api/v5/market/books", "instId=BTC-USDT&sz=401"),
                        ("/api/v5/market/history-index-candles", "instId=BTC-USDT&bar=1m&limit=101"),
                        ("/api/v5/market/candles", "instId=BTC-USDT&bar=1Dutc&limit=301"),
                        ("/api/v5/market/ticker", "instId=BTC-USDT&url=http://evil")):
        with pytest.raises(ValueError):
            protocol.validate(path, query)


def test_okx_rest_adapter_falls_back_only_for_allowed_public_get(monkeypatch):
    client = OkxRestClient()
    monkeypatch.setattr(relay, "runtime", lambda: enabled_settings())
    monkeypatch.setattr("okx_quant.okx_client.refresh_http_proxy_settings", lambda: None)
    payload = {"code": "0", "data": [{"ts": "123"}]}
    relay_calls = []
    monkeypatch.setattr(relay, "relay", lambda path, query: relay_calls.append((path, query)) or payload)
    monkeypatch.setattr("okx_quant.okx_client.request.urlopen",
                        lambda *args, **kwargs: (_ for _ in ()).throw(urllib.error.URLError("offline")))
    assert client._request("GET", "/api/v5/market/history-candles", params={"instId": "BTC-USDT", "bar": "1m", "after": "100", "limit": "200"}) == payload
    assert relay_calls == [("/api/v5/market/history-candles", "instId=BTC-USDT&bar=1m&after=100&limit=200")]
    assert client._request("GET", "/api/v5/market/history-index-candles", params={"instId": "BTC-USDT", "bar": "1Dutc", "limit": "100"}) == payload
    assert relay_calls[-1][0] == "/api/v5/market/history-index-candles"
    relay_calls.clear()
    relay.reset_runtime_for_tests()
    monkeypatch.setattr("okx_quant.okx_client.request.urlopen", lambda *args, **kwargs: _JsonResponse(payload))
    assert client._request("GET", "/api/v5/market/ticker", params={"instId": "BTC-USDT"}) == payload
    assert relay_calls == []
    monkeypatch.setattr("okx_quant.okx_client.request.urlopen",
                        lambda *args, **kwargs: (_ for _ in ()).throw(urllib.error.HTTPError("https://www.okx.com", 400, "bad", {}, None)))
    with pytest.raises(OkxApiError):
        client._request("GET", "/api/v5/market/ticker", params={"instId": "BTC-USDT"})
    assert relay_calls == []
    monkeypatch.setattr("okx_quant.okx_client.request.urlopen",
                        lambda *args, **kwargs: _JsonResponse({"code": "51000", "msg": "business error", "data": []}))
    with pytest.raises(OkxApiError, match="business error"):
        client._request("GET", "/api/v5/market/ticker", params={"instId": "BTC-USDT"})
    assert relay_calls == []
    with pytest.raises(OkxApiError):
        client._request("GET", "/api/v5/account/balance", auth=True,
                        credentials=Credentials("test-key", "test-secret", "test-pass"))
    with pytest.raises(OkxApiError):
        client._request("POST", "/api/v5/trade/order", body={"instId": "BTC-USDT"},
                        auth=True, credentials=Credentials("test-key", "test-secret", "test-pass"))
    with pytest.raises(OkxApiError):
        client._request("GET", "/api/v5/market/ticker", params={"instId": "BTC-USDT"}, simulated=True)
    assert relay_calls == []


class _JsonResponse:
    def __init__(self, payload):
        self.data = json.dumps(payload).encode()

    def __enter__(self): return self
    def __exit__(self, *args): return None
    def read(self): return self.data


def test_rest_adapter_timeout_respects_switch_and_caller_limit(monkeypatch):
    client = OkxRestClient()
    payload = {"code": "0", "data": []}
    timeouts = []
    monkeypatch.setattr("okx_quant.okx_client.refresh_http_proxy_settings", lambda: None)
    monkeypatch.setattr(relay, "runtime", lambda: relay.Settings())
    monkeypatch.setattr("okx_quant.okx_client.request.urlopen",
                        lambda req, timeout: timeouts.append(timeout) or _JsonResponse(payload))
    assert client._request("GET", PATH, params={"instId": "BTC-USDT"}) == payload
    assert timeouts[-1] == 20
    monkeypatch.setattr(relay, "runtime", lambda: enabled_settings())
    assert client._request("GET", PATH, params={"instId": "BTC-USDT"}) == payload
    assert timeouts[-1] == 4
    assert client._request("GET", PATH, params={"instId": "BTC-USDT"}, timeout_seconds=1) == payload
    assert timeouts[-1] == 1


def _start_server(monkeypatch, upstream, **kwargs):
    server = Server(("127.0.0.1", 0), TOKEN, **kwargs)
    monkeypatch.setattr(server, "upstream", upstream)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _server_request(server, token=TOKEN, path=PATH, query=QUERY, request_id="1" * 32):
    url = f"http://127.0.0.1:{server.server_port}/v1/market{path}?{query}"
    headers = {protocol.REQUEST_HEADER: request_id}
    if token is not None:
        headers[protocol.TOKEN_HEADER] = token
    req = urllib.request.Request(url, headers=headers)
    try:
        return urllib.request.urlopen(req, timeout=3)
    except urllib.error.HTTPError as exc:
        return exc


def test_server_auth_forbidden_malformed_and_rate_limit_have_no_upstream(monkeypatch):
    hits = []
    server, thread = _start_server(monkeypatch, lambda *a, **k: hits.append(a) or {"code": "0", "data": []}, requests_per_minute=1)
    try:
        assert _server_request(server, token="wrong").code == 401
        assert _server_request(server, token=None).code == 401
        assert _server_request(server, path="/api/v5/account/balance").code == 400
        assert _server_request(server, path="/api/v5/market/books", query="instId=BTC-USDT&sz=999").code == 400
        response = _server_request(server)
        assert response.status == 200
        wrapper = json.loads(response.read())
        assert wrapper["target"] == PATH + "?" + QUERY and wrapper["request_id"] == "1" * 32
        assert _server_request(server).code == 429
        assert len(hits) == 1
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_client_checks_each_envelope_field_and_freshness(monkeypatch):
    relay.save(enabled_settings())
    relay.reset_runtime_for_tests()

    class Response:
        def __init__(self, body): self.body = json.dumps(body).encode()
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def read(self, n=-1): return self.body[:n]

    request_id = "b" * 32
    monkeypatch.setattr(relay.secrets, "token_hex", lambda count: request_id)
    valid = {"protocol": 1, "request_id": request_id, "target": PATH + "?" + QUERY,
             "served_at": time.time(), "payload": {"code": "0", "data": []}}

    def run(envelope):
        monkeypatch.setattr(relay.request, "build_opener", lambda *a: type("Opener", (), {"open": lambda self, req, timeout: Response(envelope)})())
        return relay.relay(PATH, QUERY)

    assert run(valid) == {"code": "0", "data": []}
    stale = {**valid, "served_at": time.time() - 60}
    mismatch_target = {**valid, "target": PATH}
    mismatch_id = {**valid, "request_id": "c" * 32}
    malformed_payload = {**valid, "payload": {"code": "0", "data": "not-list"}}
    for invalid in (stale, mismatch_target, mismatch_id, malformed_payload):
        with pytest.raises(relay.RelayError):
            run(invalid)


def test_client_rejects_redirect_without_following(monkeypatch):
    relay.save(enabled_settings())
    relay.reset_runtime_for_tests()
    monkeypatch.setattr(relay.request, "build_opener", lambda *a: type("Opener", (), {"open": lambda self, req, timeout: (_ for _ in ()).throw(urllib.error.HTTPError("https://evil", 302, "redirect", {}, None))})())
    with pytest.raises(relay.RelayError):
        relay.relay(PATH, QUERY)


def test_server_connection_slots_bound_workers_and_release_after_request(monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    calls = []

    def upstream(path, query, timeout):
        calls.append((path, query))
        if len(calls) == 1:
            entered.set()
            assert release.wait(3)
        return {"code": "0", "data": []}

    server, thread = _start_server(monkeypatch, upstream, max_connections=1)
    first_result = []

    def first_request():
        response = _server_request(server, request_id="2" * 32)
        first_result.append(response.status)
        response.read()

    worker = threading.Thread(target=first_request)
    worker.start()
    assert entered.wait(2)
    try:
        second = _server_request(server, request_id="3" * 32)
        assert second.code in (400, 503) or second.read() == b""
    except (ConnectionError, urllib.error.URLError):
        pass
    assert len(calls) == 1
    release.set()
    worker.join(3)
    try:
        assert first_result == [200]
        third = _server_request(server, request_id="4" * 32)
        assert third.status == 200
        third.read()
        assert len(calls) == 2
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
