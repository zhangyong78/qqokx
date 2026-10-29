from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, Mock, patch
from urllib.error import URLError

import pytest

from okx_quant import network_transport, websockets_compat
from okx_quant.models import Credentials
from okx_quant.okx_algo_ws import OkxAlgoWsConnection, OkxAlgoWsConnectionUnavailable
from okx_quant.okx_candle_ws import CandleStreamKey, OkxCandleWsConnection
from okx_quant.okx_client import OkxApiError, OkxRestClient
from okx_quant.okx_private_ws import OkxPrivateWsConnection, OkxPrivateWsConnectionUnavailable
from okx_quant.okx_public_ws import OkxPublicWsConnection


def test_http_transport_refreshes_proxy_changes_and_removal() -> None:
    settings = [{"https": "http://127.0.0.1:7890"}, {"https": "http://127.0.0.1:7890"},
                {"https": "http://127.0.0.1:7891"}, {}]
    with (
        patch.object(network_transport, "_proxy_settings", None),
        patch.object(network_transport.request, "getproxies", side_effect=settings),
        patch.object(network_transport.request, "build_opener") as build,
        patch.object(network_transport.request, "install_opener") as install,
    ):
        for _ in settings:
            network_transport.refresh_http_proxy_settings()
        assert build.call_count == 3
        assert install.call_count == 3


def test_modern_websocket_keeps_proxy_detection_enabled_and_demo_headers() -> None:
    with (
        patch.object(websockets_compat, "_modern_proxy_supported", True),
        patch.object(websockets_compat, "_modern_connect") as modern,
        patch.object(websockets_compat, "_legacy_connect") as legacy,
    ):
        websockets_compat.connect_okx_websocket(
            "wss://wspap.okx.com/ws/v5/private", headers={"x-simulated-trading": "1"}, open_timeout=20,
        )
        modern.assert_called_once_with(
            "wss://wspap.okx.com/ws/v5/private", proxy=True,
            create_connection=websockets_compat._SafeClientConnection,
            additional_headers={"x-simulated-trading": "1"}, open_timeout=20,
        )
        legacy.assert_not_called()


def test_websocket_proxy_reset_before_connection_made() -> None:
    from websockets.client import ClientProtocol
    from websockets.uri import parse_uri

    async def check() -> None:
        connection = websockets_compat._SafeClientConnection(
            ClientProtocol(parse_uri("wss://example.com"))
        )
        connection.connection_lost(ConnectionResetError())
        assert connection.connection_lost_waiter.done()

    asyncio.run(check())


def test_old_websocket_api_preserves_header_compatibility() -> None:
    with (
        patch.object(websockets_compat, "_modern_proxy_supported", False),
        patch.object(websockets_compat, "_legacy_connect") as legacy,
    ):
        websockets_compat.connect_okx_websocket("wss://ws.okx.com/ws/v5/public", headers={"demo": "1"})
        legacy.assert_called_once_with("wss://ws.okx.com/ws/v5/public", extra_headers={"demo": "1"})


def test_network_failure_does_not_replay_an_order_submission() -> None:
    client = OkxRestClient()
    with (
        patch("okx_quant.okx_client.refresh_http_proxy_settings"),
        patch("okx_quant.okx_client.request.urlopen", side_effect=URLError("proxy offline")) as send,
    ):
        with pytest.raises(OkxApiError, match="proxy offline"):
            client._request("POST", "/api/v5/trade/order", body={"instId": "BTC-USDT-SWAP"})
        assert send.call_count == 1


def test_idle_websocket_sends_text_heartbeat_and_accepts_pong() -> None:
    socket = AsyncMock()
    socket.recv.side_effect = [asyncio.TimeoutError(), "pong"]
    assert asyncio.run(websockets_compat.receive_okx_websocket(socket)) == "pong"
    socket.send.assert_awaited_once_with("ping")


def test_half_open_websocket_times_out_when_ping_has_no_response() -> None:
    async def stalled_recv():
        await asyncio.Event().wait()

    socket = AsyncMock()
    socket.recv.side_effect = stalled_recv
    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(websockets_compat.receive_okx_websocket(socket, idle_timeout=0.01, pong_timeout=0.01))
    socket.send.assert_awaited_once_with("ping")


@pytest.mark.parametrize("kind", ["private", "algo"])
def test_login_wait_has_a_real_deadline_even_when_socket_stays_open(kind: str) -> None:
    credentials = Credentials(api_key="k", secret_key="s", passphrase="p", profile_name="test")
    cls, error = ((OkxPrivateWsConnection, OkxPrivateWsConnectionUnavailable) if kind == "private"
                  else (OkxAlgoWsConnection, OkxAlgoWsConnectionUnavailable))
    connection = cls(credentials, environment="demo")
    socket = AsyncMock()

    async def stalled_recv():
        await asyncio.Event().wait()

    socket.recv.side_effect = stalled_recv
    with pytest.raises(error):
        asyncio.run(connection._expect_event(socket, "login", timeout_seconds=0.01))


@pytest.mark.parametrize("kind", ["public", "private", "algo", "candle"])
def test_outage_is_retried_and_subscriptions_are_restored(kind: str) -> None:
    credentials = Credentials(api_key="k", secret_key="s", passphrase="p", profile_name="test")
    if kind == "public":
        connection = OkxPublicWsConnection(environment="demo")
        connection.watch_inst_id("BTC-USDT-SWAP")
    elif kind == "candle":
        connection = OkxCandleWsConnection(environment="demo")
        connection.watch(CandleStreamKey("BTC-USDT-SWAP", "1H", "demo"), lambda *_: None)
    else:
        cls = OkxPrivateWsConnection if kind == "private" else OkxAlgoWsConnection
        connection = cls(credentials, environment="demo")

    failed = AsyncMock()
    failed.__aenter__.side_effect = OSError("local proxy offline")
    recovered = AsyncMock()
    socket = AsyncMock()
    recovered.__aenter__.return_value = socket
    notifications = Mock()
    if kind in {"private", "algo"}:
        connection.add_update_listener(notifications)
    replies = iter([json.dumps({"event": "login"}), json.dumps({"event": "subscribe"})]
                   if kind in {"private", "algo"} else [])

    async def recv():
        try:
            return next(replies)
        except StopIteration:
            connection._stop_event.set()
            return "pong"

    socket.recv.side_effect = recv
    module = f"okx_quant.okx_{kind}_ws"
    with (
        patch(f"{module}.connect_okx_websocket", side_effect=[failed, recovered]) as connect,
        patch(f"{module}.wait_for_websocket_retry", new_callable=AsyncMock),
    ):
        asyncio.run(connection._run_forever_async())
    assert connect.call_count == 2
    sent = [json.loads(call.args[0]) for call in socket.send.await_args_list]
    assert any(item.get("op") == "subscribe" for item in sent)
    if kind in {"private", "algo"}:
        assert any(item.get("op") == "login" for item in sent)
        notifications.assert_called_once_with("reconnected", 0)
    assert connection._connected is False
    assert connection._socket is None


@pytest.mark.parametrize("kind", ["public", "private", "algo"])
def test_subscription_failure_clears_connected_flag_and_socket(kind: str) -> None:
    credentials = Credentials(api_key="k", secret_key="s", passphrase="p", profile_name="test")
    if kind == "public":
        connection = OkxPublicWsConnection(environment="demo")
        subscription = "_ensure_all_subscriptions"
    else:
        cls = OkxPrivateWsConnection if kind == "private" else OkxAlgoWsConnection
        connection = cls(credentials, environment="demo")
        connection._login = AsyncMock()
        subscription = "_subscribe"
    context = AsyncMock()
    with (
        patch(f"okx_quant.okx_{kind}_ws.connect_okx_websocket", return_value=context),
        patch.object(connection, subscription, side_effect=OSError("proxy tunnel dropped")),
    ):
        with pytest.raises(OSError):
            asyncio.run(connection._run_connection_once())
    assert connection._connected is False
    assert connection._socket is None


def test_modern_websocket_recovers_when_local_http_proxy_changes_port() -> None:
    if not websockets_compat._modern_proxy_supported:
        pytest.skip("requires proxy-aware websockets >= 15")
    from websockets.asyncio.server import serve

    async def scenario():
        proxy_settings = {}
        connections = []

        async def endpoint(socket):
            await socket.send("network-restored")
            await socket.wait_closed()

        async def unavailable_proxy(reader, writer):
            await reader.readuntil(b"\r\n\r\n")
            writer.write(b"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        async def working_proxy(reader, writer):
            header = await reader.readuntil(b"\r\n\r\n")
            authority = header.split(b" ", 2)[1].decode("ascii")
            connections.append(authority)
            host, port = authority.rsplit(":", 1)
            upstream_reader, upstream_writer = await asyncio.open_connection(host, int(port))
            writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await writer.drain()

            async def relay(source, destination):
                try:
                    while data := await source.read(8192):
                        destination.write(data)
                        await destination.drain()
                finally:
                    destination.close()

            try:
                await asyncio.gather(relay(reader, upstream_writer), relay(upstream_reader, writer))
            finally:
                upstream_writer.close()
                writer.close()

        async with serve(endpoint, "127.0.0.1", 0) as server:
            uri = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
            async with await asyncio.start_server(unavailable_proxy, "127.0.0.1", 0) as unavailable:
                proxy_settings["https"] = f"http://127.0.0.1:{unavailable.sockets[0].getsockname()[1]}"
                with (
                    patch("urllib.request.getproxies", side_effect=lambda: dict(proxy_settings)),
                    patch("urllib.request.proxy_bypass", return_value=False),
                ):
                    with pytest.raises(Exception):
                        async with websockets_compat.connect_okx_websocket(uri, open_timeout=1):
                            pytest.fail("an unavailable proxy must not connect")
                    async with await asyncio.start_server(working_proxy, "127.0.0.1", 0) as restored:
                        proxy_settings["https"] = f"http://127.0.0.1:{restored.sockets[0].getsockname()[1]}"
                        async with websockets_compat.connect_okx_websocket(uri, open_timeout=1) as socket:
                            assert await asyncio.wait_for(socket.recv(), timeout=1) == "network-restored"
        assert len(connections) == 1

    asyncio.run(scenario())
