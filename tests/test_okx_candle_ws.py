from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from okx_quant.models import Candle
from okx_quant.okx_candle_ws import CandleStreamKey, CandleStreamState, OkxCandleWsConnection, _parse_okx_candle


def _candle(ts: int, close: str, *, confirmed: bool = False) -> Candle:
    return Candle(ts, Decimal("10"), Decimal("12"), Decimal("9"), Decimal(close), Decimal("3"), confirmed)


def test_candle_stream_replaces_open_candle_and_appends_next_candle() -> None:
    state = CandleStreamState([_candle(1000, "10")])

    state.apply(_candle(1000, "11"))
    state.apply(_candle(2000, "12"))

    assert [item.close for item in state.candles] == [Decimal("11"), Decimal("12")]


def test_parse_okx_candle_keeps_confirm_flag_and_subscription_key() -> None:
    candle = _parse_okx_candle(["1000", "10", "12", "9", "11", "3", "0", "0", "1"])

    assert candle == _candle(1000, "11", confirmed=True)
    assert CandleStreamKey("btc-usdt-swap", "1H", "demo").channel == "candle1H"


def test_disconnect_clears_socket_and_subscription_state() -> None:
    connection = OkxCandleWsConnection(environment="demo")
    key = CandleStreamKey("BTC-USDT-SWAP", "1H", "demo")
    connection.watch(key, lambda *_args: None)
    socket = AsyncMock()
    socket.recv.side_effect = RuntimeError("disconnected")
    context = AsyncMock()
    context.__aenter__.return_value = socket

    with patch("okx_quant.okx_candle_ws.connect_okx_websocket", return_value=context):
        with pytest.raises(RuntimeError, match="disconnected"):
            asyncio.run(connection._run_connection_once())

    assert connection._connected is False
    assert connection._socket is None
    assert connection._subscribed == set()
    assert key in connection._listeners


def test_worker_exit_clears_event_loop_reference() -> None:
    connection = OkxCandleWsConnection(environment="demo")
    connection._stop_event.set()
    connection._run_forever()

    assert connection._loop is None
    assert connection._connected is False


def test_subscription_send_failure_can_be_retried() -> None:
    connection = OkxCandleWsConnection(environment="demo")
    key = CandleStreamKey("BTC-USDT-SWAP", "1H", "demo")
    socket = AsyncMock()
    socket.send.side_effect = [RuntimeError("send failed"), None]
    connection._socket = socket

    async def run() -> None:
        with pytest.raises(RuntimeError, match="send failed"):
            await connection._ensure_subscription(key)
        await connection._ensure_subscription(key)

    asyncio.run(run())
    assert socket.send.await_count == 2
    assert key in connection._subscribed


def test_listener_failure_does_not_interrupt_other_candle_consumers() -> None:
    messages: list[str] = []
    received: list[Candle] = []
    connection = OkxCandleWsConnection(environment="demo", logger=messages.append)
    key = CandleStreamKey("BTC-USDT-SWAP", "1H", "demo")

    def fail(*_args: object) -> None:
        raise RuntimeError("consumer failed")

    connection.watch(key, fail)
    connection.watch(key, lambda candle, _confirmed: received.append(candle))
    payload = {
        "arg": {"channel": "candle1H", "instId": "BTC-USDT-SWAP"},
        "data": [["1000", "10", "12", "9", "11", "3", "0", "0", "1"]],
    }
    asyncio.run(connection._handle_message(json.dumps(payload)))

    assert received == [_candle(1000, "11", confirmed=True)]
    assert any("consumer failed" in message for message in messages)


def test_candle_stream_handles_heartbeat_and_server_errors() -> None:
    connection = OkxCandleWsConnection(environment="demo")
    socket = AsyncMock()
    connection._socket = socket

    async def run() -> None:
        await connection._handle_message(b"ping")
        await connection._handle_message("pong")
        with pytest.raises(RuntimeError, match="subscription rejected"):
            await connection._handle_message(json.dumps({"event": "error", "msg": "subscription rejected"}))

    asyncio.run(run())
    socket.send.assert_awaited_once_with("pong")
