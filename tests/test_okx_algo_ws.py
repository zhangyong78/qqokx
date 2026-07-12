import asyncio

from okx_quant.models import Credentials
from okx_quant.okx_algo_ws import OkxAlgoWsConnection


def _connection() -> OkxAlgoWsConnection:
    return OkxAlgoWsConnection(
        Credentials(api_key="k", secret_key="s", passphrase="p", profile_name="test"),
        environment="demo",
    )


def test_algo_ws_keeps_latest_state_by_algo_id() -> None:
    connection = _connection()
    connection._store_orders([{"algoId": "a1", "state": "live", "uTime": "10"}])  # noqa: SLF001
    connection._store_orders([{"algoId": "a1", "state": "effective", "uTime": "20"}])  # noqa: SLF001

    payload = connection.get_latest_orders()

    assert payload is not None
    version, rows = payload
    assert version == 2
    assert rows == ({"algoId": "a1", "state": "effective", "uTime": "20"},)


def test_algo_ws_uses_client_order_id_when_algo_id_is_missing() -> None:
    connection = _connection()
    connection._store_orders([{"algoClOrdId": "client-1", "state": "live"}])  # noqa: SLF001

    payload = connection.get_latest_orders()

    assert payload is not None
    _, rows = payload
    assert rows[0]["algoClOrdId"] == "client-1"


def test_algo_ws_notifies_listener_and_unsubscribe_is_idempotent() -> None:
    connection = _connection()
    received: list[tuple[str, int]] = []
    unsubscribe = connection.add_update_listener(lambda channel, version: received.append((channel, version)))

    connection._store_orders([{"algoId": "a1", "state": "live"}])  # noqa: SLF001
    unsubscribe()
    unsubscribe()
    connection._store_orders([{"algoId": "a1", "state": "effective"}])  # noqa: SLF001

    assert received == [("orders-algo", 1)]


def test_algo_ws_subscribes_to_any_instrument_type() -> None:
    class Socket:
        def __init__(self) -> None:
            self.messages: list[str] = []
            self.recv_count = 0

        async def send(self, message: str) -> None:
            self.messages.append(message)

        async def recv(self) -> str:
            self.recv_count += 1
            return '{"event":"subscribe","code":"0"}'

    socket = Socket()
    asyncio.run(_connection()._subscribe(socket))  # noqa: SLF001

    assert socket.messages == ['{"op":"subscribe","args":[{"channel":"orders-algo","instType":"ANY"}]}']
    assert socket.recv_count == 1
