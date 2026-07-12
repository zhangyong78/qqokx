from okx_quant.models import Credentials
from okx_quant.okx_private_ws import OkxPrivateWsConnection


def _connection() -> OkxPrivateWsConnection:
    return OkxPrivateWsConnection(
        Credentials(api_key="k", secret_key="s", passphrase="p", profile_name="test"),
        environment="demo",
    )


def test_store_positions_merges_incremental_updates() -> None:
    connection = _connection()
    connection._store_positions(  # noqa: SLF001
        [
            {
                "instId": "BTC-USD-260626",
                "instType": "FUTURES",
                "posSide": "short",
                "mgnMode": "cross",
                "pos": "827.7",
            }
        ]
    )
    connection._store_positions(  # noqa: SLF001
        [
            {
                "instId": "BTC-USD-260925",
                "instType": "FUTURES",
                "posSide": "short",
                "mgnMode": "cross",
                "pos": "29",
            },
            {
                "instId": "BTC-USD-261225",
                "instType": "FUTURES",
                "posSide": "short",
                "mgnMode": "cross",
                "pos": "40",
            },
        ]
    )

    payload = connection.get_latest_positions()
    assert payload is not None
    _, items = payload
    assert [item["instId"] for item in items] == [
        "BTC-USD-260626",
        "BTC-USD-260925",
        "BTC-USD-261225",
    ]


def test_store_positions_removes_zeroed_position_from_snapshot() -> None:
    connection = _connection()
    connection._store_positions(  # noqa: SLF001
        [
            {
                "instId": "BTC-USD-260626",
                "instType": "FUTURES",
                "posSide": "short",
                "mgnMode": "cross",
                "pos": "827.7",
            },
            {
                "instId": "BTC-USD-260925",
                "instType": "FUTURES",
                "posSide": "short",
                "mgnMode": "cross",
                "pos": "29",
            },
        ]
    )
    connection._store_positions(  # noqa: SLF001
        [
            {
                "instId": "BTC-USD-260925",
                "instType": "FUTURES",
                "posSide": "short",
                "mgnMode": "cross",
                "pos": "0",
            }
        ]
    )

    payload = connection.get_latest_positions()
    assert payload is not None
    _, items = payload
    assert [item["instId"] for item in items] == ["BTC-USD-260626"]


def test_update_listener_receives_order_version_once_before_unsubscribe() -> None:
    connection = _connection()
    received: list[tuple[str, int]] = []

    unsubscribe = connection.add_update_listener(lambda channel, version: received.append((channel, version)))
    connection._store_orders([{"ordId": "1", "state": "live"}])  # noqa: SLF001
    unsubscribe()
    unsubscribe()
    connection._store_orders([{"ordId": "1", "state": "filled"}])  # noqa: SLF001

    assert received == [("orders", 1)]


def test_update_listener_receives_position_and_account_versions() -> None:
    connection = _connection()
    received: list[tuple[str, int]] = []
    connection.add_update_listener(lambda channel, version: received.append((channel, version)))

    connection._store_positions(  # noqa: SLF001
        [{"instId": "BTC-USDT-SWAP", "posSide": "long", "mgnMode": "cross", "pos": "1"}]
    )
    connection._store_account([{"uTime": "1"}])  # noqa: SLF001

    assert received == [("positions", 1), ("account", 2)]
