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


def test_terminal_order_cache_is_bounded_without_evicting_live_orders(monkeypatch) -> None:
    monkeypatch.setattr("okx_quant.okx_private_ws._TERMINAL_ORDER_CACHE_LIMIT", 2)
    connection = _connection()
    connection._store_orders([{"ordId": "live-1", "clOrdId": "live-client", "state": "live"}])  # noqa: SLF001
    for order_id in ("done-1", "done-2", "done-3"):
        connection._store_orders([{"ordId": order_id, "clOrdId": f"client-{order_id}", "state": "filled"}])  # noqa: SLF001

    payload = connection.get_latest_orders(limit=20)
    assert payload is not None
    _, rows = payload
    assert {row["ordId"] for row in rows} == {"live-1", "done-2", "done-3"}
    assert connection.get_latest_order(ord_id="done-1") is None
    assert connection.get_latest_order(cl_ord_id="client-done-1") is None


def test_order_update_with_one_identifier_refreshes_both_indexes() -> None:
    connection = _connection()
    connection._store_orders([{"ordId": "order-1", "clOrdId": "client-1", "state": "live"}])  # noqa: SLF001
    connection._store_orders([{"ordId": "order-1", "state": "filled"}])  # noqa: SLF001

    by_order = connection.get_latest_order(ord_id="order-1")
    by_client = connection.get_latest_order(cl_ord_id="client-1")
    assert by_order is not None
    assert by_client is not None
    assert by_order == by_client
    assert by_order[1]["state"] == "filled"
