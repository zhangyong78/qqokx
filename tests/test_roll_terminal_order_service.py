from __future__ import annotations

from dataclasses import fields
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from okx_quant.okx_client import OkxOrderStatus, OkxTradeOrderItem
from roll_terminal_qt.order_service import OrderFeedThread, load_current_order_views


def _order(order_id: str, *, state: str = "live", algo: bool = False) -> OkxTradeOrderItem:
    values = {field.name: None for field in fields(OkxTradeOrderItem)}
    values.update(
        source_kind="algo" if algo else "normal",
        source_label="pending",
        inst_id="BTC-USDT-SWAP",
        inst_type="SWAP",
        order_id=None if algo else order_id,
        algo_id=order_id if algo else None,
        state=state,
        raw={"algoId": order_id} if algo else {"ordId": order_id},
    )
    return OkxTradeOrderItem(**values)


class _FakeOrderClient:
    def __init__(self) -> None:
        self.connected = True
        self.pending_calls = 0
        self.pending = [_order("normal")]
        self.statuses: list[OkxOrderStatus] = []
        self.algo_orders: list[OkxTradeOrderItem] = []
        self.pending_error: Exception | None = None

    def get_private_ws_debug_status(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return {"connected": self.connected}

    def get_pending_orders(self, *args, **kwargs):  # noqa: ANN002, ANN003
        self.pending_calls += 1
        if self.pending_error is not None:
            raise self.pending_error
        return list(self.pending)

    def get_cached_private_order_statuses(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return 1, list(self.statuses)

    def get_cached_algo_order_statuses(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return 1, list(self.algo_orders)


def _runtime() -> object:
    return SimpleNamespace(credentials=object(), environment="demo")


def test_connected_feed_reads_ws_each_cycle_and_reconciles_rest_every_minute() -> None:
    client = _FakeOrderClient()
    with patch("roll_terminal_qt.order_service.OkxRestClient", return_value=client):
        feed = OrderFeedThread(_runtime())
        with patch("roll_terminal_qt.order_service.time.monotonic", return_value=0.0):
            assert feed._load_current_views()[1][0].state == "live"
        client.statuses = [
            OkxOrderStatus("normal", "filled", "buy", "limit", None, None, None, None, {"ordId": "normal"})
        ]
        with patch("roll_terminal_qt.order_service.time.monotonic", return_value=1.0):
            for _ in range(100):
                assert feed._load_current_views()[1][0].state == "filled"
        assert client.pending_calls == 1
        with patch("roll_terminal_qt.order_service.time.monotonic", return_value=60.0):
            feed._load_current_views()
        assert client.pending_calls == 2


def test_disconnect_uses_bounded_rest_fallback_and_reconnect_checks_immediately() -> None:
    client = _FakeOrderClient()
    with patch("roll_terminal_qt.order_service.OkxRestClient", return_value=client):
        feed = OrderFeedThread(_runtime())
        for now, connected, expected_calls in (
            (0.0, True, 1),
            (1.0, False, 2),
            (2.0, False, 2),
            (3.0, False, 3),
            (3.1, True, 4),
            (4.0, True, 4),
        ):
            client.connected = connected
            with patch("roll_terminal_qt.order_service.time.monotonic", return_value=now):
                feed._load_current_views()
            assert client.pending_calls == expected_calls


def test_failed_rest_reconcile_is_retried_next_cycle() -> None:
    client = _FakeOrderClient()
    client.pending_error = RuntimeError("temporary failure")
    with patch("roll_terminal_qt.order_service.OkxRestClient", return_value=client):
        feed = OrderFeedThread(_runtime())
        with patch("roll_terminal_qt.order_service.time.monotonic", return_value=0.0):
            with pytest.raises(RuntimeError, match="temporary failure"):
                feed._load_current_views()
        client.pending_error = None
        with patch("roll_terminal_qt.order_service.time.monotonic", return_value=1.0):
            feed._load_current_views()
        assert client.pending_calls == 2


def test_algo_ws_terminal_state_overrides_retained_rest_pending_snapshot() -> None:
    client = _FakeOrderClient()
    client.pending = [_order("algo", algo=True)]
    with patch("roll_terminal_qt.order_service.OkxRestClient", return_value=client):
        feed = OrderFeedThread(_runtime())
        with patch("roll_terminal_qt.order_service.time.monotonic", return_value=0.0):
            assert feed._load_current_views()[1][0].state == "live"
        client.algo_orders = [_order("algo", state="canceled", algo=True)]
        with patch("roll_terminal_qt.order_service.time.monotonic", return_value=1.0):
            views = feed._load_current_views()[1]
        assert len(views) == 1
        assert views[0].state == "canceled"
        assert views[0].raw["_feed_source"] == "ws"
        assert client.pending_calls == 1


def test_public_loader_preserves_rest_refresh_default_and_accepts_empty_snapshot() -> None:
    client = _FakeOrderClient()
    with patch("roll_terminal_qt.order_service.OkxRestClient", return_value=client):
        for _ in range(2):
            load_current_order_views(_runtime(), client=client)
        assert client.pending_calls == 2
        assert load_current_order_views(_runtime(), client=client, pending_orders=[])[1] == []
        assert client.pending_calls == 2
