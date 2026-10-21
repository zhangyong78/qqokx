"""Second release audit: races and failures omitted by the original happy path."""

from decimal import Decimal
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock, patch
import time

import pytest

from okx_quant.models import Candle
from tests.test_area_shape_live_acceptance import protected_fixture


def entry_fixture(direction="long", parent_state="effective"):
    monitor, session, runtime, client, _ = protected_fixture(direction)
    side = "buy" if direction == "long" else "sell"
    now = int(time.time() * 1000)
    intent = dict(entry_kind="trigger", signal_key="release-signal", cl_ord_id="trigger-parent",
        stop_algo_cl_ord_id="attached-protection", side=side, size="1", price="100",
        trigger_px="99" if direction == "long" else "101", stop="95" if direction == "long" else "105",
        trigger_expires_at_ms=now + 60_000)
    session.update(status="entry_intent", position=None, pending_intent=intent)
    session["plan_snapshot"]["config"]["entry_mode"] = "breakout"
    parent = dict(state=parent_state, algoClOrdId=intent["cl_ord_id"], instId=session["symbol"],
        side=side, sz="1", ordType="trigger", triggerPx=intent["trigger_px"], triggerPxType="last",
        ordPx="100", ordIdList=["filled-child"])
    child = SimpleNamespace(state="filled", ord_id="filled-child", avg_price=Decimal(100), filled_size=Decimal(1),
        raw=dict(instId=session["symbol"], ordId="filled-child", side=side, ordType="fok", sz="1", px="100", fillTime=now))
    client.get_order_algo = lambda *_args, **_kwargs: dict(parent)
    client.get_order = Mock(return_value=child)
    monitor._protect_fill = Mock()
    return monitor, session, client, parent, child


@pytest.mark.parametrize("direction", ["long", "short"])
@pytest.mark.parametrize("parent_state", ["effective", "canceled"])
def test_trigger_child_fill_is_protected_even_when_parent_cancel_races_with_execution(direction, parent_state):
    monitor, session, _, _, child = entry_fixture(direction, parent_state)
    monitor._reconcile_entry_intent(session["session_id"])
    monitor._protect_fill.assert_called_once_with(session["session_id"], child, child.raw["fillTime"])


@pytest.mark.parametrize("field,value", [("instId", "ETH-USDT-SWAP"), ("side", "sell"),
    ("algoClOrdId", "different-parent"), ("sz", "2"), ("triggerPxType", "mark"), ("ordPx", "200")])
def test_trigger_parent_identity_mismatch_is_not_used_as_fill_evidence(field, value):
    monitor, session, client, parent, _ = entry_fixture()
    parent[field] = value
    monitor._reconcile_entry_intent(session["session_id"])
    monitor._protect_fill.assert_not_called()
    client.get_order.assert_not_called()
    assert session["status"] == "manual_review"


@pytest.mark.parametrize("field,value", [("instId", "ETH-USDT-SWAP"), ("side", "sell"),
    ("ordId", "different-child"), ("sz", "2"), ("ordType", "market"), ("px", "200")])
def test_trigger_child_identity_mismatch_is_not_used_as_fill_evidence(field, value):
    monitor, session, _, _, child = entry_fixture()
    child.raw[field] = value
    monitor._reconcile_entry_intent(session["session_id"])
    monitor._protect_fill.assert_not_called()
    assert session["status"] == "manual_review"


def test_trigger_child_partial_fill_cancels_remainder_before_protecting_known_fill():
    monitor, session, client, _, child = entry_fixture()
    child.state, child.filled_size = "partially_filled", Decimal("0.4")
    calls = []
    def cancel(*_args, **kwargs):
        calls.append("cancel")
        assert kwargs.get("ord_id") == "filled-child"
        child.state = "canceled"
    client.cancel_order = cancel
    monitor._protect_fill.side_effect = lambda *_args, **_kwargs: calls.append("protect")
    monitor._reconcile_entry_intent(session["session_id"])
    assert calls == ["cancel", "protect"]


@pytest.mark.parametrize("route", ["fok", "trigger", "gtc"])
@pytest.mark.parametrize("cancel_failure", ["timeout", "still_live", "bad_readback"])
def test_partial_fill_is_protected_even_if_remainder_cancel_is_unconfirmed(route, cancel_failure):
    monitor, session, client, _, child = entry_fixture()
    child.state, child.filled_size = "partially_filled", Decimal("0.4")
    intent = session["pending_intent"]
    if route != "trigger":
        intent["entry_kind"] = route
        child.raw["clOrdId"] = intent["cl_ord_id"]
        child.raw["ordType"] = "limit" if route == "gtc" else "fok"
    if route == "gtc":
        intent["expires_at_ms"] = int(time.time() * 1000) + 60_000
        session["resting_entry_intent"] = intent
        session["status"] = "armed"
    def cancel(*_args, **_kwargs):
        if cancel_failure == "timeout":
            raise TimeoutError("cancel response unavailable")
    client.cancel_order = cancel
    if cancel_failure == "bad_readback":
        wrong = deepcopy(child)
        wrong.state, wrong.raw["instId"] = "canceled", "ETH-USDT-SWAP"
        client.get_order.side_effect = [child, wrong]
    if route == "gtc":
        monitor._reconcile_resting_entry(session["session_id"], force_cancel=True)
    else:
        monitor._reconcile_entry_intent(session["session_id"])
    monitor._protect_fill.assert_called_once()
    assert monitor._protect_fill.call_args.kwargs["intent"]["entry_cancel_unverified"]
    assert monitor._protect_fill.call_args.args[1].raw["instId"] == session["symbol"]


@pytest.mark.parametrize("mismatch", ["parent", "child"])
def test_restart_trigger_recovery_rejects_mismatched_order_before_constructing_position(mismatch):
    monitor, session, client, parent, child = entry_fixture()
    runtime = monitor._runtimes[session["session_id"]]
    session.update(status="needs_manual_rearm", write_disabled=True,
                   readonly_reconciled_at_ms=int(time.time() * 1000), readonly_reconciliation={})
    if mismatch == "parent":
        parent["algoClOrdId"] = "another-parent"
    else:
        child.raw["side"] = "sell"
    client.get_account_config = lambda *_args, **_kwargs: SimpleNamespace(raw={"uid": session["account_uid"]})
    client.close = lambda: None
    monitor._client_factory = lambda: client
    with pytest.raises(RuntimeError):
        monitor.rearm_session(session["session_id"], runtime=runtime, unlocked=True)
    assert session["position"] is None


@pytest.mark.parametrize("direction", ["long", "short"])
def test_previously_verified_stop_cancellation_never_auto_recreates_manual_protection(direction):
    monitor, session, _, client, _ = protected_fixture(direction)
    client.algo["state"] = "canceled"
    session["position"]["stop_verified_at_ms"] = int(time.time() * 1000)
    monitor._manage_filled_position(session["session_id"])
    assert session["status"] == "manual_control"
    with patch.object(monitor, "_retry_initial_protection") as retry:
        monitor._tick(session["session_id"], session)
    retry.assert_not_called()


@pytest.mark.parametrize("conflict", ["position", "order"])
def test_resume_empty_plan_rechecks_exposure_before_new_gtc(conflict):
    monitor, session, runtime, client, _ = protected_fixture()
    session.update(status="paused", paused=True, position=None, resting_entry_intent=None, pending_intent=None)
    client.get_account_config = lambda *_args, **_kwargs: SimpleNamespace(raw={"uid": session["account_uid"]})
    now = int(time.time() * 1000)
    client.get_candles_history = lambda *_args, **_kwargs: [
        Candle(now - 3_600_000, Decimal(100), Decimal(102), Decimal(98), Decimal(100), Decimal(1), True)]
    client.get_positions = lambda *_args, **_kwargs: ([SimpleNamespace(inst_id=session["symbol"], position=Decimal(1))]
        if conflict == "position" else [])
    client.get_area_shape_pending_orders = lambda *_args, **_kwargs: ([SimpleNamespace(client_order_id="manual-order")]
        if conflict == "order" else [])
    with patch.object(monitor, "_submit_resting_entry") as submit:
        with pytest.raises(RuntimeError):
            monitor.resume_session(session["session_id"], runtime=runtime, unlocked=True)
    submit.assert_not_called()
    assert session["status"] == "paused" and session["paused"]


def test_failed_state_save_blocks_order_writes_for_rest_of_process():
    monitor, session, _, _, _ = protected_fixture()
    def broken_store(_rows):
        raise OSError("synthetic disk full")
    monitor._store_save = broken_store
    with pytest.raises((OSError, RuntimeError)):
        monitor._persist_session(session["session_id"])
    with pytest.raises(RuntimeError):
        monitor._runtime_client(session["session_id"])
    with pytest.raises(RuntimeError):
        monitor._assert_write_enabled(session["session_id"])


def test_worker_does_not_die_when_recording_an_api_error_also_hits_a_disk_failure():
    monitor, session, _, _, _ = protected_fixture()
    monitor._stop = SimpleNamespace(wait=Mock(side_effect=[False, True]))
    monitor._tick = Mock(side_effect=TimeoutError("synthetic API outage"))
    monitor._store_save = Mock(side_effect=OSError("synthetic disk full"))
    monitor._run()
    assert monitor._store_write_blocked
    assert session["retry_count"] == 1


def test_expired_pause_cannot_submit_a_new_entry_before_discovering_expiration():
    monitor, session, runtime, client, _ = protected_fixture()
    session.update(status="paused", paused=True, position=None, resting_entry_intent=None, pending_intent=None,
                   expires_at_candle_ts=int(time.time() * 1000) - 2 * 3_600_000)
    client.get_account_config = lambda *_args, **_kwargs: SimpleNamespace(raw={"uid": session["account_uid"]})
    with patch.object(monitor, "_submit_resting_entry") as submit:
        result = monitor.resume_session(session["session_id"], runtime=runtime, unlocked=True)
    submit.assert_not_called()
    assert result["status"] == "expired"


@pytest.mark.parametrize("direction", ["long", "short"])
@pytest.mark.parametrize("cancel_timeout", [False, True])
@pytest.mark.parametrize("tp_pending", [False, True])
def test_close_fill_delay_keeps_reconciling_and_cleans_surviving_owned_protection(direction, cancel_timeout, tp_pending):
    monitor, session, _, client, sign = protected_fixture(direction)
    position = session["position"]
    position.update(direction=direction, stop_algo_cl_ord_id="stop-owned", stop_algo_id="stop-owned",
                    take_profit_algo_cl_ord_id="tp-owned")
    if tp_pending:
        position["take_profit_intent"] = {"trigger": position.pop("take_profit"), "size": "1", "side": "sell" if sign > 0 else "buy"}
    algos = {
        "stop-owned": dict(client.algo, state="effective", ordId="closing-child"),
        "tp-owned": dict(client.algo),
    }
    client.get_positions = lambda *_args, **_kwargs: []
    client.get_order_algo = lambda *_args, **kwargs: dict(algos[kwargs.get("algo_cl_ord_id") or kwargs.get("algo_id")])
    fills = []
    client.get_fills_history = lambda *_args, **_kwargs: list(fills)
    cancelled = []
    def cancel(*_args, **kwargs):
        identifier = kwargs["algo_cl_ord_id"]
        cancelled.append(identifier)
        algos[identifier]["state"] = "canceled"
        if cancel_timeout:
            raise TimeoutError("accepted cancel response lost")
    client.cancel_algo_order = cancel
    monitor._reservations.add(monitor._reservation_key(session))
    monitor._manage_filled_position(session["session_id"])
    assert session["status"] == "closing_pending"
    assert monitor._reservation_key(session) in monitor._reservations
    assert not cancelled
    fills.append(SimpleNamespace(order_id="closing-child", inst_id=session["symbol"],
        side="sell" if sign > 0 else "buy", pos_side="", fill_time=int(time.time() * 1000),
        fill_size=Decimal(1), fill_price=Decimal(110), fill_fee=Decimal("-0.01")))
    if cancel_timeout:
        with pytest.raises(TimeoutError):
            monitor._tick(session["session_id"], session)
        assert session["status"] == "closing_pending"
    monitor._tick(session["session_id"], session)
    assert session["status"] == "closed"
    assert cancelled == ["tp-owned"]
    assert monitor._reservation_key(session) not in monitor._reservations
