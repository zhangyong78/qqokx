"""Local monitor / exchange protection handoff acceptance; no network access."""

import copy
import time
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from okx_quant.area_shape_execution import AREA_SHAPE_EXECUTION_RULE_VERSION
from okx_quant.engine import StrategyEngine
from okx_quant.models import Candle
from tests.test_area_shape_live_acceptance import protected_fixture


@pytest.mark.parametrize("direction", ["long", "short"])
def test_first_history_scan_pages_back_to_actual_entry(direction):
    monitor, session, _, client, sign = protected_fixture(direction)
    minute = int(time.time() * 1000) // 60_000 * 60_000
    first = minute - 420 * 60_000
    session["position"]["entry_ts"] = first - 1234
    client.price = Decimal(100) + sign * 20
    page_calls = []

    def page(_engine, inst_id, trigger_type, bar, *, limit, before_ts=None):
        assert trigger_type == "last" and bar == "1m"
        page_calls.append(before_ts)
        end = before_ts or minute
        rows = []
        for ts in range(max(first, end - limit * 60_000), end, 60_000):
            extreme = Decimal(100) + sign * (25 if ts == first else 10)
            rows.append(Candle(ts, Decimal(100), max(Decimal(100), extreme),
                               min(Decimal(100), extreme), Decimal(100), Decimal(1), True))
        return list(reversed(rows))

    with patch.object(StrategyEngine, "_dynamic_stop_candle_page", page):
        monitor._manage_filled_position(session["session_id"])
    assert len(page_calls) >= 2
    assert session["position"]["last_minute_cursor_ts"] == minute - 60_000
    assert Decimal(session["position"]["stop"]) == Decimal(100) + sign * 15


@pytest.mark.parametrize("direction", ["long", "short"])
def test_history_gap_keeps_confirmed_extreme_and_retries_gap(direction):
    monitor, session, _, client, sign = protected_fixture(direction)
    minute = int(time.time() * 1000) // 60_000 * 60_000
    cursor = minute - 20 * 60_000
    session["position"]["last_minute_cursor_ts"] = cursor
    client.price = Decimal(100) + sign * 20
    with patch.object(StrategyEngine, "_scan_dynamic_stop_minute_candles",
                      return_value=(Decimal(100) + sign * 25, cursor)):
        monitor._manage_filled_position(session["session_id"])
    assert session["position"]["last_minute_cursor_ts"] == cursor
    assert Decimal(session["position"]["stop"]) == Decimal(100) + sign * 15


@pytest.mark.parametrize("direction", ["long", "short"])
@pytest.mark.parametrize("readback", ["old", "absent"])
def test_amend_delayed_readback_keeps_pending_without_initial_stop_replacement(direction, readback):
    monitor, session, _, client, sign = protected_fixture(direction)
    sid = session["session_id"]
    minute = int(time.time() * 1000) // 60_000 * 60_000 - 60_000
    old = copy.deepcopy(client.algo)
    client.price = Decimal(100) + sign * 20
    actual_get = client.get_order_algo
    delay = {"active": True}

    def get(*args, **kwargs):
        if delay["active"] and client.calls:
            return old if readback == "old" else None
        return actual_get(*args, **kwargs)

    client.get_order_algo = get
    with patch.object(StrategyEngine, "_scan_dynamic_stop_minute_candles", return_value=(None, minute)):
        monitor._manage_filled_position(sid)
        assert session["status"] == "protected"
        assert session["position"]["pending_amend_intent"]
        delay["active"] = False
        monitor._tick(sid, copy.deepcopy(session))
    assert session["status"] == "protected"
    assert len(client.calls) == 1
    assert "pending_amend_intent" not in session["position"]
    assert Decimal(session["position"]["stop"]) == Decimal(100) + sign * 10


@pytest.mark.parametrize("direction", ["long", "short"])
@pytest.mark.parametrize("accepted", [False, True])
def test_restart_resolves_pending_amend_from_actual_old_or_new_stop(direction, accepted):
    monitor, session, runtime, client, sign = protected_fixture(direction)
    position = session["position"]
    old = position["stop"]
    candidate = str(Decimal(100) + sign * 15)
    position.update(rule_version=AREA_SHAPE_EXECUTION_RULE_VERSION,
                    pending_stage="locked", pending_stop_price=candidate,
                    pending_amend_intent=dict(trigger=candidate, stage="locked", previous_trigger=old))
    position["favorable_price"] = str(Decimal(100) + sign * 25)
    session.update(status="needs_manual_rearm", write_disabled=True,
                   readonly_reconciled_at_ms=int(time.time() * 1000), readonly_reconciliation={})
    client.get_account_config = lambda *_args, **_kwargs: SimpleNamespace(raw={"uid": session["account_uid"]})
    client.close = lambda: None
    monitor._client_factory = lambda: client
    if accepted:
        client.algo["slTriggerPx"] = candidate
    result = monitor.rearm_session(session["session_id"], runtime=runtime, unlocked=True)
    assert result["status"] == "protected"
    assert result["position"]["stop"] == (candidate if accepted else old)
    assert ("pending_amend_intent" in result["position"]) is (not accepted)
    assert not client.calls


@pytest.mark.parametrize("direction", ["long", "short"])
def test_zero_opposite_position_row_does_not_block_valid_amend(direction):
    monitor, session, _, client, sign = protected_fixture(direction)
    get = client.get_positions
    client.get_positions = lambda *args, **kwargs: get(*args, **kwargs) + [
        SimpleNamespace(inst_id=session["symbol"], position=Decimal(0), pos_side="")]
    client.price = Decimal(100) + sign * 20
    minute = int(time.time() * 1000) // 60_000 * 60_000 - 60_000
    with patch.object(StrategyEngine, "_scan_dynamic_stop_minute_candles", return_value=(None, minute)):
        monitor._manage_filled_position(session["session_id"])
    assert session["status"] == "protected"
    assert len(client.calls) == 1


def test_desktop_counts_live_closing_sessions_and_labels_environments():
    from roll_terminal_qt.launcher import LauncherWindow

    app = SimpleNamespace(
        _pages={}, _workspace_header=MagicMock(), _local_task_status=MagicMock(),
        _area_shape_execution_monitor=SimpleNamespace(sessions=lambda: [
            dict(status="closing_pending", environment="live"),
            dict(status="protected", environment="demo"),
            dict(status="closed", environment="live"),
        ]),
    )
    assert LauncherWindow._local_task_summary(app)["area_demo"] == 2
    LauncherWindow._refresh_local_task_status(app)
    app._local_task_status.setText.assert_called_once_with("智能辅助实盘 1｜智能辅助模拟盘 1")
