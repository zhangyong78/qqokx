"""Independent live-readiness acceptance; all exchange calls are intercepted."""

from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch
import time

import pytest

from okx_quant import area_shape_execution as execution
from okx_quant.area_shape_execution_monitor import AreaShapeDemoExecutionMonitor
from okx_quant.area_shape_execution_store import load_area_shape_execution_records, save_area_shape_execution_records
from okx_quant.area_shape_trade import AreaShapeConfig, AreaShapePlan
from okx_quant.engine import StrategyEngine
from okx_quant.models import Candle, Credentials, Instrument, StrategyConfig
from okx_quant.okx_client import OkxRestClient
from tests.qt_test_case import QtWidgetTestCase


def instrument():
    return Instrument(inst_id="BTC-USDT-SWAP", inst_type="SWAP", state="live",
        tick_size=Decimal("0.1"), lot_size=Decimal("0.01"), min_size=Decimal("0.01"),
        settle_ccy="USDT", ct_val=Decimal("0.01"), ct_mult=Decimal(1), ct_val_ccy="BTC",
        inst_family="BTC-USDT")


def test_live_arm_preflight_reaches_gtc_through_real_client_adapters_without_demo_requests():
    """Exercise preflight too: correct POST alone is insufficient to enable live."""
    client = OkxRestClient()
    calls = []
    posted = {}
    def request(method, path, **kwargs):
        assert kwargs["simulated"] is False
        calls.append((method, path))
        if path == "/api/v5/account/config":
            return {"data": [{"uid": "acceptance-uid", "posMode": "net_mode", "acctLv": "2"}]}
        if path == "/api/v5/account/trade-fee":
            return {"data": [{"feeGroup": [{"taker": "-0.00036"}]}]}
        if path == "/api/v5/trade/order" and method == "POST":
            posted.update(kwargs["body"])
            return {"data": [{"ordId": "acceptance-order", "sCode": "0"}]}
        if path == "/api/v5/trade/order" and method == "GET":
            return {"data": [dict(posted, ordId="acceptance-order", state="live", accFillSz="0")]}
        if path in {"/api/v5/account/positions", "/api/v5/trade/orders-pending", "/api/v5/trade/orders-algo-pending"}:
            return {"data": []}
        raise AssertionError(f"Unexpected request: {method} {path}")
    client._request = request
    client.get_area_shape_instrument = lambda _: instrument()
    client._get_account_config_cached = lambda *_: SimpleNamespace(position_mode="net_mode")
    client._enforce_order_size_safety_limit = lambda **_: None
    baseline = int(time.time() * 1000) // 3_600_000 * 3_600_000 - 3_600_000
    client.get_candles_history = lambda *_args, **_kwargs: [
        Candle(baseline, Decimal(100), Decimal(102), Decimal(98), Decimal(100), Decimal(1), True)]
    runtime = SimpleNamespace(environment="live", credential_profile_name="acceptance",
        credentials=Credentials("fake", "fake", "fake", "acceptance"), trade_mode="cross", position_mode="net")
    plan = AreaShapePlan.create(profile_name="acceptance", environment="live", symbol="BTC-USDT-SWAP",
        support_price=Decimal(95), pressure_price=Decimal(150), config=AreaShapeConfig(
            entry_trigger_mode="direct_touch", stop_basis="fixed_distance", fixed_stop_distance=Decimal(10),
            target_buffer_atr=Decimal(0), break_even_enabled=False))
    plan.activate(last_closed_candle_ts=baseline, profile_name="acceptance", environment="live")
    paper_routes = []
    paper = SimpleNamespace(mark_area_shape_execution=lambda plan_id, mode: paper_routes.append((plan_id, mode)))
    monitor = AreaShapeDemoExecutionMonitor(client_factory=lambda: client, store_load=lambda: [], store_save=lambda _: None)
    try:
        session = monitor.arm_plan(plan=plan, runtime=runtime, unlocked=True, paper_monitor=paper, environment="live")
        assert session["status"] == "armed"
        assert session["mode"] == "live_exchange" and session["environment"] == "live"
        assert paper_routes == [(plan.plan_id, "live_exchange")]
        assert ("GET", "/api/v5/account/trade-fee") in calls
        assert ("GET", "/api/v5/trade/orders-algo-pending") in calls
        assert calls.count(("POST", "/api/v5/trade/order")) == 1
        assert posted["ordType"] == "limit" and posted["px"] == "95"
        assert len(posted["attachAlgoOrds"]) == 1
        assert session["resting_entry_intent"]["cl_ord_id"] == posted["clOrdId"]
    finally:
        client.close()


@pytest.mark.parametrize("environment", ["demo", "live"])
@pytest.mark.parametrize("direction", ["long", "short"])
@pytest.mark.parametrize("route", ["limit", "fok", "trigger"])
@pytest.mark.parametrize("position_mode", ["net", "long_short"])
def test_transport_routes_exact_environment_and_attaches_both_protections(environment, direction, route, position_mode):
    client = OkxRestClient()
    client.get_area_shape_instrument = lambda _: instrument()
    client._get_account_config_cached = lambda *_: SimpleNamespace(
        position_mode="net_mode" if position_mode == "net" else "long_short_mode")
    client._enforce_order_size_safety_limit = lambda **_: None
    calls = []
    def request(method, path, **kwargs):
        calls.append((method, path, kwargs))
        return {"data": [{"ordId": "test-entry", "algoId": "test-trigger", "sCode": "0"}]}
    client._request = request
    config = StrategyConfig(inst_id="BTC-USDT-SWAP", bar="1H", ema_period=15, atr_period=14,
        atr_stop_multiplier=Decimal(1), atr_take_multiplier=Decimal(1), order_size=Decimal(1),
        trade_mode="cross", signal_mode="both", position_mode=position_mode, environment=environment,
        tp_sl_trigger_type="last")
    stop = Decimal(95 if direction == "long" else 105)
    tp = execution.price_for_net_r(entry=Decimal(100), stop=stop, direction=direction,
        target_r=Decimal(15), fee_rate=Decimal("0.00036"), slippage_rate=Decimal(0),
        contract_multiplier=Decimal("0.01"), tick_size=Decimal("0.1"))
    kwargs = dict(inst_id="BTC-USDT-SWAP", side="buy" if direction == "long" else "sell",
        size=Decimal("0.01"), limit_price=Decimal(100), stop_price=stop,
        take_profit_price=tp, client_order_id="acceptentry", stop_algo_client_order_id="acceptprotect")
    if route == "trigger":
        kwargs["trigger_price"] = Decimal(100)
    try:
        getattr(client, f"place_area_shape_{route}_entry")(Credentials("fake", "fake", "fake"), config, **kwargs)
        assert len(calls) == 1
        method, path, call = calls[0]
        assert method == "POST"
        assert call["simulated"] is (environment == "demo")
        assert path == ("/api/v5/trade/order-algo" if route == "trigger" else "/api/v5/trade/order")
        attachment = call["body"]["attachAlgoOrds"][0]
        assert Decimal(attachment["slTriggerPx"]) == stop
        assert Decimal(attachment["tpTriggerPx"]) == tp
        assert attachment["slTriggerPxType"] == attachment["tpTriggerPxType"] == "last"
        assert attachment["slOrdPx"] == attachment["tpOrdPx"] == "-1"
        assert attachment["attachAlgoClOrdId"] == "acceptprotect"
        if position_mode == "long_short":
            assert call["body"]["posSide"] == direction
        with pytest.raises(PermissionError):
            live_config = replace(config, environment="live")
            getattr(client, f"place_area_shape_demo_{route}_entry")(
                Credentials("fake", "fake", "fake"), live_config, **kwargs)
        assert len(calls) == 1
    finally:
        client.close()


@pytest.mark.parametrize("direction", ["long", "short"])
@pytest.mark.parametrize("kind", ["sl", "tp"])
@pytest.mark.parametrize("position_mode", ["net", "long_short"])
def test_live_fallback_protection_is_closing_only_and_uses_last_market_execution(direction, kind, position_mode):
    client = OkxRestClient()
    client.get_area_shape_instrument = lambda _: instrument()
    client._get_account_config_cached = lambda *_: SimpleNamespace(
        position_mode="net_mode" if position_mode == "net" else "long_short_mode")
    calls = []
    def request(method, path, **kwargs):
        calls.append((method, path, kwargs))
        return {"data": [{"algoId": "acceptance-protection", "sCode": "0"}]}
    client._request = request
    config = StrategyConfig(inst_id="BTC-USDT-SWAP", bar="1H", ema_period=15, atr_period=14,
        atr_stop_multiplier=Decimal(1), atr_take_multiplier=Decimal(1), order_size=Decimal(1),
        trade_mode="cross", signal_mode="both", position_mode=position_mode, environment="live",
        tp_sl_trigger_type="last")
    try:
        client.place_area_shape_protection_algo(Credentials("fake", "fake", "fake"), config,
            inst_id="BTC-USDT-SWAP", side="sell" if direction == "long" else "buy", size=Decimal("0.01"),
            pos_side=direction if position_mode == "long_short" else None,
            trigger_price=Decimal(100), client_order_id="acceptprotect", protection_kind=kind)
        assert len(calls) == 1
        method, path, call = calls[0]
        assert method == "POST" and path == "/api/v5/trade/order-algo" and call["simulated"] is False
        body = call["body"]
        assert body["ordType"] == "conditional"
        assert body[f"{kind}TriggerPxType"] == "last" and body[f"{kind}OrdPx"] == "-1"
        assert body["side"] == ("sell" if direction == "long" else "buy")
        if position_mode == "net":
            assert body["reduceOnly"] is True
        else:
            assert body["posSide"] == direction
    finally:
        client.close()


def protected_fixture(direction="long"):
    now = int(time.time() * 1000)
    sign = Decimal(1 if direction == "long" else -1)
    plan = AreaShapePlan.create(profile_name="acceptance", environment="live", symbol="BTC-USDT-SWAP",
        support_price=Decimal(20), pressure_price=Decimal(180), config=AreaShapeConfig(
            direction=direction, exit_template="lock_2r_at_3r", be_trigger_r=Decimal(1),
            lock_trigger_r=Decimal(4), lock_profit_r=Decimal(2), fee_offset_enabled=False))
    plan.status = "monitoring"
    runtime = SimpleNamespace(environment="live", credential_profile_name="acceptance",
        credentials=Credentials("fake-key", "fake-secret", "fake-pass", "acceptance"),
        trade_mode="cross", position_mode="net")
    session = execution.build_area_shape_execution_session(plan=plan, runtime=runtime,
        account_uid="test-uid", instrument=instrument(), fee_rate=Decimal("0.00036"),
        slippage_rate=Decimal(0), baseline_candle_ts=now - 3_600_000,
        confirmed_at_ms=now, unlocked=True, environment="live")
    stop, tp = Decimal(100) - sign * 5, Decimal(100) + sign * 75
    position = dict(entry="100", initial_stop=str(stop), stop=str(stop), size="1",
        entry_ts=now - 3_600_000 + 1234, stage="initial", favorable_price="100",
        last_minute_cursor_ts=0, contract_multiplier="1", pos_side=None,
        stop_algo_id="shared", stop_algo_cl_ord_id="shared", take_profit_algo_cl_ord_id="shared",
        take_profit=str(tp), take_profit_verified_at_ms=now)
    session.update(status="protected", position=position)
    class Client:
        def __init__(self):
            self.price = Decimal(100) + sign * Decimal("12.5")
            self.calls = []
            self.algo = dict(state="live", instId=plan.symbol, side="sell" if direction == "long" else "buy",
                sz="1", reduceOnly="true", posSide="net", slTriggerPx=str(stop), slOrdPx="-1",
                slTriggerPxType="last", tpTriggerPx=str(tp), tpOrdPx="-1", tpTriggerPxType="last")
        def get_positions(self, *_args, **kwargs):
            assert kwargs["environment"] == "live"
            return [SimpleNamespace(inst_id=plan.symbol, position=sign, pos_side="")]
        def get_order_algo(self, *_args, **kwargs):
            assert kwargs["environment"] == "live"
            return dict(self.algo)
        def get_trigger_price(self, *_args, **kwargs):
            assert kwargs["environment"] == "live" and _args[1] == "last"
            return self.price
        def amend_algo_order(self, *_args, **kwargs):
            assert kwargs["environment"] == "live"
            assert not any(key.startswith("new_take_profit") for key in kwargs)
            self.calls.append(dict(kwargs))
            self.algo["slTriggerPx"] = str(kwargs["new_stop_loss_trigger_price"])
    client = Client()
    monitor = AreaShapeDemoExecutionMonitor(store_load=lambda: [], store_save=lambda _: None)
    session_id = session["session_id"]
    monitor._sessions[session_id] = session
    monitor._runtimes[session_id] = runtime
    monitor._clients[session_id] = client
    return monitor, session, runtime, client, sign


@pytest.mark.parametrize("direction", ["long", "short"])
def test_live_missed_excursion_waits_then_tightens_without_changing_remote_tp(direction):
    monitor, session, _, client, sign = protected_fixture(direction)
    session_id = session["session_id"]
    minute = int(time.time() * 1000) // 60_000 * 60_000 - 60_000
    frozen_tp = client.algo["tpTriggerPx"]
    with patch.object(StrategyEngine, "_scan_dynamic_stop_minute_candles",
                      return_value=(Decimal(100) + sign * 25, minute)) as scan:
        monitor._manage_filled_position(session_id)
        assert scan.call_args.kwargs["trigger_type"] == "last"
        assert scan.call_args.kwargs["minimum_candle_ts"] >= session["position"]["entry_ts"]
    assert not client.calls
    assert Decimal(session["position"]["pending_stop_price"]) == Decimal(100) + sign * 15
    client.price = Decimal(100) + sign * 20
    with patch.object(StrategyEngine, "_scan_dynamic_stop_minute_candles", return_value=(None, minute)):
        monitor._manage_filled_position(session_id)
        monitor._manage_filled_position(session_id)
    assert len(client.calls) == 1
    assert Decimal(session["position"]["stop"]) == Decimal(100) + sign * 15
    assert client.algo["tpTriggerPx"] == frozen_tp


@pytest.mark.parametrize("field,value", [("side", "buy"), ("sz", "2"), ("reduceOnly", "false"),
                                         ("slOrdPx", "99"), ("posSide", "short")])
def test_live_protection_tamper_stops_management_even_without_new_protection_stage(field, value):
    monitor, session, _, client, _ = protected_fixture()
    client.price = Decimal(100)
    client.algo[field] = value
    with patch.object(StrategyEngine, "_scan_dynamic_stop_minute_candles", return_value=(None, None)):
        monitor._manage_filled_position(session["session_id"])
    assert session["status"] in {"manual_control", "position_unprotected", "manual_review"}
    assert not client.calls


@pytest.mark.parametrize("mutation", ["environment", "profile", "key", "credential_profile", "mode",
    "trade_mode", "position_mode", "snapshot_environment", "snapshot_symbol", "snapshot_direction",
    "snapshot_profile", "snapshot_plan_id", "snapshot_trigger"])
def test_live_identity_change_is_rejected_before_any_exchange_action(mutation):
    monitor, session, runtime, client, _ = protected_fixture()
    if mutation == "environment":
        runtime.environment = "demo"
    elif mutation == "profile":
        runtime.credential_profile_name = "other"
    elif mutation == "key":
        runtime.credentials = Credentials("other", "fake", "fake", "acceptance")
    elif mutation == "credential_profile":
        runtime.credentials = Credentials("fake-key", "fake", "fake", "other")
    elif mutation == "mode":
        session["mode"] = "demo_exchange"
    elif mutation == "trade_mode":
        runtime.trade_mode = "isolated"
    elif mutation == "position_mode":
        runtime.position_mode = "long_short"
    elif mutation == "snapshot_environment":
        session["plan_snapshot"]["environment"] = "demo"
    elif mutation == "snapshot_symbol":
        session["plan_snapshot"]["symbol"] = "ETH-USDT-SWAP"
    elif mutation == "snapshot_direction":
        session["plan_snapshot"]["config"]["direction"] = "short"
    elif mutation == "snapshot_profile":
        session["plan_snapshot"]["profile_name"] = "other"
    elif mutation == "snapshot_plan_id":
        session["plan_snapshot"]["plan_id"] = "other"
    else:
        session["plan_snapshot"]["config"]["trigger_price_type"] = "mark"
    with pytest.raises((PermissionError, ValueError, RuntimeError)):
        monitor._runtime_client(session["session_id"])
    assert not client.calls


@pytest.mark.parametrize("missing", ["account_uid", "mode", "trade_mode", "position_mode",
                                     "direction", "rule_version", "trigger_price_type", "plan_snapshot"])
def test_incomplete_live_ledger_cannot_infer_missing_frozen_identity_from_current_runtime(missing):
    monitor, session, _, client, _ = protected_fixture()
    session.pop(missing)
    with pytest.raises((PermissionError, ValueError, RuntimeError)):
        monitor._runtime_client(session["session_id"])
    assert not client.calls


def test_live_session_roundtrip_and_restart_never_implicitly_reenable_writes(tmp_path):
    _, session, _, _, _ = protected_fixture()
    path = tmp_path / "isolated-execution.json"
    save_area_shape_execution_records([session], path)
    restored = load_area_shape_execution_records(path)
    assert restored[0]["environment"] == "live" and restored[0]["mode"] == "live_exchange"
    assert "fake-key" not in path.read_text(encoding="utf-8")
    created_clients = []
    monitor = AreaShapeDemoExecutionMonitor(store_load=lambda: restored, store_save=lambda _: None,
        client_factory=lambda: created_clients.append(True))
    current = monitor.sessions()[0]
    assert current["environment"] == "live" and current["mode"] == "live_exchange"
    assert current["write_disabled"] and current["status"] == "needs_manual_rearm"
    monitor._tick(current["session_id"], current)
    assert not created_clients
    with pytest.raises((PermissionError, RuntimeError)):
        monitor._runtime_client(current["session_id"])


def test_continue_live_protection_with_no_position_ends_old_session_without_new_entry():
    monitor, session, runtime, client, _ = protected_fixture()
    session.update(status="needs_manual_rearm", position=None, pending_intent=None,
        resting_entry_intent=None, write_disabled=True, readonly_reconciled_at_ms=int(time.time() * 1000),
        readonly_reconciliation={"position": []})
    def account(*_args, **kwargs):
        assert kwargs["environment"] == "live"
        return SimpleNamespace(raw={"uid": session["account_uid"]})
    client.get_account_config = account
    client.get_positions = lambda *_args, **_kwargs: []
    client.get_area_shape_pending_orders = lambda *_args, **_kwargs: []
    now = int(time.time() * 1000)
    client.get_candles_history = lambda *_args, **_kwargs: [
        Candle(now - 3_600_000, Decimal(100), Decimal(102), Decimal(98), Decimal(100), Decimal(1), True)]
    client.place_area_shape_limit_entry = lambda *_args, **_kwargs: pytest.fail("Continue protection created a new entry")
    client.close = lambda: None
    monitor._client_factory = lambda: client
    monitor._reservations.add(monitor._reservation_key(session))
    result = monitor.rearm_session(session["session_id"], runtime=runtime, unlocked=True)
    assert result["status"] == "cancelled"
    assert result.get("resting_entry_intent") is None and result.get("pending_intent") is None
    assert monitor._reservation_key(session) not in monitor._reservations
    assert any(event["kind"] == "live_recovery_ended" for event in result["events"])
    with pytest.raises(PermissionError):
        monitor._runtime_client(session["session_id"])


class LiveUiAcceptanceTests(QtWidgetTestCase):
    def setUp(self):
        from tests.test_area_shape_demo_ui import AreaShapeDemoUiTests
        AreaShapeDemoUiTests.setUp(self)

    def tearDown(self):
        from tests.test_area_shape_demo_ui import AreaShapeDemoUiTests
        AreaShapeDemoUiTests.tearDown(self)

    def create_draft(self):
        from tests.test_area_shape_demo_ui import AreaShapeDemoUiTests
        return AreaShapeDemoUiTests._create_demo_draft(self)

    def test_live_start_explicitly_names_account_risk_and_routes_live(self):
        self.window._active_environment = lambda: "live"
        self.window._runtime.environment = "live"
        plan = self.create_draft()
        combo = self.window._area_shape_mode_combo
        self.assertGreaterEqual(combo.findData("live"), 0)
        combo.setCurrentIndex(combo.findData("live"))
        self.window._start_area_shape_monitoring()
        self.assertEqual(len(self.demo_monitor.arm_calls), 1, self.window._area_shape_status.text())
        request = self.demo_monitor.arm_calls[0]
        self.assertEqual(request["environment"], "live")
        self.assertEqual(request["plan"].environment, "live")
        self.assertEqual(self.paper_monitor.get_plan(plan.plan_id).execution_mode, "live_exchange")
        confirmation = self._confirm_mock.call_args.args[2]
        self.assertIn("实盘", confirmation)
        self.assertIn("ui-test", confirmation)
        self.assertIn("风险", confirmation)
        self.assertIn(str(plan.config.risk_amount), confirmation)

    def test_live_selection_on_demo_account_is_rejected_before_arming(self):
        plan = self.create_draft()
        combo = self.window._area_shape_mode_combo
        combo.setCurrentIndex(combo.findData("live"))
        self.window._start_area_shape_monitoring()
        self.assertEqual(self.demo_monitor.arm_calls, [])
        self.assertEqual(self.paper_monitor.get_plan(plan.plan_id).status, "draft")
