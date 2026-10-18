from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch
import time

import pytest

from okx_quant.area_shape_execution import AREA_SHAPE_EXECUTION_RULE_VERSION, credential_fingerprint, restore_execution_session_for_readonly
from okx_quant.area_shape_execution_monitor import AreaShapeDemoExecutionMonitor
from okx_quant.area_shape_trade import AreaShapeConfig, AreaShapePlan
from okx_quant.engine import StrategyEngine
from okx_quant.models import Candle, Credentials, Instrument, StrategyConfig
from okx_quant.okx_client import OkxApiError, OkxRestClient


@pytest.mark.parametrize("code", ["51603", "50113"])
def test_missing_algo_is_absent_but_authentication_errors_still_fail_closed(code):
    client = OkxRestClient()
    def request(*_args, **_kwargs):
        raise OkxApiError("exchange error", code=code)
    client._request = request
    try:
        if code == "51603":
            assert client.get_order_algo(Credentials("test", "secret", "pass"), environment="demo",
                inst_id="BTC-USDT-SWAP", algo_cl_ord_id="notcreated") is None
        else:
            with pytest.raises(OkxApiError):
                client.get_order_algo(Credentials("test", "secret", "pass"), environment="demo",
                    inst_id="BTC-USDT-SWAP", algo_cl_ord_id="notcreated")
    finally:
        client.close()


@pytest.mark.parametrize("route", ["limit", "fok", "trigger"])
@pytest.mark.parametrize("direction", ["long", "short"])
def test_all_entry_adapters_attach_last_price_tp_sl_with_real_position_mode_resolver(route, direction):
    client = OkxRestClient()
    instrument = Instrument(inst_id="BTC-USDT-SWAP", inst_type="SWAP", tick_size=Decimal("0.1"),
        lot_size=Decimal("0.01"), min_size=Decimal("0.01"), state="live", settle_ccy="USDT",
        ct_val=Decimal("0.01"), ct_mult=Decimal("1"), ct_val_ccy="BTC")
    client.get_area_shape_instrument = lambda _: instrument
    client._get_account_config_cached = lambda *_: SimpleNamespace(position_mode="long_short_mode")
    client._enforce_order_size_safety_limit = lambda **_: None
    captured = []
    def request(method, path, **kwargs):
        captured.append((path, kwargs))
        return {"data": [{"ordId": "entry", "algoId": "trigger", "sCode": "0"}]}
    client._request = request
    config = StrategyConfig(inst_id=instrument.inst_id, bar="1H", ema_period=21, atr_period=14,
        atr_stop_multiplier=Decimal(1), atr_take_multiplier=Decimal(1), order_size=Decimal(1),
        trade_mode="cross", signal_mode="both", position_mode="long_short", environment="demo",
        tp_sl_trigger_type="last")
    common = dict(inst_id=instrument.inst_id, side="buy" if direction == "long" else "sell",
        size=Decimal("0.01"), limit_price=Decimal(100), stop_price=Decimal(95 if direction == "long" else 105),
        take_profit_price=Decimal(175 if direction == "long" else 25),
        client_order_id="entry1", stop_algo_client_order_id="protection1")
    if route == "trigger":
        common["trigger_price"] = Decimal(100)
    try:
        getattr(client, f"place_area_shape_demo_{route}_entry")(Credentials("test", "secret", "pass"), config, **common)
        path, call = captured[-1]
        assert call["simulated"] is True
        assert call["body"]["posSide"] == direction
        attachment = call["body"]["attachAlgoOrds"][0]
        assert attachment["slTriggerPx"] == str(common["stop_price"])
        assert attachment["tpTriggerPx"] == str(common["take_profit_price"])
        assert attachment["slTriggerPxType"] == attachment["tpTriggerPxType"] == "last"
        assert attachment["slOrdPx"] == attachment["tpOrdPx"] == "-1"
    finally:
        client.close()


def protection_fixture(direction, *, failure="", current_r=Decimal(4)):
    sign = Decimal(1) if direction == "long" else Decimal(-1)
    now = int(time.time()*1000)
    entry, stop, tp = Decimal(100), Decimal(100)-sign*5, Decimal(100)+sign*75
    plan = AreaShapePlan.create(profile_name="demo", environment="demo", symbol="BTC-USDT-SWAP",
        support_price=Decimal(95 if direction == "long" else 20), pressure_price=Decimal(180 if direction == "long" else 105),
        config=AreaShapeConfig(direction=direction, exit_template="lock_2r_at_3r", be_trigger_r=Decimal(1),
            lock_trigger_r=Decimal(4), lock_profit_r=Decimal(2), fee_offset_enabled=False))
    position = {"entry": str(entry), "initial_stop": str(stop), "stop": str(stop), "size": "1",
        "entry_ts": now-3_600_000+1234, "stage": "initial", "favorable_price": str(entry),
        "last_minute_cursor_ts": 0, "contract_multiplier": "1", "pos_side": None,
        "stop_algo_id": "shared", "stop_algo_cl_ord_id": "shared", "take_profit_algo_cl_ord_id": "shared",
        "take_profit": str(tp), "take_profit_verified_at_ms": now}
    credentials = SimpleNamespace(api_key="fake", profile_name="demo")
    runtime = SimpleNamespace(credentials=credentials, credential_profile_name="demo", environment="demo", position_mode="net", trade_mode="cross")
    session = {"session_id": "s", "plan_id": plan.plan_id, "plan_snapshot": plan.to_dict(), "profile_name": "demo",
        "environment": "demo", "api_key_sha256": credential_fingerprint("fake"), "symbol": plan.symbol,
        "rule_version": AREA_SHAPE_EXECUTION_RULE_VERSION, "status": "protected", "fee_rate": "0", "slippage_rate": "0",
        "instrument": {"tick_size": "0.1"}, "position": position, "events": []}
    class Client:
        def __init__(self):
            self.price = entry + sign*5*current_r
            self.failure = failure
            self.calls = []
            self.algo = {"state": "live", "instId": plan.symbol, "side": "sell" if direction == "long" else "buy",
                "sz": "1", "reduceOnly": "true", "posSide": "net", "slTriggerPx": str(stop), "slOrdPx": "-1",
                "slTriggerPxType": "last", "tpTriggerPx": str(tp), "tpOrdPx": "-1", "tpTriggerPxType": "last"}
        def get_positions(self, *_args, **_kwargs):
            return [SimpleNamespace(inst_id=plan.symbol, position=sign, pos_side="")]
        def get_order_algo(self, *_args, **_kwargs):
            return dict(self.algo)
        def get_trigger_price(self, *_args, **kwargs):
            assert _args[1] == "last" and kwargs["environment"] == "demo"
            return self.price
        def amend_algo_order(self, *_args, **kwargs):
            self.calls.append(kwargs)
            assert not any(key.startswith("new_take_profit") for key in kwargs)
            if self.failure == "before_accept":
                self.failure = ""
                raise TimeoutError("unaccepted amend timeout")
            self.algo["slTriggerPx"] = str(kwargs["new_stop_loss_trigger_price"])
            if self.failure == "after_accept":
                self.failure = ""
                raise TimeoutError("accepted amend response lost")
    client = Client()
    monitor = AreaShapeDemoExecutionMonitor(store_load=lambda: [], store_save=lambda _: None)
    monitor._sessions["s"] = session
    monitor._runtimes["s"] = runtime
    monitor._clients["s"] = client
    favorable = entry + sign*25  # Briefly reached net 5R while current price has already retraced.
    minute = now//60_000*60_000-60_000
    return monitor, session, client, favorable, minute, sign


@pytest.mark.parametrize("direction", ["long", "short"])
@pytest.mark.parametrize("failure", ["", "before_accept", "after_accept"])
def test_short_excursion_and_amend_timeouts_keep_highest_stage_and_never_change_tp(direction, failure):
    monitor, session, client, favorable, minute, sign = protection_fixture(direction, failure=failure)
    with patch.object(StrategyEngine, "_scan_dynamic_stop_minute_candles", return_value=(favorable, minute)) as scan:
        if failure:
            with pytest.raises(TimeoutError):
                monitor._manage_filled_position("s")
            assert session["position"]["pending_stop_price"] == str(Decimal(100)+sign*15)
        else:
            monitor._manage_filled_position("s")
        assert scan.call_args.kwargs["minimum_candle_ts"] >= session["position"]["entry_ts"]
    # A later poll has no new favorable extreme; persisted state must retain 5R.
    with patch.object(StrategyEngine, "_scan_dynamic_stop_minute_candles", return_value=(None, minute)):
        monitor._manage_filled_position("s")
        monitor._manage_filled_position("s")
    assert Decimal(session["position"]["stop"]) == Decimal(100)+sign*15
    assert client.algo["tpTriggerPx"] == str(Decimal(100)+sign*75)
    assert len(client.calls) == (2 if failure == "before_accept" else 1)
    assert session["position"]["stage"] == "locked"


@pytest.mark.parametrize("direction", ["long", "short"])
def test_historical_trigger_waits_until_current_price_allows_the_protection_price(direction):
    monitor, session, client, favorable, minute, sign = protection_fixture(direction, current_r=Decimal("2.5"))
    with patch.object(StrategyEngine, "_scan_dynamic_stop_minute_candles", return_value=(favorable, minute)):
        monitor._manage_filled_position("s")
        assert not client.calls
        assert session["position"]["pending_stage"] == "locked"
        client.price = Decimal(100)+sign*20
        monitor._manage_filled_position("s")
    assert len(client.calls) == 1
    assert Decimal(session["position"]["stop"]) == Decimal(100)+sign*15


def test_old_protection_rule_is_never_silently_upgraded_on_restart():
    restored = restore_execution_session_for_readonly({"version": AREA_SHAPE_EXECUTION_RULE_VERSION-1,
        "mode": "demo_exchange", "status": "protected"})
    assert restored["status"] == "manual_review" and restored["write_disabled"]


@pytest.mark.parametrize("pattern,span", [("double_reversal_up",2),("bottom_fractal",3)])
@pytest.mark.parametrize("touch_location", ["inside", "outside", "before_activation"])
def test_multi_candle_shape_only_uses_its_own_post_activation_touch(pattern, span, touch_location):
    hour = 3_600_000
    plan = AreaShapePlan.create(profile_name="demo", environment="demo", symbol="BTC-USDT-SWAP",
        support_price=Decimal(100), pressure_price=Decimal(150), config=AreaShapeConfig(
            atr_period=2, stop_basis="fixed_distance", fixed_stop_distance=Decimal(5), signal_patterns=(pattern,)))
    baseline = (5-span)*hour if touch_location == "before_activation" else 0
    plan.activate(last_closed_candle_ts=baseline, profile_name="demo", environment="demo")
    touch_index = 5-span if touch_location != "outside" else 4-span
    candles = [Candle(i*hour, Decimal(104), Decimal(106), Decimal(100 if i == touch_index else 103),
                      Decimal(105), Decimal(1), True) for i in range(1,5)]
    with patch("okx_quant.area_shape_trade._chart_patterns_by_candle_ts", return_value={4*hour:[(pattern,"long")]}):
        event = plan.observe(candles)
    assert (event is not None and event["kind"] == "paper_pending") == (touch_location == "inside")
