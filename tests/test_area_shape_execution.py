from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from okx_quant.area_shape_execution import (
    build_demo_execution_session,
    check_demo_execution_conflicts,
    create_persisted_entry_intent,
    credential_fingerprint,
    new_execution_client_order_id,
    price_for_net_r,
    restore_execution_session_for_readonly,
    size_usdt_linear_contracts,
    validate_demo_runtime,
    validate_usdt_linear_swap,
)
from okx_quant.area_shape_execution_store import load_area_shape_execution_records, save_area_shape_execution_records
from okx_quant.area_shape_trade import AreaShapeConfig, AreaShapePlan
from okx_quant.models import Credentials, Instrument, StrategyConfig
from okx_quant.okx_client import OkxRestClient


def _instrument(**changes):  # noqa: ANN001
    values = dict(
        inst_id="BTC-USDT-SWAP", inst_type="SWAP", tick_size=Decimal("0.1"),
        lot_size=Decimal("0.01"), min_size=Decimal("0.01"), state="live",
        settle_ccy="USDT", ct_val=Decimal("0.01"), ct_mult=Decimal("1"), ct_val_ccy="BTC",
    )
    values.update(changes)
    return Instrument(**values)


def _plan(**changes):  # noqa: ANN001
    plan = AreaShapePlan.create(
        profile_name="demo-main", environment="demo", symbol="BTC-USDT-SWAP",
        support_price=Decimal("60000"), pressure_price=Decimal("65000"),
        config=AreaShapeConfig(risk_amount=Decimal("25")),
    )
    plan.status = "monitoring"
    plan.activated_at = "2026-10-16T00:00:00Z"
    for key, value in changes.items():
        setattr(plan, key, value)
    return plan


def _runtime(*, environment="demo", profile="demo-main", api_key="test-key"):
    credentials = Credentials(api_key, "secret-not-persisted", "phrase-not-persisted", profile)
    return SimpleNamespace(credentials=credentials, environment=environment, credential_profile_name=profile,
                           trade_mode="cross", position_mode="net")


class AreaShapeExecutionCoreTests(unittest.TestCase):
    def test_demo_gate_profile_unlock_and_live_refusal(self):
        plan = _plan()
        with self.assertRaises(PermissionError):
            validate_demo_runtime(plan=plan, runtime=_runtime(), unlocked=False)
        with self.assertRaises(PermissionError):
            validate_demo_runtime(plan=plan, runtime=_runtime(environment="live"), unlocked=True)
        with self.assertRaises(PermissionError):
            validate_demo_runtime(plan=plan, runtime=_runtime(profile="other"), unlocked=True)
        self.assertEqual(validate_demo_runtime(plan=plan, runtime=_runtime(), unlocked=True)[0], "demo-main")

    def test_contract_spec_and_linear_size_are_exact(self):
        tick, lot, minimum, multiplier = validate_usdt_linear_swap(_instrument(), "BTC-USDT-SWAP")
        self.assertEqual((tick, lot, minimum, multiplier), (Decimal("0.1"), Decimal("0.01"), Decimal("0.01"), Decimal("0.01")))
        with self.assertRaises(ValueError):
            validate_usdt_linear_swap(_instrument(ct_val=None), "BTC-USDT-SWAP")
        with self.assertRaises(ValueError):
            validate_usdt_linear_swap(_instrument(settle_ccy="BTC"), "BTC-USDT-SWAP")
        size, risk_per_contract = size_usdt_linear_contracts(
            risk_amount=Decimal("25"), entry=Decimal("60000"), stop=Decimal("59000"),
            fee_rate=Decimal("0.0005"), slippage_rate=Decimal("0.001"),
            lot_size=lot, min_size=minimum, contract_multiplier=multiplier,
        )
        self.assertGreater(size, 0)
        self.assertEqual(size % lot, 0)
        self.assertLessEqual(size * risk_per_contract, Decimal("25"))
        with self.assertRaises(ValueError):
            size_usdt_linear_contracts(
                risk_amount=Decimal("0.01"), entry=Decimal("60000"), stop=Decimal("59000"),
                fee_rate=Decimal("0"), slippage_rate=Decimal("0"), lot_size=lot,
                min_size=minimum, contract_multiplier=multiplier,
            )

    def test_net_r_target_is_cost_aware_and_directional_tick_rounded(self):
        common = dict(entry=Decimal("100"), stop=Decimal("95"), target_r=Decimal("2"),
                      fee_rate=Decimal("0.001"), slippage_rate=Decimal("0.001"),
                      contract_multiplier=Decimal("1"), tick_size=Decimal("0.1"))
        long_target = price_for_net_r(direction="long", **common)
        short_target = price_for_net_r(direction="short", **common)
        net_long_r = ((long_target - common["entry"]) - (long_target + common["entry"]) * (common["fee_rate"] + common["slippage_rate"])) / (common["entry"] - common["stop"])
        net_short_r = ((common["entry"] - short_target) - (short_target + common["entry"]) * (common["fee_rate"] + common["slippage_rate"])) / (common["entry"] - common["stop"])
        self.assertGreaterEqual(net_long_r, Decimal("2"))
        self.assertGreaterEqual(net_short_r, Decimal("2"))
        long_be = price_for_net_r(**{**common, "target_r": Decimal("0")}, direction="long")
        short_be = price_for_net_r(**{**common, "target_r": Decimal("0")}, direction="short")
        self.assertGreater(long_be, common["entry"])
        self.assertLess(short_be, common["entry"])
        self.assertEqual(long_target % Decimal("0.1"), 0)
        self.assertEqual(short_target % Decimal("0.1"), 0)

    def test_session_freezes_account_rules_and_never_persists_key(self):
        session = build_demo_execution_session(
            plan=_plan(), runtime=_runtime(), account_uid="uid-1", instrument=_instrument(),
            fee_rate=Decimal("0.0005"), slippage_rate=Decimal("0.001"), baseline_candle_ts=1000,
            confirmed_at_ms=2000, unlocked=True,
        )
        self.assertEqual(session["mode"], "demo_exchange")
        self.assertEqual(session["api_key_sha256"], credential_fingerprint("test-key"))
        self.assertNotIn("test-key", str(session))
        self.assertNotIn("secret-not-persisted", str(session))
        with self.assertRaises(PermissionError):
            build_demo_execution_session(
                plan=_plan(environment="live"), runtime=_runtime(environment="live"), account_uid="uid-1",
                instrument=_instrument(), fee_rate=Decimal("0.0005"), slippage_rate=Decimal("0"),
                baseline_candle_ts=1000, confirmed_at_ms=2000, unlocked=True,
            )

    def test_restart_disables_writes_and_intent_id_is_stable(self):
        session = {"version": 1, "mode": "demo_exchange", "status": "armed", "events": [], "plan_id": "p"}
        restored = restore_execution_session_for_readonly(session)
        self.assertEqual(restored["status"], "needs_manual_rearm")
        self.assertTrue(restored["write_disabled"])
        self.assertEqual(new_execution_client_order_id("p", "signal", "entry"), new_execution_client_order_id("p", "signal", "entry"))
        intent = create_persisted_entry_intent(
            {"plan_id": "p", "status": "armed", "pending_intent": None}, signal_key="signal",
            side="buy", size=Decimal("1"), price=Decimal("10"), stop=Decimal("9"),
        )
        self.assertEqual(intent["cl_ord_id"], new_execution_client_order_id("p", "signal", "entry"))
        self.assertTrue(intent["cl_ord_id"].startswith("9b182c653bfd49BC"))
        with self.assertRaises(RuntimeError):
            create_persisted_entry_intent({"plan_id": "p", "status": "armed", "pending_intent": intent},
                                          signal_key="signal", side="buy", size=Decimal("1"),
                                          price=Decimal("10"), stop=Decimal("9"))

    def test_execution_store_version_and_secret_filter(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "state.json"
            save_area_shape_execution_records([{"session_id": "s", "mode": "bad", "api_key": "secret"}], path)
            stored = path.read_text(encoding="utf-8")
            self.assertNotIn("secret", stored)
            self.assertEqual(load_area_shape_execution_records(path)[0]["mode"], "demo_exchange")
            path.write_text('{"version":99,"sessions":[]}', encoding="utf-8")
            with self.assertRaises(ValueError):
                load_area_shape_execution_records(path)

    def test_conflict_guard_checks_uncached_exact_positions_and_orders(self):
        class Client:
            def __init__(self, positions, orders):
                self.positions, self.orders = positions, orders
                self.calls = []

            def get_positions(self, credentials, **kwargs):
                self.calls.append(kwargs)
                return self.positions

            def get_area_shape_pending_orders(self, credentials, **kwargs):
                self.calls.append(kwargs)
                return self.orders

        client = Client([], [])
        check_demo_execution_conflicts(client, object(), environment="demo", inst_id="BTC-USDT-SWAP")
        self.assertFalse(client.calls[0]["prefer_cache"])
        self.assertEqual(client.calls[1]["inst_id"], "BTC-USDT-SWAP")
        client = Client([SimpleNamespace(inst_id="BTC-USDT-SWAP", position=Decimal("1"))], [])
        with self.assertRaises(RuntimeError):
            check_demo_execution_conflicts(client, object(), environment="demo", inst_id="BTC-USDT-SWAP")
        client = Client([], [SimpleNamespace(client_order_id="foreign")])
        with self.assertRaises(RuntimeError):
            check_demo_execution_conflicts(client, object(), environment="demo", inst_id="BTC-USDT-SWAP")

    def test_client_trade_fee_and_contract_targeted_pending_are_strict(self):
        client = OkxRestClient()
        calls = []

        def request(method, path, **kwargs):
            calls.append((method, path, kwargs))
            if path.endswith("trade-fee"):
                return {"data": [{"feeGroup": [{"taker": "-0.0005"}]}]}
            return {"data": []}

        client._request = request
        self.assertEqual(client.get_area_shape_fee_rate(object(), environment="demo", inst_family="BTC-USDT"), Decimal("0.0005"))
        orders = client.get_area_shape_pending_orders(object(), environment="demo", inst_id="BTC-USDT-SWAP")
        self.assertEqual(orders, [])
        self.assertEqual(len([call for call in calls if call[1].endswith("orders-algo-pending")]), 8)
        self.assertTrue(all(call[2]["simulated"] for call in calls))

    def test_demo_entry_posts_exchange_trigger_with_bounded_fok_and_attached_stop(self):
        client = OkxRestClient()
        instrument = _instrument()
        client.get_area_shape_instrument = lambda _inst_id: instrument
        client._derivative_order_pos_side = lambda *args, **kwargs: None
        client._maybe_isolated_margin_ccy = lambda *_args, **_kwargs: None
        client._enforce_order_size_safety_limit = lambda **_kwargs: None
        captured = {}

        def request(method, path, **kwargs):
            captured.update(method=method, path=path, **kwargs)
            return {"data": [{"algoId": "algo-1", "algoClOrdId": "entry-1", "sCode": "0"}]}

        client._request = request
        config = StrategyConfig(
            inst_id=instrument.inst_id, trade_inst_id=instrument.inst_id,
            local_tp_sl_inst_id=instrument.inst_id, bar="1H", ema_period=21, atr_period=14,
            atr_stop_multiplier=Decimal("2"), atr_take_multiplier=Decimal("4"), order_size=Decimal("1"),
            trade_mode="cross", signal_mode="long_only", position_mode="net", environment="demo",
            tp_sl_trigger_type="last", tp_sl_mode="exchange", take_profit_mode="dynamic",
            risk_amount=Decimal("10"),
        )
        result = client.place_area_shape_demo_trigger_entry(
            Credentials(api_key="key", secret_key="secret", passphrase="pass"), config,
            inst_id=instrument.inst_id, side="buy", size=Decimal("1"), trigger_price=Decimal("101"),
            limit_price=Decimal("101.1"), stop_price=Decimal("99"), client_order_id="entry-1",
            stop_algo_client_order_id="stop-1",
        )
        self.assertEqual(result.ord_id, "algo-1")
        self.assertEqual(captured["path"], "/api/v5/trade/order-algo")
        self.assertTrue(captured["simulated"])
        self.assertEqual(captured["body"]["ordType"], "trigger")
        self.assertEqual(captured["body"]["triggerPxType"], "last")
        self.assertEqual(captured["body"]["advanceOrdType"], "fok")
        self.assertEqual(captured["body"]["orderPx"], "101.1")
        self.assertEqual(captured["body"]["attachAlgoOrds"][0]["slTriggerPx"], "99")

        live_config = replace(config, environment="live")
        with self.assertRaises(PermissionError):
            client.place_area_shape_demo_trigger_entry(
                Credentials(api_key="key", secret_key="secret", passphrase="pass"), live_config,
                inst_id=instrument.inst_id, side="buy", size=Decimal("1"), trigger_price=Decimal("101"),
                limit_price=Decimal("101.1"), stop_price=Decimal("99"), client_order_id="entry-2",
                stop_algo_client_order_id="stop-2",
            )

    def test_demo_local_pattern_entry_posts_regular_price_limited_fok(self):
        client = OkxRestClient()
        instrument = _instrument()
        client.get_area_shape_instrument = lambda _inst_id: instrument
        client._derivative_order_pos_side = lambda *args, **kwargs: None
        client._maybe_isolated_margin_ccy = lambda *_args, **kwargs: None
        client._enforce_order_size_safety_limit = lambda **_kwargs: None
        captured = {}

        def request(method, path, **kwargs):
            captured.update(method=method, path=path, **kwargs)
            return {"data": [{"ordId": "order-1", "clOrdId": "entry-1", "sCode": "0"}]}

        client._request = request
        config = StrategyConfig(
            inst_id=instrument.inst_id, trade_inst_id=instrument.inst_id,
            local_tp_sl_inst_id=instrument.inst_id, bar="1H", ema_period=21, atr_period=14,
            atr_stop_multiplier=Decimal("2"), atr_take_multiplier=Decimal("4"), order_size=Decimal("1"),
            trade_mode="cross", signal_mode="long_only", position_mode="net", environment="demo",
            tp_sl_trigger_type="last", tp_sl_mode="exchange", take_profit_mode="dynamic",
            risk_amount=Decimal("10"),
        )
        result = client.place_area_shape_demo_fok_entry(
            Credentials(api_key="key", secret_key="secret", passphrase="pass"), config,
            inst_id=instrument.inst_id, side="buy", size=Decimal("1"), limit_price=Decimal("101.1"),
            stop_price=Decimal("99"), client_order_id="entry-1", stop_algo_client_order_id="stop-1",
        )
        self.assertEqual(result.ord_id, "order-1")
        self.assertEqual(captured["path"], "/api/v5/trade/order")
        self.assertTrue(captured["simulated"])
        self.assertEqual(captured["body"]["ordType"], "fok")
        self.assertEqual(captured["body"]["px"], "101.1")
        self.assertEqual(captured["body"]["clOrdId"], "entry-1")
        self.assertEqual(captured["body"]["attachAlgoOrds"][0]["slTriggerPx"], "99")


if __name__ == "__main__":
    unittest.main()
