from __future__ import annotations

import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from okx_quant.area_shape_execution import credential_fingerprint
from okx_quant.area_shape_execution_monitor import AreaShapeDemoExecutionMonitor, _verified_protection
from okx_quant.area_shape_trade import AreaShapeConfig, AreaShapePlan
from okx_quant.engine import StrategyEngine
from okx_quant.models import Candle, Instrument


class _FakeClient:
    def __init__(self, filled_size: Decimal = Decimal("0.4")) -> None:
        self.filled_size = filled_size
        self.placed_ids: list[str] = []
        self.queried_ids: list[str] = []
        self.accepted: set[str] = set()
        self.instrument = Instrument(
            inst_id="BTC-USDT-SWAP", inst_type="SWAP", tick_size=Decimal("0.1"),
            lot_size=Decimal("0.1"), min_size=Decimal("0.1"), state="live",
            settle_ccy="USDT", ct_val=Decimal("1"), ct_mult=Decimal("1"), ct_val_ccy="BTC",
            inst_family="BTC-USDT",
        )

    def get_positions(self, *args, **kwargs):
        return [SimpleNamespace(inst_id="BTC-USDT-SWAP", position=self.filled_size, pos_side="")]

    def get_area_shape_instrument(self, _inst_id):
        return self.instrument

    def get_order_algo(self, credentials, *, environment, inst_id, algo_id=None, algo_cl_ord_id=None):
        client_id = str(algo_cl_ord_id or algo_id or "")
        self.queried_ids.append(client_id)
        if client_id not in self.accepted:
            return None
        return {
            "state": "live", "instId": "BTC-USDT-SWAP", "side": "sell", "sz": str(self.filled_size),
            "reduceOnly": "true", "posSide": "net", "slTriggerPx": "90", "slTriggerPxType": "last",
            "slOrdPx": "-1", "algoId": "algo-fallback",
        }

    def place_area_shape_demo_protection_algo(self, credentials, config, *, client_order_id, **kwargs):
        self.placed_ids.append(client_order_id)
        self.accepted.add(client_order_id)
        raise TimeoutError("fake response timeout after acceptance")


def _fixture(*, filled: Decimal = Decimal("0.4")):
    api_key = "fake-demo-key"
    credentials = SimpleNamespace(api_key=api_key, profile_name="demo-profile")
    runtime = SimpleNamespace(environment="demo", credential_profile_name="demo-profile", credentials=credentials,
                              position_mode="net", trade_mode="cross")
    plan = AreaShapePlan.create(
        profile_name="demo-profile", environment="demo", symbol="BTC-USDT-SWAP",
        support_price=Decimal("95"), pressure_price=Decimal("120"),
        config=AreaShapeConfig(direction="long", exit_template="lock_2r_at_3r"),
    )
    plan.status = "monitoring"
    client = _FakeClient(filled)
    monitor = AreaShapeDemoExecutionMonitor(client_factory=lambda: client, store_load=lambda: [], store_save=lambda _rows: None)
    session = {
        "session_id": "fake-session", "plan_id": plan.plan_id, "plan_snapshot": plan.to_dict(),
        "profile_name": "demo-profile", "environment": "demo", "api_key_sha256": credential_fingerprint(api_key),
        "symbol": "BTC-USDT-SWAP", "direction": "long", "fee_rate": "0", "slippage_rate": "0",
        "instrument": {"tick_size": "0.1", "ct_val": "1", "ct_mult": "1"}, "rule_version": 1,
        "status": "entry_intent", "events": [],
        "pending_intent": {"signal_key": "signal-1", "cl_ord_id": "entry-id",
                            "stop_algo_cl_ord_id": "entry-attached-stop", "side": "buy",
                            "size": "1", "price": "100", "stop": "90", "state": "prepared"},
    }
    monitor._sessions[session["session_id"]] = session
    monitor._runtimes[session["session_id"]] = runtime
    monitor._clients[session["session_id"]] = client
    order = SimpleNamespace(state="partially_filled", ord_id="entry-order", avg_price=Decimal("100"),
                             filled_size=filled, raw={"fillTime": 1_800_000_000_000})
    return monitor, session, client, order


class AreaShapeExecutionMonitorTests(unittest.TestCase):
    def test_arm_posts_gtc_at_entry_line_and_pause_confirms_cancel(self):
        class GtcClient:
            def __init__(self):
                self.instrument = Instrument(
                    inst_id="BTC-USDT-SWAP", inst_type="SWAP", tick_size=Decimal("0.1"),
                    lot_size=Decimal("0.1"), min_size=Decimal("0.1"), state="live",
                    settle_ccy="USDT", ct_val=Decimal("1"), ct_mult=Decimal("1"), ct_val_ccy="BTC",
                    inst_family="BTC-USDT",
                )
                self.orders = {}
                self.posts = []
                self.cancels = []
                self.candles = [
                    Candle(1_800_000_000_000, Decimal("100"), Decimal("102"), Decimal("98"), Decimal("100"), Decimal("1"), True),
                    Candle(1_800_003_600_000, Decimal("100"), Decimal("103"), Decimal("99"), Decimal("101"), Decimal("1"), True),
                ]

            def get_account_config(self, credentials, *, environment):
                return SimpleNamespace(raw={"uid": "uid-demo", "posMode": "net_mode"})

            def get_area_shape_instrument(self, _inst_id):
                return self.instrument

            def get_area_shape_fee_rate(self, credentials, *, environment, inst_family):
                return Decimal("0")

            def get_positions(self, *args, **kwargs):
                return []

            def get_area_shape_pending_orders(self, *args, **kwargs):
                return []

            def get_candles_history(self, *_args, **_kwargs):
                return self.candles

            def place_area_shape_demo_limit_entry(self, credentials, config, **kwargs):
                self.posts.append(kwargs)
                order = SimpleNamespace(
                    state="live", ord_id="gtc-order", avg_price=Decimal("0"), filled_size=Decimal("0"),
                    raw={"instId": kwargs["inst_id"], "clOrdId": kwargs["client_order_id"],
                         "side": kwargs["side"], "ordType": "limit", "sz": str(kwargs["size"]),
                         "px": str(kwargs["limit_price"])},
                )
                self.orders[kwargs["client_order_id"]] = order

            def get_order(self, credentials, config, *, inst_id, cl_ord_id=None, ord_id=None):
                return self.orders[cl_ord_id or ord_id]

            def cancel_order(self, credentials, config, *, inst_id, cl_ord_id=None, ord_id=None):
                self.cancels.append(cl_ord_id or ord_id)
                order = self.orders[cl_ord_id or ord_id]
                order.state = "canceled"

            def close(self):
                pass

        credentials = SimpleNamespace(api_key="fake-demo-key", profile_name="demo-profile")
        runtime = SimpleNamespace(environment="demo", credential_profile_name="demo-profile", credentials=credentials,
                                  position_mode="net", trade_mode="cross")
        plan = AreaShapePlan.create(
            profile_name="demo-profile", environment="demo", symbol="BTC-USDT-SWAP",
            support_price=Decimal("95"), pressure_price=Decimal("120"),
            config=AreaShapeConfig(atr_period=2, stop_offset_atr=Decimal("0.5"), risk_amount=Decimal("100")),
        )
        plan.status = "monitoring"
        plan.expires_at_candle_ts = 1_800_003_600_000 + 24 * 3_600_000
        client = GtcClient()
        monitor = AreaShapeDemoExecutionMonitor(client_factory=lambda: client, store_load=lambda: [], store_save=lambda _rows: None)

        class PaperMarker:
            def mark_demo_execution(self, _plan_id):
                return plan

        session = monitor.arm_plan(plan=plan, runtime=runtime, unlocked=True, paper_monitor=PaperMarker())
        self.assertEqual(len(client.posts), 1)
        self.assertEqual(client.posts[0]["limit_price"], Decimal("95"))
        self.assertEqual(session["resting_entry_intent"]["price"], "95")
        # If the UI pause arrives while a signal is queued, cancellation must
        # win and the queued signal must not race into an alternate FOK entry.
        queued = monitor._sessions[session["session_id"]]
        queued["status"] = "signal_pending"
        queued["signal_pending"] = {"signal_candle_ts": 1_800_003_600_000, "signal_key": "queued"}
        fok_calls = []
        client.place_area_shape_demo_fok_entry = lambda *_args, **_kwargs: fok_calls.append("sent")
        monitor.pause_session(session["session_id"])
        monitor._tick(session["session_id"], monitor.sessions()[0])
        self.assertEqual(client.cancels, [session["resting_entry_intent"]["cl_ord_id"]])
        self.assertEqual(monitor.sessions()[0]["status"], "paused")
        self.assertEqual(fok_calls, [])

    def test_resting_entry_uses_line_price_and_atr_stop(self):
        monitor, session, _client, _order = _fixture()
        plan = AreaShapePlan.from_dict(session["plan_snapshot"])
        plan.config.atr_period = 2
        plan.config.stop_offset_atr = Decimal("0.5")
        plan.config.risk_amount = Decimal("100")
        session.update({"plan_snapshot": plan.to_dict(), "baseline_candle_ts": 1_800_000_000_000,
                        "expires_at_candle_ts": 1_800_000_000_000 + 20 * 3_600_000,
                        "instrument": {"tick_size": "0.1", "ct_val": "1", "ct_mult": "1",
                                       "lot_size": "0.1", "min_size": "0.1"}})
        candles = [
            Candle(1_800_000_000_000, Decimal("100"), Decimal("102"), Decimal("98"), Decimal("100"), Decimal("1"), True),
            Candle(1_800_003_600_000, Decimal("100"), Decimal("103"), Decimal("99"), Decimal("101"), Decimal("1"), True),
        ]
        intent, reason = monitor._prepare_resting_entry(session, plan, candles)
        self.assertFalse(reason)
        self.assertEqual(intent["price"], "95")
        self.assertLess(Decimal(intent["stop"]), Decimal(intent["price"]))
        self.assertEqual(intent["entry_kind"], "gtc")

    def test_pause_race_without_gtc_intent_finishes_terminal_state(self):
        monitor, session, _client, _order = _fixture()
        session.update({"status": "armed", "paused": True, "cancel_requested": "cancel",
                        "resting_entry_intent": None})
        monitor._tick(session["session_id"], dict(session))
        self.assertEqual(session["status"], "cancelled")
        self.assertNotIn("cancel_requested", session)

    def test_resume_reconciles_old_gtc_before_preparing_new_epoch(self):
        monitor, session, client, _order = _fixture()
        calls = []
        credentials = monitor._runtimes[session["session_id"]].credentials
        monitor._sessions[session["session_id"]].update({
            "status": "paused", "paused": True, "account_uid": "demo-uid",
            "resting_entry_intent": {
                "entry_kind": "gtc", "cl_ord_id": "old-gtc", "side": "buy", "size": "1",
                "price": "95", "stop": "90", "stop_algo_cl_ord_id": "old-stop",
            },
        })
        session = monitor._sessions[session["session_id"]]

        def get_account_config(_credentials, *, environment):
            return SimpleNamespace(raw={"uid": "demo-uid"})

        def get_candles_history(*_args, **_kwargs):
            return [
                Candle(1_800_000_000_000, Decimal("100"), Decimal("102"), Decimal("98"), Decimal("100"), Decimal("1"), True),
                Candle(1_800_003_600_000, Decimal("100"), Decimal("103"), Decimal("99"), Decimal("101"), Decimal("1"), True),
            ]

        order = SimpleNamespace(
            state="live", ord_id="old-order", avg_price=Decimal("0"), filled_size=Decimal("0"),
            raw={"instId": "BTC-USDT-SWAP", "clOrdId": "old-gtc", "side": "buy",
                 "ordType": "limit", "sz": "1", "px": "95"},
        )

        def get_order(_credentials, _config, *, inst_id, cl_ord_id=None, ord_id=None):
            calls.append(("get", cl_ord_id))
            return order

        def cancel_order(_credentials, _config, *, inst_id, cl_ord_id=None, ord_id=None):
            calls.append(("cancel", cl_ord_id))
            order.state = "canceled"

        client.get_account_config = get_account_config
        client.get_candles_history = get_candles_history
        client.get_order = get_order
        client.cancel_order = cancel_order
        monitor._sessions[session["session_id"]]["api_key_sha256"] = credential_fingerprint(credentials.api_key)
        monitor._sessions[session["session_id"]]["profile_name"] = "demo-profile"
        result = monitor.resume_session(session["session_id"], runtime=monitor._runtimes[session["session_id"]], unlocked=True)
        self.assertEqual(calls, [("get", "old-gtc"), ("cancel", "old-gtc"), ("get", "old-gtc")])
        self.assertEqual(result["status"], "armed")
        self.assertIsNone(result["resting_entry_intent"])

    def test_resting_entry_includes_target_buffer_in_minimum_space(self):
        monitor, session, _client, _order = _fixture()
        plan = AreaShapePlan.from_dict(session["plan_snapshot"])
        plan.config.atr_period = 2
        plan.config.stop_offset_atr = Decimal("0.5")
        plan.config.target_buffer_atr = Decimal("0.5")
        plan.config.risk_amount = Decimal("100")
        plan.pressure_price = Decimal("100.9")
        session.update({"plan_snapshot": plan.to_dict(), "baseline_candle_ts": 1_800_000_000_000,
                        "expires_at_candle_ts": 1_800_000_000_000 + 20 * 3_600_000,
                        "instrument": {"tick_size": "0.1", "ct_val": "1", "ct_mult": "1",
                                       "lot_size": "0.1", "min_size": "0.1"}})
        candles = [
            Candle(1_800_000_000_000, Decimal("100"), Decimal("102"), Decimal("98"), Decimal("100"), Decimal("1"), True),
            Candle(1_800_003_600_000, Decimal("100"), Decimal("103"), Decimal("99"), Decimal("101"), Decimal("1"), True),
        ]
        intent, reason = monitor._prepare_resting_entry(session, plan, candles)
        self.assertIsNone(intent)
        self.assertIn("最低2R", reason)

    def test_partial_gtc_fill_cancels_remainder_and_never_submits_fok(self):
        monitor, session, client, _order = _fixture(filled=Decimal("0.4"))
        session["status"] = "signal_pending"
        session["pending_intent"] = None
        session["signal_pending"] = {"signal_candle_ts": 1_800_000_000_000, "signal_key": "signal-1", "stop": "90"}
        session["resting_entry_intent"] = {
            "entry_kind": "gtc", "signal_key": "resting-1", "cl_ord_id": "gtc-entry",
            "stop_algo_cl_ord_id": "gtc-stop", "side": "buy", "size": "1", "price": "95", "stop": "90",
        }
        client.accepted.add("gtc-stop")
        calls = {"cancel": [], "fok": []}
        state = {"name": "partially_filled"}
        order = SimpleNamespace(
            state="partially_filled", ord_id="gtc-order", avg_price=Decimal("95"), filled_size=Decimal("0.4"),
            raw={"fillTime": 1_800_000_000_000, "instId": "BTC-USDT-SWAP", "clOrdId": "gtc-entry",
                 "side": "buy", "ordType": "limit", "sz": "1", "px": "95"},
        )

        def get_order(*_args, **_kwargs):
            return SimpleNamespace(**{**order.__dict__, "state": state["name"]})

        def cancel_order(*_args, **_kwargs):
            calls["cancel"].append("gtc-entry")
            state["name"] = "canceled"

        client.get_order = get_order
        client.cancel_order = cancel_order
        client.place_area_shape_demo_fok_entry = lambda *_args, **_kwargs: calls["fok"].append("sent")
        monitor._try_live_entry(session["session_id"])
        self.assertEqual(calls["cancel"], ["gtc-entry"])
        self.assertEqual(calls["fok"], [])
        self.assertEqual(session["position"]["size"], "0.4")
        self.assertEqual(session["status"], "manual_control")
        self.assertIsNone(session["resting_entry_intent"])

    def test_ambiguous_fallback_timeout_recovers_same_id_and_keeps_partial_manual(self):
        monitor, session, client, order = _fixture()
        with self.assertRaises(TimeoutError):
            monitor._protect_fill(session["session_id"], order, 1_800_000_000_000)
        fallback_id = client.placed_ids[0]
        self.assertTrue(session["position"]["partial_fill_requires_review"])
        queried_before = len(client.queried_ids)

        monitor._tick(session["session_id"], dict(session))

        self.assertIn(fallback_id, client.queried_ids[queried_before:])
        self.assertEqual(session["position"]["stop_algo_cl_ord_id"], fallback_id)
        self.assertEqual(session["position"]["size"], "0.4")
        self.assertEqual(session["status"], "manual_control")
        self.assertEqual(client.placed_ids, [fallback_id])

    def test_protection_verification_requires_live_last_market_reduceonly(self):
        base = {"state": "live", "instId": "BTC-USDT-SWAP", "side": "sell", "sz": "1",
                "reduceOnly": "true", "posSide": "net", "slTriggerPx": "90",
                "slTriggerPxType": "last", "slOrdPx": "-1"}
        kwargs = dict(inst_id="BTC-USDT-SWAP", side="sell", size=Decimal("1"), trigger=Decimal("90"),
                      kind="sl", pos_side=None, position_mode="net")
        self.assertTrue(_verified_protection(base, **kwargs))
        for field, value in (("state", "effective"), ("slTriggerPxType", "mark"), ("slOrdPx", "89")):
            changed = dict(base, **{field: value})
            self.assertFalse(_verified_protection(changed, **kwargs))

    def test_sessions_returns_deepcopy_and_pending_entry_cannot_be_cancelled(self):
        monitor, session, _client, _order = _fixture()
        session["status"] = "needs_manual_rearm"
        session["position"] = None
        snapshot = monitor.sessions()[0]
        snapshot["plan_snapshot"]["config"]["direction"] = "short"
        self.assertEqual(session["plan_snapshot"]["config"]["direction"], "long")
        with self.assertRaises(ValueError):
            monitor.cancel_session(session["session_id"])

    def test_pause_cancels_exchange_trigger_and_waits_for_terminal_confirmation(self):
        monitor, session, client, _order = _fixture()
        algo_state = {"state": "live", "ordIdList": [], "algoClOrdId": "entry-id"}
        cancel_calls = []

        def get_algo(credentials, *, environment, inst_id, algo_id=None, algo_cl_ord_id=None):
            return dict(algo_state)

        def cancel_algo(credentials, *, environment, inst_id, algo_id=None, algo_cl_ord_id=None):
            cancel_calls.append(algo_cl_ord_id)
            algo_state["state"] = "canceled"
            return SimpleNamespace()

        client.get_order_algo = get_algo
        client.cancel_algo_order = cancel_algo
        monitor.pause_session(session["session_id"])
        self.assertEqual(session["status"], "entry_intent")
        self.assertEqual(session["cancel_requested"], "pause")

        monitor._reconcile_entry_intent(session["session_id"])

        self.assertEqual(cancel_calls, ["entry-id"])
        self.assertEqual(session["status"], "paused")
        self.assertIsNone(session["pending_intent"])

    def test_arm_signal_live_fok_fill_protect_manage_and_confirm_close(self):
        """Offline chain: arm baseline -> new confirmed signal -> demo FOK -> protection -> verified close."""
        class EndToEndClient(_FakeClient):
            def __init__(self, direction: str, template: str, now_ms: int, signal_ts: int) -> None:
                super().__init__(Decimal("0"))
                self.direction, self.template = direction, template
                self.now_ms, self.signal_ts = now_ms, signal_ts
                self.candles = [
                    Candle(signal_ts - 2 * 3_600_000, Decimal("104"), Decimal("105"), Decimal("102"), Decimal("103"), Decimal("1"), True),
                    Candle(signal_ts - 3_600_000, Decimal("101"), Decimal("102"), Decimal("99"), Decimal("100"), Decimal("1"), True),
                ]
                self.orders: dict[str, object] = {}
                self.algos: dict[str, dict] = {}
                self.order_posts: list[str] = []
                self.positions_open = False
                self.position_size = Decimal("0")
                self.instrument = Instrument(
                    inst_id="BTC-USDT-SWAP", inst_type="SWAP", tick_size=Decimal("0.1"),
                    lot_size=Decimal("0.1"), min_size=Decimal("0.1"), state="live",
                    settle_ccy="USDT", ct_val=Decimal("0.01"), ct_mult=Decimal("1"), ct_val_ccy="BTC",
                    inst_family="BTC-USDT",
                )

            def get_account_config(self, credentials, *, environment):
                return SimpleNamespace(raw={"uid": "uid-demo", "posMode": "net_mode"})

            def get_area_shape_fee_rate(self, credentials, *, environment, inst_family):
                return Decimal("0.0005")

            def get_area_shape_pending_orders(self, credentials, *, environment, inst_id):
                return []

            def get_candles_history(self, inst_id, bar, *, limit):
                return list(self.candles)[-limit:]

            def get_positions(self, *args, **kwargs):
                if not self.positions_open:
                    return []
                signed = self.position_size if self.direction == "long" else -self.position_size
                return [SimpleNamespace(inst_id="BTC-USDT-SWAP", position=signed, pos_side="")]

            def get_trigger_price(self, *args, **kwargs):
                if self.positions_open:
                    return Decimal("140") if self.direction == "long" else Decimal("60")
                return Decimal("97") if self.direction == "long" else Decimal("104")

            def get_order_book(self, inst_id, depth=1):
                raw = {"ts": str(self.now_ms)}
                bid, ask = ((Decimal("96.9"), Decimal("10")), (Decimal("97.1"), Decimal("10"))) if self.direction == "long" else ((Decimal("103.9"), Decimal("10")), (Decimal("104.1"), Decimal("10")))
                return SimpleNamespace(inst_id=inst_id, bids=(bid,), asks=(ask,), raw=raw)

            def place_area_shape_demo_fok_entry(self, credentials, config, *, inst_id, side, size,
                                                 limit_price, stop_price, client_order_id, stop_algo_client_order_id):
                self.order_posts.append(client_order_id)
                self.position_size = size
                self.positions_open = True
                status = SimpleNamespace(state="filled", ord_id="entry-child", avg_price=Decimal("97.1") if side == "buy" else Decimal("103.9"),
                                         filled_size=size, raw={"fillTime": self.now_ms, "instId": inst_id,
                                                               "clOrdId": client_order_id, "side": side,
                                                               "ordType": "fok", "sz": str(size)})
                self.orders["entry-child"] = status
                self.orders[client_order_id] = status
                self.algos[stop_algo_client_order_id] = {
                    "state": "live", "instId": inst_id, "side": "sell" if side == "buy" else "buy",
                    "sz": str(size), "reduceOnly": "true", "posSide": "net", "algoId": "sl-algo",
                    "slTriggerPx": str(stop_price), "slTriggerPxType": "last", "slOrdPx": "-1",
                }
                raise TimeoutError("fake accepted FOK but response timed out")

            def get_order(self, credentials, config, *, inst_id, cl_ord_id=None, ord_id=None):
                return self.orders[ord_id or cl_ord_id]

            def get_order_algo(self, credentials, *, environment, inst_id, algo_id=None, algo_cl_ord_id=None):
                key = str(algo_cl_ord_id or algo_id or "")
                self.queried_ids.append(key)
                return self.algos.get(key)

            def place_area_shape_demo_protection_algo(self, credentials, config, *, inst_id, side, size,
                                                       pos_side, trigger_price, client_order_id, protection_kind):
                field = "slTriggerPx" if protection_kind == "sl" else "tpTriggerPx"
                type_field = "slTriggerPxType" if protection_kind == "sl" else "tpTriggerPxType"
                ord_field = "slOrdPx" if protection_kind == "sl" else "tpOrdPx"
                self.algos[client_order_id] = {
                    "state": "live", "instId": inst_id, "side": side, "sz": str(size),
                    "reduceOnly": "true", "posSide": "net", "algoId": f"{protection_kind}-algo",
                    field: str(trigger_price), type_field: "last", ord_field: "-1",
                }
                return SimpleNamespace(ord_id="", cl_ord_id=client_order_id)

            def amend_algo_order(self, credentials, *, environment, inst_id, algo_id=None, algo_cl_ord_id=None,
                                 new_stop_loss_trigger_price=None, **kwargs):
                key = str(algo_cl_ord_id or "")
                self.algos[key]["slTriggerPx"] = str(new_stop_loss_trigger_price)
                return SimpleNamespace()

            def get_fills_history(self, credentials, *, environment, inst_types, limit):
                if not self.algos:
                    return []
                terminal_id = next((key for key, value in self.algos.items() if value.get("state") == "effective"), "")
                if not terminal_id:
                    return []
                return [SimpleNamespace(order_id="close-child", inst_id="BTC-USDT-SWAP",
                                        side="sell" if self.direction == "long" else "buy",
                                        pos_side="", fill_time=self.now_ms + 1, fill_size=self.position_size,
                                        fill_price=Decimal("110" if self.direction == "long" else "90"), fill_fee=Decimal("0.1"))]

        hour = 3_600_000
        now_ms = (int(__import__("time").time() * 1000) // hour) * hour + 120_000
        signal_ts = now_ms - 120_000 - hour
        for direction, template in (("long", "fixed_1r"), ("short", "lock_2r_at_3r")):
            with self.subTest(direction=direction, template=template), patch(
                "okx_quant.area_shape_execution_monitor.time.time", return_value=now_ms / 1000
            ):
                credentials = SimpleNamespace(api_key="demo-key", profile_name="demo-profile")
                runtime = SimpleNamespace(environment="demo", credential_profile_name="demo-profile",
                                          credentials=credentials, position_mode="net", trade_mode="cross")
                plan = AreaShapePlan.create(
                    profile_name="demo-profile", environment="demo", symbol="BTC-USDT-SWAP",
                    support_price=Decimal("95") if direction == "long" else Decimal("70"),
                    pressure_price=Decimal("130") if direction == "long" else Decimal("110"),
                    config=AreaShapeConfig(direction=direction, exit_template=template,
                                           atr_period=2, area_half_width_atr=Decimal("0.25"),
                                           stop_offset_atr=Decimal("0"),
                                           resting_entry_enabled=False,
                                           round_trip_fee_rate=Decimal("0.0005"), slippage_rate=Decimal("0.001")),
                )
                plan.status = "monitoring"
                plan.expires_at_candle_ts = signal_ts + 240 * hour
                client = EndToEndClient(direction, template, now_ms, signal_ts)
                monitor = AreaShapeDemoExecutionMonitor(client_factory=lambda: client, store_load=lambda: [], store_save=lambda _rows: None)
                class PaperMarker:
                    def mark_demo_execution(self, _plan_id):
                        return plan

                armed = monitor.arm_plan(plan=plan, runtime=runtime, unlocked=True, paper_monitor=PaperMarker())
                if direction == "long":
                    # Exercise the real ATR/shape/area signal path, not a synthetic event.
                    client.candles.append(Candle(signal_ts, Decimal("96"), Decimal("97.2"), Decimal("93"), Decimal("97"), Decimal("1"), True))
                    monitor._check_new_signals(armed["session_id"])
                else:
                    with patch("okx_quant.area_shape_execution_monitor.AreaShapePlan.observe", autospec=True) as observe:
                        def signal_event(self_plan, candles):
                            self_plan.last_candle_ts = signal_ts
                            return {"kind": "paper_pending", "candle_ts": signal_ts,
                                    "paper_position": {"signal_key": f"sig-{direction}", "signal_candle_ts": signal_ts,
                                                       "stop": "110", "entry_trigger": "101", "target_buffer_atr": "0"}}
                        observe.side_effect = signal_event
                        client.candles.append(Candle(signal_ts, Decimal("104"), Decimal("105"), Decimal("102"), Decimal("103"), Decimal("1"), True))
                        monitor._check_new_signals(armed["session_id"])
                monitor._tick(armed["session_id"], monitor.sessions()[0])
                current = monitor.sessions()[0]
                self.assertEqual(len(client.order_posts), 1,
                                 f"local touch+shape FOK must reconcile using its original clOrdId; session={current}")
                self.assertEqual(current["status"], "protected", current.get("events"))
                self.assertTrue(current["position"]["stop_verified_at_ms"])
                if template != "fixed_1r":
                    minute = (now_ms // 60_000) * 60_000 - 60_000
                    favorable = Decimal("140") if direction == "long" else Decimal("60")
                    with patch.object(StrategyEngine, "_scan_dynamic_stop_minute_candles", return_value=(favorable, minute)):
                        monitor._manage_filled_position(armed["session_id"])
                    current = monitor.sessions()[0]
                    self.assertEqual(current["position"]["stage"], "locked")
                # A terminal algo alone is insufficient; the child order's matching fill is required.
                exit_id = current["position"].get("take_profit_algo_cl_ord_id") or current["position"]["stop_algo_cl_ord_id"]
                client.algos[exit_id]["state"] = "effective"
                client.algos[exit_id]["ordId"] = "close-child"
                client.positions_open = False
                monitor._manage_filled_position(armed["session_id"])
                self.assertEqual(monitor.sessions()[0]["status"], "closed")

    def test_baseline_and_unconfirmed_candles_do_not_emit_execution_signals(self):
        monitor, session, _client, _order = _fixture()
        plan = AreaShapePlan.from_dict(session["plan_snapshot"])
        baseline = 1_800_000_000_000
        session.update({"status": "armed", "signal_cursor_ts": baseline, "baseline_candle_ts": baseline,
                        "expires_at_candle_ts": baseline + 24 * 3_600_000})
        class CandleClient:
            def get_candles_history(self, *_args, **_kwargs):
                return [SimpleNamespace(ts=baseline, confirmed=True),
                        SimpleNamespace(ts=baseline + 3_600_000, confirmed=False)]
        monitor._clients[session["session_id"]] = CandleClient()
        monitor._sessions[session["session_id"]] = session
        with patch("okx_quant.area_shape_execution_monitor.AreaShapePlan.observe", autospec=True) as observe:
            monitor._check_new_signals(session["session_id"])
        observe.assert_not_called()
        self.assertEqual(session["status"], "armed")

    def test_native_trigger_is_not_placed_for_a_stale_confirmed_signal(self):
        api_key = "fake-demo-key"
        credentials = SimpleNamespace(api_key=api_key, profile_name="demo-profile")
        runtime = SimpleNamespace(environment="demo", credential_profile_name="demo-profile", credentials=credentials,
                                  position_mode="net", trade_mode="cross")
        plan = AreaShapePlan.create(profile_name="demo-profile", environment="demo", symbol="BTC-USDT-SWAP",
                                    support_price=Decimal("90"), pressure_price=Decimal("130"),
                                    config=AreaShapeConfig(entry_mode="breakout"))
        plan.status = "monitoring"
        close_ts = 1_800_000_000_000
        session_id = "breakout-session"
        session = {"session_id": session_id, "plan_id": plan.plan_id, "plan_snapshot": plan.to_dict(),
                   "profile_name": "demo-profile", "environment": "demo", "api_key_sha256": credential_fingerprint(api_key),
                   "symbol": "BTC-USDT-SWAP", "status": "signal_pending", "baseline_candle_ts": close_ts - 3_600_000,
                   "expires_at_candle_ts": close_ts + 24 * 3_600_000, "signal_cursor_ts": close_ts,
                   "signal_pending": {"signal_candle_ts": close_ts, "signal_key": "breakout", "entry_trigger": "101"},
                   "events": []}
        class BreakoutClient:
            def get_candles_history(self, *_args, **_kwargs):
                return [SimpleNamespace(ts=close_ts, confirmed=True)]
            def get_trigger_price(self, *_args, **_kwargs):
                return Decimal("100")
        monitor = AreaShapeDemoExecutionMonitor(store_load=lambda: [], store_save=lambda _rows: None)
        monitor._sessions[session_id] = session
        monitor._runtimes[session_id] = runtime
        monitor._clients[session_id] = BreakoutClient()
        with patch("okx_quant.area_shape_execution_monitor.time.time", return_value=(close_ts + 90 * 60_000) / 1000):
            monitor._try_live_entry(session_id)
        self.assertEqual(session["status"], "armed")
        self.assertIsNone(session.get("pending_intent"))


if __name__ == "__main__":
    unittest.main()
