from dataclasses import replace
from decimal import Decimal as D
from types import SimpleNamespace as NS
import time
from unittest.mock import Mock, patch

import pytest

from okx_quant.models import Credentials, Instrument
from okx_quant.okx_client import OkxOrderStatus, OkxOrderBook, OkxRestClient
from okx_quant.pending_order_chase import (
    PendingOrderChaseService, attached_stop, chase_unavailable_reason, classify_order, remaining_risk_size,
)


class Client:
    def __init__(self, side="buy", close=False, pos_side="net"):
        self.instrument = Instrument("BTC-USDT-SWAP", "SWAP", D("0.1"), D("0.1"), D("0.1"), "live",
            settle_ccy="USDT", ct_val=D(1), ct_mult=D(1), ct_val_ccy="BTC", inst_family="BTC-USDT")
        self.status = OkxOrderStatus("123", "live", side, "limit", D(100), None, D(10), D(0),
            dict(instId="BTC-USDT-SWAP", side=side, posSide=pos_side, ordType="limit", clOrdId="entry1",
                 reduceOnly=close, slTriggerPx="90" if side == "buy" else "110"))
        self.uid, self.position_mode = "uid", "net_mode" if pos_side == "net" else "long_short_mode"
        self.positions = []
        self.fee = D(0)
        self.bids, self.asks = D(102), D(103)
        self.stale = False
        self.amendments = []
        self.accept, self.timeout, self.read_fail = True, False, False
        self._enforce_order_size_safety_limit = Mock()

    def row(self):
        return NS(inst_id="BTC-USDT-SWAP", order_id="123", side=self.status.side, pos_side=self.status.raw["posSide"],
                  ord_type="limit", state="live", client_order_id="entry1")

    def get_order(self, *_args, **_kwargs):
        if self.read_fail and self.amendments:
            raise TimeoutError("read timeout")
        return self.status

    def get_positions(self, *_args, **_kwargs):
        return self.positions

    def get_account_config(self, *_args, **_kwargs):
        return NS(raw=dict(uid=self.uid, posMode=self.position_mode))

    def get_instrument(self, *_args, **_kwargs):
        return self.instrument

    def get_area_shape_fee_rate(self, *_args, **_kwargs):
        return self.fee

    def get_order_book(self, *_args, **_kwargs):
        return OkxOrderBook("BTC-USDT-SWAP", ((self.bids, D(100)),), ((self.asks, D(100)),),
                            dict(ts=int(time.time()*1000)-(6000 if self.stale else 0)))

    def amend_order(self, *_args, **kwargs):
        self.amendments.append(kwargs)
        if self.accept:
            self.status = replace(self.status, price=kwargs["new_price"], size=kwargs.get("new_size", self.status.size))
        if self.timeout:
            raise TimeoutError("post timeout")


@pytest.fixture
def setup(tmp_path):
    runtime = NS(credentials=Credentials("test-key", "secret", "pass"), environment="demo", credential_profile_name="test")
    client = Client()
    service = PendingOrderChaseService(tmp_path/"chase.json")
    return service, client, runtime


def start(setup, context=None):
    service, client, runtime = setup
    key = service.start(client=client, runtime=runtime, order=client.row(), context=context)
    return key


@pytest.mark.parametrize("side,price,qty", [("buy", "102", "8.3"), ("sell", "103", "10")])
def test_open_follows_own_side_and_freezes_original_budget(setup, side, price, qty):
    service, _, runtime = setup
    client = Client(side)
    key = service.start(client=client, runtime=runtime, order=client.row())
    task = service.step(client=client, runtime=runtime, key=key)
    assert task["active"] and task["pending"] is None
    assert client.amendments[0]["new_price"] == D(price)
    assert client.amendments[0]["new_size"] == D(qty)
    assert task["budget"] == "100"
    client._enforce_order_size_safety_limit.assert_called_once()
    service.step(client=client, runtime=runtime, key=key)
    assert len(client.amendments) == (1 if side == "buy" else 2)
    if side == "sell":
        assert client.amendments[1]["new_size"] == D("14.2")


@pytest.mark.parametrize("side,pos_side,close", [("buy", "net", True), ("sell", "net", True),
    ("sell", "long", False), ("buy", "short", False)])
def test_close_amends_price_only_never_quantity(setup, side, pos_side, close):
    service, _, runtime = setup
    client = Client(side, close, pos_side)
    client.status.raw.pop("slTriggerPx")
    key = service.start(client=client, runtime=runtime, order=client.row())
    service.step(client=client, runtime=runtime, key=key)
    assert "new_size" not in client.amendments[0]
    assert client.status.size == 10
    client._enforce_order_size_safety_limit.assert_not_called()


def test_partial_fill_uses_average_and_total_quantity_includes_fills(setup):
    service, client, runtime = setup
    key = start(setup, {"risk_amount": 100})
    client.status = replace(client.status, state="partially_filled", filled_size=D(2), avg_price=D(101))
    service.step(client=client, runtime=runtime, key=key)
    # Filled risk 2*(101-90)=22; remaining 78/(102-90)=6.5. OKX expects total 8.5.
    assert client.amendments[0]["new_size"] == D("8.5")


def test_partial_managed_order_hands_off_without_amend(setup):
    service, client, runtime = setup
    key = start(setup, {"owner": "area", "stop_on_fill": True})
    client.status = replace(client.status, state="partially_filled", filled_size=D(2), avg_price=D(101))
    task = service.step(client=client, runtime=runtime, key=key)
    assert not task["active"] and "交回原策略" in task["message"]
    assert not client.amendments


@pytest.mark.parametrize("mutation,message", [
    (lambda c: c.status.raw.pop("slTriggerPx"), "缺少"),
    (lambda c: setattr(c, "uid", ""), "UID"),
    (lambda c: setattr(c, "positions", [NS(inst_id="BTC-USDT-SWAP", position=D(-1))]), "反向"),
    (lambda c: c.status.raw.pop("reduceOnly"), "只减仓"),
    (lambda c: setattr(c, "fee", D(-1)), "手续费"),
    (lambda c: setattr(c, "status", replace(c.status, filled_size=D(11))), "数量"),
    (lambda c: setattr(c, "instrument", replace(c.instrument, ct_val_ccy="USDT")), "基础币"),
])
def test_start_rejects_uncertain_or_invalid_risk_identity(setup, mutation, message):
    service, client, runtime = setup
    mutation(client)
    with pytest.raises(ValueError, match=message):
        start(setup)
    assert not client.amendments


@pytest.mark.parametrize("kind", ["oco", "conditional", "trigger", "move_order_stop", "market", "fok"])
def test_protective_algorithms_and_market_orders_are_not_chased(setup, kind):
    service, client, runtime = setup
    row = client.row()
    row.ord_type = kind
    assert chase_unavailable_reason(row)
    with pytest.raises(ValueError):
        service.start(client=client, runtime=runtime, order=row)
    assert not client.amendments


@pytest.mark.parametrize("mutation,message", [
    (lambda c: setattr(c, "stale", True), "盘口"),
    (lambda c: setattr(c, "bids", D(105)), "盘口"),
    (lambda c: setattr(c, "bids", D(89)), "越过止损"),
    (lambda c: c.status.raw.update(slTriggerPx="91"), "止损已变化"),
    (lambda c: setattr(c, "status", replace(c.status, price=D(99))), "其他来源"),
    (lambda c: setattr(c, "bids", D(1020)), "盘口"),
])
def test_step_does_not_amend_on_unsafe_change(setup, mutation, message):
    service, client, runtime = setup
    key = start(setup)
    mutation(client)
    with pytest.raises(ValueError, match=message):
        service.step(client=client, runtime=runtime, key=key)
    assert not client.amendments


def test_original_region_space_must_still_fit_after_reprice(setup):
    service, client, runtime = setup
    key = start(setup, {"target": 120, "min_space_r": 2})
    with pytest.raises(ValueError, match="盈亏比"):
        service.step(client=client, runtime=runtime, key=key)
    assert not client.amendments


def test_unmanaged_open_cannot_chase_beyond_attached_take_profit(setup):
    service, client, runtime = setup
    client.status.raw["tpTriggerPx"] = "101"
    key = start(setup)
    with pytest.raises(ValueError, match="止盈"):
        service.step(client=client, runtime=runtime, key=key)
    assert not client.amendments


def test_manual_take_profit_change_pauses_chase(setup):
    service, client, runtime = setup
    client.status.raw["tpTriggerPx"] = "120"
    key = start(setup)
    client.status.raw["tpTriggerPx"] = "119"
    with pytest.raises(ValueError, match="止盈已变化"):
        service.step(client=client, runtime=runtime, key=key)
    assert not client.amendments


@pytest.mark.parametrize("accept", [True, False])
def test_ambiguous_post_never_blindly_resends(setup, accept):
    service, client, runtime = setup
    key = start(setup)
    client.accept, client.timeout = accept, True
    task = service.step(client=client, runtime=runtime, key=key)
    assert bool(task["pending"]) == (not accept)
    if not accept:
        assert not task["active"]
        with pytest.raises(ValueError, match="尚未核实"):
            start(setup)
    service.step(client=client, runtime=runtime, key=key)
    assert len(client.amendments) == 1


def test_readback_failure_pauses_with_pending_authorization(setup):
    service, client, runtime = setup
    key = start(setup)
    client.read_fail = True
    task = service.step(client=client, runtime=runtime, key=key)
    assert not task["active"] and task["pending"]
    client.read_fail = False
    task = service.step(client=client, runtime=runtime, key=key)
    assert not task["active"] and not task["pending"]
    assert len(client.amendments) == 1


def test_restart_pauses_and_keeps_precise_authorizations_without_secrets(setup):
    service, client, runtime = setup
    key = start(setup, {"owner": "area"})
    service.step(client=client, runtime=runtime, key=key)
    loaded = PendingOrderChaseService(service.path)
    assert not loaded.snapshot(key)["active"]
    assert loaded.authorized_update(runtime=runtime, inst_id="BTC-USDT-SWAP", status=client.status,
        old_price=100, old_size=10, stop=90, account_uid="uid")
    for changed in ({"stop": 91}, {"account_uid": "other"}, {"old_price": 99}):
        args = dict(runtime=runtime, inst_id="BTC-USDT-SWAP", status=client.status,
            old_price=100, old_size=10, stop=90, account_uid="uid")
        args.update(changed)
        assert loaded.authorized_update(**args) is None
    assert "test-key" not in service.path.read_text(encoding="utf-8")
    assert "secret" not in service.path.read_text(encoding="utf-8")


def test_persistence_failure_prevents_order_write(setup):
    service, client, runtime = setup
    key = start(setup)
    with patch("okx_quant.pending_order_chase._write_json_atomic", side_effect=OSError("disk full")):
        with pytest.raises(RuntimeError, match="保存失败"):
            service.step(client=client, runtime=runtime, key=key)
    assert not client.amendments


def test_stop_and_terminal_status_never_amend(setup):
    service, client, runtime = setup
    key = start(setup)
    service.stop(key)
    service.step(client=client, runtime=runtime, key=key)
    assert not client.amendments
    key = start(setup)
    client.status = replace(client.status, state="filled", filled_size=D(10), avg_price=D(100))
    assert not service.step(client=client, runtime=runtime, key=key)["active"]
    assert not client.amendments


def test_identity_binding_rejects_other_environment(setup):
    service, client, runtime = setup
    key = start(setup)
    with pytest.raises(ValueError, match="环境"):
        service.step(client=client, runtime=NS(credentials=runtime.credentials, environment="live"), key=key)
    assert not client.amendments


def test_size_costs_and_precision():
    qty = remaining_risk_size(budget=D(100), stop=D(90), price=D(102), filled=D(2), average=D(101),
                             multiplier=D(1), cost_rate=D("0.001"), lot=D("0.1"))
    assert qty == D("8.3")
    assert qty > 2


@pytest.mark.parametrize("side,pos_side", [("buy", "long"), ("sell", "short")])
def test_hedge_opening_side_still_uses_stop_budget(setup, side, pos_side):
    service, _, runtime = setup
    client = Client(side, pos_side=pos_side)
    key = service.start(client=client, runtime=runtime, order=client.row())
    task = service.step(client=client, runtime=runtime, key=key)
    assert task["mode"] == "open" and "new_size" in client.amendments[0]


def test_better_quote_does_not_increase_risk_of_racing_old_price_fills(setup):
    service, client, runtime = setup
    client.bids, client.asks = D(95), D(96)
    key = start(setup)
    service.step(client=client, runtime=runtime, key=key)
    assert client.amendments[0]["new_size"] == 10
    client.status = replace(client.status, state="partially_filled", filled_size=D(2), avg_price=D(100))
    service.step(client=client, runtime=runtime, key=key)
    assert client.amendments[1]["new_size"] == 18  # Filled 20 risk; 80/5=16 remaining.


def test_minimum_quantity_and_existing_exposure_limit_are_respected(setup):
    service, client, runtime = setup
    key = start(setup, {"risk_amount": "0.01"})
    with pytest.raises(ValueError, match="最小下单量"):
        service.step(client=client, runtime=runtime, key=key)
    assert not client.amendments
    service.stop(key)
    key = start(setup, {"risk_amount": 100})
    client._enforce_order_size_safety_limit.side_effect = ValueError("exposure limit")
    with pytest.raises(ValueError, match="exposure limit"):
        service.step(client=client, runtime=runtime, key=key)
    assert not client.amendments


def test_failed_amend_ack_is_not_mistaken_for_effective_change(setup):
    service, client, runtime = setup
    key = start(setup)
    client.accept = False
    task = service.step(client=client, runtime=runtime, key=key)
    request = task["pending"]["req_id"]
    client.status.raw.update(reqId=request, amendResult="-1")
    task = service.step(client=client, runtime=runtime, key=key)
    assert not task["pending"] and not task["active"]
    assert "拒绝" in task["message"] and client.status.price == 100


def test_corrupted_registry_does_not_break_other_strategies(setup):
    service, client, runtime = setup
    service.path.write_text('{"version":1,"tasks":{"bad":null}}', encoding="utf-8")
    loaded = PendingOrderChaseService(service.path)
    assert loaded.error and not loaded.tasks
    with pytest.raises(RuntimeError, match="读取失败"):
        loaded.start(client=client, runtime=runtime, order=client.row())
    assert loaded.authorized_update(runtime=runtime, inst_id="BTC-USDT-SWAP", status=client.status,
        old_price=100, old_size=10, stop=90) is None


def test_amend_wire_fields_preserve_protection_and_close_omits_size():
    client = OkxRestClient()
    client._request = Mock(return_value={"data": [{"ordId": "123", "sCode": "0", "sMsg": ""}]})
    creds = Credentials("key", "secret", "pass")
    try:
        client.amend_order(creds, environment="demo", inst_id="BTC-USDT-SWAP", ord_id="123", new_price=D(102), new_size=D("8.3"))
        assert client._request.call_args.kwargs["body"] == dict(instId="BTC-USDT-SWAP", ordId="123", newPx="102", newSz="8.3")
        client.amend_order(creds, environment="demo", inst_id="BTC-USDT-SWAP", ord_id="123", new_price=D(102))
        assert "newSz" not in client._request.call_args.kwargs["body"]
        assert "newSlTriggerPx" not in client._request.call_args.kwargs["body"]
    finally:
        client.close()
