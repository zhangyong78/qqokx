from decimal import Decimal as D
from types import SimpleNamespace as NS
from unittest.mock import Mock, patch
import threading
import pytest

from okx_quant import manual_order_protection as m


@pytest.fixture
def setup():
    m._PENDING_EDITS.clear()
    item = NS(inst_id="BTC-USDT-SWAP", inst_type="SWAP", source_kind="algo", algo_id="a",
              algo_client_order_id=None, side="sell", pos_side="net", size=D("1"), ord_type="oco",
              stop_loss_trigger_price=D("90"), stop_loss_order_price=D("-1"), stop_loss_trigger_price_type="mark",
              take_profit_trigger_price=D("120"), take_profit_order_price=D("-1"), take_profit_trigger_price_type="last")
    client = Mock()
    client.get_order_algo.return_value = {"state": "live"}
    client._parse_algo_order_item.return_value = item
    client.get_positions.return_value = [NS(inst_id=item.inst_id, pos_side="net", position=D("1"), avg_price=D("100"))]
    client.get_area_shape_fee_rate.return_value = D("0.001")
    client.get_trigger_price.return_value = D("110")
    client.amend_algo_order.return_value = NS(s_code="0", s_msg="")
    runtime = NS(environment="demo", credentials=object())
    with patch.object(m, "validate_usdt_linear_swap", return_value=(D("0.1"), D("1"), D("1"), D("1"))):
        yield client, runtime, item
    m._PENDING_EDITS.clear()


def test_exact_fee_equations_and_safe_tick_rounding():
    long = m.break_even_price(D("100"), D("0.001"), D("0.1"), "long")
    short = m.break_even_price(D("100"), D("0.001"), D("0.1"), "short")
    assert long == D("100.3") and short == D("99.8")
    assert long - 100 - (100 + long)*D("0.001") >= 0
    assert 100 - short - (100 + short)*D("0.001") >= 0


def test_fee_cache_is_isolated_by_account_environment_and_product():
    m._FEE_CACHE.clear()
    first = NS(environment="demo", credentials=NS(api_key="account-a"))
    second = NS(environment="demo", credentials=NS(api_key="account-b"))
    live = NS(environment="live", credentials=NS(api_key="account-a"))
    m._FEE_CACHE[m._fee_cache_key(first, "BTC-USDT-SWAP")] = D("0.001")
    assert m.cached_fee(first, "BTC-USDT-SWAP") == D("0.001")
    assert m.cached_fee(second, "BTC-USDT-SWAP") is None
    assert m.cached_fee(live, "BTC-USDT-SWAP") is None
    assert m.cached_fee(first, "ETH-USDT-SWAP") is None
    m._FEE_CACHE.clear()


def test_prepare_checks_matching_trigger_type_and_direction(setup):
    client, runtime, item = setup
    assert m.prepare_break_even(client, runtime, item)[1] == D("100.3")
    client.get_trigger_price.assert_called_with(item.inst_id, "mark", environment="demo", max_cached_age_seconds=0)
    client.get_positions.return_value[0].position = D("-1")
    item.side, item.stop_loss_trigger_price = "buy", D("120")
    client.get_trigger_price.return_value = D("90")
    assert m.prepare_break_even(client, runtime, item)[1] == D("99.8")


@pytest.mark.parametrize("case", ["too_early", "better_stop", "ambiguous", "wrong_close", "limit_stop", "ended"])
def test_invalid_preflight_never_writes(setup, case):
    client, runtime, item = setup
    if case == "too_early": client.get_trigger_price.return_value = D("100")
    if case == "better_stop": item.stop_loss_trigger_price = D("102")
    if case == "ambiguous": client.get_positions.return_value *= 2
    if case == "wrong_close": item.side = "buy"
    if case == "limit_stop": item.stop_loss_order_price = D("89")
    if case == "ended": client.get_order_algo.return_value = {"state": "canceled"}
    with pytest.raises(ValueError): m.prepare_break_even(client, runtime, item)
    client.amend_algo_order.assert_not_called()


def test_be_preserves_tp_and_confirms_exchange_result(setup):
    client, runtime, item = setup
    service = Mock()
    service.key.return_value = "key"
    service.snapshot.return_value = {}
    service._operation_lock.return_value = threading.RLock()
    def amend(*args, **kwargs):
        assert "new_take_profit_trigger_price" not in kwargs
        assert kwargs["environment"] == "demo"
        item.stop_loss_trigger_price = kwargs["new_stop_loss_trigger_price"]
        return NS(s_code="0")
    client.amend_algo_order.side_effect = amend
    with patch("okx_quant.pending_order_chase.get_pending_order_chase_service", return_value=service), patch("okx_quant.log_utils.append_log_line"):
        assert m.amend_protection(client, runtime, item, D("999"), None, break_even=True).stop_loss_trigger_price == D("100.3")
    assert not m._PENDING_EDITS


def test_timeout_is_not_retried_or_routed_to_other_environment(setup):
    client, runtime, item = setup
    service = Mock()
    service.key.return_value = "key"
    service.snapshot.return_value = {}
    service._operation_lock.return_value = threading.RLock()
    client.amend_algo_order.side_effect = TimeoutError("timeout")
    with patch("okx_quant.pending_order_chase.get_pending_order_chase_service", return_value=service), patch("okx_quant.log_utils.append_log_line"):
        with pytest.raises(TimeoutError): m.amend_protection(client, runtime, item, None, D("95"))
        with pytest.raises(ValueError, match="禁止重复"): m.amend_protection(client, runtime, item, None, D("95"))
    assert client.amend_algo_order.call_count == 1
