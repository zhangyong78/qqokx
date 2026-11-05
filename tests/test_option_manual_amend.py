from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock, patch
import threading

import pytest

from okx_quant import manual_order_protection as protection


def test_option_price_amend_checks_tick_and_verifies_without_changing_size():
    item = SimpleNamespace(inst_type="OPTION", inst_id="BTC-USD-261225-100000-C", source_kind="order",
                           price=Decimal("0.006"), ord_type="limit", order_id="123", client_order_id="c")
    updated = SimpleNamespace(**{**vars(item), "price": Decimal("0.0065")})
    runtime = SimpleNamespace(credentials=object(), environment="demo")
    service = Mock()
    service.key.return_value = "option-test"
    service._operation_lock.return_value = threading.RLock()
    service.snapshot.return_value = {}
    client = Mock()
    client.get_option_tick_bands.return_value = [SimpleNamespace(min_price=Decimal("0"), max_price=None, tick_size=Decimal("0.0005"))]
    client.amend_order.return_value = SimpleNamespace(s_code="0", s_msg="")
    with patch("okx_quant.pending_order_chase.get_pending_order_chase_service", return_value=service), patch.object(protection, "read_order", side_effect=[item, updated]):
        assert protection.amend_option_limit_price(client, runtime, item, Decimal("0.0065")) is updated
    assert client.amend_order.call_args.kwargs["new_price"] == Decimal("0.0065")
    assert "new_size" not in client.amend_order.call_args.kwargs


def test_option_price_amend_rejects_invalid_tick_before_writing():
    item = SimpleNamespace(inst_type="OPTION", inst_id="BTC-USD-261225-100000-C", source_kind="order",
                           price=Decimal("0.006"), ord_type="limit", order_id="123", client_order_id="c")
    runtime = SimpleNamespace(credentials=object(), environment="demo")
    service = Mock()
    service.key.return_value = "option-invalid-tick"
    service._operation_lock.return_value = threading.RLock()
    service.snapshot.return_value = {}
    client = Mock()
    client.get_option_tick_bands.return_value = [SimpleNamespace(min_price=Decimal("0"), max_price=None, tick_size=Decimal("0.0005"))]
    with patch("okx_quant.pending_order_chase.get_pending_order_chase_service", return_value=service), patch.object(protection, "read_order", return_value=item):
        with pytest.raises(ValueError, match="价格分档"):
            protection.amend_option_limit_price(client, runtime, item, Decimal("0.0061"))
    client.amend_order.assert_not_called()


def test_uncertain_option_amend_is_not_repeated():
    item = SimpleNamespace(inst_type="OPTION", inst_id="BTC-USD-261225-100000-C", source_kind="order",
                           price=Decimal("0.006"), ord_type="limit", order_id="123", client_order_id="c")
    runtime = SimpleNamespace(credentials=object(), environment="demo")
    service = Mock()
    service.key.return_value = "option-uncertain"
    service._operation_lock.return_value = threading.RLock()
    service.snapshot.return_value = {}
    client = Mock()
    client.get_option_tick_bands.return_value = [SimpleNamespace(min_price=Decimal("0"), max_price=None, tick_size=Decimal("0.0005"))]
    client.amend_order.side_effect = TimeoutError("timeout")
    try:
        with patch("okx_quant.pending_order_chase.get_pending_order_chase_service", return_value=service), patch.object(protection, "read_order", return_value=item):
            with pytest.raises(TimeoutError):
                protection.amend_option_limit_price(client, runtime, item, Decimal("0.0065"))
            with pytest.raises(ValueError, match="尚未确认"):
                protection.amend_option_limit_price(client, runtime, item, Decimal("0.0065"))
        client.amend_order.assert_called_once()
    finally:
        protection._PENDING_PRICE_EDITS.pop("option-uncertain", None)
