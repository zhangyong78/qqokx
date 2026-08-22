import unittest

from okx_quant.client_order_id import (
    CUSTOM_ORDER_ID_PREFIX,
    new_strategy_order_id,
    strategy_order_identity,
    with_custom_order_id_prefix,
)
from okx_quant.kline_rr_execution import RRTradeExecutionService


class ClientOrderIdTests(unittest.TestCase):
    def test_custom_order_ids_begin_with_rebate_prefix_and_fit_okx_limit(self) -> None:
        value = with_custom_order_id_prefix("s01emaent082212345678901")

        self.assertTrue(value.startswith(CUSTOM_ORDER_ID_PREFIX))
        self.assertEqual(len(value), 32)
        self.assertTrue(value[16:].startswith("s01e"))
        self.assertTrue(value.endswith("12345678901"))

    def test_short_custom_order_id_is_retained_after_rebate_prefix(self) -> None:
        self.assertEqual(
            with_custom_order_id_prefix("arb0123456789"),
            f"{CUSTOM_ORDER_ID_PREFIX}arb0123456789",
        )

    def test_strategy_order_ids_are_unique_across_restart_equivalent_calls(self) -> None:
        values = {
            new_strategy_order_id(session_id="S233", strategy_name="EMA 动态委托做多", role="entry")
            for _ in range(100)
        }

        self.assertEqual(len(values), 100)
        self.assertTrue(all(value.startswith(CUSTOM_ORDER_ID_PREFIX) for value in values))
        self.assertTrue(all(len(value) == 32 for value in values))

    def test_strategy_order_identity_is_stable_and_session_specific(self) -> None:
        first = strategy_order_identity("S233", "EMA 动态委托做多")

        self.assertEqual(first, strategy_order_identity("S233", "EMA 动态委托做多"))
        self.assertNotEqual(first, strategy_order_identity("S234", "EMA 动态委托做多"))

    def test_rr_executor_ids_keep_rebate_prefix_and_remain_deterministic(self) -> None:
        first = RRTradeExecutionService._client_id("plan-1", "entry", revision=0)

        self.assertEqual(first, RRTradeExecutionService._client_id("plan-1", "entry", revision=0))
        self.assertTrue(first.startswith(CUSTOM_ORDER_ID_PREFIX))
        self.assertEqual(len(first), 32)
