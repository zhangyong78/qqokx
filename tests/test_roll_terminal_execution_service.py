from __future__ import annotations

import unittest
from decimal import Decimal
from types import SimpleNamespace

from okx_quant.okx_client import OkxFillHistoryItem, OkxTradeOrderItem
from roll_terminal_qt.execution_service import RollExecutionThread


class RollTerminalExecutionServiceTests(unittest.TestCase):
    def test_load_recent_roll_fills_keeps_other_leg_when_only_one_leg_has_order_history(self) -> None:
        class FakeClient:
            def get_fills_history(self, *args, **kwargs):  # noqa: ANN002, ANN003
                return [
                    OkxFillHistoryItem(
                        fill_time=100_000,
                        inst_id="BTC-USD-260626",
                        inst_type="FUTURES",
                        side="buy",
                        pos_side="short",
                        fill_price=Decimal("64000"),
                        fill_size=Decimal("1"),
                        fill_fee=Decimal("-0.0000013"),
                        fee_currency="BTC",
                        pnl=None,
                        order_id="old-leg-order",
                        trade_id="trade-1",
                        exec_type=None,
                        raw={},
                    ),
                    OkxFillHistoryItem(
                        fill_time=100_001,
                        inst_id="BTC-USD-260925",
                        inst_type="FUTURES",
                        side="sell",
                        pos_side="short",
                        fill_price=Decimal("65200"),
                        fill_size=Decimal("1"),
                        fill_fee=Decimal("-0.0000014"),
                        fee_currency="BTC",
                        pnl=None,
                        order_id="target-leg-order",
                        trade_id="trade-2",
                        exec_type=None,
                        raw={},
                    ),
                ]

        thread = RollExecutionThread.__new__(RollExecutionThread)
        thread._runtime = SimpleNamespace(credentials=object(), environment="paper")
        thread._plan = SimpleNamespace(current=SimpleNamespace(side="空"))
        matched = thread._load_recent_roll_fills(
            client=FakeClient(),
            request=SimpleNamespace(
                current_derivative_inst_id="BTC-USD-260626",
                target_derivative_inst_id="BTC-USD-260925",
            ),
            started_at_ms=100_500,
            explicit_order_ids=None,
            matched_order_ids_by_leg={"current": {"old-leg-order"}, "target": set()},
        )
        self.assertEqual([item.order_id for item in matched], ["old-leg-order", "target-leg-order"])

    def test_load_recent_roll_fills_prefers_exact_order_ids_even_if_fill_time_is_old(self) -> None:
        class FakeClient:
            def get_fills_history(self, *args, **kwargs):  # noqa: ANN002, ANN003
                return [
                    OkxFillHistoryItem(
                        fill_time=1,
                        inst_id="BTC-USD-260626",
                        inst_type="FUTURES",
                        side="buy",
                        pos_side="short",
                        fill_price=Decimal("64000"),
                        fill_size=Decimal("1"),
                        fill_fee=Decimal("-0.0000013"),
                        fee_currency="BTC",
                        pnl=None,
                        order_id="tracked-order",
                        trade_id="trade-1",
                        exec_type=None,
                        raw={},
                    )
                ]

        thread = RollExecutionThread.__new__(RollExecutionThread)
        thread._runtime = SimpleNamespace(credentials=object(), environment="paper")
        thread._plan = SimpleNamespace(current=SimpleNamespace(side="\u7a7a"))
        matched = thread._load_recent_roll_fills(
            client=FakeClient(),
            request=SimpleNamespace(
                current_derivative_inst_id="BTC-USD-260626",
                target_derivative_inst_id="BTC-USD-260925",
            ),
            started_at_ms=100_500,
            explicit_order_ids={"tracked-order"},
            matched_order_ids_by_leg={"current": set(), "target": set()},
        )
        self.assertEqual([item.order_id for item in matched], ["tracked-order"])

    def test_estimate_fill_fee_usdt_totals_from_coin_margin_fill_price(self) -> None:
        totals = RollExecutionThread._estimate_fill_fee_usdt_totals(
            [
                OkxFillHistoryItem(
                    fill_time=None,
                    inst_id="BTC-USD-260925",
                    inst_type="FUTURES",
                    side="sell",
                    pos_side="short",
                    fill_price=Decimal("64000"),
                    fill_size=Decimal("1"),
                    fill_fee=Decimal("-0.0001"),
                    fee_currency="BTC",
                    pnl=None,
                    order_id="1",
                    trade_id="1",
                    exec_type=None,
                    raw={},
                )
            ]
        )
        self.assertEqual(totals, {"BTC": Decimal("-6.4")})

    def test_format_fee_totals_appends_usdt_approximation_and_total(self) -> None:
        text = RollExecutionThread._format_fee_totals(
            {"BTC": Decimal("-0.0001"), "USDT": Decimal("-1.23")},
            {"BTC": Decimal("-6.4")},
        )
        self.assertIn("-0.0001 BTC (\u2248-6.4 USDT)", text)
        self.assertIn("-1.23 USDT", text)
        self.assertIn("\u6298\u5408USDT\u5408\u8ba1 \u2248-7.63", text)

    def test_estimate_order_fee_uses_avg_price_fallback(self) -> None:
        totals = RollExecutionThread._estimate_order_fee_usdt_totals(
            [
                OkxTradeOrderItem(
                    source_kind="order",
                    source_label="order",
                    created_time=None,
                    update_time=None,
                    inst_id="ETH-USDT",
                    inst_type="SPOT",
                    side="buy",
                    pos_side=None,
                    td_mode="cash",
                    ord_type="market",
                    state="filled",
                    price=None,
                    size=Decimal("1"),
                    filled_size=Decimal("1"),
                    avg_price=Decimal("2500"),
                    order_id="2",
                    algo_id=None,
                    client_order_id=None,
                    algo_client_order_id=None,
                    pnl=None,
                    fee=Decimal("-0.01"),
                    fee_currency="ETH",
                    reduce_only=None,
                    trigger_price=None,
                    trigger_price_type=None,
                    order_price=None,
                    actual_price=None,
                    actual_size=None,
                    actual_side=None,
                    take_profit_trigger_price=None,
                    take_profit_order_price=None,
                    take_profit_trigger_price_type=None,
                    stop_loss_trigger_price=None,
                    stop_loss_order_price=None,
                    stop_loss_trigger_price_type=None,
                    raw={},
                )
            ]
        )
        self.assertEqual(totals, {"ETH": Decimal("-25")})

    def test_sum_fee_usdt_total_combines_stable_and_converted_values(self) -> None:
        total = RollExecutionThread._sum_fee_usdt_total(
            {"BTC": Decimal("-0.0001"), "USDT": Decimal("-1.23")},
            {"BTC": Decimal("-6.4")},
        )
        self.assertEqual(total, Decimal("-7.63"))

    def test_compute_fee_per_coin_usdt(self) -> None:
        thread = RollExecutionThread.__new__(RollExecutionThread)
        thread._plan = SimpleNamespace(
            qty=Decimal("10"),
            current=SimpleNamespace(
                inst_id="BTC-USD-260925",
                contract_value=Decimal("100"),
                contract_value_ccy="USD",
                notional_base=Decimal("0.015625"),
                available=Decimal("10"),
            ),
        )
        value = thread._compute_fee_per_coin_usdt(
            total_fee_usdt=Decimal("-0.3"),
            current_avg_price=Decimal("64000"),
            target_avg_price=Decimal("64500"),
            executed_contract_qty=Decimal("10"),
        )
        self.assertEqual(value, Decimal("19.27470817120622568093385214"))

    def test_compute_fee_per_coin_usdt_uses_actual_completed_contract_qty(self) -> None:
        thread = RollExecutionThread.__new__(RollExecutionThread)
        thread._plan = SimpleNamespace(
            qty=Decimal("10"),
            current=SimpleNamespace(
                inst_id="BTC-USD-260925",
                contract_value=Decimal("100"),
                contract_value_ccy="USD",
                notional_base=Decimal("0.015625"),
                available=Decimal("10"),
            ),
        )
        value = thread._compute_fee_per_coin_usdt(
            total_fee_usdt=Decimal("-0.0717"),
            current_avg_price=Decimal("64137.56"),
            target_avg_price=Decimal("64718.87"),
            executed_contract_qty=Decimal("2"),
        )
        self.assertEqual(value, Decimal("23.09704500087354895677305355"))

    def test_build_fee_per_coin_line_reports_usdt_per_coin_and_spread_share(self) -> None:
        thread = RollExecutionThread.__new__(RollExecutionThread)
        thread._plan = SimpleNamespace(
            qty=Decimal("10"),
            current=SimpleNamespace(
                inst_id="BTC-USD-260925",
                contract_value=Decimal("100"),
                contract_value_ccy="USD",
                notional_base=Decimal("0.015625"),
                available=Decimal("10"),
            ),
        )
        text = thread._build_fee_per_coin_line(
            fee_per_coin_usdt=Decimal("19.27470817120622568093385214"),
            avg_spread=Decimal("500"),
        )
        self.assertEqual(text, "\u6309 1 BTC \u6298\u7b97\u624b\u7eed\u8d39\uff1a19.2747 USDT | \u7ea6\u5360\u672c\u6b21\u5e73\u5747\u4ef7\u5dee 3.85%")

    def test_build_net_spread_after_fee_line(self) -> None:
        thread = RollExecutionThread.__new__(RollExecutionThread)
        text = thread._build_net_spread_after_fee_line(
            fee_per_coin_usdt=Decimal("19.27470817120622568093385214"),
            avg_spread=Decimal("577.3"),
        )
        self.assertEqual(text, "\u6263\u53cc\u817f\u624b\u7eed\u8d39\u540e\u51c0\u4ef7\u5dee\uff1a558.0253 USDT/BTC")

    def test_should_defer_fee_summary_for_nonterminal_result(self) -> None:
        self.assertTrue(
            RollExecutionThread._should_defer_fee_summary_for_result(
                SimpleNamespace(message="移仓部分完成后中断：原因：目标合约挂单腿 撤单后订单仍未进入终态（当前状态：live）。")
            )
        )
        self.assertFalse(
            RollExecutionThread._should_defer_fee_summary_for_result(
                SimpleNamespace(message="已按停止请求在当前批次完成后停止：回补 BTC-USD-260626 31.5 张，开出 BTC-USD-260925 31.5 张。")
            )
        )
        self.assertFalse(
            RollExecutionThread._should_defer_fee_summary_for_result(
                SimpleNamespace(message="移仓完成：回补 10 张，开出 10 张。")
            )
        )

    def test_estimate_roll_fee_history_limit_scales_with_completed_qty(self) -> None:
        thread = RollExecutionThread.__new__(RollExecutionThread)
        thread._plan = SimpleNamespace(qty=Decimal("100"), chase_limit=3)
        self.assertEqual(
            thread._estimate_roll_fee_history_limit(executed_contract_qty=Decimal("100")),
            800,
        )


if __name__ == "__main__":
    unittest.main()
