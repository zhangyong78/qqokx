from decimal import Decimal
from threading import Event
from time import sleep
from unittest import TestCase

from okx_quant.models import Credentials, Instrument, StrategyConfig
from okx_quant.okx_client import OkxApiError, OkxOrderResult, OkxOrderStatus, OkxPosition
from okx_quant.position_protection import (
    OptionProtectionConfig,
    PositionProtectionManager,
    ProtectionReplayPoint,
    build_close_order_price,
    describe_protection_price_logic,
    evaluate_protection_trigger,
    infer_protection_profit_on_rise,
    infer_default_spot_inst_id,
    normalize_spot_inst_id,
    replay_option_protection,
    wait_order_fill,
)
from okx_quant.ui import (
    _find_position_by_key,
    _position_tree_row_id,
    _resolve_position_selection_target,
    _validate_protection_live_price_availability,
    _validate_protection_price_relationship,
)


def _make_credentials() -> Credentials:
    return Credentials(api_key="key", secret_key="secret", passphrase="pass")


def _make_strategy_config() -> StrategyConfig:
    return StrategyConfig(
        inst_id="BTC-USD-20260327-70000-C",
        bar="1m",
        ema_period=1,
        atr_period=1,
        atr_stop_multiplier=Decimal("1"),
        atr_take_multiplier=Decimal("1"),
        order_size=Decimal("1"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="long_short",
        environment="live",
        tp_sl_trigger_type="mark",
        strategy_id="manual_option_protection",
        poll_seconds=0.01,
        trade_inst_id="BTC-USD-20260327-70000-C",
        tp_sl_mode="local_trade",
        local_tp_sl_inst_id="BTC-USD-20260327-70000-C",
        entry_side_mode="follow_signal",
        run_mode="trade",
    )


def _make_option_position(
    *,
    inst_id: str = "BTC-USD-20260327-70000-C",
    position: str = "2",
    pos_side: str = "long",
) -> OkxPosition:
    return OkxPosition(
        inst_id=inst_id,
        inst_type="OPTION",
        pos_side=pos_side,
        mgn_mode="cross",
        position=Decimal(position),
        avail_position=Decimal(position.lstrip("-")),
        avg_price=Decimal("0.015"),
        mark_price=Decimal("0.015"),
        unrealized_pnl=Decimal("0"),
        unrealized_pnl_ratio=Decimal("0"),
        liquidation_price=None,
        leverage=None,
        margin_ccy="BTC",
        last_price=Decimal("0.015"),
        realized_pnl=Decimal("0"),
        margin_ratio=None,
        initial_margin=None,
        maintenance_margin=None,
        delta=None,
        gamma=None,
        vega=None,
        theta=None,
        raw={},
    )


class _StubPriceClient:
    def __init__(self, mark_price: Decimal) -> None:
        self._mark_price = mark_price

    def get_trigger_price(self, inst_id: str, price_type: str) -> Decimal:
        assert inst_id
        assert price_type == "mark"
        return self._mark_price


class _MissingMarkPriceClient:
    def get_trigger_price(self, inst_id: str, price_type: str) -> Decimal:
        raise OkxApiError(f"{inst_id} 缺少标记价格，无法触发")


class _NotifierStub:
    def __init__(self) -> None:
        self.enabled = True
        self.messages: list[tuple[str, str]] = []

    def notify_async(self, subject: str, body: str) -> None:
        self.messages.append((subject, body))


class _SimulatedProtectionClient:
    def __init__(
        self,
        *,
        initial_position: OkxPosition,
        trigger_prices: dict[tuple[str, str], list[Decimal]],
        order_results: list[dict[str, Decimal | str | None]],
        option_tick_size: Decimal = Decimal("0.0001"),
        option_lot_size: Decimal = Decimal("1"),
        option_min_size: Decimal = Decimal("1"),
    ) -> None:
        self._position_template = initial_position
        self.current_position = initial_position.position
        self._directional_position = bool(initial_position.pos_side and initial_position.pos_side.lower() != "net")
        self._trigger_prices = {key: list(values) for key, values in trigger_prices.items()}
        self._order_results = list(order_results)
        self._order_lookup: dict[str, dict[str, Decimal | str | None]] = {}
        self.price_requests: list[tuple[str, str]] = []
        self.orders: list[dict[str, Decimal | str | None]] = []
        self._instrument = Instrument(
            inst_id=initial_position.inst_id,
            inst_type="OPTION",
            tick_size=option_tick_size,
            lot_size=option_lot_size,
            min_size=option_min_size,
            state="live",
            settle_ccy="BTC",
            ct_val=Decimal("1"),
            ct_mult=Decimal("1"),
            ct_val_ccy="BTC",
            uly="BTC-USD",
            inst_family="BTC-USD",
        )

    def get_trigger_price(self, inst_id: str, price_type: str) -> Decimal:
        self.price_requests.append((inst_id, price_type))
        key = (inst_id, price_type)
        prices = self._trigger_prices[key]
        if len(prices) > 1:
            return prices.pop(0)
        return prices[0]

    def get_instrument(self, inst_id: str) -> Instrument:
        assert inst_id == self._instrument.inst_id
        return self._instrument

    def place_simple_order(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_id: str,
        side: str,
        size: Decimal,
        ord_type: str,
        pos_side: str | None = None,
        price: Decimal | None = None,
    ) -> OkxOrderResult:
        assert credentials.api_key
        assert config.environment == "live"
        ord_id = f"O{len(self.orders) + 1}"
        self.orders.append(
            {
                "inst_id": inst_id,
                "side": side,
                "size": size,
                "ord_type": ord_type,
                "pos_side": pos_side,
                "price": price,
            }
        )
        order_result = self._order_results.pop(0)
        self._order_lookup[ord_id] = dict(order_result, applied=False)
        return OkxOrderResult(ord_id=ord_id, cl_ord_id=None, s_code="0", s_msg="", raw={})

    def get_order(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_id: str,
        ord_id: str,
    ) -> OkxOrderStatus:
        assert credentials.secret_key
        assert config.trade_mode == "cross"
        order_result = self._order_lookup[ord_id]
        if not order_result["applied"]:
            self._apply_fill(order_result.get("filled_size"))
            order_result["applied"] = True
        return OkxOrderStatus(
            ord_id=ord_id,
            state=str(order_result["state"]),
            side=None,
            ord_type="ioc",
            price=order_result.get("price"),
            avg_price=order_result.get("avg_price"),
            size=order_result.get("size"),
            filled_size=order_result.get("filled_size"),
            raw={},
        )

    def get_positions(
        self,
        credentials: Credentials,
        *,
        environment: str,
        inst_type: str | None = None,
    ) -> list[OkxPosition]:
        assert credentials.passphrase
        assert environment == "live"
        if inst_type is not None:
            assert inst_type == "OPTION"
        if self.current_position == 0:
            return []
        return [
            OkxPosition(
                inst_id=self._position_template.inst_id,
                inst_type=self._position_template.inst_type,
                pos_side=self._position_template.pos_side,
                mgn_mode=self._position_template.mgn_mode,
                position=self.current_position,
                avail_position=abs(self.current_position),
                avg_price=self._position_template.avg_price,
                mark_price=self._position_template.mark_price,
                unrealized_pnl=self._position_template.unrealized_pnl,
                unrealized_pnl_ratio=self._position_template.unrealized_pnl_ratio,
                liquidation_price=self._position_template.liquidation_price,
                leverage=self._position_template.leverage,
                margin_ccy=self._position_template.margin_ccy,
                last_price=self._position_template.last_price,
                realized_pnl=self._position_template.realized_pnl,
                margin_ratio=self._position_template.margin_ratio,
                initial_margin=self._position_template.initial_margin,
                maintenance_margin=self._position_template.maintenance_margin,
                delta=self._position_template.delta,
                gamma=self._position_template.gamma,
                vega=self._position_template.vega,
                theta=self._position_template.theta,
                raw=self._position_template.raw,
            )
        ]

    def _apply_fill(self, filled_size: Decimal | None) -> None:
        fill = filled_size or Decimal("0")
        if fill <= 0:
            return
        if self._directional_position:
            self.current_position = max(Decimal("0"), self.current_position - fill)
            return
        if self.current_position >= 0:
            self.current_position = max(Decimal("0"), self.current_position - fill)
        else:
            self.current_position = min(Decimal("0"), self.current_position + fill)


class _OrderStatusClient:
    def __init__(self, *statuses: OkxOrderStatus) -> None:
        self._statuses = list(statuses)
        self.calls = 0

    def get_order(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_id: str,
        ord_id: str,
    ) -> OkxOrderStatus:
        assert credentials.api_key
        assert config.environment
        assert inst_id
        assert ord_id
        self.calls += 1
        if len(self._statuses) > 1:
            return self._statuses.pop(0)
        return self._statuses[0]


class PositionProtectionTest(TestCase):
    def test_infer_and_normalize_spot_symbol(self) -> None:
        self.assertEqual(infer_default_spot_inst_id("BTC-USD-20260327-70000-C"), "BTC-USDT")
        self.assertEqual(normalize_spot_inst_id("ethusdt"), "ETH-USDT")
        self.assertEqual(normalize_spot_inst_id("SOL-USDT"), "SOL-USDT")

    def test_evaluate_trigger_for_long_and_short_boundaries(self) -> None:
        cases = [
            ("long", Decimal("95"), Decimal("95"), Decimal("110"), (True, False)),
            ("long", Decimal("110"), Decimal("95"), Decimal("110"), (False, True)),
            ("long", Decimal("100"), Decimal("95"), Decimal("110"), (False, False)),
            ("short", Decimal("105"), Decimal("105"), Decimal("90"), (True, False)),
            ("short", Decimal("90"), Decimal("105"), Decimal("90"), (False, True)),
            ("short", Decimal("100"), Decimal("105"), Decimal("90"), (False, False)),
        ]
        for direction, current_price, stop_loss, take_profit, expected in cases:
            with self.subTest(direction=direction, current_price=current_price):
                self.assertEqual(
                    evaluate_protection_trigger(
                        direction=direction,
                        current_price=current_price,
                        stop_loss=stop_loss,
                        take_profit=take_profit,
                    ),
                    expected,
                )

    def test_evaluate_trigger_respects_call_put_when_using_spot_price(self) -> None:
        expected = {
            ("BTC-USD-20260327-70000-C", "long"): True,
            ("BTC-USD-20260327-70000-C", "short"): False,
            ("BTC-USD-20260327-70000-P", "long"): False,
            ("BTC-USD-20260327-70000-P", "short"): True,
        }
        for (inst_id, direction), profit_on_rise in expected.items():
            with self.subTest(inst_id=inst_id, direction=direction):
                self.assertEqual(
                    infer_protection_profit_on_rise(
                        option_inst_id=inst_id,
                        direction=direction,
                        trigger_inst_id="BTC-USDT",
                        trigger_price_type="last",
                    ),
                    profit_on_rise,
                )

        self.assertTrue(
            infer_protection_profit_on_rise(
                option_inst_id="BTC-USD-20260327-70000-P",
                direction="short",
                trigger_inst_id="BTC-USDT",
                trigger_price_type="last",
            )
        )
        self.assertFalse(
            infer_protection_profit_on_rise(
                option_inst_id="BTC-USD-20260327-70000-C",
                direction="short",
                trigger_inst_id="BTC-USDT",
                trigger_price_type="last",
            )
        )
        self.assertEqual(
            evaluate_protection_trigger(
                direction="short",
                current_price=Decimal("100000"),
                stop_loss=Decimal("50000"),
                take_profit=Decimal("100000"),
                option_inst_id="BTC-USD-20260327-70000-P",
                uses_underlying_trigger=True,
            ),
            (False, True),
        )
        self.assertEqual(
            evaluate_protection_trigger(
                direction="short",
                current_price=Decimal("100000"),
                stop_loss=Decimal("100000"),
                take_profit=Decimal("50000"),
                option_inst_id="BTC-USD-20260327-70000-C",
                uses_underlying_trigger=True,
            ),
            (True, False),
        )

    def test_describe_protection_price_logic_mentions_short_put_rise_take_profit(self) -> None:
        text = describe_protection_price_logic(
            option_inst_id="BTC-USD-20260626-80000-P",
            direction="short",
            trigger_inst_id="BTC-USDT",
            trigger_price_type="last",
        )
        self.assertIn("卖出认沽", text)
        self.assertIn("价格上涨偏向止盈", text)
        self.assertIn("价格下跌偏向止损", text)

    def test_ui_validation_allows_short_put_spot_take_profit_above_stop_loss(self) -> None:
        _validate_protection_price_relationship(
            option_inst_id="BTC-USD-20260626-50000-P",
            direction="short",
            trigger_inst_id="BTC-USDT",
            trigger_price_type="last",
            take_profit=Decimal("100000"),
            stop_loss=Decimal("50000"),
        )

    def test_live_price_validation_blocks_missing_mark_trigger(self) -> None:
        protection = OptionProtectionConfig(
            option_inst_id="BTC-USD-20260626-100000-C",
            trigger_inst_id="BTC-USD-20260626-100000-C",
            trigger_price_type="mark",
            direction="short",
            pos_side="short",
            take_profit_trigger=Decimal("0.01"),
            stop_loss_trigger=Decimal("0.02"),
            take_profit_order_mode="fixed_price",
            take_profit_order_price=Decimal("0.009"),
            take_profit_slippage=Decimal("0"),
            stop_loss_order_mode="fixed_price",
            stop_loss_order_price=Decimal("0.021"),
            stop_loss_slippage=Decimal("0"),
            poll_seconds=2,
            trigger_label="option mark",
        )
        with self.assertRaisesRegex(ValueError, "不能用“期权标记价格”触发"):
            _validate_protection_live_price_availability(_MissingMarkPriceClient(), protection)

    def test_live_price_validation_blocks_missing_mark_slippage_order(self) -> None:
        protection = OptionProtectionConfig(
            option_inst_id="BTC-USD-20260626-100000-C",
            trigger_inst_id="BTC-USDT",
            trigger_price_type="last",
            direction="short",
            pos_side="short",
            take_profit_trigger=Decimal("90000"),
            stop_loss_trigger=Decimal("110000"),
            take_profit_order_mode="mark_with_slippage",
            take_profit_order_price=None,
            take_profit_slippage=Decimal("0"),
            stop_loss_order_mode="fixed_price",
            stop_loss_order_price=Decimal("0.021"),
            stop_loss_slippage=Decimal("0"),
            poll_seconds=2,
            trigger_label="spot last",
        )
        with self.assertRaisesRegex(ValueError, "不能用“标记价格加减滑点”报单"):
            _validate_protection_live_price_availability(_MissingMarkPriceClient(), protection)

    def test_replay_short_put_spot_take_profit_uses_rise_logic(self) -> None:
        result = replay_option_protection(
            protection=OptionProtectionConfig(
                option_inst_id="BTC-USD-20260626-50000-P",
                trigger_inst_id="BTC-USDT",
                trigger_price_type="last",
                direction="short",
                pos_side="short",
                take_profit_trigger=Decimal("100000"),
                stop_loss_trigger=Decimal("50000"),
                take_profit_order_mode="mark_with_slippage",
                take_profit_order_price=None,
                take_profit_slippage=Decimal("0"),
                stop_loss_order_mode="mark_with_slippage",
                stop_loss_order_price=None,
                stop_loss_slippage=Decimal("0"),
                poll_seconds=2,
                trigger_label="BTC-USDT 最新价",
            ),
            initial_position=Decimal("2"),
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            points=[
                ProtectionReplayPoint(ts=1, trigger_price=Decimal("80000"), option_mark_price=Decimal("0.0500")),
                ProtectionReplayPoint(ts=2, trigger_price=Decimal("100000"), option_mark_price=Decimal("0.0300")),
            ],
        )
        self.assertEqual(result.status, "filled")
        self.assertEqual(result.trigger_reason, "take_profit")
        self.assertEqual(result.close_side, "buy")
        self.assertEqual(result.close_order_price, Decimal("0.03"))

    def test_replay_short_call_spot_stop_loss_uses_rise_as_loss_logic(self) -> None:
        result = replay_option_protection(
            protection=OptionProtectionConfig(
                option_inst_id="BTC-USD-20260626-100000-C",
                trigger_inst_id="BTC-USDT",
                trigger_price_type="last",
                direction="short",
                pos_side="short",
                take_profit_trigger=Decimal("50000"),
                stop_loss_trigger=Decimal("100000"),
                take_profit_order_mode="mark_with_slippage",
                take_profit_order_price=None,
                take_profit_slippage=Decimal("0"),
                stop_loss_order_mode="mark_with_slippage",
                stop_loss_order_price=None,
                stop_loss_slippage=Decimal("0"),
                poll_seconds=2,
                trigger_label="BTC-USDT 最新价",
            ),
            initial_position=Decimal("2"),
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            points=[
                ProtectionReplayPoint(ts=1, trigger_price=Decimal("90000"), option_mark_price=Decimal("0.0300")),
                ProtectionReplayPoint(ts=2, trigger_price=Decimal("100000"), option_mark_price=Decimal("0.0600")),
            ],
        )
        self.assertEqual(result.status, "filled")
        self.assertEqual(result.trigger_reason, "stop_loss")
        self.assertEqual(result.close_side, "buy")
        self.assertEqual(result.close_order_price, Decimal("0.06"))

    def test_position_tree_row_id_is_stable_for_same_contract_side_mode(self) -> None:
        first = _make_option_position(inst_id="BTC-USD-20260327-70000-C", position="2", pos_side="short")
        second = _make_option_position(inst_id="BTC-USD-20260327-70000-C", position="5", pos_side="short")
        self.assertEqual(_position_tree_row_id(first), _position_tree_row_id(second))

    def test_find_position_by_key_restores_same_selected_contract(self) -> None:
        target = _make_option_position(inst_id="BTC-USD-20260327-70000-C", position="2", pos_side="short")
        other = _make_option_position(inst_id="BTC-USD-20260327-65000-P", position="1", pos_side="long")
        restored = _find_position_by_key([other, target], _position_tree_row_id(target))
        self.assertIsNotNone(restored)
        self.assertEqual(restored.inst_id, target.inst_id)
        self.assertEqual(restored.pos_side, target.pos_side)

    def test_resolve_position_selection_target_prefers_protection_key(self) -> None:
        self.assertEqual(
            _resolve_position_selection_target(
                existing_ids={"pos:a", "pos:b", "asset:x"},
                selected_position_key=None,
                protection_position_key="pos:b",
                selected_before="asset:x",
                top_items=("asset:x",),
            ),
            "pos:b",
        )

    def test_build_close_order_price_supports_fixed_and_mark_slippage(self) -> None:
        client = _StubPriceClient(Decimal("0.0150"))

        fixed_sell = build_close_order_price(
            client=client,
            option_inst_id="BTC-USD-20260327-70000-C",
            close_side="sell",
            tick_size=Decimal("0.0001"),
            mode="fixed_price",
            fixed_price=Decimal("0.01436"),
            slippage=Decimal("0"),
        )
        buy_mark = build_close_order_price(
            client=client,
            option_inst_id="BTC-USD-20260327-70000-C",
            close_side="buy",
            tick_size=Decimal("0.0001"),
            mode="mark_with_slippage",
            fixed_price=None,
            slippage=Decimal("0.0003"),
        )
        sell_mark = build_close_order_price(
            client=client,
            option_inst_id="BTC-USD-20260327-70000-C",
            close_side="sell",
            tick_size=Decimal("0.0001"),
            mode="mark_with_slippage",
            fixed_price=None,
            slippage=Decimal("0.0003"),
        )

        self.assertEqual(fixed_sell, Decimal("0.0143"))
        self.assertEqual(buy_mark, Decimal("0.0153"))
        self.assertEqual(sell_mark, Decimal("0.0147"))

    def test_build_close_order_price_keeps_sell_price_above_zero(self) -> None:
        client = _StubPriceClient(Decimal("0.0002"))
        self.assertEqual(
            build_close_order_price(
                client=client,
                option_inst_id="BTC-USD-20260327-70000-C",
                close_side="sell",
                tick_size=Decimal("0.0001"),
                mode="mark_with_slippage",
                fixed_price=None,
                slippage=Decimal("0.0010"),
            ),
            Decimal("0.0001"),
        )

    def test_wait_order_fill_returns_partial_fill_immediately(self) -> None:
        client = _OrderStatusClient(
            OkxOrderStatus(
                ord_id="1",
                state="partially_filled",
                side="sell",
                ord_type="ioc",
                price=Decimal("0.0149"),
                avg_price=Decimal("0.0150"),
                size=Decimal("3"),
                filled_size=Decimal("1"),
                raw={},
            )
        )
        filled_size, filled_price = wait_order_fill(
            client=client,
            credentials=_make_credentials(),
            config=_make_strategy_config(),
            inst_id="BTC-USD-20260327-70000-C",
            ord_id="1",
            estimated_price=Decimal("0.0148"),
            wait_seconds=0.001,
            stop_event=Event(),
        )
        self.assertEqual(filled_size, Decimal("1"))
        self.assertEqual(filled_price, Decimal("0.0150"))
        self.assertEqual(client.calls, 1)

    def test_wait_order_fill_raises_when_order_is_canceled(self) -> None:
        client = _OrderStatusClient(
            OkxOrderStatus(
                ord_id="1",
                state="canceled",
                side="sell",
                ord_type="ioc",
                price=Decimal("0.0149"),
                avg_price=None,
                size=Decimal("3"),
                filled_size=Decimal("0"),
                raw={},
            )
        )
        with self.assertRaisesRegex(RuntimeError, "ordId=1"):
            wait_order_fill(
                client=client,
                credentials=_make_credentials(),
                config=_make_strategy_config(),
                inst_id="BTC-USD-20260327-70000-C",
                ord_id="1",
                estimated_price=Decimal("0.0148"),
                wait_seconds=0.001,
                stop_event=Event(),
            )

    def test_manager_take_profit_closes_long_position_on_option_mark(self) -> None:
        option_inst_id = "BTC-USD-20260327-70000-C"
        client = _SimulatedProtectionClient(
            initial_position=_make_option_position(inst_id=option_inst_id, position="2", pos_side="long"),
            trigger_prices={
                (option_inst_id, "mark"): [Decimal("0.0148"), Decimal("0.0151")],
            },
            order_results=[
                {
                    "state": "filled",
                    "filled_size": Decimal("2"),
                    "avg_price": Decimal("0.0147"),
                    "price": Decimal("0.0147"),
                    "size": Decimal("2"),
                }
            ],
        )
        notifier = _NotifierStub()
        logs: list[str] = []
        manager = PositionProtectionManager(client, logs.append, notifier=notifier)

        session_id = manager.start(
            _make_credentials(),
            _make_strategy_config(),
            OptionProtectionConfig(
                option_inst_id=option_inst_id,
                trigger_inst_id=option_inst_id,
                trigger_price_type="mark",
                direction="long",
                pos_side="long",
                take_profit_trigger=Decimal("0.0150"),
                stop_loss_trigger=Decimal("0.0130"),
                take_profit_order_mode="fixed_price",
                take_profit_order_price=Decimal("0.0147"),
                take_profit_slippage=Decimal("0"),
                stop_loss_order_mode="fixed_price",
                stop_loss_order_price=Decimal("0.0132"),
                stop_loss_slippage=Decimal("0"),
                poll_seconds=0.01,
                trigger_label=f"{option_inst_id} mark",
            ),
        )

        worker = manager._workers[session_id]
        assert worker.thread is not None
        worker.thread.join(timeout=1)

        self.assertFalse(worker.thread.is_alive())
        self.assertEqual(client.current_position, Decimal("0"))
        self.assertEqual(len(client.orders), 1)
        self.assertEqual(client.orders[0]["side"], "sell")
        self.assertEqual(client.orders[0]["price"], Decimal("0.0147"))
        self.assertIn((option_inst_id, "mark"), client.price_requests)
        self.assertTrue(any("止盈" in subject for subject, _ in notifier.messages))
        self.assertTrue(any("持仓保护成交" in subject for subject, _ in notifier.messages))
        self.assertTrue(any("止盈触发" in line for line in logs))

    def test_manager_stop_loss_closes_short_position_from_spot_trigger_in_multiple_orders(self) -> None:
        option_inst_id = "BTC-USD-20260327-70000-P"
        spot_inst_id = "BTC-USDT"
        client = _SimulatedProtectionClient(
            initial_position=_make_option_position(inst_id=option_inst_id, position="3", pos_side="short"),
            trigger_prices={
                (spot_inst_id, "last"): [Decimal("99999"), Decimal("100001")],
                (option_inst_id, "mark"): [Decimal("0.0150")],
            },
            order_results=[
                {
                    "state": "partially_filled",
                    "filled_size": Decimal("1"),
                    "avg_price": Decimal("0.0153"),
                    "price": Decimal("0.0153"),
                    "size": Decimal("3"),
                },
                {
                    "state": "filled",
                    "filled_size": Decimal("2"),
                    "avg_price": Decimal("0.0154"),
                    "price": Decimal("0.0153"),
                    "size": Decimal("2"),
                },
            ],
        )
        notifier = _NotifierStub()
        manager = PositionProtectionManager(client, lambda message: None, notifier=notifier)

        session_id = manager.start(
            _make_credentials(),
            _make_strategy_config(),
            OptionProtectionConfig(
                option_inst_id=option_inst_id,
                trigger_inst_id=spot_inst_id,
                trigger_price_type="last",
                direction="short",
                pos_side="short",
                take_profit_trigger=Decimal("98000"),
                stop_loss_trigger=Decimal("100000"),
                take_profit_order_mode="mark_with_slippage",
                take_profit_order_price=None,
                take_profit_slippage=Decimal("0.0002"),
                stop_loss_order_mode="mark_with_slippage",
                stop_loss_order_price=None,
                stop_loss_slippage=Decimal("0.0003"),
                poll_seconds=0.01,
                trigger_label=f"{spot_inst_id} last",
            ),
        )

        worker = manager._workers[session_id]
        assert worker.thread is not None
        worker.thread.join(timeout=1)

        self.assertFalse(worker.thread.is_alive())
        self.assertEqual(client.current_position, Decimal("0"))
        self.assertEqual(len(client.orders), 2)
        self.assertEqual([order["side"] for order in client.orders], ["buy", "buy"])
        self.assertEqual([order["price"] for order in client.orders], [Decimal("0.0153"), Decimal("0.0153")])
        self.assertIn((spot_inst_id, "last"), client.price_requests)
        self.assertIn((option_inst_id, "mark"), client.price_requests)
        self.assertTrue(any("止损" in subject for subject, _ in notifier.messages))

    def test_manager_can_be_stopped_without_triggering_order(self) -> None:
        option_inst_id = "BTC-USD-20260327-70000-C"
        client = _SimulatedProtectionClient(
            initial_position=_make_option_position(inst_id=option_inst_id, position="2", pos_side="long"),
            trigger_prices={
                (option_inst_id, "mark"): [Decimal("0.0140")],
            },
            order_results=[],
        )
        manager = PositionProtectionManager(client, lambda message: None, notifier=None)

        session_id = manager.start(
            _make_credentials(),
            _make_strategy_config(),
            OptionProtectionConfig(
                option_inst_id=option_inst_id,
                trigger_inst_id=option_inst_id,
                trigger_price_type="mark",
                direction="long",
                pos_side="long",
                take_profit_trigger=Decimal("0.0200"),
                stop_loss_trigger=Decimal("0.0100"),
                take_profit_order_mode="fixed_price",
                take_profit_order_price=Decimal("0.0195"),
                take_profit_slippage=Decimal("0"),
                stop_loss_order_mode="fixed_price",
                stop_loss_order_price=Decimal("0.0095"),
                stop_loss_slippage=Decimal("0"),
                poll_seconds=0.01,
                trigger_label=f"{option_inst_id} mark",
            ),
        )

        sleep(0.03)
        manager.stop(session_id)
        worker = manager._workers[session_id]
        assert worker.thread is not None
        worker.thread.join(timeout=1)

        self.assertFalse(worker.thread.is_alive())
        self.assertEqual(client.current_position, Decimal("2"))
        self.assertEqual(client.orders, [])

    def test_manager_enters_error_state_when_close_order_cannot_fill(self) -> None:
        option_inst_id = "BTC-USD-20260327-70000-C"
        client = _SimulatedProtectionClient(
            initial_position=_make_option_position(inst_id=option_inst_id, position="2", pos_side="long"),
            trigger_prices={
                (option_inst_id, "mark"): [Decimal("0.0155")],
            },
            order_results=[
                {
                    "state": "canceled",
                    "filled_size": Decimal("0"),
                    "avg_price": None,
                    "price": Decimal("0.0152"),
                    "size": Decimal("2"),
                }
            ],
        )
        notifier = _NotifierStub()
        manager = PositionProtectionManager(client, lambda message: None, notifier=notifier)

        session_id = manager.start(
            _make_credentials(),
            _make_strategy_config(),
            OptionProtectionConfig(
                option_inst_id=option_inst_id,
                trigger_inst_id=option_inst_id,
                trigger_price_type="mark",
                direction="long",
                pos_side="long",
                take_profit_trigger=Decimal("0.0150"),
                stop_loss_trigger=None,
                take_profit_order_mode="fixed_price",
                take_profit_order_price=Decimal("0.0152"),
                take_profit_slippage=Decimal("0"),
                stop_loss_order_mode="fixed_price",
                stop_loss_order_price=None,
                stop_loss_slippage=Decimal("0"),
                poll_seconds=0.01,
                trigger_label=f"{option_inst_id} mark",
            ),
        )

        worker = manager._workers[session_id]
        assert worker.thread is not None
        worker.thread.join(timeout=1)

        self.assertFalse(worker.thread.is_alive())
        self.assertEqual(len(client.orders), 1)
        self.assertEqual(client.current_position, Decimal("2"))
        self.assertTrue(any("异常" in subject for subject, _ in notifier.messages))
        self.assertIn("异常", worker.status)

    def test_manager_errors_when_remaining_position_is_below_min_size(self) -> None:
        option_inst_id = "BTC-USD-20260327-70000-C"
        client = _SimulatedProtectionClient(
            initial_position=_make_option_position(inst_id=option_inst_id, position="1.5", pos_side="long"),
            trigger_prices={
                (option_inst_id, "mark"): [Decimal("0.0155")],
            },
            order_results=[
                {
                    "state": "partially_filled",
                    "filled_size": Decimal("1"),
                    "avg_price": Decimal("0.0152"),
                    "price": Decimal("0.0152"),
                    "size": Decimal("1"),
                }
            ],
            option_lot_size=Decimal("1"),
            option_min_size=Decimal("1"),
        )
        notifier = _NotifierStub()
        manager = PositionProtectionManager(client, lambda message: None, notifier=notifier)

        session_id = manager.start(
            _make_credentials(),
            _make_strategy_config(),
            OptionProtectionConfig(
                option_inst_id=option_inst_id,
                trigger_inst_id=option_inst_id,
                trigger_price_type="mark",
                direction="long",
                pos_side="long",
                take_profit_trigger=Decimal("0.0150"),
                stop_loss_trigger=None,
                take_profit_order_mode="fixed_price",
                take_profit_order_price=Decimal("0.0152"),
                take_profit_slippage=Decimal("0"),
                stop_loss_order_mode="fixed_price",
                stop_loss_order_price=None,
                stop_loss_slippage=Decimal("0"),
                poll_seconds=0.01,
                trigger_label=f"{option_inst_id} mark",
            ),
        )

        worker = manager._workers[session_id]
        assert worker.thread is not None
        worker.thread.join(timeout=1)

        self.assertFalse(worker.thread.is_alive())
        self.assertEqual(len(client.orders), 1)
        self.assertEqual(client.current_position, Decimal("0.5"))
        self.assertTrue(any("异常" in subject for subject, _ in notifier.messages))

    def test_list_sessions_keeps_finished_worker_until_manual_clear(self) -> None:
        option_inst_id = "BTC-USD-20260327-70000-C"
        client = _SimulatedProtectionClient(
            initial_position=_make_option_position(inst_id=option_inst_id, position="2", pos_side="long"),
            trigger_prices={
                (option_inst_id, "mark"): [Decimal("0.0155")],
            },
            order_results=[
                {
                    "state": "filled",
                    "filled_size": Decimal("2"),
                    "avg_price": Decimal("0.0152"),
                    "price": Decimal("0.0152"),
                    "size": Decimal("2"),
                }
            ],
        )
        manager = PositionProtectionManager(client, lambda message: None, notifier=None)
        session_id = manager.start(
            _make_credentials(),
            _make_strategy_config(),
            OptionProtectionConfig(
                option_inst_id=option_inst_id,
                trigger_inst_id=option_inst_id,
                trigger_price_type="mark",
                direction="long",
                pos_side="long",
                take_profit_trigger=Decimal("0.0150"),
                stop_loss_trigger=None,
                take_profit_order_mode="fixed_price",
                take_profit_order_price=Decimal("0.0152"),
                take_profit_slippage=Decimal("0"),
                stop_loss_order_mode="fixed_price",
                stop_loss_order_price=None,
                stop_loss_slippage=Decimal("0"),
                poll_seconds=0.01,
                trigger_label=f"{option_inst_id} mark",
            ),
        )

        worker = manager._workers[session_id]
        assert worker.thread is not None
        worker.thread.join(timeout=1)

        self.assertFalse(worker.thread.is_alive())
        self.assertEqual(len(manager._workers), 1)
        sessions = manager.list_sessions()
        self.assertEqual(len(sessions), 1)
        self.assertEqual(sessions[0].session_id, session_id)
        self.assertEqual(len(manager._workers), 1)

    def test_clear_finished_removes_stopped_worker(self) -> None:
        option_inst_id = "BTC-USD-20260327-70000-C"
        client = _SimulatedProtectionClient(
            initial_position=_make_option_position(inst_id=option_inst_id, position="2", pos_side="long"),
            trigger_prices={
                (option_inst_id, "mark"): [Decimal("0.0140")],
            },
            order_results=[],
        )
        manager = PositionProtectionManager(client, lambda message: None, notifier=None)
        session_id = manager.start(
            _make_credentials(),
            _make_strategy_config(),
            OptionProtectionConfig(
                option_inst_id=option_inst_id,
                trigger_inst_id=option_inst_id,
                trigger_price_type="mark",
                direction="long",
                pos_side="long",
                take_profit_trigger=Decimal("0.0200"),
                stop_loss_trigger=Decimal("0.0100"),
                take_profit_order_mode="fixed_price",
                take_profit_order_price=Decimal("0.0195"),
                take_profit_slippage=Decimal("0"),
                stop_loss_order_mode="fixed_price",
                stop_loss_order_price=Decimal("0.0095"),
                stop_loss_slippage=Decimal("0"),
                poll_seconds=0.01,
                trigger_label=f"{option_inst_id} mark",
            ),
        )

        sleep(0.03)
        manager.stop(session_id)
        worker = manager._workers[session_id]
        assert worker.thread is not None
        worker.thread.join(timeout=1)

        self.assertFalse(worker.thread.is_alive())
        self.assertEqual(manager.clear_finished(), 1)
        self.assertEqual(len(manager._workers), 0)
