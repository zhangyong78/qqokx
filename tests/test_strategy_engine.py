import http.client
from decimal import Decimal
from unittest import TestCase
from unittest.mock import patch

from okx_quant.engine import (
    OKX_DYNAMIC_STOP_MONITOR_MAX_READ_FAILURES,
    FilledPosition,
    ManagedEntryOrder,
    StartupSignalGateState,
    StrategyEngine,
    _advance_dynamic_stop_live,
    _format_size_with_contract_equivalent,
    _idle_signal_wait_seconds,
    _is_exchange_dynamic_stop_candidate_valid,
    _should_skip_startup_signal,
    build_order_plan,
    can_use_exchange_managed_orders,
    determine_order_size,
    fixed_entry_side_mode_support_reason,
    supports_fixed_entry_side_mode,
    validate_entry_side_mode_support,
)
from okx_quant.models import Candle, Instrument, SignalDecision, StrategyConfig
from okx_quant.okx_client import OkxApiError, OkxOrderResult, OkxOrderStatus, OkxTradeOrderItem
from okx_quant.strategies.ema_atr import EmaAtrStrategy
from okx_quant.strategy_catalog import (
    STRATEGY_CROSS_ID,
    STRATEGY_DYNAMIC_ID,
    STRATEGY_DYNAMIC_LONG_ID,
    STRATEGY_DYNAMIC_SHORT_ID,
    STRATEGY_EMA5_EMA8_ID,
)


class StrategyEngineTest(TestCase):
    def _make_candles(self, closes: list[str]) -> list[Candle]:
        candles: list[Candle] = []
        previous_close = Decimal(closes[0])
        for index, raw_close in enumerate(closes, start=1):
            close = Decimal(raw_close)
            open_price = previous_close
            high = max(open_price, close) + Decimal("1")
            low = min(open_price, close) - Decimal("1")
            candles.append(Candle(index, open_price, high, low, close, Decimal("1"), True))
            previous_close = close
        return candles

    def test_determine_order_size_uses_contract_value_for_linear_swap_risk_amount(self) -> None:
        instrument = Instrument(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
            settle_ccy="USDT",
            ct_val=Decimal("0.01"),
            ct_mult=Decimal("1"),
            ct_val_ccy="BTC",
        )
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="4H",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="mark",
            risk_amount=Decimal("20"),
        )

        size = determine_order_size(
            instrument=instrument,
            config=config,
            entry_price=Decimal("77628.1"),
            stop_loss=Decimal("76401.8"),
            risk_price_compatible=True,
        )

        self.assertEqual(size, Decimal("1.63"))

    def test_determine_order_size_without_contract_value_keeps_spot_style_risk_math(self) -> None:
        instrument = Instrument(
            inst_id="ETH-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
        )
        config = StrategyConfig(
            inst_id="ETH-USDT-SWAP",
            bar="1H",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="mark",
            risk_amount=Decimal("20"),
        )

        size = determine_order_size(
            instrument=instrument,
            config=config,
            entry_price=Decimal("2400"),
            stop_loss=Decimal("2398"),
            risk_price_compatible=True,
        )

        self.assertEqual(size, Decimal("10"))

    def test_format_size_with_contract_equivalent_for_linear_swap(self) -> None:
        instrument = Instrument(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
            settle_ccy="USDT",
            ct_val=Decimal("0.01"),
            ct_mult=Decimal("1"),
            ct_val_ccy="BTC",
        )

        text = _format_size_with_contract_equivalent(instrument, Decimal("1.63"))

        self.assertEqual(text, "1.63\u5f20\uff08\u6298\u54080.0163 BTC\uff09")

    def test_trade_fill_pnl_text_for_close_uses_contract_value_multiplier(self) -> None:
        position = FilledPosition(
            ord_id="ord-1",
            cl_ord_id="cl-1",
            inst_id="BTC-USDT-SWAP",
            side="buy",
            close_side="sell",
            pos_side="long",
            size=Decimal("1.63"),
            entry_price=Decimal("77330.4"),
            entry_ts=0,
            price_delta_multiplier=Decimal("0.01"),
        )

        trade_pnl = StrategyEngine._trade_fill_pnl_text_for_close(
            position,
            fill_size=Decimal("1.63"),
            fill_price=Decimal("76390"),
        )

        self.assertEqual(trade_pnl, "-15.32852")

    def test_long_signal_is_detected(self) -> None:
        candles = [
            Candle(1, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True),
            Candle(2, Decimal("100"), Decimal("101"), Decimal("98"), Decimal("99"), Decimal("1"), True),
            Candle(3, Decimal("99"), Decimal("100"), Decimal("97"), Decimal("98"), Decimal("1"), True),
            Candle(4, Decimal("98"), Decimal("99"), Decimal("95"), Decimal("96"), Decimal("1"), True),
            Candle(5, Decimal("96"), Decimal("106"), Decimal("95"), Decimal("104"), Decimal("1"), True),
        ]
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=2,
            trend_ema_period=2,
            big_ema_period=3,
            atr_period=2,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="both",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
        )
        decision = EmaAtrStrategy().evaluate(candles, config)
        self.assertEqual(decision.signal, "long")
        self.assertIsNotNone(decision.atr_value)

    def test_cross_long_signal_is_blocked_when_below_ema55(self) -> None:
        candles = self._make_candles(["100"] * 60 + ["50", "80"])
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=2,
            trend_ema_period=5,
            big_ema_period=6,
            atr_period=2,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="both",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
        )

        decision = EmaAtrStrategy().evaluate(candles, config)

        self.assertIsNone(decision.signal)
        self.assertIn("EMA6", decision.reason)

    def test_order_plan_builds_tp_and_sl(self) -> None:
        instrument = Instrument(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
        )
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=14,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="both",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id=STRATEGY_DYNAMIC_ID,
        )
        plan = build_order_plan(
            instrument=instrument,
            config=config,
            order_size=Decimal("2"),
            signal="long",
            entry_reference=Decimal("2500"),
            atr_value=Decimal("10"),
            candle_ts=1,
        )
        self.assertEqual(plan.side, "buy")
        self.assertEqual(plan.pos_side, "long")
        self.assertEqual(plan.stop_loss, Decimal("2480"))
        self.assertEqual(plan.take_profit, Decimal("2540"))

    def test_dynamic_long_strategy_can_use_okx托管止盈止损(self) -> None:
        instrument = Instrument(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
        )
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="4H",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        )

        self.assertTrue(can_use_exchange_managed_orders(config, instrument, instrument))

    def test_dynamic_short_strategy_can_use_okx托管止盈止损(self) -> None:
        instrument = Instrument(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
        )
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="4H",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="short_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id=STRATEGY_DYNAMIC_SHORT_ID,
        )

        self.assertTrue(can_use_exchange_managed_orders(config, instrument, instrument))


    def test_dynamic_strategy_cannot_use_okx_when_symbols_differ(self) -> None:
        signal_instrument = Instrument(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
        )
        trade_instrument = Instrument(
            inst_id="ETH-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
        )
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="4H",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        )

        self.assertFalse(can_use_exchange_managed_orders(config, signal_instrument, trade_instrument))

    def test_fixed_entry_side_mode_only_supported_in_local_trade_mode(self) -> None:
        self.assertTrue(supports_fixed_entry_side_mode(STRATEGY_DYNAMIC_ID, "trade", "local_trade"))
        self.assertFalse(supports_fixed_entry_side_mode(STRATEGY_DYNAMIC_ID, "trade", "exchange"))
        self.assertFalse(supports_fixed_entry_side_mode(STRATEGY_DYNAMIC_ID, "signal_only", "local_trade"))
        self.assertFalse(supports_fixed_entry_side_mode(STRATEGY_EMA5_EMA8_ID, "trade", "local_trade"))

    def test_fixed_entry_side_mode_support_reason_describes_okx_managed_lock(self) -> None:
        reason = fixed_entry_side_mode_support_reason(STRATEGY_DYNAMIC_ID, "trade", "exchange")
        self.assertIsNotNone(reason)
        self.assertIn("OKX 托管模式当前只支持跟随信号", str(reason))

    def test_validate_entry_side_mode_support_rejects_fixed_sell_in_okx_managed_mode(self) -> None:
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="1H",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id=STRATEGY_DYNAMIC_ID,
            tp_sl_mode="exchange",
            entry_side_mode="fixed_sell",
        )

        with self.assertRaises(RuntimeError) as ctx:
            validate_entry_side_mode_support(config)

        self.assertIn("只支持跟随信号", str(ctx.exception))

    def test_validate_entry_side_mode_support_allows_fixed_sell_in_local_trade_mode(self) -> None:
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="1H",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id=STRATEGY_DYNAMIC_ID,
            tp_sl_mode="local_trade",
            entry_side_mode="fixed_sell",
        )

        validate_entry_side_mode_support(config)

    def test_dynamic_live_stop_locks_1r_at_2r(self) -> None:
        stop_loss, next_take_profit, next_trigger_r, moved = _advance_dynamic_stop_live(
            direction="long",
            current_price=Decimal("120.1"),
            entry_price=Decimal("100"),
            risk_per_unit=Decimal("10"),
            current_stop_loss=Decimal("90"),
            next_trigger_r=2,
            tick_size=Decimal("0.1"),
        )

        self.assertTrue(moved)
        self.assertEqual(stop_loss, Decimal("110.1"))
        self.assertEqual(next_take_profit, Decimal("130.1"))
        self.assertEqual(next_trigger_r, 3)

    def test_dynamic_live_stop_locks_2r_at_3r(self) -> None:
        stop_loss, next_take_profit, next_trigger_r, moved = _advance_dynamic_stop_live(
            direction="long",
            current_price=Decimal("130.1"),
            entry_price=Decimal("100"),
            risk_per_unit=Decimal("10"),
            current_stop_loss=Decimal("110.1"),
            next_trigger_r=3,
            tick_size=Decimal("0.1"),
        )

        self.assertTrue(moved)
        self.assertEqual(stop_loss, Decimal("120.1"))
        self.assertEqual(next_take_profit, Decimal("140.1"))
        self.assertEqual(next_trigger_r, 4)

    def test_dynamic_live_stop_can_move_to_break_even_plus_two_taker_fees_at_2r(self) -> None:
        stop_loss, next_take_profit, next_trigger_r, moved = _advance_dynamic_stop_live(
            direction="long",
            current_price=Decimal("120.1"),
            entry_price=Decimal("100"),
            risk_per_unit=Decimal("10"),
            current_stop_loss=Decimal("90"),
            next_trigger_r=2,
            tick_size=Decimal("0.1"),
            two_r_break_even=True,
        )

        self.assertTrue(moved)
        self.assertEqual(stop_loss, Decimal("100.1"))
        self.assertEqual(next_take_profit, Decimal("130.1"))
        self.assertEqual(next_trigger_r, 3)

    def test_engine_client_order_id_uses_ascii_only_tokens(self) -> None:
        engine = StrategyEngine(
            None,  # type: ignore[arg-type]
            lambda *_: None,
            strategy_name="EMA 动态委托-多头",
            session_id="S01-测试",
        )

        client_order_id = engine._next_client_order_id(role="entry")

        self.assertTrue(client_order_id.isascii())
        self.assertRegex(client_order_id, r"^[a-z0-9]+$")
        self.assertLessEqual(len(client_order_id), 32)

    def test_idle_signal_wait_seconds_caps_hourly_polling_to_one_minute(self) -> None:
        self.assertEqual(_idle_signal_wait_seconds("1H", 10, now_ts=1000.0), 60.0)

    def test_idle_signal_wait_seconds_keeps_upcoming_close_window(self) -> None:
        self.assertEqual(_idle_signal_wait_seconds("1H", 10, now_ts=3590.0), 20.0)

    def test_startup_signal_gate_skips_old_signal_when_window_disabled(self) -> None:
        gate = StartupSignalGateState(started_at_ms=180_000, chase_window_seconds=0)

        should_skip, message = _should_skip_startup_signal(
            gate,
            signal="long",
            candle_ts=60_000,
            bar="1m",
        )

        self.assertTrue(should_skip)
        self.assertIn("启动默认不追老信号", message or "")
        self.assertEqual(gate.blocked_signal, "long")

        repeated_skip, repeated_message = _should_skip_startup_signal(
            gate,
            signal="long",
            candle_ts=120_000,
            bar="1m",
        )
        self.assertTrue(repeated_skip)
        self.assertIsNone(repeated_message)

    def test_startup_signal_gate_allows_fresh_signal_within_chase_window(self) -> None:
        gate = StartupSignalGateState(started_at_ms=180_000, chase_window_seconds=45)

        should_skip, message = _should_skip_startup_signal(
            gate,
            signal="long",
            candle_ts=90_000,
            bar="1m",
        )

        self.assertFalse(should_skip)
        self.assertIn("启动追单窗口内接管当前波段", message or "")
        self.assertIsNone(gate.blocked_signal)

    def test_dynamic_exchange_strategy_continues_after_round_trip_and_respects_wave_limit(self) -> None:
        messages: list[str] = []
        waits: list[float] = []
        submit_calls = {"count": 0}
        candles = self._make_candles([str(2000 + index) for index in range(80)])

        class _StopStub:
            def __init__(self) -> None:
                self._stopped = False
                self._wait_calls = 0

            def is_set(self) -> bool:
                return self._stopped

            def wait(self, timeout: float) -> bool:
                waits.append(timeout)
                self._wait_calls += 1
                if self._wait_calls >= 2:
                    self._stopped = True
                return self._stopped

        engine = StrategyEngine(
            None,  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托-多头",
            session_id="S01",
        )
        engine._stop_event = _StopStub()  # type: ignore[assignment]
        engine._log_strategy_start = lambda *args, **kwargs: None  # type: ignore[assignment]
        engine._log_hourly_debug = lambda *args, **kwargs: None  # type: ignore[assignment]
        engine._get_candles_with_retry = lambda *args, **kwargs: candles  # type: ignore[assignment]
        engine._monitor_exchange_managed_position_until_closed = lambda *args, **kwargs: None  # type: ignore[assignment]

        def _fake_submit(*_args, **_kwargs) -> OkxOrderResult:  # noqa: ANN001
            submit_calls["count"] += 1
            return OkxOrderResult(
                ord_id="ord-live-1",
                cl_ord_id="cl-live-1",
                s_code="0",
                s_msg="accepted",
                raw={},
            )

        engine._submit_order_with_recovery = _fake_submit  # type: ignore[assignment]
        engine._get_order_with_retry = lambda *args, **kwargs: OkxOrderStatus(  # type: ignore[assignment]
            ord_id="ord-live-1",
            state="filled",
            side="buy",
            ord_type="limit",
            price=Decimal("2300"),
            avg_price=Decimal("2299"),
            size=Decimal("0.01"),
            filled_size=Decimal("0.01"),
            raw={},
        )

        config = StrategyConfig(
            inst_id="ETH-USDT-SWAP",
            bar="1m",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.01"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            poll_seconds=10,
            risk_amount=Decimal("10"),
            max_entries_per_trend=1,
            take_profit_mode="fixed",
        )
        instrument = Instrument(
            inst_id="ETH-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
        )

        def _fake_evaluate(_strategy, _candles, _config):  # noqa: ANN001
            return SignalDecision(
                signal="long",
                reason="趋势成立",
                candle_ts=_candles[-1].ts,
                entry_reference=Decimal("2300"),
                atr_value=Decimal("10"),
                ema_value=Decimal("2310"),
            )

        with patch("okx_quant.engine.time.time", return_value=0.0), patch(
            "okx_quant.engine.EmaDynamicOrderStrategy.evaluate",
            new=_fake_evaluate,
        ):
            engine._run_dynamic_exchange_strategy(None, config, instrument)  # type: ignore[arg-type]

        self.assertEqual(submit_calls["count"], 1)
        self.assertTrue(any("本轮持仓已结束，继续监控下一次信号" in message for message in messages))
        self.assertTrue(any("第1波趋势开仓次数已达上限" in message for message in messages))

    def test_dynamic_exchange_strategy_transfers_to_position_monitor_when_cancel_lookup_finds_fill(self) -> None:
        messages: list[str] = []
        submit_calls = {"count": 0}
        captured_position: dict[str, object] = {}
        first_candles = self._make_candles([str(2000 + index) for index in range(80)])
        second_candles = self._make_candles([str(2000 + index) for index in range(81)])

        class _StopStub:
            def __init__(self) -> None:
                self._stopped = False

            def is_set(self) -> bool:
                return self._stopped

            def wait(self, timeout: float) -> bool:  # noqa: ARG002
                return self._stopped

        class _StubClient:
            @staticmethod
            def cancel_order(credentials, config, *, inst_id: str, ord_id=None, cl_ord_id=None):  # noqa: ANN001
                raise OkxApiError("操作全部失败")

            @staticmethod
            def get_order(credentials, config, *, inst_id: str, ord_id=None, cl_ord_id=None):  # noqa: ANN001
                return OkxOrderStatus(
                    ord_id=ord_id or "ord-live-2",
                    state="filled",
                    side="sell",
                    ord_type="limit",
                    price=Decimal("2315.32"),
                    avg_price=Decimal("2315.11"),
                    size=Decimal("0.01"),
                    filled_size=Decimal("0.01"),
                    raw={},
                )

        stop_stub = _StopStub()
        engine = StrategyEngine(
            _StubClient(),  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托-空头",
            session_id="S01",
        )
        engine._stop_event = stop_stub  # type: ignore[assignment]
        engine._log_strategy_start = lambda *args, **kwargs: None  # type: ignore[assignment]
        engine._log_hourly_debug = lambda *args, **kwargs: None  # type: ignore[assignment]

        candle_batches = iter([first_candles, second_candles])
        engine._get_candles_with_retry = lambda *args, **kwargs: next(candle_batches)  # type: ignore[assignment]
        engine._get_order_with_retry = lambda *args, **kwargs: OkxOrderStatus(  # type: ignore[assignment]
            ord_id="ord-live-2",
            state="live",
            side="sell",
            ord_type="limit",
            price=Decimal("2315.32"),
            avg_price=None,
            size=Decimal("0.01"),
            filled_size=Decimal("0"),
            raw={},
        )

        def _fake_submit(*_args, **_kwargs) -> OkxOrderResult:  # noqa: ANN001
            submit_calls["count"] += 1
            return OkxOrderResult(
                ord_id="ord-live-2",
                cl_ord_id="cl-live-2",
                s_code="0",
                s_msg="accepted",
                raw={},
            )

        def _fake_monitor(*_args, position: FilledPosition, **_kwargs) -> None:  # noqa: ANN001
            captured_position["ord_id"] = position.ord_id
            captured_position["entry_price"] = position.entry_price
            captured_position["size"] = position.size
            stop_stub._stopped = True

        engine._submit_order_with_recovery = _fake_submit  # type: ignore[assignment]
        engine._monitor_exchange_managed_position_until_closed = _fake_monitor  # type: ignore[assignment]

        config = StrategyConfig(
            inst_id="ETH-USDT-SWAP",
            bar="1m",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.01"),
            trade_mode="cross",
            signal_mode="short_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
            strategy_id=STRATEGY_DYNAMIC_SHORT_ID,
            poll_seconds=10,
            max_entries_per_trend=1,
            take_profit_mode="fixed",
        )
        instrument = Instrument(
            inst_id="ETH-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
        )

        def _fake_evaluate(_strategy, _candles, _config):  # noqa: ANN001
            return SignalDecision(
                signal="short",
                reason="趋势成立",
                candle_ts=_candles[-1].ts,
                entry_reference=Decimal("2315.32"),
                atr_value=Decimal("10"),
                ema_value=Decimal("2310"),
            )

        with patch("okx_quant.engine.time.time", return_value=0.0), patch(
            "okx_quant.engine.EmaDynamicOrderStrategy.evaluate",
            new=_fake_evaluate,
        ):
            engine._run_dynamic_exchange_strategy(None, config, instrument)  # type: ignore[arg-type]

        self.assertEqual(submit_calls["count"], 1)
        self.assertEqual(captured_position["ord_id"], "ord-live-2")
        self.assertEqual(captured_position["entry_price"], Decimal("2315.11"))
        self.assertEqual(captured_position["size"], Decimal("0.01"))
        self.assertTrue(any("旧挂单在撤单前已成交，转入持仓监控" in message for message in messages))

    def test_dynamic_exchange_strategy_trader_virtual_mode_skips_exchange_stop_and_uses_virtual_monitor(self) -> None:
        messages: list[str] = []
        submit_calls = {"count": 0}
        place_limit_kwargs: dict[str, object] = {}
        captured_monitor: dict[str, object] = {}
        candles = self._make_candles([str(2000 + index) for index in range(80)])

        class _StopStub:
            def __init__(self) -> None:
                self._stopped = False

            def is_set(self) -> bool:
                return self._stopped

            def wait(self, timeout: float) -> bool:  # noqa: ARG002
                return self._stopped

        class _StubClient:
            def place_limit_order(
                self,
                credentials,
                config,
                plan,
                *,
                cl_ord_id=None,
                include_take_profit=True,
                stop_loss_algo_cl_ord_id=None,
                include_attached_protection=True,
            ):  # noqa: ANN001
                place_limit_kwargs["cl_ord_id"] = cl_ord_id
                place_limit_kwargs["include_take_profit"] = include_take_profit
                place_limit_kwargs["stop_loss_algo_cl_ord_id"] = stop_loss_algo_cl_ord_id
                place_limit_kwargs["include_attached_protection"] = include_attached_protection
                return OkxOrderResult(
                    ord_id="ord-virtual-1",
                    cl_ord_id=cl_ord_id,
                    s_code="0",
                    s_msg="accepted",
                    raw={},
                )

        stop_stub = _StopStub()
        engine = StrategyEngine(
            _StubClient(),  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托-多头",
            session_id="S01",
        )
        engine._stop_event = stop_stub  # type: ignore[assignment]
        engine._log_strategy_start = lambda *args, **kwargs: None  # type: ignore[assignment]
        engine._log_hourly_debug = lambda *args, **kwargs: None  # type: ignore[assignment]
        engine._get_candles_with_retry = lambda *args, **kwargs: candles  # type: ignore[assignment]

        def _fake_submit(*_args, submit_fn, **_kwargs):  # noqa: ANN001
            submit_calls["count"] += 1
            return submit_fn()

        def _fake_virtual_monitor(
            *_args,
            trade_instrument: Instrument,
            position: FilledPosition,
            initial_stop_loss: Decimal,
            take_profit: Decimal,
            dynamic_take_profit_enabled: bool,
            **_kwargs,
        ) -> None:  # noqa: ANN001
            captured_monitor["inst_id"] = trade_instrument.inst_id
            captured_monitor["ord_id"] = position.ord_id
            captured_monitor["entry_price"] = position.entry_price
            captured_monitor["initial_stop_loss"] = initial_stop_loss
            captured_monitor["take_profit"] = take_profit
            captured_monitor["dynamic_take_profit_enabled"] = dynamic_take_profit_enabled
            stop_stub._stopped = True

        engine._submit_order_with_recovery = _fake_submit  # type: ignore[assignment]
        engine._monitor_trader_virtual_position = _fake_virtual_monitor  # type: ignore[assignment]
        engine._monitor_exchange_dynamic_stop = lambda *args, **kwargs: self.fail("should not use OKX dynamic stop")  # type: ignore[assignment]
        engine._get_order_with_retry = lambda *args, **kwargs: OkxOrderStatus(  # type: ignore[assignment]
            ord_id="ord-virtual-1",
            state="filled",
            side="buy",
            ord_type="limit",
            price=Decimal("2300"),
            avg_price=Decimal("2299"),
            size=Decimal("0.01"),
            filled_size=Decimal("0.01"),
            raw={},
        )

        config = StrategyConfig(
            inst_id="ETH-USDT-SWAP",
            bar="1m",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.01"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            poll_seconds=10,
            max_entries_per_trend=1,
            take_profit_mode="dynamic",
            trader_virtual_stop_loss=True,
        )
        instrument = Instrument(
            inst_id="ETH-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
        )

        def _fake_evaluate(_strategy, _candles, _config):  # noqa: ANN001
            return SignalDecision(
                signal="long",
                reason="趋势成立",
                candle_ts=_candles[-1].ts,
                entry_reference=Decimal("2300"),
                atr_value=Decimal("10"),
                ema_value=Decimal("2310"),
            )

        with patch("okx_quant.engine.time.time", return_value=0.0), patch(
            "okx_quant.engine.EmaDynamicOrderStrategy.evaluate",
            new=_fake_evaluate,
        ):
            engine._run_dynamic_exchange_strategy(None, config, instrument)  # type: ignore[arg-type]

        self.assertEqual(submit_calls["count"], 1)
        self.assertFalse(place_limit_kwargs["include_attached_protection"])
        self.assertFalse(place_limit_kwargs["include_take_profit"])
        self.assertIsNone(place_limit_kwargs["stop_loss_algo_cl_ord_id"])
        self.assertEqual(captured_monitor["inst_id"], "ETH-USDT-SWAP")
        self.assertEqual(captured_monitor["ord_id"], "ord-virtual-1")
        self.assertEqual(captured_monitor["entry_price"], Decimal("2299"))
        self.assertEqual(captured_monitor["initial_stop_loss"], Decimal("2280"))
        self.assertEqual(captured_monitor["take_profit"], Decimal("2340"))
        self.assertTrue(captured_monitor["dynamic_take_profit_enabled"])
        self.assertTrue(any("交易员虚拟止损只记触发" in message for message in messages))

    def test_dynamic_exchange_strategy_logs_no_signal_only_once_per_candle(self) -> None:
        messages: list[str] = []
        waits: list[float] = []
        evaluate_calls = 0
        candles = self._make_candles([str(2000 + index) for index in range(80)])

        class _StopStub:
            def __init__(self) -> None:
                self._stopped = False
                self._wait_calls = 0

            def is_set(self) -> bool:
                return self._stopped

            def wait(self, timeout: float) -> bool:
                waits.append(timeout)
                self._wait_calls += 1
                if self._wait_calls >= 2:
                    self._stopped = True
                return self._stopped

        engine = StrategyEngine(
            None,  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托-多头",
            session_id="S01",
        )
        engine._stop_event = _StopStub()  # type: ignore[assignment]
        engine._log_strategy_start = lambda *args, **kwargs: None  # type: ignore[assignment]
        engine._log_hourly_debug = lambda *args, **kwargs: None  # type: ignore[assignment]
        engine._get_candles_with_retry = lambda *args, **kwargs: candles  # type: ignore[assignment]

        config = StrategyConfig(
            inst_id="ETH-USDT-SWAP",
            bar="1H",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.01"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            poll_seconds=10,
            risk_amount=Decimal("10"),
        )
        instrument = Instrument(
            inst_id="ETH-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
        )

        def _fake_evaluate(_strategy, _candles, _config):  # noqa: ANN001
            nonlocal evaluate_calls
            evaluate_calls += 1
            return SignalDecision(
                signal=None,
                reason="EMA21 仍在 EMA55 下方，当前不是有效多头趋势。",
                candle_ts=_candles[-1].ts,
                entry_reference=None,
                atr_value=Decimal("10"),
                ema_value=Decimal("2000"),
            )

        with patch("okx_quant.engine.time.time", return_value=1000.0), patch(
            "okx_quant.engine.EmaDynamicOrderStrategy.evaluate",
            new=_fake_evaluate,
        ):
            engine._run_dynamic_exchange_strategy(None, config, instrument)  # type: ignore[arg-type]

        self.assertEqual(evaluate_calls, 1)
        self.assertEqual(sum("当前无法生成挂单" in message for message in messages), 1)
        self.assertEqual(waits, [60.0, 60.0])

    def test_dynamic_exchange_strategy_accepts_fixed_size_without_risk_amount(self) -> None:
        messages: list[str] = []
        waits: list[float] = []
        submit_calls = {"count": 0}
        candles = self._make_candles([str(2000 + index) for index in range(80)])

        class _StopStub:
            def __init__(self) -> None:
                self._stopped = False
                self._wait_calls = 0

            def is_set(self) -> bool:
                return self._stopped

            def wait(self, timeout: float) -> bool:
                waits.append(timeout)
                self._wait_calls += 1
                if self._wait_calls >= 2:
                    self._stopped = True
                return self._stopped

        engine = StrategyEngine(
            None,  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托-多头",
            session_id="S01",
        )
        engine._stop_event = _StopStub()  # type: ignore[assignment]
        engine._log_strategy_start = lambda *args, **kwargs: None  # type: ignore[assignment]
        engine._log_hourly_debug = lambda *args, **kwargs: None  # type: ignore[assignment]
        engine._get_candles_with_retry = lambda *args, **kwargs: candles  # type: ignore[assignment]
        engine._monitor_exchange_managed_position_until_closed = lambda *args, **kwargs: None  # type: ignore[assignment]

        def _fake_submit(*_args, **_kwargs) -> OkxOrderResult:  # noqa: ANN001
            submit_calls["count"] += 1
            return OkxOrderResult(
                ord_id="ord-live-fixed-1",
                cl_ord_id="cl-live-fixed-1",
                s_code="0",
                s_msg="accepted",
                raw={},
            )

        engine._submit_order_with_recovery = _fake_submit  # type: ignore[assignment]
        engine._get_order_with_retry = lambda *args, **kwargs: OkxOrderStatus(  # type: ignore[assignment]
            ord_id="ord-live-fixed-1",
            state="filled",
            side="buy",
            ord_type="limit",
            price=Decimal("2300"),
            avg_price=Decimal("2299"),
            size=Decimal("0.01"),
            filled_size=Decimal("0.01"),
            raw={},
        )

        config = StrategyConfig(
            inst_id="ETH-USDT-SWAP",
            bar="1m",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.01"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            poll_seconds=10,
            risk_amount=None,
            max_entries_per_trend=1,
            take_profit_mode="fixed",
        )
        instrument = Instrument(
            inst_id="ETH-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
        )

        def _fake_evaluate(_strategy, _candles, _config):  # noqa: ANN001
            return SignalDecision(
                signal="long",
                reason="趋势成立",
                candle_ts=_candles[-1].ts,
                entry_reference=Decimal("2300"),
                atr_value=Decimal("10"),
                ema_value=Decimal("2310"),
            )

        with patch("okx_quant.engine.time.time", return_value=0.0), patch(
            "okx_quant.engine.EmaDynamicOrderStrategy.evaluate",
            new=_fake_evaluate,
        ):
            engine._run_dynamic_exchange_strategy(None, config, instrument)  # type: ignore[arg-type]

        self.assertEqual(submit_calls["count"], 1)
        self.assertTrue(any("固定数量=0.01" in message for message in messages))

    def test_dynamic_local_strategy_v2_logs_no_signal_only_once_per_candle(self) -> None:
        messages: list[str] = []
        waits: list[float] = []
        evaluate_calls = 0
        candles = self._make_candles([str(2000 + index) for index in range(80)])

        class _StopStub:
            def __init__(self) -> None:
                self._stopped = False
                self._wait_calls = 0

            def is_set(self) -> bool:
                return self._stopped

            def wait(self, timeout: float) -> bool:
                waits.append(timeout)
                self._wait_calls += 1
                if self._wait_calls >= 2:
                    self._stopped = True
                return self._stopped

        engine = StrategyEngine(
            None,  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托-多头",
            session_id="S01",
        )
        engine._stop_event = _StopStub()  # type: ignore[assignment]
        engine._log_strategy_start = lambda *args, **kwargs: None  # type: ignore[assignment]
        engine._log_local_mode_summary = lambda *args, **kwargs: None  # type: ignore[assignment]
        engine._log_hourly_debug = lambda *args, **kwargs: None  # type: ignore[assignment]
        engine._get_candles_with_retry = lambda *args, **kwargs: candles  # type: ignore[assignment]

        config = StrategyConfig(
            inst_id="ETH-USDT-SWAP",
            bar="1H",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.01"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            poll_seconds=10,
            max_entries_per_trend=1,
        )
        instrument = Instrument(
            inst_id="ETH-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
        )

        def _fake_evaluate(_strategy, _candles, _config):  # noqa: ANN001
            nonlocal evaluate_calls
            evaluate_calls += 1
            return SignalDecision(
                signal=None,
                reason="EMA21 仍在 EMA55 下方，当前不是有效多头趋势。",
                candle_ts=_candles[-1].ts,
                entry_reference=None,
                atr_value=Decimal("10"),
                ema_value=Decimal("2000"),
            )

        with patch("okx_quant.engine.time.time", return_value=1000.0), patch(
            "okx_quant.engine.EmaDynamicOrderStrategy.evaluate",
            new=_fake_evaluate,
        ):
            engine._run_dynamic_local_strategy_v2(
                None,  # type: ignore[arg-type]
                config,
                instrument,
                instrument,
            )

        self.assertEqual(evaluate_calls, 1)
        self.assertEqual(sum("当前无法生成动态开仓价" in message for message in messages), 1)
        self.assertEqual(waits, [60.0, 60.0])

    def test_okx_read_retry_recovers_from_transient_error(self) -> None:
        messages: list[str] = []
        waits: list[float] = []

        class _StopStub:
            @staticmethod
            def is_set() -> bool:
                return False

            @staticmethod
            def wait(timeout: float) -> bool:
                waits.append(timeout)
                return False

        engine = StrategyEngine(
            None,  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托-多头",
            session_id="S01",
        )
        engine._stop_event = _StopStub()  # type: ignore[assignment]

        attempts = {"count": 0}

        def _flaky_read() -> str:
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise OkxApiError("SSL handshake timed out")
            return "ok"

        result = engine._call_okx_read_with_retry("读取订单状态", _flaky_read)

        self.assertEqual(result, "ok")
        self.assertEqual(attempts["count"], 3)
        self.assertEqual(waits, [1.0, 2.0])
        self.assertTrue(any("准备重试" in message for message in messages))

    def test_okx_read_retry_recovers_from_raw_timeout_error(self) -> None:
        messages: list[str] = []
        waits: list[float] = []

        class _StopStub:
            @staticmethod
            def is_set() -> bool:
                return False

            @staticmethod
            def wait(timeout: float) -> bool:
                waits.append(timeout)
                return False

        engine = StrategyEngine(
            None,  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托-多头",
            session_id="S01",
        )
        engine._stop_event = _StopStub()  # type: ignore[assignment]

        attempts = {"count": 0}

        def _flaky_read() -> str:
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise TimeoutError("The read operation timed out")
            return "ok"

        result = engine._call_okx_read_with_retry("读取触发价格", _flaky_read)

        self.assertEqual(result, "ok")
        self.assertEqual(attempts["count"], 3)
        self.assertEqual(waits, [1.0, 2.0])
        self.assertGreaterEqual(len(messages), 2)

    def test_okx_read_retry_recovers_from_remote_end_closed_connection(self) -> None:
        messages: list[str] = []
        waits: list[float] = []

        class _StopStub:
            @staticmethod
            def is_set() -> bool:
                return False

            @staticmethod
            def wait(timeout: float) -> bool:
                waits.append(timeout)
                return False

        engine = StrategyEngine(
            None,  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托-多头",
            session_id="S01",
        )
        engine._stop_event = _StopStub()  # type: ignore[assignment]

        attempts = {"count": 0}

        def _flaky_read() -> str:
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise http.client.RemoteDisconnected("Remote end closed connection without response")
            return "ok"

        result = engine._call_okx_read_with_retry("读取触发价格", _flaky_read)

        self.assertEqual(result, "ok")
        self.assertEqual(attempts["count"], 3)
        self.assertEqual(waits, [1.0, 2.0])
        self.assertGreaterEqual(len(messages), 2)

    def test_okx_read_retry_does_not_retry_non_transient_error(self) -> None:
        messages: list[str] = []
        waits: list[float] = []

        class _StopStub:
            @staticmethod
            def is_set() -> bool:
                return False

            @staticmethod
            def wait(timeout: float) -> bool:
                waits.append(timeout)
                return False

        engine = StrategyEngine(
            None,  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托-多头",
            session_id="S01",
        )
        engine._stop_event = _StopStub()  # type: ignore[assignment]

        with self.assertRaises(OkxApiError):
            engine._call_okx_read_with_retry(
                "读取订单状态",
                lambda: (_ for _ in ()).throw(OkxApiError("参数错误", code="51000")),
            )

        self.assertEqual(waits, [])
        self.assertTrue(any("OKX 读取失败" in message for message in messages))

    def test_log_hourly_debug_retries_transient_okx_errors(self) -> None:
        messages: list[str] = []
        waits: list[float] = []
        candles = self._make_candles([str(100 + index) for index in range(80)])

        class _StopStub:
            @staticmethod
            def is_set() -> bool:
                return False

            @staticmethod
            def wait(timeout: float) -> bool:
                waits.append(timeout)
                return False

        class _StubClient:
            def __init__(self) -> None:
                self.calls = 0

            def get_candles(self, inst_id: str, bar: str, *, limit: int):  # noqa: ANN001
                self.calls += 1
                if self.calls < 3:
                    raise OkxApiError("SSL handshake timed out")
                return candles[-limit:]

        client = _StubClient()
        engine = StrategyEngine(
            client,  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托-多头",
            session_id="S01",
        )
        engine._stop_event = _StopStub()  # type: ignore[assignment]

        engine._log_hourly_debug(
            "ETH-USDT-SWAP",
            21,
            current_bar="1H",
            trend_ema_period=55,
            entry_reference_ema_period=55,
        )

        self.assertEqual(client.calls, 3)
        self.assertEqual(waits, [1.0, 2.0])
        self.assertTrue(any("OKX 读取异常，准备重试" in message for message in messages))
        self.assertTrue(any("1H调试 | ETH-USDT-SWAP" in message for message in messages))
        self.assertFalse(any("1H调试值获取失败" in message for message in messages))

    def test_log_hourly_debug_skips_reference_logs_for_non_1h_bar(self) -> None:
        messages: list[str] = []

        class _StubClient:
            def __init__(self) -> None:
                self.calls = 0

            def get_candles(self, inst_id: str, bar: str, *, limit: int):  # noqa: ANN001
                self.calls += 1
                raise AssertionError("non-1H bar should not fetch hourly debug candles")

        client = _StubClient()
        engine = StrategyEngine(
            client,  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托多头",
            session_id="S01",
        )

        engine._log_hourly_debug(
            "ETH-USDT-SWAP",
            21,
            current_bar="1m",
            trend_ema_period=55,
            entry_reference_ema_period=55,
        )

        self.assertEqual(client.calls, 0)
        self.assertEqual(messages, [])

    def test_exchange_dynamic_stop_monitor_keeps_running_after_transient_read_failures(self) -> None:
        messages: list[str] = []
        waits: list[float] = []

        class _StopStub:
            @staticmethod
            def is_set() -> bool:
                return False

            @staticmethod
            def wait(timeout: float) -> bool:
                waits.append(timeout)
                return False

        instrument = Instrument(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
        )
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="1H",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.01"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="mark",
        )
        position = FilledPosition(
            ord_id="ord-1",
            cl_ord_id="cl-1",
            inst_id="BTC-USDT-SWAP",
            side="buy",
            close_side="sell",
            pos_side="long",
            size=Decimal("0.01"),
            entry_price=Decimal("75000"),
            entry_ts=0,
        )
        engine = StrategyEngine(
            None,  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托-多头",
            session_id="S01",
        )
        engine._stop_event = _StopStub()  # type: ignore[assignment]

        attempts = {"count": 0}

        def _find_position(*_args, **_kwargs):  # noqa: ANN001
            attempts["count"] += 1
            if attempts["count"] <= 2:
                raise OkxApiError("The read operation timed out")
            return None

        engine._find_managed_position = _find_position  # type: ignore[method-assign]

        engine._monitor_exchange_dynamic_stop(
            None,  # type: ignore[arg-type]
            config,
            trade_instrument=instrument,
            position=position,
            initial_stop_loss=Decimal("73000"),
            stop_loss_algo_cl_ord_id="algo-1",
        )

        self.assertEqual(attempts["count"], 3)
        self.assertEqual(waits, [max(config.poll_seconds, 1.0), max(config.poll_seconds, 1.0)])
        self.assertTrue(any("The read operation timed out" in message and "/6" in message for message in messages))
        self.assertFalse(any("策略停止" in message for message in messages))

    def test_trader_virtual_position_monitor_logs_loss_trigger_without_closing(self) -> None:
        messages: list[str] = []
        waits: list[float] = []
        close_reasons: list[str] = []

        class _StopStub:
            def __init__(self) -> None:
                self._stopped = False

            def is_set(self) -> bool:
                return self._stopped

            def wait(self, timeout: float) -> bool:
                waits.append(timeout)
                self._stopped = True
                return True

        instrument = Instrument(
            inst_id="ETH-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
        )
        config = StrategyConfig(
            inst_id="ETH-USDT-SWAP",
            bar="1m",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.01"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            poll_seconds=10,
        )
        position = FilledPosition(
            ord_id="ord-trader-1",
            cl_ord_id="cl-trader-1",
            inst_id="ETH-USDT-SWAP",
            side="buy",
            close_side="sell",
            pos_side="long",
            size=Decimal("0.01"),
            entry_price=Decimal("100"),
            entry_ts=0,
        )
        engine = StrategyEngine(
            None,  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托-多头",
            session_id="S01",
        )
        engine._stop_event = _StopStub()  # type: ignore[assignment]
        engine._find_managed_position = lambda *_args, **_kwargs: object()  # type: ignore[assignment]
        engine._get_trigger_price_with_retry = lambda *_args, **_kwargs: Decimal("89")  # type: ignore[assignment]

        def _fake_close_position(*args, **kwargs):  # noqa: ANN001
            close_reasons.append(str(kwargs.get("reason") or args[4]))

        engine._close_position = _fake_close_position  # type: ignore[assignment]

        engine._monitor_trader_virtual_position(
            None,  # type: ignore[arg-type]
            config,
            trade_instrument=instrument,
            position=position,
            initial_stop_loss=Decimal("90"),
            take_profit=Decimal("140"),
            dynamic_take_profit_enabled=False,
        )

        self.assertEqual(close_reasons, [])
        self.assertEqual(waits, [config.poll_seconds])
        self.assertTrue(any("交易员虚拟止损已触发（不平仓）" in message for message in messages))

    def test_trader_virtual_position_monitor_closes_when_dynamic_protection_is_hit(self) -> None:
        messages: list[str] = []
        waits: list[float] = []
        close_reasons: list[str] = []
        prices = iter([Decimal("120"), Decimal("100")])

        class _StopStub:
            @staticmethod
            def is_set() -> bool:
                return False

            @staticmethod
            def wait(timeout: float) -> bool:
                waits.append(timeout)
                return False

        instrument = Instrument(
            inst_id="ETH-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
        )
        config = StrategyConfig(
            inst_id="ETH-USDT-SWAP",
            bar="1m",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.01"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            poll_seconds=10,
            dynamic_two_r_break_even=True,
            dynamic_fee_offset_enabled=False,
        )
        position = FilledPosition(
            ord_id="ord-trader-2",
            cl_ord_id="cl-trader-2",
            inst_id="ETH-USDT-SWAP",
            side="buy",
            close_side="sell",
            pos_side="long",
            size=Decimal("0.01"),
            entry_price=Decimal("100"),
            entry_ts=0,
        )
        engine = StrategyEngine(
            None,  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托-多头",
            session_id="S01",
        )
        engine._stop_event = _StopStub()  # type: ignore[assignment]
        engine._find_managed_position = lambda *_args, **_kwargs: object()  # type: ignore[assignment]
        engine._get_trigger_price_with_retry = lambda *_args, **_kwargs: next(prices)  # type: ignore[assignment]

        def _fake_close_position(*args, **kwargs):  # noqa: ANN001
            close_reasons.append(str(kwargs.get("reason") or args[4]))

        engine._close_position = _fake_close_position  # type: ignore[assignment]

        with patch("okx_quant.engine.time.time", side_effect=[0.0, 60.0]):
            engine._monitor_trader_virtual_position(
                None,  # type: ignore[arg-type]
                config,
                trade_instrument=instrument,
                position=position,
                initial_stop_loss=Decimal("90"),
                take_profit=Decimal("140"),
                dynamic_take_profit_enabled=True,
            )

        self.assertEqual(close_reasons, ["动态止盈"])
        self.assertEqual(waits, [config.poll_seconds])
        self.assertTrue(any("交易员动态止盈保护价已上移" in message for message in messages))
        self.assertTrue(any("交易员动态止盈保护价触发" in message for message in messages))

    def test_exchange_dynamic_stop_monitor_stops_after_consecutive_read_failures(self) -> None:
        messages: list[str] = []
        waits: list[float] = []

        class _StopStub:
            @staticmethod
            def is_set() -> bool:
                return False

            @staticmethod
            def wait(timeout: float) -> bool:
                waits.append(timeout)
                return False

        instrument = Instrument(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
        )
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="1H",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.01"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="mark",
        )
        position = FilledPosition(
            ord_id="ord-1",
            cl_ord_id="cl-1",
            inst_id="BTC-USDT-SWAP",
            side="buy",
            close_side="sell",
            pos_side="long",
            size=Decimal("0.01"),
            entry_price=Decimal("75000"),
            entry_ts=0,
        )
        engine = StrategyEngine(
            None,  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托-多头",
            session_id="S01",
        )
        engine._stop_event = _StopStub()  # type: ignore[assignment]
        engine._find_managed_position = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: (_ for _ in ()).throw(OkxApiError("The read operation timed out"))
        )

        with self.assertRaises(RuntimeError) as ctx:
            engine._monitor_exchange_dynamic_stop(
                None,  # type: ignore[arg-type]
                config,
                trade_instrument=instrument,
                position=position,
                initial_stop_loss=Decimal("73000"),
                stop_loss_algo_cl_ord_id="algo-1",
            )

        self.assertIn("The read operation timed out", str(ctx.exception))
        self.assertIn(str(OKX_DYNAMIC_STOP_MONITOR_MAX_READ_FAILURES), str(ctx.exception))
        self.assertEqual(len(waits), OKX_DYNAMIC_STOP_MONITOR_MAX_READ_FAILURES - 1)
        self.assertEqual(
            sum("The read operation timed out" in message and "/6" in message for message in messages),
            OKX_DYNAMIC_STOP_MONITOR_MAX_READ_FAILURES - 1,
        )

    def test_place_entry_order_recovers_when_write_response_is_lost(self) -> None:
        messages: list[str] = []
        waits: list[float] = []

        class _StopStub:
            @staticmethod
            def is_set() -> bool:
                return False

            @staticmethod
            def wait(timeout: float) -> bool:
                waits.append(timeout)
                return False

        class _StubClient:
            def __init__(self) -> None:
                self.place_calls = 0
                self.cl_ord_ids: list[str | None] = []

            def place_simple_order(self, credentials, config, *, inst_id: str, side: str, size: Decimal, ord_type: str, pos_side=None, price=None, cl_ord_id=None):  # noqa: ANN001,E501
                self.place_calls += 1
                self.cl_ord_ids.append(cl_ord_id)
                raise OkxApiError("SSL handshake timed out")

            def get_order(self, credentials, config, *, inst_id: str, ord_id=None, cl_ord_id=None):  # noqa: ANN001
                return OkxOrderStatus(
                    ord_id="ord-recovered-1",
                    state="live",
                    side="buy",
                    ord_type="market",
                    price=Decimal("2300"),
                    avg_price=None,
                    size=Decimal("0.01"),
                    filled_size=Decimal("0"),
                    raw={"clOrdId": cl_ord_id},
                )

        client = _StubClient()
        engine = StrategyEngine(
            client,  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托-多头",
            session_id="S01",
        )
        engine._stop_event = _StopStub()  # type: ignore[assignment]
        config = StrategyConfig(
            inst_id="ETH-USDT-SWAP",
            bar="1m",
            ema_period=3,
            trend_ema_period=5,
            big_ema_period=233,
            atr_period=3,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.01"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
        )
        instrument = Instrument(
            inst_id="ETH-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
        )

        result = engine._place_entry_order(
            None,  # type: ignore[arg-type]
            config,
            instrument,
            "buy",
            Decimal("0.01"),
            "long",
        )

        self.assertEqual(result.ord_id, "ord-recovered-1")
        self.assertEqual(result.cl_ord_id, client.cl_ord_ids[0])
        self.assertEqual(client.place_calls, 1)
        self.assertEqual(waits, [])
        self.assertTrue(any("回查确认委托已落地" in message for message in messages))

    def test_place_entry_order_retries_with_same_client_order_id_after_reconcile_miss(self) -> None:
        messages: list[str] = []
        waits: list[float] = []

        class _StopStub:
            @staticmethod
            def is_set() -> bool:
                return False

            @staticmethod
            def wait(timeout: float) -> bool:
                waits.append(timeout)
                return False

        class _StubClient:
            def __init__(self) -> None:
                self.place_calls = 0
                self.cl_ord_ids: list[str | None] = []

            def place_simple_order(self, credentials, config, *, inst_id: str, side: str, size: Decimal, ord_type: str, pos_side=None, price=None, cl_ord_id=None):  # noqa: ANN001,E501
                self.place_calls += 1
                self.cl_ord_ids.append(cl_ord_id)
                if self.place_calls == 1:
                    raise OkxApiError("SSL handshake timed out")
                return OkxOrderResult(
                    ord_id="ord-retry-1",
                    cl_ord_id=cl_ord_id,
                    s_code="0",
                    s_msg="accepted",
                    raw={},
                )

            def get_order(self, credentials, config, *, inst_id: str, ord_id=None, cl_ord_id=None):  # noqa: ANN001
                raise OkxApiError(f"OKX 未返回订单状态：{cl_ord_id}")

        client = _StubClient()
        engine = StrategyEngine(
            client,  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托-多头",
            session_id="S01",
        )
        engine._stop_event = _StopStub()  # type: ignore[assignment]
        config = StrategyConfig(
            inst_id="ETH-USDT-SWAP",
            bar="1m",
            ema_period=3,
            trend_ema_period=5,
            big_ema_period=233,
            atr_period=3,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.01"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
        )
        instrument = Instrument(
            inst_id="ETH-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
        )

        result = engine._place_entry_order(
            None,  # type: ignore[arg-type]
            config,
            instrument,
            "buy",
            Decimal("0.01"),
            "long",
        )

        self.assertEqual(result.ord_id, "ord-retry-1")
        self.assertEqual(client.place_calls, 2)
        self.assertEqual(client.cl_ord_ids[0], client.cl_ord_ids[1])
        self.assertEqual(waits, [1.0, 2.0])
        self.assertTrue(any("准备使用同一 clOrdId 补发一次" in message for message in messages))

    def test_cancel_active_order_recovers_when_cancel_response_is_lost(self) -> None:
        messages: list[str] = []
        waits: list[float] = []

        class _StopStub:
            @staticmethod
            def is_set() -> bool:
                return False

            @staticmethod
            def wait(timeout: float) -> bool:
                waits.append(timeout)
                return False

        class _StubClient:
            @staticmethod
            def cancel_order(credentials, config, *, inst_id: str, ord_id=None, cl_ord_id=None):  # noqa: ANN001
                raise OkxApiError("SSL handshake timed out")

            @staticmethod
            def get_order(credentials, config, *, inst_id: str, ord_id=None, cl_ord_id=None):  # noqa: ANN001
                return OkxOrderStatus(
                    ord_id=ord_id or "ord-cancel-1",
                    state="canceled",
                    side="buy",
                    ord_type="limit",
                    price=Decimal("2300"),
                    avg_price=None,
                    size=Decimal("0.01"),
                    filled_size=Decimal("0"),
                    raw={},
                )

        engine = StrategyEngine(
            _StubClient(),  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托-多头",
            session_id="S01",
        )
        engine._stop_event = _StopStub()  # type: ignore[assignment]
        config = StrategyConfig(
            inst_id="ETH-USDT-SWAP",
            bar="1m",
            ema_period=3,
            trend_ema_period=5,
            big_ema_period=233,
            atr_period=3,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.01"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
        )
        active_order = ManagedEntryOrder(
            ord_id="ord-cancel-1",
            cl_ord_id="cl-cancel-1",
            candle_ts=1,
            entry_reference=Decimal("2300"),
            stop_loss=Decimal("2200"),
            take_profit=Decimal("2400"),
            stop_loss_algo_cl_ord_id=None,
            size=Decimal("0.01"),
            side="buy",
            signal="long",
        )

        cancel_result = engine._cancel_active_order(
            None,  # type: ignore[arg-type]
            config,
            active_order,
            1,
        )

        self.assertEqual(cancel_result.action, "canceled")
        self.assertEqual(waits, [])
        self.assertTrue(any("已确认撤单" in message for message in messages))

    def test_cancel_active_order_keeps_tracking_when_cancel_failure_leaves_live_order(self) -> None:
        messages: list[str] = []
        waits: list[float] = []

        class _StopStub:
            @staticmethod
            def is_set() -> bool:
                return False

            @staticmethod
            def wait(timeout: float) -> bool:
                waits.append(timeout)
                return False

        class _StubClient:
            cancel_calls = 0

            @classmethod
            def cancel_order(cls, credentials, config, *, inst_id: str, ord_id=None, cl_ord_id=None):  # noqa: ANN001
                cls.cancel_calls += 1
                raise OkxApiError("操作全部失败")

            @staticmethod
            def get_order(credentials, config, *, inst_id: str, ord_id=None, cl_ord_id=None):  # noqa: ANN001
                return OkxOrderStatus(
                    ord_id=ord_id or "ord-cancel-2",
                    state="live",
                    side="buy",
                    ord_type="limit",
                    price=Decimal("2300"),
                    avg_price=None,
                    size=Decimal("0.01"),
                    filled_size=Decimal("0"),
                    raw={},
                )

        engine = StrategyEngine(
            _StubClient(),  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托多头",
            session_id="S01",
        )
        engine._stop_event = _StopStub()  # type: ignore[assignment]
        config = StrategyConfig(
            inst_id="ETH-USDT-SWAP",
            bar="1m",
            ema_period=3,
            trend_ema_period=5,
            big_ema_period=233,
            atr_period=3,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.01"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
        )
        active_order = ManagedEntryOrder(
            ord_id="ord-cancel-2",
            cl_ord_id="cl-cancel-2",
            candle_ts=1,
            entry_reference=Decimal("2300"),
            stop_loss=Decimal("2200"),
            take_profit=Decimal("2400"),
            stop_loss_algo_cl_ord_id=None,
            size=Decimal("0.01"),
            side="buy",
            signal="long",
        )

        cancel_result = engine._cancel_active_order(
            None,  # type: ignore[arg-type]
            config,
            active_order,
            1,
        )

        self.assertEqual(cancel_result.action, "pending")
        self.assertEqual(_StubClient.cancel_calls, 2)
        self.assertEqual(waits, [])
        self.assertTrue(any("保留旧挂单继续回查" in message for message in messages))

    def test_cancel_active_order_keeps_tracking_when_order_not_found_after_cancel_failure(self) -> None:
        messages: list[str] = []
        waits: list[float] = []

        class _StopStub:
            @staticmethod
            def is_set() -> bool:
                return False

            @staticmethod
            def wait(timeout: float) -> bool:
                waits.append(timeout)
                return False

        class _StubClient:
            cancel_calls = 0

            @classmethod
            def cancel_order(cls, credentials, config, *, inst_id: str, ord_id=None, cl_ord_id=None):  # noqa: ANN001
                cls.cancel_calls += 1
                raise OkxApiError("操作全部失败")

            @staticmethod
            def get_order(credentials, config, *, inst_id: str, ord_id=None, cl_ord_id=None):  # noqa: ANN001
                raise OkxApiError("订单不存在")

            @staticmethod
            def get_pending_orders(credentials, *, environment: str, inst_types: tuple[str, ...], limit: int):  # noqa: ANN001
                return []

        engine = StrategyEngine(
            _StubClient(),  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托多头",
            session_id="S01",
        )
        engine._stop_event = _StopStub()  # type: ignore[assignment]
        config = StrategyConfig(
            inst_id="ETH-USDT-SWAP",
            bar="1m",
            ema_period=3,
            trend_ema_period=5,
            big_ema_period=233,
            atr_period=3,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.01"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
        )
        active_order = ManagedEntryOrder(
            ord_id="ord-cancel-3",
            cl_ord_id="cl-cancel-3",
            candle_ts=1,
            entry_reference=Decimal("2300"),
            stop_loss=Decimal("2200"),
            take_profit=Decimal("2400"),
            stop_loss_algo_cl_ord_id=None,
            size=Decimal("0.01"),
            side="buy",
            signal="long",
        )

        cancel_result = engine._cancel_active_order(
            None,  # type: ignore[arg-type]
            config,
            active_order,
            1,
        )

        self.assertEqual(cancel_result.action, "pending")
        self.assertEqual(_StubClient.cancel_calls, 2)
        self.assertEqual(waits, [])
        self.assertTrue(any("挂单列表已找不到旧单" in message for message in messages))

    def test_cancel_active_order_returns_filled_when_cancel_lookup_shows_fill(self) -> None:
        messages: list[str] = []

        class _StopStub:
            @staticmethod
            def is_set() -> bool:
                return False

            @staticmethod
            def wait(timeout: float) -> bool:  # noqa: ARG002
                return False

        class _StubClient:
            @staticmethod
            def cancel_order(credentials, config, *, inst_id: str, ord_id=None, cl_ord_id=None):  # noqa: ANN001
                raise OkxApiError("操作全部失败")

            @staticmethod
            def get_order(credentials, config, *, inst_id: str, ord_id=None, cl_ord_id=None):  # noqa: ANN001
                return OkxOrderStatus(
                    ord_id=ord_id or "ord-cancel-4",
                    state="filled",
                    side="sell",
                    ord_type="limit",
                    price=Decimal("2315.32"),
                    avg_price=Decimal("2315.11"),
                    size=Decimal("0.01"),
                    filled_size=Decimal("0.01"),
                    raw={},
                )

        engine = StrategyEngine(
            _StubClient(),  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托空头",
            session_id="S01",
        )
        engine._stop_event = _StopStub()  # type: ignore[assignment]
        config = StrategyConfig(
            inst_id="ETH-USDT-SWAP",
            bar="1m",
            ema_period=3,
            trend_ema_period=5,
            big_ema_period=233,
            atr_period=3,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.01"),
            trade_mode="cross",
            signal_mode="short_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
        )
        active_order = ManagedEntryOrder(
            ord_id="ord-cancel-4",
            cl_ord_id="cl-cancel-4",
            candle_ts=1,
            entry_reference=Decimal("2315.32"),
            stop_loss=Decimal("2319.19"),
            take_profit=Decimal("2275.32"),
            stop_loss_algo_cl_ord_id=None,
            size=Decimal("0.01"),
            side="sell",
            signal="short",
        )

        cancel_result = engine._cancel_active_order(
            None,  # type: ignore[arg-type]
            config,
            active_order,
            1,
        )

        self.assertEqual(cancel_result.action, "filled")
        self.assertIsNotNone(cancel_result.status)
        self.assertEqual(cancel_result.status.state, "filled")

    def test_amend_algo_order_recovers_when_pending_stop_is_already_updated(self) -> None:
        messages: list[str] = []
        waits: list[float] = []
        target_stop = Decimal("2200")

        class _StopStub:
            @staticmethod
            def is_set() -> bool:
                return False

            @staticmethod
            def wait(timeout: float) -> bool:
                waits.append(timeout)
                return False

        class _StubClient:
            @staticmethod
            def amend_algo_order(*args, **kwargs):  # noqa: ANN002,ANN003
                raise OkxApiError("SSL handshake timed out")

            @staticmethod
            def get_pending_orders(credentials, *, environment: str, inst_types: tuple[str, ...], limit: int):  # noqa: ANN001
                return [
                    OkxTradeOrderItem(
                        source_kind="algo",
                        source_label="算法委托",
                        created_time=1,
                        update_time=2,
                        inst_id="ETH-USDT-SWAP",
                        inst_type="SWAP",
                        side="sell",
                        pos_side="long",
                        td_mode="cross",
                        ord_type="conditional",
                        state="live",
                        price=None,
                        size=None,
                        filled_size=None,
                        avg_price=None,
                        order_id=None,
                        algo_id="algo-1",
                        client_order_id=None,
                        algo_client_order_id="algo-cl-1",
                        pnl=None,
                        fee=None,
                        fee_currency=None,
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
                        stop_loss_trigger_price=target_stop,
                        stop_loss_order_price=Decimal("-1"),
                        stop_loss_trigger_price_type="last",
                        raw={},
                    )
                ]

        engine = StrategyEngine(
            _StubClient(),  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托-多头",
            session_id="S01",
        )
        engine._stop_event = _StopStub()  # type: ignore[assignment]
        config = StrategyConfig(
            inst_id="ETH-USDT-SWAP",
            bar="1m",
            ema_period=3,
            trend_ema_period=5,
            big_ema_period=233,
            atr_period=3,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.01"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
        )
        instrument = Instrument(
            inst_id="ETH-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
        )

        engine._amend_algo_order_with_recovery(
            None,  # type: ignore[arg-type]
            config,
            trade_instrument=instrument,
            algo_id="algo-1",
            algo_cl_ord_id="algo-cl-1",
            req_id="req-1",
            new_stop_loss_trigger_price=target_stop,
            new_stop_loss_trigger_price_type="last",
        )

        self.assertEqual(waits, [])
        self.assertTrue(any("回查显示已生效" in message for message in messages))

    def test_find_pending_algo_order_by_client_id_matches_target_algo(self) -> None:
        target_order = OkxTradeOrderItem(
            source_kind="algo",
            source_label="算法委托",
            created_time=1,
            update_time=2,
            inst_id="ETH-USDT-SWAP",
            inst_type="SWAP",
            side="sell",
            pos_side="long",
            td_mode="cross",
            ord_type="conditional",
            state="live",
            price=None,
            size=None,
            filled_size=None,
            avg_price=None,
            order_id=None,
            algo_id="algo-1",
            client_order_id=None,
            algo_client_order_id="algo-cl-1",
            pnl=None,
            fee=None,
            fee_currency=None,
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
            stop_loss_trigger_price=Decimal("2200"),
            stop_loss_order_price=Decimal("-1"),
            stop_loss_trigger_price_type="last",
            raw={},
        )

        class _StubClient:
            @staticmethod
            def get_pending_orders(*args, **kwargs):
                return [
                    OkxTradeOrderItem(
                        source_kind="normal",
                        source_label="普通委托",
                        created_time=1,
                        update_time=1,
                        inst_id="ETH-USDT-SWAP",
                        inst_type="SWAP",
                        side="buy",
                        pos_side="long",
                        td_mode="cross",
                        ord_type="limit",
                        state="live",
                        price=Decimal("2300"),
                        size=Decimal("0.02"),
                        filled_size=Decimal("0"),
                        avg_price=None,
                        order_id="ord-1",
                        algo_id=None,
                        client_order_id="cl-1",
                        algo_client_order_id=None,
                        pnl=None,
                        fee=None,
                        fee_currency=None,
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
                    ),
                    target_order,
                ]

        engine = StrategyEngine(
            _StubClient(),  # type: ignore[arg-type]
            lambda *_: None,
            strategy_name="EMA 动态委托-多头",
            session_id="S01",
        )
        config = StrategyConfig(
            inst_id="ETH-USDT-SWAP",
            bar="1m",
            ema_period=3,
            trend_ema_period=5,
            big_ema_period=233,
            atr_period=3,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.02"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
        )
        instrument = Instrument(
            inst_id="ETH-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
        )

        matched = engine._find_pending_algo_order_by_client_id(
            None,  # type: ignore[arg-type]
            config,
            trade_instrument=instrument,
            algo_cl_ord_id="algo-cl-1",
        )

        self.assertIsNotNone(matched)
        self.assertEqual(matched.algo_id, "algo-1")

    def test_dynamic_live_break_even_mode_is_mirrored_for_short(self) -> None:
        stop_loss, next_take_profit, next_trigger_r, moved = _advance_dynamic_stop_live(
            direction="short",
            current_price=Decimal("79.9"),
            entry_price=Decimal("100"),
            risk_per_unit=Decimal("10"),
            current_stop_loss=Decimal("110"),
            next_trigger_r=2,
            tick_size=Decimal("0.1"),
            two_r_break_even=True,
        )

        self.assertTrue(moved)
        self.assertEqual(stop_loss, Decimal("99.9"))
        self.assertEqual(next_take_profit, Decimal("69.9"))
        self.assertEqual(next_trigger_r, 3)

    def test_dynamic_live_break_even_can_disable_fee_offset(self) -> None:
        stop_loss, next_take_profit, next_trigger_r, moved = _advance_dynamic_stop_live(
            direction="long",
            current_price=Decimal("120"),
            entry_price=Decimal("100"),
            risk_per_unit=Decimal("10"),
            current_stop_loss=Decimal("90"),
            next_trigger_r=2,
            tick_size=Decimal("0.1"),
            two_r_break_even=True,
            dynamic_fee_offset_enabled=False,
        )

        self.assertTrue(moved)
        self.assertEqual(stop_loss, Decimal("100"))
        self.assertEqual(next_take_profit, Decimal("130"))
        self.assertEqual(next_trigger_r, 3)

    def test_dynamic_live_time_stop_break_even_moves_long_stop_after_threshold(self) -> None:
        stop_loss, next_take_profit, next_trigger_r, moved = _advance_dynamic_stop_live(
            direction="long",
            current_price=Decimal("100.2"),
            entry_price=Decimal("100"),
            risk_per_unit=Decimal("10"),
            current_stop_loss=Decimal("90"),
            next_trigger_r=2,
            tick_size=Decimal("0.1"),
            holding_bars=10,
            time_stop_break_even_enabled=True,
            time_stop_break_even_bars=10,
        )

        self.assertTrue(moved)
        self.assertEqual(stop_loss, Decimal("100.1"))
        self.assertEqual(next_take_profit, Decimal("120.1"))
        self.assertEqual(next_trigger_r, 2)

    def test_dynamic_live_time_stop_break_even_moves_short_stop_after_threshold(self) -> None:
        stop_loss, next_take_profit, next_trigger_r, moved = _advance_dynamic_stop_live(
            direction="short",
            current_price=Decimal("99.8"),
            entry_price=Decimal("100"),
            risk_per_unit=Decimal("10"),
            current_stop_loss=Decimal("110"),
            next_trigger_r=2,
            tick_size=Decimal("0.1"),
            holding_bars=10,
            time_stop_break_even_enabled=True,
            time_stop_break_even_bars=10,
        )

        self.assertTrue(moved)
        self.assertEqual(stop_loss, Decimal("99.9"))
        self.assertEqual(next_take_profit, Decimal("79.9"))
        self.assertEqual(next_trigger_r, 2)

    def test_dynamic_live_time_stop_break_even_waits_for_bar_threshold(self) -> None:
        stop_loss, next_take_profit, next_trigger_r, moved = _advance_dynamic_stop_live(
            direction="long",
            current_price=Decimal("100.2"),
            entry_price=Decimal("100"),
            risk_per_unit=Decimal("10"),
            current_stop_loss=Decimal("90"),
            next_trigger_r=2,
            tick_size=Decimal("0.1"),
            holding_bars=9,
            time_stop_break_even_enabled=True,
            time_stop_break_even_bars=10,
        )

        self.assertFalse(moved)
        self.assertEqual(stop_loss, Decimal("90"))
        self.assertEqual(next_take_profit, Decimal("120.1"))
        self.assertEqual(next_trigger_r, 2)

    def test_dynamic_live_time_stop_break_even_never_retrogrades_existing_stop(self) -> None:
        stop_loss, next_take_profit, next_trigger_r, moved = _advance_dynamic_stop_live(
            direction="long",
            current_price=Decimal("100.2"),
            entry_price=Decimal("100"),
            risk_per_unit=Decimal("10"),
            current_stop_loss=Decimal("105"),
            next_trigger_r=2,
            tick_size=Decimal("0.1"),
            holding_bars=10,
            time_stop_break_even_enabled=True,
            time_stop_break_even_bars=10,
        )

        self.assertFalse(moved)
        self.assertEqual(stop_loss, Decimal("105"))
        self.assertEqual(next_take_profit, Decimal("120.1"))
        self.assertEqual(next_trigger_r, 2)

    def test_exchange_dynamic_stop_candidate_for_long_requires_price_above_stop(self) -> None:
        self.assertTrue(
            _is_exchange_dynamic_stop_candidate_valid(
                direction="long",
                current_price=Decimal("100.1"),
                candidate_stop_loss=Decimal("100"),
            )
        )
        self.assertFalse(
            _is_exchange_dynamic_stop_candidate_valid(
                direction="long",
                current_price=Decimal("100"),
                candidate_stop_loss=Decimal("100"),
            )
        )
        self.assertFalse(
            _is_exchange_dynamic_stop_candidate_valid(
                direction="long",
                current_price=Decimal("99.9"),
                candidate_stop_loss=Decimal("100"),
            )
        )

    def test_exchange_dynamic_stop_candidate_for_short_requires_price_below_stop(self) -> None:
        self.assertTrue(
            _is_exchange_dynamic_stop_candidate_valid(
                direction="short",
                current_price=Decimal("99.9"),
                candidate_stop_loss=Decimal("100"),
            )
        )
        self.assertFalse(
            _is_exchange_dynamic_stop_candidate_valid(
                direction="short",
                current_price=Decimal("100"),
                candidate_stop_loss=Decimal("100"),
            )
        )
        self.assertFalse(
            _is_exchange_dynamic_stop_candidate_valid(
                direction="short",
                current_price=Decimal("100.1"),
                candidate_stop_loss=Decimal("100"),
            )
        )

    def test_cross_strategy_stop_loss_uses_signal_candle_low_minus_one_atr(self) -> None:
        instrument = Instrument(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
        )
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=14,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="both",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id=STRATEGY_CROSS_ID,
        )
        plan = build_order_plan(
            instrument=instrument,
            config=config,
            order_size=Decimal("2"),
            signal="long",
            entry_reference=Decimal("2500"),
            atr_value=Decimal("10"),
            candle_ts=1,
            signal_candle_low=Decimal("2485.2"),
        )
        self.assertEqual(plan.stop_loss, Decimal("2475.2"))
        self.assertEqual(plan.take_profit, Decimal("2540"))
