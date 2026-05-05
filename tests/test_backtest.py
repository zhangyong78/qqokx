import json
from dataclasses import replace
from decimal import Decimal
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from okx_quant.backtest import (
    ATR_BATCH_MULTIPLIERS,
    ATR_BATCH_TAKE_RATIOS,
    BACKTEST_RESERVED_CANDLES,
    BATCH_MAX_ENTRIES_OPTIONS,
    _OpenPosition,
    _create_open_position,
    _build_drawdown_curves,
    _build_equity_curve,
    _build_period_stats,
    _determine_backtest_order_size,
    _advance_dynamic_stop,
    _dynamic_stop_price,
    _dynamic_trigger_price,
    _load_backtest_candles,
    _position_initial_risk_value,
    _backtest_trade_start_index,
    _format_backtest_timestamp,
    _try_close_position,
    _try_close_position_same_candle_after_fill,
    _try_fill_dynamic_order,
    build_atr_batch_configs,
    build_parameter_batch_configs,
    format_backtest_report,
    run_backtest,
    run_backtest_batch,
)
import okx_quant.backtest_export as backtest_export_module
from okx_quant.backtest_audit import batch_backtest_artifact_paths, single_backtest_artifact_paths
import okx_quant.backtest_ui as backtest_ui_module
from okx_quant.backtest_export import export_batch_backtest_report, export_single_backtest_report
from okx_quant.backtest_ui import (
    DEFAULT_MAKER_FEE_PERCENT,
    DEFAULT_TAKER_FEE_PERCENT,
    _backtest_candle_color,
    _backtest_bar_value_from_label,
    _BacktestSnapshotStore,
    _build_backtest_symbol_options,
    _build_backtest_compare_detail,
    _build_backtest_compare_row,
    _filter_manual_positions,
    _has_extension_stats,
    _build_manual_pool_summary,
    _build_manual_position_row,
    _format_manual_gap_pct,
    _chart_hover_index_for_x,
    _chart_price_axis_values,
    _chart_time_label_indices,
    _decimal_places_for_tick_size,
    _format_trade_exit_reason,
    _format_chart_hover_lines,
    _format_price_by_tick_size,
    _format_chart_timestamp,
    _manual_focus_window,
    _manual_position_matches_filter,
    _manual_position_break_even_gap_pct,
    _manual_row_tag,
    _normalize_backtest_bar_label,
    _normalize_chart_viewport,
    _sorted_manual_positions,
    _pan_chart_viewport,
    _zoom_chart_viewport,
    _BacktestSnapshot,
)
from okx_quant.indicators import atr, ema
from okx_quant.backtest import BacktestManualPosition, BacktestOpenPosition, BacktestReport, BacktestResult, BacktestTrade
from okx_quant.models import Candle, Instrument, OrderPlan, StrategyConfig
from okx_quant.strategy_catalog import (
    STRATEGY_CROSS_ID,
    STRATEGY_EMA_BREAKDOWN_SHORT_ID,
    STRATEGY_EMA_BREAKOUT_LONG_ID,
    STRATEGY_DYNAMIC_ID,
    STRATEGY_EMA5_EMA8_ID,
)


class DummyBacktestClient:
    def __init__(self, candles: list[Candle], instrument: Instrument) -> None:
        self._candles = candles
        self._instrument = instrument
        self.history_limits: list[int] = []

    def get_instrument(self, inst_id: str) -> Instrument:
        return self._instrument

    def get_candles(self, inst_id: str, bar: str, limit: int = 200) -> list[Candle]:
        return self._candles[-limit:]

    def get_candles_history(self, inst_id: str, bar: str, limit: int = 200) -> list[Candle]:
        self.history_limits.append(limit)
        fetch_full_history = limit <= 0
        requested_count = 0 if fetch_full_history else limit
        returned = list(self._candles) if fetch_full_history else self._candles[-limit:]
        self.last_candle_history_stats = {
            "cache_hit_count": len(returned) if fetch_full_history else max(limit - 12, 0),
            "latest_fetch_count": 0 if fetch_full_history else 12,
            "older_fetch_count": 0,
            "requested_count": requested_count,
            "returned_count": len(returned),
            "full_history": fetch_full_history,
        }
        return returned

    def get_candles_history_range(
        self,
        inst_id: str,
        bar: str,
        *,
        start_ts: int,
        end_ts: int,
        limit: int = 200,
        preload_count: int = 0,
    ) -> list[Candle]:
        self.history_limits.append(limit)
        filtered = [candle for candle in self._candles if start_ts <= candle.ts <= end_ts]
        fetch_full_history = limit <= 0
        requested_count = 0 if fetch_full_history else limit
        selected_returned = list(filtered) if fetch_full_history else filtered[-limit:]
        preload = (
            [candle for candle in self._candles if candle.ts < start_ts][-preload_count:]
            if preload_count > 0
            else []
        )
        returned = preload + selected_returned
        self.last_candle_history_stats = {
            "range_mode": True,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "requested_count": requested_count,
            "selected_count": len(selected_returned),
            "preload_count": len(preload),
            "returned_count": len(returned),
            "full_history": fetch_full_history,
        }
        return returned


class BacktestTest(TestCase):
    def test_backtest_default_fee_percents(self) -> None:
        self.assertEqual(DEFAULT_MAKER_FEE_PERCENT, "0.015")
        self.assertEqual(DEFAULT_TAKER_FEE_PERCENT, "0.036")

    def test_dynamic_backtest_stop_locks_1r_at_2r(self) -> None:
        position = _OpenPosition(
            signal="long",
            entry_index=0,
            entry_ts=0,
            entry_price=Decimal("100"),
            stop_loss=Decimal("90"),
            take_profit=Decimal("120"),
            initial_stop_loss=Decimal("90"),
            initial_take_profit=Decimal("120"),
            atr_value=Decimal("10"),
            size=Decimal("1"),
            risk_per_unit=Decimal("10"),
            tick_size=Decimal("0.1"),
        )

        stop_price = _dynamic_stop_price(position, 2)

        self.assertEqual(stop_price, Decimal("110"))

    def test_dynamic_backtest_stop_can_move_to_break_even_plus_two_taker_fees_at_2r(self) -> None:
        position = _OpenPosition(
            signal="long",
            entry_index=0,
            entry_ts=0,
            entry_price=Decimal("100"),
            stop_loss=Decimal("90"),
            take_profit=Decimal("120"),
            initial_stop_loss=Decimal("90"),
            initial_take_profit=Decimal("120"),
            atr_value=Decimal("10"),
            size=Decimal("1"),
            risk_per_unit=Decimal("10"),
            tick_size=Decimal("0.1"),
            dynamic_exit_fee_rate=Decimal("0.00036"),
            dynamic_two_r_break_even=True,
        )

        stop_price = _dynamic_stop_price(position, 2)

        self.assertEqual(stop_price, Decimal("100.1"))

    def test_dynamic_backtest_stop_can_disable_fee_offset_and_move_to_plain_break_even_at_2r(self) -> None:
        position = _OpenPosition(
            signal="long",
            entry_index=0,
            entry_ts=0,
            entry_price=Decimal("100"),
            stop_loss=Decimal("90"),
            take_profit=Decimal("120"),
            initial_stop_loss=Decimal("90"),
            initial_take_profit=Decimal("120"),
            atr_value=Decimal("10"),
            size=Decimal("1"),
            risk_per_unit=Decimal("10"),
            tick_size=Decimal("0.1"),
            dynamic_exit_fee_rate=Decimal("0.00036"),
            dynamic_two_r_break_even=True,
            dynamic_fee_offset_enabled=False,
        )

        stop_price = _dynamic_stop_price(position, 2)

        self.assertEqual(stop_price, Decimal("100"))

    def test_dynamic_backtest_time_stop_break_even_moves_long_stop_after_threshold(self) -> None:
        position = _OpenPosition(
            signal="long",
            entry_index=0,
            entry_ts=0,
            entry_price=Decimal("100"),
            entry_price_raw=Decimal("100"),
            stop_loss=Decimal("90"),
            take_profit=Decimal("120"),
            initial_stop_loss=Decimal("90"),
            initial_take_profit=Decimal("120"),
            atr_value=Decimal("10"),
            size=Decimal("1"),
            risk_per_unit=Decimal("10"),
            tick_size=Decimal("0.1"),
            dynamic_exit_fee_rate=Decimal("0.00036"),
            dynamic_take_profit_enabled=True,
            time_stop_break_even_enabled=True,
            time_stop_break_even_bars=10,
        )

        _advance_dynamic_stop(position, Decimal("100.2"), holding_bars=10)

        self.assertEqual(position.stop_loss, Decimal("100.1"))

    def test_dynamic_backtest_time_stop_break_even_moves_short_stop_after_threshold(self) -> None:
        position = _OpenPosition(
            signal="short",
            entry_index=0,
            entry_ts=0,
            entry_price=Decimal("100"),
            entry_price_raw=Decimal("100"),
            stop_loss=Decimal("110"),
            take_profit=Decimal("80"),
            initial_stop_loss=Decimal("110"),
            initial_take_profit=Decimal("80"),
            atr_value=Decimal("10"),
            size=Decimal("1"),
            risk_per_unit=Decimal("10"),
            tick_size=Decimal("0.1"),
            dynamic_exit_fee_rate=Decimal("0.00036"),
            dynamic_take_profit_enabled=True,
            time_stop_break_even_enabled=True,
            time_stop_break_even_bars=10,
        )

        _advance_dynamic_stop(position, Decimal("99.8"), holding_bars=10)

        self.assertEqual(position.stop_loss, Decimal("99.9"))

    def test_dynamic_backtest_time_stop_break_even_waits_for_bar_threshold(self) -> None:
        position = _OpenPosition(
            signal="long",
            entry_index=0,
            entry_ts=0,
            entry_price=Decimal("100"),
            entry_price_raw=Decimal("100"),
            stop_loss=Decimal("90"),
            take_profit=Decimal("120"),
            initial_stop_loss=Decimal("90"),
            initial_take_profit=Decimal("120"),
            atr_value=Decimal("10"),
            size=Decimal("1"),
            risk_per_unit=Decimal("10"),
            tick_size=Decimal("0.1"),
            dynamic_exit_fee_rate=Decimal("0.00036"),
            dynamic_take_profit_enabled=True,
            time_stop_break_even_enabled=True,
            time_stop_break_even_bars=10,
        )

        _advance_dynamic_stop(position, Decimal("100.2"), holding_bars=9)

        self.assertEqual(position.stop_loss, Decimal("90"))

    def test_dynamic_backtest_time_stop_break_even_never_retrogrades_existing_stop(self) -> None:
        position = _OpenPosition(
            signal="long",
            entry_index=0,
            entry_ts=0,
            entry_price=Decimal("100"),
            entry_price_raw=Decimal("100"),
            stop_loss=Decimal("105"),
            take_profit=Decimal("120"),
            initial_stop_loss=Decimal("90"),
            initial_take_profit=Decimal("120"),
            atr_value=Decimal("10"),
            size=Decimal("1"),
            risk_per_unit=Decimal("10"),
            tick_size=Decimal("0.1"),
            dynamic_exit_fee_rate=Decimal("0.00036"),
            dynamic_take_profit_enabled=True,
            time_stop_break_even_enabled=True,
            time_stop_break_even_bars=10,
        )

        _advance_dynamic_stop(position, Decimal("100.2"), holding_bars=10)

        self.assertEqual(position.stop_loss, Decimal("105"))

    def test_dynamic_limit_fill_does_not_apply_entry_slippage(self) -> None:
        plan = OrderPlan(
            inst_id="BTC-USDT-SWAP",
            side="buy",
            pos_side=None,
            size=Decimal("1"),
            take_profit=Decimal("140"),
            stop_loss=Decimal("90"),
            entry_reference=Decimal("100"),
            atr_value=Decimal("10"),
            signal="long",
            candle_ts=1,
        )
        candle = Candle(
            1,
            Decimal("101"),
            Decimal("102"),
            Decimal("99"),
            Decimal("100"),
            Decimal("1"),
            True,
        )

        position = _try_fill_dynamic_order(
            self._build_instrument(),
            plan,
            candle,
            0,
            entry_fee_rate=Decimal("0"),
            entry_fee_type="maker",
            entry_slippage_rate=Decimal("0.01"),
            exit_slippage_rate=Decimal("0.02"),
            dynamic_take_profit_enabled=True,
            dynamic_exit_fee_rate=Decimal("0.00036"),
            dynamic_two_r_break_even=True,
        )

        self.assertIsNotNone(position)
        assert position is not None
        self.assertEqual(position.entry_price, Decimal("100"))
        self.assertEqual(position.entry_price_raw, Decimal("100"))
        self.assertEqual(position.entry_slippage_cost, Decimal("0"))
        self.assertEqual(position.risk_per_unit, Decimal("10"))
        self.assertEqual(_dynamic_trigger_price(position, 2), Decimal("120.1"))

    def test_dynamic_limit_fill_at_open_uses_open_price_and_taker_fee_for_long(self) -> None:
        plan = OrderPlan(
            inst_id="BTC-USDT-SWAP",
            side="buy",
            pos_side=None,
            size=Decimal("1"),
            take_profit=Decimal("140"),
            stop_loss=Decimal("90"),
            entry_reference=Decimal("100"),
            atr_value=Decimal("10"),
            signal="long",
            candle_ts=1,
        )
        candle = Candle(
            1,
            Decimal("95"),
            Decimal("99"),
            Decimal("94"),
            Decimal("96"),
            Decimal("1"),
            True,
        )

        position = _try_fill_dynamic_order(
            self._build_instrument(),
            plan,
            candle,
            0,
            entry_fee_rate=Decimal("0.00015"),
            entry_fee_type="maker",
            entry_slippage_rate=Decimal("0.01"),
            exit_slippage_rate=Decimal("0.02"),
            dynamic_take_profit_enabled=True,
            dynamic_exit_fee_rate=Decimal("0.00036"),
            dynamic_two_r_break_even=True,
            immediate_entry_fee_rate=Decimal("0.00036"),
            immediate_entry_fee_type="taker",
        )

        self.assertIsNotNone(position)
        assert position is not None
        self.assertEqual(position.entry_price, Decimal("95"))
        self.assertEqual(position.entry_price_raw, Decimal("100"))
        self.assertEqual(position.entry_path_price, Decimal("95"))
        self.assertEqual(position.entry_fee_rate, Decimal("0.00036"))
        self.assertEqual(position.entry_fee_type, "taker")
        self.assertEqual(position.entry_slippage_cost, Decimal("0"))
        self.assertEqual(_dynamic_trigger_price(position, 2), Decimal("120.1"))

    def test_dynamic_limit_fill_at_open_uses_open_price_and_taker_fee_for_short(self) -> None:
        plan = OrderPlan(
            inst_id="BTC-USDT-SWAP",
            side="sell",
            pos_side=None,
            size=Decimal("1"),
            take_profit=Decimal("60"),
            stop_loss=Decimal("110"),
            entry_reference=Decimal("100"),
            atr_value=Decimal("10"),
            signal="short",
            candle_ts=1,
        )
        candle = Candle(
            1,
            Decimal("105"),
            Decimal("106"),
            Decimal("101"),
            Decimal("104"),
            Decimal("1"),
            True,
        )

        position = _try_fill_dynamic_order(
            self._build_instrument(),
            plan,
            candle,
            0,
            entry_fee_rate=Decimal("0.00015"),
            entry_fee_type="maker",
            entry_slippage_rate=Decimal("0.01"),
            exit_slippage_rate=Decimal("0.02"),
            dynamic_take_profit_enabled=True,
            dynamic_exit_fee_rate=Decimal("0.00036"),
            dynamic_two_r_break_even=True,
            immediate_entry_fee_rate=Decimal("0.00036"),
            immediate_entry_fee_type="taker",
        )

        self.assertIsNotNone(position)
        assert position is not None
        self.assertEqual(position.entry_price, Decimal("105"))
        self.assertEqual(position.entry_price_raw, Decimal("100"))
        self.assertEqual(position.entry_path_price, Decimal("105"))
        self.assertEqual(position.entry_fee_rate, Decimal("0.00036"))
        self.assertEqual(position.entry_fee_type, "taker")
        self.assertEqual(position.entry_slippage_cost, Decimal("0"))
        self.assertEqual(_dynamic_trigger_price(position, 2), Decimal("79.9"))

    def test_dynamic_trigger_uses_strategy_entry_price_for_long_when_fill_price_slips(self) -> None:
        position = _create_open_position(
            instrument=self._build_instrument(),
            signal="long",
            entry_index=0,
            entry_ts=0,
            entry_price_raw=Decimal("100"),
            stop_loss=Decimal("90"),
            take_profit=Decimal("140"),
            atr_value=Decimal("10"),
            size=Decimal("1"),
            entry_fee_rate=Decimal("0"),
            exit_fee_rate=Decimal("0"),
            entry_fee_type="taker",
            entry_slippage_rate=Decimal("0.01"),
            exit_slippage_rate=Decimal("0.02"),
            funding_rate=Decimal("0"),
            dynamic_take_profit_enabled=True,
        )

        self.assertEqual(position.entry_price, Decimal("101"))
        self.assertEqual(position.risk_per_unit, Decimal("10"))
        self.assertEqual(_dynamic_trigger_price(position, 2), Decimal("120"))
        self.assertEqual(position.entry_slippage_rate, Decimal("0.01"))
        self.assertEqual(position.exit_slippage_rate, Decimal("0.02"))

    def test_dynamic_trigger_uses_strategy_entry_price_for_short_when_fill_price_slips(self) -> None:
        position = _create_open_position(
            instrument=self._build_instrument(),
            signal="short",
            entry_index=0,
            entry_ts=0,
            entry_price_raw=Decimal("100"),
            stop_loss=Decimal("110"),
            take_profit=Decimal("60"),
            atr_value=Decimal("10"),
            size=Decimal("1"),
            entry_fee_rate=Decimal("0"),
            exit_fee_rate=Decimal("0"),
            entry_fee_type="taker",
            entry_slippage_rate=Decimal("0.01"),
            exit_slippage_rate=Decimal("0.02"),
            funding_rate=Decimal("0"),
            dynamic_take_profit_enabled=True,
        )

        self.assertEqual(position.entry_price, Decimal("99"))
        self.assertEqual(position.risk_per_unit, Decimal("10"))
        self.assertEqual(_dynamic_trigger_price(position, 2), Decimal("80"))

    def test_position_initial_risk_value_ignores_moved_stop_loss(self) -> None:
        position = _create_open_position(
            instrument=self._build_instrument(),
            signal="long",
            entry_index=0,
            entry_ts=0,
            entry_price_raw=Decimal("100"),
            stop_loss=Decimal("90"),
            take_profit=Decimal("140"),
            atr_value=Decimal("10"),
            size=Decimal("1"),
            entry_fee_rate=Decimal("0"),
            exit_fee_rate=Decimal("0"),
            entry_fee_type="maker",
            entry_slippage_rate=Decimal("0"),
            exit_slippage_rate=Decimal("0"),
            funding_rate=Decimal("0"),
            dynamic_take_profit_enabled=True,
        )

        self.assertIsNotNone(position)
        assert position is not None
        position.stop_loss = Decimal("101.5")

        self.assertEqual(_position_initial_risk_value(position), Decimal("10"))

    def test_backtest_risk_size_below_min_order_size_is_clamped_to_min_size(self) -> None:
        instrument = Instrument(
            inst_id="BNB-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
        )
        config = StrategyConfig(
            inst_id="BNB-USDT-SWAP",
            bar="1H",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0"),
            trade_mode="cross",
            signal_mode="short_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id=STRATEGY_DYNAMIC_ID,
            risk_amount=Decimal("100"),
        )

        size = _determine_backtest_order_size(
            instrument=instrument,
            config=config,
            entry_price=Decimal("600"),
            stop_loss=Decimal("900"),
            risk_price_compatible=True,
        )

        self.assertEqual(size, Decimal("1"))

    def _build_instrument(self) -> Instrument:
        return Instrument(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
        )

    def _build_custom_instrument(
        self,
        *,
        inst_id: str,
        tick_size: str,
        lot_size: str,
        min_size: str,
    ) -> Instrument:
        return Instrument(
            inst_id=inst_id,
            inst_type="SWAP",
            tick_size=Decimal(tick_size),
            lot_size=Decimal(lot_size),
            min_size=Decimal(min_size),
            state="live",
        )

    def _load_cached_candles(self, cache_name: str, *, end_ts: int | None = None) -> list[Candle]:
        cache_path = Path(__file__).resolve().parents[1] / ".okx_quant_candle_cache" / cache_name
        if not cache_path.exists():
            self.skipTest(f"missing candle cache: {cache_path.name}")
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        candles: list[Candle] = []
        for item in payload["candles"]:
            candle = Candle(
                int(item["ts"]),
                Decimal(item["open"]),
                Decimal(item["high"]),
                Decimal(item["low"]),
                Decimal(item["close"]),
                Decimal(item["volume"]),
                bool(item.get("confirmed", True)),
            )
            candles.append(candle)
            if end_ts is not None and candle.ts >= end_ts:
                break
        return candles

    def _build_config(self, *, ema_period: int = 2, atr_period: int = 2) -> StrategyConfig:
        return StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=ema_period,
            trend_ema_period=max(ema_period + 1, 3),
            big_ema_period=max(ema_period + 2, 4),
            atr_period=atr_period,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id=STRATEGY_EMA_BREAKOUT_LONG_ID,
            risk_amount=Decimal("100"),
        )

    def _build_ema5_ema8_config(self, *, signal_mode: str = "both") -> StrategyConfig:
        return StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="4H",
            ema_period=2,
            trend_ema_period=3,
            big_ema_period=4,
            atr_period=10,
            atr_stop_multiplier=Decimal("1"),
            atr_take_multiplier=Decimal("1"),
            order_size=Decimal("0"),
            trade_mode="cross",
            signal_mode=signal_mode,
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id=STRATEGY_EMA5_EMA8_ID,
            risk_amount=Decimal("100"),
        )

    def test_dynamic_backtest_short_gap_fill_regression_uses_next_candle_open_for_sol_4h(self) -> None:
        target_entry_ts = 1645747200000  # 2022-02-25 08:00:00
        candles = self._load_cached_candles("SOL-USDT-SWAP__4H.json", end_ts=1645819200000)
        instrument = self._build_custom_instrument(
            inst_id="SOL-USDT-SWAP",
            tick_size="0.01",
            lot_size="0.01",
            min_size="0.01",
        )
        client = DummyBacktestClient(candles, instrument)
        config = StrategyConfig(
            inst_id="SOL-USDT-SWAP",
            bar="4H",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=0,
            atr_period=10,
            atr_stop_multiplier=Decimal("1"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0"),
            trade_mode="cross",
            signal_mode="short_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
            strategy_id=STRATEGY_DYNAMIC_ID,
            risk_amount=Decimal("10"),
            take_profit_mode="dynamic",
            max_entries_per_trend=1,
            entry_reference_ema_period=21,
            dynamic_two_r_break_even=True,
            dynamic_fee_offset_enabled=True,
        )

        result = run_backtest(client, config, candle_limit=len(candles))
        trade = next((item for item in result.trades if item.entry_ts == target_entry_ts), None)

        self.assertIsNotNone(trade)
        assert trade is not None
        self.assertEqual(trade.signal, "short")
        self.assertEqual(trade.entry_price, Decimal("89.33"))
        self.assertEqual(trade.entry_fee_type, "taker")

    def test_dynamic_backtest_long_gap_fill_regression_uses_next_candle_open_for_btc_4h(self) -> None:
        target_entry_ts = 1634990400000  # 2021-10-23 20:00:00
        candles = self._load_cached_candles("BTC-USDT-SWAP__4H.json", end_ts=1636560000000)
        instrument = self._build_custom_instrument(
            inst_id="BTC-USDT-SWAP",
            tick_size="0.1",
            lot_size="0.001",
            min_size="0.001",
        )
        client = DummyBacktestClient(candles, instrument)
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="4H",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=0,
            atr_period=10,
            atr_stop_multiplier=Decimal("1.5"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
            strategy_id=STRATEGY_DYNAMIC_ID,
            risk_amount=Decimal("10"),
            take_profit_mode="dynamic",
            max_entries_per_trend=1,
            entry_reference_ema_period=21,
            dynamic_two_r_break_even=True,
            dynamic_fee_offset_enabled=True,
        )

        result = run_backtest(client, config, candle_limit=len(candles))
        trade = next((item for item in result.trades if item.entry_ts == target_entry_ts), None)

        self.assertIsNotNone(
            trade,
            msg=f"entries={[item.entry_ts for item in result.trades[:5]]}",
        )
        assert trade is not None
        self.assertEqual(trade.signal, "long")
        self.assertEqual(trade.entry_price, Decimal("61566.6"))
        self.assertEqual(trade.entry_fee_type, "taker")

    def test_cross_backtest_generates_trade_and_report(self) -> None:
        warmup_candles = [
            Candle(index, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True)
            for index in range(1, BACKTEST_RESERVED_CANDLES + 1)
        ]
        trade_candles = [
            Candle(1, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True),
            Candle(2, Decimal("100"), Decimal("101"), Decimal("98"), Decimal("99"), Decimal("1"), True),
            Candle(3, Decimal("99"), Decimal("100"), Decimal("97"), Decimal("98"), Decimal("1"), True),
            Candle(4, Decimal("98"), Decimal("99"), Decimal("95"), Decimal("96"), Decimal("1"), True),
            Candle(5, Decimal("96"), Decimal("106"), Decimal("95"), Decimal("104"), Decimal("1"), True),
            Candle(6, Decimal("104"), Decimal("133"), Decimal("100"), Decimal("130"), Decimal("1"), True),
        ]
        candles = warmup_candles + [
            Candle(
                BACKTEST_RESERVED_CANDLES + candle.ts,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
                candle.confirmed,
            )
            for candle in trade_candles
        ]
        client = DummyBacktestClient(candles, self._build_instrument())
        config = replace(self._build_config(), take_profit_mode="fixed")

        result = run_backtest(client, config, candle_limit=len(candles))

        self.assertGreaterEqual(len(result.trades), 1)
        self.assertTrue(all(trade.entry_index >= BACKTEST_RESERVED_CANDLES for trade in result.trades))
        self.assertTrue(any(trade.exit_reason == "take_profit" for trade in result.trades))
        self.assertGreater(result.report.total_pnl, Decimal("0"))
        self.assertEqual(result.ema_values, ema([candle.close for candle in candles], config.ema_period))
        self.assertEqual(result.trend_ema_values, ema([candle.close for candle in candles], config.trend_ema_period))
        self.assertEqual(result.atr_values, atr(candles, config.atr_period))
        self.assertTrue(all(trade.atr_value == result.atr_values[trade.entry_index] for trade in result.trades))
        self.assertEqual(len(result.equity_curve), len(candles))
        self.assertEqual(result.equity_curve[-1], result.report.total_pnl)
        self.assertEqual(result.trend_ema_period, config.trend_ema_period)
        self.assertIn(str(result.report.total_trades), format_backtest_report(result))
        self.assertIn("开始时间：", format_backtest_report(result))
        self.assertIn("结束时间：", format_backtest_report(result))
        self.assertIn(f"预热K线：前 {BACKTEST_RESERVED_CANDLES} 根", format_backtest_report(result))
        self.assertEqual(client.history_limits, [len(candles)])

    def test_cross_backtest_applies_taker_fees_on_entry_and_exit(self) -> None:
        warmup_candles = [
            Candle(index, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True)
            for index in range(1, BACKTEST_RESERVED_CANDLES + 1)
        ]
        trade_candles = [
            Candle(1, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True),
            Candle(2, Decimal("100"), Decimal("101"), Decimal("98"), Decimal("99"), Decimal("1"), True),
            Candle(3, Decimal("99"), Decimal("100"), Decimal("97"), Decimal("98"), Decimal("1"), True),
            Candle(4, Decimal("98"), Decimal("99"), Decimal("95"), Decimal("96"), Decimal("1"), True),
            Candle(5, Decimal("96"), Decimal("106"), Decimal("95"), Decimal("104"), Decimal("1"), True),
            Candle(6, Decimal("104"), Decimal("133"), Decimal("100"), Decimal("130"), Decimal("1"), True),
        ]
        candles = warmup_candles + [
            Candle(
                BACKTEST_RESERVED_CANDLES + candle.ts,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
                candle.confirmed,
            )
            for candle in trade_candles
        ]
        client = DummyBacktestClient(candles, self._build_instrument())
        config = replace(self._build_config(), take_profit_mode="fixed")

        no_fee_result = run_backtest(client, config, candle_limit=len(candles))
        fee_result = run_backtest(
            client,
            config,
            candle_limit=len(candles),
            maker_fee_rate=Decimal("0"),
            taker_fee_rate=Decimal("0.001"),
        )

        self.assertGreater(fee_result.report.total_fees, Decimal("0"))
        self.assertEqual(fee_result.report.maker_fees, Decimal("0"))
        self.assertEqual(fee_result.report.taker_fees, fee_result.report.total_fees)
        self.assertLess(fee_result.report.total_pnl, no_fee_result.report.total_pnl)
        self.assertTrue(all(trade.entry_fee_type == "taker" for trade in fee_result.trades))
        self.assertTrue(all(trade.exit_fee_type == "taker" for trade in fee_result.trades))
        self.assertEqual(
            fee_result.report.total_fees,
            sum((trade.total_fee for trade in fee_result.trades), Decimal("0")),
        )

    def test_cross_backtest_rejects_signal_mode_both(self) -> None:
        candles = [
            Candle(i, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True)
            for i in range(1, 260)
        ]
        client = DummyBacktestClient(candles, self._build_instrument())
        config = replace(
            self._build_config(),
            strategy_id=STRATEGY_CROSS_ID,
            signal_mode="both",
        )
        with self.assertRaises(RuntimeError) as ctx:
            run_backtest(client, config, candle_limit=len(candles))
        self.assertIn("双向", str(ctx.exception))

    def test_backtest_supports_more_than_300_candles(self) -> None:
        candles = [
            Candle(
                index,
                Decimal("100"),
                Decimal("101"),
                Decimal("99"),
                Decimal("100"),
                Decimal("1"),
                True,
            )
            for index in range(1, 701)
        ]
        client = DummyBacktestClient(candles, self._build_instrument())
        config = self._build_config(ema_period=21, atr_period=14)

        result = run_backtest(client, config, candle_limit=500)

        self.assertEqual(len(result.candles), 500)
        self.assertEqual(client.history_limits, [500])
        self.assertIn("本次命中本地缓存", result.data_source_note)
        self.assertIn("补拉最新 12 根", result.data_source_note)

    def test_format_backtest_report_includes_terminal_open_position(self) -> None:
        result = BacktestResult(
            candles=[
                Candle(1730000000000, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True),
                Candle(1730003600000, Decimal("101"), Decimal("102"), Decimal("100"), Decimal("101"), Decimal("1"), True),
            ],
            trades=[],
            report=BacktestReport(
                total_trades=0,
                win_trades=0,
                loss_trades=0,
                breakeven_trades=0,
                win_rate=Decimal("0"),
                total_pnl=Decimal("0"),
                average_pnl=Decimal("0"),
                gross_profit=Decimal("0"),
                gross_loss=Decimal("0"),
                profit_factor=None,
                average_win=Decimal("0"),
                average_loss=Decimal("0"),
                profit_loss_ratio=None,
                average_r_multiple=Decimal("0"),
                max_drawdown=Decimal("0"),
            ),
            instrument=self._build_instrument(),
            open_position=BacktestOpenPosition(
                signal="long",
                entry_index=0,
                entry_ts=1730000000000,
                current_ts=1730003600000,
                entry_price=Decimal("100"),
                current_price=Decimal("95"),
                stop_loss=Decimal("105"),
                take_profit=Decimal("90"),
                initial_stop_loss=Decimal("105"),
                initial_take_profit=Decimal("90"),
                size=Decimal("2"),
                gross_pnl=Decimal("10"),
                pnl=Decimal("9.5"),
                risk_value=Decimal("10"),
                r_multiple=Decimal("0.95"),
                entry_fee=Decimal("0.5"),
                funding_cost=Decimal("0.1"),
            ),
        )

        report_text = format_backtest_report(result)

        self.assertIn("期末未平仓：", report_text)
        self.assertIn("做多", report_text)
        self.assertIn("开仓时间：", report_text)
        self.assertIn("当前时间：", report_text)
        self.assertIn("开仓数量：2.0000", report_text)
        self.assertIn("浮动盈亏：9.5000", report_text)

    def test_backtest_rejects_more_than_10000_candles(self) -> None:
        candles = [
            Candle(
                index,
                Decimal("100"),
                Decimal("101"),
                Decimal("99"),
                Decimal("100"),
                Decimal("1"),
                True,
            )
            for index in range(1, 50)
        ]
        client = DummyBacktestClient(candles, self._build_instrument())
        config = self._build_config()

        with self.assertRaises(ValueError):
            run_backtest(client, config, candle_limit=10001)

    def test_run_backtest_accepts_zero_candle_limit_for_full_history(self) -> None:
        candles = [
            Candle(
                index,
                Decimal("100"),
                Decimal("101"),
                Decimal("99"),
                Decimal("100"),
                Decimal("1"),
                True,
            )
            for index in range(1, 701)
        ]
        client = DummyBacktestClient(candles, self._build_instrument())
        config = self._build_config(ema_period=21, atr_period=14)

        result = run_backtest(client, config, candle_limit=0)

        self.assertEqual(len(result.candles), len(candles))
        self.assertEqual(client.history_limits, [0])
        self.assertIn("全量历史", result.data_source_note)

    def test_load_backtest_candles_supports_10000_candles(self) -> None:
        candles = [
            Candle(
                index,
                Decimal("100"),
                Decimal("101"),
                Decimal("99"),
                Decimal("100"),
                Decimal("1"),
                True,
            )
            for index in range(1, 10021)
        ]
        client = DummyBacktestClient(candles, self._build_instrument())
        result = _load_backtest_candles(client, "BTC-USDT-SWAP", "15m", 10000)

        self.assertEqual(len(result), 10000)
        self.assertEqual(client.history_limits, [10000])

    def test_load_backtest_candles_zero_limit_returns_full_history(self) -> None:
        candles = [
            Candle(
                index,
                Decimal("100"),
                Decimal("101"),
                Decimal("99"),
                Decimal("100"),
                Decimal("1"),
                True,
            )
            for index in range(1, 321)
        ]
        client = DummyBacktestClient(candles, self._build_instrument())

        result = _load_backtest_candles(client, "BTC-USDT-SWAP", "15m", 0)

        self.assertEqual(len(result), len(candles))
        self.assertEqual(client.history_limits, [0])

    def test_load_backtest_candles_filters_selected_time_range(self) -> None:
        candles = [
            Candle(
                1711929600000 + (index * 3600 * 1000),
                Decimal("100"),
                Decimal("101"),
                Decimal("99"),
                Decimal("100"),
                Decimal("1"),
                True,
            )
            for index in range(10)
        ]
        client = DummyBacktestClient(candles, self._build_instrument())

        result = _load_backtest_candles(
            client,
            "BTC-USDT-SWAP",
            "1H",
            100,
            start_ts=candles[2].ts,
            end_ts=candles[5].ts,
        )

        self.assertEqual([candle.ts for candle in result], [candle.ts for candle in candles[2:6]])
        self.assertEqual(client.history_limits, [100])

    def test_load_backtest_candles_auto_prepends_preload_for_selected_range(self) -> None:
        candles = [
            Candle(
                1711929600000 + (index * 3600 * 1000),
                Decimal("100"),
                Decimal("101"),
                Decimal("99"),
                Decimal("100"),
                Decimal("1"),
                True,
            )
            for index in range(20)
        ]
        client = DummyBacktestClient(candles, self._build_instrument())

        result = _load_backtest_candles(
            client,
            "BTC-USDT-SWAP",
            "1H",
            100,
            start_ts=candles[10].ts,
            end_ts=candles[12].ts,
            preload_count=5,
        )

        self.assertEqual([candle.ts for candle in result], [candle.ts for candle in candles[5:13]])
        self.assertEqual(client.history_limits, [100])

    def test_run_backtest_selected_range_auto_prepends_warmup_candles(self) -> None:
        candles = [
            Candle(
                1711929600000 + (index * 3600 * 1000),
                Decimal("100"),
                Decimal("101"),
                Decimal("99"),
                Decimal("100"),
                Decimal("1"),
                True,
            )
            for index in range(400)
        ]
        client = DummyBacktestClient(candles, self._build_instrument())
        config = replace(
            self._build_config(),
            strategy_id=STRATEGY_DYNAMIC_ID,
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=14,
        )

        result = run_backtest(
            client,
            config,
            candle_limit=10000,
            start_ts=candles[300].ts,
            end_ts=candles[302].ts,
        )

        self.assertEqual(result.candles[0].ts, candles[68].ts)
        self.assertEqual(result.candles[-1].ts, candles[302].ts)
        self.assertIn("前置补足 232 根", result.data_source_note)

    def test_build_atr_batch_configs_returns_nine_combinations(self) -> None:
        configs = build_atr_batch_configs(self._build_config())

        self.assertEqual(len(configs), 9)
        self.assertEqual(configs[0].atr_stop_multiplier, ATR_BATCH_MULTIPLIERS[0])
        self.assertEqual(configs[0].atr_take_multiplier, ATR_BATCH_MULTIPLIERS[0])
        self.assertEqual(configs[-1].atr_stop_multiplier, ATR_BATCH_MULTIPLIERS[-1])
        self.assertEqual(configs[-1].atr_take_multiplier, ATR_BATCH_MULTIPLIERS[-1] * ATR_BATCH_TAKE_RATIOS[-1])
        self.assertEqual(
            {(config.atr_stop_multiplier, config.atr_take_multiplier) for config in configs},
            {
                (Decimal("1"), Decimal("1")),
                (Decimal("1"), Decimal("2")),
                (Decimal("1"), Decimal("3")),
                (Decimal("1.5"), Decimal("1.5")),
                (Decimal("1.5"), Decimal("3")),
                (Decimal("1.5"), Decimal("4.5")),
                (Decimal("2"), Decimal("2")),
                (Decimal("2"), Decimal("4")),
                (Decimal("2"), Decimal("6")),
            },
        )

    def test_build_parameter_batch_configs_for_dynamic_fixed_take_profit_returns_thirty_six_combinations(self) -> None:
        base_config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=14,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id=STRATEGY_DYNAMIC_ID,
            risk_amount=Decimal("100"),
            take_profit_mode="fixed",
        )

        configs = build_parameter_batch_configs(base_config)

        self.assertEqual(len(configs), 36)
        self.assertEqual({config.max_entries_per_trend for config in configs}, set(BATCH_MAX_ENTRIES_OPTIONS))
        self.assertEqual(configs[0].max_entries_per_trend, 0)
        self.assertEqual(configs[0].atr_stop_multiplier, ATR_BATCH_MULTIPLIERS[0])
        self.assertEqual(configs[0].atr_take_multiplier, ATR_BATCH_MULTIPLIERS[0] * ATR_BATCH_TAKE_RATIOS[0])
        self.assertEqual(configs[-1].max_entries_per_trend, 3)
        self.assertEqual(configs[-1].atr_stop_multiplier, ATR_BATCH_MULTIPLIERS[-1])
        self.assertEqual(configs[-1].atr_take_multiplier, ATR_BATCH_MULTIPLIERS[-1] * ATR_BATCH_TAKE_RATIOS[-1])

    def test_build_parameter_batch_configs_for_dynamic_take_profit_returns_twelve_stop_and_entry_variants(self) -> None:
        base_config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=14,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id=STRATEGY_DYNAMIC_ID,
            risk_amount=Decimal("100"),
            take_profit_mode="dynamic",
        )

        configs = build_parameter_batch_configs(base_config)

        self.assertEqual(len(configs), 12)
        self.assertEqual({config.max_entries_per_trend for config in configs}, set(BATCH_MAX_ENTRIES_OPTIONS))
        self.assertEqual({config.atr_stop_multiplier for config in configs}, set(ATR_BATCH_MULTIPLIERS))
        self.assertTrue(all(config.atr_take_multiplier == Decimal("4") for config in configs))
        self.assertEqual(configs[0].atr_stop_multiplier, ATR_BATCH_MULTIPLIERS[0])
        self.assertEqual(configs[0].max_entries_per_trend, 0)
        self.assertEqual(configs[-1].atr_stop_multiplier, ATR_BATCH_MULTIPLIERS[-1])
        self.assertEqual(configs[-1].max_entries_per_trend, 3)

    def test_run_backtest_batch_returns_nine_results_and_reuses_history_fetch(self) -> None:
        candles = [
            Candle(
                index,
                Decimal("100"),
                Decimal("101"),
                Decimal("99"),
                Decimal("100"),
                Decimal("1"),
                True,
            )
            for index in range(1, 701)
        ]
        client = DummyBacktestClient(candles, self._build_instrument())
        config = self._build_config(ema_period=21, atr_period=14)

        results = run_backtest_batch(client, config, candle_limit=500)

        self.assertEqual(len(results), 9)
        self.assertEqual(client.history_limits, [500])
        self.assertEqual(results[0][0].atr_stop_multiplier, Decimal("1"))
        self.assertEqual(results[0][0].atr_take_multiplier, Decimal("1"))
        self.assertEqual(results[-1][0].atr_stop_multiplier, Decimal("2"))
        self.assertEqual(results[-1][0].atr_take_multiplier, Decimal("6"))
        self.assertTrue(all(len(result.candles) == 500 for _, result in results))

    def test_run_backtest_batch_for_dynamic_fixed_take_profit_returns_thirty_six_results(self) -> None:
        candles = [
            Candle(
                index,
                Decimal("100"),
                Decimal("101"),
                Decimal("99"),
                Decimal("100"),
                Decimal("1"),
                True,
            )
            for index in range(1, 701)
        ]
        client = DummyBacktestClient(candles, self._build_instrument())
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=14,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id=STRATEGY_DYNAMIC_ID,
            risk_amount=Decimal("100"),
            take_profit_mode="fixed",
        )

        results = run_backtest_batch(client, config, candle_limit=500)

        self.assertEqual(len(results), 36)
        self.assertEqual(client.history_limits, [500])
        self.assertEqual({cfg.max_entries_per_trend for cfg, _ in results}, set(BATCH_MAX_ENTRIES_OPTIONS))

    def test_run_backtest_batch_for_dynamic_take_profit_returns_twelve_results(self) -> None:
        candles = [
            Candle(
                index,
                Decimal("100"),
                Decimal("101"),
                Decimal("99"),
                Decimal("100"),
                Decimal("1"),
                True,
            )
            for index in range(1, 701)
        ]
        client = DummyBacktestClient(candles, self._build_instrument())
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=14,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id=STRATEGY_DYNAMIC_ID,
            risk_amount=Decimal("100"),
            take_profit_mode="dynamic",
        )

        results = run_backtest_batch(client, config, candle_limit=500)

        self.assertEqual(len(results), 12)
        self.assertEqual(client.history_limits, [500])
        self.assertEqual({cfg.max_entries_per_trend for cfg, _ in results}, set(BATCH_MAX_ENTRIES_OPTIONS))
        self.assertEqual({cfg.atr_stop_multiplier for cfg, _ in results}, set(ATR_BATCH_MULTIPLIERS))

        self.assertTrue(all(not result.backtest_profile_name for _, result in results))

    def test_ema5_ema8_strategy_rejects_atr_batch_backtest(self) -> None:
        candles = [
            Candle(index, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True)
            for index in range(1, 701)
        ]
        client = DummyBacktestClient(candles, self._build_instrument())

        with self.assertRaisesRegex(RuntimeError, "不参与 ATR 批量矩阵回测"):
            run_backtest_batch(client, self._build_ema5_ema8_config(), candle_limit=500)

    def test_backtest_bar_label_mapping_accepts_raw_values_and_labels(self) -> None:
        self.assertEqual(_normalize_backtest_bar_label("5m"), "5分钟")
        self.assertEqual(_normalize_backtest_bar_label("15分钟"), "15分钟")
        self.assertEqual(_normalize_backtest_bar_label("1H"), "1小时")
        self.assertEqual(_normalize_backtest_bar_label("4H"), "4小时")
        self.assertEqual(_backtest_bar_value_from_label("1小时"), "1H")

    def test_backtest_bar_label_mapping_falls_back_to_15_minutes(self) -> None:
        self.assertEqual(_normalize_backtest_bar_label("3m"), "15分钟")
        self.assertEqual(_backtest_bar_value_from_label("3m"), "15m")

    def test_backtest_symbol_options_include_default_pairs_and_current_symbol(self) -> None:
        self.assertIn("BTC-USDT-SWAP", _build_backtest_symbol_options("BTC-USDT-SWAP"))
        self.assertIn("ETH-USDT-SWAP", _build_backtest_symbol_options("BTC-USDT-SWAP"))
        self.assertIn("SOL-USDT-SWAP", _build_backtest_symbol_options("BTC-USDT-SWAP"))
        self.assertIn("BNB-USDT-SWAP", _build_backtest_symbol_options("BTC-USDT-SWAP"))
        self.assertIn("DOGE-USDT-SWAP", _build_backtest_symbol_options("BTC-USDT-SWAP"))
        self.assertEqual(_build_backtest_symbol_options("XRP-USDT-SWAP")[0], "XRP-USDT-SWAP")

    def test_backtest_trade_start_index_reserves_first_200_candles(self) -> None:
        self.assertEqual(_backtest_trade_start_index(4), BACKTEST_RESERVED_CANDLES)
        self.assertEqual(_backtest_trade_start_index(250), 249)

    def test_format_backtest_timestamp_supports_seconds_and_milliseconds(self) -> None:
        self.assertEqual(_format_backtest_timestamp(1710976500000), _format_backtest_timestamp(1710976500))
        self.assertEqual(len(_format_backtest_timestamp(1710976500000)), 16)
        self.assertEqual(_format_backtest_timestamp(12345), "12345")

    def test_dynamic_backtest_report_includes_ema_relationship_filter(self) -> None:
        result = run_backtest(
            DummyBacktestClient(
                [
                    Candle(index, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True)
                    for index in range(1, 401)
                ],
                self._build_instrument(),
            ),
            StrategyConfig(
                inst_id="BTC-USDT-SWAP",
                bar="15m",
                ema_period=21,
                trend_ema_period=55,
                big_ema_period=233,
                atr_period=14,
                atr_stop_multiplier=Decimal("2"),
                atr_take_multiplier=Decimal("4"),
                order_size=Decimal("0"),
                trade_mode="cross",
                signal_mode="long_only",
                position_mode="net",
                environment="demo",
                tp_sl_trigger_type="mark",
                strategy_id=STRATEGY_DYNAMIC_ID,
                risk_amount=Decimal("100"),
            ),
            candle_limit=400,
        )

        report_text = format_backtest_report(result)

        self.assertIn("趋势过滤：EMA21 与 EMA55 组成趋势过滤", report_text)
        self.assertIn("挂单参考EMA：EMA21", report_text)
        self.assertIn("止盈方式：固定止盈", report_text)
        self.assertIn("止盈说明：固定止盈为 ATR 倍数止盈。", report_text)
        self.assertIn("同K线撮合：阳线按 O→L→H→C，阴线按 O→H→L→C，十字线不做同K线平仓", report_text)

    def test_ema5_ema8_backtest_uses_dynamic_ema_stop(self) -> None:
        warmup_candles = [
            Candle(index, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True)
            for index in range(1, BACKTEST_RESERVED_CANDLES + 1)
        ]
        trade_closes = ["100", "100", "100", "103", "100", "97"]
        trade_candles: list[Candle] = []
        previous_close = Decimal(trade_closes[0])
        for index, raw_close in enumerate(trade_closes, start=1):
            close = Decimal(raw_close)
            open_price = previous_close
            high = max(open_price, close) + Decimal("1")
            low = min(open_price, close) - Decimal("1")
            trade_candles.append(
                Candle(
                    BACKTEST_RESERVED_CANDLES + index,
                    open_price,
                    high,
                    low,
                    close,
                    Decimal("1"),
                    True,
                )
            )
            previous_close = close
        candles = warmup_candles + trade_candles
        client = DummyBacktestClient(candles, self._build_instrument())

        result = run_backtest(client, self._build_ema5_ema8_config(), candle_limit=len(candles))

        self.assertGreaterEqual(len(result.trades), 1)
        self.assertEqual(result.trades[0].signal, "long")
        self.assertEqual(result.trades[0].exit_reason, "stop_loss")
        report_text = format_backtest_report(result)
        self.assertIn("EMA2/EMA3", report_text)
        self.assertIn("动态止损", report_text)

    def test_same_candle_short_fill_on_bullish_candle_does_not_take_profit_before_entry(self) -> None:
        position = _OpenPosition(
            signal="short",
            entry_index=10,
            entry_ts=1710976500000,
            entry_price=Decimal("105"),
            stop_loss=Decimal("115"),
            take_profit=Decimal("95"),
            size=Decimal("1"),
        )
        candle = Candle(
            1710977400000,
            Decimal("100"),
            Decimal("120"),
            Decimal("90"),
            Decimal("110"),
            Decimal("1"),
            True,
        )

        trade = _try_close_position_same_candle_after_fill(position, candle, 10)

        self.assertIsNotNone(trade)
        assert trade is not None
        self.assertEqual(trade.exit_reason, "stop_loss")
        self.assertEqual(trade.exit_price, Decimal("115"))
        self.assertLess(trade.pnl, Decimal("0"))

    def test_same_candle_short_fill_on_bearish_candle_can_take_profit_after_entry(self) -> None:
        position = _OpenPosition(
            signal="short",
            entry_index=10,
            entry_ts=1710976500000,
            entry_price=Decimal("105"),
            stop_loss=Decimal("115"),
            take_profit=Decimal("95"),
            size=Decimal("1"),
        )
        candle = Candle(
            1710977400000,
            Decimal("100"),
            Decimal("110"),
            Decimal("90"),
            Decimal("95"),
            Decimal("1"),
            True,
        )

        trade = _try_close_position_same_candle_after_fill(position, candle, 10)

        self.assertIsNotNone(trade)
        assert trade is not None
        self.assertEqual(trade.exit_reason, "take_profit")
        self.assertEqual(trade.exit_price, Decimal("95"))
        self.assertGreater(trade.pnl, Decimal("0"))

    def test_same_candle_fill_on_doji_does_not_close_position(self) -> None:
        position = _OpenPosition(
            signal="long",
            entry_index=10,
            entry_ts=1710976500000,
            entry_price=Decimal("95"),
            stop_loss=Decimal("90"),
            take_profit=Decimal("105"),
            size=Decimal("1"),
        )
        candle = Candle(
            1710977400000,
            Decimal("100"),
            Decimal("110"),
            Decimal("90"),
            Decimal("100"),
            Decimal("1"),
            True,
        )

        trade = _try_close_position_same_candle_after_fill(position, candle, 10)

        self.assertIsNone(trade)

    def test_same_candle_immediate_open_fill_uses_execution_path_price(self) -> None:
        position = _OpenPosition(
            signal="long",
            entry_index=10,
            entry_ts=1710976500000,
            entry_price=Decimal("95"),
            entry_price_raw=Decimal("100"),
            entry_path_price=Decimal("95"),
            stop_loss=Decimal("94"),
            take_profit=Decimal("110"),
            initial_stop_loss=Decimal("94"),
            initial_take_profit=Decimal("110"),
            size=Decimal("1"),
            tick_size=Decimal("0.1"),
        )
        candle = Candle(
            1710977400000,
            Decimal("95"),
            Decimal("99"),
            Decimal("94"),
            Decimal("98"),
            Decimal("1"),
            True,
        )

        trade = _try_close_position_same_candle_after_fill(position, candle, 10)

        self.assertIsNotNone(trade)
        assert trade is not None
        self.assertEqual(trade.exit_reason, "stop_loss")
        self.assertEqual(trade.exit_price, Decimal("94"))

    def test_close_position_subtracts_maker_and_taker_fees_from_trade_pnl(self) -> None:
        position = _OpenPosition(
            signal="long",
            entry_index=10,
            entry_ts=1710976500000,
            entry_price=Decimal("100"),
            stop_loss=Decimal("95"),
            take_profit=Decimal("110"),
            size=Decimal("2"),
            entry_fee_rate=Decimal("0.001"),
            entry_fee_type="maker",
        )
        candle = Candle(
            1710977400000,
            Decimal("101"),
            Decimal("111"),
            Decimal("96"),
            Decimal("109"),
            Decimal("1"),
            True,
        )

        trade = _try_close_position(
            position,
            candle,
            11,
            exit_fee_rate=Decimal("0.002"),
            exit_fee_type="taker",
        )

        self.assertIsNotNone(trade)
        assert trade is not None
        self.assertEqual(trade.gross_pnl, Decimal("20"))
        self.assertEqual(trade.entry_fee, Decimal("0.200"))
        self.assertEqual(trade.exit_fee, Decimal("0.440"))
        self.assertEqual(trade.total_fee, Decimal("0.640"))
        self.assertEqual(trade.pnl, Decimal("19.360"))
        self.assertEqual(trade.entry_fee_type, "maker")
        self.assertEqual(trade.exit_fee_type, "taker")

    def test_backtest_candle_color_uses_green_for_up_and_red_for_down(self) -> None:
        self.assertEqual(_backtest_candle_color(Decimal("100"), Decimal("101")), "#1a7f37")
        self.assertEqual(_backtest_candle_color(Decimal("100"), Decimal("99")), "#d1242f")

    def test_build_backtest_compare_row_contains_key_metrics(self) -> None:
        report = BacktestReport(
            total_trades=3,
            win_trades=2,
            loss_trades=1,
            breakeven_trades=0,
            win_rate=Decimal("66.67"),
            total_pnl=Decimal("123.4567"),
            average_pnl=Decimal("41.1522"),
            gross_profit=Decimal("200"),
            gross_loss=Decimal("76.5433"),
            profit_factor=Decimal("2.61"),
            average_win=Decimal("100"),
            average_loss=Decimal("76.5433"),
            profit_loss_ratio=Decimal("1.31"),
            average_r_multiple=Decimal("0.9"),
            max_drawdown=Decimal("55.4321"),
            take_profit_hits=2,
            stop_loss_hits=1,
        )
        snapshot = _BacktestSnapshot(
            snapshot_id="R001",
            created_at=datetime(2026, 3, 23, 12, 30, 45),
            config=StrategyConfig(
                inst_id="BTC-USDT-SWAP",
                bar="15m",
                ema_period=21,
                trend_ema_period=55,
                atr_period=14,
                atr_stop_multiplier=Decimal("2"),
                atr_take_multiplier=Decimal("4"),
                order_size=Decimal("0"),
                trade_mode="cross",
                signal_mode="long_only",
                position_mode="net",
                environment="demo",
                tp_sl_trigger_type="mark",
                strategy_id=STRATEGY_DYNAMIC_ID,
                risk_amount=Decimal("100"),
            ),
            candle_limit=500,
            candle_count=500,
            report=report,
            report_text="绀轰緥鎶ュ憡",
            start_ts=1711152000000,
            end_ts=1711238400000,
            result=BacktestResult(
                candles=[],
                trades=[],
                report=report,
                instrument=self._build_instrument(),
                ema_period=21,
                trend_ema_period=55,
                strategy_id=STRATEGY_DYNAMIC_ID,
            ),
        )

        row = _build_backtest_compare_row(snapshot)

        self.assertEqual(row[0], "R001")
        self.assertEqual(row[2], "2024-03-23 08:00")
        self.assertEqual(row[3], "2024-03-24 08:00")
        self.assertIn("EMA 动态委托", row[4])
        self.assertEqual(row[5], "BTC-USDT-SWAP")
        self.assertEqual(row[6], "15分钟")
        self.assertIn("EMA21", row[7])
        self.assertEqual(row[8], "3")
        self.assertEqual(row[9], "66.67%")

    def test_build_backtest_compare_detail_contains_snapshot_metadata(self) -> None:
        report = BacktestReport(
            total_trades=0,
            win_trades=0,
            loss_trades=0,
            breakeven_trades=0,
            win_rate=Decimal("0"),
            total_pnl=Decimal("0"),
            average_pnl=Decimal("0"),
            gross_profit=Decimal("0"),
            gross_loss=Decimal("0"),
            profit_factor=None,
            average_win=Decimal("0"),
            average_loss=Decimal("0"),
            profit_loss_ratio=None,
            average_r_multiple=Decimal("0"),
            max_drawdown=Decimal("0"),
            take_profit_hits=0,
            stop_loss_hits=0,
        )
        snapshot = _BacktestSnapshot(
            snapshot_id="R009",
            created_at=datetime(2026, 3, 23, 8, 0, 0),
            config=StrategyConfig(
                inst_id="ETH-USDT-SWAP",
                bar="1H",
                ema_period=34,
                trend_ema_period=89,
                atr_period=14,
                atr_stop_multiplier=Decimal("1.5"),
                atr_take_multiplier=Decimal("3"),
                order_size=Decimal("0"),
                trade_mode="cross",
                signal_mode="short_only",
                position_mode="net",
                environment="demo",
                tp_sl_trigger_type="mark",
                strategy_id=STRATEGY_EMA_BREAKDOWN_SHORT_ID,
                risk_amount=Decimal("200"),
            ),
            candle_limit=800,
            candle_count=800,
            report=report,
            report_text="示例详情",
            start_ts=1711065600000,
            end_ts=1711152000000,
            result=BacktestResult(
                candles=[],
                trades=[],
                report=report,
                instrument=self._build_instrument(),
                ema_period=34,
                trend_ema_period=89,
                strategy_id=STRATEGY_EMA_BREAKDOWN_SHORT_ID,
            ),
        )

        detail = _build_backtest_compare_detail(snapshot)

        self.assertIn("编号：R009", detail)
        self.assertIn("策略：EMA 跌破做空策略", detail)
        self.assertIn("K线周期：1小时", detail)
        self.assertIn("开始时间：2024-03-22 08:00", detail)
        self.assertIn("结束时间：2024-03-23 08:00", detail)
        self.assertIn("回测K线数：800", detail)
        self.assertIn("方向只做空", detail)

    def test_build_backtest_compare_detail_formats_zero_candle_limit_as_full_history(self) -> None:
        report = BacktestReport(
            total_trades=0,
            win_trades=0,
            loss_trades=0,
            breakeven_trades=0,
            win_rate=Decimal("0"),
            total_pnl=Decimal("0"),
            average_pnl=Decimal("0"),
            gross_profit=Decimal("0"),
            gross_loss=Decimal("0"),
            profit_factor=None,
            average_win=Decimal("0"),
            average_loss=Decimal("0"),
            profit_loss_ratio=None,
            average_r_multiple=Decimal("0"),
            max_drawdown=Decimal("0"),
            take_profit_hits=0,
            stop_loss_hits=0,
        )
        snapshot = _BacktestSnapshot(
            snapshot_id="R011",
            created_at=datetime(2026, 3, 23, 8, 30, 0),
            config=StrategyConfig(
                inst_id="BTC-USDT-SWAP",
                bar="15m",
                ema_period=21,
                trend_ema_period=55,
                atr_period=14,
                atr_stop_multiplier=Decimal("1"),
                atr_take_multiplier=Decimal("2"),
                order_size=Decimal("0"),
                trade_mode="cross",
                signal_mode="long_only",
                position_mode="net",
                environment="demo",
                tp_sl_trigger_type="mark",
                strategy_id=STRATEGY_EMA_BREAKOUT_LONG_ID,
                risk_amount=Decimal("100"),
            ),
            candle_limit=0,
            candle_count=1200,
            report=report,
            report_text="示例详情",
            start_ts=1711065600000,
            end_ts=1711152000000,
            result=BacktestResult(
                candles=[],
                trades=[],
                report=report,
                instrument=self._build_instrument(),
                ema_period=21,
                trend_ema_period=55,
                strategy_id=STRATEGY_EMA_BREAKOUT_LONG_ID,
            ),
        )

        detail = _build_backtest_compare_detail(snapshot)

        self.assertIn("回测K线数：全量", detail)

    def test_build_backtest_compare_detail_includes_audit_file_paths(self) -> None:
        report = BacktestReport(
            total_trades=0,
            win_trades=0,
            loss_trades=0,
            breakeven_trades=0,
            win_rate=Decimal("0"),
            total_pnl=Decimal("0"),
            average_pnl=Decimal("0"),
            gross_profit=Decimal("0"),
            gross_loss=Decimal("0"),
            profit_factor=None,
            average_win=Decimal("0"),
            average_loss=Decimal("0"),
            profit_loss_ratio=None,
            average_r_multiple=Decimal("0"),
            max_drawdown=Decimal("0"),
            take_profit_hits=0,
            stop_loss_hits=0,
        )
        with TemporaryDirectory() as temp_dir:
            report_path = Path(temp_dir) / "demo.txt"
            report_path.write_text("demo", encoding="utf-8")
            artifact_paths = single_backtest_artifact_paths(report_path)
            artifact_paths["capital"].write_text("header\n", encoding="utf-8-sig")
            artifact_paths["operations"].write_text("header\n", encoding="utf-8-sig")
            artifact_paths["manifest"].write_text("{}", encoding="utf-8")
            snapshot = _BacktestSnapshot(
                snapshot_id="R010",
                created_at=datetime(2026, 3, 23, 9, 0, 0),
                config=StrategyConfig(
                    inst_id="ETH-USDT-SWAP",
                    bar="1H",
                    ema_period=34,
                    trend_ema_period=89,
                    atr_period=14,
                    atr_stop_multiplier=Decimal("1.5"),
                    atr_take_multiplier=Decimal("3"),
                    order_size=Decimal("0"),
                    trade_mode="cross",
                    signal_mode="short_only",
                    position_mode="net",
                    environment="demo",
                    tp_sl_trigger_type="mark",
                    strategy_id=STRATEGY_EMA_BREAKDOWN_SHORT_ID,
                    risk_amount=Decimal("200"),
                ),
                candle_limit=800,
                candle_count=800,
                report=report,
                report_text="????",
                start_ts=1711065600000,
                end_ts=1711152000000,
                result=BacktestResult(
                    candles=[],
                    trades=[],
                    report=report,
                    instrument=self._build_instrument(),
                    ema_period=34,
                    trend_ema_period=89,
                    strategy_id=STRATEGY_EMA_BREAKDOWN_SHORT_ID,
                ),
                export_path=str(report_path),
            )

            detail = _build_backtest_compare_detail(snapshot)

            self.assertIn("\u62a5\u544a\u6587\u4ef6\uff1a", detail)
            self.assertIn("\u8d44\u91d1\u5ba1\u8ba1\uff1a", detail)
            self.assertIn("\u64cd\u4f5c\u65e5\u5fd7\uff1a", detail)
            self.assertIn("\u5ba1\u8ba1\u6e05\u5355\uff1a", detail)

    def test_batch_mode_for_strategy_pool_snapshot_returns_strategy_pool(self) -> None:
        snapshot = _BacktestSnapshot(
            snapshot_id="R888",
            created_at=datetime(2026, 4, 16, 21, 0, 0),
            config=StrategyConfig(
                inst_id="BTC-USDT-SWAP",
                bar="5m",
                ema_period=21,
                trend_ema_period=55,
                big_ema_period=0,
                atr_period=10,
                atr_stop_multiplier=Decimal("1"),
                atr_take_multiplier=Decimal("2"),
                order_size=Decimal("0.5"),
                trade_mode="cross",
                signal_mode="long_only",
                position_mode="net",
                environment="demo",
                tp_sl_trigger_type="mark",
                strategy_id=STRATEGY_DYNAMIC_ID,
                risk_amount=None,
                max_entries_per_trend=10,
                backtest_sizing_mode="fixed_size",
                backtest_profile_id="03_balanced_trend",
                backtest_profile_name="均衡 21/55",
                backtest_profile_summary="demo",
            ),
            candle_limit=500,
            candle_count=500,
            report=BacktestReport(
                total_trades=0,
                win_trades=0,
                loss_trades=0,
                breakeven_trades=0,
                win_rate=Decimal("0"),
                total_pnl=Decimal("0"),
                average_pnl=Decimal("0"),
                gross_profit=Decimal("0"),
                gross_loss=Decimal("0"),
                profit_factor=None,
                average_win=Decimal("0"),
                average_loss=Decimal("0"),
                profit_loss_ratio=None,
                average_r_multiple=Decimal("0"),
                max_drawdown=Decimal("0"),
            ),
            report_text="demo",
            result=None,
        )

        self.assertEqual(backtest_ui_module._batch_mode_for_snapshots([snapshot]), "strategy_pool")

    def test_backtest_report_contains_fee_lines(self) -> None:
        report = BacktestReport(
            total_trades=1,
            win_trades=1,
            loss_trades=0,
            breakeven_trades=0,
            win_rate=Decimal("100"),
            total_pnl=Decimal("9.36"),
            average_pnl=Decimal("9.36"),
            gross_profit=Decimal("9.36"),
            gross_loss=Decimal("0"),
            profit_factor=None,
            average_win=Decimal("9.36"),
            average_loss=Decimal("0"),
            profit_loss_ratio=None,
            average_r_multiple=Decimal("1.87"),
            max_drawdown=Decimal("0"),
            take_profit_hits=1,
            stop_loss_hits=0,
            maker_fees=Decimal("0.20"),
            taker_fees=Decimal("0.44"),
            total_fees=Decimal("0.64"),
        )
        result = BacktestResult(
            candles=[
                Candle(1710976500000, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True),
                Candle(1711062900000, Decimal("101"), Decimal("102"), Decimal("100"), Decimal("101"), Decimal("1"), True),
            ],
            trades=[],
            report=report,
            instrument=self._build_instrument(),
            ema_period=21,
            trend_ema_period=55,
            strategy_id=STRATEGY_DYNAMIC_ID,
            maker_fee_rate=Decimal("0.0002"),
            taker_fee_rate=Decimal("0.0005"),
            entry_slippage_rate=Decimal("0.0003"),
            exit_slippage_rate=Decimal("0.0005"),
        )

        report_text = format_backtest_report(result)

        self.assertIn("Maker手续费：0.0200%", report_text)
        self.assertIn("Taker手续费：0.0500%", report_text)
        self.assertIn("开仓滑点：0.0300%", report_text)
        self.assertIn("平仓滑点：0.0500%", report_text)
        self.assertIn("手续费合计：0.6400", report_text)
        self.assertIn("手续费前盈亏：10.0000", report_text)
        self.assertIn("平均单笔手续费：0.6400", report_text)
        self.assertIn("手续费占手续费前盈亏：6.40%", report_text)
        self.assertIn("手续费占净盈亏绝对值：6.84%", report_text)

    def test_export_single_backtest_report_writes_file(self) -> None:
        with TemporaryDirectory() as temp_dir:
            original_export_dir = backtest_export_module.backtest_report_export_dir_path
            backtest_export_module.backtest_report_export_dir_path = lambda base_dir=None: Path(temp_dir)
            try:
                config = StrategyConfig(
                    inst_id="BTC-USDT-SWAP",
                    bar="1H",
                    ema_period=21,
                    trend_ema_period=55,
                    atr_period=10,
                    atr_stop_multiplier=Decimal("1.5"),
                    atr_take_multiplier=Decimal("4.5"),
                    order_size=Decimal("0"),
                    trade_mode="cross",
                    signal_mode="long_only",
                    position_mode="net",
                    environment="demo",
                    tp_sl_trigger_type="mark",
                    strategy_id=STRATEGY_DYNAMIC_ID,
                    risk_amount=Decimal("100"),
                    backtest_entry_slippage_rate=Decimal("0.0003"),
                    backtest_exit_slippage_rate=Decimal("0.0005"),
                )
                result = BacktestResult(
                    candles=[
                        Candle(1710976500000, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True),
                        Candle(1711062900000, Decimal("101"), Decimal("102"), Decimal("100"), Decimal("101"), Decimal("1"), True),
                    ],
                    trades=[],
                    report=BacktestReport(
                        total_trades=1,
                        win_trades=1,
                        loss_trades=0,
                        breakeven_trades=0,
                        win_rate=Decimal("100"),
                        total_pnl=Decimal("12.34"),
                        average_pnl=Decimal("12.34"),
                        gross_profit=Decimal("12.34"),
                        gross_loss=Decimal("0"),
                        profit_factor=None,
                        average_win=Decimal("12.34"),
                        average_loss=Decimal("0"),
                        profit_loss_ratio=None,
                        average_r_multiple=Decimal("1.2"),
                        max_drawdown=Decimal("0"),
                        take_profit_hits=1,
                        stop_loss_hits=0,
                        maker_fees=Decimal("0.2"),
                        taker_fees=Decimal("0.5"),
                        total_fees=Decimal("0.7"),
                    ),
                    instrument=self._build_instrument(),
                    ema_period=21,
                    trend_ema_period=55,
                    strategy_id=STRATEGY_DYNAMIC_ID,
                    data_source_note="cache hit 9988 | latest 12 | total 10000",
                    maker_fee_rate=Decimal("0.0002"),
                    taker_fee_rate=Decimal("0.0005"),
                    entry_slippage_rate=Decimal("0.0003"),
                    exit_slippage_rate=Decimal("0.0005"),
                )

                exported = export_single_backtest_report(
                    result,
                    config,
                    10000,
                    exported_at=datetime(2026, 3, 26, 20, 30, 0),
                )

                self.assertTrue(exported.exists())
                self.assertIn("single_20260326_203000", exported.name)
                content = exported.read_text(encoding="utf-8-sig")
                self.assertIn("BTC-USDT-SWAP", content)
                self.assertIn("EMA21", content)
                self.assertIn("10000", content)
                self.assertIn("0.0200%", content)
                self.assertIn("开滑0.0300%", content)
                self.assertIn("平滑0.0500%", content)
                artifact_paths = single_backtest_artifact_paths(exported)
                self.assertTrue(artifact_paths["capital"].exists())
                self.assertTrue(artifact_paths["operations"].exists())
                self.assertTrue(artifact_paths["manifest"].exists())
                self.assertIn("marked_equity_liquidation_basis", artifact_paths["capital"].read_text(encoding="utf-8-sig"))
                self.assertIn("event_seq", artifact_paths["operations"].read_text(encoding="utf-8-sig"))
                manifest_payload = json.loads(artifact_paths["manifest"].read_text(encoding="utf-8"))
                self.assertEqual(manifest_payload["export_scope"], "single")
                self.assertTrue(manifest_payload["files"]["report"]["exists"])
            finally:
                backtest_export_module.backtest_report_export_dir_path = original_export_dir

    def test_export_single_backtest_report_formats_zero_candle_limit_as_full_history(self) -> None:
        with TemporaryDirectory() as temp_dir:
            original_export_dir = backtest_export_module.backtest_report_export_dir_path
            backtest_export_module.backtest_report_export_dir_path = lambda base_dir=None: Path(temp_dir)
            try:
                config = StrategyConfig(
                    inst_id="BTC-USDT-SWAP",
                    bar="1H",
                    ema_period=21,
                    trend_ema_period=55,
                    atr_period=10,
                    atr_stop_multiplier=Decimal("1.5"),
                    atr_take_multiplier=Decimal("4.5"),
                    order_size=Decimal("0"),
                    trade_mode="cross",
                    signal_mode="long_only",
                    position_mode="net",
                    environment="demo",
                    tp_sl_trigger_type="mark",
                    strategy_id=STRATEGY_DYNAMIC_ID,
                    risk_amount=Decimal("100"),
                )
                result = BacktestResult(
                    candles=[
                        Candle(1710976500000, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True),
                        Candle(1711062900000, Decimal("101"), Decimal("102"), Decimal("100"), Decimal("101"), Decimal("1"), True),
                    ],
                    trades=[],
                    report=BacktestReport(
                        total_trades=0,
                        win_trades=0,
                        loss_trades=0,
                        breakeven_trades=0,
                        win_rate=Decimal("0"),
                        total_pnl=Decimal("0"),
                        average_pnl=Decimal("0"),
                        gross_profit=Decimal("0"),
                        gross_loss=Decimal("0"),
                        profit_factor=None,
                        average_win=Decimal("0"),
                        average_loss=Decimal("0"),
                        profit_loss_ratio=None,
                        average_r_multiple=Decimal("0"),
                        max_drawdown=Decimal("0"),
                    ),
                    instrument=self._build_instrument(),
                    ema_period=21,
                    trend_ema_period=55,
                    strategy_id=STRATEGY_DYNAMIC_ID,
                )

                exported = export_single_backtest_report(
                    result,
                    config,
                    0,
                    exported_at=datetime(2026, 3, 26, 20, 45, 0),
                )

                content = exported.read_text(encoding="utf-8-sig")
                self.assertIn("回测K线数：全量", content)
            finally:
                backtest_export_module.backtest_report_export_dir_path = original_export_dir

    def test_deserialize_strategy_config_legacy_single_slippage_maps_to_entry_and_exit(self) -> None:
        config = backtest_ui_module._deserialize_strategy_config(
            {
                "inst_id": "BTC-USDT-SWAP",
                "bar": "1H",
                "ema_period": 21,
                "trend_ema_period": 55,
                "big_ema_period": 233,
                "entry_reference_ema_period": 55,
                "atr_period": 10,
                "atr_stop_multiplier": "1.5",
                "atr_take_multiplier": "4.5",
                "order_size": "0",
                "trade_mode": "cross",
                "signal_mode": "long_only",
                "position_mode": "net",
                "environment": "demo",
                "tp_sl_trigger_type": "mark",
                "strategy_id": STRATEGY_DYNAMIC_ID,
                "risk_amount": "100",
                "backtest_slippage_rate": "0.0005",
            }
        )

        self.assertEqual(config.resolved_backtest_entry_slippage_rate(), Decimal("0.0005"))
        self.assertEqual(config.resolved_backtest_exit_slippage_rate(), Decimal("0.0005"))

    def test_export_batch_backtest_report_writes_matrix_summary(self) -> None:
        with TemporaryDirectory() as temp_dir:
            original_export_dir = backtest_export_module.backtest_report_export_dir_path
            backtest_export_module.backtest_report_export_dir_path = lambda base_dir=None: Path(temp_dir)
            try:
                base_config = StrategyConfig(
                    inst_id="ETH-USDT-SWAP",
                    bar="4H",
                    ema_period=21,
                    trend_ema_period=55,
                    atr_period=10,
                    atr_stop_multiplier=Decimal("1"),
                    atr_take_multiplier=Decimal("1"),
                    order_size=Decimal("0"),
                    trade_mode="cross",
                    signal_mode="short_only",
                    position_mode="net",
                    environment="demo",
                    tp_sl_trigger_type="mark",
                    strategy_id=STRATEGY_DYNAMIC_ID,
                    risk_amount=Decimal("100"),
                    take_profit_mode="fixed",
                )
                results = []
                for take_multiplier, total_pnl in (
                    (Decimal("1"), Decimal("100")),
                    (Decimal("2"), Decimal("200")),
                    (Decimal("3"), Decimal("300")),
                ):
                    config = StrategyConfig(
                        inst_id=base_config.inst_id,
                        bar=base_config.bar,
                        ema_period=base_config.ema_period,
                        trend_ema_period=base_config.trend_ema_period,
                        atr_period=base_config.atr_period,
                        atr_stop_multiplier=Decimal("1"),
                        atr_take_multiplier=take_multiplier,
                        order_size=base_config.order_size,
                        trade_mode=base_config.trade_mode,
                        signal_mode=base_config.signal_mode,
                        position_mode=base_config.position_mode,
                        environment=base_config.environment,
                        tp_sl_trigger_type=base_config.tp_sl_trigger_type,
                        strategy_id=base_config.strategy_id,
                        risk_amount=base_config.risk_amount,
                        take_profit_mode="fixed",
                    )
                    result = BacktestResult(
                        candles=[
                            Candle(1710976500000, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True),
                            Candle(1711062900000, Decimal("101"), Decimal("102"), Decimal("100"), Decimal("101"), Decimal("1"), True),
                        ],
                        trades=[],
                        report=BacktestReport(
                            total_trades=10,
                            win_trades=4,
                            loss_trades=6,
                            breakeven_trades=0,
                            win_rate=Decimal("40"),
                            total_pnl=total_pnl,
                            average_pnl=Decimal("10"),
                            gross_profit=Decimal("150"),
                            gross_loss=Decimal("50"),
                            profit_factor=Decimal("3"),
                            average_win=Decimal("37.5"),
                            average_loss=Decimal("8.3333"),
                            profit_loss_ratio=Decimal("4.5"),
                            average_r_multiple=Decimal("0.5"),
                            max_drawdown=Decimal("20"),
                            take_profit_hits=4,
                            stop_loss_hits=6,
                        ),
                        instrument=self._build_instrument(),
                        ema_period=21,
                        trend_ema_period=55,
                        strategy_id=STRATEGY_DYNAMIC_ID,
                        maker_fee_rate=Decimal("0.0002"),
                        taker_fee_rate=Decimal("0.0005"),
                        take_profit_mode="fixed",
                    )
                    results.append((config, result))

                exported = export_batch_backtest_report(
                    results,
                    10000,
                    batch_label="B001",
                    exported_at=datetime(2026, 3, 26, 21, 0, 0),
                )

                self.assertTrue(exported.exists())
                self.assertIn("batch_20260326_210000", exported.name)
                content = exported.read_text(encoding="utf-8-sig")
                self.assertIn("ETH-USDT-SWAP", content)
                self.assertIn("SL \\\\ TP", content)
                self.assertIn("TP = SL x2", content)
                self.assertIn("300.0000", content)
                artifact_paths = batch_backtest_artifact_paths(exported)
                self.assertTrue(artifact_paths["manifest"].exists())
                self.assertTrue(artifact_paths["detail_dir"].exists())
                self.assertEqual(len(list(artifact_paths["detail_dir"].glob("*.txt"))), len(results))
                self.assertEqual(len(list(artifact_paths["detail_dir"].glob("*.capital.csv"))), len(results))
                manifest_payload = json.loads(artifact_paths["manifest"].read_text(encoding="utf-8"))
                self.assertEqual(manifest_payload["export_scope"], "batch")
                self.assertEqual(manifest_payload["result_count"], len(results))
            finally:
                backtest_export_module.backtest_report_export_dir_path = original_export_dir

    def test_export_batch_backtest_report_writes_strategy_pool_summary(self) -> None:
        with TemporaryDirectory() as temp_dir:
            original_export_dir = backtest_export_module.backtest_report_export_dir_path
            backtest_export_module.backtest_report_export_dir_path = lambda base_dir=None: Path(temp_dir)
            try:
                results: list[tuple[StrategyConfig, BacktestResult]] = []
                for profile_id, profile_name, total_pnl in (
                    ("01_fast_breakout", "快突破 9/21", Decimal("88")),
                    ("03_balanced_trend", "均衡 21/55", Decimal("132")),
                ):
                    config = StrategyConfig(
                        inst_id="BTC-USDT-SWAP",
                        bar="5m",
                        ema_period=9 if profile_id == "01_fast_breakout" else 21,
                        trend_ema_period=21 if profile_id == "01_fast_breakout" else 55,
                        big_ema_period=0,
                        atr_period=7 if profile_id == "01_fast_breakout" else 10,
                        atr_stop_multiplier=Decimal("0.8") if profile_id == "01_fast_breakout" else Decimal("1"),
                        atr_take_multiplier=Decimal("1.6") if profile_id == "01_fast_breakout" else Decimal("2"),
                        order_size=Decimal("0.5"),
                        trade_mode="cross",
                        signal_mode="long_only",
                        position_mode="net",
                        environment="demo",
                        tp_sl_trigger_type="mark",
                        strategy_id=STRATEGY_DYNAMIC_ID,
                        risk_amount=None,
                        max_entries_per_trend=10,
                        backtest_sizing_mode="fixed_size",
                        backtest_profile_id=profile_id,
                        backtest_profile_name=profile_name,
                        backtest_profile_summary="demo profile",
                    )
                    result = BacktestResult(
                        candles=[
                            Candle(1710976500000, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True),
                            Candle(1711062900000, Decimal("101"), Decimal("102"), Decimal("100"), Decimal("101"), Decimal("1"), True),
                        ],
                        trades=[],
                        report=BacktestReport(
                            total_trades=8,
                            win_trades=4,
                            loss_trades=4,
                            breakeven_trades=0,
                            win_rate=Decimal("50"),
                            total_pnl=total_pnl,
                            average_pnl=total_pnl / Decimal("8"),
                            gross_profit=total_pnl + Decimal("20"),
                            gross_loss=Decimal("20"),
                            profit_factor=Decimal("5.4"),
                            average_win=Decimal("27"),
                            average_loss=Decimal("5"),
                            profit_loss_ratio=Decimal("5.4"),
                            average_r_multiple=Decimal("0.8"),
                            max_drawdown=Decimal("15"),
                            manual_handoffs=2,
                            manual_open_positions=1,
                            manual_open_size=Decimal("0.5"),
                            manual_open_pnl=Decimal("-1.2"),
                            max_manual_positions=2,
                            max_total_occupied_slots=4,
                        ),
                        instrument=self._build_instrument(),
                        strategy_id=STRATEGY_DYNAMIC_ID,
                        max_entries_per_trend=10,
                        backtest_profile_id=profile_id,
                        backtest_profile_name=profile_name,
                        backtest_profile_summary="demo profile",
                    )
                    results.append((config, result))

                exported = export_batch_backtest_report(
                    results,
                    10000,
                    batch_label="B777",
                    exported_at=datetime(2026, 4, 16, 22, 0, 0),
                )

                content = exported.read_text(encoding="utf-8-sig")
                self.assertIn("候选策略 | 参数 | 总盈亏 | 胜率 | 交易数 | PF | 平均R", content)
                self.assertIn("快突破 9/21", content)
                self.assertIn("均衡 21/55", content)
                manifest_payload = json.loads(batch_backtest_artifact_paths(exported)["manifest"].read_text(encoding="utf-8"))
                self.assertEqual(manifest_payload["results"][0]["strategy"]["backtest_profile_name"], "快突破 9/21")
            finally:
                backtest_export_module.backtest_report_export_dir_path = original_export_dir

    def test_backtest_snapshot_store_persists_records_to_disk(self) -> None:
        with TemporaryDirectory() as temp_dir:
            history_path = Path(temp_dir) / ".okx_quant_backtest_history.json"
            original_path_factory = backtest_ui_module.backtest_history_file_path
            backtest_ui_module.backtest_history_file_path = lambda: history_path
            try:
                store = _BacktestSnapshotStore()
                store.clear()
                result = BacktestResult(
                    candles=[
                        Candle(1710976500000, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True),
                        Candle(1711062900000, Decimal("101"), Decimal("102"), Decimal("100"), Decimal("101"), Decimal("1"), True),
                    ],
                    trades=[],
                    report=BacktestReport(
                        total_trades=1,
                        win_trades=1,
                        loss_trades=0,
                        breakeven_trades=0,
                        win_rate=Decimal("100"),
                        total_pnl=Decimal("12.34"),
                        average_pnl=Decimal("12.34"),
                        gross_profit=Decimal("12.34"),
                        gross_loss=Decimal("0"),
                        profit_factor=None,
                        average_win=Decimal("12.34"),
                        average_loss=Decimal("0"),
                        profit_loss_ratio=None,
                        average_r_multiple=Decimal("1.2"),
                        max_drawdown=Decimal("0"),
                        take_profit_hits=1,
                        stop_loss_hits=0,
                        manual_handoffs=2,
                        manual_open_positions=1,
                        manual_open_size=Decimal("0.5"),
                        manual_open_pnl=Decimal("-3.21"),
                        max_manual_positions=1,
                        max_total_occupied_slots=4,
                    ),
                    instrument=self._build_instrument(),
                    ema_period=21,
                    trend_ema_period=55,
                    strategy_id=STRATEGY_DYNAMIC_ID,
                )
                config = StrategyConfig(
                    inst_id="BTC-USDT-SWAP",
                    bar="15m",
                    ema_period=21,
                    trend_ema_period=55,
                    atr_period=14,
                    atr_stop_multiplier=Decimal("2"),
                    atr_take_multiplier=Decimal("4"),
                    order_size=Decimal("0"),
                    trade_mode="cross",
                    signal_mode="long_only",
                    position_mode="net",
                    environment="demo",
                    tp_sl_trigger_type="mark",
                    strategy_id=STRATEGY_DYNAMIC_ID,
                    risk_amount=Decimal("100"),
                )

                store.add_snapshot(result, config, 500, export_path="D:/qqokx/reports/backtest_exports/demo.txt")
                reloaded_store = _BacktestSnapshotStore()
                snapshots = reloaded_store.list_snapshots()

                self.assertEqual(len(snapshots), 1)
                self.assertEqual(snapshots[0].config.inst_id, "BTC-USDT-SWAP")
                self.assertEqual(snapshots[0].report.total_trades, 1)
                self.assertEqual(snapshots[0].report.manual_handoffs, 2)
                self.assertEqual(snapshots[0].report.manual_open_positions, 1)
                self.assertEqual(snapshots[0].report.manual_open_size, Decimal("0.5"))
                self.assertEqual(snapshots[0].report.manual_open_pnl, Decimal("-3.21"))
                self.assertEqual(snapshots[0].report.max_total_occupied_slots, 4)
                self.assertEqual(snapshots[0].start_ts, 1710976500000)
                self.assertEqual(snapshots[0].end_ts, 1711062900000)
                self.assertIn("趋势过滤", snapshots[0].report_text)
            finally:
                backtest_ui_module.backtest_history_file_path = original_path_factory

    def test_has_extension_stats_returns_false_for_plain_results(self) -> None:
        result = BacktestResult(
            candles=[],
            trades=[],
            report=BacktestReport(
                total_trades=1,
                win_trades=1,
                loss_trades=0,
                breakeven_trades=0,
                win_rate=Decimal("100"),
                total_pnl=Decimal("1"),
                average_pnl=Decimal("1"),
                gross_profit=Decimal("1"),
                gross_loss=Decimal("0"),
                profit_factor=None,
                average_win=Decimal("1"),
                average_loss=Decimal("0"),
                profit_loss_ratio=None,
                average_r_multiple=Decimal("1"),
                max_drawdown=Decimal("0"),
            ),
            instrument=self._build_instrument(),
            strategy_id=STRATEGY_DYNAMIC_ID,
        )

        self.assertFalse(_has_extension_stats(result))
        self.assertFalse(_has_extension_stats(None))

    def test_has_extension_stats_returns_true_when_manual_positions_exist(self) -> None:
        manual_position = BacktestManualPosition(
            signal="long",
            entry_index=1,
            handoff_index=2,
            entry_ts=1710976500000,
            handoff_ts=1710976800000,
            current_ts=1710977100000,
            entry_price=Decimal("100"),
            handoff_price=Decimal("99"),
            current_price=Decimal("98"),
            stop_loss=Decimal("95"),
            take_profit=Decimal("110"),
            size=Decimal("0.5"),
            gross_pnl=Decimal("-1"),
            pnl=Decimal("-1.1"),
            risk_value=Decimal("2"),
            r_multiple=Decimal("-0.55"),
            break_even_price=Decimal("100.2"),
            handoff_reason="demo",
        )
        result = BacktestResult(
            candles=[],
            trades=[],
            report=BacktestReport(
                total_trades=0,
                win_trades=0,
                loss_trades=0,
                breakeven_trades=0,
                win_rate=Decimal("0"),
                total_pnl=Decimal("0"),
                average_pnl=Decimal("0"),
                gross_profit=Decimal("0"),
                gross_loss=Decimal("0"),
                profit_factor=None,
                average_win=Decimal("0"),
                average_loss=Decimal("0"),
                profit_loss_ratio=None,
                average_r_multiple=Decimal("0"),
                max_drawdown=Decimal("0"),
                manual_handoffs=1,
                manual_open_positions=1,
                manual_open_size=Decimal("0.5"),
                manual_open_pnl=Decimal("-1.1"),
                max_manual_positions=1,
                max_total_occupied_slots=1,
            ),
            instrument=self._build_instrument(),
            strategy_id=STRATEGY_DYNAMIC_ID,
            manual_positions=[manual_position],
        )

        self.assertTrue(_has_extension_stats(result))

    def test_build_manual_pool_summary_and_row_for_slot_strategy(self) -> None:
        manual_position = BacktestManualPosition(
            signal="long",
            entry_index=3,
            handoff_index=5,
            entry_ts=1710976500000,
            handoff_ts=1710977400000,
            current_ts=1710978300000,
            entry_price=Decimal("100"),
            handoff_price=Decimal("98"),
            current_price=Decimal("96"),
            stop_loss=Decimal("95"),
            take_profit=Decimal("110"),
            size=Decimal("0.5"),
            gross_pnl=Decimal("-2"),
            pnl=Decimal("-2.3"),
            risk_value=Decimal("2.5"),
            r_multiple=Decimal("-0.92"),
            break_even_price=Decimal("100.8"),
            handoff_reason="close fell back below EMA21",
            atr_value=Decimal("5"),
            entry_fee=Decimal("0.1"),
            funding_cost=Decimal("0.2"),
        )
        result = BacktestResult(
            candles=[
                Candle(1710976500000, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True),
                Candle(1710978300000, Decimal("99"), Decimal("100"), Decimal("95"), Decimal("96"), Decimal("1"), True),
            ],
            trades=[],
            report=BacktestReport(
                total_trades=0,
                win_trades=0,
                loss_trades=0,
                breakeven_trades=0,
                win_rate=Decimal("0"),
                total_pnl=Decimal("0"),
                average_pnl=Decimal("0"),
                gross_profit=Decimal("0"),
                gross_loss=Decimal("0"),
                profit_factor=None,
                average_win=Decimal("0"),
                average_loss=Decimal("0"),
                profit_loss_ratio=None,
                average_r_multiple=Decimal("0"),
                max_drawdown=Decimal("0"),
                manual_handoffs=3,
                manual_open_positions=1,
                manual_open_size=Decimal("0.5"),
                manual_open_pnl=Decimal("-2.3"),
                max_manual_positions=2,
                max_total_occupied_slots=4,
            ),
            instrument=self._build_instrument(),
            strategy_id=STRATEGY_DYNAMIC_ID,
            max_entries_per_trend=10,
            manual_positions=[manual_position],
        )
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="5m",
            ema_period=21,
            trend_ema_period=55,
            atr_period=14,
            atr_stop_multiplier=Decimal("1"),
            atr_take_multiplier=Decimal("2"),
            order_size=Decimal("0.5"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id=STRATEGY_DYNAMIC_ID,
            risk_amount=None,
            max_entries_per_trend=10,
            backtest_sizing_mode="fixed_size",
        )

        summary_text = _build_manual_pool_summary(result, config)
        row = _build_manual_position_row(1, manual_position)
        filtered_empty_summary = _build_manual_pool_summary(
            result,
            config,
            visible_positions=[],
            filter_label="仅亏损仓",
        )
        loss_sort_summary = _build_manual_pool_summary(
            result,
            config,
            sort_label="浮亏最大",
        )

        self.assertIn("托管仓位：1 笔 / 0.5000", summary_text)
        self.assertIn("累计转托管：3", summary_text)
        self.assertIn("峰值占槽：4/10", summary_text)
        self.assertIn("开仓手续费：0.1000", summary_text)
        self.assertIn("资金费：0.2000", summary_text)
        self.assertIn("方向分组：做多 1 笔 / 0.5000 / 浮盈亏 -2.3000 / 最近保本 4.76%", summary_text)
        self.assertIn("最接近保本：4.76%", summary_text)
        self.assertIn("当前排序：浮亏最大", loss_sort_summary)
        self.assertIn("全池按浮亏从大到小排序", loss_sort_summary)
        self.assertIn("当前筛选：仅亏损仓 (0/1)", filtered_empty_summary)
        self.assertIn("当前筛选下暂无仓位。", filtered_empty_summary)
        self.assertEqual(row[0], "1")
        self.assertEqual(row[1], "做多")
        self.assertEqual(row[4], "15m")
        self.assertEqual(row[5], "100.0000")
        self.assertEqual(row[6], "98.0000")
        self.assertEqual(row[8], "100.8000")
        self.assertEqual(row[9], "-4.8000")
        self.assertEqual(row[10], "4.76%")
        self.assertEqual(row[12], "-2.3000")
        self.assertEqual(row[13], "0.1000")
        self.assertEqual(row[14], "0.2000")
        self.assertEqual(row[15], "close fell back below EMA21")

    def test_manual_helpers_sort_gap_and_tag_rows(self) -> None:
        long_far = BacktestManualPosition(
            signal="long",
            entry_index=1,
            handoff_index=2,
            entry_ts=1710976500000,
            handoff_ts=1710976800000,
            current_ts=1710977100000,
            entry_price=Decimal("100"),
            handoff_price=Decimal("99"),
            current_price=Decimal("96"),
            stop_loss=Decimal("95"),
            take_profit=Decimal("110"),
            size=Decimal("1"),
            gross_pnl=Decimal("-4"),
            pnl=Decimal("-4"),
            risk_value=Decimal("5"),
            r_multiple=Decimal("-0.8"),
            break_even_price=Decimal("100"),
            handoff_reason="far",
        )
        long_near = BacktestManualPosition(
            signal="long",
            entry_index=3,
            handoff_index=4,
            entry_ts=1710977400000,
            handoff_ts=1710977700000,
            current_ts=1710978000000,
            entry_price=Decimal("100"),
            handoff_price=Decimal("100.1"),
            current_price=Decimal("99.8"),
            stop_loss=Decimal("95"),
            take_profit=Decimal("110"),
            size=Decimal("1"),
            gross_pnl=Decimal("0.2"),
            pnl=Decimal("0.2"),
            risk_value=Decimal("2"),
            r_multiple=Decimal("0.04"),
            break_even_price=Decimal("100"),
            handoff_reason="near",
        )
        short_mid = BacktestManualPosition(
            signal="short",
            entry_index=5,
            handoff_index=6,
            entry_ts=1710978300000,
            handoff_ts=1710978600000,
            current_ts=1710978900000,
            entry_price=Decimal("100"),
            handoff_price=Decimal("100.5"),
            current_price=Decimal("101"),
            stop_loss=Decimal("105"),
            take_profit=Decimal("90"),
            size=Decimal("1"),
            gross_pnl=Decimal("0"),
            pnl=Decimal("0"),
            risk_value=Decimal("8"),
            r_multiple=Decimal("0"),
            break_even_price=Decimal("100"),
            handoff_reason="mid",
        )

        all_positions = [short_mid, long_far, long_near]
        sorted_positions = _sorted_manual_positions(all_positions)
        oldest_positions = _sorted_manual_positions(all_positions, "oldest_handoff")
        largest_loss_positions = _sorted_manual_positions(all_positions, "largest_loss")
        largest_risk_positions = _sorted_manual_positions(all_positions, "largest_risk")

        self.assertEqual(sorted_positions, [long_near, long_far, short_mid])
        self.assertEqual(oldest_positions, [long_far, long_near, short_mid])
        self.assertEqual(largest_loss_positions, [long_far, short_mid, long_near])
        self.assertEqual(largest_risk_positions, [short_mid, long_far, long_near])
        self.assertEqual(_format_manual_gap_pct(_manual_position_break_even_gap_pct(long_near)), "0.20%")
        self.assertTrue(_manual_position_matches_filter(long_near, "near_break_even"))
        self.assertFalse(_manual_position_matches_filter(long_far, "near_break_even"))
        self.assertTrue(_manual_position_matches_filter(long_far, "loss_only"))
        self.assertEqual(_filter_manual_positions([long_far, long_near, short_mid], "loss_only"), [long_far])
        self.assertEqual(_manual_row_tag(long_near), "manual_profit_near")
        self.assertEqual(_manual_row_tag(long_far), "manual_loss")
        self.assertEqual(_manual_row_tag(short_mid), "manual_flat")

    def test_manual_focus_window_builds_centered_view(self) -> None:
        manual_position = BacktestManualPosition(
            signal="long",
            entry_index=25,
            handoff_index=31,
            entry_ts=1710976500000,
            handoff_ts=1710977400000,
            current_ts=1710978300000,
            entry_price=Decimal("100"),
            handoff_price=Decimal("102"),
            current_price=Decimal("101"),
            stop_loss=Decimal("95"),
            take_profit=Decimal("110"),
            size=Decimal("1"),
            gross_pnl=Decimal("1"),
            pnl=Decimal("1"),
            risk_value=Decimal("5"),
            r_multiple=Decimal("0.2"),
            break_even_price=Decimal("100.2"),
            handoff_reason="focus",
        )

        start_index, visible_count, hover_index = _manual_focus_window(manual_position, 120)

        self.assertEqual(hover_index, 31)
        self.assertGreaterEqual(visible_count, 40)
        self.assertLessEqual(start_index, 25)
        self.assertGreaterEqual(start_index + visible_count - 1, 31)

    def test_format_trade_exit_reason_handles_signal_profit_exit(self) -> None:
        self.assertEqual(_format_trade_exit_reason("signal_profit_exit"), "信号失效盈利平仓")
        self.assertEqual(_format_trade_exit_reason("take_profit"), "止盈")
        self.assertEqual(_format_trade_exit_reason("custom_reason"), "custom_reason")

    def test_chart_price_axis_values_builds_even_grid(self) -> None:
        values = _chart_price_axis_values(Decimal("100"), Decimal("200"))
        self.assertEqual(len(values), 5)
        self.assertEqual(values[0], Decimal("100"))
        self.assertEqual(values[-1], Decimal("200"))

    def test_format_chart_timestamp_supports_milliseconds(self) -> None:
        self.assertEqual(_format_chart_timestamp(1710976500000), _format_chart_timestamp(1710976500))
        self.assertEqual(_format_chart_timestamp(12345), "12345")
        self.assertEqual(len(_format_chart_timestamp(1710976500000)), 16)
        self.assertTrue(_format_chart_timestamp(1710976500000).startswith("202"))

    def test_chart_time_label_indices_samples_visible_range(self) -> None:
        self.assertEqual(_chart_time_label_indices(10, 15), [10, 11, 12, 13, 14])
        self.assertEqual(_chart_time_label_indices(0, 100), [0, 20, 40, 59, 79, 99])

    def test_chart_hover_index_for_x_maps_cursor_to_visible_candle(self) -> None:
        self.assertEqual(
            _chart_hover_index_for_x(x=60, left=50, width=400, start_index=10, end_index=30, candle_step=20),
            10,
        )
        self.assertEqual(
            _chart_hover_index_for_x(x=130, left=50, width=400, start_index=10, end_index=30, candle_step=20),
            14,
        )
        self.assertIsNone(
            _chart_hover_index_for_x(x=20, left=50, width=400, start_index=10, end_index=30, candle_step=20)
        )

    def _disabled_test_format_chart_hover_lines_contains_time_ohlc_and_emas(self) -> None:
        lines = _format_chart_hover_lines(
            candle=Candle(
                1710976500000,
                Decimal("100"),
                Decimal("110"),
                Decimal("95"),
                Decimal("108"),
                Decimal("1"),
                True,
            ),
            ema_value=Decimal("104.5"),
            trend_ema_value=Decimal("101.25"),
            big_ema_value=Decimal("98.75"),
            atr_value=Decimal("1000.36"),
            equity_value=Decimal("88.4321"),
            drawdown_pct_value=Decimal("12.34"),
            ema_period="21",
            trend_ema_period="55",
            big_ema_period="233",
            atr_period="10",
            tick_size=Decimal("0.0001"),
        )
        self.assertEqual(len(lines), 8)
        self.assertTrue(lines[0].startswith("鏃堕棿: "))
        self.assertIn("开/高/低/收", lines[1])
        self.assertIn("202", lines[0])
        self.assertIn("EMA(21): 104.5000", lines[2])
        self.assertIn("EMA(55): 101.2500", lines[3])
        self.assertIn("ATR(10): 1000.3600", lines[4])
        self.assertIn("EMA(233): 98.7500", lines[5])
        self.assertIn("净值曲线: 88.43", lines[5])
        self.assertIn("褰撳墠鍥炴挙: 12.34%", lines[6])

    def test_format_chart_hover_lines_contains_atr_and_emas(self) -> None:
        lines = _format_chart_hover_lines(
            candle=Candle(
                1710976500000,
                Decimal("100"),
                Decimal("110"),
                Decimal("95"),
                Decimal("108"),
                Decimal("1"),
                True,
            ),
            ema_value=Decimal("104.5"),
            trend_ema_value=Decimal("101.25"),
            big_ema_value=Decimal("98.75"),
            atr_value=Decimal("1000.36"),
            equity_value=Decimal("88.4321"),
            drawdown_pct_value=Decimal("12.34"),
            ema_period="21",
            trend_ema_period="55",
            big_ema_period="233",
            atr_period="10",
            tick_size=Decimal("0.0001"),
        )
        self.assertEqual(len(lines), 8)
        self.assertIn("EMA(21): 104.5000", lines[2])
        self.assertIn("EMA(55): 101.2500", lines[3])
        self.assertIn("ATR(10): 1000.3600", lines[4])
        self.assertIn("EMA(233): 98.7500", lines[5])

    def test_build_equity_curve_accumulates_trade_pnl_by_exit_candle(self) -> None:
        candles = [
            Candle(index, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True)
            for index in range(5)
        ]
        trades = [
            BacktestTrade(
                signal="long",
                entry_index=0,
                exit_index=1,
                entry_ts=1,
                exit_ts=2,
                entry_price=Decimal("100"),
                exit_price=Decimal("102"),
                stop_loss=Decimal("95"),
                take_profit=Decimal("110"),
                size=Decimal("1"),
                gross_pnl=Decimal("2"),
                pnl=Decimal("1.5"),
                risk_value=Decimal("5"),
                r_multiple=Decimal("0.3"),
                exit_reason="take_profit",
            ),
            BacktestTrade(
                signal="short",
                entry_index=2,
                exit_index=3,
                entry_ts=3,
                exit_ts=4,
                entry_price=Decimal("100"),
                exit_price=Decimal("104"),
                stop_loss=Decimal("105"),
                take_profit=Decimal("95"),
                size=Decimal("1"),
                gross_pnl=Decimal("-4"),
                pnl=Decimal("-4.5"),
                risk_value=Decimal("5"),
                r_multiple=Decimal("-0.9"),
                exit_reason="stop_loss",
            ),
        ]

        self.assertEqual(
            _build_equity_curve(candles, trades),
            [
                Decimal("0"),
                Decimal("1.5"),
                Decimal("1.5"),
                Decimal("-3.0"),
                Decimal("-3.0"),
            ],
        )

    def test_build_drawdown_curves_tracks_peak_to_trough(self) -> None:
        drawdown_curve, drawdown_pct_curve = _build_drawdown_curves(
            [
                Decimal("10000"),
                Decimal("10100"),
                Decimal("9950"),
                Decimal("10200"),
                Decimal("10000"),
            ]
        )

        self.assertEqual(drawdown_curve, [Decimal("0"), Decimal("0"), Decimal("150"), Decimal("0"), Decimal("200")])
        self.assertEqual(drawdown_pct_curve[0], Decimal("0"))
        self.assertEqual(drawdown_pct_curve[2].quantize(Decimal("0.01")), Decimal("1.49"))
        self.assertEqual(drawdown_pct_curve[4].quantize(Decimal("0.01")), Decimal("1.96"))

    def test_build_period_stats_groups_monthly_results(self) -> None:
        trades = [
            BacktestTrade(
                signal="long",
                entry_index=0,
                exit_index=1,
                entry_ts=1735660800000,
                exit_ts=1735840800000,
                entry_price=Decimal("100"),
                exit_price=Decimal("102"),
                stop_loss=Decimal("95"),
                take_profit=Decimal("110"),
                size=Decimal("1"),
                gross_pnl=Decimal("2"),
                pnl=Decimal("100"),
                risk_value=Decimal("5"),
                r_multiple=Decimal("1"),
                exit_reason="take_profit",
            ),
            BacktestTrade(
                signal="short",
                entry_index=2,
                exit_index=3,
                entry_ts=1738368000000,
                exit_ts=1738454400000,
                entry_price=Decimal("100"),
                exit_price=Decimal("101"),
                stop_loss=Decimal("105"),
                take_profit=Decimal("95"),
                size=Decimal("1"),
                gross_pnl=Decimal("-1"),
                pnl=Decimal("-50"),
                risk_value=Decimal("5"),
                r_multiple=Decimal("-0.5"),
                exit_reason="stop_loss",
            ),
        ]

        monthly_stats = _build_period_stats(trades, initial_capital=Decimal("10000"), by="month")

        self.assertEqual([item.period_label for item in monthly_stats], ["2025-01", "2025-02"])
        self.assertEqual(monthly_stats[0].trades, 1)
        self.assertEqual(monthly_stats[0].end_equity, Decimal("10100"))
        self.assertEqual(monthly_stats[1].start_equity, Decimal("10100"))
        self.assertEqual(monthly_stats[1].end_equity, Decimal("10050"))

    def test_format_price_by_tick_size_uses_tick_decimals(self) -> None:
        self.assertEqual(_decimal_places_for_tick_size(Decimal("0.1")), 1)
        self.assertEqual(_decimal_places_for_tick_size(Decimal("0.0001")), 4)
        self.assertEqual(_format_price_by_tick_size(Decimal("71210.94"), Decimal("0.1")), "71210.9")
        self.assertEqual(_format_price_by_tick_size(Decimal("0.05243"), Decimal("0.0001")), "0.0524")

    def test_normalize_chart_viewport_clamps_to_available_range(self) -> None:
        self.assertEqual(_normalize_chart_viewport(50, 120, 100), (0, 100))
        self.assertEqual(_normalize_chart_viewport(-5, 30, 100), (0, 30))
        self.assertEqual(_normalize_chart_viewport(95, 20, 100), (80, 20))

    def test_zoom_chart_viewport_zooms_around_anchor(self) -> None:
        start_index, visible_count = _zoom_chart_viewport(
            start_index=0,
            visible_count=None,
            total_count=200,
            anchor_ratio=0.5,
            zoom_in=True,
        )
        self.assertEqual((start_index, visible_count), (20, 160))

    def test_pan_chart_viewport_moves_within_bounds(self) -> None:
        self.assertEqual(_pan_chart_viewport(20, 80, 200, 15), 35)
        self.assertEqual(_pan_chart_viewport(20, 80, 200, 500), 120)
        self.assertEqual(_pan_chart_viewport(20, 80, 200, -50), 0)


def _patched_dynamic_backtest_report_includes_ema_relationship_filter(self: BacktestTest) -> None:
    result = run_backtest(
        DummyBacktestClient(
            [
                Candle(index, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True)
                for index in range(1, 401)
            ],
            self._build_instrument(),
        ),
        StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=14,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id=STRATEGY_DYNAMIC_ID,
            risk_amount=Decimal("100"),
            entry_reference_ema_period=21,
        ),
        candle_limit=400,
    )

    report_text = format_backtest_report(result)

    self.assertIn("趋势过滤：EMA21 与 EMA55 组成趋势过滤", report_text)
    self.assertIn("挂单参考EMA：EMA21", report_text)
    self.assertIn("止盈方式：", report_text)
    self.assertIn("每波最多开仓次数：", report_text)
    self.assertIn("同K线撮合：阳线按 O→L→H→C，阴线按 O→H→L→C，十字线不做同K线平仓", report_text)


BacktestTest.test_dynamic_backtest_report_includes_ema_relationship_filter = (
    _patched_dynamic_backtest_report_includes_ema_relationship_filter
)


def _patched_run_backtest_selected_range_auto_prepends_warmup_candles(self: BacktestTest) -> None:
    candles = [
        Candle(
            1711929600000 + (index * 3600 * 1000),
            Decimal("100"),
            Decimal("101"),
            Decimal("99"),
            Decimal("100"),
            Decimal("1"),
            True,
        )
        for index in range(400)
    ]
    client = DummyBacktestClient(candles, self._build_instrument())
    config = replace(
        self._build_config(),
        strategy_id=STRATEGY_DYNAMIC_ID,
        ema_period=21,
        trend_ema_period=55,
        big_ema_period=233,
        atr_period=14,
    )

    result = run_backtest(
        client,
        config,
        candle_limit=10000,
        start_ts=candles[300].ts,
        end_ts=candles[302].ts,
    )

    self.assertEqual(result.candles[0].ts, candles[100].ts)
    self.assertEqual(result.candles[-1].ts, candles[302].ts)
    self.assertIn("前置补足 200 根", result.data_source_note)


BacktestTest.test_run_backtest_selected_range_auto_prepends_warmup_candles = (
    _patched_run_backtest_selected_range_auto_prepends_warmup_candles
)


def _slot_test_candles(sequence: list[tuple[str, str, str, str]], *, start_ts: int = 1711929600000) -> list[Candle]:
    warmup = [
        Candle(
            start_ts + (index * 300000),
            Decimal("100"),
            Decimal("101"),
            Decimal("99"),
            Decimal("100"),
            Decimal("1"),
            True,
        )
        for index in range(BACKTEST_RESERVED_CANDLES)
    ]
    candles = list(warmup)
    base_ts = start_ts + (BACKTEST_RESERVED_CANDLES * 300000)
    for offset, (open_price, high, low, close) in enumerate(sequence):
        candles.append(
            Candle(
                base_ts + (offset * 300000),
                Decimal(open_price),
                Decimal(high),
                Decimal(low),
                Decimal(close),
                Decimal("1"),
                True,
            )
        )
    return candles



