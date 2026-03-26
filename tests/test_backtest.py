from decimal import Decimal
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from okx_quant.backtest import (
    ATR_BATCH_MULTIPLIERS,
    ATR_BATCH_TAKE_RATIOS,
    BACKTEST_RESERVED_CANDLES,
    _OpenPosition,
    _load_backtest_candles,
    _backtest_trade_start_index,
    _format_backtest_timestamp,
    _try_close_position,
    _try_close_position_same_candle_after_fill,
    build_atr_batch_configs,
    format_backtest_report,
    run_backtest,
    run_backtest_batch,
)
import okx_quant.backtest_export as backtest_export_module
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
    _chart_hover_index_for_x,
    _chart_price_axis_values,
    _chart_time_label_indices,
    _decimal_places_for_tick_size,
    _format_chart_hover_lines,
    _format_price_by_tick_size,
    _format_chart_timestamp,
    _normalize_backtest_bar_label,
    _normalize_chart_viewport,
    _pan_chart_viewport,
    _zoom_chart_viewport,
    _BacktestSnapshot,
)
from okx_quant.indicators import ema
from okx_quant.backtest import BacktestReport, BacktestResult
from okx_quant.models import Candle, Instrument, StrategyConfig
from okx_quant.strategy_catalog import STRATEGY_CROSS_ID, STRATEGY_DYNAMIC_ID


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
        self.last_candle_history_stats = {
            "cache_hit_count": max(limit - 12, 0),
            "latest_fetch_count": 12,
            "older_fetch_count": 0,
            "requested_count": limit,
            "returned_count": min(limit, len(self._candles)),
        }
        return self._candles[-limit:]


class BacktestTest(TestCase):
    def test_backtest_default_fee_percents(self) -> None:
        self.assertEqual(DEFAULT_MAKER_FEE_PERCENT, "0.01")
        self.assertEqual(DEFAULT_TAKER_FEE_PERCENT, "0.028")

    def _build_instrument(self) -> Instrument:
        return Instrument(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
        )

    def _build_config(self, *, ema_period: int = 2, atr_period: int = 2) -> StrategyConfig:
        return StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=ema_period,
            atr_period=atr_period,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="both",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id=STRATEGY_CROSS_ID,
            risk_amount=Decimal("100"),
        )

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
        config = self._build_config()

        result = run_backtest(client, config, candle_limit=len(candles))

        self.assertGreaterEqual(len(result.trades), 1)
        self.assertTrue(all(trade.entry_index >= BACKTEST_RESERVED_CANDLES for trade in result.trades))
        self.assertTrue(any(trade.exit_reason == "take_profit" for trade in result.trades))
        self.assertGreater(result.report.total_pnl, Decimal("0"))
        self.assertEqual(result.ema_values, ema([candle.close for candle in candles], config.ema_period))
        self.assertEqual(result.trend_ema_values, ema([candle.close for candle in candles], config.trend_ema_period))
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
        config = self._build_config()

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

    def test_backtest_bar_label_mapping_accepts_raw_values_and_labels(self) -> None:
        self.assertEqual(_normalize_backtest_bar_label("5m"), "5分钟")
        self.assertEqual(_normalize_backtest_bar_label("15分钟"), "15分钟")
        self.assertEqual(_normalize_backtest_bar_label("1H"), "1小时")
        self.assertEqual(_normalize_backtest_bar_label("4小时"), "4小时")
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

        self.assertIn("趋势过滤：EMA21 > EMA55 才做多，EMA21 < EMA55 才做空", report_text)
        self.assertIn("同K线撮合：阳线按 O→L→H→C，阴线按 O→H→L→C，十字线不做同K线平仓", report_text)

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
            report_text="示例报告",
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
        self.assertEqual(row[4], "EMA 动态委托")
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
                strategy_id=STRATEGY_CROSS_ID,
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
                strategy_id=STRATEGY_CROSS_ID,
            ),
        )

        detail = _build_backtest_compare_detail(snapshot)

        self.assertIn("编号：R009", detail)
        self.assertIn("策略：EMA 穿越市价", detail)
        self.assertIn("K线周期：1小时", detail)
        self.assertIn("开始时间：2024-03-22 08:00", detail)
        self.assertIn("结束时间：2024-03-23 08:00", detail)
        self.assertIn("回测K线数：800", detail)
        self.assertIn("方向只做空", detail)

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
        )

        report_text = format_backtest_report(result)

        self.assertIn("Maker手续费：0.0200%", report_text)
        self.assertIn("Taker手续费：0.0500%", report_text)
        self.assertIn("手续费合计：0.6400", report_text)

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
            finally:
                backtest_export_module.backtest_report_export_dir_path = original_export_dir

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
                self.assertEqual(snapshots[0].start_ts, 1710976500000)
                self.assertEqual(snapshots[0].end_ts, 1711062900000)
                self.assertIn("趋势过滤", snapshots[0].report_text)
            finally:
                backtest_ui_module.backtest_history_file_path = original_path_factory

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

    def test_format_chart_hover_lines_contains_time_ohlc_and_emas(self) -> None:
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
            ema_period="21",
            trend_ema_period="55",
            tick_size=Decimal("0.0001"),
        )
        self.assertEqual(len(lines), 4)
        self.assertTrue(lines[0].startswith("时间: "))
        self.assertIn("开/高/低/收:", lines[1])
        self.assertIn("202", lines[0])
        self.assertIn("EMA(21): 104.5000", lines[2])
        self.assertIn("趋势EMA(55): 101.2500", lines[3])

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
