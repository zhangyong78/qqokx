from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from okx_quant.backtest import BACKTEST_RESERVED_CANDLES, build_parameter_batch_configs, run_backtest
import okx_quant.backtest_ui as backtest_ui_module
from okx_quant.backtest_export import export_single_backtest_report
from okx_quant.models import Candle, Instrument, StrategyConfig
from okx_quant.strategy_catalog import STRATEGY_BTC_EMA15_MA50_PULLBACK_SHORT_ID
from okx_quant.strategies.btc_ema15_ma50_pullback_short import (
    PullbackCandidate,
    evaluate_btc_ema15_ma50_pullback_short_signal,
    is_cross_down,
    scan_btc_ema15_ma50_pullback_short_candidates,
)


class DummyBacktestClient:
    def __init__(self, candles: list[Candle], instrument: Instrument) -> None:
        self._candles = candles
        self._instrument = instrument

    def get_instrument(self, inst_id: str) -> Instrument:
        return self._instrument

    def get_candles_history(self, inst_id: str, bar: str, limit: int = 200) -> list[Candle]:
        return list(self._candles) if limit <= 0 else self._candles[-limit:]

    def get_candles(self, inst_id: str, bar: str, limit: int = 200) -> list[Candle]:
        return self.get_candles_history(inst_id, bar, limit=limit)


def _instrument() -> Instrument:
    return Instrument(
        inst_id="BTC-USDT-SWAP",
        inst_type="SWAP",
        tick_size=Decimal("0.1"),
        lot_size=Decimal("0.01"),
        min_size=Decimal("0.01"),
        state="live",
        settle_ccy="USDT",
        ct_val=Decimal("0.01"),
    )


def _config() -> StrategyConfig:
    return StrategyConfig(
        inst_id="BTC-USDT-SWAP",
        bar="4H",
        ema_type="ema",
        ema_period=15,
        trend_ema_type="ema",
        trend_ema_period=55,
        atr_period=14,
        atr_stop_multiplier=Decimal("1"),
        atr_take_multiplier=Decimal("1"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="short_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_BTC_EMA15_MA50_PULLBACK_SHORT_ID,
        risk_amount=Decimal("100"),
        take_profit_mode="fixed",
        cross_window_bars=10,
        max_pullback_index=1,
        exit_mode="fixed_rr",
        rr=Decimal("2"),
    )


def _candles(count: int) -> list[Candle]:
    candles: list[Candle] = []
    ts = 1_700_000_000_000
    for index in range(count):
        price = Decimal("100")
        candles.append(Candle(ts + (index * 14_400_000), price, price, price, price, Decimal("1"), True))
    return candles


class BtcEma15Ma50PullbackShortTest(TestCase):
    def test_build_parameter_batch_configs_covers_research_matrix_dimensions(self) -> None:
        configs = build_parameter_batch_configs(_config())

        self.assertTrue(configs)
        self.assertEqual({item.resolved_trend_ema_type() for item in configs}, {"ema"})
        self.assertEqual({item.trend_ema_period for item in configs}, {50, 55})
        self.assertEqual({item.atr_period for item in configs}, {10, 14})
        self.assertEqual({item.cross_window_bars for item in configs}, {8, 10, 15, 20})
        self.assertEqual({item.max_pullback_index for item in configs}, {1, 2, 3})
        self.assertIn("fixed_rr", {item.exit_mode for item in configs})
        self.assertIn("dynamic_or_ema15_close", {item.exit_mode for item in configs})
        self.assertIn(Decimal("3"), {item.rr for item in configs if item.exit_mode.startswith("fixed_rr")})

    def test_is_cross_down_uses_previous_and_current_values(self) -> None:
        fast = [Decimal("101"), Decimal("100"), Decimal("99")]
        slow = [Decimal("100"), Decimal("100"), Decimal("100")]

        self.assertFalse(is_cross_down(fast, slow, 1))
        self.assertTrue(is_cross_down(fast, slow, 2))

    def test_scan_candidates_detects_first_pullback_after_cross(self) -> None:
        candles = _candles(12)
        candles[7] = Candle(candles[7].ts, Decimal("100"), Decimal("101"), Decimal("96"), Decimal("97"), Decimal("1"), True)
        config = replace(_config(), ema_period=5, trend_ema_period=6, atr_period=5, cross_window_bars=5)
        fast_values = [None, None, None, None, Decimal("101"), Decimal("101"), Decimal("99"), Decimal("98"), Decimal("97"), Decimal("96"), Decimal("95"), Decimal("94")]
        slow_values = [None, None, None, None, Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100")]
        atr_values = [None, None, None, None, Decimal("10"), Decimal("10"), Decimal("10"), Decimal("10"), Decimal("10"), Decimal("10"), Decimal("10"), Decimal("10")]

        with patch("okx_quant.strategies.btc_ema15_ma50_pullback_short.moving_average", side_effect=[fast_values, slow_values]), patch(
            "okx_quant.strategies.btc_ema15_ma50_pullback_short.atr",
            return_value=atr_values,
        ):
            candidates = scan_btc_ema15_ma50_pullback_short_candidates(candles, config)

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].cross_index, 6)
        self.assertEqual(candidates[0].signal_index, 7)
        self.assertEqual(candidates[0].pullback_index, 1)

    def test_scan_candidates_invalidates_when_cross_window_expires(self) -> None:
        candles = _candles(12)
        candles[9] = Candle(candles[9].ts, Decimal("100"), Decimal("101"), Decimal("95"), Decimal("97"), Decimal("1"), True)
        config = replace(_config(), ema_period=5, trend_ema_period=6, atr_period=5, cross_window_bars=2)
        fast_values = [None, None, None, None, Decimal("101"), Decimal("101"), Decimal("99"), Decimal("98"), Decimal("97"), Decimal("96"), Decimal("95"), Decimal("94")]
        slow_values = [None, None, None, None, Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100")]
        atr_values = [None, None, None, None, Decimal("10"), Decimal("10"), Decimal("10"), Decimal("10"), Decimal("10"), Decimal("10"), Decimal("10"), Decimal("10")]

        with patch("okx_quant.strategies.btc_ema15_ma50_pullback_short.moving_average", side_effect=[fast_values, slow_values]), patch(
            "okx_quant.strategies.btc_ema15_ma50_pullback_short.atr",
            return_value=atr_values,
        ):
            candidates = scan_btc_ema15_ma50_pullback_short_candidates(candles, config)

        self.assertEqual(candidates, [])

    def test_scan_candidates_invalidates_when_fast_reclaims_above_slow(self) -> None:
        candles = _candles(12)
        candles[8] = Candle(candles[8].ts, Decimal("100"), Decimal("102"), Decimal("95"), Decimal("97"), Decimal("1"), True)
        config = replace(_config(), ema_period=5, trend_ema_period=6, atr_period=5, cross_window_bars=5)
        fast_values = [None, None, None, None, Decimal("101"), Decimal("101"), Decimal("99"), Decimal("101"), Decimal("102"), Decimal("103"), Decimal("104"), Decimal("105")]
        slow_values = [None, None, None, None, Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100")]
        atr_values = [None, None, None, None, Decimal("10"), Decimal("10"), Decimal("10"), Decimal("10"), Decimal("10"), Decimal("10"), Decimal("10"), Decimal("10")]

        with patch("okx_quant.strategies.btc_ema15_ma50_pullback_short.moving_average", side_effect=[fast_values, slow_values]), patch(
            "okx_quant.strategies.btc_ema15_ma50_pullback_short.atr",
            return_value=atr_values,
        ):
            candidates = scan_btc_ema15_ma50_pullback_short_candidates(candles, config)

        self.assertEqual(candidates, [])

    def test_evaluate_signal_respects_pullback_index_limit(self) -> None:
        candles = _candles(20)
        config = replace(_config(), ema_period=5, trend_ema_period=6, atr_period=5, max_pullback_index=1)
        candidate = PullbackCandidate(
            cross_index=10,
            signal_index=19,
            cross_ts=candles[10].ts,
            signal_ts=candles[19].ts,
            pullback_index=2,
            bars_after_cross=3,
            ema15_at_signal=Decimal("99"),
            ma50_at_signal=Decimal("100"),
            atr_at_signal=Decimal("10"),
            pullback_depth_pct=Decimal("0.5"),
            ema15_slope_5=Decimal("-0.01"),
            ema15_slope_10=Decimal("-0.02"),
            ma50_slope_10=Decimal("-0.005"),
            daily_filter_pass=True,
        )

        with patch(
            "okx_quant.strategies.btc_ema15_ma50_pullback_short.scan_btc_ema15_ma50_pullback_short_candidates",
            return_value=[candidate],
        ):
            decision = evaluate_btc_ema15_ma50_pullback_short_signal(candles, config)

        self.assertIsNone(decision.signal)
        self.assertIn("pullback_index_exceeds_limit", decision.reason)

    def test_backtest_enters_on_next_open_and_applies_fixed_risk_with_costs(self) -> None:
        candles = _candles(BACKTEST_RESERVED_CANDLES + 12)
        entry_index = BACKTEST_RESERVED_CANDLES + 1
        candles[entry_index] = Candle(
            candles[entry_index].ts,
            Decimal("99"),
            Decimal("100"),
            Decimal("70"),
            Decimal("75"),
            Decimal("1"),
            True,
        )
        candidate = PullbackCandidate(
            cross_index=BACKTEST_RESERVED_CANDLES - 2,
            signal_index=BACKTEST_RESERVED_CANDLES,
            cross_ts=candles[BACKTEST_RESERVED_CANDLES - 2].ts,
            signal_ts=candles[BACKTEST_RESERVED_CANDLES].ts,
            pullback_index=1,
            bars_after_cross=2,
            ema15_at_signal=Decimal("100"),
            ma50_at_signal=Decimal("101"),
            atr_at_signal=Decimal("10"),
            pullback_depth_pct=Decimal("0.4"),
            ema15_slope_5=Decimal("-0.01"),
            ema15_slope_10=Decimal("-0.02"),
            ma50_slope_10=Decimal("-0.01"),
            daily_filter_pass=True,
        )
        client = DummyBacktestClient(candles, _instrument())

        with patch("okx_quant.backtest.scan_btc_ema15_ma50_pullback_short_candidates", return_value=[candidate]):
            result = run_backtest(
                client,
                _config(),
                candle_limit=len(candles),
                maker_fee_rate=Decimal("0.001"),
                taker_fee_rate=Decimal("0.001"),
            )

        self.assertEqual(len(result.trades), 1)
        trade = result.trades[0]
        self.assertEqual(trade.entry_index, entry_index)
        self.assertEqual(trade.entry_price, Decimal("99"))
        self.assertEqual(trade.exit_reason, "take_profit")
        self.assertEqual(trade.entry_fee_type, "maker")
        self.assertEqual(trade.exit_fee_type, "taker")
        self.assertEqual(trade.size, Decimal("10"))
        self.assertEqual(trade.metadata["pullback_index"], 1)
        self.assertEqual(trade.metadata["bars_after_cross"], 2)
        self.assertGreater(trade.total_fee, Decimal("0"))
        self.assertLess(trade.pnl, trade.gross_pnl)

    def test_backtest_ema15_close_exit_uses_next_open(self) -> None:
        candles = _candles(BACKTEST_RESERVED_CANDLES + 14)
        entry_index = BACKTEST_RESERVED_CANDLES + 1
        candles[entry_index] = Candle(
            candles[entry_index].ts,
            Decimal("99"),
            Decimal("101"),
            Decimal("95"),
            Decimal("96"),
            Decimal("1"),
            True,
        )
        candles[entry_index + 1] = Candle(
            candles[entry_index + 1].ts,
            Decimal("96"),
            Decimal("105"),
            Decimal("95"),
            Decimal("105"),
            Decimal("1"),
            True,
        )
        candles[entry_index + 2] = Candle(
            candles[entry_index + 2].ts,
            Decimal("103"),
            Decimal("104"),
            Decimal("102"),
            Decimal("103"),
            Decimal("1"),
            True,
        )
        candidate = PullbackCandidate(
            cross_index=BACKTEST_RESERVED_CANDLES - 2,
            signal_index=BACKTEST_RESERVED_CANDLES,
            cross_ts=candles[BACKTEST_RESERVED_CANDLES - 2].ts,
            signal_ts=candles[BACKTEST_RESERVED_CANDLES].ts,
            pullback_index=1,
            bars_after_cross=2,
            ema15_at_signal=Decimal("100"),
            ma50_at_signal=Decimal("101"),
            atr_at_signal=Decimal("10"),
            pullback_depth_pct=Decimal("0.4"),
            ema15_slope_5=Decimal("-0.01"),
            ema15_slope_10=Decimal("-0.02"),
            ma50_slope_10=Decimal("-0.01"),
            daily_filter_pass=True,
        )
        client = DummyBacktestClient(candles, _instrument())
        config = replace(_config(), exit_mode="ema15_close", rr=Decimal("2"))

        with patch("okx_quant.backtest.scan_btc_ema15_ma50_pullback_short_candidates", return_value=[candidate]):
            result = run_backtest(client, config, candle_limit=len(candles))

        self.assertEqual(len(result.trades), 1)
        trade = result.trades[0]
        self.assertEqual(trade.entry_index, entry_index)
        self.assertEqual(trade.exit_index, entry_index + 2)
        self.assertEqual(trade.exit_price, Decimal("103"))
        self.assertEqual(trade.exit_reason, "ema15_close_exit")

    def test_export_single_backtest_report_creates_research_bundle_files(self) -> None:
        candles = _candles(BACKTEST_RESERVED_CANDLES + 12)
        entry_index = BACKTEST_RESERVED_CANDLES + 1
        candles[entry_index] = Candle(
            candles[entry_index].ts,
            Decimal("99"),
            Decimal("100"),
            Decimal("70"),
            Decimal("75"),
            Decimal("1"),
            True,
        )
        candidate = PullbackCandidate(
            cross_index=BACKTEST_RESERVED_CANDLES - 2,
            signal_index=BACKTEST_RESERVED_CANDLES,
            cross_ts=candles[BACKTEST_RESERVED_CANDLES - 2].ts,
            signal_ts=candles[BACKTEST_RESERVED_CANDLES].ts,
            pullback_index=1,
            bars_after_cross=2,
            ema15_at_signal=Decimal("100"),
            ma50_at_signal=Decimal("101"),
            atr_at_signal=Decimal("10"),
            pullback_depth_pct=Decimal("0.4"),
            ema15_slope_5=Decimal("-0.01"),
            ema15_slope_10=Decimal("-0.02"),
            ma50_slope_10=Decimal("-0.01"),
            daily_filter_pass=True,
        )
        client = DummyBacktestClient(candles, _instrument())
        with patch("okx_quant.backtest.scan_btc_ema15_ma50_pullback_short_candidates", return_value=[candidate]):
            result = run_backtest(client, _config(), candle_limit=len(candles))

        with TemporaryDirectory() as temp_dir:
            export_single_backtest_report(
                result,
                _config(),
                len(candles),
                base_dir=Path(temp_dir),
            )
            research_dir = Path(temp_dir) / "btc_ema15_ma50_short" / "latest"
            report_html = (research_dir / "report.html").read_text(encoding="utf-8")
            trades_csv = (research_dir / "trades.csv").read_text(encoding="utf-8-sig")
            comparison_csv = (research_dir / "strategy_comparison.csv").read_text(encoding="utf-8-sig")
            self.assertTrue((research_dir / "summary.csv").exists())
            self.assertTrue((research_dir / "strategy_comparison.csv").exists())
            self.assertTrue((research_dir / "equity_curve.csv").exists())
            self.assertTrue((research_dir / "monthly_returns.csv").exists())
            self.assertTrue((research_dir / "yearly_returns.csv").exists())
            self.assertTrue((research_dir / "trade_charts" / "T0001.html").exists())
            self.assertIn("EMA15", report_html)
            self.assertIn("trade_id", trades_csv)
            self.assertIn("pullback_index", trades_csv)
            self.assertIn("ema_period", comparison_csv)
            self.assertIn("trend_ema_period", comparison_csv)

    def test_backtest_ui_strategy_config_roundtrip_preserves_research_fields(self) -> None:
        payload = backtest_ui_module._serialize_strategy_config(
            replace(
                _config(),
                ema_type="ma",
                ema_period=12,
                trend_ema_type="ma",
                trend_ema_period=50,
                cross_window_bars=15,
                max_pullback_index=3,
                exit_mode="dynamic_or_ema15_close",
                rr=Decimal("3"),
            )
        )

        restored = backtest_ui_module._deserialize_strategy_config(payload)

        self.assertEqual(restored.resolved_ema_type(), "ma")
        self.assertEqual(restored.ema_period, 12)
        self.assertEqual(restored.resolved_trend_ema_type(), "ma")
        self.assertEqual(restored.trend_ema_period, 50)
        self.assertEqual(restored.cross_window_bars, 15)
        self.assertEqual(restored.max_pullback_index, 3)
        self.assertEqual(restored.exit_mode, "dynamic_or_ema15_close")
        self.assertEqual(restored.rr, Decimal("3"))
