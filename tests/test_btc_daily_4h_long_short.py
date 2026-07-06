from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from unittest import TestCase
from unittest.mock import patch

from okx_quant.backtest import BACKTEST_RESERVED_CANDLES, build_parameter_batch_configs, run_backtest
from okx_quant.models import Candle, Instrument, StrategyConfig
from okx_quant.strategy_catalog import STRATEGY_BTC_DAILY_4H_LONG_SHORT_ID
from okx_quant.strategies.btc_ema15_ma50_pullback_long import PullbackCandidate as LongPullbackCandidate
from okx_quant.strategies.btc_ema15_ma50_pullback_short import PullbackCandidate as ShortPullbackCandidate


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
        trend_ema_type="ma",
        trend_ema_period=50,
        atr_period=14,
        atr_stop_multiplier=Decimal("1.2"),
        atr_take_multiplier=Decimal("1"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="both",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_BTC_DAILY_4H_LONG_SHORT_ID,
        risk_amount=Decimal("100"),
        take_profit_mode="dynamic",
        cross_window_bars=10,
        max_pullback_index=1,
        exit_mode="dynamic_or_ema15_close",
        rr=Decimal("2"),
        dynamic_two_r_break_even=True,
        dynamic_break_even_trigger_r=2,
        dynamic_fee_offset_enabled=True,
        ema55_slope_lock_profit_trigger_r=3,
        dynamic_first_lock_r=1,
        dynamic_trailing_step_r=1,
        daily_filter_enabled=True,
        daily_filter_bar="1D",
        daily_filter_boundary="exchange",
        daily_filter_mode="close_vs_ma",
        daily_filter_scope="both",
        daily_filter_ma_type="ma",
        daily_filter_period=50,
    )


def _candles(count: int) -> list[Candle]:
    candles: list[Candle] = []
    ts = 1_700_000_000_000
    for index in range(count):
        price = Decimal("100")
        candles.append(Candle(ts + (index * 14_400_000), price, price, price, price, Decimal("1"), True))
    return candles


def _long_candidate(candles: list[Candle]) -> LongPullbackCandidate:
    return LongPullbackCandidate(
        cross_index=BACKTEST_RESERVED_CANDLES - 2,
        signal_index=BACKTEST_RESERVED_CANDLES,
        cross_ts=candles[BACKTEST_RESERVED_CANDLES - 2].ts,
        signal_ts=candles[BACKTEST_RESERVED_CANDLES].ts,
        pullback_index=1,
        bars_after_cross=2,
        ema15_at_signal=Decimal("100"),
        ma50_at_signal=Decimal("99"),
        atr_at_signal=Decimal("10"),
        pullback_depth_pct=Decimal("0.4"),
        ema15_slope_5=Decimal("0.01"),
        ema15_slope_10=Decimal("0.02"),
        ma50_slope_10=Decimal("0.01"),
        daily_filter_pass=True,
    )


def _short_candidate(candles: list[Candle]) -> ShortPullbackCandidate:
    return ShortPullbackCandidate(
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


class BtcDaily4hLongShortBacktestTest(TestCase):
    def test_build_parameter_batch_configs_keeps_single_research_config(self) -> None:
        configs = build_parameter_batch_configs(_config())

        self.assertEqual(len(configs), 1)
        self.assertEqual(configs[0], _config())

    def test_backtest_opens_long_when_long_candidate_is_available(self) -> None:
        candles = _candles(BACKTEST_RESERVED_CANDLES + 12)
        entry_index = BACKTEST_RESERVED_CANDLES + 1
        wave_low_index = BACKTEST_RESERVED_CANDLES - 1
        candles[wave_low_index] = Candle(
            candles[wave_low_index].ts,
            Decimal("100"),
            Decimal("105"),
            Decimal("94"),
            Decimal("103"),
            Decimal("1"),
            True,
        )
        candles[entry_index] = Candle(
            candles[entry_index].ts,
            Decimal("101"),
            Decimal("140"),
            Decimal("100"),
            Decimal("130"),
            Decimal("1"),
            True,
        )
        client = DummyBacktestClient(candles, _instrument())

        with patch("okx_quant.backtest.scan_btc_ema15_ma50_pullback_long_candidates", return_value=[_long_candidate(candles)]), patch(
            "okx_quant.backtest.scan_btc_ema15_ma50_pullback_short_candidates",
            return_value=[],
        ):
            result = run_backtest(client, _config(), candle_limit=len(candles))

        self.assertEqual(len(result.trades), 1)
        trade = result.trades[0]
        self.assertEqual(trade.signal, "long")
        self.assertEqual(trade.entry_index, entry_index)
        self.assertEqual(trade.stop_loss, Decimal("94"))
        self.assertEqual(trade.metadata["pullback_index"], 1)
        self.assertEqual(trade.metadata["stop_source"], "wave_low")

    def test_backtest_opens_short_when_short_candidate_is_available(self) -> None:
        candles = _candles(BACKTEST_RESERVED_CANDLES + 12)
        entry_index = BACKTEST_RESERVED_CANDLES + 1
        wave_high_index = BACKTEST_RESERVED_CANDLES - 1
        candles[wave_high_index] = Candle(
            candles[wave_high_index].ts,
            Decimal("100"),
            Decimal("107"),
            Decimal("98"),
            Decimal("99"),
            Decimal("1"),
            True,
        )
        candles[entry_index] = Candle(
            candles[entry_index].ts,
            Decimal("101"),
            Decimal("102"),
            Decimal("60"),
            Decimal("70"),
            Decimal("1"),
            True,
        )
        client = DummyBacktestClient(candles, _instrument())

        with patch("okx_quant.backtest.scan_btc_ema15_ma50_pullback_long_candidates", return_value=[]), patch(
            "okx_quant.backtest.scan_btc_ema15_ma50_pullback_short_candidates",
            return_value=[_short_candidate(candles)],
        ):
            result = run_backtest(client, _config(), candle_limit=len(candles))

        self.assertEqual(len(result.trades), 1)
        trade = result.trades[0]
        self.assertEqual(trade.signal, "short")
        self.assertEqual(trade.entry_index, entry_index)
        self.assertEqual(trade.stop_loss, Decimal("107"))
        self.assertEqual(trade.metadata["pullback_index"], 1)
        self.assertEqual(trade.metadata["stop_source"], "wave_high")

    def test_backtest_signal_mode_can_limit_to_long_only(self) -> None:
        candles = _candles(BACKTEST_RESERVED_CANDLES + 12)
        client = DummyBacktestClient(candles, _instrument())
        config = replace(_config(), signal_mode="long_only")

        with patch("okx_quant.backtest.scan_btc_ema15_ma50_pullback_long_candidates", return_value=[]), patch(
            "okx_quant.backtest.scan_btc_ema15_ma50_pullback_short_candidates",
            return_value=[_short_candidate(candles)],
        ):
            result = run_backtest(client, config, candle_limit=len(candles))

        self.assertEqual(result.trades, [])

    def test_backtest_long_stop_falls_back_to_recent_ten_bar_low_when_wave_low_is_invalid(self) -> None:
        candles = _candles(BACKTEST_RESERVED_CANDLES + 12)
        signal_index = BACKTEST_RESERVED_CANDLES
        entry_index = signal_index + 1
        recent_low_index = BACKTEST_RESERVED_CANDLES - 8
        candles[recent_low_index] = Candle(
            candles[recent_low_index].ts,
            Decimal("100"),
            Decimal("104"),
            Decimal("95"),
            Decimal("103"),
            Decimal("1"),
            True,
        )
        candles[signal_index - 1] = Candle(
            candles[signal_index - 1].ts,
            Decimal("104"),
            Decimal("106"),
            Decimal("103"),
            Decimal("105"),
            Decimal("1"),
            True,
        )
        candles[signal_index] = Candle(
            candles[signal_index].ts,
            Decimal("105"),
            Decimal("108"),
            Decimal("104"),
            Decimal("107"),
            Decimal("1"),
            True,
        )
        candles[entry_index] = Candle(
            candles[entry_index].ts,
            Decimal("101"),
            Decimal("140"),
            Decimal("100"),
            Decimal("130"),
            Decimal("1"),
            True,
        )
        client = DummyBacktestClient(candles, _instrument())
        candidate = replace(
            _long_candidate(candles),
            cross_index=signal_index - 1,
            cross_ts=candles[signal_index - 1].ts,
        )

        with patch("okx_quant.backtest.scan_btc_ema15_ma50_pullback_long_candidates", return_value=[candidate]), patch(
            "okx_quant.backtest.scan_btc_ema15_ma50_pullback_short_candidates",
            return_value=[],
        ):
            result = run_backtest(client, _config(), candle_limit=len(candles))

        self.assertEqual(len(result.trades), 1)
        trade = result.trades[0]
        self.assertEqual(trade.stop_loss, Decimal("95"))
        self.assertEqual(trade.metadata["stop_source"], "fallback_10_low")
