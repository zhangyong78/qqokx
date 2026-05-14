from decimal import Decimal
from unittest import TestCase

from okx_quant.indicators import atr, ema
from okx_quant.backtest import run_backtest
from okx_quant.models import Candle, Instrument, StrategyConfig
from okx_quant.strategies.adaptive_ema_rail import (
    adaptive_rail_candidate_periods,
    evaluate_adaptive_rail_signal,
    is_adaptive_rail_hard_break,
)
from okx_quant.strategy_catalog import (
    BACKTEST_STRATEGY_DEFINITIONS,
    STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID,
    STRATEGY_DEFINITIONS,
)


class _DummyBacktestClient:
    def __init__(self, candles: list[Candle], instrument: Instrument) -> None:
        self._candles = candles
        self._instrument = instrument

    def get_instrument(self, inst_id: str) -> Instrument:
        return self._instrument

    def get_candles_history(self, inst_id: str, bar: str, limit: int = 200) -> list[Candle]:
        return list(self._candles) if limit <= 0 else self._candles[-limit:]


def _make_config(**overrides: object) -> StrategyConfig:
    defaults = {
        "inst_id": "BTC-USDT-SWAP",
        "bar": "1H",
        "ema_period": 21,
        "trend_ema_period": 55,
        "atr_period": 10,
        "atr_stop_multiplier": Decimal("1.2"),
        "atr_take_multiplier": Decimal("2"),
        "order_size": Decimal("1"),
        "trade_mode": "cross",
        "signal_mode": "long_only",
        "position_mode": "net",
        "environment": "demo",
        "tp_sl_trigger_type": "last",
        "strategy_id": STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID,
        "take_profit_mode": "dynamic",
        "max_entries_per_trend": 2,
        "rail_candidate_ema_periods": (8, 13, 21),
        "rail_touch_atr_ratio": Decimal("2"),
        "rail_bounce_atr_ratio": Decimal("0.1"),
        "rail_score_lookback_bars": 60,
    }
    defaults.update(overrides)
    return StrategyConfig(**defaults)


def _uptrend_candles(count: int = 320) -> list[Candle]:
    candles: list[Candle] = []
    for index in range(count):
        close = Decimal("100") + (Decimal(index) * Decimal("0.6"))
        if index % 9 == 0:
            close -= Decimal("1.8")
        open_price = close - Decimal("0.2")
        high = close + Decimal("1.4")
        low = close - Decimal("2.4")
        candles.append(
            Candle(
                ts=index * 3_600_000,
                open=open_price,
                high=high,
                low=low,
                close=close,
                volume=Decimal("100"),
                confirmed=True,
            )
        )
    return candles


class AdaptiveEmaRailTest(TestCase):
    def test_adaptive_rail_generates_long_signal_after_confirmed_bounces(self) -> None:
        config = _make_config()
        candles = _uptrend_candles()
        closes = [candle.close for candle in candles]
        ema_by_period = {period: ema(closes, period) for period in adaptive_rail_candidate_periods(config)}
        snapshot = evaluate_adaptive_rail_signal(
            candles,
            len(candles) - 2,
            ema_by_period=ema_by_period,
            ema200_values=ema(closes, 200),
            atr_values=atr(candles, config.atr_period),
            config=config,
        )

        self.assertEqual(snapshot.decision.signal, "long")
        self.assertIsNotNone(snapshot.dominant_period)
        self.assertIsNotNone(snapshot.decision.entry_reference)
        self.assertIsNotNone(snapshot.metrics)
        self.assertGreaterEqual(snapshot.metrics.bounce_count, 2)

    def test_hard_break_uses_close_below_ema_minus_atr_band(self) -> None:
        config = _make_config(rail_break_atr_ratio=Decimal("1"))
        candle = Candle(
            ts=0,
            open=Decimal("100"),
            high=Decimal("101"),
            low=Decimal("95"),
            close=Decimal("98.9"),
            volume=Decimal("1"),
            confirmed=True,
        )

        self.assertTrue(
            is_adaptive_rail_hard_break(
                candle,
                ema_value=Decimal("100"),
                atr_value=Decimal("1"),
                config=config,
            )
        )

    def test_adaptive_rail_is_backtest_only_in_catalog(self) -> None:
        launcher_ids = {item.strategy_id for item in STRATEGY_DEFINITIONS}
        backtest_ids = {item.strategy_id for item in BACKTEST_STRATEGY_DEFINITIONS}

        self.assertNotIn(STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID, launcher_ids)
        self.assertIn(STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID, backtest_ids)

    def test_adaptive_rail_runs_through_public_backtest_entry(self) -> None:
        instrument = Instrument(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
        )
        result = run_backtest(
            _DummyBacktestClient(_uptrend_candles(), instrument),
            _make_config(risk_amount=Decimal("100"), order_size=Decimal("0")),
            candle_limit=0,
        )

        self.assertEqual(result.strategy_id, STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID)
        self.assertGreater(len(result.candles), 0)
