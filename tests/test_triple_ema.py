from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from unittest import TestCase

import pandas as pd

from okx_quant.backtest import (
    BACKTEST_RESERVED_CANDLES,
    _run_backtest_with_loaded_data,
    build_parameter_batch_configs,
    format_backtest_report,
)
from okx_quant.models import Candle, Instrument, StrategyConfig
from okx_quant.strategy_catalog import (
    BACKTEST_STRATEGY_DEFINITIONS,
    STRATEGY_TRIPLE_EMA_ID,
)
from okx_quant.strategies.triple_ema import (
    TREND_DOWN,
    TREND_SIDEWAY,
    TREND_UP,
    calculate_triple_ema,
    evaluate_triple_ema_signal,
    generate_signal,
    live_position_action,
)


def _instrument() -> Instrument:
    return Instrument(
        inst_id="BTC-USDT-SWAP",
        inst_type="SWAP",
        tick_size=Decimal("1"),
        lot_size=Decimal("0.01"),
        min_size=Decimal("0.01"),
        state="live",
        settle_ccy="USDT",
        ct_val=Decimal("1"),
    )


def _config() -> StrategyConfig:
    return StrategyConfig(
        inst_id="BTC-USDT-SWAP",
        bar="1H",
        ema_type="ema",
        ema_period=3,
        trend_ema_type="ema",
        trend_ema_period=5,
        big_ema_period=8,
        atr_period=14,
        atr_stop_multiplier=Decimal("0"),
        atr_take_multiplier=Decimal("4"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="both",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_TRIPLE_EMA_ID,
        risk_amount=Decimal("100"),
        take_profit_mode="fixed",
    )


def _candles() -> list[Candle]:
    start = 1_700_000_000_000
    candles = [
        Candle(
            start + (index * 3_600_000),
            Decimal("100"),
            Decimal("100"),
            Decimal("100"),
            Decimal("100"),
            Decimal("1"),
            True,
        )
        for index in range(BACKTEST_RESERVED_CANDLES)
    ]
    candles.append(
        Candle(
            start + (BACKTEST_RESERVED_CANDLES * 3_600_000),
            Decimal("100"),
            Decimal("110"),
            Decimal("100"),
            Decimal("110"),
            Decimal("1"),
            True,
        )
    )
    candles.append(
        Candle(
            start + ((BACKTEST_RESERVED_CANDLES + 1) * 3_600_000),
            Decimal("111"),
            Decimal("112"),
            Decimal("90"),
            Decimal("111"),
            Decimal("1"),
            True,
        )
    )
    return candles


def _live_candles(closes: list[str]) -> list[Candle]:
    previous_close = Decimal(closes[0])
    candles: list[Candle] = []
    for index, raw_close in enumerate(closes, start=1):
        close = Decimal(raw_close)
        candles.append(
            Candle(
                index,
                previous_close,
                max(previous_close, close) + Decimal("1"),
                min(previous_close, close) - Decimal("1"),
                close,
                Decimal("1"),
                True,
            )
        )
        previous_close = close
    return candles


class TripleEmaStrategyTest(TestCase):
    def test_catalog_exposes_real_and_backtest_strategy(self) -> None:
        definition = next(item for item in BACKTEST_STRATEGY_DEFINITIONS if item.strategy_id == STRATEGY_TRIPLE_EMA_ID)

        self.assertTrue(definition.supports_trade)
        self.assertTrue(definition.supports_signal_only)
        self.assertTrue(definition.supports_backtest)

    def test_signal_generation_only_enters_when_trend_first_forms(self) -> None:
        frame = pd.DataFrame(
            {"close": [100, 101, 102, 103, 104]},
            index=pd.date_range("2026-01-01", periods=5, freq="h"),
        )
        signals = calculate_triple_ema(frame, fast_period=2, middle_period=3, slow_period=4)

        self.assertEqual(signals.iloc[0]["trend_state"], TREND_SIDEWAY)
        self.assertEqual(signals.iloc[-1]["trend_state"], TREND_UP)
        self.assertEqual(signals["long_entry"].sum(), 1)
        entry_index = int(signals["long_entry"].to_numpy().argmax())
        self.assertEqual(generate_signal(signals.iloc[: entry_index + 1], position=None).action, "buy")

    def test_position_exit_and_reverse_rules(self) -> None:
        frame = pd.DataFrame(
            {
                "close": [100],
                "ema_fast": [3],
                "ema_mid": [2],
                "ema_slow": [1],
                "trend_state": [TREND_UP],
                "previous_trend_state": [TREND_DOWN],
            },
            index=pd.date_range("2026-01-01", periods=1, freq="h"),
        )

        self.assertEqual(generate_signal(frame, position="short").action, "reverse_to_long")
        sideway = frame.assign(trend_state=TREND_SIDEWAY)
        self.assertEqual(generate_signal(sideway, position="long").action, "exit")

    def test_backtest_enters_next_open_and_uses_slow_ema_stop_by_default(self) -> None:
        result = _run_backtest_with_loaded_data(_candles(), _instrument(), _config())

        self.assertEqual(len(result.trades), 1)
        trade = result.trades[0]
        self.assertEqual(trade.signal, "long")
        self.assertEqual(trade.entry_index, BACKTEST_RESERVED_CANDLES + 1)
        self.assertEqual(trade.entry_price, Decimal("111"))
        self.assertEqual(trade.exit_reason, "stop_loss")
        self.assertEqual(trade.metadata["initial_stop_source"], "slow_ema")

    def test_backtest_uses_atr_stop_when_multiplier_is_positive(self) -> None:
        result = _run_backtest_with_loaded_data(
            _candles(),
            _instrument(),
            replace(_config(), atr_stop_multiplier=Decimal("1")),
        )

        self.assertEqual(len(result.trades), 1)
        self.assertEqual(result.trades[0].metadata["initial_stop_source"], "atr")

    def test_live_signal_uses_first_completed_trend_and_reverses_position(self) -> None:
        config = replace(_config(), ema_period=2, trend_ema_period=3, big_ema_period=4)
        candles = _live_candles(["100", "100", "100", "100", "90", "120"])

        decision = evaluate_triple_ema_signal(candles, config)

        self.assertEqual(decision.signal, "long")
        self.assertEqual(live_position_action(candles, config, "short"), "reverse_to_long")

    def test_batch_and_report_keep_the_triple_ema_rules(self) -> None:
        config = _config()
        result = _run_backtest_with_loaded_data(_candles(), _instrument(), config)

        self.assertEqual(build_parameter_batch_configs(config), [config])
        report = format_backtest_report(result)
        self.assertIn("仅在趋势首次形成时入场", report)
        self.assertIn("慢线 EMA8", report)
