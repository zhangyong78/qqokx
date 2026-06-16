from decimal import Decimal
from unittest import TestCase

from okx_quant.backtest import BacktestReport, BacktestResult
from okx_quant.models import Candle, Instrument, StrategyConfig
from scripts.build_best_parameter_bundle import BundleRun, BundleSpec, _note_takeaway_text, _strategy_detail_note_html


def _config() -> StrategyConfig:
    return StrategyConfig(
        inst_id="DOGE-USDT-SWAP",
        bar="1H",
        ema_period=21,
        atr_period=13,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("4"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="short_only",
        position_mode="net",
        environment="live",
        tp_sl_trigger_type="mark",
    )


class BestParameterBundleNotesTest(TestCase):
    def test_strategy_detail_note_html_appends_backtest_range_when_missing(self) -> None:
        spec = BundleSpec(
            side="做空",
            symbol="DOGE-USDT-SWAP",
            profile_id="doge-test",
            profile_name="DOGE test",
            strategy_id="ema55_slope_short",
            strategy_label="均线斜率做空",
            core_label="MA21 / MA21",
            protection_label="ATR13 / SL2",
            note="DOGE 定稿说明。",
            config=_config(),
        )
        result = BacktestResult(
            candles=[
                Candle(
                    ts=1576504800000,
                    open=Decimal("1"),
                    high=Decimal("1"),
                    low=Decimal("1"),
                    close=Decimal("1"),
                    volume=Decimal("1"),
                    confirmed=True,
                ),
                Candle(
                    ts=1718391600000,
                    open=Decimal("1"),
                    high=Decimal("1"),
                    low=Decimal("1"),
                    close=Decimal("1"),
                    volume=Decimal("1"),
                    confirmed=True,
                ),
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
            instrument=Instrument(
                inst_id="DOGE-USDT-SWAP",
                inst_type="SWAP",
                tick_size=Decimal("0.0001"),
                lot_size=Decimal("1"),
                min_size=Decimal("1"),
                state="live",
            ),
        )
        run = BundleRun(spec=spec, result=result, data_source_note="local cache")

        html = _strategy_detail_note_html(spec, run)

        self.assertIn("&#x5B9A;&#x7A3F;&#x7ED3;&#x8BBA;", html)
        self.assertIn("&#x5168;&#x6837;&#x672C;", html)
        self.assertIn("2022-01-01", html)
        self.assertIn('class="note-meta"', html)
        self.assertIn("2019-12-16 14:00 -&gt; 2024-06-14 19:00", html)
        self.assertIn("2 &#x6839;&#xFF08;&#x5168;&#x91CF;&#xFF09;&#x3002;", html)

    def test_strategy_detail_note_html_keeps_existing_backtest_range_text(self) -> None:
        spec = BundleSpec(
            side="做空",
            symbol="BTC-USDT-SWAP",
            profile_id="btc-test",
            profile_name="BTC test",
            strategy_id="ema55_slope_short",
            strategy_label="均线斜率做空",
            core_label="EMA55 / EMA55",
            protection_label="ATR14 / SL2",
            note="BTC 说明；回测区间 2019-12-16 14:00 至 2026-06-14 19:00。",
            config=_config(),
        )
        result = BacktestResult(
            candles=[
                Candle(
                    ts=1576504800000,
                    open=Decimal("1"),
                    high=Decimal("1"),
                    low=Decimal("1"),
                    close=Decimal("1"),
                    volume=Decimal("1"),
                    confirmed=True,
                ),
                Candle(
                    ts=1781463600000,
                    open=Decimal("1"),
                    high=Decimal("1"),
                    low=Decimal("1"),
                    close=Decimal("1"),
                    volume=Decimal("1"),
                    confirmed=True,
                ),
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
            instrument=Instrument(
                inst_id="BTC-USDT-SWAP",
                inst_type="SWAP",
                tick_size=Decimal("0.0001"),
                lot_size=Decimal("1"),
                min_size=Decimal("1"),
                state="live",
            ),
        )
        run = BundleRun(spec=spec, result=result, data_source_note="local cache")

        html = _strategy_detail_note_html(spec, run)

        self.assertIn("BTC&#x505A;&#x7A7A;&#x5F53;&#x524D;&#x9ED8;&#x8BA4;&#x91C7;&#x7528;", html)
        self.assertIn('class="note-meta"', html)
        self.assertIn("2019-12-16 14:00 -&gt; 2026-06-14 19:00", html)

    def test_note_takeaway_text_keeps_last_research_clause(self) -> None:
        note = "ETH 定稿为 EMA21：全样本总盈亏 1；对比 EMA34 挂单线，EMA55 在盈亏与 PF 上更优。"

        takeaway = _note_takeaway_text(note)

        self.assertEqual(takeaway, "对比 EMA34 挂单线，EMA55 在盈亏与 PF 上更优。")
