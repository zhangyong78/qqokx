from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path


class UiBacktestEntryMixin:
    def open_backtest_window(self) -> None:
        if self._backtest_window is not None and self._backtest_window.window.winfo_exists():
            self._backtest_window.window.focus_force()
            return

        backtest_strategy_name = self.strategy_name.get()
        backtest_strategy_names = {item.name for item in BACKTEST_STRATEGY_DEFINITIONS}
        if backtest_strategy_name not in backtest_strategy_names:
            backtest_strategy_name = BACKTEST_STRATEGY_DEFINITIONS[0].name

        self._backtest_window = BacktestWindow(
            self.root,
            self.client,
            BacktestLaunchState(
                strategy_name=backtest_strategy_name,
                symbol=_normalize_symbol_input(self.symbol.get()),
                bar=self.bar.get(),
                ema_period=self.ema_period.get(),
                trend_ema_period=self.trend_ema_period.get(),
                big_ema_period=self.big_ema_period.get(),
                entry_reference_ema_period=self.entry_reference_ema_period.get(),
                mtf_filter_bar=self.mtf_filter_bar.get(),
                mtf_filter_fast_ema_period=self.mtf_filter_fast_ema_period.get(),
                mtf_filter_slow_ema_period=self.mtf_filter_slow_ema_period.get(),
                mtf_reversal_mode_label=self.mtf_reversal_mode_label.get(),
                atr_period=self.atr_period.get(),
                stop_atr=self.stop_atr.get(),
                take_atr=self.take_atr.get(),
                risk_amount=self.risk_amount.get(),
                take_profit_mode_label=self.take_profit_mode_label.get(),
                max_entries_per_trend=self.max_entries_per_trend.get(),
                dynamic_two_r_break_even=self.dynamic_two_r_break_even.get(),
                dynamic_fee_offset_enabled=self.dynamic_fee_offset_enabled.get(),
                time_stop_break_even_enabled=self.time_stop_break_even_enabled.get(),
                time_stop_break_even_bars=self.time_stop_break_even_bars.get(),
                hold_close_exit_bars="0",
                signal_mode_label=self.signal_mode_label.get(),
                trade_mode_label=self.trade_mode_label.get(),
                position_mode_label=self.position_mode_label.get(),
                trigger_type_label=self.trigger_type_label.get(),
                environment_label=self.environment_label.get(),
                maker_fee_percent="0.015",
                taker_fee_percent="0.036",
                initial_capital="10000",
                sizing_mode_label="固定风险金",
                risk_percent="1",
                compounding_enabled=False,
                entry_slippage_percent="0",
                exit_slippage_percent="0",
                funding_rate_percent="0",
                candle_limit="10000",
            ),
        )

    def open_smart_order_window(self) -> None:
        if self._smart_order_window is not None and self._smart_order_window.window.winfo_exists():
            self._smart_order_window.show()
            return
        self._smart_order_window = SmartOrderWindow(
            self.root,
            self.client,
            runtime_config_provider=self._build_smart_order_runtime_config_or_none,
            logger=self._enqueue_log,
        )

    def open_backtest_compare_window(self) -> None:
        if self._backtest_compare_window is not None and self._backtest_compare_window.window.winfo_exists():
            self._backtest_compare_window.show()
            return
        self._backtest_compare_window = BacktestCompareOverviewWindow(self.root)

    def open_btc_market_analysis_window(self) -> None:
        if (
            self._btc_market_analysis_window is not None
            and self._btc_market_analysis_window.window.winfo_exists()
        ):
            self._btc_market_analysis_window.show()
            return

        self._btc_market_analysis_window = BtcMarketAnalysisWindow(
            self.root,
            self.client,
            logger=self._enqueue_log,
        )
