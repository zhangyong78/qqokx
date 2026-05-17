from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from okx_quant.duration_input import format_duration_cn_compact

SignalMode = Literal["both", "long_only", "short_only"]
PositionMode = Literal["net", "long_short"]
EnvironmentMode = Literal["demo", "live"]
TriggerPriceType = Literal["last", "mark", "index"]
TradeMode = Literal["cross", "isolated"]
SignalDirection = Literal["long", "short"]
InstrumentType = Literal["SWAP", "OPTION", "SPOT"]
TpSlMode = Literal["exchange", "local_trade", "local_signal", "local_custom"]
EntrySideMode = Literal["follow_signal", "fixed_buy", "fixed_sell"]
RunMode = Literal["trade", "signal_only"]
BacktestSizingMode = Literal["fixed_risk", "fixed_size", "risk_percent"]
TakeProfitMode = Literal["fixed", "dynamic"]
MtfReversalMode = Literal["ignore", "block_new_entries"]
MovingAverageType = Literal["ema", "ma"]


def normalize_moving_average_type(value: str | None) -> MovingAverageType:
    return "ma" if str(value or "").strip().lower() == "ma" else "ema"


def moving_average_display_label(
    ma_type: str | None,
    period: int,
    *,
    with_parentheses: bool = False,
) -> str:
    prefix = "MA" if normalize_moving_average_type(ma_type) == "ma" else "EMA"
    return f"{prefix}({period})" if with_parentheses else f"{prefix}{period}"


@dataclass(frozen=True)
class Candle:
    ts: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    confirmed: bool


@dataclass(frozen=True)
class Instrument:
    inst_id: str
    inst_type: InstrumentType
    tick_size: Decimal
    lot_size: Decimal
    min_size: Decimal
    state: str
    settle_ccy: str | None = None
    ct_val: Decimal | None = None
    ct_mult: Decimal | None = None
    ct_val_ccy: str | None = None
    uly: str | None = None
    inst_family: str | None = None


@dataclass(frozen=True)
class Credentials:
    api_key: str
    secret_key: str
    passphrase: str
    profile_name: str = ""


@dataclass(frozen=True)
class StrategyConfig:
    inst_id: str
    bar: str
    ema_period: int
    atr_period: int
    atr_stop_multiplier: Decimal
    atr_take_multiplier: Decimal
    order_size: Decimal
    trade_mode: TradeMode
    signal_mode: SignalMode
    position_mode: PositionMode
    environment: EnvironmentMode
    tp_sl_trigger_type: TriggerPriceType
    ema_type: MovingAverageType = "ema"
    trend_ema_period: int = 55
    trend_ema_type: MovingAverageType = "ema"
    big_ema_period: int = 233
    strategy_id: str = "ema_dynamic_order"
    poll_seconds: float = 3.0
    risk_amount: Decimal | None = None
    trade_inst_id: str | None = None
    tp_sl_mode: TpSlMode = "exchange"
    local_tp_sl_inst_id: str | None = None
    entry_side_mode: EntrySideMode = "follow_signal"
    run_mode: RunMode = "trade"
    backtest_initial_capital: Decimal = Decimal("10000")
    backtest_sizing_mode: BacktestSizingMode = "fixed_risk"
    backtest_risk_percent: Decimal | None = None
    backtest_compounding: bool = False
    backtest_entry_slippage_rate: Decimal = Decimal("0")
    backtest_exit_slippage_rate: Decimal = Decimal("0")
    backtest_slippage_rate: Decimal = Decimal("0")
    backtest_funding_rate: Decimal = Decimal("0")
    take_profit_mode: TakeProfitMode = "dynamic"
    max_entries_per_trend: int = 1
    entry_reference_ema_period: int = 55
    entry_reference_ema_type: MovingAverageType = "ema"
    dynamic_two_r_break_even: bool = True
    dynamic_fee_offset_enabled: bool = True
    startup_chase_window_seconds: int = 0
    time_stop_break_even_enabled: bool = False
    time_stop_break_even_bars: int = 10
    hold_close_exit_bars: int = 0
    trader_virtual_stop_loss: bool = False
    backtest_profile_id: str = ""
    backtest_profile_name: str = ""
    backtest_profile_summary: str = ""
    # EMA 突破/跌破：可选更高周期偏置序列（例如 4H 收盘 vs 参考 EMA 过滤 1H 方向的突破/跌破）
    cross_higher_tf_inst_id: str | None = None
    cross_higher_tf_bar: str | None = None
    cross_higher_tf_ref_ema_period: int = 0
    mtf_filter_inst_id: str | None = None
    mtf_filter_bar: str | None = None
    mtf_filter_fast_ema_period: int = 21
    mtf_filter_slow_ema_period: int = 55
    mtf_reversal_mode: MtfReversalMode = "block_new_entries"
    rail_candidate_ema_periods: tuple[int, ...] = (5, 8, 13, 21, 34, 55, 89, 144, 233)
    rail_touch_atr_ratio: Decimal = Decimal("0.2")
    rail_bounce_atr_ratio: Decimal = Decimal("0.6")
    rail_bounce_confirm_bars: int = 3
    rail_break_atr_ratio: Decimal = Decimal("1.0")
    rail_reclaim_bars: int = 2
    rail_score_lookback_bars: int = 60
    rail_switch_min_score_delta: Decimal = Decimal("8")
    rail_min_touches: int = 2
    rail_min_bounces: int = 1

    def resolved_entry_reference_ema_period(self) -> int:
        if self.entry_reference_ema_period > 0:
            return self.entry_reference_ema_period
        return self.ema_period

    def resolved_ema_type(self) -> MovingAverageType:
        return normalize_moving_average_type(self.ema_type)

    def resolved_trend_ema_type(self) -> MovingAverageType:
        return normalize_moving_average_type(self.trend_ema_type)

    def resolved_entry_reference_ema_type(self) -> MovingAverageType:
        if self.entry_reference_ema_period > 0:
            return normalize_moving_average_type(self.entry_reference_ema_type)
        return self.resolved_ema_type()

    def ema_label(self) -> str:
        return moving_average_display_label(self.resolved_ema_type(), self.ema_period)

    def trend_ema_label(self) -> str:
        return moving_average_display_label(self.resolved_trend_ema_type(), self.trend_ema_period)

    def resolved_mtf_filter_inst_id(self) -> str:
        return (self.mtf_filter_inst_id or self.inst_id).strip()

    def resolved_mtf_filter_bar(self) -> str:
        return (self.mtf_filter_bar or self.bar).strip()

    def resolved_backtest_entry_slippage_rate(self) -> Decimal:
        if self.backtest_entry_slippage_rate > 0 or self.backtest_exit_slippage_rate > 0:
            return self.backtest_entry_slippage_rate
        return self.backtest_slippage_rate

    def resolved_backtest_exit_slippage_rate(self) -> Decimal:
        if self.backtest_entry_slippage_rate > 0 or self.backtest_exit_slippage_rate > 0:
            return self.backtest_exit_slippage_rate
        return self.backtest_slippage_rate

    def entry_reference_line_label(self) -> str:
        resolved_period = self.resolved_entry_reference_ema_period()
        resolved_type = self.resolved_entry_reference_ema_type()
        if self.entry_reference_ema_period > 0:
            return moving_average_display_label(resolved_type, resolved_period)
        return f"跟随快线({moving_average_display_label(resolved_type, resolved_period)})"

    def entry_reference_ema_label(self) -> str:
        resolved_period = self.resolved_entry_reference_ema_period()
        if self.entry_reference_ema_period > 0:
            return f"EMA{resolved_period}"
        return f"跟随快线(EMA{resolved_period})"

    def dynamic_two_r_break_even_label(self) -> str:
        return "\u5f00\u542f" if self.dynamic_two_r_break_even else "\u5173\u95ed"

    def dynamic_fee_offset_enabled_label(self) -> str:
        return "\u5f00\u542f" if self.dynamic_fee_offset_enabled else "\u5173\u95ed"

    def resolved_startup_chase_window_seconds(self) -> int:
        return max(int(self.startup_chase_window_seconds), 0)

    def startup_chase_window_label(self) -> str:
        seconds = self.resolved_startup_chase_window_seconds()
        if seconds <= 0:
            return "\u5173\u95ed\uff08\u542f\u52a8\u4e0d\u8ffd\u8001\u4fe1\u53f7\uff09"
        base = f"{seconds}\u79d2"
        if seconds >= 60:
            return f"{base}\uff08{format_duration_cn_compact(seconds)}\uff09"
        return base

    def resolved_time_stop_break_even_bars(self) -> int:
        return max(int(self.time_stop_break_even_bars), 0)

    def time_stop_break_even_enabled_label(self) -> str:
        return "\u5f00\u542f" if self.time_stop_break_even_enabled else "\u5173\u95ed"



@dataclass(frozen=True)
class EmailNotificationConfig:
    enabled: bool = False
    smtp_host: str = ""
    smtp_port: int = 465
    smtp_username: str = ""
    smtp_password: str = ""
    sender_email: str = ""
    recipient_emails: tuple[str, ...] = ()
    use_ssl: bool = True
    notify_trade_fills: bool = True
    notify_signals: bool = True
    notify_errors: bool = True


@dataclass(frozen=True)
class SignalDecision:
    signal: SignalDirection | None
    reason: str
    candle_ts: int | None
    entry_reference: Decimal | None
    atr_value: Decimal | None
    ema_value: Decimal | None
    signal_candle_high: Decimal | None = None
    signal_candle_low: Decimal | None = None


@dataclass(frozen=True)
class OrderPlan:
    inst_id: str
    side: Literal["buy", "sell"]
    pos_side: Literal["long", "short"] | None
    size: Decimal
    take_profit: Decimal
    stop_loss: Decimal
    entry_reference: Decimal
    atr_value: Decimal
    signal: SignalDirection
    candle_ts: int
    tp_sl_inst_id: str | None = None
    tp_sl_mode: TpSlMode = "exchange"


@dataclass(frozen=True)
class ProtectionPlan:
    trigger_inst_id: str
    trigger_price_type: TriggerPriceType
    take_profit: Decimal
    stop_loss: Decimal
    entry_reference: Decimal
    atr_value: Decimal
    direction: SignalDirection
    candle_ts: int
