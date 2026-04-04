from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

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
    trend_ema_period: int = 55
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
    backtest_slippage_rate: Decimal = Decimal("0")
    backtest_funding_rate: Decimal = Decimal("0")


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
