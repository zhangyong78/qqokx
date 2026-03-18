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
    tick_size: Decimal
    lot_size: Decimal
    min_size: Decimal
    state: str
    settle_ccy: str | None = None
    ct_val: Decimal | None = None
    ct_val_ccy: str | None = None


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
    strategy_id: str = "ema_dynamic_order"
    poll_seconds: float = 3.0
    risk_amount: Decimal | None = None


@dataclass(frozen=True)
class SignalDecision:
    signal: SignalDirection | None
    reason: str
    candle_ts: int | None
    entry_reference: Decimal | None
    atr_value: Decimal | None
    ema_value: Decimal | None


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
