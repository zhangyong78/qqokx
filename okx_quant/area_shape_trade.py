"""Local, signal-only support/resistance area trading plans.

This module deliberately has no exchange-order methods. It evaluates confirmed
1H candles and maintains a persisted paper position for review.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from typing import Any
from uuid import uuid4

from okx_quant.indicators import atr
from okx_quant.models import Candle


AREA_PLAN_VERSION = 1
AREA_TAKER_FEE_RATE = Decimal("0.00036")
AREA_DYNAMIC_TAKE_PROFIT_R = Decimal("15")
AREA_MIN_ENTRY_SPACE_R = Decimal("2")
AREA_PLAN_STATUSES = {
    "draft", "monitoring", "touched", "paper_pending", "paper_position", "paper_break_even",
    "paper_locked", "paper_closed", "paused", "expired", "cancelled", "manual_review",
}
EXIT_TEMPLATES = {"fixed_1r", "lock_2r_at_3r", "lock_3r_at_4r"}
SUPPORTED_SIGNALS = {
    "big_bullish", "big_bearish", "long_upper_shadow", "long_lower_shadow",
    "double_reversal_up", "double_reversal_down", "inside_bar", "top_fractal", "bottom_fractal",
    # Keep old saved drafts loadable; new plans use the chart's 1H marker IDs.
    "hammer", "inverted_hammer", "dragonfly_doji", "shooting_star", "hanging_man", "gravestone_doji",
}
CHART_PATTERN_SIGNALS = {
    "big_bullish", "big_bearish", "long_upper_shadow", "long_lower_shadow",
    "double_reversal_up", "double_reversal_down", "inside_bar", "top_fractal", "bottom_fractal",
}


def _decimal(value: object, default: str = "0") -> Decimal:
    try:
        parsed = Decimal(str(value))
        return parsed if parsed.is_finite() else Decimal(default)
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _required_decimal(raw: dict[str, object], key: str) -> Decimal:
    value = Decimal(str(raw[key]))
    if not value.is_finite():
        raise ValueError(f"{key} must be finite")
    return value


@dataclass
class AreaShapeConfig:
    direction: str = "long"
    risk_amount: Decimal = Decimal("10")
    min_space_r: Decimal = Decimal("2")
    atr_period: int = 10
    area_half_width_atr: Decimal = Decimal("0.25")
    entry_offset_atr: Decimal = Decimal("0")
    stop_offset_atr: Decimal = Decimal("0.5")
    target_buffer_atr: Decimal = Decimal("0")
    round_trip_fee_rate: Decimal = AREA_TAKER_FEE_RATE
    slippage_rate: Decimal = Decimal("0")
    fixed_tp_r: Decimal = Decimal("1")
    break_even_enabled: bool = True
    be_trigger_r: Decimal = Decimal("2")
    time_break_even_enabled: bool = False
    time_break_even_bars: int = 0
    lock_trigger_r: Decimal = Decimal("3")
    lock_profit_r: Decimal = Decimal("2")
    lock_trail_every_r: Decimal = Decimal("1")
    lock_trail_add_r: Decimal = Decimal("1")
    entry_mode: str = "next_open"
    entry_trigger_mode: str = "pattern"
    stop_basis: str = "signal_candle"
    fixed_stop_distance: Decimal = Decimal("1000")
    fixed_stop_price: Decimal = Decimal("0")
    fee_offset_enabled: bool = True
    resting_entry_enabled: bool = False
    exit_template: str = "fixed_1r"
    trigger_price_type: str = "last"
    valid_for_bars: int = 240
    signal_patterns: tuple[str, ...] = (
        "double_reversal_up", "double_reversal_down", "long_lower_shadow", "long_upper_shadow",
        "top_fractal", "bottom_fractal",
    )

    def requires_atr(self) -> bool:
        return (
            (self.stop_basis not in {"fixed_distance", "fixed_price"} and self.stop_offset_atr > 0)
            or self.target_buffer_atr > 0
            or (self.entry_trigger_mode == "pattern" and self.entry_mode == "breakout"
                and self.entry_offset_atr > 0)
        )

    def validate(self) -> None:
        if self.direction not in {"long", "short"}:
            raise ValueError("direction must be long or short")
        if not self.risk_amount.is_finite() or not self.min_space_r.is_finite() or self.risk_amount <= 0 or self.min_space_r <= 0:
            raise ValueError("risk amount and minimum space R must be positive")
        if self.atr_period < 2 or self.valid_for_bars < 1:
            raise ValueError("ATR period and validity must be positive")
        if any(not value.is_finite() or value < 0 for value in (
            self.area_half_width_atr, self.entry_offset_atr, self.stop_offset_atr,
            self.target_buffer_atr, self.round_trip_fee_rate, self.slippage_rate,
            self.fixed_stop_distance, self.fixed_stop_price,
        )):
            raise ValueError("ATR offsets and costs must be finite and non-negative")
        if self.exit_template not in EXIT_TEMPLATES:
            raise ValueError("unsupported exit template")
        if self.time_break_even_bars < 0 or (self.time_break_even_enabled and self.time_break_even_bars < 1):
            raise ValueError("time break-even bars must be positive when enabled")
        if not all(item.is_finite() for item in (
            self.fixed_tp_r, self.be_trigger_r, self.lock_trigger_r, self.lock_profit_r,
            self.lock_trail_every_r, self.lock_trail_add_r,
        )):
            raise ValueError("exit R thresholds must be finite")
        dynamic_exit = self.exit_template != "fixed_1r"
        if (self.fixed_tp_r <= 0 or self.be_trigger_r <= 0
                or (dynamic_exit and self.break_even_enabled and self.lock_trigger_r <= self.be_trigger_r)
                or self.lock_profit_r < 0 or (dynamic_exit and self.lock_profit_r >= self.lock_trigger_r)
                or self.lock_trail_every_r <= 0 or self.lock_trail_add_r <= 0):
            raise ValueError("exit R thresholds are invalid")
        if self.entry_mode not in {"next_open", "breakout"}:
            raise ValueError("unsupported paper entry mode")
        if self.entry_trigger_mode not in {"pattern", "direct_touch"}:
            raise ValueError("unsupported entry trigger mode")
        if self.stop_basis not in {"signal_candle", "three_candle", "pattern_candles", "fixed_distance", "fixed_price", "entry_line_atr"}:
            raise ValueError("unsupported stop basis")
        if self.stop_basis == "fixed_distance" and self.fixed_stop_distance <= 0:
            raise ValueError("fixed stop distance must be positive")
        if self.stop_basis == "fixed_price" and self.fixed_stop_price <= 0:
            raise ValueError("fixed stop price must be positive")
        if self.stop_basis == "entry_line_atr" and self.stop_offset_atr <= 0:
            raise ValueError("entry-line ATR stop multiple must be positive")
        if self.trigger_price_type != "last":
            raise ValueError("area monitoring currently uses confirmed last-price candles only")
        if self.entry_trigger_mode == "pattern" and not self.signal_patterns:
            raise ValueError("at least one chart pattern must be selected")
        if any(item not in SUPPORTED_SIGNALS for item in self.signal_patterns):
            raise ValueError("signal pattern list is unsupported")

    def to_dict(self) -> dict[str, object]:
        data = asdict(self)
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = str(value)
        data["signal_patterns"] = list(self.signal_patterns)
        return data

    @classmethod
    def from_dict(cls, payload: dict[str, object] | None) -> "AreaShapeConfig":
        raw = payload if isinstance(payload, dict) else {}
        exit_template = str(raw.get("exit_template", "fixed_1r"))
        return cls(
            direction=str(raw.get("direction", "long")),
            risk_amount=_required_decimal(raw, "risk_amount"),
            min_space_r=_required_decimal(raw, "min_space_r"),
            atr_period=max(2, int(raw.get("atr_period", 10) or 10)),
            area_half_width_atr=_required_decimal(raw, "area_half_width_atr"),
            entry_offset_atr=_required_decimal(raw, "entry_offset_atr"),
            stop_offset_atr=_required_decimal(raw, "stop_offset_atr"),
            target_buffer_atr=_required_decimal(raw, "target_buffer_atr"),
            round_trip_fee_rate=_required_decimal(raw, "round_trip_fee_rate"),
            slippage_rate=_required_decimal(raw, "slippage_rate"),
            fixed_tp_r=_required_decimal(raw, "fixed_tp_r"),
            # Older fixed-target plans had no break-even protection; preserve that behavior.
            break_even_enabled=bool(raw.get("break_even_enabled", exit_template != "fixed_1r")),
            be_trigger_r=_required_decimal(raw, "be_trigger_r"),
            time_break_even_enabled=bool(raw.get("time_break_even_enabled", False)),
            time_break_even_bars=max(0, int(raw.get("time_break_even_bars", 0) or 0)),
            lock_trigger_r=_required_decimal(raw, "lock_trigger_r"),
            lock_profit_r=_required_decimal(raw, "lock_profit_r"),
            lock_trail_every_r=_decimal(raw.get("lock_trail_every_r", "1"), "1"),
            lock_trail_add_r=_decimal(raw.get("lock_trail_add_r", "1"), "1"),
            entry_mode=str(raw.get("entry_mode", "next_open")),
            entry_trigger_mode=str(raw.get("entry_trigger_mode", "pattern")),
            stop_basis=str(raw.get("stop_basis", "signal_candle")),
            fixed_stop_distance=_required_decimal(raw, "fixed_stop_distance") if "fixed_stop_distance" in raw else Decimal("1000"),
            fixed_stop_price=_required_decimal(raw, "fixed_stop_price") if "fixed_stop_price" in raw else Decimal("0"),
            # Older plans did not expose a fee-offset switch; preserve their behavior.
            fee_offset_enabled=bool(raw.get("fee_offset_enabled", False)),
            # Kept in the serialized schema for compatibility with older
            # snapshots, but pattern mode no longer supports a pre-hung GTC.
            resting_entry_enabled=False,
            exit_template=exit_template,
            trigger_price_type=str(raw.get("trigger_price_type", "last")),
            valid_for_bars=max(1, int(raw.get("valid_for_bars", 240) or 240)),
            signal_patterns=tuple(str(item) for item in raw.get("signal_patterns", cls.signal_patterns) if str(item)),
        )


def area_shape_pattern_candle_count(pattern: str | None) -> int:
    if pattern in {"bottom_fractal", "top_fractal"}:
        return 3
    if pattern in {"double_reversal_up", "double_reversal_down", "inside_bar"}:
        return 2
    return 1


def calculate_initial_stop(
    config: AreaShapeConfig, direction: str, *, entry_reference: Decimal,
    signal_candle: Candle, recent_candles: list[Candle], atr_value: Decimal,
) -> Decimal:
    """Return the configured initial stop relative to the entry line or signal candles."""
    stop_basis = config.stop_basis
    if stop_basis == "fixed_price":
        return config.fixed_stop_price
    if stop_basis == "fixed_distance":
        return entry_reference - config.fixed_stop_distance if direction == "long" else entry_reference + config.fixed_stop_distance
    if stop_basis == "entry_line_atr":
        distance = atr_value * config.stop_offset_atr
        return entry_reference - distance if direction == "long" else entry_reference + distance
    candles = recent_candles if stop_basis in {"three_candle", "pattern_candles"} else [signal_candle]
    low = min(item.low for item in candles)
    high = max(item.high for item in candles)
    return low - atr_value * config.stop_offset_atr if direction == "long" else high + atr_value * config.stop_offset_atr


def area_shape_target_space_r(
    *, entry: Decimal, stop: Decimal, target: Decimal, direction: str,
    cost_rate: Decimal, target_buffer: Decimal = Decimal("0"),
) -> Decimal:
    """Use the same original price-distance R as fixed targets and protection stages."""
    risk = abs(entry - stop)
    if risk <= 0:
        raise ValueError("initial R must be positive")
    reward = target - entry if direction == "long" else entry - target
    return (reward - target_buffer - (entry + target) * cost_rate) / risk


def area_shape_lock_profit_r(config: AreaShapeConfig, progress_r: Decimal) -> Decimal:
    """Return the stepped stop lock level once the configured lock trigger is reached."""
    if progress_r < config.lock_trigger_r:
        return config.lock_profit_r
    steps = ((progress_r - config.lock_trigger_r) / config.lock_trail_every_r).to_integral_value(rounding=ROUND_FLOOR)
    return config.lock_profit_r + steps * config.lock_trail_add_r


def area_shape_net_progress_r(
    config: AreaShapeConfig, *, direction: str, entry: Decimal, initial_stop: Decimal,
    price: Decimal, fee_rate: Decimal | None = None,
) -> Decimal:
    """Measure favorable progress after entry and exit taker fees, in initial-stop R units."""
    rate = fee_rate if fee_rate is not None else AREA_TAKER_FEE_RATE
    if not config.fee_offset_enabled:
        rate = Decimal("0")
    risk = abs(entry - initial_stop)
    if risk <= 0:
        raise ValueError("initial R must be positive")
    gross = price - entry if direction == "long" else entry - price
    round_trip_fee = (entry + price) * rate
    return (gross - round_trip_fee) / risk


def area_shape_price_for_net_r(
    *, entry: Decimal, initial_stop: Decimal, direction: str, target_r: Decimal,
    fee_rate: Decimal,
) -> Decimal:
    """Calculate the stop price that retains target R after fees on entry and exit."""
    risk = abs(entry - initial_stop)
    total_cost_rate = fee_rate
    if risk <= 0 or total_cost_rate < 0 or total_cost_rate >= 1:
        raise ValueError("net R price inputs are invalid")
    if direction == "long":
        return (entry * (Decimal("1") + total_cost_rate) + target_r * risk) / (Decimal("1") - total_cost_rate)
    if direction == "short":
        return (entry * (Decimal("1") - total_cost_rate) - target_r * risk) / (Decimal("1") + total_cost_rate)
    raise ValueError("direction must be long or short")


@dataclass
class AreaShapePlan:
    plan_id: str
    profile_name: str
    environment: str
    symbol: str
    period: str
    support_line_id: str
    pressure_line_id: str
    support_price: Decimal
    pressure_price: Decimal
    config: AreaShapeConfig = field(default_factory=AreaShapeConfig)
    status: str = "draft"
    created_at: str = ""
    activated_at: str = ""
    activation_candle_ts: int = 0
    expires_at_candle_ts: int = 0
    last_signal_key: str = ""
    last_candle_ts: int = 0
    events: list[dict[str, object]] = field(default_factory=list)
    paper_position: dict[str, object] | None = None
    version: int = AREA_PLAN_VERSION
    execution_enabled: bool = False
    execution_mode: str = "paper"
    cancel_after_position: bool = False

    def __post_init__(self) -> None:
        self.config.validate()
        if self.version != AREA_PLAN_VERSION:
            raise ValueError("unsupported area plan version")
        if self.status not in AREA_PLAN_STATUSES:
            raise ValueError(f"unsupported area plan status: {self.status}")
        if not self.plan_id or not self.profile_name or self.environment not in {"demo", "live"} or not self.symbol or self.period != "1H":
            raise ValueError("area plan identity or market context is invalid")
        self.support_price = _decimal(self.support_price)
        self.pressure_price = _decimal(self.pressure_price)
        if self.support_price <= 0 or self.pressure_price <= self.support_price:
            raise ValueError("support must be positive and below pressure")
        if self.execution_enabled:
            raise ValueError("area shape exchange execution is disabled")
        if self.execution_mode not in {"paper", "demo_exchange", "live_exchange"}:
            raise ValueError("unsupported area execution mode")

    @classmethod
    def create(
        cls, *, profile_name: str, environment: str, symbol: str, support_price: Decimal,
        pressure_price: Decimal, config: AreaShapeConfig | None = None,
        support_line_id: str | None = None, pressure_line_id: str | None = None,
    ) -> "AreaShapePlan":
        return cls(
            plan_id=uuid4().hex, profile_name=profile_name.strip(), environment=environment.strip().lower(),
            symbol=symbol.strip().upper(), period="1H", support_line_id=support_line_id or uuid4().hex,
            pressure_line_id=pressure_line_id or uuid4().hex, support_price=support_price,
            pressure_price=pressure_price, config=config or AreaShapeConfig(),
            created_at=_now(),
        )


    def to_dict(self) -> dict[str, object]:
        return {
            "version": self.version, "plan_id": self.plan_id, "profile_name": self.profile_name,
            "environment": self.environment, "symbol": self.symbol, "period": self.period,
            "support_line_id": self.support_line_id, "pressure_line_id": self.pressure_line_id,
            "support_price": str(self.support_price), "pressure_price": str(self.pressure_price),
            "config": self.config.to_dict(), "status": self.status, "created_at": self.created_at,
            "activated_at": self.activated_at, "expires_at_candle_ts": self.expires_at_candle_ts,
            "activation_candle_ts": self.activation_candle_ts,
            "last_signal_key": self.last_signal_key, "last_candle_ts": self.last_candle_ts,
            "events": list(self.events), "paper_position": self.paper_position,
            "cancel_after_position": self.cancel_after_position,
            "execution_enabled": False,
            "execution_mode": self.execution_mode,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "AreaShapePlan":
        if int(payload.get("version", 0) or 0) != AREA_PLAN_VERSION:
            raise ValueError("unsupported area plan version")
        return cls(
            version=int(payload.get("version", 0) or 0),
            plan_id=str(payload.get("plan_id", "")), profile_name=str(payload.get("profile_name", "")),
            environment=str(payload.get("environment", "")), symbol=str(payload.get("symbol", "")),
            period=str(payload.get("period", "")), support_line_id=str(payload.get("support_line_id", "")),
            pressure_line_id=str(payload.get("pressure_line_id", "")),
            support_price=_decimal(payload.get("support_price")), pressure_price=_decimal(payload.get("pressure_price")),
            config=AreaShapeConfig.from_dict(payload.get("config") if isinstance(payload.get("config"), dict) else None),
            status=str(payload.get("status", "draft")), created_at=str(payload.get("created_at", "")),
            activated_at=str(payload.get("activated_at", "")), expires_at_candle_ts=int(payload.get("expires_at_candle_ts", 0) or 0),
            activation_candle_ts=int(payload.get("activation_candle_ts", 0) or 0),
            last_signal_key=str(payload.get("last_signal_key", "")), last_candle_ts=int(payload.get("last_candle_ts", 0) or 0),
            events=[dict(item) for item in payload.get("events", []) if isinstance(item, dict)],
            paper_position=dict(payload["paper_position"]) if isinstance(payload.get("paper_position"), dict) else None,
            cancel_after_position=bool(payload.get("cancel_after_position", False)),
            execution_enabled=False,
            execution_mode=str(payload.get("execution_mode", "paper")),
        )

    def activate(self, *, last_closed_candle_ts: int, profile_name: str, environment: str) -> None:
        if self.status not in {"draft", "paused"}:
            raise ValueError("only draft or paused plans can start monitoring")
        if profile_name.strip() != self.profile_name or environment.strip().lower() != self.environment:
            raise ValueError("plan account context does not match the selected account")
        self.activated_at = _now()
        self.activation_candle_ts = last_closed_candle_ts
        self.expires_at_candle_ts = last_closed_candle_ts + self.config.valid_for_bars * 3_600_000
        self.last_candle_ts = max(self.last_candle_ts, last_closed_candle_ts)
        self.status = "monitoring"
        self._event("monitoring_started", "监控已启动；等待后续收盘形态与触区确认")

    def pause(self) -> None:
        if self.status in {"monitoring", "touched", "paper_pending"}:
            if self.status == "paper_pending":
                self.paper_position = None
            self.status = "paused"
            self._event("paused", "已暂停新入场信号")

    def cancel(self) -> None:
        if self.status in {"paper_position", "paper_break_even", "paper_locked"} and self.paper_position is not None:
            if not self.cancel_after_position:
                self.cancel_after_position = True
                self._event("cancel_after_position", "已请求取消；当前纸面持仓保护继续运行，平仓后计划取消")
            return
        if self.status in {"draft", "monitoring", "touched", "paper_pending", "paused", "paper_closed", "expired"}:
            if self.status == "paper_pending":
                self.paper_position = None
            self.status = "cancelled"
            self._event("cancelled", "计划已取消")

    def observe(self, candles: list[Candle]) -> dict[str, object] | None:
        """Evaluate fresh confirmed candles only; returns an accepted/skip signal event."""
        if self.status not in {"monitoring", "touched", "paper_pending"} or not candles:
            return None
        confirmed = [item for item in candles if item.confirmed]
        needs_atr = self.config.requires_atr()
        minimum_history = 3 if self.config.stop_basis == "three_candle" or self.config.entry_trigger_mode == "pattern" else 1
        if needs_atr:
            minimum_history = max(minimum_history, self.config.atr_period + 1)
        if len(confirmed) < minimum_history:
            return None
        if self.pressure_price <= self.support_price:
            self.status = "manual_review"
            return self._event("invalid_region", "支撑价必须低于压力价；监控已停止")
        values = atr(confirmed, self.config.atr_period) if needs_atr else []
        patterns = set(self.config.signal_patterns)
        chart_patterns_by_ts = _chart_patterns_by_candle_ts(confirmed) if self.config.entry_trigger_mode == "pattern" else {}
        activation_baseline = self.activation_candle_ts or (
            self.expires_at_candle_ts - self.config.valid_for_bars * 3_600_000
        )
        for index, candle in enumerate(confirmed):
            if candle.ts <= self.last_candle_ts:
                continue
            if candle.ts > self.expires_at_candle_ts:
                self.status = "expired"
                return self._event("expired", "计划已超过有效期")
            self.last_candle_ts = candle.ts
            if self.status == "paper_pending":
                pending_event = self._observe_pending_entry(candle)
                if pending_event is not None:
                    return pending_event
                continue
            current_atr = values[index] if needs_atr and index < len(values) else Decimal("0")
            if needs_atr and (current_atr is None or current_atr <= 0):
                continue
            entry_line = self.support_price if self.config.direction == "long" else self.pressure_price
            if self.config.entry_trigger_mode == "direct_touch":
                # A resting buy at support is triggered by a downward cross;
                # a resting sell at resistance is triggered by an upward cross.
                # Merely straddling the line while approaching from the wrong
                # side must not create an entry or replay opportunity.
                previous_price = confirmed[index - 1].close if index > 0 else candle.open
                crossed = (
                    previous_price > entry_line and candle.low <= entry_line
                    if self.config.direction == "long"
                    else previous_price < entry_line and candle.high >= entry_line
                )
                if candle.ts <= activation_baseline or not crossed:
                    continue
                return self.observe_direct_touch_price(
                    entry_line, candle.ts + 3_600_000, confirmed[: index + 1],
                    source="confirmed_1h_backfill",
                )
            expected_direction = self.config.direction
            matched_chart_pattern = None if self.config.entry_trigger_mode == "direct_touch" else _matching_chart_pattern(
                chart_patterns_by_ts, candle_ts=candle.ts,
                expected_direction=expected_direction, selected=patterns,
            )
            matching_pattern = "direct_touch" if self.config.entry_trigger_mode == "direct_touch" else (
                matched_chart_pattern[0] if matched_chart_pattern is not None else None
            )
            matching_score = matched_chart_pattern[1] if matched_chart_pattern is not None else None
            # Old persisted drafts retain their original single-candle signal semantics.
            if self.config.entry_trigger_mode != "direct_touch" and matching_pattern is None and patterns.intersection({
                "hammer", "inverted_hammer", "dragonfly_doji", "shooting_star", "hanging_man", "gravestone_doji",
            }):
                matching_pattern = _matching_legacy_single_candle_pattern(
                    confirmed[: index + 1], expected_direction=expected_direction, selected=patterns,
                )
                if matching_pattern is not None:
                    matching_score = 60
            # Multi-candle patterns may touch the line before their confirmation
            # candle. Limit the touch to this pattern's own confirmed window and
            # to candles after activation; never retain an unrelated old touch.
            span = area_shape_pattern_candle_count(matching_pattern)
            touched = any(
                item.ts > activation_baseline and item.low <= entry_line <= item.high
                for item in confirmed[max(0, index - span + 1): index + 1]
            )
            if not touched:
                continue
            self.status = "touched"
            signal_key = f"{self.profile_name}|{self.environment}|{self.symbol}|{candle.ts}|{matching_pattern or 'no-pattern'}"
            if signal_key == self.last_signal_key:
                continue
            if matching_pattern is None:
                self.last_signal_key = signal_key
                self._event("touch_without_signal", f"触及区域但没有有效的{'看涨' if self.config.direction == 'long' else '看跌'}反转形态")
                continue
            stop_span = span if self.config.stop_basis == "pattern_candles" else 3
            stop_candles = confirmed[max(0, index - stop_span + 1): index + 1]
            stop_price = calculate_initial_stop(
                self.config, self.config.direction, entry_reference=entry_line,
                signal_candle=candle, recent_candles=stop_candles, atr_value=current_atr,
            )
            pattern_score = min(100, int(matching_score or 60) + 15) if matching_pattern != "direct_touch" else None
            event = self._build_paper_entry(
                candle, current_atr, matching_pattern, signal_key,
                stop_price=stop_price, pattern_score=pattern_score,
            )
            self.last_signal_key = signal_key
            self._event(str(event["kind"]), str(event["message"]), **{k: v for k, v in event.items() if k not in {"kind", "message"}})
            if event["kind"] == "paper_pending":
                self.paper_position = dict(event["paper_position"])
                self.status = "paper_pending"
            return event
        return None

    def observe_direct_touch_price(
        self, price: Decimal, timestamp_ms: int, candles: list[Candle], *, source: str = "live_last_price",
    ) -> dict[str, object] | None:
        """Enter a paper trade at the configured line after a direct last-price touch."""
        if (self.config.entry_trigger_mode != "direct_touch"
                or self.status not in {"monitoring", "touched"} or not candles):
            return None
        confirmed = sorted((item for item in candles if item.confirmed), key=lambda item: item.ts)
        if not confirmed:
            return None
        signal_candle = confirmed[-1]
        current_atr = Decimal("0")
        if self.config.requires_atr():
            values = atr(confirmed, self.config.atr_period)
            atr_index = -2 if source == "confirmed_1h_backfill" else -1
            current_atr = values[atr_index] if len(values) >= abs(atr_index) else None
            if current_atr is None or current_atr <= 0:
                return None
        side = self.config.direction
        entry_line = self.support_price if side == "long" else self.pressure_price
        signal_key = f"{self.profile_name}|{self.environment}|{self.symbol}|{signal_candle.ts}|direct_touch"
        if signal_key == self.last_signal_key:
            return None
        recent = confirmed[-3:]
        stop = calculate_initial_stop(
            self.config, side, entry_reference=entry_line,
            signal_candle=signal_candle, recent_candles=recent, atr_value=current_atr,
        )
        event = self._build_paper_entry(
            signal_candle, current_atr, "direct_touch", signal_key, stop_price=stop,
        )
        self.last_signal_key = signal_key
        if event.get("kind") != "paper_pending":
            self.status = "monitoring"
            details = {key: value for key, value in event.items() if key not in {"kind", "message"}}
            details.update(
                candle_ts=timestamp_ms, trigger_price=str(entry_line), trigger_source=source,
            )
            return self._event(
                str(event.get("kind", "signal_skipped")), str(event.get("message", "直接触线信号无效")),
                **details,
            )
        pending = dict(event.get("paper_position", {}))
        pending["trigger_source"] = source
        pending["trigger_price"] = str(entry_line)
        self.paper_position = pending
        self.status = "paper_pending"
        fill_ts = max(int(timestamp_ms), signal_candle.ts + 1)
        tick_candle = Candle(fill_ts, price, price, price, price, Decimal("0"), True)
        result = self._observe_pending_entry(tick_candle)
        if result is None:
            return self._event(
                "touch_without_fill", "已触及入场线，但纸面入场校验未通过",
                candle_ts=fill_ts, trigger_price=str(entry_line), trigger_source=source,
            )
        if result.get("kind") == "paper_entry":
            # The entry is the planned line price; current last price is retained
            # for audit and later paper protection updates.
            result["trigger_price"] = str(entry_line)
            result["trigger_source"] = source
        return result

    def _build_paper_entry(
        self, candle: Candle, current_atr: Decimal, pattern: str, key: str, *,
        stop_price: Decimal | None = None, pattern_score: int | None = None,
    ) -> dict[str, object]:
        side = self.config.direction
        stop = stop_price if stop_price is not None else calculate_initial_stop(
            self.config, side, entry_reference=(self.support_price if side == "long" else self.pressure_price),
            signal_candle=candle, recent_candles=[candle], atr_value=current_atr,
        )
        trigger = (candle.high + current_atr * self.config.entry_offset_atr) if side == "long" else (candle.low - current_atr * self.config.entry_offset_atr)
        stop_reference = ((self.support_price if side == "long" else self.pressure_price)
                          if self.config.entry_trigger_mode == "direct_touch" else candle.close)
        if stop <= 0 or (side == "long" and stop >= stop_reference) or (side == "short" and stop <= stop_reference):
            return {"kind": "signal_skipped", "message": "形态止损关系无效，跳过本次信号", "candle_ts": candle.ts}
        pending = {
            "signal_key": key, "side": side, "stop": str(stop), "signal_candle_ts": candle.ts,
            # Kept for legacy paper replay snapshots; the current live pattern
            # executor enters immediately after the confirmed signal.
            "entry_trigger": str(trigger),
            "entry_mode": self.config.entry_mode, "entry_trigger_mode": self.config.entry_trigger_mode,
            "stop_basis": self.config.stop_basis, "stage": "pending_entry", "pattern": pattern,
            "pattern_score": pattern_score,
            "risk_amount": str(self.config.risk_amount), "rule_version": self.version,
            "target_buffer_atr": str(current_atr * self.config.target_buffer_atr),
        }
        return {
            "kind": "paper_pending",
            "message": f"{side} {pattern} 信号通过方向与区域检查；纸面入场等待未来行情触发",
            "candle_ts": candle.ts, "pattern_score": pattern_score, "paper_position": pending,
        }

    def _observe_pending_entry(self, candle: Candle) -> dict[str, object] | None:
        pending = self.paper_position
        if not pending or candle.ts <= int(pending.get("signal_candle_ts", 0) or 0):
            return None
        side = str(pending["side"])
        stop = _decimal(pending["stop"])
        if str(pending.get("entry_trigger_mode")) == "direct_touch":
            entry = self.support_price if side == "long" else self.pressure_price
        elif str(pending.get("entry_mode")) == "breakout":
            trigger = _decimal(pending.get("entry_trigger"))
            crossed = candle.high >= trigger if side == "long" else candle.low <= trigger
            if not crossed:
                return None
            entry = max(candle.open, trigger) if side == "long" else min(candle.open, trigger)
        else:
            # A next-open simulation uses the next confirmed bar's open as the fill proxy.
            entry = candle.open
        if entry <= 0 or (side == "long" and stop >= entry) or (side == "short" and stop <= entry):
            self.status = "monitoring"
            self.paper_position = None
            return self._event("signal_skipped", "纸面入场价已越过初始止损，未模拟成交", candle_ts=candle.ts)
        risk = abs(entry - stop)
        target = self.pressure_price if side == "long" else self.support_price
        space_r = area_shape_target_space_r(
            entry=entry, stop=stop, target=target, direction=side,
            cost_rate=self.config.round_trip_fee_rate + self.config.slippage_rate,
            target_buffer=_decimal(pending.get("target_buffer_atr")),
        )
        required_space_r = max(self.config.min_space_r,
                               self.config.fixed_tp_r if self.config.exit_template == "fixed_1r" else Decimal(0))
        if space_r < required_space_r:
            self.status = "monitoring"
            self.paper_position = None
            return self._event(
                "signal_skipped",
                f"盈亏比不足：入场价目标空间 {space_r:.2f}R 低于最低要求 {required_space_r}R，未模拟成交",
                candle_ts=candle.ts,
                signal_candle_ts=int(pending.get("signal_candle_ts", 0) or 0),
                entry=str(entry), stop=str(stop), target=str(target), risk_distance=str(risk),
                space_r=str(space_r), min_space_r=str(required_space_r),
                pattern=str(pending.get("pattern", "")), side=side,
                pattern_score=pending.get("pattern_score"),
            )
        fixed = self.config.exit_template == "fixed_1r"
        target_r = self.config.fixed_tp_r if fixed else AREA_DYNAMIC_TAKE_PROFIT_R
        try:
            take_profit = area_shape_price_for_net_r(
                entry=entry, initial_stop=stop, direction=side, target_r=target_r,
                fee_rate=self.config.round_trip_fee_rate + self.config.slippage_rate,
            )
            if take_profit <= 0:
                raise ValueError("止盈目标价非正")
        except ValueError as exc:
            self.status = "monitoring"
            self.paper_position = None
            return self._event("signal_skipped", f"纸面止盈保护价不可用：{exc}", candle_ts=candle.ts)
        self.paper_position = {
            **pending, "entry": str(entry), "initial_stop": str(stop), "risk_distance": str(risk),
            "target": str(target), "take_profit": str(take_profit),
            "entry_ts": candle.ts, "entry_time": datetime.fromtimestamp(candle.ts / 1000, tz=timezone.utc).isoformat(),
            "last_checked_ts": (
                (candle.ts - 1 if pending.get("trigger_source") == "confirmed_1h_backfill"
                 else candle.ts // 3_600_000 * 3_600_000)
                if str(pending.get("entry_trigger_mode")) == "direct_touch" else candle.ts - 1
            ),
            "stage": "initial", "space_r": str(space_r),
            "contracts": None, "sizing_status": "未计算：未核验合约张值、手续费与滑点规格",
        }
        self.status = "paper_position"
        result = self._event(
            "paper_entry",
            (f"纸面价格触发在画线价 {entry} 入场；成交所在1H K线不倒推保护触发，后续实时价继续检查止损/止盈"
             if str(pending.get("entry_trigger_mode")) == "direct_touch"
             else f"纸面模拟按未来1H行情在 {entry} 成交；成交K线不参与止损/止盈回放"),
            candle_ts=candle.ts, entry=str(entry), stop=str(stop), risk_distance=str(risk),
            pattern=str(pending.get("pattern", "")), space_r=str(space_r), entry_mode=self.config.entry_mode,
            pattern_score=pending.get("pattern_score"), trigger_source=pending.get("trigger_source"),
            trigger_price=pending.get("trigger_price"), last_price=str(candle.close),
        )
        if self.config.entry_mode == "breakout":
            # Entry and stop can both be crossed in this bar; OHLC does not reveal
            # their order, so the simulation conservatively applies the stop first.
            stop_hit = candle.low <= stop if side == "long" else candle.high >= stop
            tp = _decimal(self.paper_position.get("take_profit"))
            tp_hit = bool(self.paper_position.get("take_profit")) and (candle.high >= tp if side == "long" else candle.low <= tp)
            if stop_hit:
                self.status = "paper_closed"
                self._event("paper_stop", "突破成交bar同时包含入场与止损条件；按保守顺序记止损退出", candle_ts=candle.ts, exit_price=str(stop), stop_price=str(stop), ambiguous_bar=True, take_profit_also_touched=tp_hit)
                self.paper_position = None
        elif str(pending.get("entry_trigger_mode")) != "direct_touch":
            # Next-open fill occurs before the rest of the confirmed bar.
            self.observe_paper_candle(candle)
        return result

    def observe_paper_live_price(self, price: Decimal, timestamp_ms: int) -> dict[str, object] | None:
        """Apply paper stop/target checks to a fresh live last-price update."""
        position = self.paper_position
        if (not position or position.get("trigger_source") != "live_last_price"
                or self.status not in {"paper_position", "paper_break_even", "paper_locked"}):
            return None
        side = str(position["side"])
        stop = _decimal(position["stop"])
        take_profit_raw = str(position.get("take_profit", "") or "")
        take_profit = _decimal(take_profit_raw)
        stop_hit = price <= stop if side == "long" else price >= stop
        target_hit = bool(take_profit_raw) and (price >= take_profit if side == "long" else price <= take_profit)
        if stop_hit:
            self.status = "paper_closed"
            result = self._event(
                "paper_stop", f"纸面最新价触发止损于 {stop}", candle_ts=timestamp_ms,
                exit_price=str(stop), stop_price=str(stop), trigger_source="live_last_price",
            )
            self.paper_position = None
            self._complete_deferred_cancel()
            return result
        if target_hit:
            self.status = "paper_closed"
            result = self._event(
                "paper_take_profit", f"纸面最新价触发止盈于 {take_profit}", candle_ts=timestamp_ms,
                exit_price=str(take_profit), trigger_source="live_last_price",
            )
            self.paper_position = None
            self._complete_deferred_cancel()
            return result
        return None

    def observe_paper_candle(self, candle: Candle) -> dict[str, object] | None:
        """Conservative paper-only stop/exit management; stop wins ambiguous bars."""
        position = self.paper_position
        if not position or self.status not in {"paper_position", "paper_break_even", "paper_locked"} or not candle.confirmed:
            return None
        if candle.ts <= int(position.get("last_checked_ts", 0) or 0):
            return None
        self.last_candle_ts = max(self.last_candle_ts, candle.ts)
        side = str(position["side"])
        entry = _decimal(position["entry"])
        initial_stop = _decimal(position["initial_stop"])
        risk = abs(entry - initial_stop)
        stop = _decimal(position["stop"])
        favorable = candle.high if side == "long" else candle.low
        adverse = candle.low if side == "long" else candle.high
        stop_hit = adverse <= stop if side == "long" else adverse >= stop
        tp_raw = str(position.get("take_profit", "") or "")
        tp = _decimal(tp_raw)
        tp_hit = bool(tp_raw) and (favorable >= tp if side == "long" else favorable <= tp)
        position["last_checked_ts"] = candle.ts
        if stop_hit:
            self.status = "paper_closed"
            ambiguous = tp_hit
            result = self._event(
                "paper_stop", f"纸面止损触发于 {stop}；同bar多价位顺序不明时按止损先发生处理",
                candle_ts=candle.ts, exit_price=str(stop), stop_price=str(stop), ambiguous_bar=ambiguous,
            )
            self.paper_position = None
            self._complete_deferred_cancel()
            return result
        if tp_hit:
            self.status = "paper_closed"
            target_r = self.config.fixed_tp_r if self.config.exit_template == "fixed_1r" else AREA_DYNAMIC_TAKE_PROFIT_R
            result = self._event("paper_take_profit", f"纸面 {target_r}R 止盈触发于 {tp}", candle_ts=candle.ts, exit_price=str(tp))
            self.paper_position = None
            self._complete_deferred_cancel()
            return result
        fee_rate = AREA_TAKER_FEE_RATE if self.config.fee_offset_enabled else Decimal("0")
        progress_r = area_shape_net_progress_r(
            self.config, direction=side, entry=entry, initial_stop=initial_stop,
            price=favorable, fee_rate=fee_rate,
        )
        target_stop: Decimal | None = None
        pending_stage = str(position.get("pending_stage", ""))
        dynamic_exit = self.config.exit_template != "fixed_1r"
        elapsed_bars = max(0, (candle.ts - int(position.get("entry_ts", candle.ts))) // 3_600_000)
        current_progress_r = area_shape_net_progress_r(
            self.config, direction=side, entry=entry, initial_stop=initial_stop,
            price=candle.close, fee_rate=fee_rate,
        )
        time_be_ready = (
            self.config.break_even_enabled and self.config.time_break_even_enabled
            and elapsed_bars >= self.config.time_break_even_bars and current_progress_r >= 0
        )
        if dynamic_exit and (progress_r >= self.config.lock_trigger_r or pending_stage == "locked"):
            locked_r = area_shape_lock_profit_r(self.config, progress_r)
            if pending_stage == "locked":
                locked_r = max(locked_r, _decimal(position.get("pending_lock_r", "0")))
            target_r = locked_r
            target_stop = area_shape_price_for_net_r(
                entry=entry, initial_stop=initial_stop, direction=side,
                target_r=target_r, fee_rate=fee_rate,
            )
        elif self.config.break_even_enabled and (
            progress_r >= self.config.be_trigger_r or pending_stage == "break_even" or time_be_ready
        ):
            target_stop = area_shape_price_for_net_r(
                entry=entry, initial_stop=initial_stop, direction=side,
                target_r=Decimal("0"), fee_rate=fee_rate,
            )
        if target_stop is not None:
            old_stop = _decimal(position["stop"])
            tightens = target_stop > old_stop if side == "long" else target_stop < old_stop
            if tightens:
                close_allows = target_stop < candle.close if side == "long" else target_stop > candle.close
                candidate_already_crossed = candle.low <= target_stop if side == "long" else candle.high >= target_stop
                if not close_allows or candidate_already_crossed:
                    position["pending_stop_price"] = str(target_stop)
                    position["pending_stage"] = "locked" if pending_stage == "locked" or progress_r >= self.config.lock_trigger_r else "break_even"
                    if position["pending_stage"] == "locked":
                        position["pending_lock_r"] = str(locked_r)
                    return self._event("paper_stop_pending", "K线触发保护档位，但收盘价或bar内路径不能证明候选止损安全；保留待处理档位", candle_ts=candle.ts, candidate_stop=str(target_stop), progress_r=str(progress_r), ambiguous_bar=True)
                position["stop"] = str(target_stop)
                updated_stage = "locked" if pending_stage == "locked" or progress_r >= self.config.lock_trigger_r else "break_even"
                position.pop("pending_stop_price", None)
                position.pop("pending_stage", None)
                position.pop("pending_lock_r", None)
                position["stage"] = updated_stage
                self.status = "paper_locked" if updated_stage == "locked" else "paper_break_even"
                return self._event("paper_stop_tightened", f"纸面保护档位已在当前收盘价校验后更新至 {target_stop}", candle_ts=candle.ts, stop=str(target_stop), progress_r=str(progress_r), stage=updated_stage)
        return None

    def _complete_deferred_cancel(self) -> None:
        if self.cancel_after_position:
            self.status = "cancelled"
            self._event("cancelled", "纸面持仓已结束，计划按待取消请求结束")

    def _event(self, kind: str, message: str, **payload: object) -> dict[str, object]:
        event = {"kind": kind, "message": message, "occurred_at": _now(), **payload}
        self.events = [event, *self.events][:200]
        return event


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _chart_patterns_by_candle_ts(candles: list[Candle]) -> dict[int, list[tuple[str, str, int]]]:
    """Run the same confirmed-candle marker algorithm used by the 1H chart."""
    if not candles:
        return {}
    # Import lazily because the Qt window imports this trading-plan module.
    from roll_terminal_qt.kline_analysis_window import _build_replay_signal_markers, _to_ema, _to_sma

    closes = [float(item.close) for item in candles]
    markers = _build_replay_signal_markers(
        candles=candles, period="1H", ema15_values=_to_ema(closes, 15),
        sma50_values=_to_sma(closes, 50),
    )
    result: dict[int, list[tuple[str, str, int]]] = {}
    for marker in markers:
        try:
            candle_ts = int(marker.get("time", 0)) * 1000
        except (TypeError, ValueError):
            continue
        pattern = str(marker.get("pattern_id", "")).strip().lower()
        direction = str(marker.get("direction", "")).strip().lower()
        if candle_ts > 0 and pattern and direction in {"long", "short"}:
            result.setdefault(candle_ts, []).append((pattern, direction, int(marker.get("score", 60) or 60)))
    return result


def _matching_chart_pattern(
    patterns_by_ts: dict[int, list[tuple[str, ...]]], *, candle_ts: int,
    expected_direction: str, selected: set[str],
) -> tuple[str, int] | None:
    if not selected.intersection(CHART_PATTERN_SIGNALS):
        return None
    for item in patterns_by_ts.get(int(candle_ts), []):
        if len(item) < 2:
            continue
        pattern, direction = str(item[0]), str(item[1])
        if pattern in selected and direction == expected_direction:
            try:
                score = int(item[2]) if len(item) >= 3 else 60
            except (TypeError, ValueError):
                score = 60
            return pattern, max(0, min(score, 100))
    return None


def _matching_legacy_single_candle_pattern(
    candles: list[Candle], *, expected_direction: str, selected: set[str],
) -> str | None:
    from okx_quant.candle_patterns import SingleCandlePatternConfig, analyze_single_candle_patterns

    report = analyze_single_candle_patterns(candles, inst_id=None, config=SingleCandlePatternConfig())
    expected_bias = "bullish_reversal" if expected_direction == "long" else "bearish_reversal"
    return next((item.pattern for item in report.matches if item.bias == expected_bias and item.pattern in selected), None)

