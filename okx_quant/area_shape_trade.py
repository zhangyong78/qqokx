"""Local, signal-only support/resistance area trading plans.

This module deliberately has no exchange-order methods. It evaluates confirmed
1H candles and maintains a persisted paper position for review.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import uuid4

from okx_quant.indicators import atr
from okx_quant.models import Candle


AREA_PLAN_VERSION = 1
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
    atr_period: int = 14
    area_half_width_atr: Decimal = Decimal("0.25")
    entry_offset_atr: Decimal = Decimal("0")
    stop_offset_atr: Decimal = Decimal("0.2")
    target_buffer_atr: Decimal = Decimal("0")
    round_trip_fee_rate: Decimal = Decimal("0")
    slippage_rate: Decimal = Decimal("0")
    fixed_tp_r: Decimal = Decimal("1")
    be_trigger_r: Decimal = Decimal("2")
    lock_trigger_r: Decimal = Decimal("3")
    lock_profit_r: Decimal = Decimal("2")
    entry_mode: str = "next_open"
    resting_entry_enabled: bool = True
    exit_template: str = "fixed_1r"
    trigger_price_type: str = "last"
    valid_for_bars: int = 240
    signal_patterns: tuple[str, ...] = (
        "double_reversal_up", "double_reversal_down", "long_lower_shadow", "long_upper_shadow",
        "top_fractal", "bottom_fractal",
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
        )):
            raise ValueError("ATR offsets and costs must be finite and non-negative")
        if self.exit_template not in EXIT_TEMPLATES:
            raise ValueError("unsupported exit template")
        if not all(item.is_finite() for item in (self.fixed_tp_r, self.be_trigger_r, self.lock_trigger_r, self.lock_profit_r)):
            raise ValueError("exit R thresholds must be finite")
        if self.fixed_tp_r <= 0 or self.be_trigger_r <= 0 or self.lock_trigger_r <= self.be_trigger_r or self.lock_profit_r < 0 or self.lock_profit_r >= self.lock_trigger_r:
            raise ValueError("exit R thresholds are invalid")
        if self.entry_mode not in {"next_open", "breakout"}:
            raise ValueError("unsupported paper entry mode")
        if self.trigger_price_type != "last":
            raise ValueError("area monitoring currently uses confirmed last-price candles only")
        if not self.signal_patterns or any(item not in SUPPORTED_SIGNALS for item in self.signal_patterns):
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
        return cls(
            direction=str(raw.get("direction", "long")),
            risk_amount=_required_decimal(raw, "risk_amount"),
            min_space_r=_required_decimal(raw, "min_space_r"),
            atr_period=max(2, int(raw.get("atr_period", 14) or 14)),
            area_half_width_atr=_required_decimal(raw, "area_half_width_atr"),
            entry_offset_atr=_required_decimal(raw, "entry_offset_atr"),
            stop_offset_atr=_required_decimal(raw, "stop_offset_atr"),
            target_buffer_atr=_required_decimal(raw, "target_buffer_atr"),
            round_trip_fee_rate=_required_decimal(raw, "round_trip_fee_rate"),
            slippage_rate=_required_decimal(raw, "slippage_rate"),
            fixed_tp_r=_required_decimal(raw, "fixed_tp_r"),
            be_trigger_r=_required_decimal(raw, "be_trigger_r"),
            lock_trigger_r=_required_decimal(raw, "lock_trigger_r"),
            lock_profit_r=_required_decimal(raw, "lock_profit_r"),
            entry_mode=str(raw.get("entry_mode", "next_open")),
            resting_entry_enabled=bool(raw.get("resting_entry_enabled", True)),
            exit_template=str(raw.get("exit_template", "fixed_1r")),
            trigger_price_type=str(raw.get("trigger_price_type", "last")),
            valid_for_bars=max(1, int(raw.get("valid_for_bars", 240) or 240)),
            signal_patterns=tuple(str(item) for item in raw.get("signal_patterns", cls.signal_patterns) if str(item)),
        )


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
    expires_at_candle_ts: int = 0
    last_signal_key: str = ""
    last_candle_ts: int = 0
    events: list[dict[str, object]] = field(default_factory=list)
    paper_position: dict[str, object] | None = None
    version: int = AREA_PLAN_VERSION
    execution_enabled: bool = False
    execution_mode: str = "paper"

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
        if self.execution_mode not in {"paper", "demo_exchange"}:
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
            "last_signal_key": self.last_signal_key, "last_candle_ts": self.last_candle_ts,
            "events": list(self.events), "paper_position": self.paper_position,
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
            last_signal_key=str(payload.get("last_signal_key", "")), last_candle_ts=int(payload.get("last_candle_ts", 0) or 0),
            events=[dict(item) for item in payload.get("events", []) if isinstance(item, dict)],
            paper_position=dict(payload["paper_position"]) if isinstance(payload.get("paper_position"), dict) else None,
            execution_enabled=False,
            execution_mode=str(payload.get("execution_mode", "paper")),
        )

    def activate(self, *, last_closed_candle_ts: int, profile_name: str, environment: str) -> None:
        if self.status not in {"draft", "paused"}:
            raise ValueError("only draft or paused plans can start monitoring")
        if profile_name.strip() != self.profile_name or environment.strip().lower() != self.environment:
            raise ValueError("plan account context does not match the selected account")
        self.activated_at = _now()
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
        if self.status in {"draft", "monitoring", "touched", "paper_pending", "paused"}:
            if self.status == "paper_pending":
                self.paper_position = None
            self.status = "cancelled"
            self._event("cancelled", "计划已取消")

    def observe(self, candles: list[Candle]) -> dict[str, object] | None:
        """Evaluate fresh confirmed candles only; returns an accepted/skip signal event."""
        if self.status not in {"monitoring", "touched", "paper_pending"} or not candles:
            return None
        confirmed = [item for item in candles if item.confirmed]
        if len(confirmed) < self.config.atr_period + 1:
            return None
        if self.pressure_price <= self.support_price:
            self.status = "manual_review"
            return self._event("invalid_region", "支撑价必须低于压力价；监控已停止")
        values = atr(confirmed, self.config.atr_period)
        patterns = set(self.config.signal_patterns)
        chart_patterns_by_ts = _chart_patterns_by_candle_ts(confirmed)
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
            current_atr = values[index] if index < len(values) else None
            if current_atr is None or current_atr <= 0:
                continue
            entry_line = self.support_price if self.config.direction == "long" else self.pressure_price
            touched = candle.low <= entry_line <= candle.high
            if not touched:
                continue
            self.status = "touched"
            expected_direction = self.config.direction
            matching_pattern = _matching_chart_pattern(
                chart_patterns_by_ts, candle_ts=candle.ts,
                expected_direction=expected_direction, selected=patterns,
            )
            # Old persisted drafts retain their original single-candle signal semantics.
            if matching_pattern is None and patterns.intersection({
                "hammer", "inverted_hammer", "dragonfly_doji", "shooting_star", "hanging_man", "gravestone_doji",
            }):
                matching_pattern = _matching_legacy_single_candle_pattern(
                    confirmed[: index + 1], expected_direction=expected_direction, selected=patterns,
                )
            signal_key = f"{self.profile_name}|{self.environment}|{self.symbol}|{candle.ts}|{matching_pattern or 'no-pattern'}"
            if signal_key == self.last_signal_key:
                continue
            if matching_pattern is None:
                self.last_signal_key = signal_key
                self._event("touch_without_signal", f"触及区域但没有有效的{'看涨' if self.config.direction == 'long' else '看跌'}反转形态")
                continue
            event = self._build_paper_entry(candle, current_atr, matching_pattern, signal_key)
            self.last_signal_key = signal_key
            self._event(str(event["kind"]), str(event["message"]), **{k: v for k, v in event.items() if k not in {"kind", "message"}})
            if event["kind"] == "paper_pending":
                self.paper_position = dict(event["paper_position"])
                self.status = "paper_pending"
            return event
        return None

    def _build_paper_entry(self, candle: Candle, current_atr: Decimal, pattern: str, key: str) -> dict[str, object]:
        side = self.config.direction
        stop = (candle.low - current_atr * self.config.stop_offset_atr) if side == "long" else (candle.high + current_atr * self.config.stop_offset_atr)
        trigger = (candle.high + current_atr * self.config.entry_offset_atr) if side == "long" else (candle.low - current_atr * self.config.entry_offset_atr)
        if stop <= 0 or (side == "long" and stop >= candle.close) or (side == "short" and stop <= candle.close):
            return {"kind": "signal_skipped", "message": "形态止损关系无效，跳过本次信号", "candle_ts": candle.ts}
        pending = {
            "signal_key": key, "side": side, "stop": str(stop), "signal_candle_ts": candle.ts,
            # Keep the shape breakout threshold on every accepted signal. Paper
            # next-open simulation still ignores it; the demo executor submits
            # this threshold as an exchange-hosted trigger order.
            "entry_trigger": str(trigger),
            "entry_mode": self.config.entry_mode, "stage": "pending_entry", "pattern": pattern,
            "risk_amount": str(self.config.risk_amount), "rule_version": self.version,
            "target_buffer_atr": str(current_atr * self.config.target_buffer_atr),
        }
        return {
            "kind": "paper_pending",
            "message": f"{side} {pattern} 信号通过方向与区域检查；纸面入场等待未来行情触发",
            "candle_ts": candle.ts, "paper_position": pending,
        }

    def _observe_pending_entry(self, candle: Candle) -> dict[str, object] | None:
        pending = self.paper_position
        if not pending or candle.ts <= int(pending.get("signal_candle_ts", 0) or 0):
            return None
        side = str(pending["side"])
        sign = Decimal("1") if side == "long" else Decimal("-1")
        stop = _decimal(pending["stop"])
        if str(pending.get("entry_mode")) == "breakout":
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
            return self._event("signal_skipped", "未来纸面入场价已越过初始止损，未模拟成交", candle_ts=candle.ts)
        risk = abs(entry - stop)
        target = self.pressure_price if side == "long" else self.support_price
        buffer = entry * (self.config.round_trip_fee_rate + self.config.slippage_rate) + _decimal(pending.get("target_buffer_atr"))
        available = (target - entry - buffer) if side == "long" else (entry - target - buffer)
        space_r = available / risk
        if space_r < self.config.min_space_r:
            self.status = "monitoring"
            self.paper_position = None
            return self._event("signal_skipped", f"未来入场价目标空间 {space_r:.2f}R 低于最低要求，未模拟成交", candle_ts=candle.ts, space_r=str(space_r))
        cost_offset = entry * (self.config.round_trip_fee_rate + self.config.slippage_rate)
        fixed = self.config.exit_template == "fixed_1r"
        take_profit = entry + sign * (risk * self.config.fixed_tp_r + cost_offset) if fixed else Decimal("0")
        self.paper_position = {
            **pending, "entry": str(entry), "initial_stop": str(stop), "risk_distance": str(risk),
            "target": str(target), "take_profit": str(take_profit) if fixed else "",
            "entry_ts": candle.ts, "entry_time": datetime.fromtimestamp(candle.ts / 1000, tz=timezone.utc).isoformat(),
            "last_checked_ts": candle.ts - 1, "stage": "initial", "space_r": str(space_r),
            "contracts": None, "sizing_status": "未计算：未核验合约张值、手续费与滑点规格",
        }
        self.status = "paper_position"
        result = self._event("paper_entry", f"纸面模拟按未来1H行情在 {entry} 成交；成交K线不参与止损/止盈回放", candle_ts=candle.ts, entry=str(entry), space_r=str(space_r), entry_mode=self.config.entry_mode)
        if self.config.entry_mode == "breakout":
            # Entry and stop can both be crossed in this bar; OHLC does not reveal
            # their order, so the simulation conservatively applies the stop first.
            stop_hit = candle.low <= stop if side == "long" else candle.high >= stop
            tp = _decimal(self.paper_position.get("take_profit"))
            tp_hit = bool(self.paper_position.get("take_profit")) and (candle.high >= tp if side == "long" else candle.low <= tp)
            if stop_hit:
                self.status = "paper_closed"
                self._event("paper_stop", "突破成交bar同时包含入场与止损条件；按保守顺序记止损退出", candle_ts=candle.ts, exit_price=str(stop), ambiguous_bar=True, take_profit_also_touched=tp_hit)
                self.paper_position = None
        else:
            # Next-open fill occurs before the rest of the confirmed bar.
            self.observe_paper_candle(candle)
        return result

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
        risk = _decimal(position["risk_distance"])
        stop = _decimal(position["stop"])
        favorable = candle.high if side == "long" else candle.low
        adverse = candle.low if side == "long" else candle.high
        stop_hit = adverse <= stop if side == "long" else adverse >= stop
        tp_raw = str(position.get("take_profit", "") or "")
        tp = _decimal(tp_raw)
        tp_hit = bool(tp_raw) and (favorable >= tp if side == "long" else favorable <= tp)
        position["last_checked_ts"] = candle.ts
        sign = Decimal("1") if side == "long" else Decimal("-1")
        if stop_hit:
            self.status = "paper_closed"
            ambiguous = tp_hit
            result = self._event("paper_stop", f"纸面止损触发于 {stop}；同bar多价位顺序不明时按止损先发生处理", candle_ts=candle.ts, exit_price=str(stop), ambiguous_bar=ambiguous)
            self.paper_position = None
            return result
        if tp_hit:
            self.status = "paper_closed"
            result = self._event("paper_take_profit", f"纸面固定 {self.config.fixed_tp_r}R 止盈触发于 {tp}", candle_ts=candle.ts, exit_price=str(tp))
            self.paper_position = None
            return result
        progress_r = ((favorable - entry) / risk) if side == "long" else ((entry - favorable) / risk)
        target_stop: Decimal | None = None
        pending_stage = str(position.get("pending_stage", ""))
        if self.config.exit_template != "fixed_1r" and (progress_r >= self.config.lock_trigger_r or pending_stage == "locked"):
            target_stop = entry + sign * (risk * self.config.lock_profit_r + entry * (self.config.round_trip_fee_rate + self.config.slippage_rate))
        elif self.config.exit_template != "fixed_1r" and (progress_r >= self.config.be_trigger_r or pending_stage == "break_even"):
            target_stop = entry + sign * entry * (self.config.round_trip_fee_rate + self.config.slippage_rate)
        if target_stop is not None:
            old_stop = _decimal(position["stop"])
            tightens = target_stop > old_stop if side == "long" else target_stop < old_stop
            if tightens:
                close_allows = target_stop < candle.close if side == "long" else target_stop > candle.close
                candidate_already_crossed = candle.low <= target_stop if side == "long" else candle.high >= target_stop
                if not close_allows or candidate_already_crossed:
                    position["pending_stop_price"] = str(target_stop)
                    position["pending_stage"] = "locked" if pending_stage == "locked" or progress_r >= self.config.lock_trigger_r else "break_even"
                    return self._event("paper_stop_pending", "K线触发保护档位，但收盘价或bar内路径不能证明候选止损安全；保留待处理档位", candle_ts=candle.ts, candidate_stop=str(target_stop), progress_r=str(progress_r), ambiguous_bar=True)
                position["stop"] = str(target_stop)
                updated_stage = "locked" if pending_stage == "locked" or progress_r >= self.config.lock_trigger_r else "break_even"
                position.pop("pending_stop_price", None)
                position.pop("pending_stage", None)
                position["stage"] = updated_stage
                self.status = "paper_locked" if updated_stage == "locked" else "paper_break_even"
                return self._event("paper_stop_tightened", f"纸面保护档位已在当前收盘价校验后更新至 {target_stop}", candle_ts=candle.ts, stop=str(target_stop), progress_r=str(progress_r), stage=updated_stage)
        return None

    def _event(self, kind: str, message: str, **payload: object) -> dict[str, object]:
        event = {"kind": kind, "message": message, "occurred_at": _now(), **payload}
        self.events = [event, *self.events][:200]
        return event


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _chart_patterns_by_candle_ts(candles: list[Candle]) -> dict[int, list[tuple[str, str]]]:
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
    result: dict[int, list[tuple[str, str]]] = {}
    for marker in markers:
        try:
            candle_ts = int(marker.get("time", 0)) * 1000
        except (TypeError, ValueError):
            continue
        pattern = str(marker.get("pattern_id", "")).strip().lower()
        direction = str(marker.get("direction", "")).strip().lower()
        if candle_ts > 0 and pattern and direction in {"long", "short"}:
            result.setdefault(candle_ts, []).append((pattern, direction))
    return result


def _matching_chart_pattern(
    patterns_by_ts: dict[int, list[tuple[str, str]]], *, candle_ts: int,
    expected_direction: str, selected: set[str],
) -> str | None:
    if not selected.intersection(CHART_PATTERN_SIGNALS):
        return None
    for pattern, direction in patterns_by_ts.get(int(candle_ts), []):
        if pattern in selected and direction == expected_direction:
            return pattern
    return None


def _matching_legacy_single_candle_pattern(
    candles: list[Candle], *, expected_direction: str, selected: set[str],
) -> str | None:
    from okx_quant.candle_patterns import SingleCandlePatternConfig, analyze_single_candle_patterns

    report = analyze_single_candle_patterns(candles, inst_id=None, config=SingleCandlePatternConfig())
    expected_bias = "bullish_reversal" if expected_direction == "long" else "bearish_reversal"
    return next((item.pattern for item in report.matches if item.bias == expected_bias and item.pattern in selected), None)

