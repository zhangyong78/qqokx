from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal

from okx_quant.duration_input import format_duration_cn_compact

SignalMode = Literal["both", "long_only", "short_only"]
PositionMode = Literal["net", "long_short"]
EnvironmentMode = Literal["demo", "live"]
TriggerPriceType = Literal["last", "mark", "index"]
TradeMode = Literal["cross", "isolated"]
SignalDirection = Literal["long", "short"]
InstrumentType = Literal["SWAP", "FUTURES", "OPTION", "SPOT"]
TpSlMode = Literal["exchange", "local_trade", "local_signal", "local_custom"]
EntrySideMode = Literal["follow_signal", "fixed_buy", "fixed_sell"]
RunMode = Literal["trade", "signal_only"]
BacktestSizingMode = Literal["fixed_risk", "fixed_size", "risk_percent"]
TakeProfitMode = Literal["fixed", "dynamic"]
MtfReversalMode = Literal["ignore", "block_new_entries"]
MovingAverageType = Literal["ema", "ma"]
DailyFilterBoundary = Literal["exchange", "bjt_00", "bjt_08"]
DailyFilterMode = Literal["disabled", "close_vs_ma", "weak_day"]
DailyFilterScope = Literal["both", "long_only", "short_only"]
DynamicProtectionAction = Literal["break_even", "lock_profit"]
DynamicProtectionTrailMode = Literal["none", "step"]


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


def normalize_dynamic_protection_action(value: str | None) -> DynamicProtectionAction:
    return "break_even" if str(value or "").strip().lower() == "break_even" else "lock_profit"


def normalize_dynamic_protection_trail_mode(value: str | None) -> DynamicProtectionTrailMode:
    return "step" if str(value or "").strip().lower() == "step" else "none"


@dataclass(frozen=True)
class DynamicProtectionRule:
    trigger_r: int
    action: DynamicProtectionAction
    lock_r: int | None = None
    trail_mode: DynamicProtectionTrailMode = "none"
    trail_every_r: int | None = None
    trail_add_r: int | None = None

    def resolved_trigger_r(self) -> int:
        return max(int(self.trigger_r), 1)

    def resolved_action(self) -> DynamicProtectionAction:
        return normalize_dynamic_protection_action(self.action)

    def resolved_lock_r(self) -> int:
        if self.resolved_action() == "break_even":
            return 0
        return max(int(self.lock_r or 0), 0)

    def resolved_trail_mode(self) -> DynamicProtectionTrailMode:
        return normalize_dynamic_protection_trail_mode(self.trail_mode)

    def resolved_trail_every_r(self) -> int:
        return max(int(self.trail_every_r or 0), 1)

    def resolved_trail_add_r(self) -> int:
        return max(int(self.trail_add_r or 0), 1)

    def trailing_enabled(self) -> bool:
        return self.resolved_action() == "lock_profit" and self.resolved_trail_mode() == "step"

    def normalized(self) -> "DynamicProtectionRule":
        return DynamicProtectionRule(
            trigger_r=self.resolved_trigger_r(),
            action=self.resolved_action(),
            lock_r=self.resolved_lock_r() if self.resolved_action() == "lock_profit" else None,
            trail_mode=self.resolved_trail_mode() if self.resolved_action() == "lock_profit" else "none",
            trail_every_r=self.resolved_trail_every_r() if self.trailing_enabled() else None,
            trail_add_r=self.resolved_trail_add_r() if self.trailing_enabled() else None,
        )

    def to_payload(self) -> dict[str, Any]:
        normalized = self.normalized()
        return {
            "trigger_r": normalized.trigger_r,
            "action": normalized.action,
            "lock_r": normalized.lock_r,
            "trail_mode": normalized.trail_mode,
            "trail_every_r": normalized.trail_every_r,
            "trail_add_r": normalized.trail_add_r,
        }


def normalize_dynamic_protection_rules(
    rules: tuple[DynamicProtectionRule, ...] | list[DynamicProtectionRule] | tuple[dict[str, Any], ...] | list[dict[str, Any]] | None,
) -> tuple[DynamicProtectionRule, ...]:
    normalized: list[DynamicProtectionRule] = []
    for item in rules or ():
        if isinstance(item, DynamicProtectionRule):
            rule = item.normalized()
        else:
            rule = DynamicProtectionRule(
                trigger_r=max(int(item.get("trigger_r", 0) or 0), 1),
                action=normalize_dynamic_protection_action(str(item.get("action", "lock_profit"))),
                lock_r=(None if item.get("lock_r") in (None, "") else max(int(item.get("lock_r", 0) or 0), 0)),
                trail_mode=normalize_dynamic_protection_trail_mode(str(item.get("trail_mode", "none"))),
                trail_every_r=(None if item.get("trail_every_r") in (None, "") else max(int(item.get("trail_every_r", 0) or 0), 1)),
                trail_add_r=(None if item.get("trail_add_r") in (None, "") else max(int(item.get("trail_add_r", 0) or 0), 1)),
            ).normalized()
        normalized.append(rule)
    normalized.sort(key=lambda rule: rule.trigger_r)
    deduped: list[DynamicProtectionRule] = []
    seen_triggers: set[int] = set()
    for rule in normalized:
        if rule.trigger_r in seen_triggers:
            continue
        seen_triggers.add(rule.trigger_r)
        deduped.append(rule)
    return tuple(deduped)


def dynamic_protection_rules_to_payload(
    rules: tuple[DynamicProtectionRule, ...] | list[DynamicProtectionRule] | None,
) -> tuple[dict[str, Any], ...]:
    return tuple(rule.to_payload() for rule in normalize_dynamic_protection_rules(rules))


def dynamic_protection_rule_fires_at(rule: DynamicProtectionRule, trigger_r: int) -> bool:
    normalized = rule.normalized()
    current_trigger = max(int(trigger_r), 1)
    base_trigger = normalized.resolved_trigger_r()
    if current_trigger < base_trigger:
        return False
    if current_trigger == base_trigger:
        return True
    if not normalized.trailing_enabled():
        return False
    return (current_trigger - base_trigger) % normalized.resolved_trail_every_r() == 0


def dynamic_protection_rule_lock_r_at(rule: DynamicProtectionRule, trigger_r: int) -> int:
    normalized = rule.normalized()
    current_trigger = max(int(trigger_r), 1)
    lock_r = 0 if normalized.resolved_action() == "break_even" else normalized.resolved_lock_r()
    if not normalized.trailing_enabled() or current_trigger <= normalized.resolved_trigger_r():
        return lock_r
    step_count = (current_trigger - normalized.resolved_trigger_r()) // normalized.resolved_trail_every_r()
    return max(lock_r + step_count * normalized.resolved_trail_add_r(), 0)


def _dynamic_protection_lock_label(lock_r: int) -> str:
    return "保本" if max(int(lock_r), 0) <= 0 else f"锁 {max(int(lock_r), 0)}R"


def describe_dynamic_protection_rule_overlap_warnings(
    rules: tuple[DynamicProtectionRule, ...] | list[DynamicProtectionRule] | None,
) -> tuple[str, ...]:
    normalized = normalize_dynamic_protection_rules(rules)
    warnings: list[str] = []
    for index, rule in enumerate(normalized):
        trigger_r = rule.resolved_trigger_r()
        current_lock_r = dynamic_protection_rule_lock_r_at(rule, trigger_r)
        best_prior_rule: DynamicProtectionRule | None = None
        best_prior_lock_r: int | None = None
        for prior_rule in normalized[:index]:
            if not dynamic_protection_rule_fires_at(prior_rule, trigger_r):
                continue
            prior_lock_r = dynamic_protection_rule_lock_r_at(prior_rule, trigger_r)
            if best_prior_lock_r is None or prior_lock_r > best_prior_lock_r:
                best_prior_rule = prior_rule
                best_prior_lock_r = prior_lock_r
        if best_prior_rule is None or best_prior_lock_r is None:
            continue
        if best_prior_lock_r < current_lock_r:
            continue
        warnings.append(
            f"{trigger_r}R 规则在触发点不优于前序 {best_prior_rule.resolved_trigger_r()}R 规则"
            f"（前序已可{_dynamic_protection_lock_label(best_prior_lock_r)}，本条仅{_dynamic_protection_lock_label(current_lock_r)}）。"
        )
    return tuple(warnings)


def merge_dynamic_protection_rules(
    base_rules: tuple[DynamicProtectionRule, ...] | list[DynamicProtectionRule] | None,
    override_rules: tuple[DynamicProtectionRule, ...] | list[DynamicProtectionRule] | tuple[dict[str, Any], ...] | list[dict[str, Any]] | None,
) -> tuple[DynamicProtectionRule, ...]:
    merged_by_trigger: dict[int, DynamicProtectionRule] = {
        rule.resolved_trigger_r(): rule
        for rule in normalize_dynamic_protection_rules(base_rules)
    }
    for rule in normalize_dynamic_protection_rules(override_rules):
        merged_by_trigger[rule.resolved_trigger_r()] = rule
    return tuple(sorted(merged_by_trigger.values(), key=lambda rule: rule.resolved_trigger_r()))


def build_legacy_dynamic_protection_rules(
    *,
    break_even_enabled: bool,
    break_even_trigger_r: int,
    trailing_start_r: int,
    first_lock_r: int,
    trailing_step_r: int,
) -> tuple[DynamicProtectionRule, ...]:
    rules: list[DynamicProtectionRule] = []
    break_even_trigger = max(int(break_even_trigger_r), 1)
    trailing_start = max(int(trailing_start_r), 2)
    trailing_step = max(int(trailing_step_r), 1)
    configured_first_lock_r = max(int(first_lock_r), 0)
    auto_first_lock_r = max(trailing_start - trailing_step, 0)
    effective_first_lock_r = configured_first_lock_r or auto_first_lock_r

    if not break_even_enabled:
        rules.append(
            DynamicProtectionRule(
                trigger_r=trailing_start,
                action="lock_profit",
                lock_r=effective_first_lock_r,
                trail_mode="step",
                trail_every_r=trailing_step,
                trail_add_r=trailing_step,
            ).normalized()
        )
        return normalize_dynamic_protection_rules(rules)

    # Legacy snapshots used one special path when保本触发R与移动止盈触发R相同，
    # 且首档锁盈R保持自动值：先在该触发点保本，再从下一档开始按 n-stepR 递进。
    if trailing_start == break_even_trigger and configured_first_lock_r <= 0:
        rules.append(
            DynamicProtectionRule(
                trigger_r=break_even_trigger,
                action="break_even",
            ).normalized()
        )
        rules.append(
            DynamicProtectionRule(
                trigger_r=trailing_start + trailing_step,
                action="lock_profit",
                lock_r=effective_first_lock_r + trailing_step,
                trail_mode="step",
                trail_every_r=trailing_step,
                trail_add_r=trailing_step,
            ).normalized()
        )
        return normalize_dynamic_protection_rules(rules)

    # When the first profit-lock starts no later than break-even and already
    # locks positive R, the lock-profit leg dominates the break-even leg.
    if trailing_start <= break_even_trigger:
        rules.append(
            DynamicProtectionRule(
                trigger_r=trailing_start,
                action="lock_profit",
                lock_r=effective_first_lock_r,
                trail_mode="step",
                trail_every_r=trailing_step,
                trail_add_r=trailing_step,
            ).normalized()
        )
        return normalize_dynamic_protection_rules(rules)

    rules.append(
        DynamicProtectionRule(
            trigger_r=break_even_trigger,
            action="break_even",
        ).normalized()
    )
    rules.append(
        DynamicProtectionRule(
            trigger_r=trailing_start,
            action="lock_profit",
            lock_r=effective_first_lock_r,
            trail_mode="step",
            trail_every_r=trailing_step,
            trail_add_r=trailing_step,
        ).normalized()
    )
    return normalize_dynamic_protection_rules(rules)


def describe_dynamic_protection_rules(
    rules: tuple[DynamicProtectionRule, ...] | list[DynamicProtectionRule] | None,
    *,
    fee_offset_enabled: bool,
) -> tuple[str, ...]:
    fee_text = " + 双向手续费" if fee_offset_enabled else ""
    lines: list[str] = []
    for rule in normalize_dynamic_protection_rules(rules):
        trigger_r = rule.resolved_trigger_r()
        if rule.resolved_action() == "break_even":
            lines.append(f"{trigger_r}R -> 保本{fee_text}")
            continue
        base_lock_r = rule.resolved_lock_r()
        if rule.trailing_enabled():
            lines.append(
                f"{trigger_r}R -> 锁 {base_lock_r}R{fee_text}；之后每 {rule.resolved_trail_every_r()}R 再上移 {rule.resolved_trail_add_r()}R"
            )
        else:
            lines.append(f"{trigger_r}R -> 锁 {base_lock_r}R{fee_text}")
    return tuple(lines)


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
    poll_seconds: float = 10.0
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
    reentry_confirmation_enabled: bool = False
    reentry_confirmation_min_sequence: int = 0
    reentry_confirmation_ma_period: int = 21
    reentry_confirmation_ma_type: MovingAverageType = "ema"
    dynamic_two_r_break_even: bool = True
    dynamic_break_even_trigger_r: int = 2
    dynamic_fee_offset_enabled: bool = True
    dynamic_protection_rules: tuple[DynamicProtectionRule, ...] = ()
    ema55_slope_exit_enabled: bool = True
    ema55_slope_lock_profit_enabled: bool = False
    ema55_slope_lock_profit_trigger_r: int = 5
    dynamic_first_lock_r: int = 0
    dynamic_trailing_step_r: int = 1
    ema55_slope_negative_entry_bars: int = 1
    ema55_slope_same_bar_reentry_block: bool = False
    ema55_slope_dynamic_exit_requires_bear_reentry: bool = False
    ema55_slope_dynamic_exit_bear_reentry_break_prev_low: bool = False
    ema55_slope_dynamic_exit_requires_ema_reclaim: bool = False
    ema55_slope_locked_reentry_requires_ema21_near: bool = False
    ema55_slope_locked_reentry_min_r: int = 0
    ema55_slope_locked_reentry_max_r: int = 0
    ema55_slope_dynamic_exit_bull_bar_requires_bear_reentry: bool = False
    ema55_slope_dynamic_exit_bull_bar_reentry_min_r: int = 0
    ema55_slope_dynamic_exit_bull_bar_reentry_max_r: int = 0
    atr_percentile_filter_max: Decimal = Decimal("0")
    trend_ema_slope_filter_enabled: bool = True
    trend_ema_slope_filter_lookback_bars: int = 5
    trend_ema_slope_filter_min_ratio: Decimal = Decimal("0")
    body_retest_breakdown_atr_multiplier: Decimal = Decimal("0.2")
    body_retest_retest_atr_multiplier: Decimal = Decimal("0.3")
    body_retest_stop_buffer_atr_multiplier: Decimal = Decimal("0.3")
    body_retest_body_atr_limit: Decimal = Decimal("1.0")
    body_retest_watch_bars: int = 6
    cross_window_bars: int = 10
    max_pullback_index: int = 1
    exit_mode: str = "fixed_rr"
    rr: Decimal = Decimal("2")
    startup_chase_current_signal: bool = False
    startup_chase_window_seconds: int = 0
    time_stop_break_even_enabled: bool = False
    time_stop_break_even_bars: int = 10
    trend_ema_close_exit_after_trigger_r_enabled: bool = False
    trend_ema_close_exit_after_trigger_r: int = 5
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
    daily_filter_enabled: bool = False
    daily_filter_inst_id: str | None = None
    daily_filter_bar: str | None = None
    daily_filter_boundary: DailyFilterBoundary = "exchange"
    daily_filter_mode: DailyFilterMode = "disabled"
    daily_filter_scope: DailyFilterScope = "both"
    daily_filter_ma_type: MovingAverageType = "ema"
    daily_filter_period: int = 5
    rail_candidate_ema_periods: tuple[int, ...] = (21, 34, 55, 89)
    rail_touch_atr_ratio: Decimal = Decimal("0.2")
    rail_bounce_atr_ratio: Decimal = Decimal("0.6")
    rail_bounce_confirm_bars: int = 3
    rail_break_atr_ratio: Decimal = Decimal("1.0")
    rail_reclaim_bars: int = 2
    rail_score_lookback_bars: int = 60
    rail_switch_min_score_delta: Decimal = Decimal("8")
    rail_min_touches: int = 2
    rail_min_bounces: int = 1
    rail_fast_gate_enabled: bool = True
    rail_fast_gate_period: int = 21
    rail_fast_min_gap_ema200_atr: Decimal = Decimal("5.0")
    rail_fast_min_spread_trend_atr: Decimal = Decimal("1.5")
    rail_fast_max_recent_range_atr: Decimal = Decimal("3.0")
    rail_fast_recent_range_bars: int = 8

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

    def resolved_daily_filter_inst_id(self) -> str:
        return (self.daily_filter_inst_id or self.inst_id).strip()

    def resolved_daily_filter_bar(self) -> str:
        return (self.daily_filter_bar or "1D").strip()

    def uses_daily_filter(self) -> bool:
        return bool(self.daily_filter_enabled and self.daily_filter_mode != "disabled")

    def daily_filter_summary(self) -> str:
        if not self.uses_daily_filter():
            return "日线过滤：关闭"
        boundary_labels = {
            "exchange": "交易所1D",
            "bjt_00": "北京时间0点",
            "bjt_08": "北京时间8点",
        }
        scope_labels = {
            "both": "多空都过滤",
            "long_only": "只过滤多头",
            "short_only": "只过滤空头",
        }
        mode = str(self.daily_filter_mode or "disabled").strip().lower()
        boundary_label = boundary_labels.get(str(self.daily_filter_boundary or "exchange"), str(self.daily_filter_boundary or "exchange"))
        scope_label = scope_labels.get(str(self.daily_filter_scope or "both"), str(self.daily_filter_scope or "both"))
        if mode == "weak_day":
            return f"日线过滤：{boundary_label} 弱日规则 | {scope_label}"
        return (
            f"日线过滤：{boundary_label} {str(self.daily_filter_ma_type or 'ema').upper()}"
            f"{max(int(self.daily_filter_period), 1)} close-vs-MA | {scope_label}"
        )

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

    def uses_reentry_confirmation(self) -> bool:
        return bool(self.reentry_confirmation_enabled and self.reentry_confirmation_min_sequence > 0)

    def resolved_reentry_confirmation_min_sequence(self) -> int:
        return max(int(self.reentry_confirmation_min_sequence), 0)

    def resolved_reentry_confirmation_ma_period(self) -> int:
        return max(int(self.reentry_confirmation_ma_period), 1)

    def resolved_reentry_confirmation_ma_type(self) -> MovingAverageType:
        return normalize_moving_average_type(self.reentry_confirmation_ma_type)

    def reentry_confirmation_line_label(self) -> str:
        return moving_average_display_label(
            self.resolved_reentry_confirmation_ma_type(),
            self.resolved_reentry_confirmation_ma_period(),
        )

    def reentry_confirmation_summary(self) -> str:
        if not self.uses_reentry_confirmation():
            return "再开仓确认：关闭"
        return (
            f"再开仓确认：本波第 {self.resolved_reentry_confirmation_min_sequence()} 次及以后，"
            f"上一根已收K收盘价需站上 {self.reentry_confirmation_line_label()}"
        )

    def dynamic_two_r_break_even_label(self) -> str:
        return "\u5f00\u542f" if self.dynamic_two_r_break_even else "\u5173\u95ed"

    def resolved_dynamic_break_even_trigger_r(self) -> int:
        return max(int(self.dynamic_break_even_trigger_r), 1)

    def resolved_dynamic_trailing_start_r(self) -> int:
        return max(int(self.ema55_slope_lock_profit_trigger_r), 2)

    def resolved_dynamic_first_lock_r(self) -> int:
        return max(int(self.dynamic_first_lock_r), 0)

    def resolved_dynamic_trailing_step_r(self) -> int:
        return max(int(self.dynamic_trailing_step_r), 1)

    def resolved_dynamic_protection_rules(self) -> tuple[DynamicProtectionRule, ...]:
        if self.take_profit_mode != "dynamic":
            return ()
        legacy_rules = build_legacy_dynamic_protection_rules(
            break_even_enabled=bool(self.dynamic_two_r_break_even),
            break_even_trigger_r=self.resolved_dynamic_break_even_trigger_r(),
            trailing_start_r=self.resolved_dynamic_trailing_start_r(),
            first_lock_r=self.resolved_dynamic_first_lock_r(),
            trailing_step_r=self.resolved_dynamic_trailing_step_r(),
        )
        return merge_dynamic_protection_rules(legacy_rules, self.dynamic_protection_rules)

    def dynamic_fee_offset_enabled_label(self) -> str:
        return "\u5f00\u542f" if self.dynamic_fee_offset_enabled else "\u5173\u95ed"

    def resolved_startup_chase_window_seconds(self) -> int:
        return max(int(self.startup_chase_window_seconds), 0)

    def startup_chase_current_signal_label(self) -> str:
        return "\u5f00\u542f" if self.startup_chase_current_signal else "\u5173\u95ed"

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

    def resolved_trend_ema_close_exit_after_trigger_r(self) -> int:
        return max(int(self.trend_ema_close_exit_after_trigger_r), 1)

    def resolved_cross_window_bars(self) -> int:
        return max(int(self.cross_window_bars), 1)

    def resolved_max_pullback_index(self) -> int:
        return max(int(self.max_pullback_index), 1)

    def resolved_fixed_rr(self) -> Decimal:
        return max(Decimal(str(self.rr)), Decimal("0.1"))



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
