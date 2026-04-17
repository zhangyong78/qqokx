from __future__ import annotations

from dataclasses import dataclass, field, replace
from decimal import Decimal
from typing import Literal


StrategyProductType = Literal["SPOT", "SWAP", "OPTION"]
SignalSourceMarket = Literal["SPOT", "SWAP", "OPTION_INDEX"]
DirectionBias = Literal["long", "short", "both", "neutral"]
LabExitMode = Literal["fixed_hold", "tp_sl_handoff"]
LabStopHitMode = Literal["close_loss", "handoff_manual"]
LabLifecycleStatus = Literal[
    "gate_rejected",
    "quota_rejected",
    "fixed_hold_closed",
    "take_profit_closed",
    "stop_loss_closed",
    "timeout_profit_closed",
    "handoff_manual",
    "open_unresolved",
]
PlaybookAction = Literal[
    "SPOT_BUY",
    "SPOT_SELL",
    "SWAP_LONG",
    "SWAP_SHORT",
    "OPTION_SHORT_CALL",
    "OPTION_SHORT_PUT",
    "OPTION_LONG_CALL",
    "OPTION_LONG_PUT",
    "OPTION_ROLL",
    "SPOT_REBALANCE",
]
GateEffect = Literal["allow_open", "deny_open", "warn_only"]
LifecycleStatus = Literal[
    "candidate",
    "submitted",
    "opened_auto",
    "take_profit_closed",
    "handoff_manual",
    "manual_managed",
    "rolled",
    "closed_manual",
    "expired",
    "cancelled",
]
ManualPool = Literal["auto", "manual"]
OptionType = Literal["call", "put"]
TradeSide = Literal["buy", "sell"]
HedgeGroupType = Literal[
    "protected_long",
    "protected_short",
    "covered_call",
    "cash_secured_put",
    "inventory_rotation",
    "custom",
]
QuotaSourceType = Literal[
    "long_direction",
    "short_direction",
    "covered_call",
    "cash_secured_put",
    "spot_inventory",
    "protected_long",
    "protected_short",
]

ZERO = Decimal("0")


def _decimal(value: Decimal | int | str) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _non_negative(value: Decimal | int | str) -> Decimal:
    return max(_decimal(value), ZERO)


def _optional_non_negative(value: Decimal | int | str | None) -> Decimal | None:
    if value is None:
        return None
    return _non_negative(value)


@dataclass(frozen=True)
class ParentStrategyConfig:
    strategy_id: str
    strategy_name: str
    base_bar: str = "5m"
    enabled: bool = True
    notes: str = ""


@dataclass(frozen=True)
class SignalSource:
    market: SignalSourceMarket
    inst_id: str
    bar: str


@dataclass(frozen=True)
class ChildSignalConfig:
    signal_id: str
    signal_name: str
    source: SignalSource
    underlying_family: str
    direction_bias: DirectionBias
    trigger_rule_id: str
    invalidation_rule_id: str
    evidence_template_id: str
    enabled: bool = True
    notes: str = ""


@dataclass(frozen=True)
class GateRuleConfig:
    gate_id: str
    gate_name: str
    underlying_family: str
    condition_expr: str
    effect: GateEffect
    enabled: bool = True
    notes: str = ""


@dataclass(frozen=True)
class ExecutionPlaybookConfig:
    playbook_id: str
    playbook_name: str
    action: PlaybookAction
    underlying_family: str
    sizing_mode: Literal["fixed_size", "fixed_slot"]
    slot_size: Decimal
    lab_hold_bars: int = 1
    option_selector_id: str | None = None
    enabled: bool = True
    notes: str = ""


@dataclass(frozen=True)
class LabSimulationConfig:
    exit_mode: LabExitMode = "fixed_hold"
    fixed_hold_bars: int = 0
    max_hold_bars: int = 24
    stop_loss_pct: Decimal = ZERO
    take_profit_pct: Decimal = ZERO
    fee_rate: Decimal = ZERO
    slippage_rate: Decimal = ZERO
    funding_rate_per_8h: Decimal = ZERO
    stop_hit_mode: LabStopHitMode = "handoff_manual"
    close_on_timeout_if_profitable: bool = True

    def normalized(self) -> "LabSimulationConfig":
        return replace(
            self,
            fixed_hold_bars=max(int(self.fixed_hold_bars), 0),
            max_hold_bars=max(int(self.max_hold_bars), 0),
            stop_loss_pct=_non_negative(self.stop_loss_pct),
            take_profit_pct=_non_negative(self.take_profit_pct),
            fee_rate=_non_negative(self.fee_rate),
            slippage_rate=_non_negative(self.slippage_rate),
            funding_rate_per_8h=_non_negative(self.funding_rate_per_8h),
        )


@dataclass(frozen=True)
class ChildSignalLabProfile:
    signal_id: str
    profile_name: str = ""
    exit_mode: LabExitMode | None = None
    fixed_hold_bars: int | None = None
    max_hold_bars: int | None = None
    stop_loss_pct: Decimal | None = None
    take_profit_pct: Decimal | None = None
    fee_rate: Decimal | None = None
    slippage_rate: Decimal | None = None
    funding_rate_per_8h: Decimal | None = None
    stop_hit_mode: LabStopHitMode | None = None
    close_on_timeout_if_profitable: bool | None = None
    notes: str = ""

    def normalized(self) -> "ChildSignalLabProfile":
        return replace(
            self,
            fixed_hold_bars=None if self.fixed_hold_bars is None else max(int(self.fixed_hold_bars), 0),
            max_hold_bars=None if self.max_hold_bars is None else max(int(self.max_hold_bars), 0),
            stop_loss_pct=_optional_non_negative(self.stop_loss_pct),
            take_profit_pct=_optional_non_negative(self.take_profit_pct),
            fee_rate=_optional_non_negative(self.fee_rate),
            slippage_rate=_optional_non_negative(self.slippage_rate),
            funding_rate_per_8h=_optional_non_negative(self.funding_rate_per_8h),
        )

    def resolve(self, base: LabSimulationConfig) -> LabSimulationConfig:
        normalized = self.normalized()
        return replace(
            base,
            exit_mode=base.exit_mode if normalized.exit_mode is None else normalized.exit_mode,
            fixed_hold_bars=base.fixed_hold_bars if normalized.fixed_hold_bars is None else normalized.fixed_hold_bars,
            max_hold_bars=base.max_hold_bars if normalized.max_hold_bars is None else normalized.max_hold_bars,
            stop_loss_pct=base.stop_loss_pct if normalized.stop_loss_pct is None else normalized.stop_loss_pct,
            take_profit_pct=base.take_profit_pct if normalized.take_profit_pct is None else normalized.take_profit_pct,
            fee_rate=base.fee_rate if normalized.fee_rate is None else normalized.fee_rate,
            slippage_rate=base.slippage_rate if normalized.slippage_rate is None else normalized.slippage_rate,
            funding_rate_per_8h=(
                base.funding_rate_per_8h
                if normalized.funding_rate_per_8h is None
                else normalized.funding_rate_per_8h
            ),
            stop_hit_mode=base.stop_hit_mode if normalized.stop_hit_mode is None else normalized.stop_hit_mode,
            close_on_timeout_if_profitable=(
                base.close_on_timeout_if_profitable
                if normalized.close_on_timeout_if_profitable is None
                else normalized.close_on_timeout_if_profitable
            ),
        ).normalized()


@dataclass(frozen=True)
class QuotaSnapshot:
    underlying_family: str
    long_limit_total: Decimal = ZERO
    long_limit_used: Decimal = ZERO
    short_limit_total: Decimal = ZERO
    short_limit_used: Decimal = ZERO
    manual_extra_long_limit: Decimal = ZERO
    manual_extra_short_limit: Decimal = ZERO
    spot_inventory_total: Decimal = ZERO
    spot_inventory_reserved: Decimal = ZERO
    covered_call_quota_total: Decimal = ZERO
    covered_call_quota_used: Decimal = ZERO
    cash_secured_put_quota_total: Decimal = ZERO
    cash_secured_put_quota_used: Decimal = ZERO
    protected_long_quota_total: Decimal = ZERO
    protected_long_quota_used: Decimal = ZERO
    protected_short_quota_total: Decimal = ZERO
    protected_short_quota_used: Decimal = ZERO

    def normalized(self) -> "QuotaSnapshot":
        return replace(
            self,
            long_limit_total=_non_negative(self.long_limit_total),
            long_limit_used=_non_negative(self.long_limit_used),
            short_limit_total=_non_negative(self.short_limit_total),
            short_limit_used=_non_negative(self.short_limit_used),
            manual_extra_long_limit=_non_negative(self.manual_extra_long_limit),
            manual_extra_short_limit=_non_negative(self.manual_extra_short_limit),
            spot_inventory_total=_non_negative(self.spot_inventory_total),
            spot_inventory_reserved=_non_negative(self.spot_inventory_reserved),
            covered_call_quota_total=_non_negative(self.covered_call_quota_total),
            covered_call_quota_used=_non_negative(self.covered_call_quota_used),
            cash_secured_put_quota_total=_non_negative(self.cash_secured_put_quota_total),
            cash_secured_put_quota_used=_non_negative(self.cash_secured_put_quota_used),
            protected_long_quota_total=_non_negative(self.protected_long_quota_total),
            protected_long_quota_used=_non_negative(self.protected_long_quota_used),
            protected_short_quota_total=_non_negative(self.protected_short_quota_total),
            protected_short_quota_used=_non_negative(self.protected_short_quota_used),
        )

    @property
    def effective_long_limit_total(self) -> Decimal:
        return _non_negative(self.long_limit_total + self.manual_extra_long_limit)

    @property
    def effective_short_limit_total(self) -> Decimal:
        return _non_negative(self.short_limit_total + self.manual_extra_short_limit)

    @property
    def available_long_direction(self) -> Decimal:
        return max(self.effective_long_limit_total - _non_negative(self.long_limit_used), ZERO)

    @property
    def available_short_direction(self) -> Decimal:
        return max(self.effective_short_limit_total - _non_negative(self.short_limit_used), ZERO)

    @property
    def available_spot_inventory(self) -> Decimal:
        return max(_non_negative(self.spot_inventory_total) - _non_negative(self.spot_inventory_reserved), ZERO)

    @property
    def available_covered_call(self) -> Decimal:
        return max(_non_negative(self.covered_call_quota_total) - _non_negative(self.covered_call_quota_used), ZERO)

    @property
    def available_cash_secured_put(self) -> Decimal:
        return max(
            _non_negative(self.cash_secured_put_quota_total) - _non_negative(self.cash_secured_put_quota_used),
            ZERO,
        )

    @property
    def available_protected_long(self) -> Decimal:
        return max(
            _non_negative(self.protected_long_quota_total) - _non_negative(self.protected_long_quota_used),
            ZERO,
        )

    @property
    def available_protected_short(self) -> Decimal:
        return max(
            _non_negative(self.protected_short_quota_total) - _non_negative(self.protected_short_quota_used),
            ZERO,
        )


@dataclass(frozen=True)
class QuotaAllocation:
    source_type: QuotaSourceType
    quantity: Decimal


@dataclass(frozen=True)
class QuotaRequest:
    underlying_family: str
    action: PlaybookAction
    quantity: Decimal
    request_id: str = ""
    required_long_direction_qty: Decimal = ZERO
    required_short_direction_qty: Decimal = ZERO
    required_spot_inventory_qty: Decimal = ZERO
    required_covered_call_qty: Decimal = ZERO
    required_cash_secured_put_qty: Decimal = ZERO
    preferred_protected_long_qty: Decimal = ZERO
    minimum_protected_long_qty: Decimal = ZERO
    preferred_protected_short_qty: Decimal = ZERO
    minimum_protected_short_qty: Decimal = ZERO

    @classmethod
    def from_action(
        cls,
        *,
        underlying_family: str,
        action: PlaybookAction,
        quantity: Decimal,
        request_id: str = "",
        require_full_protection: bool = False,
    ) -> "QuotaRequest":
        base = {
            "underlying_family": underlying_family,
            "action": action,
            "quantity": _non_negative(quantity),
            "request_id": request_id,
        }
        if action == "SPOT_BUY":
            return cls(required_long_direction_qty=_non_negative(quantity), **base)
        if action == "SPOT_SELL":
            return cls(required_spot_inventory_qty=_non_negative(quantity), **base)
        if action == "SWAP_LONG":
            preferred = _non_negative(quantity)
            minimum = preferred if require_full_protection else ZERO
            return cls(
                required_long_direction_qty=_non_negative(quantity),
                preferred_protected_long_qty=preferred,
                minimum_protected_long_qty=minimum,
                **base,
            )
        if action == "SWAP_SHORT":
            preferred = _non_negative(quantity)
            minimum = preferred if require_full_protection else ZERO
            return cls(
                required_short_direction_qty=_non_negative(quantity),
                preferred_protected_short_qty=preferred,
                minimum_protected_short_qty=minimum,
                **base,
            )
        if action == "OPTION_SHORT_CALL":
            return cls(required_covered_call_qty=_non_negative(quantity), **base)
        if action == "OPTION_SHORT_PUT":
            return cls(required_cash_secured_put_qty=_non_negative(quantity), **base)
        return cls(**base)

    def normalized(self) -> "QuotaRequest":
        return replace(
            self,
            quantity=_non_negative(self.quantity),
            required_long_direction_qty=_non_negative(self.required_long_direction_qty),
            required_short_direction_qty=_non_negative(self.required_short_direction_qty),
            required_spot_inventory_qty=_non_negative(self.required_spot_inventory_qty),
            required_covered_call_qty=_non_negative(self.required_covered_call_qty),
            required_cash_secured_put_qty=_non_negative(self.required_cash_secured_put_qty),
            preferred_protected_long_qty=_non_negative(self.preferred_protected_long_qty),
            minimum_protected_long_qty=_non_negative(self.minimum_protected_long_qty),
            preferred_protected_short_qty=_non_negative(self.preferred_protected_short_qty),
            minimum_protected_short_qty=_non_negative(self.minimum_protected_short_qty),
        )


@dataclass(frozen=True)
class QuotaDecision:
    allowed: bool
    request: QuotaRequest
    reason: str
    updated_snapshot: QuotaSnapshot
    allocations: tuple[QuotaAllocation, ...] = ()
    errors: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    protected_long_applied: Decimal = ZERO
    protected_short_applied: Decimal = ZERO
    unprotected_long_quantity: Decimal = ZERO
    unprotected_short_quantity: Decimal = ZERO


@dataclass(frozen=True)
class StrategyOrderContext:
    parent_strategy_name: str
    child_signal_name: str
    execution_playbook_name: str
    entry_reason: str
    invalidation_reason: str
    hedge_group_id: str | None = None
    roll_chain_id: str | None = None
    evidence_snapshot_path: str | None = None


@dataclass(frozen=True)
class SignalRuleMatch:
    triggered: bool
    reason: str
    signal_price: Decimal | None = None


@dataclass(frozen=True)
class SignalEvent:
    signal_id: str
    signal_name: str
    underlying_family: str
    source_market: SignalSourceMarket
    source_inst_id: str
    source_bar: str
    candle_index: int
    candle_ts: int
    signal_price: Decimal
    bar_step: int
    direction_bias: DirectionBias
    reason: str


@dataclass(frozen=True)
class EvidenceCandle:
    ts: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal = ZERO


@dataclass(frozen=True)
class GateCheckResult:
    gate_id: str
    gate_name: str
    effect: GateEffect
    matched: bool
    allowed: bool
    reason: str


@dataclass(frozen=True)
class LabEventRecord:
    signal_id: str
    signal_name: str
    playbook_id: str
    playbook_name: str
    evidence_id: str
    evidence_template_id: str
    underlying_family: str
    candle_ts: int
    signal_price: Decimal
    position_size: Decimal
    accepted: bool
    trigger_reason: str
    gate_reason: str
    evidence_summary: str = ""
    gate_checks: tuple[GateCheckResult, ...] = ()
    quota_reason: str = ""
    quota_errors: tuple[str, ...] = ()
    quota_warnings: tuple[str, ...] = ()
    quota_allocations: tuple[tuple[str, Decimal], ...] = ()
    lifecycle_status: LabLifecycleStatus = "open_unresolved"
    entry_price: Decimal | None = None
    stop_loss_price: Decimal | None = None
    take_profit_price: Decimal | None = None
    holding_bars: int = 0
    exit_ts: int | None = None
    exit_price: Decimal | None = None
    exit_reason: str = ""
    gross_pnl_value: Decimal | None = None
    fee_cost_value: Decimal | None = None
    slippage_cost_value: Decimal | None = None
    funding_cost_value: Decimal | None = None
    realized_pnl_points: Decimal | None = None
    realized_pnl_pct: Decimal | None = None
    realized_pnl_value: Decimal | None = None


@dataclass(frozen=True)
class LabEntryEvidenceRecord:
    evidence_id: str
    signal_id: str
    signal_name: str
    playbook_id: str
    playbook_name: str
    evidence_template_id: str
    underlying_family: str
    source_market: SignalSourceMarket
    source_inst_id: str
    source_bar: str
    direction_bias: DirectionBias
    candle_ts: int
    signal_price: Decimal
    trigger_reason: str
    evidence_summary: str
    setup_candles: tuple[EvidenceCandle, ...] = ()
    trigger_candle: EvidenceCandle | None = None
    followthrough_candles: tuple[EvidenceCandle, ...] = ()
    note: str = ""


@dataclass(frozen=True)
class ChildSignalLabSummary:
    signal_id: str
    signal_name: str
    underlying_family: str
    total_signal_events: int
    routed_events: int
    accepted_events: int
    rejected_events: int
    lab_profile_name: str = ""
    applied_simulation_config: LabSimulationConfig = field(default_factory=LabSimulationConfig)
    gate_rejected_events: int = 0
    quota_rejected_events: int = 0
    realized_outcomes: int = 0
    take_profit_closed_events: int = 0
    stop_loss_closed_events: int = 0
    timeout_profit_closed_events: int = 0
    manual_handoff_events: int = 0
    unresolved_events: int = 0
    win_events: int = 0
    loss_events: int = 0
    breakeven_events: int = 0
    win_rate_pct: Decimal = ZERO
    total_pnl_value: Decimal = ZERO
    average_pnl_value: Decimal = ZERO
    gross_profit_value: Decimal = ZERO
    gross_loss_value: Decimal = ZERO
    profit_factor: Decimal | None = None
    average_pnl_pct: Decimal = ZERO
    average_win_pct: Decimal = ZERO
    average_loss_pct: Decimal = ZERO
    profit_loss_ratio: Decimal | None = None
    playbook_accept_counts: tuple[tuple[str, int], ...] = ()
    rejection_reasons: tuple[tuple[str, int], ...] = ()


@dataclass(frozen=True)
class EnhancedBacktestLabResult:
    parent_strategy_id: str
    parent_strategy_name: str
    simulation_config: LabSimulationConfig
    summaries: tuple[ChildSignalLabSummary, ...]
    events: tuple[LabEventRecord, ...]
    evidences: tuple[LabEntryEvidenceRecord, ...] = ()
    ending_quota_snapshots: tuple[QuotaSnapshot, ...] = ()


@dataclass(frozen=True)
class SpotLedgerEntry:
    entry_id: str
    underlying_family: str
    inst_id: str
    quantity: Decimal
    avg_cost: Decimal
    fee_total: Decimal = ZERO
    covered_call_eligible_quantity: Decimal | None = None
    covered_call_committed_quantity: Decimal = ZERO
    sell_reserved_quantity: Decimal = ZERO
    protected_quantity: Decimal = ZERO
    counts_toward_long_limit: bool = False
    status: LifecycleStatus = "opened_auto"

    @property
    def is_active(self) -> bool:
        return self.status not in {"closed_manual", "expired", "cancelled"}

    @property
    def covered_call_capacity(self) -> Decimal:
        base = self.covered_call_eligible_quantity
        if base is None:
            base = self.quantity
        return max(_non_negative(base) - _non_negative(self.sell_reserved_quantity), ZERO)

    @property
    def available_for_covered_call(self) -> Decimal:
        return max(self.covered_call_capacity - _non_negative(self.covered_call_committed_quantity), ZERO)


@dataclass(frozen=True)
class SwapLedgerEntry:
    position_id: str
    underlying_family: str
    inst_id: str
    direction: Literal["long", "short"]
    quantity: Decimal
    avg_entry_price: Decimal
    break_even_price: Decimal
    fee_total: Decimal = ZERO
    funding_fee_total: Decimal = ZERO
    protected_quantity: Decimal = ZERO
    pool: ManualPool = "auto"
    status: LifecycleStatus = "opened_auto"
    hedge_group_id: str | None = None
    roll_chain_id: str | None = None

    @property
    def is_active(self) -> bool:
        return self.status not in {"take_profit_closed", "closed_manual", "expired", "cancelled"}

    @property
    def unprotected_quantity(self) -> Decimal:
        return max(_non_negative(self.quantity) - _non_negative(self.protected_quantity), ZERO)


@dataclass(frozen=True)
class OptionLedgerEntry:
    position_id: str
    underlying_family: str
    inst_id: str
    option_type: OptionType
    side: TradeSide
    quantity: Decimal
    expiry_code: str
    strike: Decimal
    premium_cashflow: Decimal
    fee_total: Decimal = ZERO
    mark_price: Decimal | None = None
    time_value: Decimal | None = None
    delta: Decimal | None = None
    theta: Decimal | None = None
    vega: Decimal | None = None
    covered_call_quantity: Decimal = ZERO
    cash_secured_put_quantity: Decimal = ZERO
    protected_long_capacity: Decimal = ZERO
    protected_short_capacity: Decimal = ZERO
    pool: ManualPool = "auto"
    status: LifecycleStatus = "opened_auto"
    hedge_group_id: str | None = None
    roll_chain_id: str | None = None

    @property
    def is_active(self) -> bool:
        return self.status not in {"take_profit_closed", "closed_manual", "expired", "cancelled"}


@dataclass(frozen=True)
class HedgeGroupEntry:
    hedge_group_id: str
    underlying_family: str
    group_type: HedgeGroupType
    status: LifecycleStatus = "opened_auto"
    note: str = ""
    spot_leg_ids: tuple[str, ...] = ()
    swap_leg_ids: tuple[str, ...] = ()
    option_leg_ids: tuple[str, ...] = ()
    protected_long_capacity: Decimal = ZERO
    protected_short_capacity: Decimal = ZERO

    @property
    def is_active(self) -> bool:
        return self.status not in {"closed_manual", "expired", "cancelled"}


@dataclass(frozen=True)
class FamilyLedgerSnapshot:
    underlying_family: str
    spot_entries: tuple[SpotLedgerEntry, ...] = field(default_factory=tuple)
    swap_entries: tuple[SwapLedgerEntry, ...] = field(default_factory=tuple)
    option_entries: tuple[OptionLedgerEntry, ...] = field(default_factory=tuple)
    hedge_groups: tuple[HedgeGroupEntry, ...] = field(default_factory=tuple)
    quota_snapshot: QuotaSnapshot | None = None

    @property
    def auto_swap_count(self) -> int:
        return sum(1 for item in self.swap_entries if item.pool == "auto")

    @property
    def manual_swap_count(self) -> int:
        return sum(1 for item in self.swap_entries if item.pool == "manual")

    @property
    def auto_option_count(self) -> int:
        return sum(1 for item in self.option_entries if item.pool == "auto")

    @property
    def manual_option_count(self) -> int:
        return sum(1 for item in self.option_entries if item.pool == "manual")
