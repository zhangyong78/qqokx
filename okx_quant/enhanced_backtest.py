from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from okx_quant.enhanced_gate_engine import EnhancedGateEngine
from okx_quant.enhanced_models import (
    ChildSignalConfig,
    ChildSignalLabSummary,
    EvidenceCandle,
    EnhancedBacktestLabResult,
    ExecutionPlaybookConfig,
    GateCheckResult,
    LabEntryEvidenceRecord,
    LabEventRecord,
    LabLifecycleStatus,
    LabSimulationConfig,
    QuotaAllocation,
    QuotaRequest,
    QuotaSnapshot,
    SignalEvent,
)
from okx_quant.enhanced_quota_engine import QuotaEngine
from okx_quant.enhanced_registry import EnhancedStrategyRegistry
from okx_quant.enhanced_signal_engine import EnhancedSignalEngine
from okx_quant.models import Candle


ZERO = Decimal("0")
EIGHT_HOURS_MS = Decimal("28800000")
LONG_ACTIONS = {"SPOT_BUY", "SWAP_LONG", "OPTION_LONG_CALL", "OPTION_LONG_PUT"}
SHORT_ACTIONS = {"SPOT_SELL", "SWAP_SHORT", "OPTION_SHORT_CALL", "OPTION_SHORT_PUT"}


@dataclass(frozen=True)
class _PendingRoute:
    child_signal: ChildSignalConfig
    signal_event: SignalEvent
    playbook: ExecutionPlaybookConfig
    source_candles: tuple[Candle, ...]


@dataclass(frozen=True)
class _ActiveReservation:
    release_ts: int
    allocations: tuple[QuotaAllocation, ...]


@dataclass(frozen=True)
class _SimulatedOutcome:
    lifecycle_status: LabLifecycleStatus
    holding_bars: int
    entry_price: Decimal | None = None
    stop_loss_price: Decimal | None = None
    take_profit_price: Decimal | None = None
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
    release_ts: int | None = None
    should_release_quota: bool = False


class EnhancedBacktestLab:
    def __init__(
        self,
        registry: EnhancedStrategyRegistry,
        *,
        signal_engine: EnhancedSignalEngine | None = None,
        gate_engine: EnhancedGateEngine | None = None,
        quota_engine: QuotaEngine | None = None,
    ) -> None:
        self._registry = registry
        self._signal_engine = signal_engine or EnhancedSignalEngine()
        self._gate_engine = gate_engine or EnhancedGateEngine()
        self._quota_engine = quota_engine or QuotaEngine()

    def run(
        self,
        *,
        parent_strategy_id: str,
        candle_feeds: dict[tuple[str, str], list[Candle]],
        base_bar: str = "5m",
        quota_snapshots: dict[str, QuotaSnapshot] | None = None,
        extra_gate_variables: dict[str, dict[str, object]] | None = None,
        simulation_config: LabSimulationConfig | None = None,
    ) -> EnhancedBacktestLabResult:
        parent = self._registry.get_parent_strategy(parent_strategy_id)
        simulation = (simulation_config or LabSimulationConfig()).normalized()
        quota_by_family = {
            family: snapshot.normalized()
            for family, snapshot in (quota_snapshots or {}).items()
        }
        extra_by_family = extra_gate_variables or {}
        pending_routes = self._build_pending_routes(
            parent_strategy_id=parent_strategy_id,
            candle_feeds=candle_feeds,
            base_bar=base_bar,
        )
        pending_routes.sort(
            key=lambda item: (
                item.signal_event.candle_ts,
                item.child_signal.signal_id,
                item.playbook.playbook_id,
            )
        )

        active_reservations: dict[str, list[_ActiveReservation]] = {}
        playbook_accept_counts_by_signal: dict[str, Counter[str]] = {}
        rejection_reasons_by_signal: dict[str, Counter[str]] = {}
        accepted_counts_by_signal: Counter[str] = Counter()
        routed_counts_by_signal: Counter[str] = Counter()
        gate_rejected_by_signal: Counter[str] = Counter()
        quota_rejected_by_signal: Counter[str] = Counter()
        realized_counts_by_signal: Counter[str] = Counter()
        take_profit_counts_by_signal: Counter[str] = Counter()
        stop_loss_counts_by_signal: Counter[str] = Counter()
        timeout_profit_counts_by_signal: Counter[str] = Counter()
        manual_handoff_counts_by_signal: Counter[str] = Counter()
        unresolved_counts_by_signal: Counter[str] = Counter()
        win_counts_by_signal: Counter[str] = Counter()
        loss_counts_by_signal: Counter[str] = Counter()
        breakeven_counts_by_signal: Counter[str] = Counter()
        pnl_pct_sums_by_signal: dict[str, Decimal] = {}
        pnl_value_sums_by_signal: dict[str, Decimal] = {}
        win_pct_sums_by_signal: dict[str, Decimal] = {}
        loss_pct_sums_by_signal: dict[str, Decimal] = {}
        gross_profit_value_by_signal: dict[str, Decimal] = {}
        gross_loss_value_by_signal: dict[str, Decimal] = {}
        total_signal_events_by_signal: Counter[str] = Counter()
        resolved_simulation_by_signal: dict[str, LabSimulationConfig] = {}
        profile_name_by_signal: dict[str, str] = {}

        for child_signal in self._registry.list_child_signals(parent_strategy_id):
            source_key = (child_signal.source.market, child_signal.source.inst_id)
            base_candles = candle_feeds.get(source_key)
            if not base_candles:
                raise KeyError(f"missing candle feed for {source_key}")
            signal_profile = self._registry.get_signal_lab_profile(child_signal.signal_id)
            resolved_simulation_by_signal[child_signal.signal_id] = (
                simulation if signal_profile is None else signal_profile.resolve(simulation)
            )
            profile_name_by_signal[child_signal.signal_id] = "" if signal_profile is None else signal_profile.profile_name
            source_candles = self._signal_engine.aggregate_candles(
                base_candles,
                base_bar=base_bar,
                target_bar=child_signal.source.bar,
            )
            trigger_rule = self._registry.get_trigger_rule(child_signal.trigger_rule_id)
            invalidation_rule = self._registry.get_invalidation_rule(child_signal.invalidation_rule_id)
            total_signal_events_by_signal[child_signal.signal_id] = len(
                self._signal_engine.evaluate_signal(
                    child_signal,
                    source_candles,
                    trigger_rule=trigger_rule,
                    invalidation_rule=invalidation_rule,
                )
            )

        event_records: list[LabEventRecord] = []
        evidence_records: list[LabEntryEvidenceRecord] = []
        for route in pending_routes:
            signal_id = route.child_signal.signal_id
            route_simulation = resolved_simulation_by_signal.get(signal_id, simulation)
            evidence_summary = _build_evidence_summary(route)
            evidence_id = _build_evidence_id(route)
            evidence_records.append(
                _build_entry_evidence_record(
                    route,
                    evidence_id=evidence_id,
                    evidence_summary=evidence_summary,
                )
            )
            routed_counts_by_signal[signal_id] += 1
            family = route.playbook.underlying_family
            current_snapshot = quota_by_family.get(family, QuotaSnapshot(underlying_family=family))
            current_snapshot = self._release_due_reservations(
                current_snapshot,
                active_reservations.setdefault(family, []),
                route.signal_event.candle_ts,
            )
            quota_by_family[family] = current_snapshot

            gate_rules = [
                rule
                for rule in self._registry.list_gate_rules_for_signal(signal_id)
                if rule.underlying_family in {"", "*", family, route.child_signal.underlying_family}
            ]
            variables = self._gate_engine.build_gate_variables(
                quota_snapshot=current_snapshot,
                price=route.signal_event.signal_price,
                extra={
                    "playbook_action": route.playbook.action,
                    "signal_direction_bias": route.signal_event.direction_bias,
                    "signal_ts": route.signal_event.candle_ts,
                    **extra_by_family.get(route.child_signal.underlying_family, {}),
                    **extra_by_family.get(family, {}),
                },
            )
            allowed_by_gates, gate_reason, gate_checks = self._gate_engine.evaluate_gates(gate_rules, variables)
            if not allowed_by_gates:
                rejection_reasons_by_signal.setdefault(signal_id, Counter())[gate_reason] += 1
                gate_rejected_by_signal[signal_id] += 1
                event_records.append(
                    LabEventRecord(
                        signal_id=signal_id,
                        signal_name=route.child_signal.signal_name,
                        playbook_id=route.playbook.playbook_id,
                        playbook_name=route.playbook.playbook_name,
                        evidence_id=evidence_id,
                        evidence_template_id=route.child_signal.evidence_template_id,
                        underlying_family=family,
                        candle_ts=route.signal_event.candle_ts,
                        signal_price=route.signal_event.signal_price,
                        position_size=route.playbook.slot_size,
                        accepted=False,
                        trigger_reason=route.signal_event.reason,
                        evidence_summary=evidence_summary,
                        gate_reason=gate_reason,
                        gate_checks=gate_checks,
                        quota_reason="skipped_due_to_gate_reject",
                        lifecycle_status="gate_rejected",
                    )
                )
                continue

            quota_request = QuotaRequest.from_action(
                underlying_family=family,
                action=route.playbook.action,
                quantity=route.playbook.slot_size,
            )
            quota_decision = self._quota_engine.evaluate(current_snapshot, quota_request)
            quota_by_family[family] = quota_decision.updated_snapshot
            allocation_summary = tuple((item.source_type, item.quantity) for item in quota_decision.allocations)
            outcome = _SimulatedOutcome(lifecycle_status="open_unresolved", holding_bars=0)

            if quota_decision.allowed:
                accepted_counts_by_signal[signal_id] += 1
                playbook_accept_counts_by_signal.setdefault(signal_id, Counter())[route.playbook.playbook_name] += 1
                outcome = _simulate_event_outcome(
                    route.source_candles,
                    route.signal_event,
                    route.playbook,
                    route_simulation,
                )
                if outcome.realized_pnl_value is not None:
                    realized_counts_by_signal[signal_id] += 1
                    pnl_value_sums_by_signal[signal_id] = (
                        pnl_value_sums_by_signal.get(signal_id, ZERO) + outcome.realized_pnl_value
                    )
                    pnl_pct_sums_by_signal[signal_id] = (
                        pnl_pct_sums_by_signal.get(signal_id, ZERO) + (outcome.realized_pnl_pct or ZERO)
                    )
                    if outcome.realized_pnl_value > 0:
                        win_counts_by_signal[signal_id] += 1
                        win_pct_sums_by_signal[signal_id] = (
                            win_pct_sums_by_signal.get(signal_id, ZERO) + (outcome.realized_pnl_pct or ZERO)
                        )
                        gross_profit_value_by_signal[signal_id] = (
                            gross_profit_value_by_signal.get(signal_id, ZERO) + outcome.realized_pnl_value
                        )
                    elif outcome.realized_pnl_value < 0:
                        loss_counts_by_signal[signal_id] += 1
                        loss_pct_sums_by_signal[signal_id] = (
                            loss_pct_sums_by_signal.get(signal_id, ZERO) + abs(outcome.realized_pnl_pct or ZERO)
                        )
                        gross_loss_value_by_signal[signal_id] = (
                            gross_loss_value_by_signal.get(signal_id, ZERO) + abs(outcome.realized_pnl_value)
                        )
                    else:
                        breakeven_counts_by_signal[signal_id] += 1
                if outcome.lifecycle_status == "take_profit_closed":
                    take_profit_counts_by_signal[signal_id] += 1
                elif outcome.lifecycle_status == "stop_loss_closed":
                    stop_loss_counts_by_signal[signal_id] += 1
                elif outcome.lifecycle_status == "timeout_profit_closed":
                    timeout_profit_counts_by_signal[signal_id] += 1
                elif outcome.lifecycle_status == "handoff_manual":
                    manual_handoff_counts_by_signal[signal_id] += 1
                elif outcome.lifecycle_status == "open_unresolved":
                    unresolved_counts_by_signal[signal_id] += 1
                if outcome.should_release_quota and outcome.release_ts is not None and quota_decision.allocations:
                    active_reservations.setdefault(family, []).append(
                        _ActiveReservation(
                            release_ts=outcome.release_ts,
                            allocations=quota_decision.allocations,
                        )
                    )
            else:
                rejection_text = "; ".join(quota_decision.errors) if quota_decision.errors else quota_decision.reason
                rejection_reasons_by_signal.setdefault(signal_id, Counter())[rejection_text] += 1
                quota_rejected_by_signal[signal_id] += 1
                outcome = _SimulatedOutcome(lifecycle_status="quota_rejected", holding_bars=0)

            event_records.append(
                LabEventRecord(
                    signal_id=signal_id,
                    signal_name=route.child_signal.signal_name,
                    playbook_id=route.playbook.playbook_id,
                    playbook_name=route.playbook.playbook_name,
                    evidence_id=evidence_id,
                    evidence_template_id=route.child_signal.evidence_template_id,
                    underlying_family=family,
                    candle_ts=route.signal_event.candle_ts,
                    signal_price=route.signal_event.signal_price,
                    position_size=route.playbook.slot_size,
                    accepted=quota_decision.allowed,
                    trigger_reason=route.signal_event.reason,
                    evidence_summary=evidence_summary,
                    gate_reason=gate_reason,
                    gate_checks=gate_checks,
                    quota_reason=quota_decision.reason,
                    quota_errors=quota_decision.errors,
                    quota_warnings=quota_decision.warnings,
                    quota_allocations=allocation_summary,
                    lifecycle_status=outcome.lifecycle_status,
                    entry_price=outcome.entry_price,
                    stop_loss_price=outcome.stop_loss_price,
                    take_profit_price=outcome.take_profit_price,
                    holding_bars=outcome.holding_bars,
                    exit_ts=outcome.exit_ts,
                    exit_price=outcome.exit_price,
                    exit_reason=outcome.exit_reason,
                    gross_pnl_value=outcome.gross_pnl_value,
                    fee_cost_value=outcome.fee_cost_value,
                    slippage_cost_value=outcome.slippage_cost_value,
                    funding_cost_value=outcome.funding_cost_value,
                    realized_pnl_points=outcome.realized_pnl_points,
                    realized_pnl_pct=outcome.realized_pnl_pct,
                    realized_pnl_value=outcome.realized_pnl_value,
                )
            )

        summaries: list[ChildSignalLabSummary] = []
        for child_signal in self._registry.list_child_signals(parent_strategy_id):
            signal_id = child_signal.signal_id
            accepted_events = accepted_counts_by_signal[signal_id]
            routed_events = routed_counts_by_signal[signal_id]
            realized_outcomes = realized_counts_by_signal[signal_id]
            win_events = win_counts_by_signal[signal_id]
            loss_events = loss_counts_by_signal[signal_id]
            breakeven_events = breakeven_counts_by_signal[signal_id]
            total_pnl_value = pnl_value_sums_by_signal.get(signal_id, ZERO)
            average_pnl_value = total_pnl_value / Decimal(realized_outcomes) if realized_outcomes > 0 else ZERO
            average_pnl_pct = (
                pnl_pct_sums_by_signal.get(signal_id, ZERO) / Decimal(realized_outcomes)
                if realized_outcomes > 0
                else ZERO
            )
            average_win_pct = (
                win_pct_sums_by_signal.get(signal_id, ZERO) / Decimal(win_events)
                if win_events > 0
                else ZERO
            )
            average_loss_pct = (
                loss_pct_sums_by_signal.get(signal_id, ZERO) / Decimal(loss_events)
                if loss_events > 0
                else ZERO
            )
            gross_profit_value = gross_profit_value_by_signal.get(signal_id, ZERO)
            gross_loss_value = gross_loss_value_by_signal.get(signal_id, ZERO)
            profit_factor = None if gross_loss_value == 0 else gross_profit_value / gross_loss_value
            profit_loss_ratio = None if average_loss_pct == 0 else average_win_pct / average_loss_pct
            win_rate_pct = (
                (Decimal(win_events) / Decimal(realized_outcomes)) * Decimal("100")
                if realized_outcomes > 0
                else ZERO
            )
            summaries.append(
                ChildSignalLabSummary(
                    signal_id=signal_id,
                    signal_name=child_signal.signal_name,
                    underlying_family=child_signal.underlying_family,
                    total_signal_events=total_signal_events_by_signal[signal_id],
                    routed_events=routed_events,
                    accepted_events=accepted_events,
                    rejected_events=max(routed_events - accepted_events, 0),
                    lab_profile_name=profile_name_by_signal.get(signal_id, ""),
                    applied_simulation_config=resolved_simulation_by_signal.get(signal_id, simulation),
                    gate_rejected_events=gate_rejected_by_signal[signal_id],
                    quota_rejected_events=quota_rejected_by_signal[signal_id],
                    realized_outcomes=realized_outcomes,
                    take_profit_closed_events=take_profit_counts_by_signal[signal_id],
                    stop_loss_closed_events=stop_loss_counts_by_signal[signal_id],
                    timeout_profit_closed_events=timeout_profit_counts_by_signal[signal_id],
                    manual_handoff_events=manual_handoff_counts_by_signal[signal_id],
                    unresolved_events=unresolved_counts_by_signal[signal_id],
                    win_events=win_events,
                    loss_events=loss_events,
                    breakeven_events=breakeven_events,
                    win_rate_pct=win_rate_pct,
                    total_pnl_value=total_pnl_value,
                    average_pnl_value=average_pnl_value,
                    gross_profit_value=gross_profit_value,
                    gross_loss_value=gross_loss_value,
                    profit_factor=profit_factor,
                    average_pnl_pct=average_pnl_pct,
                    average_win_pct=average_win_pct,
                    average_loss_pct=average_loss_pct,
                    profit_loss_ratio=profit_loss_ratio,
                    playbook_accept_counts=tuple(
                        sorted(playbook_accept_counts_by_signal.get(signal_id, Counter()).items())
                    ),
                    rejection_reasons=tuple(sorted(rejection_reasons_by_signal.get(signal_id, Counter()).items())),
                )
            )

        return EnhancedBacktestLabResult(
            parent_strategy_id=parent.strategy_id,
            parent_strategy_name=parent.strategy_name,
            simulation_config=simulation,
            summaries=tuple(summaries),
            events=tuple(event_records),
            evidences=tuple(evidence_records),
            ending_quota_snapshots=tuple(
                snapshot
                for _, snapshot in sorted(quota_by_family.items(), key=lambda item: item[0])
            ),
        )

    def _build_pending_routes(
        self,
        *,
        parent_strategy_id: str,
        candle_feeds: dict[tuple[str, str], list[Candle]],
        base_bar: str,
    ) -> list[_PendingRoute]:
        pending_routes: list[_PendingRoute] = []
        for child_signal in self._registry.list_child_signals(parent_strategy_id):
            source_key = (child_signal.source.market, child_signal.source.inst_id)
            base_candles = candle_feeds.get(source_key)
            if not base_candles:
                raise KeyError(f"missing candle feed for {source_key}")
            source_candles = self._signal_engine.aggregate_candles(
                base_candles,
                base_bar=base_bar,
                target_bar=child_signal.source.bar,
            )
            trigger_rule = self._registry.get_trigger_rule(child_signal.trigger_rule_id)
            invalidation_rule = self._registry.get_invalidation_rule(child_signal.invalidation_rule_id)
            signal_events = self._signal_engine.evaluate_signal(
                child_signal,
                source_candles,
                trigger_rule=trigger_rule,
                invalidation_rule=invalidation_rule,
            )
            playbooks = self._registry.list_playbooks_for_signal(child_signal.signal_id)
            for signal_event in signal_events:
                for playbook in playbooks:
                    pending_routes.append(
                        _PendingRoute(
                            child_signal=child_signal,
                            signal_event=signal_event,
                            playbook=playbook,
                            source_candles=tuple(source_candles),
                        )
                    )
        return pending_routes

    def _release_due_reservations(
        self,
        snapshot: QuotaSnapshot,
        reservations: list[_ActiveReservation],
        current_ts: int,
    ) -> QuotaSnapshot:
        updated = snapshot
        still_active: list[_ActiveReservation] = []
        for reservation in reservations:
            if reservation.release_ts <= current_ts:
                updated = self._quota_engine.release_allocations(updated, reservation.allocations)
            else:
                still_active.append(reservation)
        reservations[:] = still_active
        return updated


def _build_evidence_id(route: _PendingRoute) -> str:
    return (
        f"{route.child_signal.signal_id}"
        f"::{route.playbook.playbook_id}"
        f"::{route.signal_event.candle_ts}"
        f"::{route.signal_event.candle_index}"
    )


def _build_evidence_summary(route: _PendingRoute) -> str:
    return (
        f"{route.child_signal.signal_name}"
        f" | {route.playbook.playbook_name}"
        f" | {route.signal_event.reason}"
    )


def _build_entry_evidence_record(
    route: _PendingRoute,
    *,
    evidence_id: str,
    evidence_summary: str,
) -> LabEntryEvidenceRecord:
    setup_start = max(route.signal_event.candle_index - 8, 0)
    setup_candles = tuple(
        _to_evidence_candle(item)
        for item in route.source_candles[setup_start : route.signal_event.candle_index + 1]
    )
    followthrough_end = min(route.signal_event.candle_index + 5, len(route.source_candles) - 1)
    followthrough_candles = tuple(
        _to_evidence_candle(item)
        for item in route.source_candles[route.signal_event.candle_index + 1 : followthrough_end + 1]
    )
    trigger_candle = _to_evidence_candle(route.source_candles[route.signal_event.candle_index])
    return LabEntryEvidenceRecord(
        evidence_id=evidence_id,
        signal_id=route.child_signal.signal_id,
        signal_name=route.child_signal.signal_name,
        playbook_id=route.playbook.playbook_id,
        playbook_name=route.playbook.playbook_name,
        evidence_template_id=route.child_signal.evidence_template_id,
        underlying_family=route.child_signal.underlying_family,
        source_market=route.signal_event.source_market,
        source_inst_id=route.signal_event.source_inst_id,
        source_bar=route.signal_event.source_bar,
        direction_bias=route.signal_event.direction_bias,
        candle_ts=route.signal_event.candle_ts,
        signal_price=route.signal_event.signal_price,
        trigger_reason=route.signal_event.reason,
        evidence_summary=evidence_summary,
        setup_candles=setup_candles,
        trigger_candle=trigger_candle,
        followthrough_candles=followthrough_candles,
        note=(
            f"entry on {route.signal_event.source_market}:{route.signal_event.source_inst_id} "
            f"{route.signal_event.source_bar}"
        ),
    )


def _to_evidence_candle(candle: Candle) -> EvidenceCandle:
    return EvidenceCandle(
        ts=candle.ts,
        open=candle.open,
        high=candle.high,
        low=candle.low,
        close=candle.close,
        volume=candle.volume,
    )


def _simulate_event_outcome(
    source_candles: tuple[Candle, ...],
    signal_event: SignalEvent,
    playbook: ExecutionPlaybookConfig,
    simulation: LabSimulationConfig,
) -> _SimulatedOutcome:
    if simulation.exit_mode == "tp_sl_handoff":
        return _simulate_tp_sl_handoff(source_candles, signal_event, playbook, simulation)
    return _simulate_fixed_hold(source_candles, signal_event, playbook, simulation)


def _simulate_fixed_hold(
    source_candles: tuple[Candle, ...],
    signal_event: SignalEvent,
    playbook: ExecutionPlaybookConfig,
    simulation: LabSimulationConfig,
) -> _SimulatedOutcome:
    hold_bars = simulation.fixed_hold_bars if simulation.fixed_hold_bars > 0 else max(playbook.lab_hold_bars, 0)
    entry_price_raw = signal_event.signal_price
    entry_price = _apply_slippage_price(
        entry_price_raw,
        is_long=_is_long_action(playbook.action),
        slippage_rate=simulation.slippage_rate,
        is_entry=True,
    )
    if hold_bars <= 0:
        return _SimulatedOutcome(
            lifecycle_status="open_unresolved",
            holding_bars=0,
            entry_price=entry_price,
        )
    exit_index = signal_event.candle_index + hold_bars
    if exit_index >= len(source_candles):
        return _SimulatedOutcome(
            lifecycle_status="open_unresolved",
            holding_bars=max(len(source_candles) - signal_event.candle_index - 1, 0),
            entry_price=entry_price,
        )
    exit_candle = source_candles[exit_index]
    raw_exit_price = exit_candle.close
    exit_price = _apply_slippage_price(
        raw_exit_price,
        is_long=_is_long_action(playbook.action),
        slippage_rate=simulation.slippage_rate,
        is_entry=False,
    )
    return _build_realized_outcome(
        lifecycle_status="fixed_hold_closed",
        exit_reason="fixed_hold",
        signal_event=signal_event,
        playbook=playbook,
        simulation=simulation,
        holding_bars=hold_bars,
        entry_price_raw=entry_price_raw,
        entry_price=entry_price,
        exit_ts=exit_candle.ts,
        exit_price_raw=raw_exit_price,
        exit_price=exit_price,
    )


def _simulate_tp_sl_handoff(
    source_candles: tuple[Candle, ...],
    signal_event: SignalEvent,
    playbook: ExecutionPlaybookConfig,
    simulation: LabSimulationConfig,
) -> _SimulatedOutcome:
    if simulation.max_hold_bars <= 0 or simulation.stop_loss_pct <= 0 or simulation.take_profit_pct <= 0:
        return _simulate_fixed_hold(source_candles, signal_event, playbook, simulation)

    is_long = _is_long_action(playbook.action)
    entry_price_raw = signal_event.signal_price
    entry_price = _apply_slippage_price(
        entry_price_raw,
        is_long=is_long,
        slippage_rate=simulation.slippage_rate,
        is_entry=True,
    )
    if entry_price <= 0:
        return _SimulatedOutcome(lifecycle_status="open_unresolved", holding_bars=0)

    if is_long:
        stop_loss_price = entry_price * (Decimal("1") - simulation.stop_loss_pct)
        take_profit_price = entry_price * (Decimal("1") + simulation.take_profit_pct)
    else:
        stop_loss_price = entry_price * (Decimal("1") + simulation.stop_loss_pct)
        take_profit_price = entry_price * (Decimal("1") - simulation.take_profit_pct)

    last_seen_candle: Candle | None = None
    last_seen_bars = 0
    upper_index = min(signal_event.candle_index + simulation.max_hold_bars, len(source_candles) - 1)
    for candle_index in range(signal_event.candle_index + 1, upper_index + 1):
        candle = source_candles[candle_index]
        holding_bars = candle_index - signal_event.candle_index
        last_seen_candle = candle
        last_seen_bars = holding_bars
        touched = _detect_exit_on_candle(
            candle,
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price,
        )
        if touched is None:
            continue
        raw_exit_price, exit_reason = touched
        if exit_reason == "stop_loss" and simulation.stop_hit_mode == "handoff_manual":
            return _build_handoff_outcome(
                holding_bars=holding_bars,
                entry_price=entry_price,
                stop_loss_price=stop_loss_price,
                take_profit_price=take_profit_price,
                handoff_candle=candle,
                exit_reason="stop_loss_handoff",
            )
        exit_price = _apply_slippage_price(
            raw_exit_price,
            is_long=is_long,
            slippage_rate=simulation.slippage_rate,
            is_entry=False,
        )
        return _build_realized_outcome(
            lifecycle_status="take_profit_closed" if exit_reason == "take_profit" else "stop_loss_closed",
            exit_reason=exit_reason,
            signal_event=signal_event,
            playbook=playbook,
            simulation=simulation,
            holding_bars=holding_bars,
            entry_price_raw=entry_price_raw,
            entry_price=entry_price,
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price,
            exit_ts=candle.ts,
            exit_price_raw=raw_exit_price,
            exit_price=exit_price,
        )

    if last_seen_candle is None:
        return _SimulatedOutcome(
            lifecycle_status="open_unresolved",
            holding_bars=0,
            entry_price=entry_price,
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price,
        )

    timeout_exit_price = _apply_slippage_price(
        last_seen_candle.close,
        is_long=is_long,
        slippage_rate=simulation.slippage_rate,
        is_entry=False,
    )
    timeout_outcome = _build_realized_outcome(
        lifecycle_status="timeout_profit_closed",
        exit_reason="timeout_close",
        signal_event=signal_event,
        playbook=playbook,
        simulation=simulation,
        holding_bars=last_seen_bars,
        entry_price_raw=entry_price_raw,
        entry_price=entry_price,
        stop_loss_price=stop_loss_price,
        take_profit_price=take_profit_price,
        exit_ts=last_seen_candle.ts,
        exit_price_raw=last_seen_candle.close,
        exit_price=timeout_exit_price,
    )
    if simulation.close_on_timeout_if_profitable and (timeout_outcome.realized_pnl_value or ZERO) > 0:
        return timeout_outcome
    return _build_handoff_outcome(
        holding_bars=last_seen_bars,
        entry_price=entry_price,
        stop_loss_price=stop_loss_price,
        take_profit_price=take_profit_price,
        handoff_candle=last_seen_candle,
        exit_reason="timeout_handoff",
    )


def _build_realized_outcome(
    *,
    lifecycle_status: LabLifecycleStatus,
    exit_reason: str,
    signal_event: SignalEvent,
    playbook: ExecutionPlaybookConfig,
    simulation: LabSimulationConfig,
    holding_bars: int,
    entry_price_raw: Decimal,
    entry_price: Decimal,
    exit_ts: int,
    exit_price_raw: Decimal,
    exit_price: Decimal,
    stop_loss_price: Decimal | None = None,
    take_profit_price: Decimal | None = None,
) -> _SimulatedOutcome:
    size = playbook.slot_size
    gross_points = _directional_points(entry_price, exit_price, is_long=_is_long_action(playbook.action))
    fee_per_unit = (abs(entry_price) + abs(exit_price)) * simulation.fee_rate
    slippage_per_unit = abs(entry_price - entry_price_raw) + abs(exit_price - exit_price_raw)
    funding_periods = Decimal(str(max(exit_ts - signal_event.candle_ts, 0))) / EIGHT_HOURS_MS
    funding_per_unit = abs(entry_price) * simulation.funding_rate_per_8h * funding_periods
    net_points = gross_points - fee_per_unit - funding_per_unit
    notional = abs(entry_price)
    net_pct = ZERO if notional == 0 else (net_points / notional) * Decimal("100")
    return _SimulatedOutcome(
        lifecycle_status=lifecycle_status,
        holding_bars=holding_bars,
        entry_price=entry_price,
        stop_loss_price=stop_loss_price,
        take_profit_price=take_profit_price,
        exit_ts=exit_ts,
        exit_price=exit_price,
        exit_reason=exit_reason,
        gross_pnl_value=gross_points * size,
        fee_cost_value=fee_per_unit * size,
        slippage_cost_value=slippage_per_unit * size,
        funding_cost_value=funding_per_unit * size,
        realized_pnl_points=net_points,
        realized_pnl_pct=net_pct,
        realized_pnl_value=net_points * size,
        release_ts=exit_ts,
        should_release_quota=True,
    )


def _build_handoff_outcome(
    *,
    holding_bars: int,
    entry_price: Decimal,
    stop_loss_price: Decimal,
    take_profit_price: Decimal,
    handoff_candle: Candle,
    exit_reason: str,
) -> _SimulatedOutcome:
    return _SimulatedOutcome(
        lifecycle_status="handoff_manual",
        holding_bars=holding_bars,
        entry_price=entry_price,
        stop_loss_price=stop_loss_price,
        take_profit_price=take_profit_price,
        exit_ts=handoff_candle.ts,
        exit_price=handoff_candle.close,
        exit_reason=exit_reason,
        should_release_quota=False,
    )


def _directional_points(entry_price: Decimal, exit_price: Decimal, *, is_long: bool) -> Decimal:
    return exit_price - entry_price if is_long else entry_price - exit_price


def _apply_slippage_price(
    price: Decimal,
    *,
    is_long: bool,
    slippage_rate: Decimal,
    is_entry: bool,
) -> Decimal:
    if price <= 0 or slippage_rate <= 0:
        return price
    if is_long:
        multiplier = Decimal("1") + slippage_rate if is_entry else Decimal("1") - slippage_rate
    else:
        multiplier = Decimal("1") - slippage_rate if is_entry else Decimal("1") + slippage_rate
    return price * multiplier


def _is_long_action(action: str) -> bool:
    if action in LONG_ACTIONS:
        return True
    if action in SHORT_ACTIONS:
        return False
    raise ValueError(f"unsupported playbook action for lab outcome: {action}")


def _detect_exit_on_candle(
    candle: Candle,
    *,
    stop_loss_price: Decimal,
    take_profit_price: Decimal,
) -> tuple[Decimal, str] | None:
    path_points = _candle_path_points(candle)
    segment_start = path_points[0]
    for segment_end in path_points[1:]:
        touched = _detect_exit_on_segment(
            segment_start,
            segment_end,
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price,
        )
        if touched is not None:
            return touched
        segment_start = segment_end
    return None


def _candle_path_points(candle: Candle) -> tuple[Decimal, ...]:
    if candle.close >= candle.open:
        return candle.open, candle.low, candle.high, candle.close
    return candle.open, candle.high, candle.low, candle.close


def _detect_exit_on_segment(
    start: Decimal,
    end: Decimal,
    *,
    stop_loss_price: Decimal,
    take_profit_price: Decimal,
) -> tuple[Decimal, str] | None:
    touched: list[tuple[Decimal, Decimal, str]] = []
    if _segment_contains_price(start, end, stop_loss_price):
        touched.append((abs(stop_loss_price - start), stop_loss_price, "stop_loss"))
    if _segment_contains_price(start, end, take_profit_price):
        touched.append((abs(take_profit_price - start), take_profit_price, "take_profit"))
    if not touched:
        return None
    _, price, reason = min(touched, key=lambda item: item[0])
    return price, reason


def _segment_contains_price(start: Decimal, end: Decimal, target: Decimal) -> bool:
    low = min(start, end)
    high = max(start, end)
    return low <= target <= high


def format_lab_report_markdown(result: EnhancedBacktestLabResult) -> str:
    simulation = result.simulation_config
    lines = [
        f"# {result.parent_strategy_name} 子策略实验室报告",
        "",
        f"- 父策略 ID：`{result.parent_strategy_id}`",
        f"- 子策略数量：`{len(result.summaries)}`",
        f"- 事件总数：`{len(result.events)}`",
        f"- 进场证据数：`{len(result.evidences)}`",
        f"- 实验模式：`{simulation.exit_mode}`",
        f"- 单边手续费率：`{_format_rate(simulation.fee_rate)}`",
        f"- 单边滑点率：`{_format_rate(simulation.slippage_rate)}`",
        f"- 8小时资金费率：`{_format_rate(simulation.funding_rate_per_8h)}`",
    ]
    if simulation.exit_mode == "fixed_hold":
        fixed_hold_bars = simulation.fixed_hold_bars if simulation.fixed_hold_bars > 0 else "playbook_default"
        lines.append(f"- 固定持有 bars：`{fixed_hold_bars}`")
    else:
        lines.extend(
            [
                f"- 止损幅度：`{_format_rate(simulation.stop_loss_pct)}`",
                f"- 止盈幅度：`{_format_rate(simulation.take_profit_pct)}`",
                f"- 最大持有 bars：`{simulation.max_hold_bars}`",
                f"- 止损触发处理：`{simulation.stop_hit_mode}`",
                f"- 超时盈利平仓：`{'yes' if simulation.close_on_timeout_if_profitable else 'no'}`",
            ]
        )
    lines.extend(
        [
            "",
            "## 口径说明",
            "- 已实现胜率与盈亏比，只统计真正平掉的实验单。",
            "- `handoff_manual` 表示该单触发人工接管，不计入已实现胜率，但会持续占用额度。",
            "- 详细事件明细建议配合 JSON / CSV 永久文件一起审阅。",
            "- 进场证据会额外保存信号说明与前后 K 线切片，便于后续人工复核。",
            "",
            "## 子策略参数表",
        ]
    )
    for summary in result.summaries:
        lines.extend(
            [
                f"### {summary.signal_name}",
                f"- 参数档名：`{summary.lab_profile_name or 'default'}`",
            ]
        )
        lines.extend(_format_simulation_lines(summary.applied_simulation_config))
        lines.append("")
    lines.extend(
        [
            "## 子策略摘要",
        ]
    )
    for summary in result.summaries:
        lines.extend(
            [
                f"### {summary.signal_name}",
                f"- 标的家族：`{summary.underlying_family}`",
                f"- 参数档名：`{summary.lab_profile_name or 'default'}`",
                f"- 信号次数：`{summary.total_signal_events}`",
                f"- 路由次数：`{summary.routed_events}`",
                f"- 通过次数：`{summary.accepted_events}`",
                f"- 拒绝次数：`{summary.rejected_events}`",
                f"- 门控拒绝：`{summary.gate_rejected_events}`",
                f"- 配额拒绝：`{summary.quota_rejected_events}`",
                f"- 已实现样本：`{summary.realized_outcomes}`",
                f"- 手工接管：`{summary.manual_handoff_events}`",
                f"- 未完结样本：`{summary.unresolved_events}`",
                f"- 止盈平仓：`{summary.take_profit_closed_events}`",
                f"- 止损平仓：`{summary.stop_loss_closed_events}`",
                f"- 超时盈利平仓：`{summary.timeout_profit_closed_events}`",
                f"- 胜率：`{summary.win_rate_pct:.4f}%`",
                f"- 总净盈亏：`{summary.total_pnl_value:.6f}`",
                f"- 平均每笔净盈亏：`{summary.average_pnl_value:.6f}`",
                f"- 平均净收益率：`{summary.average_pnl_pct:.4f}%`",
                f"- 盈利因子：`{'-' if summary.profit_factor is None else f'{summary.profit_factor:.4f}'}`",
                f"- 平均盈利：`{summary.average_win_pct:.4f}%`",
                f"- 平均亏损：`{summary.average_loss_pct:.4f}%`",
                f"- 盈亏比：`{'-' if summary.profit_loss_ratio is None else f'{summary.profit_loss_ratio:.4f}'}`",
            ]
        )
        if summary.playbook_accept_counts:
            joined = ", ".join(f"{name}={count}" for name, count in summary.playbook_accept_counts)
            lines.append(f"- 通过分布：`{joined}`")
        if summary.rejection_reasons:
            joined = ", ".join(f"{reason}={count}" for reason, count in summary.rejection_reasons)
            lines.append(f"- 拒绝原因：`{joined}`")
        lines.append("")
    if result.ending_quota_snapshots:
        lines.append("## 期末额度占用")
        for snapshot in result.ending_quota_snapshots:
            lines.extend(
                [
                    f"### {snapshot.underlying_family}",
                    f"- 多头额度：`{snapshot.long_limit_used}` / `{snapshot.effective_long_limit_total}`",
                    f"- 空头额度：`{snapshot.short_limit_used}` / `{snapshot.effective_short_limit_total}`",
                    f"- 备兑额度：`{snapshot.covered_call_quota_used}` / `{snapshot.covered_call_quota_total}`",
                    f"- 备付认沽额度：`{snapshot.cash_secured_put_quota_used}` / `{snapshot.cash_secured_put_quota_total}`",
                    f"- 保护多头额度：`{snapshot.protected_long_quota_used}` / `{snapshot.protected_long_quota_total}`",
                    f"- 保护空头额度：`{snapshot.protected_short_quota_used}` / `{snapshot.protected_short_quota_total}`",
                    "",
                ]
            )
    return "\n".join(lines).strip() + "\n"


def export_lab_report_markdown(
    result: EnhancedBacktestLabResult,
    path: Path | str,
) -> dict[str, Path]:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    markdown = format_lab_report_markdown(result)
    target.write_text(markdown, encoding="utf-8")
    json_path = target.with_suffix(".json")
    payload = {
        "parent_strategy_id": result.parent_strategy_id,
        "parent_strategy_name": result.parent_strategy_name,
        "simulation_config": _serialize_simulation_config(result.simulation_config),
        "summaries": [
            {
                "signal_id": item.signal_id,
                "signal_name": item.signal_name,
                "underlying_family": item.underlying_family,
                "lab_profile_name": item.lab_profile_name,
                "applied_simulation_config": _serialize_simulation_config(item.applied_simulation_config),
                "total_signal_events": item.total_signal_events,
                "routed_events": item.routed_events,
                "accepted_events": item.accepted_events,
                "rejected_events": item.rejected_events,
                "gate_rejected_events": item.gate_rejected_events,
                "quota_rejected_events": item.quota_rejected_events,
                "realized_outcomes": item.realized_outcomes,
                "take_profit_closed_events": item.take_profit_closed_events,
                "stop_loss_closed_events": item.stop_loss_closed_events,
                "timeout_profit_closed_events": item.timeout_profit_closed_events,
                "manual_handoff_events": item.manual_handoff_events,
                "unresolved_events": item.unresolved_events,
                "win_events": item.win_events,
                "loss_events": item.loss_events,
                "breakeven_events": item.breakeven_events,
                "win_rate_pct": str(item.win_rate_pct),
                "total_pnl_value": str(item.total_pnl_value),
                "average_pnl_value": str(item.average_pnl_value),
                "gross_profit_value": str(item.gross_profit_value),
                "gross_loss_value": str(item.gross_loss_value),
                "profit_factor": None if item.profit_factor is None else str(item.profit_factor),
                "average_pnl_pct": str(item.average_pnl_pct),
                "average_win_pct": str(item.average_win_pct),
                "average_loss_pct": str(item.average_loss_pct),
                "profit_loss_ratio": None if item.profit_loss_ratio is None else str(item.profit_loss_ratio),
                "playbook_accept_counts": list(item.playbook_accept_counts),
                "rejection_reasons": list(item.rejection_reasons),
            }
            for item in result.summaries
        ],
        "events": [
            {
                "signal_id": item.signal_id,
                "signal_name": item.signal_name,
                "playbook_id": item.playbook_id,
                "playbook_name": item.playbook_name,
                "evidence_id": item.evidence_id,
                "evidence_template_id": item.evidence_template_id,
                "underlying_family": item.underlying_family,
                "candle_ts": item.candle_ts,
                "signal_price": str(item.signal_price),
                "position_size": str(item.position_size),
                "accepted": item.accepted,
                "trigger_reason": item.trigger_reason,
                "evidence_summary": item.evidence_summary,
                "gate_reason": item.gate_reason,
                "gate_checks": [_serialize_gate_check(check) for check in item.gate_checks],
                "quota_reason": item.quota_reason,
                "quota_errors": list(item.quota_errors),
                "quota_warnings": list(item.quota_warnings),
                "quota_allocations": [(source, str(quantity)) for source, quantity in item.quota_allocations],
                "lifecycle_status": item.lifecycle_status,
                "entry_price": None if item.entry_price is None else str(item.entry_price),
                "stop_loss_price": None if item.stop_loss_price is None else str(item.stop_loss_price),
                "take_profit_price": None if item.take_profit_price is None else str(item.take_profit_price),
                "holding_bars": item.holding_bars,
                "exit_ts": item.exit_ts,
                "exit_price": None if item.exit_price is None else str(item.exit_price),
                "exit_reason": item.exit_reason,
                "gross_pnl_value": None if item.gross_pnl_value is None else str(item.gross_pnl_value),
                "fee_cost_value": None if item.fee_cost_value is None else str(item.fee_cost_value),
                "slippage_cost_value": None if item.slippage_cost_value is None else str(item.slippage_cost_value),
                "funding_cost_value": None if item.funding_cost_value is None else str(item.funding_cost_value),
                "realized_pnl_points": None if item.realized_pnl_points is None else str(item.realized_pnl_points),
                "realized_pnl_pct": None if item.realized_pnl_pct is None else str(item.realized_pnl_pct),
                "realized_pnl_value": None if item.realized_pnl_value is None else str(item.realized_pnl_value),
            }
            for item in result.events
        ],
        "evidences": [
            {
                "evidence_id": item.evidence_id,
                "signal_id": item.signal_id,
                "signal_name": item.signal_name,
                "playbook_id": item.playbook_id,
                "playbook_name": item.playbook_name,
                "evidence_template_id": item.evidence_template_id,
                "underlying_family": item.underlying_family,
                "source_market": item.source_market,
                "source_inst_id": item.source_inst_id,
                "source_bar": item.source_bar,
                "direction_bias": item.direction_bias,
                "candle_ts": item.candle_ts,
                "signal_price": str(item.signal_price),
                "trigger_reason": item.trigger_reason,
                "evidence_summary": item.evidence_summary,
                "setup_candles": [_serialize_evidence_candle(candle) for candle in item.setup_candles],
                "trigger_candle": None if item.trigger_candle is None else _serialize_evidence_candle(item.trigger_candle),
                "followthrough_candles": [_serialize_evidence_candle(candle) for candle in item.followthrough_candles],
                "note": item.note,
            }
            for item in result.evidences
        ],
        "ending_quota_snapshots": [_serialize_quota_snapshot(item) for item in result.ending_quota_snapshots],
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "report": target,
        "json": json_path,
    }


def _serialize_simulation_config(config: LabSimulationConfig) -> dict[str, object]:
    return {
        "exit_mode": config.exit_mode,
        "fixed_hold_bars": config.fixed_hold_bars,
        "max_hold_bars": config.max_hold_bars,
        "stop_loss_pct": str(config.stop_loss_pct),
        "take_profit_pct": str(config.take_profit_pct),
        "fee_rate": str(config.fee_rate),
        "slippage_rate": str(config.slippage_rate),
        "funding_rate_per_8h": str(config.funding_rate_per_8h),
        "stop_hit_mode": config.stop_hit_mode,
        "close_on_timeout_if_profitable": config.close_on_timeout_if_profitable,
    }


def _format_simulation_lines(config: LabSimulationConfig) -> list[str]:
    lines = [
        f"- 实验模式：`{config.exit_mode}`",
        f"- 单边手续费率：`{_format_rate(config.fee_rate)}`",
        f"- 单边滑点率：`{_format_rate(config.slippage_rate)}`",
        f"- 8小时资金费率：`{_format_rate(config.funding_rate_per_8h)}`",
    ]
    if config.exit_mode == "fixed_hold":
        fixed_hold_bars = config.fixed_hold_bars if config.fixed_hold_bars > 0 else "playbook_default"
        lines.append(f"- 固定持有 bars：`{fixed_hold_bars}`")
    else:
        lines.extend(
            [
                f"- 止损幅度：`{_format_rate(config.stop_loss_pct)}`",
                f"- 止盈幅度：`{_format_rate(config.take_profit_pct)}`",
                f"- 最大持有 bars：`{config.max_hold_bars}`",
                f"- 止损触发处理：`{config.stop_hit_mode}`",
                f"- 超时盈利平仓：`{'yes' if config.close_on_timeout_if_profitable else 'no'}`",
            ]
        )
    return lines


def _serialize_gate_check(check: GateCheckResult) -> dict[str, object]:
    return {
        "gate_id": check.gate_id,
        "gate_name": check.gate_name,
        "effect": check.effect,
        "matched": check.matched,
        "allowed": check.allowed,
        "reason": check.reason,
    }


def _serialize_quota_snapshot(snapshot: QuotaSnapshot) -> dict[str, object]:
    return {
        "underlying_family": snapshot.underlying_family,
        "long_limit_total": str(snapshot.long_limit_total),
        "long_limit_used": str(snapshot.long_limit_used),
        "effective_long_limit_total": str(snapshot.effective_long_limit_total),
        "short_limit_total": str(snapshot.short_limit_total),
        "short_limit_used": str(snapshot.short_limit_used),
        "effective_short_limit_total": str(snapshot.effective_short_limit_total),
        "covered_call_quota_total": str(snapshot.covered_call_quota_total),
        "covered_call_quota_used": str(snapshot.covered_call_quota_used),
        "cash_secured_put_quota_total": str(snapshot.cash_secured_put_quota_total),
        "cash_secured_put_quota_used": str(snapshot.cash_secured_put_quota_used),
        "protected_long_quota_total": str(snapshot.protected_long_quota_total),
        "protected_long_quota_used": str(snapshot.protected_long_quota_used),
        "protected_short_quota_total": str(snapshot.protected_short_quota_total),
        "protected_short_quota_used": str(snapshot.protected_short_quota_used),
    }


def _serialize_evidence_candle(candle: EvidenceCandle) -> dict[str, object]:
    return {
        "ts": candle.ts,
        "open": str(candle.open),
        "high": str(candle.high),
        "low": str(candle.low),
        "close": str(candle.close),
        "volume": str(candle.volume),
    }


def _format_rate(value: Decimal) -> str:
    return f"{(value * Decimal('100')):.4f}%"
