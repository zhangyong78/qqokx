from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace
from typing import Callable

from okx_quant.enhanced_models import (
    ChildSignalConfig,
    ChildSignalLabProfile,
    ExecutionPlaybookConfig,
    GateRuleConfig,
    ParentStrategyConfig,
    SignalRuleMatch,
)
from okx_quant.models import Candle


SignalRule = Callable[[list[Candle], int], SignalRuleMatch | None]


@dataclass(frozen=True)
class SignalRouting:
    signal_id: str
    playbook_ids: tuple[str, ...]
    gate_ids: tuple[str, ...]


class EnhancedStrategyRegistry:
    def __init__(self) -> None:
        self._parents: dict[str, ParentStrategyConfig] = {}
        self._child_signals: dict[str, ChildSignalConfig] = {}
        self._gate_rules: dict[str, GateRuleConfig] = {}
        self._playbooks: dict[str, ExecutionPlaybookConfig] = {}
        self._signal_lab_profiles: dict[str, ChildSignalLabProfile] = {}
        self._trigger_rules: dict[str, SignalRule] = {}
        self._invalidation_rules: dict[str, SignalRule] = {}
        self._parent_signals: dict[str, list[str]] = defaultdict(list)
        self._signal_gate_ids: dict[str, list[str]] = defaultdict(list)
        self._signal_playbook_ids: dict[str, list[str]] = defaultdict(list)

    def register_parent_strategy(self, config: ParentStrategyConfig) -> ParentStrategyConfig:
        self._parents[config.strategy_id] = config
        return config

    def register_child_signal(self, parent_strategy_id: str, config: ChildSignalConfig) -> ChildSignalConfig:
        if parent_strategy_id not in self._parents:
            raise KeyError(f"unknown parent strategy: {parent_strategy_id}")
        self._child_signals[config.signal_id] = config
        signal_ids = self._parent_signals[parent_strategy_id]
        if config.signal_id not in signal_ids:
            signal_ids.append(config.signal_id)
        return config

    def register_gate_rule(self, config: GateRuleConfig) -> GateRuleConfig:
        self._gate_rules[config.gate_id] = config
        return config

    def register_playbook(self, config: ExecutionPlaybookConfig) -> ExecutionPlaybookConfig:
        self._playbooks[config.playbook_id] = config
        return config

    def register_signal_lab_profile(self, config: ChildSignalLabProfile) -> ChildSignalLabProfile:
        if config.signal_id not in self._child_signals:
            raise KeyError(f"unknown child signal: {config.signal_id}")
        self._signal_lab_profiles[config.signal_id] = config.normalized()
        return self._signal_lab_profiles[config.signal_id]

    def set_child_signal_enabled(self, signal_id: str, enabled: bool) -> ChildSignalConfig:
        signal = self.get_child_signal(signal_id)
        updated = replace(signal, enabled=bool(enabled))
        self._child_signals[signal_id] = updated
        return updated

    def patch_signal_lab_profile(self, signal_id: str, **changes: object) -> ChildSignalLabProfile:
        if signal_id not in self._child_signals:
            raise KeyError(f"unknown child signal: {signal_id}")
        current = self._signal_lab_profiles.get(signal_id, ChildSignalLabProfile(signal_id=signal_id))
        updated = replace(current, **changes).normalized()
        self._signal_lab_profiles[signal_id] = updated
        return updated

    def replace_signal_lab_profile(
        self,
        signal_id: str,
        profile: ChildSignalLabProfile | None,
    ) -> ChildSignalLabProfile | None:
        if signal_id not in self._child_signals:
            raise KeyError(f"unknown child signal: {signal_id}")
        if profile is None:
            self._signal_lab_profiles.pop(signal_id, None)
            return None
        updated = replace(profile, signal_id=signal_id).normalized()
        self._signal_lab_profiles[signal_id] = updated
        return updated

    def clear_signal_lab_profile(self, signal_id: str) -> ChildSignalLabProfile | None:
        return self._signal_lab_profiles.pop(signal_id, None)

    def register_trigger_rule(self, rule_id: str, rule: SignalRule) -> None:
        self._trigger_rules[rule_id] = rule

    def register_invalidation_rule(self, rule_id: str, rule: SignalRule) -> None:
        self._invalidation_rules[rule_id] = rule

    def bind_signal(
        self,
        signal_id: str,
        *,
        playbook_ids: list[str] | tuple[str, ...] = (),
        gate_ids: list[str] | tuple[str, ...] = (),
    ) -> SignalRouting:
        if signal_id not in self._child_signals:
            raise KeyError(f"unknown child signal: {signal_id}")
        for playbook_id in playbook_ids:
            if playbook_id not in self._playbooks:
                raise KeyError(f"unknown playbook: {playbook_id}")
            if playbook_id not in self._signal_playbook_ids[signal_id]:
                self._signal_playbook_ids[signal_id].append(playbook_id)
        for gate_id in gate_ids:
            if gate_id not in self._gate_rules:
                raise KeyError(f"unknown gate rule: {gate_id}")
            if gate_id not in self._signal_gate_ids[signal_id]:
                self._signal_gate_ids[signal_id].append(gate_id)
        return SignalRouting(
            signal_id=signal_id,
            playbook_ids=tuple(self._signal_playbook_ids[signal_id]),
            gate_ids=tuple(self._signal_gate_ids[signal_id]),
        )

    def get_parent_strategy(self, strategy_id: str) -> ParentStrategyConfig:
        try:
            return self._parents[strategy_id]
        except KeyError as exc:
            raise KeyError(f"unknown parent strategy: {strategy_id}") from exc

    def get_child_signal(self, signal_id: str) -> ChildSignalConfig:
        try:
            return self._child_signals[signal_id]
        except KeyError as exc:
            raise KeyError(f"unknown child signal: {signal_id}") from exc

    def get_gate_rule(self, gate_id: str) -> GateRuleConfig:
        try:
            return self._gate_rules[gate_id]
        except KeyError as exc:
            raise KeyError(f"unknown gate rule: {gate_id}") from exc

    def get_playbook(self, playbook_id: str) -> ExecutionPlaybookConfig:
        try:
            return self._playbooks[playbook_id]
        except KeyError as exc:
            raise KeyError(f"unknown playbook: {playbook_id}") from exc

    def get_signal_lab_profile(self, signal_id: str) -> ChildSignalLabProfile | None:
        return self._signal_lab_profiles.get(signal_id)

    def get_trigger_rule(self, rule_id: str) -> SignalRule:
        try:
            return self._trigger_rules[rule_id]
        except KeyError as exc:
            raise KeyError(f"unknown trigger rule: {rule_id}") from exc

    def get_invalidation_rule(self, rule_id: str) -> SignalRule | None:
        return self._invalidation_rules.get(rule_id)

    def list_child_signals(self, parent_strategy_id: str, *, enabled_only: bool = True) -> list[ChildSignalConfig]:
        if parent_strategy_id not in self._parents:
            raise KeyError(f"unknown parent strategy: {parent_strategy_id}")
        results: list[ChildSignalConfig] = []
        for signal_id in self._parent_signals[parent_strategy_id]:
            item = self._child_signals.get(signal_id)
            if item is None:
                continue
            if enabled_only and not item.enabled:
                continue
            results.append(item)
        return results

    def list_gate_rules_for_signal(self, signal_id: str, *, enabled_only: bool = True) -> list[GateRuleConfig]:
        results: list[GateRuleConfig] = []
        for gate_id in self._signal_gate_ids.get(signal_id, ()):
            item = self._gate_rules.get(gate_id)
            if item is None:
                continue
            if enabled_only and not item.enabled:
                continue
            results.append(item)
        return results

    def list_playbooks_for_signal(self, signal_id: str, *, enabled_only: bool = True) -> list[ExecutionPlaybookConfig]:
        results: list[ExecutionPlaybookConfig] = []
        for playbook_id in self._signal_playbook_ids.get(signal_id, ()):
            item = self._playbooks.get(playbook_id)
            if item is None:
                continue
            if enabled_only and not item.enabled:
                continue
            results.append(item)
        return results

    def list_signal_lab_profiles(
        self,
        parent_strategy_id: str,
        *,
        enabled_only: bool = True,
    ) -> list[ChildSignalLabProfile]:
        if parent_strategy_id not in self._parents:
            raise KeyError(f"unknown parent strategy: {parent_strategy_id}")
        results: list[ChildSignalLabProfile] = []
        for signal_id in self._parent_signals[parent_strategy_id]:
            signal = self._child_signals.get(signal_id)
            if signal is None:
                continue
            if enabled_only and not signal.enabled:
                continue
            profile = self._signal_lab_profiles.get(signal_id)
            if profile is not None:
                results.append(profile)
        return results
