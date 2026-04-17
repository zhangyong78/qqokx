from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from okx_quant.enhanced_models import (
    FamilyLedgerSnapshot,
    HedgeGroupEntry,
    LifecycleStatus,
    OptionLedgerEntry,
    QuotaSnapshot,
    SpotLedgerEntry,
    SwapLedgerEntry,
)


ZERO = Decimal("0")


class EnhancedLedger:
    def __init__(self) -> None:
        self._spot_entries: dict[str, SpotLedgerEntry] = {}
        self._swap_entries: dict[str, SwapLedgerEntry] = {}
        self._option_entries: dict[str, OptionLedgerEntry] = {}
        self._hedge_groups: dict[str, HedgeGroupEntry] = {}

    def upsert_spot_entry(self, entry: SpotLedgerEntry) -> SpotLedgerEntry:
        self._spot_entries[entry.entry_id] = entry
        return entry

    def upsert_swap_entry(self, entry: SwapLedgerEntry) -> SwapLedgerEntry:
        self._swap_entries[entry.position_id] = entry
        return entry

    def upsert_option_entry(self, entry: OptionLedgerEntry) -> OptionLedgerEntry:
        self._option_entries[entry.position_id] = entry
        return entry

    def upsert_hedge_group(self, entry: HedgeGroupEntry) -> HedgeGroupEntry:
        self._hedge_groups[entry.hedge_group_id] = entry
        return entry

    def get_spot_entry(self, entry_id: str) -> SpotLedgerEntry | None:
        return self._spot_entries.get(entry_id)

    def get_swap_entry(self, position_id: str) -> SwapLedgerEntry | None:
        return self._swap_entries.get(position_id)

    def get_option_entry(self, position_id: str) -> OptionLedgerEntry | None:
        return self._option_entries.get(position_id)

    def get_hedge_group(self, hedge_group_id: str) -> HedgeGroupEntry | None:
        return self._hedge_groups.get(hedge_group_id)

    def mark_swap_manual(self, position_id: str) -> SwapLedgerEntry:
        current = self._require_swap(position_id)
        updated = replace(current, pool="manual", status=self._manual_status(current.status))
        self._swap_entries[position_id] = updated
        return updated

    def mark_option_manual(self, position_id: str) -> OptionLedgerEntry:
        current = self._require_option(position_id)
        updated = replace(current, pool="manual", status=self._manual_status(current.status))
        self._option_entries[position_id] = updated
        return updated

    def close_swap(self, position_id: str, *, status: LifecycleStatus = "closed_manual") -> SwapLedgerEntry:
        current = self._require_swap(position_id)
        updated = replace(current, status=status)
        self._swap_entries[position_id] = updated
        return updated

    def close_option(self, position_id: str, *, status: LifecycleStatus = "closed_manual") -> OptionLedgerEntry:
        current = self._require_option(position_id)
        updated = replace(current, status=status)
        self._option_entries[position_id] = updated
        return updated

    def close_spot(self, entry_id: str, *, status: LifecycleStatus = "closed_manual") -> SpotLedgerEntry:
        current = self._require_spot(entry_id)
        updated = replace(current, status=status)
        self._spot_entries[entry_id] = updated
        return updated

    def list_spot_entries(self, *, underlying_family: str | None = None, active_only: bool = True) -> list[SpotLedgerEntry]:
        return self._filter_entries(self._spot_entries.values(), underlying_family=underlying_family, active_only=active_only)

    def list_swap_entries(self, *, underlying_family: str | None = None, active_only: bool = True) -> list[SwapLedgerEntry]:
        return self._filter_entries(self._swap_entries.values(), underlying_family=underlying_family, active_only=active_only)

    def list_option_entries(
        self,
        *,
        underlying_family: str | None = None,
        active_only: bool = True,
    ) -> list[OptionLedgerEntry]:
        return self._filter_entries(
            self._option_entries.values(),
            underlying_family=underlying_family,
            active_only=active_only,
        )

    def list_hedge_groups(
        self,
        *,
        underlying_family: str | None = None,
        active_only: bool = True,
    ) -> list[HedgeGroupEntry]:
        return self._filter_entries(
            self._hedge_groups.values(),
            underlying_family=underlying_family,
            active_only=active_only,
        )

    def build_family_snapshot(
        self,
        underlying_family: str,
        *,
        long_limit_total: Decimal = ZERO,
        short_limit_total: Decimal = ZERO,
        manual_extra_long_limit: Decimal = ZERO,
        manual_extra_short_limit: Decimal = ZERO,
        cash_secured_put_quota_total: Decimal = ZERO,
    ) -> FamilyLedgerSnapshot:
        spot_entries = tuple(self.list_spot_entries(underlying_family=underlying_family, active_only=True))
        swap_entries = tuple(self.list_swap_entries(underlying_family=underlying_family, active_only=True))
        option_entries = tuple(self.list_option_entries(underlying_family=underlying_family, active_only=True))
        hedge_groups = tuple(self.list_hedge_groups(underlying_family=underlying_family, active_only=True))

        spot_inventory_total = sum((max(item.quantity, ZERO) for item in spot_entries), ZERO)
        spot_inventory_reserved = sum((max(item.sell_reserved_quantity, ZERO) for item in spot_entries), ZERO)
        long_limit_used = sum(
            (max(item.quantity, ZERO) for item in spot_entries if item.counts_toward_long_limit),
            ZERO,
        )
        long_limit_used += sum(
            (max(item.quantity, ZERO) for item in swap_entries if item.direction == "long"),
            ZERO,
        )
        short_limit_used = sum(
            (max(item.quantity, ZERO) for item in swap_entries if item.direction == "short"),
            ZERO,
        )
        covered_call_quota_total_value = sum((item.covered_call_capacity for item in spot_entries), ZERO)
        covered_call_quota_used = sum((max(item.covered_call_committed_quantity, ZERO) for item in spot_entries), ZERO)
        cash_secured_put_quota_used = sum((max(item.cash_secured_put_quantity, ZERO) for item in option_entries), ZERO)
        protected_long_quota_total = sum((max(item.protected_long_capacity, ZERO) for item in option_entries), ZERO)
        protected_short_quota_total = sum((max(item.protected_short_capacity, ZERO) for item in option_entries), ZERO)
        protected_long_quota_used = sum(
            (max(item.protected_quantity, ZERO) for item in swap_entries if item.direction == "long"),
            ZERO,
        )
        protected_long_quota_used += sum((max(item.protected_quantity, ZERO) for item in spot_entries), ZERO)
        protected_short_quota_used = sum(
            (max(item.protected_quantity, ZERO) for item in swap_entries if item.direction == "short"),
            ZERO,
        )

        quota_snapshot = QuotaSnapshot(
            underlying_family=underlying_family,
            long_limit_total=long_limit_total,
            long_limit_used=long_limit_used,
            short_limit_total=short_limit_total,
            short_limit_used=short_limit_used,
            manual_extra_long_limit=manual_extra_long_limit,
            manual_extra_short_limit=manual_extra_short_limit,
            spot_inventory_total=spot_inventory_total,
            spot_inventory_reserved=spot_inventory_reserved,
            covered_call_quota_total=covered_call_quota_total_value,
            covered_call_quota_used=covered_call_quota_used,
            cash_secured_put_quota_total=cash_secured_put_quota_total,
            cash_secured_put_quota_used=cash_secured_put_quota_used,
            protected_long_quota_total=protected_long_quota_total,
            protected_long_quota_used=protected_long_quota_used,
            protected_short_quota_total=protected_short_quota_total,
            protected_short_quota_used=protected_short_quota_used,
        ).normalized()
        return FamilyLedgerSnapshot(
            underlying_family=underlying_family,
            spot_entries=spot_entries,
            swap_entries=swap_entries,
            option_entries=option_entries,
            hedge_groups=hedge_groups,
            quota_snapshot=quota_snapshot,
        )

    def _manual_status(self, current_status: LifecycleStatus) -> LifecycleStatus:
        if current_status in {"candidate", "submitted"}:
            return current_status
        return "handoff_manual"

    def _require_spot(self, entry_id: str) -> SpotLedgerEntry:
        entry = self.get_spot_entry(entry_id)
        if entry is None:
            raise KeyError(f"unknown spot entry: {entry_id}")
        return entry

    def _require_swap(self, position_id: str) -> SwapLedgerEntry:
        entry = self.get_swap_entry(position_id)
        if entry is None:
            raise KeyError(f"unknown swap entry: {position_id}")
        return entry

    def _require_option(self, position_id: str) -> OptionLedgerEntry:
        entry = self.get_option_entry(position_id)
        if entry is None:
            raise KeyError(f"unknown option entry: {position_id}")
        return entry

    def _filter_entries(self, entries: object, *, underlying_family: str | None, active_only: bool) -> list[object]:
        filtered: list[object] = []
        for entry in entries:
            if underlying_family is not None and getattr(entry, "underlying_family", None) != underlying_family:
                continue
            if active_only and not bool(getattr(entry, "is_active", False)):
                continue
            filtered.append(entry)
        return filtered
