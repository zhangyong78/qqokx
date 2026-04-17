from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from okx_quant.enhanced_models import (
    QuotaAllocation,
    QuotaDecision,
    QuotaRequest,
    QuotaSnapshot,
)


ZERO = Decimal("0")


class QuotaEngine:
    def evaluate(self, snapshot: QuotaSnapshot, request: QuotaRequest) -> QuotaDecision:
        normalized_snapshot = snapshot.normalized()
        normalized_request = request.normalized()
        errors: list[str] = []
        warnings: list[str] = []

        if normalized_snapshot.underlying_family != normalized_request.underlying_family:
            errors.append(
                f"request family {normalized_request.underlying_family} does not match "
                f"snapshot family {normalized_snapshot.underlying_family}"
            )

        if normalized_snapshot.available_long_direction < normalized_request.required_long_direction_qty:
            errors.append(
                "insufficient long direction quota: "
                f"need {normalized_request.required_long_direction_qty}, "
                f"have {normalized_snapshot.available_long_direction}"
            )
        if normalized_snapshot.available_short_direction < normalized_request.required_short_direction_qty:
            errors.append(
                "insufficient short direction quota: "
                f"need {normalized_request.required_short_direction_qty}, "
                f"have {normalized_snapshot.available_short_direction}"
            )
        if normalized_snapshot.available_spot_inventory < normalized_request.required_spot_inventory_qty:
            errors.append(
                "insufficient spot inventory quota: "
                f"need {normalized_request.required_spot_inventory_qty}, "
                f"have {normalized_snapshot.available_spot_inventory}"
            )
        if normalized_snapshot.available_covered_call < normalized_request.required_covered_call_qty:
            errors.append(
                "insufficient covered call quota: "
                f"need {normalized_request.required_covered_call_qty}, "
                f"have {normalized_snapshot.available_covered_call}"
            )
        if normalized_snapshot.available_cash_secured_put < normalized_request.required_cash_secured_put_qty:
            errors.append(
                "insufficient cash secured put quota: "
                f"need {normalized_request.required_cash_secured_put_qty}, "
                f"have {normalized_snapshot.available_cash_secured_put}"
            )

        protected_long_applied = min(
            normalized_request.preferred_protected_long_qty,
            normalized_snapshot.available_protected_long,
        )
        protected_short_applied = min(
            normalized_request.preferred_protected_short_qty,
            normalized_snapshot.available_protected_short,
        )

        if protected_long_applied < normalized_request.minimum_protected_long_qty:
            errors.append(
                "insufficient protected long quota: "
                f"need at least {normalized_request.minimum_protected_long_qty}, "
                f"have {normalized_snapshot.available_protected_long}"
            )
        if protected_short_applied < normalized_request.minimum_protected_short_qty:
            errors.append(
                "insufficient protected short quota: "
                f"need at least {normalized_request.minimum_protected_short_qty}, "
                f"have {normalized_snapshot.available_protected_short}"
            )

        unprotected_long_quantity = max(
            normalized_request.preferred_protected_long_qty - protected_long_applied,
            ZERO,
        )
        unprotected_short_quantity = max(
            normalized_request.preferred_protected_short_qty - protected_short_applied,
            ZERO,
        )

        if unprotected_long_quantity > 0 and normalized_request.minimum_protected_long_qty == 0:
            warnings.append(f"unprotected long remainder: {unprotected_long_quantity}")
        if unprotected_short_quantity > 0 and normalized_request.minimum_protected_short_qty == 0:
            warnings.append(f"unprotected short remainder: {unprotected_short_quantity}")

        if errors:
            return QuotaDecision(
                allowed=False,
                request=normalized_request,
                reason="quota_rejected",
                updated_snapshot=normalized_snapshot,
                errors=tuple(errors),
                warnings=tuple(warnings),
                protected_long_applied=protected_long_applied,
                protected_short_applied=protected_short_applied,
                unprotected_long_quantity=unprotected_long_quantity,
                unprotected_short_quantity=unprotected_short_quantity,
            )

        allocations = self._build_allocations(
            normalized_request=normalized_request,
            protected_long_applied=protected_long_applied,
            protected_short_applied=protected_short_applied,
        )
        updated_snapshot = self.apply_allocations(normalized_snapshot, allocations)
        return QuotaDecision(
            allowed=True,
            request=normalized_request,
            reason="quota_reserved",
            updated_snapshot=updated_snapshot,
            allocations=allocations,
            warnings=tuple(warnings),
            protected_long_applied=protected_long_applied,
            protected_short_applied=protected_short_applied,
            unprotected_long_quantity=unprotected_long_quantity,
            unprotected_short_quantity=unprotected_short_quantity,
        )

    def apply_allocations(
        self,
        snapshot: QuotaSnapshot,
        allocations: tuple[QuotaAllocation, ...],
    ) -> QuotaSnapshot:
        updated = snapshot.normalized()
        for allocation in allocations:
            quantity = max(allocation.quantity, ZERO)
            if quantity == 0:
                continue
            if allocation.source_type == "long_direction":
                updated = replace(updated, long_limit_used=updated.long_limit_used + quantity)
            elif allocation.source_type == "short_direction":
                updated = replace(updated, short_limit_used=updated.short_limit_used + quantity)
            elif allocation.source_type == "spot_inventory":
                updated = replace(updated, spot_inventory_reserved=updated.spot_inventory_reserved + quantity)
            elif allocation.source_type == "covered_call":
                updated = replace(updated, covered_call_quota_used=updated.covered_call_quota_used + quantity)
            elif allocation.source_type == "cash_secured_put":
                updated = replace(
                    updated,
                    cash_secured_put_quota_used=updated.cash_secured_put_quota_used + quantity,
                )
            elif allocation.source_type == "protected_long":
                updated = replace(updated, protected_long_quota_used=updated.protected_long_quota_used + quantity)
            elif allocation.source_type == "protected_short":
                updated = replace(updated, protected_short_quota_used=updated.protected_short_quota_used + quantity)
        return updated.normalized()

    def release_allocations(
        self,
        snapshot: QuotaSnapshot,
        allocations: tuple[QuotaAllocation, ...],
    ) -> QuotaSnapshot:
        updated = snapshot.normalized()
        for allocation in allocations:
            quantity = max(allocation.quantity, ZERO)
            if quantity == 0:
                continue
            if allocation.source_type == "long_direction":
                updated = replace(updated, long_limit_used=max(updated.long_limit_used - quantity, ZERO))
            elif allocation.source_type == "short_direction":
                updated = replace(updated, short_limit_used=max(updated.short_limit_used - quantity, ZERO))
            elif allocation.source_type == "spot_inventory":
                updated = replace(
                    updated,
                    spot_inventory_reserved=max(updated.spot_inventory_reserved - quantity, ZERO),
                )
            elif allocation.source_type == "covered_call":
                updated = replace(
                    updated,
                    covered_call_quota_used=max(updated.covered_call_quota_used - quantity, ZERO),
                )
            elif allocation.source_type == "cash_secured_put":
                updated = replace(
                    updated,
                    cash_secured_put_quota_used=max(updated.cash_secured_put_quota_used - quantity, ZERO),
                )
            elif allocation.source_type == "protected_long":
                updated = replace(
                    updated,
                    protected_long_quota_used=max(updated.protected_long_quota_used - quantity, ZERO),
                )
            elif allocation.source_type == "protected_short":
                updated = replace(
                    updated,
                    protected_short_quota_used=max(updated.protected_short_quota_used - quantity, ZERO),
                )
        return updated.normalized()

    def _build_allocations(
        self,
        *,
        normalized_request: QuotaRequest,
        protected_long_applied: Decimal,
        protected_short_applied: Decimal,
    ) -> tuple[QuotaAllocation, ...]:
        allocations: list[QuotaAllocation] = []
        if normalized_request.required_long_direction_qty > 0:
            allocations.append(
                QuotaAllocation(
                    source_type="long_direction",
                    quantity=normalized_request.required_long_direction_qty,
                )
            )
        if normalized_request.required_short_direction_qty > 0:
            allocations.append(
                QuotaAllocation(
                    source_type="short_direction",
                    quantity=normalized_request.required_short_direction_qty,
                )
            )
        if normalized_request.required_spot_inventory_qty > 0:
            allocations.append(
                QuotaAllocation(
                    source_type="spot_inventory",
                    quantity=normalized_request.required_spot_inventory_qty,
                )
            )
        if normalized_request.required_covered_call_qty > 0:
            allocations.append(
                QuotaAllocation(
                    source_type="covered_call",
                    quantity=normalized_request.required_covered_call_qty,
                )
            )
        if normalized_request.required_cash_secured_put_qty > 0:
            allocations.append(
                QuotaAllocation(
                    source_type="cash_secured_put",
                    quantity=normalized_request.required_cash_secured_put_qty,
                )
            )
        if protected_long_applied > 0:
            allocations.append(
                QuotaAllocation(
                    source_type="protected_long",
                    quantity=protected_long_applied,
                )
            )
        if protected_short_applied > 0:
            allocations.append(
                QuotaAllocation(
                    source_type="protected_short",
                    quantity=protected_short_applied,
                )
            )
        return tuple(allocations)
