from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal

from okx_quant.models import Credentials, EnvironmentMode, PositionMode, TradeMode

ArbitragePairKind = Literal["spot_swap", "spot_quarter", "spot_next_quarter", "spot_future"]
ArbitrageDirection = Literal["cash_and_carry", "reverse_cash_and_carry"]
SizeUnit = Literal["coin", "usdt", "contracts"]
LedgerCloseMode = Literal["full", "partial", "open"]
ArbitrageTriggerMode = Literal["spread", "spread_abs", "limit_price"]


@dataclass(frozen=True)
class ArbitrageFeeProfile:
    spot_maker: Decimal = Decimal("0.00015")
    spot_taker: Decimal = Decimal("0.00036")
    swap_maker: Decimal = Decimal("0.00060")
    swap_taker: Decimal = Decimal("0.00070")


@dataclass(frozen=True)
class ArbitrageRuntimeConfig:
    environment: EnvironmentMode = "live"
    max_slippage: Decimal = Decimal("0.0015")
    fee_profile: ArbitrageFeeProfile = ArbitrageFeeProfile()
    scan_reference_notional_usdt: Decimal = Decimal("1000")
    funding_intervals_per_day: Decimal = Decimal("3")


@dataclass(frozen=True)
class ArbitrageTradeRuntime:
    credentials: Credentials
    environment: EnvironmentMode
    trade_mode: TradeMode
    position_mode: PositionMode
    credential_profile_name: str = ""


@dataclass(frozen=True)
class ArbitrageOpportunity:
    base_ccy: str
    pair_kind: ArbitragePairKind
    pair_kind_label: str
    spot_inst_id: str
    derivative_inst_id: str
    spot_mid: Decimal
    derivative_mid: Decimal
    basis_abs: Decimal
    basis_pct: Decimal
    funding_rate: Decimal | None
    funding_annual_pct: Decimal | None
    fee_round_trip_pct: Decimal
    slippage_est_pct: Decimal
    net_annual_pct: Decimal
    days_to_expiry: int | None
    scanned_at: datetime

    def sort_key(self) -> Decimal:
        return self.net_annual_pct


@dataclass(frozen=True)
class ArbitrageSizePreview:
    spot_base_qty: Decimal
    swap_contracts: Decimal
    notional_usdt: Decimal


@dataclass(frozen=True)
class ArbitrageLedgerEntry:
    entry_id: str
    base_ccy: str
    pair_kind: ArbitragePairKind
    spot_inst_id: str
    derivative_inst_id: str
    spot_qty: Decimal
    derivative_qty: Decimal
    open_spot_price: Decimal | None
    open_derivative_price: Decimal | None
    close_spot_price: Decimal | None
    close_derivative_price: Decimal | None
    basis_at_open_pct: Decimal | None
    fee_total: Decimal
    funding_total: Decimal
    realized_pnl: Decimal | None
    close_mode: LedgerCloseMode
    opened_at: str
    closed_at: str | None
    notes: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "entry_id": self.entry_id,
            "base_ccy": self.base_ccy,
            "pair_kind": self.pair_kind,
            "spot_inst_id": self.spot_inst_id,
            "derivative_inst_id": self.derivative_inst_id,
            "spot_qty": str(self.spot_qty),
            "derivative_qty": str(self.derivative_qty),
            "open_spot_price": (str(self.open_spot_price) if self.open_spot_price is not None else None),
            "open_derivative_price": (str(self.open_derivative_price) if self.open_derivative_price is not None else None),
            "close_spot_price": (str(self.close_spot_price) if self.close_spot_price is not None else None),
            "close_derivative_price": (
                str(self.close_derivative_price) if self.close_derivative_price is not None else None
            ),
            "basis_at_open_pct": (str(self.basis_at_open_pct) if self.basis_at_open_pct is not None else None),
            "fee_total": str(self.fee_total),
            "funding_total": str(self.funding_total),
            "realized_pnl": (str(self.realized_pnl) if self.realized_pnl is not None else None),
            "close_mode": self.close_mode,
            "opened_at": self.opened_at,
            "closed_at": self.closed_at,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> ArbitrageLedgerEntry:
        def _dec(key: str) -> Decimal | None:
            raw = payload.get(key)
            if raw in {None, ""}:
                return None
            return Decimal(str(raw))

        return cls(
            entry_id=str(payload.get("entry_id", "")),
            base_ccy=str(payload.get("base_ccy", "")),
            pair_kind=str(payload.get("pair_kind", "spot_swap")),  # type: ignore[arg-type]
            spot_inst_id=str(payload.get("spot_inst_id", "")),
            derivative_inst_id=str(payload.get("derivative_inst_id", "")),
            spot_qty=Decimal(str(payload.get("spot_qty", "0"))),
            derivative_qty=Decimal(str(payload.get("derivative_qty", "0"))),
            open_spot_price=_dec("open_spot_price"),
            open_derivative_price=_dec("open_derivative_price"),
            close_spot_price=_dec("close_spot_price"),
            close_derivative_price=_dec("close_derivative_price"),
            basis_at_open_pct=_dec("basis_at_open_pct"),
            fee_total=Decimal(str(payload.get("fee_total", "0"))),
            funding_total=Decimal(str(payload.get("funding_total", "0"))),
            realized_pnl=_dec("realized_pnl"),
            close_mode=str(payload.get("close_mode", "open")),  # type: ignore[arg-type]
            opened_at=str(payload.get("opened_at", "")),
            closed_at=(str(payload["closed_at"]) if payload.get("closed_at") is not None else None),
            notes=str(payload.get("notes", "") or ""),
        )
