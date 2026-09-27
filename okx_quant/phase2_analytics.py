"""Cash-flow and spot cost-basis calculations for the second report phase."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Iterable, Mapping

from okx_quant.daily_trade_report import REPORT_TIMEZONE, _decimal, _datetime
from okx_quant.okx_client import OkxAccountBillItem, OkxAssetBillItem, OkxFillHistoryItem
from roll_terminal_qt.bill_history_service import asset_bill_category, bill_amount, bill_category

ZERO = Decimal("0")


@dataclass(frozen=True)
class SpotRealized:
    close_time: datetime
    base_currency: str
    quote_currency: str
    quantity: Decimal
    realized_pnl: Decimal | None
    fee: Decimal | None
    valuation_note: str = ""


@dataclass(frozen=True)
class CashFlowSummary:
    deposits: dict[str, Decimal]
    withdrawals: dict[str, Decimal]
    internal_transfers: dict[str, Decimal]
    unknown: dict[str, Decimal]
    fees: dict[str, Decimal]
    row_count: int

    @staticmethod
    def _sum(mapping: dict[str, Decimal]) -> Decimal:
        return sum(mapping.values(), ZERO)

    @property
    def external_net(self) -> dict[str, Decimal]:
        keys = set(self.deposits) | set(self.withdrawals)
        return {key: self.deposits.get(key, ZERO) - self.withdrawals.get(key, ZERO) for key in keys}


def _pair(inst_id: str) -> tuple[str, str] | None:
    parts = str(inst_id or "").upper().split("-")
    if len(parts) != 2:
        return None
    return parts[0], parts[1]


def _price(currency: str, prices: Mapping[str, object]) -> Decimal | None:
    if currency == "USDT":
        return Decimal("1")
    value = _decimal(prices.get(currency))
    return value if value is not None and value > 0 else None


def spot_cost_basis(fills: Iterable[OkxFillHistoryItem], *, usdt_prices: Mapping[str, object] | None = None) -> tuple[SpotRealized, ...]:
    """FIFO spot realized PnL. Unknown quote conversion remains explicitly unvalued."""
    prices = usdt_prices or {}
    lots: dict[str, list[list[Decimal]]] = {}
    realized: list[SpotRealized] = []
    rows = sorted((item for item in fills if str(item.inst_type or "").upper() == "SPOT"), key=lambda item: item.fill_time or 0)
    for fill in rows:
        pair = _pair(fill.inst_id)
        quantity, price = _decimal(fill.fill_size), _decimal(fill.fill_price)
        stamp = _datetime(fill.fill_time)
        if pair is None or quantity is None or quantity <= 0 or price is None or stamp is None:
            continue
        base, quote = pair
        fee = _decimal(fill.fill_fee)
        fee_ccy = str(fill.fee_currency or "").upper()
        if fill.side == "buy":
            acquired = quantity - fee if fee_ccy == base and fee is not None and fee > 0 else quantity
            cost = price * quantity + (fee if fee_ccy == quote and fee is not None else ZERO)
            lots.setdefault(base, []).append([acquired, cost])
            continue
        remaining = quantity
        cost = ZERO
        bucket = lots.setdefault(base, [])
        while remaining > 0 and bucket:
            lot_qty, lot_cost = bucket[0]
            original_qty = lot_qty
            used = min(remaining, lot_qty)
            cost += lot_cost * used / lot_qty if lot_qty else ZERO
            lot_qty -= used
            remaining -= used
            if lot_qty <= 0:
                bucket.pop(0)
            else:
                bucket[0] = [lot_qty, lot_cost * lot_qty / original_qty]
        proceeds = price * quantity - (fee if fee_ccy == quote and fee is not None else ZERO)
        if remaining > 0:
            note = "库存不足，未完整计算 FIFO 成本"
            value = None
        else:
            note = ""
            value = proceeds - cost
        rate = _price(quote, prices)
        if value is not None and rate is not None:
            value *= rate
        elif value is not None and quote != "USDT":
            value = None
            note = f"缺少 {quote} 汇率，未计入 USDT 合计"
        realized.append(SpotRealized(stamp, base, quote, quantity, value, fee, note))
    return tuple(realized)


def cash_flow_summary(account_bills: Iterable[OkxAccountBillItem], asset_bills: Iterable[OkxAssetBillItem]) -> CashFlowSummary:
    deposits: dict[str, Decimal] = {}
    withdrawals: dict[str, Decimal] = {}
    internal: dict[str, Decimal] = {}
    unknown: dict[str, Decimal] = {}
    fees: dict[str, Decimal] = {}
    count = 0
    for item in asset_bills:
        amount = _decimal(item.amount)
        if amount is None:
            continue
        count += 1
        category = asset_bill_category(item)
        currency = str(item.currency or "?").upper()
        if category == "充值":
            deposits[currency] = deposits.get(currency, ZERO) + abs(amount)
        elif category == "提现":
            withdrawals[currency] = withdrawals.get(currency, ZERO) + abs(amount)
        elif category == "内部划转":
            internal[currency] = internal.get(currency, ZERO) + amount
        else:
            unknown[currency] = unknown.get(currency, ZERO) + amount
    # Account bills can contain trading-account transfers not present in the
    # funding-account endpoint. Keep them separate from external cash flow.
    for item in account_bills:
        amount = bill_amount(item)
        if amount is not None and bill_category(item) == "内部划转":
            currency = str(item.currency or "?").upper()
            internal[currency] = internal.get(currency, ZERO) + amount
            count += 1
        elif amount is not None and bill_category(item) == "费用资金费":
            currency = str(item.currency or "?").upper()
            fees[currency] = fees.get(currency, ZERO) + amount
            count += 1
    return CashFlowSummary(deposits, withdrawals, internal, unknown, fees, count)


def spot_pnl_by_day(records: Iterable[SpotRealized]) -> dict[date, Decimal | None]:
    result: dict[date, Decimal | None] = {}
    for row in records:
        day = row.close_time.astimezone(REPORT_TIMEZONE).date()
        result.setdefault(day, None)
        if row.realized_pnl is not None:
            result[day] = (result[day] or ZERO) + row.realized_pnl
    return result
