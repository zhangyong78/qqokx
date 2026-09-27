"""Cached account bills used by phase-two asset and cash-flow analytics."""
from __future__ import annotations

from dataclasses import asdict
from decimal import Decimal, InvalidOperation

from okx_quant.okx_client import OkxAccountBillItem, OkxAssetBillItem
from okx_quant.persistence import load_history_cache_records, save_history_cache_records
from okx_quant.ui_shell import _merge_history_cache_records


def _decimal(value):
    if value in (None, ""):
        return None


def _first(*values):
    for value in values:
        if value not in (None, ""):
            return value
    return None
    try:
        number = Decimal(str(value))
        return number if number.is_finite() else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def _serialize(item: OkxAccountBillItem) -> dict[str, object]:
    result = {}
    for key, value in asdict(item).items():
        result[key] = str(value) if isinstance(value, Decimal) else value
    return result


def bill_from_cache(record: dict[str, object]) -> OkxAccountBillItem | None:
    bill_id = str(record.get("bill_id") or record.get("billId") or "").strip()
    timestamp = record.get("bill_time", record.get("ts"))
    try:
        bill_time = int(timestamp) if timestamp not in (None, "") else None
    except (TypeError, ValueError):
        bill_time = None
    if not bill_id or bill_time is None:
        return None
    raw = record.get("raw") if isinstance(record.get("raw"), dict) else {}
    return OkxAccountBillItem(
        bill_id=bill_id,
        bill_time=bill_time,
        inst_id=str(record.get("inst_id") or record.get("instId") or "").upper(),
        inst_type=str(record.get("inst_type") or record.get("instType") or "").upper(),
        bill_type=str(record.get("bill_type") or record.get("type") or "") or None,
        bill_sub_type=str(record.get("bill_sub_type") or record.get("subType") or "") or None,
        business_type=str(record.get("business_type") or record.get("bizType") or "") or None,
        event_type=str(record.get("event_type") or record.get("eventType") or "") or None,
        side=str(record.get("side") or "") or None,
        pos_side=str(record.get("pos_side") or record.get("posSide") or "") or None,
        size=_decimal(_first(record.get("size"), record.get("sz"))),
        price=_decimal(_first(record.get("price"), record.get("px"))),
        amount=_decimal(_first(record.get("amount"), record.get("balChg"), record.get("posBalChg"))),
        fee=_decimal(record.get("fee")),
        pnl=_decimal(record.get("pnl")),
        balance_change=_decimal(_first(record.get("balance_change"), record.get("balChg"), record.get("posBalChg"))),
        currency=str(record.get("currency") or record.get("ccy") or "") or None,
        order_id=str(record.get("order_id") or record.get("ordId") or "") or None,
        trade_id=str(record.get("trade_id") or record.get("tradeId") or "") or None,
        client_order_id=str(record.get("client_order_id") or record.get("clOrdId") or "") or None,
        raw=raw,
    )


def load_local_account_bills(profile_name: str, environment: str) -> list[OkxAccountBillItem]:
    records = load_history_cache_records("bills", profile_name, environment)
    items = [item for record in records if isinstance(record, dict) and (item := bill_from_cache(record)) is not None]
    return sorted(items, key=lambda item: item.bill_time or 0, reverse=True)


def merge_account_bills(*, profile_name: str, environment: str,
                        remote_items: list[OkxAccountBillItem], limit: int = 500) -> list[OkxAccountBillItem]:
    records = _merge_history_cache_records(
        local_records=load_history_cache_records("bills", profile_name, environment),
        remote_records=[_serialize(item) for item in remote_items],
        dedup_fields=("bill_id",),
    )
    save_history_cache_records("bills", profile_name, environment, records)
    return load_local_account_bills(profile_name, environment)[:max(20, int(limit))]


def bill_category(item: OkxAccountBillItem) -> str:
    bill_type = str(item.bill_type or "").lower()
    subtype = str(item.bill_sub_type or "").lower()
    text = " ".join(str(value or "").lower() for value in (
        bill_type, subtype, item.business_type, item.event_type, item.raw.get("notes", "")
    ))
    if bill_type in {"1", "transfer"} or subtype in {"11", "12"} or "transfer" in text:
        return "内部划转"
    if any(term in text for term in ("deposit", "withdraw", "充值", "提现")):
        return "资金流"
    if subtype in {"7", "8", "9", "173", "174"} or any(term in text for term in ("fee", "funding", "interest")):
        return "费用资金费"
    if any(term in text for term in ("liquidation", "adl", "强平")):
        return "强平/ADL"
    if bill_type in {"2", "trade"} or item.inst_id:
        return "交易"
    return "其他"


def bill_amount(item: OkxAccountBillItem) -> Decimal | None:
    return item.balance_change if item.balance_change is not None else item.amount


def _serialize_asset(item: OkxAssetBillItem) -> dict[str, object]:
    result = {}
    for key, value in asdict(item).items():
        result[key] = str(value) if isinstance(value, Decimal) else value
    return result


def asset_bill_from_cache(record: dict[str, object]) -> OkxAssetBillItem | None:
    bill_id = str(record.get("bill_id") or record.get("billId") or "").strip()
    timestamp = record.get("bill_time", record.get("ts"))
    try:
        bill_time = int(timestamp) if timestamp not in (None, "") else None
    except (TypeError, ValueError):
        bill_time = None
    if not bill_id or bill_time is None:
        return None
    return OkxAssetBillItem(
        bill_id=bill_id, bill_time=bill_time,
        currency=str(record.get("currency") or record.get("ccy") or "") or None,
        bill_type=str(record.get("bill_type") or record.get("type") or "") or None,
        amount=_decimal(_first(record.get("amount"), record.get("amt"))),
        fee=_decimal(record.get("fee")),
        state=str(record.get("state") or "") or None,
        tx_id=str(record.get("tx_id") or record.get("txId") or "") or None,
        client_id=str(record.get("client_id") or record.get("clientId") or "") or None,
        raw=record.get("raw") if isinstance(record.get("raw"), dict) else {},
    )


def load_local_asset_bills(profile_name: str, environment: str) -> list[OkxAssetBillItem]:
    records = load_history_cache_records("asset_bills", profile_name, environment)
    items = [item for record in records if isinstance(record, dict) and (item := asset_bill_from_cache(record)) is not None]
    return sorted(items, key=lambda item: item.bill_time or 0, reverse=True)


def merge_asset_bills(*, profile_name: str, environment: str,
                      remote_items: list[OkxAssetBillItem], limit: int = 200) -> list[OkxAssetBillItem]:
    records = _merge_history_cache_records(
        local_records=load_history_cache_records("asset_bills", profile_name, environment),
        remote_records=[_serialize_asset(item) for item in remote_items],
        dedup_fields=("bill_id",),
    )
    save_history_cache_records("asset_bills", profile_name, environment, records)
    return load_local_asset_bills(profile_name, environment)[:max(20, int(limit))]


def asset_bill_category(item: OkxAssetBillItem) -> str:
    kind = str(item.bill_type or "").lower()
    if kind in {"1", "deposit", "充值"}:
        return "充值"
    if kind in {"2", "withdraw", "提现"}:
        return "提现"
    if kind in {"3", "transfer", "划转"}:
        return "内部划转"
    return "其他"
