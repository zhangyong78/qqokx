from __future__ import annotations

from okx_quant.arbitrage.models import ArbitrageLedgerEntry
from okx_quant.persistence import load_arbitrage_ledger_snapshot, save_arbitrage_ledger_snapshot


def load_ledger_entries() -> list[ArbitrageLedgerEntry]:
    payload = load_arbitrage_ledger_snapshot()
    rows = payload.get("entries")
    if not isinstance(rows, list):
        return []
    entries: list[ArbitrageLedgerEntry] = []
    for item in rows:
        if isinstance(item, dict):
            entries.append(ArbitrageLedgerEntry.from_dict(item))
    return entries


def save_ledger_entries(entries: list[ArbitrageLedgerEntry]) -> None:
    save_arbitrage_ledger_snapshot(entries=[item.to_dict() for item in entries])


def load_open_ledger_entries() -> list[ArbitrageLedgerEntry]:
    return [item for item in load_ledger_entries() if item.close_mode == "open"]


def find_ledger_entry(entry_id: str) -> ArbitrageLedgerEntry | None:
    for item in load_ledger_entries():
        if item.entry_id == entry_id:
            return item
    return None


def upsert_ledger_entry(updated: ArbitrageLedgerEntry) -> None:
    entries = load_ledger_entries()
    replaced = False
    for index, item in enumerate(entries):
        if item.entry_id == updated.entry_id:
            entries[index] = updated
            replaced = True
            break
    if not replaced:
        entries.insert(0, updated)
    save_ledger_entries(entries)
