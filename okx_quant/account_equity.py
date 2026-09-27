"""Hourly snapshots of the trading account, shared with the existing curve file."""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from pathlib import Path

from okx_quant.daily_trade_report import _datetime, _decimal
from okx_quant.persistence import load_account_equity_curve_records, save_account_equity_curve_records

_SAMPLE_LOCK = threading.Lock()


def record_account_equity(profile_name: str, environment: str, account: object, *,
                          sampled_at: datetime | None = None, base_dir: Path | None = None) -> bool:
    total = _decimal(getattr(account, "total_equity", None))
    if not profile_name or environment not in {"live", "demo"} or total is None or not total.is_finite():
        return False
    now = sampled_at or datetime.now(timezone.utc)
    now = now.replace(tzinfo=timezone.utc) if now.tzinfo is None else now.astimezone(timezone.utc)
    hour = now.replace(minute=0, second=0, microsecond=0)

    def value(item, name):
        number = _decimal(getattr(item, name, None))
        return str(number) if number is not None and number.is_finite() else None

    with _SAMPLE_LOCK:
        records = load_account_equity_curve_records(profile_name, environment, base_dir=base_dir)
        for record in reversed(records):
            stamp = _datetime(record.get("time"))
            if stamp is not None and stamp.astimezone(timezone.utc) >= hour:
                return False
        records.append({
            "time": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
            "total_equity": str(total), "adjusted_equity": value(account, "adjusted_equity"),
            "available_equity": value(account, "available_equity"), "upl": value(account, "unrealized_pnl"),
            "valuation_currency": "USD", "scope": "trading_account",
            "assets": [{"ccy": str(asset.ccy), "equity": value(asset, "equity"),
                        "equity_usd": value(asset, "equity_usd")}
                       for asset in getattr(account, "details", ())],
        })
        save_account_equity_curve_records(profile_name, environment, records, base_dir=base_dir)
    return True
