"""校验：列出全部 8 月平仓记录（带序号）、分日净损益合计、归因口径合计。"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path

BJT = timezone(timedelta(hours=8))
RAW = Path(__file__).resolve().parent / "reapai_august_raw.json"
AUG_BEGIN_MS = int(datetime(2026, 8, 1, 0, 0, tzinfo=BJT).timestamp() * 1000)


def d(value) -> Decimal:
    try:
        return Decimal(str(value)) if value not in (None, "") else Decimal("0")
    except Exception:
        return Decimal("0")


def main() -> None:
    data = json.loads(RAW.read_text(encoding="utf-8"))
    pos_records = data["positions_history_swap"]
    aug = [r for r in pos_records if int(r.get("uTime") or 0) >= AUG_BEGIN_MS]
    aug.sort(key=lambda r: int(r.get("uTime") or 0))

    print(f"records in Aug: {len(aug)}")
    daily: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    total = Decimal("0")
    for i, r in enumerate(aug, 1):
        utime = int(r.get("uTime") or 0)
        dt = datetime.fromtimestamp(utime / 1000, BJT)
        pnl = d(r.get("pnl"))
        fee = d(r.get("fee"))
        fund = d(r.get("fundingFee"))
        realized = d(r.get("realizedPnl"))
        check = pnl + fee + fund
        if abs(realized - check) > Decimal("0.01"):
            realized = check
        daily[dt.strftime("%m-%d")] += realized
        total += realized
        print(f"{i:>2} {dt.strftime('%m-%d %H:%M')} {r.get('instId'):<15} {str(r.get('direction')):<6} realized={realized:>9.2f}")

    print("\n分日净损益:")
    for day in sorted(daily):
        print(f"  {day}: {daily[day]:>9.2f}")
    print(f"  TOTAL: {total:.2f}")

    # 区间汇总
    def rng(a: str, b: str) -> Decimal:
        return sum(v for k, v in daily.items() if a <= k <= b)
    print(f"8/1-8/3:  {rng('08-01','08-03'):.2f}")
    print(f"8/4-8/7:  {rng('08-04','08-07'):.2f}")
    print(f"8/8-8/11: {rng('08-08','08-11'):.2f}")
    print(f"8/12-8/15:{rng('08-12','08-15'):.2f}")


if __name__ == "__main__":
    main()
