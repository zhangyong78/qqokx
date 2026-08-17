"""按仓位方向切分 8 月净损益：多头（EMA 动态多头）vs 空头（斜率空头）。"""

from __future__ import annotations

import json
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

    for direction in ("long", "short"):
        recs = [r for r in aug if (r.get("direction") or r.get("posSide")) == direction]
        pnl = sum(d(r.get("pnl")) for r in recs)
        fee = sum(d(r.get("fee")) for r in recs)
        fund = sum(d(r.get("fundingFee")) for r in recs)
        realized = pnl + fee + fund
        wins = sum(1 for r in recs if (d(r.get("pnl")) + d(r.get("fee")) + d(r.get("fundingFee"))) > 0)
        print(f"{direction:<6} 笔数={len(recs):>2} 胜率={wins/len(recs)*100:.0f}% 毛损益={pnl:>9.2f} 手续费={fee:>7.2f} 资金费={fund:>6.2f} 净损益={realized:>9.2f}")

    # 权益对账：8月已实现合计与曲线变动
    total_realized = sum(d(r.get("pnl")) + d(r.get("fee")) + d(r.get("fundingFee")) for r in aug)
    total_fee = sum(d(r.get("fee")) for r in aug)
    print(f"\n8月已实现合计={total_realized:.2f}, 其中手续费={total_fee:.2f}")
    print(f"权益曲线: 2557.48 (8/2 09:28) -> 2414.91 (8/15 12:41), 变动 -142.57")


if __name__ == "__main__":
    main()
