"""诊断：逐笔列出 st 前缀会话的成交，检查 fillPnl 归因是否重复计算。"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path

BJT = timezone(timedelta(hours=8))
RAW = Path(__file__).resolve().parent / "reapai_august_raw.json"


def d(v) -> Decimal:
    try:
        return Decimal(str(v)) if v not in (None, "") else Decimal("0")
    except Exception:
        return Decimal("0")


def main() -> None:
    data = json.loads(RAW.read_text(encoding="utf-8"))
    fills = data["fills_august"]
    orders = data["orders_august"]

    ord_prefix = {}
    for o in orders:
        oid = o.get("ordId")
        cl = (o.get("clOrdId") or "").strip()
        if oid and cl:
            ord_prefix[oid] = cl[:6]

    # 按前缀分组列出每笔 fill
    groups: dict[str, list] = defaultdict(list)
    for f in fills:
        pre = ord_prefix.get(f.get("ordId"), "?")
        groups[pre].append(f)

    total_all = sum(d(f.get("fillPnl")) for f in fills)
    total_fee_all = sum(d(f.get("fee")) for f in fills)
    print(f"全部 fills: {len(fills)} 笔, fillPnl={total_all:.2f}, fee={total_fee_all:.2f}")

    for pre in sorted(groups):
        fs = groups[pre]
        pnl = sum(d(f.get("fillPnl")) for f in fs)
        fee = sum(d(f.get("fee")) for f in fs)
        print(f"\n=== {pre} : {len(fs)} fills, pnl={pnl:.2f}, fee={fee:.2f} ===")
        if not pre.endswith("st"):
            continue
        for f in sorted(fs, key=lambda x: int(x.get("fillTime") or 0)):
            t = datetime.fromtimestamp(int(f.get("fillTime") or 0) / 1000, BJT).strftime("%m-%d %H:%M")
            print(f"  {t} {f.get('instId',''):<15} {f.get('side'):<5} sz={f.get('fillSz')} px={f.get('fillPx')} pnl={d(f.get('fillPnl')):>9.2f} fee={d(f.get('fee')):>7.3f} exec={f.get('execType')}")


if __name__ == "__main__":
    main()
