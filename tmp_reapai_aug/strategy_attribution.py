"""按 clOrdId 前缀（策略会话）归因 8 月成交盈亏。"""

from __future__ import annotations

import json
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

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

    strat: dict[str, dict] = defaultdict(lambda: {"fills": 0, "fee": Decimal("0"), "pnl": Decimal("0"), "symbols": set(), "sides": set()})
    unknown = 0
    for f in fills:
        pre = ord_prefix.get(f.get("ordId"), "")
        if not pre:
            unknown += 1
            continue
        s = strat[pre]
        s["fills"] += 1
        s["fee"] += d(f.get("fee"))
        s["pnl"] += d(f.get("fillPnl"))
        s["symbols"].add((f.get("instId") or "").replace("-USDT-SWAP", ""))
        s["sides"].add(f.get("side"))

    header = f"{'前缀':<8} {'推断类型':<12} {'品种':<10} {'成交数':>5} {'fillPnl':>10} {'手续费':>8}"
    print(header)
    print("-" * len(header))
    total_pnl = Decimal("0")
    total_fee = Decimal("0")
    for pre in sorted(strat):
        s = strat[pre]
        kind = "EMA动态多头" if pre.endswith("em") else ("斜率空头" if pre.endswith("st") else "手动/其他")
        total_pnl += s["pnl"]
        total_fee += s["fee"]
        print(f"{pre:<8} {kind:<12} {','.join(sorted(s['symbols'])):<10} {s['fills']:>5} {s['pnl']:>10.2f} {s['fee']:>8.2f}")
    print("-" * len(header))
    print(f"{'合计':<8} {'':<12} {'':<10} {sum(s['fills'] for s in strat.values()):>5} {total_pnl:>10.2f} {total_fee:>8.2f}")
    print(f"未匹配到订单前缀的成交: {unknown} 笔")


if __name__ == "__main__":
    main()
