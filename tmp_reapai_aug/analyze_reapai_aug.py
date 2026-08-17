"""分析 ReapAI 账户 2026-08 交易：已平仓汇总、分币种、分策略、当前持仓。"""

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


def fmt(x: Decimal, n: int = 2) -> str:
    return f"{x:,.{n}f}"


def main() -> None:
    data = json.loads(RAW.read_text(encoding="utf-8"))
    pos_records = data["positions_history_swap"]
    fills = data["fills_august"]
    orders = data["orders_august"]
    current = data["current_positions"]

    print("=" * 72)
    print("1) 已平仓仓位（按平仓时间 uTime 落在 2026-08，北京时间）")
    print("=" * 72)

    aug_positions = []
    for r in pos_records:
        utime = int(r.get("uTime") or 0)
        if utime >= AUG_BEGIN_MS:
            aug_positions.append(r)
    aug_positions.sort(key=lambda r: int(r.get("uTime") or 0))

    if not aug_positions:
        print("(positions-history 里没有 8 月平仓记录，检查时间范围)")
    if pos_records:
        t0 = datetime.fromtimestamp(int(pos_records[-1].get("uTime") or 0) / 1000, BJT)
        t1 = datetime.fromtimestamp(int(pos_records[0].get("uTime") or 0) / 1000, BJT)
        print(f"(positions-history 覆盖范围: {t0} ~ {t1}，共 {len(pos_records)} 条)")

    total_pnl = total_fee = total_funding = total_realized = Decimal("0")
    by_symbol: dict[str, dict] = defaultdict(lambda: {"n": 0, "pnl": Decimal("0"), "fee": Decimal("0"), "fund": Decimal("0"), "realized": Decimal("0"), "wins": 0})
    rows = []
    for r in aug_positions:
        pnl = d(r.get("pnl"))
        fee = d(r.get("fee"))
        fund = d(r.get("fundingFee"))
        realized = d(r.get("realizedPnl"))
        # OKX 的 realizedPnl 通常已含 fee+funding；校验一下
        check = pnl + fee + fund
        if abs(realized - check) > Decimal("0.01"):
            realized = check  # 以分项加总为准，避免口径不一致
        inst = r.get("instId", "")
        total_pnl += pnl
        total_fee += fee
        total_funding += fund
        total_realized += realized
        s = by_symbol[inst]
        s["n"] += 1
        s["pnl"] += pnl
        s["fee"] += fee
        s["fund"] += fund
        s["realized"] += realized
        if realized > 0:
            s["wins"] += 1
        rows.append({
            "close_time": datetime.fromtimestamp(int(r.get("uTime") or 0) / 1000, BJT).strftime("%m-%d %H:%M"),
            "open_time": datetime.fromtimestamp(int(r.get("cTime") or 0) / 1000, BJT).strftime("%m-%d %H:%M"),
            "inst": inst,
            "dir": r.get("direction") or r.get("posSide"),
            "size": r.get("closeTotalPos") or r.get("closePos"),
            "open_px": r.get("openAvgPx"),
            "close_px": r.get("closeAvgPx"),
            "lever": r.get("lever"),
            "pnl": pnl, "fee": fee, "fund": fund, "realized": realized,
        })

    print(f"\n平仓笔数: {len(aug_positions)}")
    print(f"交易毛损益 pnl     : {fmt(total_pnl)} USDT")
    print(f"手续费 fee          : {fmt(total_fee)} USDT")
    print(f"资金费 funding      : {fmt(total_funding)} USDT")
    print(f"已实现净损益        : {fmt(total_realized)} USDT")
    wins = sum(1 for x in rows if x["realized"] > 0)
    print(f"胜/负: {wins}/{len(rows) - wins}  胜率: {wins / len(rows) * 100:.1f}%" if rows else "")

    print("\n-- 逐笔明细 --")
    print(f"{'平仓时间':<12} {'品种':<15} {'方向':<6} {'数量':>10} {'开仓价':>10} {'平仓价':>10} {'毛损益':>9} {'净损益':>9}")
    for x in rows:
        print(f"{x['close_time']:<12} {x['inst']:<15} {str(x['dir']):<6} {str(x['size']):>10} {str(x['open_px']):>10} {str(x['close_px']):>10} {fmt(x['pnl']):>9} {fmt(x['realized']):>9}")

    print("\n-- 分币种汇总 --")
    print(f"{'品种':<15} {'笔数':>4} {'胜率':>7} {'毛损益':>10} {'手续费':>9} {'资金费':>8} {'净损益':>10}")
    for inst in sorted(by_symbol, key=lambda k: by_symbol[k]["realized"]):
        s = by_symbol[inst]
        print(f"{inst:<15} {s['n']:>4} {s['wins']/s['n']*100:>6.0f}% {fmt(s['pnl']):>10} {fmt(s['fee']):>9} {fmt(s['fund']):>8} {fmt(s['realized']):>10}")

    print()
    print("=" * 72)
    print("2) 委托与策略归因（按 clOrdId 前缀）")
    print("=" * 72)
    prefix_stat: dict[str, int] = defaultdict(int)
    for o in orders:
        cl = (o.get("clOrdId") or "").strip()
        prefix = cl[:6] if cl else "(空)"
        prefix_stat[prefix] += 1
    for p, n in sorted(prefix_stat.items(), key=lambda kv: -kv[1]):
        print(f"  {p:<10} {n} 条")

    print()
    print("=" * 72)
    print("3) 8 月成交统计（fills）")
    print("=" * 72)
    fill_fee = Decimal("0")
    fill_pnl = Decimal("0")
    by_symbol_fill: dict[str, int] = defaultdict(int)
    for f in fills:
        fill_fee += d(f.get("fee"))
        fill_pnl += d(f.get("fillPnl"))
        by_symbol_fill[f.get("instId", "")] += 1
    print(f"成交笔数: {len(fills)}")
    print(f"成交口径手续费合计: {fmt(fill_fee)} USDT")
    print(f"成交口径 fillPnl 合计: {fmt(fill_pnl)} USDT")
    for inst, n in sorted(by_symbol_fill.items(), key=lambda kv: -kv[1]):
        print(f"  {inst:<15} {n} 笔")

    print()
    print("=" * 72)
    print("4) 当前未平仓位")
    print("=" * 72)
    for p in current:
        pos_amt = p.get("pos")
        if pos_amt in (None, "", "0"):
            continue
        open_dt = datetime.fromtimestamp(int(p.get("cTime") or 0) / 1000, BJT)
        print(f"  {p.get('instId')} {p.get('posSide')} 数量={pos_amt} 开仓均价={p.get('avgPx')} 标记价={p.get('markPx')} 未实现盈亏={p.get('upl')} 杠杆={p.get('lever')} 开仓时间={open_dt}")

    print()
    print("=" * 72)
    print("5) 账户快照")
    print("=" * 72)
    bal = data["balance"]
    if bal:
        det = bal[0].get("details", [{}])[0]
        print(f"  总权益 eq     : {det.get('eq')} USDT")
        print(f"  可用权益      : {det.get('availEq')} USDT")
        print(f"  未实现盈亏 upl: {det.get('upl')} USDT")


if __name__ == "__main__":
    main()
