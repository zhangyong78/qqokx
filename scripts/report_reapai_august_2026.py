"""Build the Chinese August review, reconciliation and reviewable artifacts."""
from __future__ import annotations

import hashlib
import html
import json
from decimal import Decimal as D

import pandas as pd

from review_reapai_august_2026 import ROOT, OUT, START, END, HOUR, SYMBOLS, read, save, date, metrics

SIDE = {"long":"多头", "short":"空头"}


def export(df, name):
    df.to_csv(OUT/name,index=False,encoding="utf-8-sig")


def match_trades(live, replay):
    """Conservative time candidates, not asserted strategy-ID matches."""
    candidates = []
    for i,a in live.iterrows():
        for j,b in replay.iterrows():
            if (a.symbol,a.side) != (b.symbol,b.side):
                continue
            # Original short timestamps label the signal candle, while its
            # close execution becomes available one hour later.
            entry_effective = int(b.entry_ts) + (HOUR if b.side == "short" else 0)
            exit_effective = int(b.exit_ts) + (HOUR if b.exit_reason == "slope_turn_positive" else 0)
            entry_gap = abs(int(a.entry_ts)-entry_effective)/HOUR
            exit_gap = abs(int(a.exit_ts)-exit_effective)/HOUR
            if entry_gap <= 1.25 and exit_gap <= 2:
                candidates.append((entry_gap+exit_gap,i,j,entry_gap,exit_gap))
    used_a, used_b, rows = set(),set(),[]
    for _,i,j,eg,xg in sorted(candidates):
        if i in used_a or j in used_b:
            continue
        used_a.add(i); used_b.add(j)
        a,b=live.loc[i],replay.loc[j]
        rows.append(dict(symbol=a.symbol, side=a.side, live_entry=a.entry_time, replay_entry=b.entry_time,
                         live_exit=a.exit_time,replay_exit=b.exit_time, live_net=a.net_pnl,replay_net=b.net_pnl,
                         delta=a.net_pnl-b.net_pnl, entry_gap_hours=eg,exit_gap_hours=xg,
                         entry_price_delta_bps=(a.entry_price/b.entry_price-1)*10000,
                         actual_contracts=a.contracts, replay_base_size=b.base_size,exit_reason=b.exit_reason_cn,
                         lifecycle=a.lifecycle))
    matched=pd.DataFrame(rows)
    export(matched,"trade_match_candidates.csv")
    export(live.loc[~live.index.isin(used_a)],"unmatched_live_trades.csv")
    export(replay.loc[~replay.index.isin(used_b)],"unmatched_replay_trades.csv")
    return matched


def table(df):
    return '<div class="table-wrap">'+df.to_html(index=False,border=0,escape=True,float_format=lambda x:f"{x:,.2f}")+"</div>"


def mdtable(df):
    def f(x):
        return f"{x:,.2f}" if isinstance(x,float) else str(x).replace("|","/")
    return "\n".join(["| "+" | ".join(df.columns)+" |", "| "+" | ".join("---" for _ in df.columns)+" |"]+["| "+" | ".join(f(v) for v in row)+" |" for row in df.itertuples(index=False,name=None)])


def curve_svg(frames):
    w,h,left,right,top,bottom=1050,320,78,24,34,50
    sequences=[]
    for name,df,color in frames:
        x=df.sort_values("exit_ts")
        sequences.append((name,[START]+list(x.exit_ts)+[END],[0.0]+list(x.net_pnl.cumsum())+[x.net_pnl.sum()],color))
    ys=[v for _,_,y,_ in sequences for v in y]
    ymin,ymax=min(ys)-20,max(ys)+20
    px=lambda t:left+(t-START)/(END-START)*(w-left-right)
    py=lambda v:top+(ymax-v)/(ymax-ymin)*(h-top-bottom)
    parts=[f'<svg viewBox="0 0 {w} {h}" role="img" aria-label="实盘与参数包原执行回测的月内已实现累计盈亏曲线" xmlns="http://www.w3.org/2000/svg">']
    for i in range(5):
        v=ymin+(ymax-ymin)*i/4;y=py(v)
        parts.append(f'<path d="M{left} {y}H{w-right}" stroke="#dce4ea"/><text x="{left-10}" y="{y+4}" text-anchor="end" font-size="12" fill="#64748b">{v:.0f} U</text>')
    for day in (1,8,15,22,29):
        x=px(START+(day-1)*24*HOUR)
        parts.append(f'<text x="{x}" y="{h-17}" text-anchor="middle" font-size="12" fill="#64748b">8/{day}</text>')
    for i,(name,xs,ys,color) in enumerate(sequences):
        path=f'M{px(xs[0]):.2f},{py(ys[0]):.2f}'
        for x,y in zip(xs[1:],ys[1:]):
            path+=f'H{px(x):.2f}V{py(y):.2f}'
        parts.append(f'<path d="{path}" stroke="{color}" fill="none" stroke-width="2.5"/><text x="{left+i*310}" y="18" font-size="13" fill="{color}">{html.escape(name)}</text>')
    return "".join(parts)+"</svg>"


def build_report():
    live=pd.read_csv(OUT/"live_trades.csv")
    replay=pd.read_csv(OUT/"backtest_calibrated_original_profile_risk.csv")
    strict=pd.read_csv(OUT/"backtest_next_open_100u.csv")
    bundle=read(OUT/"parameter_bundle.json")
    summary=read(OUT/"live_summary.json")
    summary_bt=read(OUT/"backtest_comparison.json")
    matched=match_trades(live,replay)
    bills=read(OUT/"bills_raw.json")
    fills=read(OUT/"fills_raw.json")
    augfills=[r for r in fills if START <= int(r.get("fillTime") or r["ts"]) < END]
    bills.sort(key=lambda r:(int(r["ts"]),int(r["billId"])))
    opening=D(bills[0]["bal"])-D(bills[0]["balChg"])
    closing=D(bills[-1]["bal"])
    sums=lambda key,rs:sum(D(r.get(key) or "0") for r in rs)
    ledger_gross=sums("fillPnl",augfills)
    ledger_fee=sums("fee",augfills)
    ledger_fund=sums("balChg",[r for r in bills if r["type"]=="8"])
    interest=sums("balChg",[r for r in bills if r["subType"]=="381"])
    ledger_net=ledger_gross+ledger_fee+ledger_fund
    assert abs(closing-opening-ledger_net-interest) < D("0.00000001")
    assert abs(float(ledger_gross)-live.gross_pnl.sum())<1e-8
    assert len(augfills)==len([r for r in bills if r["type"]=="2"])
    assert {r["billId"] for r in augfills} == {r["billId"] for r in bills if r["type"]=="2"}
    assert len({r["billId"] for r in bills}) == len(bills)
    assert len(set(live.lifecycle))==len(live)
    fill_checks=[]
    assigned_fill_ids=[]
    for row in live.itertuples():
        # OKX cTime can follow the first fillTime by a few milliseconds.
        fs=[f for f in fills if f["instId"]==row.symbol and f["posSide"]==row.side and row.entry_ts-1000 <= int(f.get("fillTime") or f["ts"]) <= row.exit_ts]
        assigned_fill_ids.extend(f["billId"] for f in fs)
        fill_checks.append({"lifecycle":row.lifecycle,"fills":len(fs),"gross_residual":float(sums("fillPnl",fs))-row.gross_pnl,
                            "fee_residual":float(sums("fee",fs))-row.fee,
                            "opening_prefixes":sorted({str(f.get("clOrdId",""))[:6] for f in fs if f["side"]==("buy" if row.side=="long" else "sell")})})
    save("position_fill_reconciliation.json",fill_checks)
    assert max(abs(r["gross_residual"]) for r in fill_checks)<1e-8
    assert max(abs(r["fee_residual"]) for r in fill_checks)<1e-8
    assert len(assigned_fill_ids)==len(set(assigned_fill_ids)), "One fill assigned to multiple positions"
    month_end=[]
    for (symbol,side),g in live.groupby(["symbol","side"]):
        last_flat=int(g.exit_ts.max())
        tail=sorted([f for f in augfills if f["instId"]==symbol and f["posSide"]==side and int(f.get("fillTime") or f["ts"])>last_flat],key=lambda f:int(f.get("fillTime") or f["ts"]))
        qty,avg=D("0"),D("0")
        for f in tail:
            q,p=D(f["fillSz"]),D(f["fillPx"])
            if f["side"]==("buy" if side=="long" else "sell"):
                avg=(avg*qty+p*q)/(qty+q)
                qty+=q
            else:
                qty-=q
                assert qty>=0
        if qty:
            inst=read(OUT/f"instrument_{symbol}.json")[0]
            close=D(read(OUT/f"candles_{symbol}.json")[-1][4])
            upl=(close-avg)*qty*D(inst["ctVal"])*(1 if side=="long" else -1)
            month_end.append({"币对":symbol,"方向":SIDE[side],"月末未平张数":float(qty),"开仓均价":float(avg),"月末1H收盘价":float(close),"按收盘价估算浮盈U":float(upl)})
    export(pd.DataFrame(month_end),"live_month_end_positions.csv")
    peakcum=live.sort_values("exit_ts").net_pnl.cumsum()
    trough_idx=peakcum.idxmin()
    top=live.nlargest(6,"net_pnl")
    top1=top.net_pnl.iloc[0]
    bystrategy=[]
    params=[]
    for p in bundle["profiles"]:
        c=p["config_snapshot"];symbol=p["symbol"];side="long" if c["signal_mode"]=="long_only" else "short"
        a=live[(live.symbol==symbol)&(live.side==side)]
        b=replay[(replay.symbol==symbol)&(replay.side==side)]
        bystrategy.append({"币种":symbol.split("-")[0],"方向":SIDE[side],"实盘笔数":len(a),"回测笔数":len(b),"实盘净损益U":a.net_pnl.sum(),"原执行/包内风险回测U":b.net_pnl.sum(),"实盘-回测U":a.net_pnl.sum()-b.net_pnl.sum(),"包内参考风险U":float(c["risk_amount"])})
        params.append({"币种":symbol.split("-")[0],"方向":SIDE[side],"快线":f'{c["ema_type"].upper()}{c["ema_period"]}',"趋势线":f'{c["trend_ema_type"].upper()}{c["trend_ema_period"]}',"ATR周期":c["atr_period"],"止损ATR倍数":float(c["atr_stop_multiplier"]),"斜率过滤":str(c["trend_ema_slope_filter_enabled"]),"斜率阈值":c["trend_ema_slope_filter_min_ratio"],"包内参考风险U":float(c["risk_amount"])})
    strategies=pd.DataFrame(bystrategy)
    params=pd.DataFrame(params)
    coins=[]
    for symbol in SYMBOLS:
        d=summary["symbols"][symbol]
        coins.append({"币种":symbol.split("-")[0],"平仓笔数":d["trades"],"胜率%":d["win_rate"],"多头净损益U":summary["symbol_sides"][symbol+" long"]["net_pnl"],"空头净损益U":summary["symbol_sides"][symbol+" short"]["net_pnl"],"合计净损益U":d["net_pnl"]})
    coins=pd.DataFrame(coins)
    variant_names={"original_100u":"未校准原引擎 · 每笔100U", "original_profile_risk":"未校准原引擎 · 包内参考风险", "calibrated_original_100u":"合约单位校准/原执行 · 每笔100U", "calibrated_original_profile_risk":"合约单位校准/原执行 · 包内参考风险", "next_open_100u":"单位校准/下一根开盘 · 每笔100U", "next_open_profile_risk":"单位校准/下一根开盘 · 包内参考风险"}
    comparisons=[{"结果口径":"ReapAI实盘 · 实际仓位","平仓笔数":100,"净损益U":summary["live"]["net_pnl"],"胜率%":26.0,"最大已实现回撤U":summary["live"]["max_realized_drawdown"]}]
    for mode,s in summary_bt.items():
        comparisons.append({"结果口径":variant_names[mode],"平仓笔数":s["trades"],"净损益U":s["net_pnl"],"胜率%":s["win_rate"],"最大已实现回撤U":s["max_realized_drawdown"]})
    comparisons=pd.DataFrame(comparisons)
    weeks=[]
    for begin,end in ((1,8),(8,15),(15,22),(22,29),(29,32)):
        x=live[(live.exit_ts>=START+(begin-1)*24*HOUR)&(live.exit_ts<min(END,START+(end-1)*24*HOUR))]
        weeks.append({"区间":f"8/{begin}—8/{end-1}","平仓笔数":len(x),"多头净损益U":x[x.side=="long"].net_pnl.sum(),"空头净损益U":x[x.side=="short"].net_pnl.sum(),"合计净损益U":x.net_pnl.sum()})
    weeks=pd.DataFrame(weeks)
    top_display=top[["symbol","side","entry_time","exit_time","net_pnl","holding_hours"]].copy()
    top_display["side"]=top_display.side.map(SIDE)
    top_display.columns=["币对","方向","开仓时间","平仓时间","净损益U","持仓小时"]
    side_bt=[]
    for mode in ("calibrated_original_100u","next_open_100u"):
        sm=read(OUT/f"summary_{mode}.json")
        for side in ("long","short"):
            s=sm["directions"][side]
            side_bt.append({"回测口径":variant_names[mode],"方向":SIDE[side],"笔数":s["trades"],"净损益U":s["net_pnl"],"胜率%":s["win_rate"],"最大已实现回撤U":s["max_realized_drawdown"]})
    side_bt=pd.DataFrame(side_bt)
    reconcile=pd.DataFrame([
        {"账单项目":"8月平仓价格毛损益","金额U":float(ledger_gross)},
        {"账单项目":"8月实际扣除的手续费","金额U":float(ledger_fee)},
        {"账单项目":"8月实际结算的资金费","金额U":float(ledger_fund)},
        {"账单项目":"8月交易相关净变动","金额U":float(ledger_net)},
        {"账单项目":"自动赚币收益（381）","金额U":float(interest)},
        {"账单项目":"账户现金余额变动","金额U":float(closing-opening)},
        {"账单项目":"月初现金余额","金额U":float(opening)},
        {"账单项目":"月末现金余额","金额U":float(closing)}])
    diff=pd.DataFrame([
        {"差异来源":"价格/交易序列/仓位差异","实盘-原执行包内风险回测U":live.gross_pnl.sum()-replay.gross_pnl.sum()},
        {"差异来源":"手续费差异","实盘-原执行包内风险回测U":live.fee.sum()-replay.fee.sum()},
        {"差异来源":"资金费差异（回测为估算）","实盘-原执行包内风险回测U":live.funding.sum()-replay.funding.sum()},
        {"差异来源":"净差合计","实盘-原执行包内风险回测U":live.net_pnl.sum()-replay.net_pnl.sum()}])
    export(strategies,"live_vs_backtest_by_strategy.csv")
    export(params,"parameters.csv")
    export(comparisons,"comparison.csv")
    export(coins,"live_by_coin.csv")
    export(weeks,"weekly_review.csv")
    export(strict,"trades.csv")
    summary_rows=[]
    for category,groups in (("合计",[("全部",strict)]),("方向",strict.groupby("side")),("币种",strict.groupby("symbol")),("策略",strict.groupby("strategy"))):
        for name,g in groups:
            summary_rows.append({"分类":category,"名称":SIDE.get(name,name),**metrics(g)})
    export(pd.DataFrame(summary_rows),"summary.csv")
    curve=[]
    for t in range(START,END+1,HOUR):
        past=strict[strict.exit_ts<t]
        curve.append({"时间":date(t),"初始参考资金U":10000,"已实现累计利润U":past.net_pnl.sum(),"已实现参考权益U":10000+past.net_pnl.sum(),"多头已实现利润U":past[past.side=="long"].net_pnl.sum(),"空头已实现利润U":past[past.side=="short"].net_pnl.sum()})
    curve=pd.DataFrame(curve)
    curve["已实现回撤U"]=curve["已实现参考权益U"].cummax()-curve["已实现参考权益U"]
    export(curve,"equity_curve.csv")
    period={"统计范围":"2026-08（仅本月）","初始参考资金U":10000,"已实现利润U":strict.net_pnl.sum(),"已实现收益率%":strict.net_pnl.sum()/100,"说明":"按平仓归属，含跨月持仓完整损益；非账户盯市收益率，不年化"}
    export(pd.DataFrame([{"月份":"2026-08",**period}]),"monthly_returns.csv")
    export(pd.DataFrame([{"年份":2026,**period}]),"yearly_returns.csv")
    sections=[]
    def add(title,text,df=None):
        sections.append((title,text,df))
    add("8月结论",f"ReapAI 的 BTC、ETH、SOL、DOGE 四个 USDT 永续币对，8月共有 578 条成交、100 笔完整平仓交易，26 胜、74 负。按8月平仓交易的完整生命周期计算，毛损益 +164.42U，手续费 -42.88U，资金费 -6.60U，净盈利 +114.95U。多头 +223.33U，空头 -108.38U。\n\n整月盈利依赖少数大趋势单。最大一笔 ETH 多头 +{top1:.2f}U；剔除这一笔，其余99笔合计 {live.net_pnl.sum()-top1:+.2f}U。这说明收益集中，不能从一个盈利月份推出策略已经稳定。")
    add("四币种与多空拆分","ETH 贡献最多；SOL 次之。BTC 接近盈亏平衡，DOGE 整月小亏。四个币种的空头全部亏损。",coins)
    add("亏损如何转为盈利",f"按平仓累计，月内最低点为 {date(live.loc[trough_idx,'exit_ts'])} 的 {peakcum.min():+.2f}U。最大已实现回撤 223.28U，最长连续亏损14笔；多头单独看最长连续亏损15笔。随后8/18—8/19建立的 ETH、SOL 多头兑现大收益。\n\n已实现回撤仅依据平仓顺序计算，未纳入持仓浮盈浮亏；账户真实权益最大回撤尚不能由本次数据精确重建。",weeks)
    add("决定月度结果的大单","这些交易适合重点复盘入场、持有和保护退出。保留大盈利的能力，比单纯追求胜率更影响这个月的结果。",top_display)
    add("空头问题在哪里","39笔空头仅11笔盈利，胜率28.21%；平均盈利约2.47U，平均亏损约4.84U，盈利因子0.20。亏损不只来自胜率低，还来自盈利单普遍太小。原参数回测也出现四币种空头共同亏损，因此仅凭本月数据，无法把空头亏损主要归结为人工操作或下单延迟。\n\n可优先复核空头斜率退出和动态保护，确认是否在8月上涨/反弹中反复小赚大亏。是否调整或停用，需要更长样本与样本外测试；本次未对8月做参数搜索。")
    add("完整月度账单对账",f"账单按实际发生时间统计，8月交易相关净变动为 +{ledger_net:.2f}U，加自动赚币 +{interest:.2f}U，账户现金余额从 {opening:.2f}U 变为 {closing:.2f}U，对账残差小于0.00000001U。账单类别与资金费正负依据 OKX 官方字段定义（https://www.okx.com/docs-v5）。\n\n平仓生命周期净利润 +114.95U 与账单交易净变动的差额 {float(ledger_net)-live.net_pnl.sum():+.6f}U，来自跨月开仓手续费、资金费的归属差异。本月有3笔从7月延续到8月的平仓交易，以及月末尚未平仓的头寸。现金余额不含持仓浮盈浮亏，不能当成月末总权益。",reconcile)
    add("8月末未平仓", "从每个币对/方向最后一次完整平仓后的成交继续累计，重建8月末头寸。下面浮盈用8月最后一根已确认1H收盘价估算，不是当时的交易所标记价快照，也未计未来平仓成本。",pd.DataFrame(month_end))
    add("最佳参数回测结果","使用6月28日保存的既有最佳参数组合包（8条1H策略），未使用8月盈亏重新挑参数。四币种均补齐8月744根已确认1H K线；5月至7月提供预热与持仓状态，按8月平仓时间归属，月末仓位保留，不强制平仓。\n\n100U版：初始参考资金10000U、每笔固定风险100U、非复利、无组合资金池或暴露限制。包内参考风险版：多头BTC/ETH/SOL/DOGE=20/12/4/4U，空头=10/8/6/6U。这是参数包中的配置，缺少ReapAI当时的会话快照，不能断言实盘整月始终使用这些风险值。",comparisons)
    add("100U回测多空结果","所有回测均使用成交核实的 Maker 0.015% / Taker 0.036%。标准开盘版按每边0.03%滑点；资金费使用历史实际费率与结算前完整1H收盘价估算，未获得逐结算标记价格及精确盘中持仓时刻，资金费不是精确实盘复刻。",side_bt)
    add("成交口径审计：两组结果不能混用","原引擎动态多头在下一根K线按限价触及成交，若开盘已经可成交则按Taker；原引擎斜率空头按信号当根收盘成交，收盘信号退出也在当根成交。原多头成交函数关闭入场滑点，出场仍计0.03%滑点。这些行为与旧报告笼统写的‘下一根开盘、双边滑点’不一致。\n\n原执行复现保留上述行为，适合与现有实盘策略做近似对照。统一下一根开盘版在独立研究进程中复用信号、止损和动态保护，只调整信号成交与信号退出时点；多头的限价等待改为下一根开盘，因此交易数从100降为73，是执行假设变化，不是参数优化收益。多头开仓仍按项目规定的Maker成本假设计费，实际保证开盘成交通常需要Taker，报告另列该费用敏感性。\n\n研究适配器同时处理入场当根止损；每笔标准版已校验信号时间+1小时=开仓时间、滑点后成交价符合下一根开盘。生产回测引擎和实盘参数保持原状。执行变更可查 execution_adapter_audit.json。")
    add("合约单位审计","原引擎固定风险定仓把风险金除以价格距离，得到币数量，但直接用OKX的合约张数步长取整。BTC每张0.01BTC、每次0.01张，正确币数量步长应为0.0001BTC；原引擎却按0.01BTC取整。ETH对应应为0.001ETH，DOGE应为10DOGE。小额风险下，BTC仓位截断尤为明显。\n\n本次校准仅在研究脚本传入的模拟品种中换算步长与最小数量，生产代码保持原样。‘合约单位校准/原执行’继续使用原信号和原限价成交逻辑；‘下一根开盘’版同时校准单位和执行时点。未经校准的原引擎结果保留为审计对照，不能优先作为实盘差异判断依据。")
    add("实盘与原执行回测的差异",f"用于比较的合约单位校准/原执行/包内参考风险回测为 {replay.net_pnl.sum():+.2f}U，实盘 +114.95U，实盘相对回测差额 {live.net_pnl.sum()-replay.net_pnl.sum():+.2f}U。两边虽然都是100笔，但币种/方向分布及单笔结果不同，不能把总笔数相同理解成逐笔一致。",strategies)
    add("差额归因",f"按币种、方向、入场时间差≤1.25小时及离场时间差≤2小时，得到 {len(matched)} 组一对一候选匹配。此匹配基于时间，未证明信号、参数或策略ID一致；差异文件保留全部未匹配交易。价格差还包含下单位置、挂单成交、止损路径和仓位差异，不能称为纯滑点。",diff)
    add("需要纠正旧半月报告的判断","8/15旧报告只覆盖前半月，不能代表整月。旧报告曾把BTC空头约-10U、ETH约-8U笼统视为超出‘常规-4U止损带’，但参数包对应风险分别为10U与8U，这种判断缺少依据。应按每个币对、方向、当时仓位和参数分别核对。\n\n本月8/17 ETH多头 -22.31U、8/28 BTC多头 -23.64U值得对照成交明细和原始止损计划。缺少历史初始止损与运行快照时，不能直接断言是风控失效。")
    add("参数快照", "动态保护的完整分级规则保存在 parameter_bundle.json；下表只列核心参数。参数包保存日期早于8月，但策略代码沿用当前工作区版本，因此结果是‘现有代码重放旧参数’，不能当成严格锁定历史软件版本的样本外实盘复制。",params)
    add("复盘优先顺序","1. 先核对8/18—8/19 ETH、SOL大盈利单：是否按既定规则入场和持有，回测与实盘在哪个退出节点分叉。\n2. 再看8/17 ETH与8/28 BTC亏损单：按实际张数和当时止损距离核对风险，不用统一4U阈值。\n3. 对空头按币对统计斜率退出、动态保护退出的盈利保留情况，先确认策略状态与执行一致性，再考虑改参数。\n4. 如后续要优化，使用8月之前的数据选参，再用8月或其他留出月份验证；不要把8月上调出的最优值当作对8月的独立回测。")
    add("数据完整性与局限",f"OKX只读同步获得8月578条成交、100个完整已平仓生命周期、1524条账单；成交billId与交易账单逐条一一对应。100笔仓位的毛损益、手续费均与分配到该仓位的成交逐笔核对一致，无重复分配；仓位净损益分项也无残差。匹配允许仓位创建时间与首笔成交相差最多1秒，本次观测到的提前成交时间差最多27毫秒。四个币对各744根8月K线，无缺口、无重复。原执行8条策略用5月与6月两种预热起点复跑，8月交易与损益一致；标准开盘版8条策略均通过截断未来行情后的前缀一致性检查。\n\n1H回测的盘中高低点先后采用项目现有路径假设；缺少分钟/逐笔行情验证。ReapAI本地策略会话记录和逐笔策略账本未覆盖8月，策略名称根据参数包及客户端订单ID特征推断。月度主要损益采用已平仓口径，实盘月末仓位另存CSV，回测未平仓明细另存JSON；equity_curve.csv 是已实现参考权益曲线，monthly_returns.csv 与 yearly_returns.csv 也只覆盖本次8月，不代表盯市收益或完整年度收益。")
    # Conservative sensitivity: extra cost if every standard long entry is taker.
    taker_extra=float((strict[strict.side=="long"].entry_price*strict[strict.side=="long"].base_size).sum())*float(D("0.00036")-D("0.00015"))
    add("开盘成交的成本敏感性",f"标准下一根开盘/100U版净损益 +{strict.net_pnl.sum():.2f}U。若仅把多头开仓手续费从Maker静态改为Taker，额外费用约{taker_extra:.2f}U，净损益约{strict.net_pnl.sum()-taker_extra:.2f}U；这是固定交易路径的成本敏感性，不是重新运行保护规则后的精确结果。")
    # Stable, data-only display of all one hundred completed lifecycle trades.
    detail=live[["symbol","side","entry_time","exit_time","entry_price","exit_price","contracts","gross_pnl","fee","funding","net_pnl","holding_hours"]].copy()
    detail["side"]=detail.side.map(SIDE)
    detail.columns=["币对","方向","开仓时间","平仓时间","开仓均价","平仓均价","平仓张数","毛损益U","手续费U","资金费U","净损益U","持仓小时"]
    add("全部100笔实盘平仓明细","成交拆单层面的578条记录见 live_fills.csv；全部1524条账单见 live_bills.csv。",detail)
    svg=curve_svg([("ReapAI 实盘",live,"#0e7490"),("原执行 / 包内参考风险回测",replay,"#a16207")])
    (OUT/"realized_pnl_curve.svg").write_text(svg,encoding="utf-8")
    css='''body{margin:0;background:#eef2f5;color:#172c3c;font-family:"Microsoft YaHei","Segoe UI",sans-serif;line-height:1.7}main{max-width:1180px;margin:auto;padding:32px 24px 60px}header{background:#12384a;color:white;padding:32px;border-radius:16px}h1{margin:0;font-size:30px}header p{margin-bottom:0;color:#cde0e8}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin:20px 0}.card,section{background:white;border:1px solid #dce4ea;border-radius:12px;padding:22px}.card span{display:block;color:#607585;font-size:13px}.card strong{font-size:26px}section{margin-top:18px}h2{font-size:21px;margin:0 0 12px}p{white-space:pre-line;margin:12px 0}.table-wrap{overflow-x:auto}table{border-collapse:collapse;width:100%;font-size:13px;white-space:nowrap}th,td{padding:9px 11px;border-bottom:1px solid #e3e9ed;text-align:right}th{background:#f0f5f7;color:#345266}td:first-child,th:first-child{text-align:left}tr:nth-child(even){background:#f9fbfc}svg{width:100%;height:auto}a{color:#0e7490}.note{font-size:13px;color:#64748b}details summary{cursor:pointer;font-size:21px;font-weight:bold}footer{padding:20px 0;color:#607585;font-size:12px}@media(max-width:720px){main{padding:16px}.cards{grid-template-columns:repeat(2,1fr)}h1{font-size:23px}}'''
    body=[]
    for title,text,df in sections:
        content=f'<p>{html.escape(text)}</p>'+(table(df) if df is not None else "")
        if title.startswith("全部100"):
            body.append(f'<section><details><summary>{html.escape(title)}</summary>{content}</details></section>')
        else:
            body.append(f'<section><h2>{html.escape(title)}</h2>{content}</section>')
    page='<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>ReapAI 2026年8月完整复盘与最佳参数回测</title><style>'+css+'</style></head><body><main><header><h1>ReapAI · 2026年8月完整复盘</h1><p>BTC / ETH / SOL / DOGE · 北京时间 8/1 00:00—9/1 00:00 · 既有最佳参数复跑</p></header><div class="cards"><div class="card"><span>实盘已平仓净利润</span><strong>+114.95 U</strong></div><div class="card"><span>平仓 / 成交记录</span><strong>100 / 578</strong></div><div class="card"><span>实盘胜率</span><strong>26.0%</strong></div><div class="card"><span>最大已实现回撤</span><strong>223.28 U</strong></div></div><section><h2>月内已实现累计盈亏</h2>'+svg+'<p class="note">两条曲线使用实盘实际仓位 / 参数包参考小额风险，回测资金费为估算；不含持仓浮盈浮亏。</p></section>'+''.join(body)+'<footer>原始数据、参数快照、研究脚本与验证结果均保存在本地。<a href="https://www.okx.com/docs-v5">OKX API字段与接口说明</a>。仅完成历史分析与离线回测。</footer></main></body></html>'
    (OUT/"report.html").write_text(page,encoding="utf-8")
    markdown="# ReapAI 2026年8月完整复盘与最佳参数回测\n\n"
    for title,text,df in sections:
        if title.startswith("全部100"):
            continue
        markdown+=f"## {title}\n\n{text}\n\n"+(mdtable(df)+"\n\n" if df is not None else "")
    markdown+="[OKX官方接口与账单字段说明](https://www.okx.com/docs-v5)\n"
    (OUT/"report.md").write_text(markdown,encoding="utf-8")
    checks={"fill_bill_ids_exact":True,"unique_positions":len(live),"balance_reconciliation_residual":str(closing-opening-ledger_net-interest),
            "position_fill_gross_max_residual":max(abs(r["gross_residual"]) for r in fill_checks),
            "position_fill_fee_max_residual":max(abs(r["fee_residual"]) for r in fill_checks),
            "time_match_candidates":len(matched),"unmatched_live":len(live)-len(matched),"unmatched_replay":len(replay)-len(matched),
            "live_without_best_trade":float(live.net_pnl.sum()-top1),"cash_open":str(opening),"cash_close":str(closing),
            "cash_trading_delta":str(ledger_net),"auto_earn":str(interest),"strict_taker_extra_cost":taker_extra}
    save("verification.json",checks)
    hashes={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [ROOT/"okx_quant/backtest.py",ROOT/"okx_quant/models.py",OUT/"parameter_bundle.json",OUT/"positions_raw.json",OUT/"fills_raw.json",OUT/"bills_raw.json"]}
    save("run_manifest.json",{"period_start":date(START),"period_end_exclusive":date(END),"profile":"ReapAI","symbols":SYMBOLS,
         "parameter_optimization":False,"bundle_created_at":bundle["created_at"],"maker_fee":"0.00015","taker_fee":"0.00036","standard_slippage_per_side":"0.0003",
         "official_outputs_execution":"next_open_100u","capital":10000,"risk_per_trade":100,"compounding":False,"portfolio_constraints":False,
         "pnl_basis":"closed_lifecycle","funding":"historical rates; prior completed trade-candle close approximation","hashes":hashes})
    print(json.dumps(checks,ensure_ascii=False,indent=2))
    print(str(OUT/"report.html"))


if __name__=="__main__":
    build_report()
