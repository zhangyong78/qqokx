from __future__ import annotations

import html
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
HISTORY_PATH = Path(r"D:\qqokx_data\state\backtest_history.json")
DATA_REPORT_DIR = Path(r"D:\qqokx_data\reports\analysis")
PROJECT_REPORT_DIR = ROOT / "reports"
LATEST_HTML = PROJECT_REPORT_DIR / "btc_long_5_software_results_analysis_latest.html"


def dec(value: object) -> Decimal:
    return Decimal(str(value))


def fmt(value: object | None, digits: int = 2) -> str:
    if value is None:
        return "-"
    q = Decimal("1").scaleb(-digits)
    return str(dec(value).quantize(q))


def esc(value: object) -> str:
    return html.escape(str(value))


def build_rows() -> list[dict[str, object]]:
    data = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
    records = data["records"][-5:]
    labels = ["R001", "R002", "R003", "R004", "R005"]
    rows: list[dict[str, object]] = []
    for label, rec in zip(labels, records):
        cfg = rec["config"]
        report = rec["report"]
        pnl = dec(report["total_pnl"])
        fees = dec(report.get("total_fees", "0"))
        dd = dec(report["max_drawdown"])
        pf = dec(report["profit_factor"]) if report.get("profit_factor") is not None else None
        entry_period = cfg["entry_reference_ema_period"]
        entry_label = (
            f"{cfg['entry_reference_ema_type'].upper()}{entry_period}"
            if entry_period
            else "跟随快线"
        )
        rows.append(
            {
                "label": label,
                "snapshot": rec["snapshot_id"],
                "created_at": rec["created_at"],
                "params": (
                    f"{cfg['ema_type'].upper()}{cfg['ema_period']}/"
                    f"{cfg['trend_ema_type'].upper()}{cfg['trend_ema_period']} / "
                    f"挂单{entry_label} / ATR{cfg['atr_period']} / "
                    f"SLx{cfg['atr_stop_multiplier']} / TPx{cfg['atr_take_multiplier']} / "
                    f"首档R{cfg['ema55_slope_lock_profit_trigger_r']}"
                ),
                "trades": int(report["total_trades"]),
                "win_rate": dec(report["win_rate"]),
                "pnl": pnl,
                "return_pct": dec(report["total_return_pct"]),
                "pf": pf,
                "avg_r": dec(report.get("average_r_multiple", "0")),
                "dd": dd,
                "dd_pct": dec(report.get("max_drawdown_pct", "0")),
                "fees": fees,
                "maker_fees": dec(report.get("maker_fees", "0")),
                "taker_fees": dec(report.get("taker_fees", "0")),
                "gross_profit": dec(report["gross_profit"]),
                "gross_loss": dec(report["gross_loss"]),
                "fee_net_pct": (fees / pnl * Decimal("100")) if pnl != 0 else None,
                "pnl_dd": (pnl / dd) if dd != 0 else None,
                "trigger_r": int(cfg["ema55_slope_lock_profit_trigger_r"]),
                "fast_type": cfg["ema_type"].upper(),
                "fast_period": int(cfg["ema_period"]),
                "trend_type": cfg["trend_ema_type"].upper(),
                "trend_period": int(cfg["trend_ema_period"]),
                "entry_label": entry_label,
                "sl": dec(cfg["atr_stop_multiplier"]),
                "candle_count": rec["candle_count"],
                "export_path": rec.get("export_path", ""),
            }
        )
    return rows


def render_html(rows: list[dict[str, object]]) -> str:
    best_pnl = max(rows, key=lambda row: row["pnl"])
    best_pf = max(rows, key=lambda row: row["pf"] or Decimal("-1"))
    best_dd = min(rows, key=lambda row: row["dd"])
    worst_fee = max(
        rows,
        key=lambda row: row["fee_net_pct"] if row["fee_net_pct"] is not None else Decimal("-1"),
    )

    r1, r2, r3, r4, r5 = rows
    insights = [
        f"利润第一是 {best_pnl['label']}：净利 {fmt(best_pnl['pnl'])}U，收益率 {fmt(best_pnl['return_pct'])}%，但交易 {best_pnl['trades']} 笔、手续费 {fmt(best_pnl['fees'])}U，属于进攻型。",
        f"质量第一按 PF 看是 {best_pf['label']}：PF {fmt(best_pf['pf'], 4)}，交易只有 {best_pf['trades']} 笔，比 R001 更克制。",
        f"回撤最小是 {best_dd['label']}：最大回撤 {fmt(best_dd['dd'])}U，但净利只有 {fmt(best_dd['pnl'])}U，说明保守不等于好。",
        f"EMA21/MA50 同样 SL1 下，R6 明显强于 R2：R002 净利 {fmt(r2['pnl'])}U / PF {fmt(r2['pf'], 4)}，R003 只有 {fmt(r3['pnl'])}U / PF {fmt(r3['pf'], 4)}。",
        f"MA21 不如 EMA21：R005 比 R002 少赚 {fmt(r2['pnl'] - r5['pnl'])}U，且回撤多 {fmt(r5['dd'] - r2['dd'])}U；R004 几乎被手续费吃平。",
        "R003/R004 胜率约 30%，但 PF 接近 1，说明提高胜率主要来自过早保本，不是真正提高交易优势。",
    ]

    style = """
body{font-family:"Microsoft YaHei",Arial,sans-serif;background:#f7f3ea;color:#14201d;margin:0;padding:28px}
h1{font-size:32px;margin:0 0 8px}.sub{color:#52615c;margin-bottom:24px}
.cards{display:grid;grid-template-columns:repeat(4,minmax(180px,1fr));gap:14px;margin:20px 0}
.card{background:#fffaf0;border:1px solid #ead7ae;border-radius:16px;padding:18px;box-shadow:0 8px 24px #0001}
.card b{display:block;color:#0b7a55;font-size:24px;margin-top:8px}
table{border-collapse:collapse;width:100%;background:white;border-radius:14px;overflow:hidden;box-shadow:0 8px 24px #0001}
th,td{padding:10px 12px;border-bottom:1px solid #eee;text-align:right;white-space:nowrap}
th:first-child,td:first-child,td:nth-child(2){text-align:left}th{background:#0f4c45;color:white}
.good{color:#087846;font-weight:700}.warn{color:#b86b00;font-weight:700}.bad{color:#b23b3b;font-weight:700}
.note{background:#fff3c4;border:1px solid #e9c967;border-radius:14px;padding:16px 18px;margin:18px 0;line-height:1.75}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:16px}.small{font-size:13px;color:#60706b}
@media(max-width:900px){.cards,.grid2{grid-template-columns:1fr}body{padding:16px}table{font-size:12px;display:block;overflow-x:auto}}
"""
    table_rows = []
    for row in rows:
        fee_pct = row["fee_net_pct"]
        fee_cls = "bad" if fee_pct and fee_pct > Decimal("100") else "warn" if fee_pct and fee_pct > Decimal("70") else ""
        pf = row["pf"]
        pf_cls = "good" if pf and pf >= Decimal("1.15") else "warn" if pf and pf >= Decimal("1.05") else "bad"
        pnl_cls = "good" if row["pnl"] > Decimal("10000") else "warn" if row["pnl"] > Decimal("1000") else "bad"
        table_rows.append(
            f"""
<tr>
<td><b>{esc(row['label'])}</b></td>
<td>{esc(row['params'])}</td>
<td>{row['trades']}</td>
<td>{fmt(row['win_rate'])}%</td>
<td class="{pnl_cls}">{fmt(row['pnl'])}</td>
<td class="{pf_cls}">{fmt(row['pf'], 4)}</td>
<td>{fmt(row['avg_r'], 4)}</td>
<td>{fmt(row['dd'])}</td>
<td>{fmt(row['dd_pct'])}%</td>
<td>{fmt(row['fees'])}</td>
<td class="{fee_cls}">{fmt(row['fee_net_pct'])}%</td>
<td>{fmt(row['pnl_dd'])}</td>
</tr>"""
        )

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>BTC 动态委托做多 5组软件回测分析</title>
<style>{style}</style>
</head>
<body>
<h1>BTC 动态委托做多 5组软件回测分析</h1>
<div class="sub">数据源：{esc(HISTORY_PATH)}，取最近 5 条软件回测记录；生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}；K线：{rows[0]['candle_count']} 根，全量。</div>
<div class="cards">
<div class="card">净利第一<b>{esc(best_pnl['label'])}</b><span>{fmt(best_pnl['pnl'])}U</span></div>
<div class="card">PF 第一<b>{esc(best_pf['label'])}</b><span>{fmt(best_pf['pf'], 4)}</span></div>
<div class="card">回撤最小<b>{esc(best_dd['label'])}</b><span>{fmt(best_dd['dd'])}U</span></div>
<div class="card">最应警惕手续费<b>{esc(worst_fee['label'])}</b><span>{fmt(worst_fee['fee_net_pct'])}%</span></div>
</div>
<table>
<thead><tr><th>编号</th><th>参数</th><th>交易数</th><th>胜率</th><th>净利U</th><th>PF</th><th>平均R</th><th>最大回撤U</th><th>回撤%</th><th>手续费U</th><th>手续费/净利</th><th>净利/回撤</th></tr></thead>
<tbody>{''.join(table_rows)}</tbody>
</table>
<div class="grid2">
<div class="note"><h2>交易员结论</h2><ul>{''.join('<li>' + esc(line) + '</li>' for line in insights)}</ul></div>
<div class="note"><h2>为什么和我前面跑的差很多</h2><ul>
<li>第一，前面那份 3 组脚本不是这 5 组同参数，尤其前面有 SLx2，而你软件这 5 组全是 SLx1。</li>
<li>第二，R001 虽然交易数和胜率能对上，但软件手续费更高，说明我前面脚本和软件正式回测的费用/成交审计口径不完全一致。</li>
<li>第三，后续做决策应以软件导出的结果为准；脚本只用来做批量实验，除非先校准到同一手续费和成交口径。</li>
</ul></div>
</div>
<div class="note"><h2>一句话建议</h2>如果只看这 5 组：进攻选 R001；更稳、更少交易选 R002；R003/R004 不建议，R005 回撤太大也不建议。下一步应做分年份/月度稳定性，而不是继续只追最高净利。</div>
<div class="small">示例导出文件：{esc(rows[0]['export_path'])}</div>
</body>
</html>"""


def write_outputs(rows: list[dict[str, object]], html_doc: str) -> tuple[Path, Path, Path]:
    DATA_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    html_path = DATA_REPORT_DIR / f"btc_long_5_software_results_analysis_{stamp}.html"
    json_path = html_path.with_suffix(".json")
    html_path.write_text(html_doc, encoding="utf-8")
    LATEST_HTML.write_text(html_doc, encoding="utf-8")
    json_path.write_text(
        json.dumps(
            {"generated_at": datetime.now().isoformat(), "rows": rows},
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    return LATEST_HTML, html_path, json_path


def main() -> None:
    rows = build_rows()
    html_doc = render_html(rows)
    latest, html_path, json_path = write_outputs(rows, html_doc)
    print(latest)
    print(html_path)
    print(json_path)
    for row in rows:
        print(
            row["label"],
            row["params"],
            "trades",
            row["trades"],
            "win",
            fmt(row["win_rate"]),
            "pnl",
            fmt(row["pnl"]),
            "pf",
            fmt(row["pf"], 4),
            "dd",
            fmt(row["dd"]),
            "fees/net",
            fmt(row["fee_net_pct"]),
        )


if __name__ == "__main__":
    main()
