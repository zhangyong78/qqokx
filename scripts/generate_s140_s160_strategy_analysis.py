from __future__ import annotations

import html
import json
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from okx_quant.app_paths import analysis_reports_dir_path, state_dir_path


def dec(value: object, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def u(text: str) -> str:
    return text.encode("ascii").decode("unicode_escape")


def ent(text: str) -> str:
    return "".join(ch if ord(ch) < 128 else f"&#{ord(ch)};" for ch in text)


def e(text: str) -> str:
    return ent(u(text))


LABELS = {
    "page_title": e("\\u56de\\u6d4b S140-S160 \\u7b56\\u7565\\u5206\\u6790"),
    "main_title": e("S140-S160 \\u7b56\\u7565\\u56de\\u6d4b\\u5206\\u6790"),
    "data_source": e("\\u6570\\u636e\\u6e90"),
    "generated_at": e("\\u751f\\u6210\\u65f6\\u95f4"),
    "record_count": e("\\u8bb0\\u5f55\\u6570"),
    "summary": e("\\u603b\\u7ed3\\u8bba"),
    "main_conclusion": e("\\u4e3b\\u7ed3\\u8bba"),
    "main_conclusion_text": e(
        "\\u5982\\u679c\\u4e3b\\u505aBTC\\uff0c\\u4f18\\u5148\\u7528 S144 / S150\\uff1a"
        " ATR\\u6b62\\u635f2\\u500d + 5R\\u5f00\\u542f + \\u659c\\u7387\\u8f6c\\u6b63\\u5e73\\u4ed3\\u5f00\\u542f\\u3002"
        "\\u8fd9\\u7ec4\\u4e0d\\u662f\\u6240\\u6709\\u7b56\\u7565\\u91cc\\u6700\\u9ad8\\u6536\\u76ca\\uff0c\\u4f46\\u662f"
        "\\u6536\\u76ca\\u3001\\u56de\\u64a4\\u3001PF\\u3001\\u624b\\u7eed\\u8d39\\u538b\\u529b\\u7684\\u5747\\u8861\\u6700\\u597d\\u3002"
    ),
    "exit_rule": e("\\u5e73\\u4ed3\\u6761\\u4ef6"),
    "exit_rule_text": e(
        "\\u659c\\u7387\\u8f6c\\u6b63\\u5e73\\u4ed3\\u6574\\u4f53\\u5efa\\u8bae\\u4fdd\\u7559\\u3002"
        " ETH\\u3001SOL\\u3001BNB \\u5173\\u6389\\u540e\\u660e\\u663e\\u53d8\\u5dee\\uff0c\\u53ea\\u6709 DOGE \\u662f\\u4f8b\\u5916\\u3002"
    ),
    "atr_rule": e("ATR\\u500d\\u6570"),
    "atr_rule_text": e(
        "BTC \\u8fd9\\u7ec4\\u91cc\\uff0cATR 2 \\u6bd4 ATR 1.5 \\u66f4\\u9002\\u5408\\u5b9e\\u76d8\\uff0c"
        "\\u56de\\u64a4\\u66f4\\u4f4e\\uff0c\\u624b\\u7eed\\u8d39\\u66f4\\u4f4e\\u3002"
    ),
    "return_top5": e("\\u6536\\u76ca\\u6392\\u540d Top 5"),
    "score_top5": e("\\u98ce\\u9669\\u8c03\\u6574\\u540e\\u6392\\u540d Top 5"),
    "score_formula": e("\\u8bc4\\u5206\\u516c\\u5f0f"),
    "return_word": e("\\u6536\\u76ca"),
    "drawdown_word": e("\\u56de\\u64a4"),
    "score_word": e("\\u8bc4\\u5206"),
    "all_compare": e("\\u5168\\u90e8\\u7b56\\u7565\\u5bf9\\u6bd4"),
    "id": e("\\u7f16\\u53f7"),
    "coin": e("\\u5e01\\u79cd"),
    "strategy": e("\\u7b56\\u7565"),
    "atr_stop": e("ATR\\u6b62\\u635f"),
    "nr_trigger": e("nR\\u5f00\\u542f"),
    "slope_exit": e("\\u659c\\u7387\\u5e73\\u4ed3"),
    "nr_lock": e("nR\\u9501\\u76c8"),
    "total_return": e("\\u603b\\u6536\\u76ca"),
    "max_drawdown": e("\\u6700\\u5927\\u56de\\u64a4"),
    "avg_r": e("\\u5e73\\u5747R"),
    "trades": e("\\u4ea4\\u6613\\u6b21\\u6570"),
    "fees": e("\\u624b\\u7eed\\u8d39"),
    "compare_exit": e("\\u659c\\u7387\\u8f6c\\u6b63\\u5e73\\u4ed3\\u5bf9\\u6bd4"),
    "compare_exit_text": e(
        "\\u540c\\u6837\\u662f ATR 2 + 5R \\u7684\\u6761\\u4ef6\\u4e0b\\uff0cETH\\u3001SOL\\u3001BNB \\u5728"
        "\\u5173\\u6389\\u659c\\u7387\\u5e73\\u4ed3\\u540e\\uff0c\\u6536\\u76ca\\u5927\\u964d\\u4e14\\u56de\\u64a4\\u6269\\u5927\\u3002"
        " DOGE \\u5173\\u6389\\u540e PF \\u66f4\\u9ad8\\uff0c\\u4f46\\u603b\\u6536\\u76ca\\u4e0d\\u5982\\u5f00\\u542f\\u65f6\\u9ad8\\u3002"
    ),
    "exit_on_id": e("\\u5f00\\u542f\\u7f16\\u53f7"),
    "exit_on_return": e("\\u5f00\\u542f\\u6536\\u76ca"),
    "exit_on_drawdown": e("\\u5f00\\u542f\\u56de\\u64a4"),
    "exit_on_pf": e("\\u5f00\\u542fPF"),
    "exit_off_id": e("\\u5173\\u95ed\\u7f16\\u53f7"),
    "exit_off_return": e("\\u5173\\u95ed\\u6536\\u76ca"),
    "exit_off_drawdown": e("\\u5173\\u95ed\\u56de\\u64a4"),
    "exit_off_pf": e("\\u5173\\u95edPF"),
    "duplicates": e("\\u91cd\\u590d\\u7ed3\\u679c"),
    "duplicates_text": e("\\u4ee5\\u4e0b\\u7f16\\u53f7\\u7684\\u7ed3\\u679c\\u5b8c\\u5168\\u4e00\\u81f4"),
    "duplicates_note": e(
        "\\u8fd9\\u8bf4\\u660e\\u5728\\u5f53\\u524d\\u53c2\\u6570\\u7ec4\\u5408\\u4e0b\\uff0cBTC EMA55"
        "\\u659c\\u7387\\u505a\\u7a7a\\u4e0e\\u5747\\u7ebf\\u659c\\u7387\\u505a\\u7a7a\\u7684\\u7ed3\\u679c\\u53ef\\u4ee5\\u6309\\u540c\\u4e00\\u7ec4\\u770b\\u3002"
    ),
    "final_advice": e("\\u6700\\u7ec8\\u5efa\\u8bae"),
    "advice_1": e("\\u5b9e\\u76d8\\u4e3b\\u7b56\\u7565\\u7528 S144 / S150\\u3002"),
    "advice_2": e("\\u60f3\\u8ffd\\u6c42\\u6700\\u9ad8\\u6536\\u76ca\\uff0c\\u53ef\\u4ee5\\u5173\\u6ce8 S155\\uff0c\\u4f46\\u5efa\\u8bae\\u5355\\u72ec\\u63a7\\u4ed3\\u3002"),
    "advice_3": e(
        "\\u60f3\\u505a\\u9632\\u5b88\\u578b DOGE \\u53ef\\u4ee5\\u770b S156\\uff0c\\u4f46\\u4e0d\\u8981\\u76f4\\u63a5"
        "\\u628a\\u8fd9\\u4e2a\\u7ed3\\u8bba\\u5957\\u5230 ETH\\u3001SOL\\u3001BNB\\u3002"
    ),
    "advice_4": e("\\u4e0d\\u5efa\\u8bae S157 / S158 / S159\\uff0c\\u5173\\u95ed\\u659c\\u7387\\u5e73\\u4ed3\\u540e\\u6574\\u4f53\\u8868\\u73b0\\u504f\\u5dee\\u3002"),
    "foot": e(
        "\\u6ce8\\uff1a\\u8bc4\\u5206\\u53ea\\u662f\\u8f85\\u52a9\\u6307\\u6807\\uff0c\\u6700\\u7ec8\\u8fd8\\u662f\\u8981"
        "\\u770b\\u56de\\u64a4\\u3001PF\\u3001\\u624b\\u7eed\\u8d39\\u538b\\u529b\\u548c\\u5b9e\\u76d8\\u53ef\\u6267\\u884c\\u6027\\u3002"
    ),
    "open": e("\\u5f00"),
    "close": e("\\u5173"),
    "card_main": e("\\u4e3b\\u63a8\\u7ec4\\u5408"),
    "card_main_desc": e(
        "\\u6bd4\\u7279BTC\\uff0cATR\\u6b62\\u635f2\\u500d\\uff0c5R\\u5f00\\u542f\\uff0c\\u659c\\u7387\\u8f6c\\u6b63\\u5e73\\u4ed3\\u5f00\\u542f\\u3002"
        "\\u6536\\u76ca75.52%\\uff0c\\u56de\\u64a411.54%\\u3002"
    ),
    "card_return": e("\\u6700\\u9ad8\\u6536\\u76ca"),
    "card_return_desc": e("\\u72d7\\u72d7DOGE\\u7ec4\\u5408\\u6700\\u5f3a\\uff0c\\u603b\\u6536\\u76ca83.05%\\uff0c\\u4f46\\u5c5e\\u4e8e\\u8fdb\\u653b\\u578b\\u7b56\\u7565\\u3002"),
    "card_pf": e("\\u6700\\u9ad8PF"),
    "card_pf_desc": e("\\u72d7\\u72d7DOGE\\u5173\\u95ed\\u659c\\u7387\\u5e73\\u4ed3\\u540e\\uff0cPF 1.329\\uff0c\\u56de\\u64a48.83%\\uff0c\\u66f4\\u504f\\u9632\\u5b88\\u3002"),
    "card_avoid": e("\\u9700\\u8981\\u907f\\u5f00"),
    "card_avoid_desc": e("\\u5173\\u95ed\\u659c\\u7387\\u8f6c\\u6b63\\u5e73\\u4ed3\\u540e\\uff0cBNB\\u3001SOL\\u3001ETH\\u8868\\u73b0\\u660e\\u663e\\u53d8\\u5dee\\u3002"),
}


def strategy_label(strategy_id: str) -> str:
    if strategy_id == "btc_ema55_slope_short":
        return u("\\u6bd4\\u7279EMA55\\u659c\\u7387\\u505a\\u7a7a")
    return u("\\u5747\\u7ebf\\u659c\\u7387\\u505a\\u7a7a")


def duplicate_groups(rows: list[dict[str, object]]) -> list[list[str]]:
    groups: dict[tuple[object, ...], list[str]] = defaultdict(list)
    for row in rows:
        key = (
            row["inst"],
            row["atr"],
            row["trigger_r"],
            row["slope_exit"],
            row["total_return_pct"],
            row["max_drawdown_pct"],
            row["profit_factor"],
            row["total_trades"],
        )
        groups[key].append(str(row["snapshot_id"]))
    return [ids for ids in groups.values() if len(ids) > 1]


def fmt_pct(value: Decimal) -> str:
    return f"{value:.2f}%"


def fmt_num(value: Decimal, digits: int = 2) -> str:
    return f"{value:.{digits}f}"


def bar(value: Decimal, maximum: Decimal, good: bool) -> str:
    width = 0.0 if maximum <= 0 else max(0.0, min(100.0, float(value / maximum * 100)))
    kind = "good" if good else "bad"
    return f'<div class="bar"><span class="{kind}" style="width:{width:.1f}%"></span></div>'


def build_rows() -> list[dict[str, object]]:
    ids = {f"S{i:03d}" for i in range(140, 161)}
    history_path = state_dir_path() / "backtest_history.json"
    payload = json.loads(history_path.read_text(encoding="utf-8"))
    selected = [record for record in payload["records"] if record.get("snapshot_id") in ids]
    selected.sort(key=lambda item: item["snapshot_id"])

    rows: list[dict[str, object]] = []
    for record in selected:
        config = record.get("config", {})
        report = record.get("report", {})
        total_return_pct = dec(report.get("total_return_pct"))
        max_drawdown_pct = dec(report.get("max_drawdown_pct"))
        profit_factor = dec(report.get("profit_factor"))
        score = Decimal("0")
        if max_drawdown_pct > 0:
            score = (total_return_pct / max_drawdown_pct) * profit_factor
        rows.append(
            {
                "snapshot_id": record["snapshot_id"],
                "strategy_label": strategy_label(str(config.get("strategy_id", ""))),
                "inst": str(config.get("inst_id", "")).replace("-USDT-SWAP", ""),
                "atr": str(config.get("atr_stop_multiplier", "")),
                "trigger_r": int(config.get("ema55_slope_lock_profit_trigger_r") or 0),
                "slope_exit": bool(config.get("ema55_slope_exit_enabled")),
                "lock_profit": bool(
                    config.get("ema55_slope_lock_profit_enabled")
                    or config.get("dynamic_two_r_break_even")
                ),
                "total_return_pct": total_return_pct,
                "max_drawdown_pct": max_drawdown_pct,
                "profit_factor": profit_factor,
                "average_r_multiple": dec(report.get("average_r_multiple")),
                "total_trades": int(report.get("total_trades") or 0),
                "total_fees": dec(report.get("total_fees")),
                "score": score,
            }
        )
    return rows


def ranking_list(items: list[dict[str, object]]) -> str:
    parts: list[str] = []
    for item in items:
        parts.append(
            "<li>"
            f"<b>{html.escape(str(item['snapshot_id']))}</b> "
            f"{html.escape(str(item['inst']))} "
            f"{LABELS['return_word']} {fmt_pct(dec(item['total_return_pct']))} / "
            f"{LABELS['drawdown_word']} {fmt_pct(dec(item['max_drawdown_pct']))} / "
            f"PF {fmt_num(dec(item['profit_factor']), 3)} / "
            f"{LABELS['score_word']} {fmt_num(dec(item['score']), 2)}"
            "</li>"
        )
    return "".join(parts)


def render() -> Path:
    rows = build_rows()
    max_return = max((row["total_return_pct"] for row in rows), default=Decimal("1"))
    max_drawdown = max((row["max_drawdown_pct"] for row in rows), default=Decimal("1"))
    return_rank = sorted(rows, key=lambda row: row["total_return_pct"], reverse=True)
    score_rank = sorted(rows, key=lambda row: row["score"], reverse=True)

    cards = [
        ("S144 / S150", LABELS["card_main"], LABELS["card_main_desc"], "good"),
        ("S155", LABELS["card_return"], LABELS["card_return_desc"], "warn"),
        ("S156", LABELS["card_pf"], LABELS["card_pf_desc"], "good"),
        ("S157-S159", LABELS["card_avoid"], LABELS["card_avoid_desc"], "bad"),
    ]

    cards_html = "".join(
        "<div class=\"card\">"
        f"<div class=\"label\">{label}</div>"
        f"<div class=\"value {klass}\">{html.escape(code)}</div>"
        f"<p>{desc}</p>"
        "</div>"
        for code, label, desc, klass in cards
    )

    def table_row(row: dict[str, object]) -> str:
        row_class = ""
        sid = str(row["snapshot_id"])
        if sid in {"S144", "S150"}:
            row_class = ' class="best"'
        elif sid in {"S157", "S158", "S159"}:
            row_class = ' class="badrow"'
        slope_exit = LABELS["open"] if row["slope_exit"] else LABELS["close"]
        lock_profit = LABELS["open"] if row["lock_profit"] else LABELS["close"]
        return (
            f"<tr{row_class}>"
            f"<td><b>{html.escape(sid)}</b></td>"
            f"<td>{html.escape(str(row['inst']))}</td>"
            f"<td>{ent(str(row['strategy_label']))}</td>"
            f"<td class=\"num\">{html.escape(str(row['atr']))}</td>"
            f"<td class=\"num\">{int(row['trigger_r'])}R</td>"
            f"<td class=\"num\">{slope_exit}</td>"
            f"<td class=\"num\">{lock_profit}</td>"
            f"<td class=\"num\">{fmt_pct(dec(row['total_return_pct']))}{bar(dec(row['total_return_pct']), max_return, True)}</td>"
            f"<td class=\"num\">{fmt_pct(dec(row['max_drawdown_pct']))}{bar(dec(row['max_drawdown_pct']), max_drawdown, False)}</td>"
            f"<td class=\"num\">{fmt_num(dec(row['profit_factor']), 3)}</td>"
            f"<td class=\"num\">{fmt_num(dec(row['average_r_multiple']), 4)}</td>"
            f"<td class=\"num\">{int(row['total_trades'])}</td>"
            f"<td class=\"num\">{fmt_num(dec(row['total_fees']), 0)}</td>"
            f"<td class=\"num\">{fmt_num(dec(row['score']), 2)}</td>"
            "</tr>"
        )

    rows_html = "".join(table_row(row) for row in rows)

    pair_by_coin: dict[str, dict[str, dict[str, object]]] = {}
    for row in rows:
        if row["inst"] in {"ETH", "SOL", "BNB", "DOGE"} and row["atr"] == "2" and row["trigger_r"] == 5:
            pair_by_coin.setdefault(str(row["inst"]), {})["on" if row["slope_exit"] else "off"] = row

    pair_rows: list[str] = []
    for coin in ("DOGE", "SOL", "ETH", "BNB"):
        pair = pair_by_coin.get(coin)
        if not pair or "on" not in pair or "off" not in pair:
            continue
        on_row = pair["on"]
        off_row = pair["off"]
        pair_rows.append(
            "<tr>"
            f"<td><b>{coin}</b></td>"
            f"<td>{html.escape(str(on_row['snapshot_id']))}</td>"
            f"<td class=\"num\">{fmt_pct(dec(on_row['total_return_pct']))}</td>"
            f"<td class=\"num\">{fmt_pct(dec(on_row['max_drawdown_pct']))}</td>"
            f"<td class=\"num\">{fmt_num(dec(on_row['profit_factor']), 3)}</td>"
            f"<td>{html.escape(str(off_row['snapshot_id']))}</td>"
            f"<td class=\"num\">{fmt_pct(dec(off_row['total_return_pct']))}</td>"
            f"<td class=\"num\">{fmt_pct(dec(off_row['max_drawdown_pct']))}</td>"
            f"<td class=\"num\">{fmt_num(dec(off_row['profit_factor']), 3)}</td>"
            "</tr>"
        )
    pair_rows_html = "".join(pair_rows)

    duplicates_text = html.escape(" ; ".join(" = ".join(group) for group in duplicate_groups(rows)))
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    history_path = html.escape(str(state_dir_path() / "backtest_history.json"))

    html_text = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{LABELS["page_title"]}</title>
  <style>
    :root {{
      --bg: #f5efe4;
      --panel: #fffaf2;
      --ink: #17222b;
      --muted: #667782;
      --line: #e2d5c2;
      --good: #157347;
      --warn: #c78512;
      --bad: #bf4b3c;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, #ffe7b9 0, transparent 30%),
        radial-gradient(circle at top right, #d9eee4 0, transparent 28%),
        linear-gradient(180deg, #f8f2e8 0%, #f3efe9 100%);
    }}
    header {{
      padding: 36px 42px 28px;
      border-bottom: 1px solid var(--line);
      background: linear-gradient(90deg, rgba(255, 234, 196, 0.92), rgba(224, 242, 234, 0.88));
    }}
    h1 {{
      margin: 0 0 10px;
      font-size: 38px;
      letter-spacing: 0.5px;
    }}
    .sub {{
      color: var(--muted);
      font-size: 14px;
    }}
    main {{
      max-width: 1480px;
      margin: 0 auto;
      padding: 26px 42px 54px;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(220px, 1fr));
      gap: 18px;
      margin-bottom: 22px;
    }}
    .card, .panel {{
      background: rgba(255, 250, 242, 0.92);
      border: 1px solid var(--line);
      border-radius: 20px;
      box-shadow: 0 12px 36px rgba(77, 58, 35, 0.08);
    }}
    .card {{
      padding: 18px 20px;
    }}
    .panel {{
      padding: 20px 22px;
      margin-top: 18px;
    }}
    .label {{
      font-size: 14px;
      color: var(--muted);
      margin-bottom: 10px;
      font-weight: 700;
    }}
    .value {{
      font-size: 26px;
      font-weight: 800;
      margin-bottom: 12px;
    }}
    .good {{ color: var(--good); }}
    .warn {{ color: var(--warn); }}
    .bad {{ color: var(--bad); }}
    h2 {{
      margin: 0 0 14px;
      font-size: 22px;
    }}
    .note {{
      line-height: 1.8;
      background: #fff2cc;
      border: 1px solid #eccf86;
      border-radius: 16px;
      padding: 16px 18px;
    }}
    .cols {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 18px;
    }}
    ul {{
      margin: 0;
      padding-left: 22px;
      line-height: 1.9;
    }}
    table {{
      width: 100%;
      border-collapse: separate;
      border-spacing: 0;
      overflow: hidden;
      border: 1px solid var(--line);
      border-radius: 18px;
      background: #fffdfa;
    }}
    th, td {{
      padding: 10px 11px;
      border-bottom: 1px solid #efe5d8;
      text-align: left;
      vertical-align: top;
      font-size: 13px;
    }}
    th {{
      background: #efe2cf;
      white-space: nowrap;
    }}
    tr:last-child td {{
      border-bottom: none;
    }}
    .num {{
      text-align: right;
      font-variant-numeric: tabular-nums;
    }}
    .bar {{
      height: 5px;
      margin-top: 4px;
      background: #eee4d5;
      border-radius: 999px;
      overflow: hidden;
    }}
    .bar span {{
      display: block;
      height: 100%;
      border-radius: 999px;
    }}
    .bar .good {{
      background: linear-gradient(90deg, #7ec79a, var(--good));
    }}
    .bar .bad {{
      background: linear-gradient(90deg, #efaea5, var(--bad));
    }}
    .best {{
      background: #ebf8ef;
    }}
    .badrow {{
      background: #fff1ed;
    }}
    .foot {{
      margin-top: 18px;
      color: var(--muted);
      font-size: 12px;
    }}
    @media (max-width: 980px) {{
      header, main {{ padding-left: 18px; padding-right: 18px; }}
      .grid, .cols {{ grid-template-columns: 1fr; }}
      table {{ font-size: 12px; display: block; overflow-x: auto; }}
    }}
  </style>
</head>
<body>
  <header>
    <h1>{LABELS["main_title"]}</h1>
    <div class="sub">{LABELS["data_source"]} {history_path} &nbsp;|&nbsp; {LABELS["generated_at"]} {generated_at} &nbsp;|&nbsp; {LABELS["record_count"]} {len(rows)}</div>
  </header>
  <main>
    <div class="grid">{cards_html}</div>

    <section class="panel">
      <h2>{LABELS["summary"]}</h2>
      <div class="note">
        <b>{LABELS["main_conclusion"]}</b> {LABELS["main_conclusion_text"]}<br>
        <b>{LABELS["exit_rule"]}</b> {LABELS["exit_rule_text"]}<br>
        <b>{LABELS["atr_rule"]}</b> {LABELS["atr_rule_text"]}
      </div>
    </section>

    <section class="cols">
      <div class="panel">
        <h2>{LABELS["return_top5"]}</h2>
        <ul>{ranking_list(return_rank[:5])}</ul>
      </div>
      <div class="panel">
        <h2>{LABELS["score_top5"]}</h2>
        <div class="note">{LABELS["score_formula"]} = ({LABELS["return_word"]} / {LABELS["drawdown_word"]}) x PF</div>
        <ul>{ranking_list(score_rank[:5])}</ul>
      </div>
    </section>

    <section class="panel">
      <h2>{LABELS["all_compare"]}</h2>
      <table>
        <thead>
          <tr>
            <th>{LABELS["id"]}</th>
            <th>{LABELS["coin"]}</th>
            <th>{LABELS["strategy"]}</th>
            <th>{LABELS["atr_stop"]}</th>
            <th>{LABELS["nr_trigger"]}</th>
            <th>{LABELS["slope_exit"]}</th>
            <th>{LABELS["nr_lock"]}</th>
            <th>{LABELS["total_return"]}</th>
            <th>{LABELS["max_drawdown"]}</th>
            <th>PF</th>
            <th>{LABELS["avg_r"]}</th>
            <th>{LABELS["trades"]}</th>
            <th>{LABELS["fees"]}</th>
            <th>{LABELS["score_word"]}</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
    </section>

    <section class="panel">
      <h2>{LABELS["compare_exit"]}</h2>
      <div class="note">{LABELS["compare_exit_text"]}</div>
      <table>
        <thead>
          <tr>
            <th>{LABELS["coin"]}</th>
            <th>{LABELS["exit_on_id"]}</th>
            <th>{LABELS["exit_on_return"]}</th>
            <th>{LABELS["exit_on_drawdown"]}</th>
            <th>{LABELS["exit_on_pf"]}</th>
            <th>{LABELS["exit_off_id"]}</th>
            <th>{LABELS["exit_off_return"]}</th>
            <th>{LABELS["exit_off_drawdown"]}</th>
            <th>{LABELS["exit_off_pf"]}</th>
          </tr>
        </thead>
        <tbody>{pair_rows_html}</tbody>
      </table>
    </section>

    <section class="panel">
      <h2>{LABELS["duplicates"]}</h2>
      <div class="note">
        {LABELS["duplicates_text"]}: {duplicates_text}<br>
        {LABELS["duplicates_note"]}
      </div>
    </section>

    <section class="panel">
      <h2>{LABELS["final_advice"]}</h2>
      <ul>
        <li>{LABELS["advice_1"]}</li>
        <li>{LABELS["advice_2"]}</li>
        <li>{LABELS["advice_3"]}</li>
        <li>{LABELS["advice_4"]}</li>
      </ul>
    </section>

    <div class="foot">{LABELS["foot"]}</div>
  </main>
</body>
</html>
"""

    out_dir = analysis_reports_dir_path()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "s140_s160_strategy_analysis_safe_cn.html"
    out_path.write_text(html_text, encoding="ascii")
    return out_path


if __name__ == "__main__":
    print(render())
