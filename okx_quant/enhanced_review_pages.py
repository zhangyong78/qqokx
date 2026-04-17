from __future__ import annotations

import os
from datetime import datetime
from decimal import Decimal
from html import escape
from pathlib import Path
from zoneinfo import ZoneInfo

from okx_quant.enhanced_position_ledger import (
    TOTAL_BUCKET_MODE_PLAYBOOK,
    TOTAL_BUCKET_MODE_PLAYBOOK_SOURCE,
    TOTAL_BUCKET_MODE_UNDERLYING,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")


def build_manual_review_rows(
    ledger_rows: list[dict[str, object]],
    *,
    summary_rows: list[dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    rows = [
        dict(item)
        for item in ledger_rows
        if str(item.get("chart_path", "")).strip()
    ]
    if summary_rows:
        for row in rows:
            summary = next(
                (item for item in summary_rows if _match_summary_group(row, item)),
                None,
            )
            if summary is None:
                continue
            row["group_net_break_even_reference_price"] = summary.get("net_break_even_reference_price")
            row["group_unified_close_reference_price"] = summary.get("unified_close_reference_price")
            row["group_buffer_to_unified_close_pct"] = summary.get("buffer_to_unified_close_pct")
            row["group_target_reduce_reference_price"] = summary.get("target_reduce_reference_price")
            row["group_buffer_to_target_reduce_pct"] = summary.get("buffer_to_target_reduce_pct")
            row["group_target_small_profit_reference_price"] = summary.get("target_small_profit_reference_price")
            row["group_buffer_to_target_small_profit_pct"] = summary.get("buffer_to_target_small_profit_pct")
            row["group_risk_priority_rank"] = summary.get("risk_priority_rank")
            row["group_risk_priority_label"] = summary.get("risk_priority_label")
            row["group_risk_priority_note"] = summary.get("risk_priority_note")
            row["group_suggested_action"] = summary.get("suggested_action")
    rows.sort(
        key=lambda item: (
            int(item.get("group_risk_priority_rank", 99)),
            0 if str(item.get("manual_pool", "")) == "manual" else 1,
            str(item.get("signal_id", "")),
            int(item.get("entry_ts", 0)),
            str(item.get("ledger_id", "")),
        )
    )
    return rows


def write_manual_review_gallery_html(
    path: Path | str,
    *,
    parent_strategy_name: str,
    rows: list[dict[str, object]],
) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        build_manual_review_gallery_html(
            html_path=target,
            parent_strategy_name=parent_strategy_name,
            rows=rows,
        ),
        encoding="utf-8",
    )
    return target


def build_manual_review_gallery_html(
    *,
    html_path: Path,
    parent_strategy_name: str,
    rows: list[dict[str, object]],
) -> str:
    manual_rows = [item for item in rows if str(item.get("manual_pool", "")) == "manual"]
    auto_rows = [item for item in rows if str(item.get("manual_pool", "")) != "manual"]
    high_priority_rows = [item for item in rows if int(item.get("group_risk_priority_rank", 99)) <= 2]
    cards_html = "\n".join(_build_manual_review_card(html_path, row) for row in rows)
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{escape(parent_strategy_name)} 人工接管专用画廊</title>
  <style>
    :root {{
      --bg: #f3efe8;
      --card: rgba(255, 252, 245, 0.96);
      --ink: #1c2530;
      --muted: #5f6a77;
      --line: #ded4c6;
      --gold: #bf7b30;
      --green: #17765a;
      --red: #ba4d45;
      --blue: #2a73d9;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(191,123,48,0.18), transparent 32%),
        linear-gradient(180deg, #faf7f1 0%, var(--bg) 100%);
    }}
    header {{
      padding: 28px 28px 20px;
      border-bottom: 1px solid rgba(0, 0, 0, 0.08);
      backdrop-filter: blur(3px);
    }}
    h1 {{
      margin: 0 0 10px;
      font-size: 30px;
      letter-spacing: 0.02em;
    }}
    .intro {{
      margin: 0;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.5;
    }}
    .stats {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      margin-top: 16px;
    }}
    .stat {{
      min-width: 160px;
      padding: 12px 14px;
      border-radius: 14px;
      background: rgba(255,255,255,0.72);
      border: 1px solid var(--line);
      box-shadow: 0 14px 28px rgba(42, 50, 60, 0.06);
    }}
    .stat small {{
      display: block;
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 4px;
    }}
    .stat strong {{
      font-size: 20px;
    }}
    main {{
      padding: 22px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 18px;
    }}
    .card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 18px;
      overflow: hidden;
      box-shadow: 0 20px 34px rgba(36, 44, 54, 0.08);
    }}
    .thumb {{
      display: block;
      padding: 14px 14px 0;
    }}
    .thumb img {{
      width: 100%;
      display: block;
      border-radius: 12px;
      border: 1px solid rgba(0,0,0,0.08);
      background: #faf8f4;
    }}
    .body {{
      padding: 14px 16px 16px;
    }}
    .eyebrow {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-bottom: 10px;
    }}
    .badge {{
      display: inline-flex;
      align-items: center;
      padding: 4px 8px;
      border-radius: 999px;
      font-size: 12px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.72);
    }}
    .badge.manual {{ color: var(--red); }}
    .badge.auto {{ color: var(--blue); }}
    h2 {{
      margin: 0 0 8px;
      font-size: 18px;
      line-height: 1.35;
    }}
    .meta, .metrics p {{
      margin: 6px 0;
      font-size: 13px;
      color: var(--muted);
      line-height: 1.5;
    }}
    .metrics {{
      margin-top: 12px;
      padding-top: 12px;
      border-top: 1px dashed var(--line);
    }}
    .profit {{ color: var(--green); font-weight: 700; }}
    .loss {{ color: var(--red); font-weight: 700; }}
    .actions {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 12px;
    }}
    .actions a {{
      text-decoration: none;
      color: var(--ink);
      padding: 8px 12px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.86);
      font-size: 13px;
    }}
    .actions a.primary {{
      border-color: rgba(42,115,217,0.24);
      background: rgba(42,115,217,0.12);
      color: var(--blue);
    }}
  </style>
</head>
<body>
  <header>
    <h1>{escape(parent_strategy_name)} 人工接管专用画廊</h1>
    <p class="intro">这里不再混入全部 1000 多张证据图，只保留当前活动仓位相关的图。人工接管仓优先排在前面，方便你直接扫图、扫成本，并且一眼看清净保本线、目标减仓线、目标小赚线。</p>
    <div class="stats">
      <div class="stat"><small>活动仓位图</small><strong>{len(rows)}</strong></div>
      <div class="stat"><small>人工接管仓</small><strong>{len(manual_rows)}</strong></div>
      <div class="stat"><small>自动未完结仓</small><strong>{len(auto_rows)}</strong></div>
      <div class="stat"><small>高优先级</small><strong>{len(high_priority_rows)}</strong></div>
      <div class="stat"><small>覆盖子策略</small><strong>{len({str(item.get("signal_id", "")) for item in rows})}</strong></div>
    </div>
  </header>
  <main>
    {cards_html}
  </main>
</body>
</html>
"""


def write_position_management_html(
    path: Path | str,
    *,
    parent_strategy_name: str,
    summary_rows: list[dict[str, object]],
    ledger_rows: list[dict[str, object]],
) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        build_position_management_html(
            html_path=target,
            parent_strategy_name=parent_strategy_name,
            summary_rows=summary_rows,
            ledger_rows=ledger_rows,
        ),
        encoding="utf-8",
    )
    return target


def build_position_management_html(
    *,
    html_path: Path,
    parent_strategy_name: str,
    summary_rows: list[dict[str, object]],
    ledger_rows: list[dict[str, object]],
) -> str:
    group_sections = []
    navigation_items = []
    for index, summary in enumerate(summary_rows, start=1):
        group_id = _build_management_group_anchor(index=index, summary=summary)
        members = [
            row
            for row in ledger_rows
            if _match_summary_group(row, summary)
        ]
        navigation_items.append(
            f'<a href="#{group_id}">{escape(str(summary.get("signal_name", summary.get("signal_id", ""))))} ({len(members)})</a>'
        )
        group_sections.append(
            _build_management_group_section(
                html_path=html_path,
                group_id=group_id,
                summary=summary,
                members=members,
            )
        )

    total_positions = sum(int(summary.get("active_position_count", 0)) for summary in summary_rows)
    negative_groups = sum(
        1 for summary in summary_rows if _to_decimal(summary.get("estimated_net_pnl_value_if_closed_now", 0)) < 0
    )
    positive_groups = sum(
        1 for summary in summary_rows if _to_decimal(summary.get("estimated_net_pnl_value_if_closed_now", 0)) >= 0
    )
    p1_groups = sum(1 for summary in summary_rows if int(summary.get("risk_priority_rank", 99)) == 1)
    p2_groups = sum(1 for summary in summary_rows if int(summary.get("risk_priority_rank", 99)) == 2)
    p4_groups = sum(1 for summary in summary_rows if int(summary.get("risk_priority_rank", 99)) == 4)
    p5_groups = sum(1 for summary in summary_rows if int(summary.get("risk_priority_rank", 99)) == 5)
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{escape(parent_strategy_name)} 人工处理清单</title>
  <style>
    :root {{
      --bg: #eef2ef;
      --panel: rgba(252, 255, 250, 0.94);
      --ink: #1f2731;
      --muted: #60707d;
      --line: #d7e1d7;
      --green: #1b7a61;
      --red: #b64d47;
      --amber: #b4792f;
      --blue: #2f72d6;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top right, rgba(47,114,214,0.10), transparent 28%),
        radial-gradient(circle at left center, rgba(180,121,47,0.11), transparent 24%),
        linear-gradient(180deg, #f8fbf8 0%, var(--bg) 100%);
    }}
    header {{
      padding: 28px;
      border-bottom: 1px solid rgba(0,0,0,0.08);
    }}
    h1 {{
      margin: 0 0 10px;
      font-size: 30px;
      letter-spacing: 0.02em;
    }}
    p.intro {{
      margin: 0;
      max-width: 960px;
      line-height: 1.55;
      color: var(--muted);
      font-size: 14px;
    }}
    .hero-stats {{
      margin-top: 18px;
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
    }}
    .hero-card {{
      min-width: 160px;
      padding: 12px 14px;
      border-radius: 14px;
      background: rgba(255,255,255,0.76);
      border: 1px solid var(--line);
      box-shadow: 0 14px 30px rgba(33, 42, 52, 0.05);
    }}
    .hero-card small {{
      display: block;
      margin-bottom: 4px;
      font-size: 12px;
      color: var(--muted);
    }}
    .hero-card strong {{
      font-size: 22px;
    }}
    nav.jump {{
      padding: 18px 24px 0;
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }}
    nav.jump a {{
      text-decoration: none;
      color: var(--blue);
      background: rgba(47,114,214,0.08);
      border: 1px solid rgba(47,114,214,0.16);
      padding: 8px 12px;
      border-radius: 999px;
      font-size: 13px;
    }}
    main {{
      padding: 20px 24px 30px;
      display: grid;
      gap: 16px;
    }}
    details.group {{
      border: 1px solid var(--line);
      border-radius: 18px;
      background: var(--panel);
      box-shadow: 0 18px 30px rgba(34, 42, 52, 0.05);
      overflow: hidden;
    }}
    details.group[open] {{
      border-color: rgba(47,114,214,0.18);
    }}
    summary {{
      list-style: none;
      cursor: pointer;
      padding: 18px 20px;
      display: grid;
      gap: 10px;
    }}
    summary::-webkit-details-marker {{ display: none; }}
    .summary-top {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }}
    .summary-top h2 {{
      margin: 0;
      font-size: 20px;
    }}
    .pill {{
      display: inline-flex;
      align-items: center;
      padding: 4px 9px;
      border-radius: 999px;
      font-size: 12px;
      border: 1px solid var(--line);
      color: var(--muted);
      background: rgba(255,255,255,0.8);
    }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 10px;
    }}
    .metric {{
      padding: 10px 12px;
      border-radius: 12px;
      background: rgba(255,255,255,0.72);
      border: 1px solid rgba(0,0,0,0.04);
    }}
    .metric small {{
      display: block;
      font-size: 11px;
      color: var(--muted);
      margin-bottom: 4px;
    }}
    .metric strong {{
      font-size: 16px;
    }}
    .content {{
      padding: 0 18px 18px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      overflow: hidden;
      border-radius: 14px;
    }}
    th, td {{
      padding: 10px 10px;
      border-bottom: 1px solid rgba(0,0,0,0.06);
      text-align: left;
      font-size: 13px;
      vertical-align: top;
    }}
    th {{
      font-size: 12px;
      color: var(--muted);
      background: rgba(255,255,255,0.72);
      position: sticky;
      top: 0;
    }}
    tr:last-child td {{
      border-bottom: none;
    }}
    .profit {{ color: var(--green); font-weight: 700; }}
    .loss {{ color: var(--red); font-weight: 700; }}
    .muted {{ color: var(--muted); }}
    .chart-link {{
      text-decoration: none;
      color: var(--blue);
      white-space: nowrap;
    }}
    .nowrap {{ white-space: nowrap; }}
  </style>
</head>
<body>
  <header>
    <h1>{escape(parent_strategy_name)} 人工处理清单</h1>
    <p class="intro">这页按子策略分组，把期末活动仓、平均持仓成本、净保本线、目标减仓线、目标小赚线和证据图链接放到一起。你人工处理时，先看分组汇总，再点进单笔图，效率会比直接翻 CSV 高很多。</p>
    <div class="hero-stats">
      <div class="hero-card"><small>分组数</small><strong>{len(summary_rows)}</strong></div>
      <div class="hero-card"><small>活动仓位</small><strong>{total_positions}</strong></div>
      <div class="hero-card"><small>P1 临近平仓线</small><strong>{p1_groups}</strong></div>
      <div class="hero-card"><small>P2 浮亏风险组</small><strong>{p2_groups}</strong></div>
      <div class="hero-card"><small>P4 达到小赚区</small><strong>{p4_groups}</strong></div>
      <div class="hero-card"><small>P5 达到减仓区</small><strong>{p5_groups}</strong></div>
      <div class="hero-card"><small>估算为正分组</small><strong>{positive_groups}</strong></div>
      <div class="hero-card"><small>估算为负分组</small><strong>{negative_groups}</strong></div>
    </div>
  </header>
  <nav class="jump">
    {' '.join(navigation_items)}
  </nav>
  <main>
    {''.join(group_sections)}
  </main>
</body>
</html>
"""


def write_total_position_management_html(
    path: Path | str,
    *,
    parent_strategy_name: str,
    total_summary_rows: list[dict[str, object]],
    summary_rows: list[dict[str, object]],
    group_detail_path: Path | str | None = None,
) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        build_total_position_management_html(
            html_path=target,
            parent_strategy_name=parent_strategy_name,
            total_summary_rows=total_summary_rows,
            summary_rows=summary_rows,
            group_detail_path=group_detail_path,
        ),
        encoding="utf-8",
    )
    return target


def build_total_position_management_html(
    *,
    html_path: Path,
    parent_strategy_name: str,
    total_summary_rows: list[dict[str, object]],
    summary_rows: list[dict[str, object]],
    group_detail_path: Path | str | None = None,
) -> str:
    group_detail_target = None if group_detail_path is None else Path(group_detail_path)
    bucket_mode_label = (
        "-"
        if not total_summary_rows
        else str(total_summary_rows[0].get("bucket_mode_label", total_summary_rows[0].get("bucket_mode", "-")))
    )
    group_anchor_by_key = {
        _summary_group_key(summary): _build_management_group_anchor(index=index, summary=summary)
        for index, summary in enumerate(summary_rows, start=1)
    }
    bucket_sections = []
    navigation_items = []
    for index, total_summary in enumerate(total_summary_rows, start=1):
        bucket_id = f"bucket-{index:02d}-{_sanitize_anchor(str(total_summary.get('bucket_key', 'bucket')))}"
        member_groups = [
            item
            for item in summary_rows
            if _match_total_bucket(item, total_summary)
        ]
        navigation_items.append(
            (
                f'<a href="#{bucket_id}">'
                f'{escape(str(total_summary.get("bucket_label", total_summary.get("playbook_action", ""))))} '
                f'({len(member_groups)})'
                f"</a>"
            )
        )
        bucket_sections.append(
            _build_total_management_bucket_section(
                html_path=html_path,
                bucket_id=bucket_id,
                total_summary=total_summary,
                member_groups=member_groups,
                group_anchor_by_key=group_anchor_by_key,
                group_detail_path=group_detail_target,
            )
        )

    total_positions = sum(int(item.get("active_position_count", 0)) for item in total_summary_rows)
    manual_positions = sum(int(item.get("manual_position_count", 0)) for item in total_summary_rows)
    auto_positions = sum(int(item.get("auto_position_count", 0)) for item in total_summary_rows)
    mixed_buckets = sum(1 for item in total_summary_rows if str(item.get("pool_state", "")) == "mixed")
    high_priority_buckets = sum(1 for item in total_summary_rows if int(item.get("risk_priority_rank", 99)) <= 2)
    total_estimated_net = sum(
        (_to_decimal(item.get("estimated_net_pnl_value_if_closed_now", 0)) for item in total_summary_rows),
        start=Decimal("0"),
    )
    net_class = "profit" if total_estimated_net >= 0 else "loss"
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{escape(parent_strategy_name)} 总持仓总览</title>
  <style>
    :root {{
      --bg: #f4f1ea;
      --panel: rgba(255, 253, 248, 0.96);
      --ink: #18212a;
      --muted: #607080;
      --line: #ddd3c5;
      --green: #176d59;
      --red: #b64d47;
      --blue: #2f72d6;
      --amber: #b4792f;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top right, rgba(47,114,214,0.12), transparent 26%),
        radial-gradient(circle at left center, rgba(180,121,47,0.13), transparent 28%),
        linear-gradient(180deg, #fbf8f2 0%, var(--bg) 100%);
    }}
    header {{
      padding: 28px;
      border-bottom: 1px solid rgba(0,0,0,0.08);
    }}
    h1 {{
      margin: 0 0 10px;
      font-size: 30px;
      letter-spacing: 0.02em;
    }}
    .intro {{
      margin: 0;
      max-width: 980px;
      line-height: 1.6;
      color: var(--muted);
      font-size: 14px;
    }}
    .hero-stats {{
      margin-top: 18px;
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
    }}
    .hero-card {{
      min-width: 170px;
      padding: 12px 14px;
      border-radius: 14px;
      background: rgba(255,255,255,0.82);
      border: 1px solid var(--line);
      box-shadow: 0 14px 30px rgba(33, 42, 52, 0.05);
    }}
    .hero-card small {{
      display: block;
      margin-bottom: 4px;
      font-size: 12px;
      color: var(--muted);
    }}
    .hero-card strong {{
      font-size: 22px;
    }}
    nav.jump {{
      padding: 18px 24px 0;
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }}
    nav.jump a {{
      text-decoration: none;
      color: var(--blue);
      background: rgba(47,114,214,0.08);
      border: 1px solid rgba(47,114,214,0.16);
      padding: 8px 12px;
      border-radius: 999px;
      font-size: 13px;
    }}
    main {{
      padding: 20px 24px 30px;
      display: grid;
      gap: 16px;
    }}
    details.bucket {{
      border: 1px solid var(--line);
      border-radius: 18px;
      background: var(--panel);
      box-shadow: 0 18px 30px rgba(34, 42, 52, 0.05);
      overflow: hidden;
    }}
    details.bucket[open] {{
      border-color: rgba(47,114,214,0.18);
    }}
    summary {{
      list-style: none;
      cursor: pointer;
      padding: 18px 20px;
      display: grid;
      gap: 10px;
    }}
    summary::-webkit-details-marker {{ display: none; }}
    .summary-top {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }}
    .summary-top h2 {{
      margin: 0;
      font-size: 20px;
    }}
    .pill {{
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 5px 10px;
      font-size: 12px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.74);
      color: var(--muted);
    }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 10px;
    }}
    .metric {{
      border-radius: 14px;
      padding: 12px;
      border: 1px solid rgba(0,0,0,0.06);
      background: rgba(255,255,255,0.78);
    }}
    .metric small {{
      display: block;
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 4px;
    }}
    .metric strong {{
      font-size: 16px;
      line-height: 1.45;
      word-break: break-word;
    }}
    .content {{
      padding: 0 20px 18px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    thead th {{
      text-align: left;
      padding: 12px 10px;
      color: var(--muted);
      font-weight: 600;
      border-bottom: 1px solid var(--line);
      white-space: nowrap;
    }}
    tbody td {{
      padding: 12px 10px;
      border-bottom: 1px solid rgba(0,0,0,0.05);
      vertical-align: top;
    }}
    tbody tr:last-child td {{
      border-bottom: none;
    }}
    .profit {{ color: var(--green); font-weight: 700; }}
    .loss {{ color: var(--red); font-weight: 700; }}
    .muted {{ color: var(--muted); }}
    .detail-link {{
      text-decoration: none;
      color: var(--blue);
      white-space: nowrap;
    }}
    .nowrap {{ white-space: nowrap; }}
  </style>
</head>
<body>
  <header>
    <h1>{escape(parent_strategy_name)} 总持仓总览</h1>
    <p class="intro">这页把自动等待止盈的仓和人工接管的仓合并到总成本视角里，当前总仓桶模式是 `{escape(bucket_mode_label)}`。你可以先看总成本、三线参考和风险优先级，再下钻到各个子策略分组。为避免把期权权利金与现货/永续价格直接混算，宽桶模式下依然会按价格域自动兀底拆桶。</p>
    <div class="hero-stats">
      <div class="hero-card"><small>合并模式</small><strong>{escape(bucket_mode_label)}</strong></div>
      <div class="hero-card"><small>总持仓桶数</small><strong>{len(total_summary_rows)}</strong></div>
      <div class="hero-card"><small>总活动仓位</small><strong>{total_positions}</strong></div>
      <div class="hero-card"><small>人工仓位</small><strong>{manual_positions}</strong></div>
      <div class="hero-card"><small>自动仓位</small><strong>{auto_positions}</strong></div>
      <div class="hero-card"><small>混合仓桶</small><strong>{mixed_buckets}</strong></div>
      <div class="hero-card"><small>高优先级桶</small><strong>{high_priority_buckets}</strong></div>
      <div class="hero-card"><small>总估算净盈亏</small><strong class="{net_class}">{escape(_fmt_decimal(total_estimated_net))}</strong></div>
    </div>
  </header>
  <nav class="jump">
    {' '.join(navigation_items)}
  </nav>
  <main>
    {''.join(bucket_sections)}
  </main>
</body>
</html>
"""


def _build_manual_review_card(html_path: Path, row: dict[str, object]) -> str:
    chart_href = _relative_href(html_path, row.get("chart_path", ""))
    pnl_value = _to_decimal(row.get("estimated_net_pnl_value_if_closed_now", 0))
    pnl_class = "profit" if pnl_value >= 0 else "loss"
    pool = str(row.get("manual_pool", "manual"))
    priority_label = str(row.get("group_risk_priority_label", ""))
    line_cluster = _format_reference_line_cluster(
        break_even_line=row.get("group_unified_close_reference_price"),
        reduce_line=row.get("group_target_reduce_reference_price"),
        small_profit_line=row.get("group_target_small_profit_reference_price"),
    )
    buffer_cluster = _format_reference_buffer_cluster(
        break_even_buffer=row.get("group_buffer_to_unified_close_pct"),
        reduce_buffer=row.get("group_buffer_to_target_reduce_pct"),
        small_profit_buffer=row.get("group_buffer_to_target_small_profit_pct"),
    )
    return "".join(
        [
            '<article class="card">',
            f'<a class="thumb" href="{escape(chart_href)}" target="_blank"><img src="{escape(chart_href)}" alt="{escape(str(row.get("signal_id", "")))}"/></a>',
            '<div class="body">',
            '<div class="eyebrow">',
            f'<span class="badge {escape(pool)}">{escape("人工接管" if pool == "manual" else "自动未完结")}</span>',
            f'<span class="badge">{escape(str(row.get("position_side", "")))}</span>',
            f'<span class="badge">{escape(str(row.get("playbook_name", "")))}</span>',
            (f'<span class="badge">{escape(priority_label)}</span>' if priority_label else ""),
            "</div>",
            f'<h2>{escape(str(row.get("signal_name", row.get("signal_id", ""))))}</h2>',
            f'<p class="meta"><strong>ID:</strong> {escape(str(row.get("evidence_id", "")))}</p>',
            f'<p class="meta"><strong>进场理由:</strong> {escape(_shorten(str(row.get("trigger_reason", "")), 120))}</p>',
            '<div class="metrics">',
            f'<p><strong>当前参考价:</strong> {escape(_fmt_decimal(row.get("latest_reference_price")))} | <strong>单笔保本价:</strong> {escape(_fmt_decimal(row.get("break_even_reference_price")))} </p>',
            f'<p><strong>三线参考:</strong> {escape(line_cluster)}</p>',
            f'<p><strong>三线距离:</strong> {escape(buffer_cluster)}</p>',
            f'<p><strong>估算净盈亏:</strong> <span class="{pnl_class}">{escape(_fmt_decimal(pnl_value))}</span> | <strong>仓位:</strong> {escape(_fmt_decimal(row.get("position_size")))} </p>',
            f'<p><strong>处理建议:</strong> {escape(str(row.get("group_suggested_action", "-")))} | <strong>优先级说明:</strong> {escape(_shorten(str(row.get("group_risk_priority_note", "")), 68))}</p>',
            f'<p><strong>入场时间:</strong> {escape(_fmt_ts(row.get("entry_ts")))} | <strong>接管时间:</strong> {escape(_fmt_ts(row.get("handoff_ts")))} </p>',
            "</div>",
            '<div class="actions">',
            f'<a class="primary" href="{escape(chart_href)}" target="_blank">查看证据图</a>',
            f'<a href="{escape(chart_href)}" download>下载 SVG</a>',
            "</div>",
            "</div>",
            "</article>",
        ]
    )


def _build_total_management_bucket_section(
    *,
    html_path: Path,
    bucket_id: str,
    total_summary: dict[str, object],
    member_groups: list[dict[str, object]],
    group_anchor_by_key: dict[tuple[str, ...], str],
    group_detail_path: Path | None,
) -> str:
    net_value = _to_decimal(total_summary.get("estimated_net_pnl_value_if_closed_now", 0))
    net_class = "profit" if net_value >= 0 else "loss"
    bucket_label = str(total_summary.get("bucket_label", ""))
    line_cluster = _format_reference_line_cluster(
        break_even_line=total_summary.get("unified_close_reference_price"),
        reduce_line=total_summary.get("target_reduce_reference_price"),
        small_profit_line=total_summary.get("target_small_profit_reference_price"),
    )
    buffer_cluster = _format_reference_buffer_cluster(
        break_even_buffer=total_summary.get("buffer_to_unified_close_pct"),
        reduce_buffer=total_summary.get("buffer_to_target_reduce_pct"),
        small_profit_buffer=total_summary.get("buffer_to_target_small_profit_pct"),
    )
    rows_html = "".join(
        _build_total_management_group_row(
            html_path=html_path,
            summary=summary,
            group_anchor=group_anchor_by_key.get(_summary_group_key(summary), ""),
            group_detail_path=group_detail_path,
        )
        for summary in member_groups
    )
    return f"""
<details class="bucket" id="{escape(bucket_id)}" open>
  <summary>
    <div class="summary-top">
      <h2>{escape(bucket_label)}</h2>
      <span class="pill">{escape(str(total_summary.get("bucket_mode_label", "-")))}</span>
      <span class="pill">{escape(_format_pool_state_label(total_summary.get("pool_state", "")))}</span>
      <span class="pill">{escape(str(total_summary.get("risk_priority_label", "")))}</span>
      <span class="pill">{escape(str(total_summary.get("price_domain_label", "-")))}</span>
      <span class="pill">{escape(str(total_summary.get("position_side", "-")))}</span>
    </div>
    <div class="metrics">
      <div class="metric"><small>合并模式</small><strong>{escape(str(total_summary.get("bucket_mode_label", "-")))}</strong></div>
      <div class="metric"><small>当前桶标签</small><strong>{escape(bucket_label or "-")}</strong></div>
      <div class="metric"><small>覆盖子策略</small><strong>{escape(str(total_summary.get("signal_coverage_count", 0)))}</strong></div>
      <div class="metric"><small>覆盖信号ID</small><strong>{escape(_shorten(str(total_summary.get("signal_ids", "-")), 120))}</strong></div>
      <div class="metric"><small>总活动仓位</small><strong>{escape(str(total_summary.get("active_position_count", 0)))}</strong></div>
      <div class="metric"><small>人工 / 自动</small><strong>{escape(str(total_summary.get("manual_position_count", 0)))} / {escape(str(total_summary.get("auto_position_count", 0)))}</strong></div>
      <div class="metric"><small>总仓位</small><strong>{escape(_fmt_decimal(total_summary.get("total_position_size")))}</strong></div>
      <div class="metric"><small>加权持仓价</small><strong>{escape(_fmt_decimal(total_summary.get("weighted_avg_entry_price")))}</strong></div>
      <div class="metric"><small>当前参考价</small><strong>{escape(_fmt_decimal(total_summary.get("latest_reference_price")))}</strong></div>
      <div class="metric"><small>净保本线</small><strong>{escape(_fmt_decimal(total_summary.get("unified_close_reference_price")))}</strong></div>
      <div class="metric"><small>目标减仓线</small><strong>{escape(_fmt_decimal(total_summary.get("target_reduce_reference_price")))}</strong></div>
      <div class="metric"><small>目标小赚线</small><strong>{escape(_fmt_decimal(total_summary.get("target_small_profit_reference_price")))}</strong></div>
      <div class="metric"><small>总估算净盈亏</small><strong class="{net_class}">{escape(_fmt_decimal(net_value))}</strong></div>
      <div class="metric"><small>总估算净收益率</small><strong>{escape(_fmt_decimal(total_summary.get("estimated_net_pnl_pct_if_closed_now")))}%</strong></div>
    </div>
    <div class="metrics">
      <div class="metric"><small>三线参考</small><strong>{escape(line_cluster)}</strong></div>
      <div class="metric"><small>三线距离</small><strong>{escape(buffer_cluster)}</strong></div>
      <div class="metric"><small>优先级说明</small><strong>{escape(str(total_summary.get("risk_priority_note", "-")))}</strong></div>
      <div class="metric"><small>建议动作</small><strong>{escape(str(total_summary.get("suggested_action", "-")))}</strong></div>
      <div class="metric"><small>价格域</small><strong>{escape(str(total_summary.get("price_domain_label", "-")))}</strong></div>
      <div class="metric"><small>来源覆盖</small><strong>{escape(_shorten(str(total_summary.get("source_labels", "-")), 120))}</strong></div>
      <div class="metric"><small>包含动作</small><strong>{escape(_shorten(str(total_summary.get("playbook_actions", "-")), 120))}</strong></div>
      <div class="metric"><small>包含 playbook</small><strong>{escape(_shorten(str(total_summary.get("playbook_names", "-")), 120))}</strong></div>
    </div>
  </summary>
  <div class="content">
    <table>
      <thead>
        <tr>
          <th>子策略</th>
          <th>动作</th>
          <th>来源</th>
          <th>池子</th>
          <th>活动仓</th>
          <th>总仓位</th>
          <th>加权持仓价</th>
          <th>当前参考价</th>
          <th>三线参考</th>
          <th>三线距离</th>
          <th>估算净盈亏</th>
          <th>优先级</th>
          <th>详情</th>
        </tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
  </div>
</details>
"""


def _build_management_group_section(
    *,
    html_path: Path,
    group_id: str,
    summary: dict[str, object],
    members: list[dict[str, object]],
) -> str:
    net_value = _to_decimal(summary.get("estimated_net_pnl_value_if_closed_now", 0))
    net_class = "profit" if net_value >= 0 else "loss"
    priority_label = str(summary.get("risk_priority_label", ""))
    line_cluster = _format_reference_line_cluster(
        break_even_line=summary.get("unified_close_reference_price"),
        reduce_line=summary.get("target_reduce_reference_price"),
        small_profit_line=summary.get("target_small_profit_reference_price"),
    )
    buffer_cluster = _format_reference_buffer_cluster(
        break_even_buffer=summary.get("buffer_to_unified_close_pct"),
        reduce_buffer=summary.get("buffer_to_target_reduce_pct"),
        small_profit_buffer=summary.get("buffer_to_target_small_profit_pct"),
    )
    rows_html = "".join(_build_management_row(html_path, row) for row in members)
    return f"""
<details class="group" id="{escape(group_id)}" open>
  <summary>
    <div class="summary-top">
      <h2>{escape(str(summary.get("signal_name", summary.get("signal_id", ""))))}</h2>
      <span class="pill">{escape(priority_label)}</span>
      <span class="pill">{escape(str(summary.get("manual_pool", "")))}</span>
      <span class="pill">{escape(str(summary.get("position_side", "")))}</span>
      <span class="pill">{escape(str(summary.get("playbook_name", "")))}</span>
    </div>
    <div class="metrics">
      <div class="metric"><small>活动仓位</small><strong>{escape(str(summary.get("active_position_count", 0)))}</strong></div>
      <div class="metric"><small>加权持仓价</small><strong>{escape(_fmt_decimal(summary.get("weighted_avg_entry_price")))}</strong></div>
      <div class="metric"><small>当前参考价</small><strong>{escape(_fmt_decimal(summary.get("latest_reference_price")))}</strong></div>
      <div class="metric"><small>净保本线</small><strong>{escape(_fmt_decimal(summary.get("unified_close_reference_price")))}</strong></div>
      <div class="metric"><small>目标减仓线</small><strong>{escape(_fmt_decimal(summary.get("target_reduce_reference_price")))}</strong></div>
      <div class="metric"><small>目标小赚线</small><strong>{escape(_fmt_decimal(summary.get("target_small_profit_reference_price")))}</strong></div>
      <div class="metric"><small>估算净盈亏</small><strong class="{net_class}">{escape(_fmt_decimal(net_value))}</strong></div>
      <div class="metric"><small>估算净收益率</small><strong>{escape(_fmt_decimal(summary.get("estimated_net_pnl_pct_if_closed_now")))}%</strong></div>
    </div>
    <div class="metrics">
      <div class="metric"><small>三线距离</small><strong>{escape(buffer_cluster)}</strong></div>
      <div class="metric"><small>三线参考</small><strong>{escape(line_cluster)}</strong></div>
      <div class="metric"><small>优先级说明</small><strong>{escape(str(summary.get("risk_priority_note", "-")))}</strong></div>
      <div class="metric"><small>建议动作</small><strong>{escape(str(summary.get("suggested_action", "-")))}</strong></div>
      <div class="metric"><small>净保本线价值缓冲</small><strong>{escape(_fmt_decimal(summary.get("buffer_to_unified_close_value")))}</strong></div>
      <div class="metric"><small>目标减仓价值缓冲</small><strong>{escape(_fmt_decimal(summary.get("buffer_to_target_reduce_value")))}</strong></div>
      <div class="metric"><small>目标小赚价值缓冲</small><strong>{escape(_fmt_decimal(summary.get("buffer_to_target_small_profit_value")))}</strong></div>
      <div class="metric"><small>加权单笔保本价</small><strong>{escape(_fmt_decimal(summary.get("weighted_break_even_reference_price")))}</strong></div>
    </div>
  </summary>
  <div class="content">
    <table>
      <thead>
        <tr>
          <th>时间</th>
          <th>仓位</th>
          <th>入场价</th>
          <th>保本价</th>
          <th>三线参考</th>
          <th>当前参考价</th>
          <th>三线距离</th>
          <th>估算净盈亏</th>
          <th>持有 bars</th>
          <th>证据</th>
          <th>进场理由</th>
        </tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
  </div>
</details>
"""


def _build_management_row(html_path: Path, row: dict[str, object]) -> str:
    pnl_value = _to_decimal(row.get("estimated_net_pnl_value_if_closed_now", 0))
    pnl_class = "profit" if pnl_value >= 0 else "loss"
    chart_href = _relative_href(html_path, row.get("chart_path", ""))
    line_cluster = _format_reference_line_cluster(
        break_even_line=row.get("group_unified_close_reference_price"),
        reduce_line=row.get("group_target_reduce_reference_price"),
        small_profit_line=row.get("group_target_small_profit_reference_price"),
    )
    buffer_cluster = _format_reference_buffer_cluster(
        break_even_buffer=row.get("group_buffer_to_unified_close_pct"),
        reduce_buffer=row.get("group_buffer_to_target_reduce_pct"),
        small_profit_buffer=row.get("group_buffer_to_target_small_profit_pct"),
    )
    return f"""
<tr>
  <td class="nowrap">{escape(_fmt_ts(row.get("entry_ts")))}</td>
  <td>{escape(_fmt_decimal(row.get("position_size")))}</td>
  <td>{escape(_fmt_decimal(row.get("entry_price")))}</td>
  <td>{escape(_fmt_decimal(row.get("break_even_reference_price")))}</td>
  <td>{escape(line_cluster)}</td>
  <td>{escape(_fmt_decimal(row.get("latest_reference_price")))}</td>
  <td>{escape(buffer_cluster)}</td>
  <td class="{pnl_class}">{escape(_fmt_decimal(pnl_value))}</td>
  <td>{escape(str(row.get("bars_since_entry_to_reference", "")))}</td>
  <td><a class="chart-link" href="{escape(chart_href)}" target="_blank">看图</a></td>
  <td class="muted">{escape(_shorten(str(row.get("trigger_reason", "")), 88))}</td>
</tr>
"""


def _build_total_management_group_row(
    *,
    html_path: Path,
    summary: dict[str, object],
    group_anchor: str,
    group_detail_path: Path | None,
) -> str:
    pnl_value = _to_decimal(summary.get("estimated_net_pnl_value_if_closed_now", 0))
    pnl_class = "profit" if pnl_value >= 0 else "loss"
    source_label = _format_source_label(
        source_market=summary.get("source_market"),
        source_inst_id=summary.get("source_inst_id"),
        source_bar=summary.get("source_bar"),
    )
    line_cluster = _format_reference_line_cluster(
        break_even_line=summary.get("unified_close_reference_price"),
        reduce_line=summary.get("target_reduce_reference_price"),
        small_profit_line=summary.get("target_small_profit_reference_price"),
    )
    buffer_cluster = _format_reference_buffer_cluster(
        break_even_buffer=summary.get("buffer_to_unified_close_pct"),
        reduce_buffer=summary.get("buffer_to_target_reduce_pct"),
        small_profit_buffer=summary.get("buffer_to_target_small_profit_pct"),
    )
    detail_html = escape(str(summary.get("signal_id", "")))
    if group_detail_path is not None and group_anchor:
        detail_href = _relative_href(html_path, group_detail_path) + f"#{group_anchor}"
        detail_html = f'<a class="detail-link" href="{escape(detail_href)}" target="_blank">分组详情</a>'
    return f"""
<tr>
  <td class="nowrap">{escape(str(summary.get("signal_name", summary.get("signal_id", ""))))}</td>
  <td>{escape(str(summary.get("playbook_action", "-")))}</td>
  <td>{escape(source_label)}</td>
  <td>{escape(str(summary.get("manual_pool", "")))}</td>
  <td>{escape(str(summary.get("active_position_count", 0)))}</td>
  <td>{escape(_fmt_decimal(summary.get("total_position_size")))}</td>
  <td>{escape(_fmt_decimal(summary.get("weighted_avg_entry_price")))}</td>
  <td>{escape(_fmt_decimal(summary.get("latest_reference_price")))}</td>
  <td>{escape(line_cluster)}</td>
  <td>{escape(buffer_cluster)}</td>
  <td class="{pnl_class}">{escape(_fmt_decimal(pnl_value))}</td>
  <td>{escape(str(summary.get("risk_priority_label", "-")))}</td>
  <td>{detail_html}</td>
</tr>
"""


def _format_reference_line_cluster(
    *,
    break_even_line: object,
    reduce_line: object,
    small_profit_line: object,
) -> str:
    return " / ".join(
        [
            f"净保本 { _fmt_decimal(break_even_line) }",
            f"减仓 { _fmt_decimal(reduce_line) }",
            f"小赚 { _fmt_decimal(small_profit_line) }",
        ]
    )


def _format_reference_buffer_cluster(
    *,
    break_even_buffer: object,
    reduce_buffer: object,
    small_profit_buffer: object,
) -> str:
    return " / ".join(
        [
            f"净保本 { _fmt_decimal(break_even_buffer) }%",
            f"减仓 { _fmt_decimal(reduce_buffer) }%",
            f"小赚 { _fmt_decimal(small_profit_buffer) }%",
        ]
    )


def _format_source_label(*, source_market: object, source_inst_id: object, source_bar: object) -> str:
    parts = [
        str(source_market or "").strip(),
        str(source_inst_id or "").strip(),
        str(source_bar or "").strip(),
    ]
    values = [item for item in parts if item]
    return " / ".join(values) if values else "-"


def _build_management_group_anchor(*, index: int, summary: dict[str, object]) -> str:
    return f"group-{index:02d}-{_sanitize_anchor(str(summary.get('signal_id', 'signal')))}"


def _format_pool_state_label(value: object) -> str:
    pool_state = str(value)
    labels = {
        "mixed": "混合仓",
        "manual_only": "全部人工",
        "auto_only": "全部自动",
    }
    return labels.get(pool_state, pool_state or "-")


def _summary_group_key(summary: dict[str, object]) -> tuple[str, ...]:
    keys = (
        "parent_strategy_id",
        "manual_pool",
        "underlying_family",
        "source_market",
        "source_inst_id",
        "source_bar",
        "playbook_id",
        "playbook_name",
        "playbook_action",
        "position_side",
        "signal_id",
        "signal_name",
    )
    return tuple(str(summary.get(key, "")) for key in keys)


def _match_summary_group(row: dict[str, object], summary: dict[str, object]) -> bool:
    return _summary_group_key(row) == _summary_group_key(summary)


def _match_total_bucket(summary: dict[str, object], total_summary: dict[str, object]) -> bool:
    bucket_mode = str(total_summary.get("bucket_mode", TOTAL_BUCKET_MODE_PLAYBOOK_SOURCE))
    return _build_total_bucket_membership_key(summary, bucket_mode=bucket_mode) == str(
        total_summary.get("bucket_key", "")
    )


def _build_total_bucket_membership_key(summary: dict[str, object], *, bucket_mode: str) -> str:
    parent_strategy_id = str(summary.get("parent_strategy_id", ""))
    parent_strategy_name = str(summary.get("parent_strategy_name", ""))
    position_side = str(summary.get("position_side", ""))
    playbook_action = str(summary.get("playbook_action", ""))
    source_market = str(summary.get("source_market", ""))
    source_inst_id = str(summary.get("source_inst_id", ""))
    source_bar = str(summary.get("source_bar", ""))
    price_domain_key = _build_total_bucket_price_domain_key(summary)
    if bucket_mode == TOTAL_BUCKET_MODE_PLAYBOOK_SOURCE:
        key = (
            bucket_mode,
            parent_strategy_id,
            parent_strategy_name,
            playbook_action,
            position_side,
            source_market,
            source_inst_id,
            source_bar,
        )
    elif bucket_mode == TOTAL_BUCKET_MODE_PLAYBOOK:
        key = (
            bucket_mode,
            parent_strategy_id,
            parent_strategy_name,
            playbook_action,
            position_side,
            price_domain_key,
        )
    else:
        key = (
            bucket_mode,
            parent_strategy_id,
            parent_strategy_name,
            position_side,
            price_domain_key,
        )
    return " | ".join(str(part) for part in key)


def _build_total_bucket_price_domain_key(summary: dict[str, object]) -> str:
    action = str(summary.get("playbook_action", ""))
    underlying_family = str(summary.get("underlying_family", ""))
    source_market = str(summary.get("source_market", ""))
    source_inst_id = str(summary.get("source_inst_id", ""))
    if action in {"SPOT_BUY", "SPOT_SELL", "SWAP_LONG", "SWAP_SHORT"}:
        return f"underlying::{underlying_family}"
    return f"instrument::{source_market}::{source_inst_id}"


def _relative_href(html_path: Path, raw_target: object) -> str:
    target = Path(str(raw_target))
    return Path(os.path.relpath(target, start=html_path.parent)).as_posix()


def _sanitize_anchor(value: str) -> str:
    safe = []
    for char in value:
        if char.isalnum() or char in {"-", "_"}:
            safe.append(char)
        else:
            safe.append("-")
    return "".join(safe).strip("-") or "group"


def _fmt_decimal(value: object) -> str:
    if value in {None, ""}:
        return "-"
    number = _to_decimal(value)
    text = f"{number:.6f}"
    return text.rstrip("0").rstrip(".")


def _fmt_ts(value: object) -> str:
    if value in {None, "", 0}:
        return "-"
    ts = int(value)
    return datetime.fromtimestamp(ts / 1000, SHANGHAI).strftime("%m-%d %H:%M")


def _shorten(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def _to_decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))
