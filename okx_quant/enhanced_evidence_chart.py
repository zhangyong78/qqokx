from __future__ import annotations

from decimal import Decimal
from html import escape
from pathlib import Path

from okx_quant.enhanced_models import EnhancedBacktestLabResult, EvidenceCandle
from okx_quant.models import Candle


SVG_WIDTH = 620
SVG_HEIGHT = 360
MARGIN_LEFT = 54
MARGIN_RIGHT = 18
MARGIN_TOP = 54
MARGIN_BOTTOM = 42
PLOT_WIDTH = SVG_WIDTH - MARGIN_LEFT - MARGIN_RIGHT
PLOT_HEIGHT = SVG_HEIGHT - MARGIN_TOP - MARGIN_BOTTOM
GRID_STROKE = "#d8dee6"
AXIS_TEXT = "#5a6573"
BG = "#f7f5f1"
BULL = "#178f5b"
BEAR = "#ca4f42"
ENTRY_LINE = "#2a7fff"
STOP_LINE = "#ca4f42"
TAKE_PROFIT_LINE = "#178f5b"
REFERENCE_LINE = "#8a5cf6"
TRIGGER_SHADE = "#ffe4a8"
FOLLOW_SHADE = "#dff5e8"
TRIGGER_BORDER = "#d88c00"


def export_evidence_chart_bundle(
    *,
    result: EnhancedBacktestLabResult,
    target_dir: Path | str,
    latest_reference_candles: dict[tuple[str, str], Candle] | None = None,
) -> dict[str, object]:
    latest_reference_candles = latest_reference_candles or {}
    target = Path(target_dir)
    target.mkdir(parents=True, exist_ok=True)

    event_by_evidence_id = {item.evidence_id: item for item in result.events}
    manifest_rows: list[dict[str, object]] = []
    chart_paths_by_evidence: dict[str, str] = {}

    for index, evidence in enumerate(result.evidences, start=1):
        event = event_by_evidence_id.get(evidence.evidence_id)
        if event is None:
            continue
        file_name = f"{index:04d}_{_sanitize_name(evidence.signal_id)}_{evidence.candle_ts}.svg"
        chart_path = target / file_name
        reference_candle = latest_reference_candles.get((evidence.source_market, evidence.source_inst_id))
        chart_path.write_text(
            build_evidence_chart_svg(
                result=result,
                evidence=evidence,
                event=event,
                reference_candle=reference_candle,
            ),
            encoding="utf-8",
        )
        chart_paths_by_evidence[evidence.evidence_id] = str(chart_path)
        manifest_rows.append(
            {
                "evidence_id": evidence.evidence_id,
                "signal_id": evidence.signal_id,
                "signal_name": evidence.signal_name,
                "playbook_id": evidence.playbook_id,
                "playbook_name": evidence.playbook_name,
                "lifecycle_status": event.lifecycle_status,
                "accepted": event.accepted,
                "chart_file_name": file_name,
                "chart_path": str(chart_path),
                "trigger_reason": evidence.trigger_reason,
                "evidence_summary": evidence.evidence_summary,
                "source_market": evidence.source_market,
                "source_inst_id": evidence.source_inst_id,
                "source_bar": evidence.source_bar,
            }
        )

    gallery_path = target / "index.html"
    gallery_path.write_text(
        build_evidence_gallery_html(
            result=result,
            manifest_rows=manifest_rows,
        ),
        encoding="utf-8",
    )

    return {
        "chart_dir": target,
        "gallery_html": gallery_path,
        "manifest_rows": manifest_rows,
        "chart_paths_by_evidence": chart_paths_by_evidence,
    }


def build_evidence_chart_svg(
    *,
    result: EnhancedBacktestLabResult,
    evidence,
    event,
    reference_candle: Candle | None,
) -> str:
    candles = _build_chart_candles(evidence)
    if not candles:
        raise ValueError(f"evidence {evidence.evidence_id!r} has no candles")

    reference_price = reference_candle.close if reference_candle is not None else None
    price_marks = [candle.high for candle in candles] + [candle.low for candle in candles]
    for maybe_price in (
        event.entry_price,
        event.stop_loss_price,
        event.take_profit_price,
        event.exit_price,
        reference_price,
    ):
        if maybe_price is not None:
            price_marks.append(maybe_price)

    min_price = min(price_marks)
    max_price = max(price_marks)
    if max_price == min_price:
        max_price = min_price + 1
    price_padding = (max_price - min_price) * Decimal("0.08")
    plot_min = min_price - price_padding
    plot_max = max_price + price_padding
    price_range = plot_max - plot_min

    candle_count = len(candles)
    step = PLOT_WIDTH / max(candle_count, 1)
    body_width = max(min(step * 0.56, 18), 4)

    svg_parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{SVG_WIDTH}" height="{SVG_HEIGHT}" viewBox="0 0 {SVG_WIDTH} {SVG_HEIGHT}">',
        f'<rect x="0" y="0" width="{SVG_WIDTH}" height="{SVG_HEIGHT}" fill="{BG}" rx="16" ry="16"/>',
        '<style>',
        "text { font-family: 'Segoe UI', 'Microsoft YaHei', sans-serif; }",
        "</style>",
    ]

    trigger_index = len(evidence.setup_candles) - 1 if evidence.setup_candles else 0
    if 0 <= trigger_index < candle_count:
        shade_x = MARGIN_LEFT + (trigger_index * step)
        svg_parts.append(
            f'<rect x="{shade_x:.2f}" y="{MARGIN_TOP}" width="{step:.2f}" height="{PLOT_HEIGHT}" fill="{TRIGGER_SHADE}" opacity="0.55"/>'
        )
    if evidence.followthrough_candles:
        follow_start = trigger_index + 1
        follow_width = len(evidence.followthrough_candles) * step
        shade_x = MARGIN_LEFT + (follow_start * step)
        svg_parts.append(
            f'<rect x="{shade_x:.2f}" y="{MARGIN_TOP}" width="{follow_width:.2f}" height="{PLOT_HEIGHT}" fill="{FOLLOW_SHADE}" opacity="0.35"/>'
        )

    for line_index in range(5):
        y = MARGIN_TOP + (line_index * PLOT_HEIGHT / 4)
        price_value = plot_max - (price_range * line_index / 4)
        svg_parts.append(
            f'<line x1="{MARGIN_LEFT}" y1="{y:.2f}" x2="{MARGIN_LEFT + PLOT_WIDTH}" y2="{y:.2f}" stroke="{GRID_STROKE}" stroke-width="1"/>'
        )
        svg_parts.append(
            f'<text x="{MARGIN_LEFT - 8}" y="{y + 4:.2f}" text-anchor="end" font-size="12" fill="{AXIS_TEXT}">{escape(_fmt_price(price_value))}</text>'
        )

    svg_parts.append(
        f'<line x1="{MARGIN_LEFT}" y1="{MARGIN_TOP + PLOT_HEIGHT}" x2="{MARGIN_LEFT + PLOT_WIDTH}" y2="{MARGIN_TOP + PLOT_HEIGHT}" stroke="#8f99a6" stroke-width="1.2"/>'
    )

    for idx, candle in enumerate(candles):
        x_center = MARGIN_LEFT + (idx + 0.5) * step
        wick_y1 = _price_to_y(candle.high, plot_min=plot_min, plot_max=plot_max)
        wick_y2 = _price_to_y(candle.low, plot_min=plot_min, plot_max=plot_max)
        open_y = _price_to_y(candle.open, plot_min=plot_min, plot_max=plot_max)
        close_y = _price_to_y(candle.close, plot_min=plot_min, plot_max=plot_max)
        body_y = min(open_y, close_y)
        body_h = max(abs(close_y - open_y), 1.2)
        is_bull = candle.close >= candle.open
        fill = BULL if is_bull else BEAR
        stroke = fill
        svg_parts.append(
            f'<line x1="{x_center:.2f}" y1="{wick_y1:.2f}" x2="{x_center:.2f}" y2="{wick_y2:.2f}" stroke="{stroke}" stroke-width="2"/>'
        )
        svg_parts.append(
            f'<rect x="{x_center - body_width / 2:.2f}" y="{body_y:.2f}" width="{body_width:.2f}" height="{body_h:.2f}" fill="{fill}" stroke="{stroke}" stroke-width="1.3" rx="1.2" ry="1.2"/>'
        )
        if idx == trigger_index:
            svg_parts.append(
                f'<rect x="{x_center - body_width / 2 - 2:.2f}" y="{body_y - 2:.2f}" width="{body_width + 4:.2f}" height="{body_h + 4:.2f}" fill="none" stroke="{TRIGGER_BORDER}" stroke-width="2" rx="2" ry="2"/>'
            )

    overlays = [
        ("Entry", event.entry_price, ENTRY_LINE),
        ("SL", event.stop_loss_price, STOP_LINE),
        ("TP", event.take_profit_price, TAKE_PROFIT_LINE),
        ("Ref", reference_price, REFERENCE_LINE),
    ]
    for label, price, color in overlays:
        if price is None:
            continue
        y = _price_to_y(price, plot_min=plot_min, plot_max=plot_max)
        svg_parts.append(
            f'<line x1="{MARGIN_LEFT}" y1="{y:.2f}" x2="{MARGIN_LEFT + PLOT_WIDTH}" y2="{y:.2f}" stroke="{color}" stroke-width="1.4" stroke-dasharray="6 5"/>'
        )
        svg_parts.append(
            f'<rect x="{MARGIN_LEFT + PLOT_WIDTH - 76}" y="{y - 10:.2f}" width="72" height="18" fill="{BG}" opacity="0.94"/>'
        )
        svg_parts.append(
            f'<text x="{MARGIN_LEFT + PLOT_WIDTH - 6}" y="{y + 4:.2f}" text-anchor="end" font-size="11" fill="{color}">{escape(label)} {escape(_fmt_price(price))}</text>'
        )

    header_left = f"{result.parent_strategy_name} | {evidence.signal_id}"
    header_right = f"{event.lifecycle_status} | {evidence.source_market}:{evidence.source_inst_id} {evidence.source_bar}"
    footer = f"evidence={evidence.evidence_id} | playbook={evidence.playbook_id} | trigger={evidence.trigger_reason}"
    svg_parts.extend(
        [
            f'<text x="{MARGIN_LEFT}" y="24" font-size="16" font-weight="700" fill="#1f2732">{escape(header_left)}</text>',
            f'<text x="{SVG_WIDTH - MARGIN_RIGHT}" y="24" font-size="12" text-anchor="end" fill="{AXIS_TEXT}">{escape(header_right)}</text>',
            f'<text x="{MARGIN_LEFT}" y="42" font-size="12" fill="{AXIS_TEXT}">{escape(evidence.evidence_summary)}</text>',
            f'<text x="{MARGIN_LEFT}" y="{SVG_HEIGHT - 14}" font-size="11" fill="{AXIS_TEXT}">{escape(_shorten(footer, 108))}</text>',
        ]
    )
    svg_parts.append("</svg>")
    return "".join(svg_parts)


def build_evidence_gallery_html(
    *,
    result: EnhancedBacktestLabResult,
    manifest_rows: list[dict[str, object]],
) -> str:
    cards: list[str] = []
    for item in manifest_rows:
        file_name = escape(str(item["chart_file_name"]))
        title = escape(f'{item["signal_id"]} | {item["lifecycle_status"]}')
        summary = escape(_shorten(str(item["evidence_summary"]), 120))
        trigger = escape(_shorten(str(item["trigger_reason"]), 120))
        evidence_id = escape(str(item["evidence_id"]))
        cards.append(
            "".join(
                [
                    '<article class="card">',
                    f'<a href="{file_name}" target="_blank"><img src="{file_name}" alt="{title}"/></a>',
                    f"<h3>{title}</h3>",
                    f"<p><strong>ID:</strong> {evidence_id}</p>",
                    f"<p><strong>Summary:</strong> {summary}</p>",
                    f"<p><strong>Trigger:</strong> {trigger}</p>",
                    "</article>",
                ]
            )
        )
    cards_html = "\n".join(cards)
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{escape(result.parent_strategy_name)} 证据图画廊</title>
  <style>
    :root {{
      --bg: #f4f1ea;
      --card: #fffdf8;
      --ink: #1e2732;
      --muted: #5c6775;
      --line: #ded7ca;
      --accent: #b96c31;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(185,108,49,0.16), transparent 34%),
        linear-gradient(180deg, #faf7f1 0%, var(--bg) 100%);
      color: var(--ink);
    }}
    header {{
      padding: 28px 28px 18px;
      border-bottom: 1px solid var(--line);
      backdrop-filter: blur(4px);
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 28px;
      letter-spacing: 0.02em;
    }}
    p.meta {{
      margin: 0;
      color: var(--muted);
      font-size: 14px;
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
      border-radius: 16px;
      padding: 14px;
      box-shadow: 0 18px 34px rgba(39, 47, 58, 0.07);
    }}
    .card img {{
      display: block;
      width: 100%;
      border-radius: 12px;
      border: 1px solid rgba(0,0,0,0.07);
      background: #faf8f4;
    }}
    .card h3 {{
      margin: 12px 0 10px;
      font-size: 16px;
    }}
    .card p {{
      margin: 6px 0;
      font-size: 13px;
      line-height: 1.45;
      color: var(--muted);
    }}
    strong {{
      color: var(--ink);
    }}
  </style>
</head>
<body>
  <header>
    <h1>{escape(result.parent_strategy_name)} 证据图画廊</h1>
    <p class="meta">共 {len(manifest_rows)} 张证据小图。点击图片可查看原图，方便人工快速扫图复核。</p>
  </header>
  <main>
    {cards_html}
  </main>
</body>
</html>
"""


def _build_chart_candles(evidence) -> list[EvidenceCandle]:
    candles = list(evidence.setup_candles)
    trigger = evidence.trigger_candle
    if trigger is not None and (not candles or candles[-1].ts != trigger.ts):
        candles.append(trigger)
    candles.extend(evidence.followthrough_candles)
    return candles


def _price_to_y(price, *, plot_min, plot_max) -> float:
    if plot_max == plot_min:
        return MARGIN_TOP + PLOT_HEIGHT / 2
    ratio = (price - plot_min) / (plot_max - plot_min)
    return float(MARGIN_TOP + PLOT_HEIGHT - (ratio * PLOT_HEIGHT))


def _sanitize_name(value: str) -> str:
    safe = []
    for char in value:
        if char.isalnum() or char in {"_", "-"}:
            safe.append(char)
        else:
            safe.append("_")
    return "".join(safe).strip("_") or "chart"


def _shorten(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def _fmt_price(value) -> str:
    text = f"{value:.6f}"
    return text.rstrip("0").rstrip(".")
