from __future__ import annotations

import json
from html import escape
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from okx_quant.btc_market_analyzer import (
    BtcMarketAnalysis,
    BtcMarketAnalyzerConfig,
    TimeframeAnalysis,
    analyze_btc_market_from_client,
    btc_market_analysis_payload,
    load_btc_market_email_notifier,
)
from okx_quant.analysis_email_validation import (
    build_recent_email_validation_summary,
    load_latest_email_validation_payload,
    refresh_email_validation_report,
)
from okx_quant.candle_cache import load_candle_cache
from okx_quant.mini_chart import LINE_COLORS, MiniChartOverlay, render_candles_png_base64
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import (
    analysis_report_dir_path,
    load_btc_market_email_state,
    save_btc_market_email_state,
)
from okx_quant.strategy_profiles import read_strategy_bundle


DEFAULT_DIGEST_SYMBOLS: tuple[str, ...] = (
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "BNB-USDT-SWAP",
    "SOL-USDT-SWAP",
    "DOGE-USDT-SWAP",
)
DEFAULT_DEFERRED_RELEASE_SLOT = "08:00"


@dataclass(frozen=True)
class DigestLeader:
    symbol: str
    label: str
    summary: str
    score: float
    explicit: bool = True


@dataclass(frozen=True)
class MultiCoinMarketDigest:
    generated_at: str
    symbols: tuple[str, ...]
    analyses: tuple[BtcMarketAnalysis, ...]
    strongest_long: DigestLeader
    weakest_short: DigestLeader
    best_trade_candidate: DigestLeader


def analyze_multi_coin_market(
    client,
    *,
    symbols: Iterable[str] = DEFAULT_DIGEST_SYMBOLS,
    config: BtcMarketAnalyzerConfig | None = None,
) -> MultiCoinMarketDigest:
    analyses = tuple(
        analyze_btc_market_from_client(client, symbol=symbol, config=config)
        for symbol in tuple(symbols)
    )
    generated_at = analyses[0].generated_at if analyses else datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    return MultiCoinMarketDigest(
        generated_at=generated_at,
        symbols=tuple(item.symbol for item in analyses),
        analyses=analyses,
        strongest_long=_pick_strongest_long(analyses),
        weakest_short=_pick_weakest_short(analyses),
        best_trade_candidate=_pick_best_trade_candidate(analyses),
    )


def multi_coin_market_digest_payload(digest: MultiCoinMarketDigest) -> dict[str, object]:
    return {
        "generated_at": digest.generated_at,
        "symbols": list(digest.symbols),
        "leaders": {
            "strongest_long": _leader_payload(digest.strongest_long),
            "weakest_short": _leader_payload(digest.weakest_short),
            "best_trade_candidate": _leader_payload(digest.best_trade_candidate),
        },
        "analyses": [btc_market_analysis_payload(item) for item in digest.analyses],
    }


def multi_coin_market_digest_json(digest: MultiCoinMarketDigest) -> str:
    return json.dumps(multi_coin_market_digest_payload(digest), ensure_ascii=False, indent=2)


def save_multi_coin_market_digest(
    digest: MultiCoinMarketDigest,
    *,
    path: Path | None = None,
    base_dir: Path | None = None,
) -> Path:
    if path is None:
        report_dir = analysis_report_dir_path(base_dir)
        report_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = report_dir / f"multi_coin_market_digest_{timestamp}.json"
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(multi_coin_market_digest_json(digest), encoding="utf-8")
    return path


def build_multi_coin_market_email_subject(digest: MultiCoinMarketDigest) -> str:
    return f"[QQOKX] 5币行情简报 | 多头焦点={digest.strongest_long.label} | 空头焦点={digest.weakest_short.label}"


def build_multi_coin_market_email_body(digest: MultiCoinMarketDigest) -> str:
    return _build_multi_coin_market_email_body(digest, validation_summary=None)


def _build_multi_coin_market_email_body(
    digest: MultiCoinMarketDigest,
    *,
    validation_summary: dict[str, object] | None,
) -> str:
    email_state = load_btc_market_email_state()
    viewpoints = build_multi_coin_market_viewpoints(digest)
    resolved_validation_summary = validation_summary if validation_summary is not None else _load_recent_validation_summary()
    last_sent_at = _parse_iso_datetime(email_state.get("last_sent_at", ""))
    lines = [
        "简明结论：",
        f"- 做多最强：{_leader_headline(digest.strongest_long)}",
        f"- 做空最弱：{_leader_headline(digest.weakest_short)}",
        f"- 最值得跟踪做单：{digest.best_trade_candidate.label}。{digest.best_trade_candidate.summary}",
        "",
        "明确观点：",
        *[f"- {item['asset']}：{item['stance']}。{item['summary']}" for item in viewpoints],
        "",
        "最近复盘：",
        *_build_recent_validation_text_lines(resolved_validation_summary, viewpoints=viewpoints),
        "",
        "分币摘要：",
    ]
    for analysis in digest.analyses:
        lines.extend(_build_coin_section(analysis, last_sent_at=last_sent_at))
    return "\n".join(lines)


def build_multi_coin_market_email_html(
    digest: MultiCoinMarketDigest,
    *,
    chart_image_map: dict[str, dict[str, str]] | None = None,
    overlay_legend_map: dict[str, dict[str, str]] | None = None,
) -> str:
    return _build_multi_coin_market_email_html(
        digest,
        chart_image_map=chart_image_map,
        overlay_legend_map=overlay_legend_map,
        validation_summary=None,
    )


def _build_multi_coin_market_email_html(
    digest: MultiCoinMarketDigest,
    *,
    chart_image_map: dict[str, dict[str, str]] | None = None,
    overlay_legend_map: dict[str, dict[str, str]] | None = None,
    validation_summary: dict[str, object] | None,
) -> str:
    email_state = load_btc_market_email_state()
    viewpoints = build_multi_coin_market_viewpoints(digest)
    resolved_validation_summary = validation_summary if validation_summary is not None else _load_recent_validation_summary()
    last_sent_at = _parse_iso_datetime(email_state.get("last_sent_at", ""))
    strongest_long_asset = digest.strongest_long.label.upper()
    weakest_short_asset = digest.weakest_short.label.upper()
    summary_rows = [
        ("做多最强", _leader_headline(digest.strongest_long)),
        ("做空最弱", _leader_headline(digest.weakest_short)),
        ("最值得跟踪做单", f"{digest.best_trade_candidate.label}。{digest.best_trade_candidate.summary}"),
    ]
    summary_html = "".join(
        f"""
        <tr>
            <td style="padding: 6px 0; color: #34495e; font-size: 14px; line-height: 1.7;">
                - <strong>{escape(label)}</strong>：{escape(content)}
            </td>
        </tr>
        """
        for label, content in summary_rows
    )
    viewpoint_html = "".join(
        f"""
        <tr>
            <td style="padding: 6px 0; color: #34495e; font-size: 14px; line-height: 1.7;">
                - <strong>{escape(str(item['asset']))}</strong>：{escape(str(item['stance']))}。{escape(str(item['summary']))}
            </td>
        </tr>
        """
        for item in viewpoints
    )
    validation_html = _build_recent_validation_html(resolved_validation_summary, viewpoints=viewpoints)
    coin_cards_html = "".join(
        _build_coin_card_html(
            analysis,
            last_sent_at=last_sent_at,
            strongest_long_asset=strongest_long_asset,
            weakest_short_asset=weakest_short_asset,
            chart_image_map=chart_image_map or {},
            overlay_legend_map=overlay_legend_map or {},
        )
        for analysis in digest.analyses
    )
    headline = build_multi_coin_market_email_subject(digest)
    generated_at = _format_generated_at_display(digest.generated_at)
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{escape(headline)}</title>
</head>
<body style="margin: 0; padding: 0; font-family: 'Microsoft YaHei', Arial, sans-serif; background-color: #f5f7fa;">
    <table width="100%" border="0" cellspacing="0" cellpadding="0" style="background-color: #f5f7fa;">
        <tr>
            <td align="center" style="padding: 20px 0;">
                <table width="650" border="0" cellspacing="0" cellpadding="0" style="background-color: #ffffff; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.05);">
                    <tr>
                        <td style="background-color: #2c3e50; padding: 18px 24px;">
                            <h1 style="margin: 0; color: #ffffff; font-size: 20px; font-weight: 600;">{escape(headline)}</h1>
                            <p style="margin: 8px 0 0 0; color: #bdc3c7; font-size: 14px;">生成时间：{escape(generated_at)}</p>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 20px 24px; background-color: #e8f4fd;">
                            <h2 style="margin: 0 0 12px 0; color: #2980b9; font-size: 16px; font-weight: 600;">简明结论</h2>
                            <table width="100%" border="0" cellspacing="0" cellpadding="0">
                                {summary_html}
                            </table>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 20px 24px; background-color: #f6fbf8; border-top: 1px solid #d5eadb;">
                            <h2 style="margin: 0 0 12px 0; color: #1f7a4d; font-size: 16px; font-weight: 600;">明确观点</h2>
                            <table width="100%" border="0" cellspacing="0" cellpadding="0">
                                {viewpoint_html}
                            </table>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 20px 24px; background-color: #fff8ed; border-top: 1px solid #f3dfb2;">
                            <h2 style="margin: 0 0 12px 0; color: #9a6700; font-size: 16px; font-weight: 600;">最近复盘</h2>
                            <table width="100%" border="0" cellspacing="0" cellpadding="0">
                                {validation_html}
                            </table>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 24px;">
                            <h2 style="margin: 0 0 16px 0; color: #2c3e50; font-size: 16px; font-weight: 600;">分币摘要</h2>
                            {coin_cards_html}
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 16px 24px; background-color: #f8f9fa; text-align: center; font-size: 12px; color: #95a5a6;">
                            本简报仅供参考，不构成任何投资建议。请结合自身风险承受能力理性决策。
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
</body>
</html>"""


def send_multi_coin_market_email(
    digest: MultiCoinMarketDigest,
    *,
    report_path: Path | None = None,
) -> bool:
    notifier = load_btc_market_email_notifier()
    if notifier is None or not notifier.enabled:
        return False
    release_due_pending_multi_coin_market_emails(
        scheduled_release_slot=DEFAULT_DEFERRED_RELEASE_SLOT,
        update_email_state=False,
    )
    prepared = prepare_multi_coin_market_email(digest, report_path=report_path)
    _deliver_email_message(
        notifier,
        subject=prepared["subject"],
        body=prepared["body"],
        html_body=prepared["html_body"],
    )
    archive_path = Path(str(prepared["archive_path"]))
    save_btc_market_email_state(
        last_sent_at=datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        last_subject=str(prepared["subject"]),
        last_report_path=str(archive_path if archive_path is not None else report_path) if (archive_path is not None or report_path is not None) else "",
    )
    return True


def prepare_multi_coin_market_email(
    digest: MultiCoinMarketDigest,
    *,
    report_path: Path | None = None,
    delivery_status: str = "sent",
    scheduled_release_slot: str = "",
    analysis_slot: str = "",
) -> dict[str, object]:
    validation_summary = _load_recent_validation_summary()
    subject = build_multi_coin_market_email_subject(digest)
    body = _build_multi_coin_market_email_body(digest, validation_summary=validation_summary)
    overlay_map = build_multi_coin_overlay_map(digest)
    chart_image_map = build_multi_coin_chart_image_map(digest, overlay_map=overlay_map)
    overlay_legend_map = {
        symbol: {
            "1H": build_overlay_legend_html(overlays.get("1H", default_symbol_overlays())),
            "4H": build_overlay_legend_html(overlays.get("4H", default_4h_overlays())),
            "1D": build_overlay_legend_html(overlays.get("1D", default_4h_overlays())),
            "1W": build_overlay_legend_html(overlays.get("1W", default_4h_overlays())),
        }
        for symbol, overlays in overlay_map.items()
    }
    html_body = _build_multi_coin_market_email_html(
        digest,
        chart_image_map=chart_image_map,
        overlay_legend_map=overlay_legend_map,
        validation_summary=validation_summary,
    )
    archive_path = archive_multi_coin_market_email(
        digest,
        subject=subject,
        body=body,
        html_body=html_body,
        report_path=report_path,
        delivery_status=delivery_status,
        scheduled_release_slot=scheduled_release_slot,
        analysis_slot=analysis_slot,
    )
    return {
        "subject": subject,
        "body": body,
        "html_body": html_body,
        "archive_path": archive_path,
    }


def archive_pending_multi_coin_market_email(
    digest: MultiCoinMarketDigest,
    *,
    report_path: Path | None = None,
    scheduled_release_slot: str = DEFAULT_DEFERRED_RELEASE_SLOT,
    analysis_slot: str = "",
) -> Path:
    prepared = prepare_multi_coin_market_email(
        digest,
        report_path=report_path,
        delivery_status="pending_morning_release",
        scheduled_release_slot=scheduled_release_slot,
        analysis_slot=analysis_slot,
    )
    return Path(str(prepared["archive_path"]))


def release_pending_multi_coin_market_emails(
    *,
    scheduled_release_slot: str = DEFAULT_DEFERRED_RELEASE_SLOT,
    update_email_state: bool = True,
) -> int:
    return _release_pending_email_archive_meta_paths(
        _iter_pending_email_archive_meta_paths(scheduled_release_slot=scheduled_release_slot),
        scheduled_release_slot=scheduled_release_slot,
        update_email_state=update_email_state,
    )


def release_due_pending_multi_coin_market_emails(
    *,
    scheduled_release_slot: str = DEFAULT_DEFERRED_RELEASE_SLOT,
    update_email_state: bool = True,
    now: datetime | None = None,
) -> int:
    return _release_pending_email_archive_meta_paths(
        _iter_due_pending_email_archive_meta_paths(
            scheduled_release_slot=scheduled_release_slot,
            now=now,
        ),
        scheduled_release_slot=scheduled_release_slot,
        update_email_state=update_email_state,
    )


def _release_pending_email_archive_meta_paths(
    meta_paths: list[Path],
    *,
    scheduled_release_slot: str,
    update_email_state: bool,
) -> int:
    notifier = load_btc_market_email_notifier()
    if notifier is None or not notifier.enabled:
        return 0
    released_count = 0
    last_archive_path = ""
    last_subject = ""
    for meta_path in meta_paths:
        metadata = _load_archive_metadata(meta_path)
        if not metadata:
            continue
        subject = str(metadata.get("subject", "") or "").strip()
        html_path = Path(str(metadata.get("archive_html_path", "") or "").strip())
        text_path = Path(str(metadata.get("archive_text_path", "") or "").strip())
        if not subject or not html_path.exists() or not text_path.exists():
            continue
        body = text_path.read_text(encoding="utf-8")
        html_body = html_path.read_text(encoding="utf-8")
        _deliver_email_message(notifier, subject=subject, body=body, html_body=html_body)
        released_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        metadata["delivery_status"] = "released"
        metadata["released_at"] = released_at
        metadata["released_by_slot"] = scheduled_release_slot
        meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        released_count += 1
        last_archive_path = str(html_path)
        last_subject = subject
    if released_count > 0 and update_email_state:
        save_btc_market_email_state(
            last_sent_at=datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            last_subject=last_subject,
            last_report_path=last_archive_path,
        )
    return released_count


def _deliver_email_message(notifier, *, subject: str, body: str, html_body: str) -> None:
    sender = getattr(notifier, "_send", None)
    if callable(sender):
        sender(subject, body, html_body=html_body)
    else:
        notifier.notify_async(subject, body, html_body=html_body)


def build_multi_coin_market_viewpoints(digest: MultiCoinMarketDigest) -> list[dict[str, object]]:
    viewpoints: list[dict[str, object]] = []
    for analysis in digest.analyses:
        asset = _asset_name(analysis.symbol)
        tf4h = _find_timeframe(analysis, "4H")
        tf1h = _find_timeframe(analysis, "1H")
        stance = _coin_view_stance(analysis, tf4h, tf1h)
        summary = _coin_view_summary(analysis, tf4h, tf1h)
        viewpoints.append(
            {
                "symbol": analysis.symbol,
                "asset": asset,
                "stance": stance,
                "summary": summary,
                "direction": analysis.direction,
                "score": analysis.score,
                "confidence": _pct(analysis.confidence),
                "focus_reason": _coin_view_focus_reason(analysis, tf4h, tf1h),
                "invalidation": _coin_view_invalidation(analysis, tf4h, tf1h),
            }
        )
    return viewpoints


def _load_recent_validation_summary() -> dict[str, object] | None:
    _refresh_recent_validation_summary_if_needed()
    payload = load_latest_email_validation_payload()
    if not payload:
        return None
    return build_recent_email_validation_summary(payload, recent_email_limit=20)


def _refresh_recent_validation_summary_if_needed() -> None:
    payload = load_latest_email_validation_payload()
    if payload is not None:
        return
    try:
        refresh_email_validation_report(archive_limit=60)
    except Exception:
        return


def _build_recent_validation_text_lines(
    summary: dict[str, object] | None,
    *,
    viewpoints: list[dict[str, object]] | None = None,
) -> list[str]:
    if not summary:
        return ["- 暂无本地复盘汇总。可先运行 scripts/run_multi_coin_email_validation.py 生成。"]
    overall = summary.get("overall") if isinstance(summary.get("overall"), dict) else {}
    actionable = summary.get("actionable") if isinstance(summary.get("actionable"), dict) else {}
    by_symbol = summary.get("by_symbol") if isinstance(summary.get("by_symbol"), dict) else {}
    highlights = summary.get("highlights") if isinstance(summary.get("highlights"), dict) else {}
    email_count = int(summary.get("email_count", 0) or 0)
    sample_count = int(summary.get("sample_count", 0) or 0)
    actionable_count = int(summary.get("actionable_sample_count", 0) or 0)
    generated_at = str(summary.get("generated_at", "") or "").strip()
    lines = [
        (
            f"- 基于最近一次本地复盘汇总，覆盖最近 {email_count} 封已发送邮件，"
            f"共 {sample_count} 个样本。"
        ),
        (
            f"- 总体命中率：{_summary_hit_rate_text(overall)}"
            f"（已完成 {overall.get('completed', 0)}，有效 {overall.get('effective', 0)}，"
            f"部分有效 {overall.get('partial', 0)}，失效 {overall.get('invalid', 0)}，"
            f"待验证 {overall.get('pending', 0)}）。"
        ),
        (
            f"- 明确观点命中率：{_summary_hit_rate_text(actionable)}"
            f"（样本 {actionable_count}，24H 平均回报 {_summary_avg_return_text(actionable, 'avg_return_24h_pct')}）。"
        ),
        *_build_recent_validation_highlight_lines(highlights),
    ]
    for item in _build_recent_validation_action_items(highlights, viewpoints=viewpoints):
        lines.append(f"- {item['title']}：{item['headline']}")
        for detail_line in item.get("detail_lines", []):
            text = str(detail_line or "").strip()
            if text:
                lines.append(f"  {text}")
    lines.append(f"- 最近复盘汇总生成时间：{generated_at or '-'}")
    if by_symbol:
        lines.append("- 各币种最近命中率简表：")
        for symbol, item in by_symbol.items():
            if not isinstance(item, dict):
                continue
            asset = str(symbol).split("-")[0].upper()
            lines.append(
                f"- {asset}：命中率 {_summary_hit_rate_text(item)} | "
                f"已完成 {item.get('completed', 0)} | 待验证 {item.get('pending', 0)} | "
                f"24H 平均回报 {_summary_avg_return_text(item, 'avg_return_24h_pct')}"
            )
    return lines


def _build_recent_validation_html(
    summary: dict[str, object] | None,
    *,
    viewpoints: list[dict[str, object]] | None = None,
) -> str:
    if not summary:
        return """
        <tr>
            <td style="padding: 6px 0; color: #7c5f10; font-size: 14px; line-height: 1.7;">
                - 暂无本地复盘汇总。可先运行 <strong>scripts/run_multi_coin_email_validation.py</strong> 生成。
            </td>
        </tr>
        """
    overall = summary.get("overall") if isinstance(summary.get("overall"), dict) else {}
    actionable = summary.get("actionable") if isinstance(summary.get("actionable"), dict) else {}
    by_symbol = summary.get("by_symbol") if isinstance(summary.get("by_symbol"), dict) else {}
    highlights = summary.get("highlights") if isinstance(summary.get("highlights"), dict) else {}
    email_count = int(summary.get("email_count", 0) or 0)
    sample_count = int(summary.get("sample_count", 0) or 0)
    actionable_count = int(summary.get("actionable_sample_count", 0) or 0)
    generated_at = str(summary.get("generated_at", "") or "").strip() or "-"
    rows = [
        f"基于最近一次本地复盘汇总，覆盖最近 {email_count} 封已发送邮件，共 {sample_count} 个样本。",
        (
            f"总体命中率：{_summary_hit_rate_text(overall)}"
            f"（已完成 {overall.get('completed', 0)}，有效 {overall.get('effective', 0)}，"
            f"部分有效 {overall.get('partial', 0)}，失效 {overall.get('invalid', 0)}，"
            f"待验证 {overall.get('pending', 0)}）。"
        ),
        (
            f"明确观点命中率：{_summary_hit_rate_text(actionable)}"
            f"（样本 {actionable_count}，24H 平均回报 {_summary_avg_return_text(actionable, 'avg_return_24h_pct')}）。"
        ),
    ]
    rows.extend(_build_recent_validation_highlight_lines(highlights))
    for item in _build_recent_validation_action_items(highlights, viewpoints=viewpoints):
        rows.append(f"{item['title']}：{item['headline']}")
        for detail_line in item.get("detail_lines", []):
            text = str(detail_line or "").strip()
            if text:
                rows.append(text)
    rows.append(f"最近复盘汇总生成时间：{generated_at}")
    summary_html = "".join(
        f"""
        <tr>
            <td style="padding: 6px 0; color: #7c5f10; font-size: 14px; line-height: 1.7;">
                - {escape(row)}
            </td>
        </tr>
        """
        for row in rows
    )
    highlight_cards_html = _build_recent_validation_highlight_cards_html(highlights, viewpoints=viewpoints)
    if not by_symbol:
        return highlight_cards_html + summary_html
    symbol_table = """
        <tr>
            <td style="padding: 10px 0 6px 0; color: #7c5f10; font-size: 14px; line-height: 1.7; font-weight: 600;">
                各币种最近命中率简表
            </td>
        </tr>
        <tr>
            <td style="padding: 0;">
                <table width="100%" border="0" cellspacing="0" cellpadding="0" style="border-collapse: collapse; font-size: 12px; color: #7c5f10;">
                    <tr>
                        <td style="padding: 6px 8px; border-bottom: 1px solid #ecd7ab; font-weight: 600;">币种</td>
                        <td style="padding: 6px 8px; border-bottom: 1px solid #ecd7ab; font-weight: 600;">命中率</td>
                        <td style="padding: 6px 8px; border-bottom: 1px solid #ecd7ab; font-weight: 600;">已完成</td>
                        <td style="padding: 6px 8px; border-bottom: 1px solid #ecd7ab; font-weight: 600;">待验证</td>
                        <td style="padding: 6px 8px; border-bottom: 1px solid #ecd7ab; font-weight: 600;">24H均回报</td>
                    </tr>
    """
    for symbol, item in by_symbol.items():
        if not isinstance(item, dict):
            continue
        symbol_table += f"""
                    <tr>
                        <td style="padding: 6px 8px; border-bottom: 1px solid #f3e5c5;">{escape(str(symbol).split('-')[0].upper())}</td>
                        <td style="padding: 6px 8px; border-bottom: 1px solid #f3e5c5;">{escape(_summary_hit_rate_text(item))}</td>
                        <td style="padding: 6px 8px; border-bottom: 1px solid #f3e5c5;">{escape(str(item.get('completed', 0)))}</td>
                        <td style="padding: 6px 8px; border-bottom: 1px solid #f3e5c5;">{escape(str(item.get('pending', 0)))}</td>
                        <td style="padding: 6px 8px; border-bottom: 1px solid #f3e5c5;">{escape(_summary_avg_return_text(item, 'avg_return_24h_pct'))}</td>
                    </tr>
        """
    symbol_table += """
                </table>
            </td>
        </tr>
    """
    return highlight_cards_html + summary_html + symbol_table


def _build_recent_validation_highlight_lines(highlights: dict[str, object]) -> list[str]:
    if not highlights:
        return []
    lines: list[str] = []
    best = highlights.get("best_symbol") if isinstance(highlights.get("best_symbol"), dict) else None
    worst = highlights.get("worst_symbol") if isinstance(highlights.get("worst_symbol"), dict) else None
    notable_change = str(highlights.get("notable_change", "") or "").strip()
    if best is not None:
        best_summary = best.get("summary") if isinstance(best.get("summary"), dict) else {}
        lines.append(
            f"- 命中率最高币种：{best.get('asset', '-')}"
            f"（命中率 {_summary_hit_rate_text(best_summary)}，已完成 {best_summary.get('completed', 0)}）。"
        )
    if worst is not None:
        worst_summary = worst.get("summary") if isinstance(worst.get("summary"), dict) else {}
        lines.append(
            f"- 命中率最低币种：{worst.get('asset', '-')}"
            f"（命中率 {_summary_hit_rate_text(worst_summary)}，已完成 {worst_summary.get('completed', 0)}）。"
        )
    if notable_change:
        lines.append(f"- 最值得关注的变化：{notable_change}")
    return lines


def _build_recent_validation_action_items(
    highlights: dict[str, object],
    *,
    viewpoints: list[dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    if not highlights:
        return []
    viewpoint_by_asset: dict[str, dict[str, object]] = {}
    for item in viewpoints or []:
        if not isinstance(item, dict):
            continue
        asset = str(item.get("asset", "") or "").strip().upper()
        if asset:
            viewpoint_by_asset[asset] = item
    items: list[dict[str, object]] = []
    best = highlights.get("best_symbol") if isinstance(highlights.get("best_symbol"), dict) else None
    worst = highlights.get("worst_symbol") if isinstance(highlights.get("worst_symbol"), dict) else None
    if best is not None:
        best_asset = str(best.get("asset", "") or "").strip().upper()
        best_summary = best.get("summary") if isinstance(best.get("summary"), dict) else {}
        best_view = viewpoint_by_asset.get(best_asset)
        best_stance = _recent_validation_viewpoint_stance(best_view)
        best_reason = _recent_validation_focus_reason(best_view)
        best_invalidation = _recent_validation_invalidation(best_view)
        best_headline = best_asset
        if best_stance:
            best_headline += f" | 当前观点：{best_stance}"
        detail_lines = [f"命中率：{_summary_hit_rate_text(best_summary)}"]
        if best_reason:
            detail_lines.append(f"理由：{best_reason}")
        if best_invalidation:
            detail_lines.append(f"失效条件：{best_invalidation}")
        items.append(
            {
                "title": "今日优先跟踪",
                "headline": best_headline,
                "detail_lines": detail_lines,
                "accent": "#175cd3",
                "background": "#eef4ff",
            }
        )
        one_trade_headline = f"优先看 {best_asset} 的{_recent_validation_trade_side(best_view)}"
        one_trade_detail_lines: list[str] = []
        if best_reason:
            one_trade_detail_lines.append(f"前提：{best_reason}")
        if best_invalidation:
            one_trade_detail_lines.append(f"若{best_invalidation}，先不做")
        items.append(
            {
                "title": "若只做一笔",
                "headline": one_trade_headline,
                "detail_lines": one_trade_detail_lines,
                "accent": "#7a5af8",
                "background": "#f4f3ff",
            }
        )
    if worst is not None:
        worst_asset = str(worst.get("asset", "") or "").strip().upper()
        worst_summary = worst.get("summary") if isinstance(worst.get("summary"), dict) else {}
        worst_view = viewpoint_by_asset.get(worst_asset)
        worst_stance = _recent_validation_viewpoint_stance(worst_view)
        worst_invalidation = _recent_validation_invalidation(worst_view)
        worst_headline = worst_asset
        if worst_stance:
            worst_headline += f" | 当前观点：{worst_stance}"
        worst_detail_lines = [
            f"最近命中率：{_summary_hit_rate_text(worst_summary)}",
            "处理：先等信号重新收敛再碰",
        ]
        if worst_invalidation:
            worst_detail_lines.append(f"观察条件：{worst_invalidation}")
        items.append(
            {
                "title": "今日谨慎对待",
                "headline": worst_headline,
                "detail_lines": worst_detail_lines,
                "accent": "#b42318",
                "background": "#fff5f4",
            }
        )
    return items


def _recent_validation_viewpoint_stance(viewpoint: dict[str, object] | None) -> str:
    if viewpoint is None:
        return ""
    return str(viewpoint.get("stance", "") or "").strip()


def _recent_validation_focus_reason(viewpoint: dict[str, object] | None) -> str:
    if viewpoint is None:
        return ""
    return str(viewpoint.get("focus_reason", "") or "").strip()


def _recent_validation_invalidation(viewpoint: dict[str, object] | None) -> str:
    if viewpoint is None:
        return ""
    return str(viewpoint.get("invalidation", "") or "").strip()


def _recent_validation_trade_side(viewpoint: dict[str, object] | None) -> str:
    if viewpoint is None:
        return "顺势侧"
    stance = str(viewpoint.get("stance", "") or "").strip()
    direction = str(viewpoint.get("direction", "") or "").strip().lower()
    if "做多" in stance or "偏多" in stance or direction == "long":
        return "多头侧"
    if "做空" in stance or "偏空" in stance or direction == "short":
        return "空头侧"
    return "观望侧"


def _build_recent_validation_highlight_cards_html(
    highlights: dict[str, object],
    *,
    viewpoints: list[dict[str, object]] | None = None,
) -> str:
    if not highlights:
        return ""
    cards: list[str] = []
    best = highlights.get("best_symbol") if isinstance(highlights.get("best_symbol"), dict) else None
    worst = highlights.get("worst_symbol") if isinstance(highlights.get("worst_symbol"), dict) else None
    notable_change = str(highlights.get("notable_change", "") or "").strip()
    if best is not None:
        best_summary = best.get("summary") if isinstance(best.get("summary"), dict) else {}
        cards.append(
            _recent_validation_card_html(
                title="命中率最高币种",
                accent="#1f7a4d",
                background="#f3fcf6",
                body=f"{best.get('asset', '-')} | 命中率 {_summary_hit_rate_text(best_summary)} | 已完成 {best_summary.get('completed', 0)}",
            )
        )
    if worst is not None:
        worst_summary = worst.get("summary") if isinstance(worst.get("summary"), dict) else {}
        cards.append(
            _recent_validation_card_html(
                title="命中率最低币种",
                accent="#b42318",
                background="#fff5f4",
                body=f"{worst.get('asset', '-')} | 命中率 {_summary_hit_rate_text(worst_summary)} | 已完成 {worst_summary.get('completed', 0)}",
            )
        )
    if notable_change:
        cards.append(
            _recent_validation_card_html(
                title="最值得关注的变化",
                accent="#9a6700",
                background="#fff9eb",
                body=notable_change,
            )
        )
    for item in _build_recent_validation_action_items(highlights, viewpoints=viewpoints):
        cards.append(
            _recent_validation_card_html(
                title=item["title"],
                accent=item["accent"],
                background=item["background"],
                body=item["headline"],
                detail_lines=tuple(str(line or "").strip() for line in item.get("detail_lines", [])),
            )
        )
    if not cards:
        return ""
    rows: list[str] = []
    for index in range(0, len(cards), 2):
        left = cards[index]
        right = cards[index + 1] if index + 1 < len(cards) else ""
        rows.append(
            f"""
                    <tr>
                        <td width="50%" valign="top" style="padding: 0 6px 8px 0;">{left}</td>
                        <td width="50%" valign="top" style="padding: 0 0 8px 6px;">{right}</td>
                    </tr>
            """
        )
    return (
        """
        <tr>
            <td style="padding: 4px 0 10px 0;">
                <table width="100%" border="0" cellspacing="0" cellpadding="0">
        """
        + "".join(rows)
        + """
                </table>
            </td>
        </tr>
        """
    )


def _recent_validation_card_html(
    *,
    title: str,
    accent: str,
    background: str,
    body: str,
    detail_lines: tuple[str, ...] = (),
) -> str:
    filtered_lines = tuple(line for line in detail_lines if str(line or "").strip())
    detail_html = "".join(_recent_validation_detail_line_html(line=line, accent=accent) for line in filtered_lines)
    return f"""
    <table width="100%" border="0" cellspacing="0" cellpadding="0" style="border: 1px solid {accent}; border-left: 4px solid {accent}; border-radius: 12px; background: {background}; box-shadow: 0 6px 18px rgba(15, 23, 42, 0.06);">
        <tr>
            <td style="padding: 12px 14px;">
                <div style="margin-bottom: 8px;">
                    <span style="display: inline-block; padding: 3px 8px; border-radius: 999px; background: #ffffff; color: {accent}; font-size: 11px; font-weight: 700; letter-spacing: 0.2px;">{escape(title)}</span>
                </div>
                <div style="font-size: 15px; line-height: 1.5; color: #101828; font-weight: 700;">{escape(body)}</div>
                {detail_html}
            </td>
        </tr>
    </table>
    """


def _recent_validation_detail_line_html(*, line: str, accent: str) -> str:
    label, content = _split_recent_validation_detail_line(line)
    if label and content:
        return (
            f'<div style="margin-top: 8px; padding: 7px 9px; background: #ffffff; border-radius: 8px; border: 1px solid #e6eaf0;">'
            f'<span style="font-size: 12px; font-weight: 700; color: {accent};">{escape(label)}：</span>'
            f'<span style="font-size: 12px; line-height: 1.6; color: #475467;">{escape(content)}</span>'
            f"</div>"
        )
    return (
        f'<div style="margin-top: 8px; padding: 7px 9px; background: #ffffff; border-radius: 8px; border: 1px solid #e6eaf0; font-size: 12px; line-height: 1.6; color: #475467;">'
        f"{escape(line)}"
        f"</div>"
    )


def _split_recent_validation_detail_line(line: str) -> tuple[str, str]:
    text = str(line or "").strip()
    if not text or "：" not in text:
        return "", text
    label, content = text.split("：", 1)
    return label.strip(), content.strip()


def _summary_hit_rate_text(summary: dict[str, object]) -> str:
    return f"{float(summary.get('hit_rate_pct', 0) or 0):.2f}%"


def _summary_avg_return_text(summary: dict[str, object], key: str) -> str:
    value = summary.get(key)
    if isinstance(value, (int, float)):
        return f"{float(value):.4f}%"
    return "-"


def _build_coin_section(analysis: BtcMarketAnalysis, *, last_sent_at: datetime | None) -> list[str]:
    asset = _asset_name(analysis.symbol)
    tf4h = _find_timeframe(analysis, "4H")
    tf1h = _find_timeframe(analysis, "1H")
    lines = [
        f"- {asset} | 综合={_direction_label(analysis.direction)} | 分数={analysis.score} | 置信度={_pct(analysis.confidence)}",
        f"  4H：{_timeframe_line(tf4h)}",
        f"  1H：{_timeframe_line(tf1h)}",
        f"  跟踪：{_coin_tracking_summary(analysis, tf4h, tf1h)}",
    ]
    recent_events = _collect_recent_events((tf4h, tf1h), last_sent_at=last_sent_at)
    if recent_events:
        lines.append(f"  新形态：{'; '.join(recent_events[:3])}")
    else:
        lines.append("  新形态：上次发送后没有新的代表性K线")
    return lines


def _coin_tracking_summary(
    analysis: BtcMarketAnalysis,
    tf4h: TimeframeAnalysis | None,
    tf1h: TimeframeAnalysis | None,
) -> str:
    if tf4h is not None and tf1h is not None and tf4h.direction == tf1h.direction and tf4h.direction in {"long", "short"}:
        return f"4H/1H 同向偏{_direction_label(tf4h.direction)}，可优先盯它"
    return f"当前以{_direction_label(analysis.direction)}为主，但节奏还要看 1H 变化"


def _coin_view_stance(
    analysis: BtcMarketAnalysis,
    tf4h: TimeframeAnalysis | None,
    tf1h: TimeframeAnalysis | None,
) -> str:
    if tf4h is not None and tf1h is not None and tf4h.direction == tf1h.direction == "long":
        return "优先做多"
    if tf4h is not None and tf1h is not None and tf4h.direction == tf1h.direction == "short":
        return "优先做空"
    if analysis.direction == "long" and analysis.score >= 4:
        return "偏多跟踪"
    if analysis.direction == "short" and analysis.score <= -4:
        return "偏空跟踪"
    return "暂观望"


def _coin_view_summary(
    analysis: BtcMarketAnalysis,
    tf4h: TimeframeAnalysis | None,
    tf1h: TimeframeAnalysis | None,
) -> str:
    if tf4h is not None and tf1h is not None and tf4h.direction == tf1h.direction and tf4h.direction in {"long", "short"}:
        return (
            f"4H 与 1H 同向，综合分数 {analysis.score}，置信度 {_pct(analysis.confidence)}，"
            f"更适合按 {_direction_label(tf4h.direction)} 方向处理。"
        )
    lead_reason = analysis.reason[0] if analysis.reason else "当前缺少足够强的主导信号。"
    return f"综合分数 {analysis.score}，置信度 {_pct(analysis.confidence)}，核心依据：{lead_reason}"


def _coin_view_focus_reason(
    analysis: BtcMarketAnalysis,
    tf4h: TimeframeAnalysis | None,
    tf1h: TimeframeAnalysis | None,
) -> str:
    if tf4h is not None and tf1h is not None and tf4h.direction == tf1h.direction == "long":
        return "4H 与 1H 同向偏多，顺势一致性最好"
    if tf4h is not None and tf1h is not None and tf4h.direction == tf1h.direction == "short":
        return "4H 与 1H 同向偏空，顺势压制最清晰"
    if analysis.direction == "long" and analysis.score >= 4:
        return "综合分数仍偏强，但需要 1H 继续确认"
    if analysis.direction == "short" and analysis.score <= -4:
        return "综合分数仍偏弱，但需要 1H 继续确认"
    lead_reason = analysis.reason[0] if analysis.reason else ""
    return lead_reason


def _coin_view_invalidation(
    analysis: BtcMarketAnalysis,
    tf4h: TimeframeAnalysis | None,
    tf1h: TimeframeAnalysis | None,
) -> str:
    if tf4h is not None and tf1h is not None and tf4h.direction == tf1h.direction == "long":
        return "1H 与 4H 不再同向偏多"
    if tf4h is not None and tf1h is not None and tf4h.direction == tf1h.direction == "short":
        return "1H 与 4H 不再同向偏空"
    if analysis.direction == "long" and analysis.score >= 4:
        return "综合分数回落到 3 分以下，或 1H 明显转弱"
    if analysis.direction == "short" and analysis.score <= -4:
        return "综合分数回到 -3 分以上，或 1H 明显转强"
    return "1H 与 4H 分歧继续扩大"


def _collect_recent_events(
    items: tuple[TimeframeAnalysis | None, ...],
    *,
    last_sent_at: datetime | None,
) -> list[str]:
    threshold_ms = 0
    if last_sent_at is not None:
        threshold_ms = int(last_sent_at.astimezone(timezone.utc).timestamp() * 1000)
    rows: list[tuple[int, str]] = []
    for item in items:
        if item is None:
            continue
        for event in item.focus_events:
            if threshold_ms and event.ts <= threshold_ms:
                continue
            rows.append((event.ts, f"{item.timeframe} {event.label}"))
    rows.sort(key=lambda item: item[0], reverse=True)
    unique: list[str] = []
    for _, text in rows:
        if text not in unique:
            unique.append(text)
    return unique


def _pick_strongest_long(analyses: tuple[BtcMarketAnalysis, ...]) -> DigestLeader:
    candidates = [item for item in analyses if item.direction == "long"]
    if candidates:
        best = max(candidates, key=lambda item: (_long_strength_score(item), _tradeability_score(item), item.symbol))
        return DigestLeader(
            symbol=best.symbol,
            label=_asset_name(best.symbol),
            summary=_leader_summary(best),
            score=_long_strength_score(best),
            explicit=True,
        )
    backup = max(analyses, key=lambda item: (_long_strength_score(item), _tradeability_score(item), item.symbol))
    return DigestLeader(
        symbol=backup.symbol,
        label=_asset_name(backup.symbol),
        summary=f"当前没有明确强多头，{_asset_name(backup.symbol)} 只是离转强最近。",
        score=_long_strength_score(backup),
        explicit=False,
    )


def _pick_weakest_short(analyses: tuple[BtcMarketAnalysis, ...]) -> DigestLeader:
    candidates = [item for item in analyses if item.direction == "short"]
    if candidates:
        best = min(candidates, key=lambda item: (_short_strength_score(item), -_tradeability_score(item), item.symbol))
        return DigestLeader(
            symbol=best.symbol,
            label=_asset_name(best.symbol),
            summary=_leader_summary(best),
            score=_short_strength_score(best),
            explicit=True,
        )
    backup = min(analyses, key=lambda item: (_short_strength_score(item), -_tradeability_score(item), item.symbol))
    return DigestLeader(
        symbol=backup.symbol,
        label=_asset_name(backup.symbol),
        summary=f"当前没有明确强空头，{_asset_name(backup.symbol)} 只是离转弱最近。",
        score=_short_strength_score(backup),
        explicit=False,
    )


def _pick_best_trade_candidate(analyses: tuple[BtcMarketAnalysis, ...]) -> DigestLeader:
    best = max(analyses, key=lambda item: (_tradeability_score(item), abs(item.score), item.symbol))
    return DigestLeader(
        symbol=best.symbol,
        label=_asset_name(best.symbol),
        summary=_leader_summary(best),
        score=_tradeability_score(best),
        explicit=True,
    )


def _leader_summary(analysis: BtcMarketAnalysis) -> str:
    tf4h = _find_timeframe(analysis, "4H")
    tf1h = _find_timeframe(analysis, "1H")
    if tf4h is not None and tf1h is not None and tf4h.direction == tf1h.direction and tf4h.direction in {"long", "short"}:
        return f"4H 和 1H 都偏{_direction_label(tf4h.direction)}，综合分数 {analysis.score}"
    return f"综合分数 {analysis.score}，最近形态最活跃"


def _long_strength_score(analysis: BtcMarketAnalysis) -> float:
    return float(analysis.score) + float(analysis.confidence) * 10 + _alignment_bonus(analysis)


def _short_strength_score(analysis: BtcMarketAnalysis) -> float:
    return float(analysis.score) - float(analysis.confidence) * 10 - _alignment_bonus(analysis)


def _tradeability_score(analysis: BtcMarketAnalysis) -> float:
    tf4h = _find_timeframe(analysis, "4H")
    tf1h = _find_timeframe(analysis, "1H")
    focus_bonus = sum(len(item.focus_events[:3]) for item in (tf4h, tf1h) if item is not None)
    return abs(float(analysis.score)) + float(analysis.confidence) * 10 + _alignment_bonus(analysis) + focus_bonus


def _alignment_bonus(analysis: BtcMarketAnalysis) -> float:
    tf4h = _find_timeframe(analysis, "4H")
    tf1h = _find_timeframe(analysis, "1H")
    if tf4h is None or tf1h is None:
        return 0.0
    if tf4h.direction == tf1h.direction and tf4h.direction in {"long", "short"}:
        return 4.0
    return 0.0


def _find_timeframe(analysis: BtcMarketAnalysis, timeframe: str) -> TimeframeAnalysis | None:
    for item in analysis.timeframes:
        if item.timeframe == timeframe:
            return item
    return None


def _timeframe_line(item: TimeframeAnalysis | None) -> str:
    if item is None:
        return "无数据"
    return f"{_direction_label(item.direction)} | 分数={item.score} | 核心={item.reason[0] if item.reason else '暂无'}"


def _build_coin_card_html(
    analysis: BtcMarketAnalysis,
    *,
    last_sent_at: datetime | None,
    strongest_long_asset: str,
    weakest_short_asset: str,
    chart_image_map: dict[str, dict[str, str]],
    overlay_legend_map: dict[str, dict[str, str]],
) -> str:
    asset = _asset_name(analysis.symbol)
    tf4h = _find_timeframe(analysis, "4H")
    tf1h = _find_timeframe(analysis, "1H")
    border_color, header_bg, title_color, accent_color = _coin_card_palette(
        asset=asset,
        strongest_long_asset=strongest_long_asset,
        weakest_short_asset=weakest_short_asset,
    )
    recent_events = _collect_recent_events((tf4h, tf1h), last_sent_at=last_sent_at)
    recent_events_text = "; ".join(recent_events[:3]) if recent_events else "上次发送后没有新的代表性K线"
    header_summary = f"综合={_direction_label(analysis.direction)} | 分数={analysis.score} | 置信度={_pct(analysis.confidence)}"
    chart_html = _build_coin_chart_html(
        asset=asset,
        symbol=analysis.symbol,
        chart_image_map=chart_image_map,
        overlay_legends=overlay_legend_map.get(analysis.symbol, {}),
    )
    return f"""
    <table width="100%" border="0" cellspacing="0" cellpadding="0" style="border: 1px solid {border_color}; border-radius: 6px; margin-bottom: 16px;">
        <tr>
            <td style="padding: 12px 16px; background-color: {header_bg}; border-bottom: 1px solid {border_color};">
                <table width="100%" border="0" cellspacing="0" cellpadding="0">
                    <tr>
                        <td style="font-size: 15px; font-weight: 600; color: {title_color};">{escape(asset)}</td>
                        <td align="right" style="font-size: 13px; color: {accent_color};">{escape(header_summary)}</td>
                    </tr>
                </table>
            </td>
        </tr>
        <tr>
            <td style="padding: 16px;">
                {chart_html}
                <table width="100%" border="0" cellspacing="0" cellpadding="0" style="font-size: 13px; color: #34495e; line-height: 1.7;">
                    <tr>
                        <td style="padding: 4px 0;"><strong>4H 周期：</strong>{escape(_timeframe_line(tf4h))}</td>
                    </tr>
                    <tr>
                        <td style="padding: 4px 0;"><strong>1H 周期：</strong>{escape(_timeframe_line(tf1h))}</td>
                    </tr>
                    <tr>
                        <td style="padding: 4px 0;"><strong>跟踪提示：</strong>{escape(_coin_tracking_summary(analysis, tf4h, tf1h))}</td>
                    </tr>
                    <tr>
                        <td style="padding: 4px 0;"><strong>关注新形态：</strong>{escape(recent_events_text)}</td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
    """


def _coin_card_palette(
    *,
    asset: str,
    strongest_long_asset: str,
    weakest_short_asset: str,
) -> tuple[str, str, str, str]:
    if asset == weakest_short_asset:
        return "#e74c3c", "#fef5f5", "#e74c3c", "#e74c3c"
    if asset == strongest_long_asset:
        return "#27ae60", "#f4fdf4", "#27ae60", "#7f8c8d"
    return "#e0e6ed", "#f8f9fa", "#2c3e50", "#7f8c8d"


def archive_multi_coin_market_email(
    digest: MultiCoinMarketDigest,
    *,
    subject: str,
    body: str,
    html_body: str,
    report_path: Path | None,
    delivery_status: str = "sent",
    scheduled_release_slot: str = "",
    analysis_slot: str = "",
) -> Path:
    archive_dir = analysis_report_dir_path() / "email_archives"
    archive_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    base_name = f"multi_coin_market_digest_email_{stamp}"
    html_path = archive_dir / f"{base_name}.html"
    text_path = archive_dir / f"{base_name}.txt"
    meta_path = archive_dir / f"{base_name}.json"
    html_path.write_text(html_body, encoding="utf-8")
    text_path.write_text(body, encoding="utf-8")
    metadata = {
        "subject": subject,
        "generated_at": digest.generated_at,
        "symbols": list(digest.symbols),
        "archive_html_path": str(html_path),
        "archive_text_path": str(text_path),
        "report_path": str(report_path) if report_path is not None else "",
        "viewpoints": build_multi_coin_market_viewpoints(digest),
        "digest_payload": multi_coin_market_digest_payload(digest),
        "delivery_status": str(delivery_status or "").strip() or "sent",
        "scheduled_release_slot": str(scheduled_release_slot or "").strip(),
        "analysis_slot": str(analysis_slot or "").strip(),
        "archived_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    return html_path


def _iter_pending_email_archive_meta_paths(*, scheduled_release_slot: str) -> list[Path]:
    archive_dir = analysis_report_dir_path() / "email_archives"
    if not archive_dir.exists():
        return []
    rows: list[tuple[str, Path]] = []
    for meta_path in sorted(archive_dir.glob("multi_coin_market_digest_email_*.json")):
        metadata = _load_archive_metadata(meta_path)
        if not metadata:
            continue
        if str(metadata.get("delivery_status", "") or "").strip() != "pending_morning_release":
            continue
        if str(metadata.get("scheduled_release_slot", "") or "").strip() != scheduled_release_slot:
            continue
        generated_at = str(metadata.get("generated_at", "") or "").strip()
        rows.append((generated_at, meta_path))
    rows.sort(key=lambda item: item[0])
    return [item[1] for item in rows]


def _iter_due_pending_email_archive_meta_paths(
    *,
    scheduled_release_slot: str,
    now: datetime | None = None,
) -> list[Path]:
    now_bjt = _as_beijing_time(now or datetime.now(timezone.utc))
    release_minutes = _slot_minutes(scheduled_release_slot)
    rows: list[Path] = []
    for meta_path in _iter_pending_email_archive_meta_paths(scheduled_release_slot=scheduled_release_slot):
        metadata = _load_archive_metadata(meta_path)
        if not metadata:
            continue
        pending_at = _pending_release_anchor_datetime(metadata)
        if pending_at is None:
            rows.append(meta_path)
            continue
        pending_bjt = _as_beijing_time(pending_at)
        if pending_bjt.date() < now_bjt.date():
            rows.append(meta_path)
            continue
        if pending_bjt.date() == now_bjt.date() and now_bjt.hour * 60 + now_bjt.minute >= release_minutes:
            rows.append(meta_path)
    return rows


def _load_archive_metadata(meta_path: Path) -> dict[str, object]:
    if not meta_path.exists():
        return {}
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _pending_release_anchor_datetime(metadata: dict[str, object]) -> datetime | None:
    for key in ("generated_at", "archived_at"):
        parsed = _parse_iso_datetime(metadata.get(key))
        if parsed is not None:
            return parsed
    return None


def _slot_minutes(slot_text: str) -> int:
    raw = str(slot_text or "").strip()
    try:
        hour_text, minute_text = raw.split(":", 1)
        hour = int(hour_text)
        minute = int(minute_text)
    except Exception:
        return 8 * 60
    hour = max(0, min(hour, 23))
    minute = max(0, min(minute, 59))
    return hour * 60 + minute


def build_multi_coin_chart_image_map(
    digest: MultiCoinMarketDigest,
    *,
    client: OkxRestClient | None = None,
    overlay_map: dict[str, dict[str, tuple[MiniChartOverlay, ...]]] | None = None,
    timeframes: tuple[str, ...] = ("1H", "4H", "1D", "1W"),
    visible_limit: int = 72,
) -> dict[str, dict[str, str]]:
    resolved_client = client or OkxRestClient()
    resolved_overlay_map = overlay_map or {}
    images: dict[str, dict[str, str]] = {}
    for analysis in digest.analyses:
        symbol_images: dict[str, str] = {}
        for timeframe in timeframes:
            symbol_overlays = _resolve_chart_overlays_for_timeframe(
                resolved_overlay_map.get(analysis.symbol, {}),
                timeframe,
            )
            preload_limit = visible_limit + max((item.period for item in symbol_overlays), default=55)
            candles = _load_chart_candles(
                analysis.symbol,
                timeframe,
                limit=preload_limit,
                client=resolved_client,
            )
            if not candles:
                continue
            try:
                symbol_images[timeframe] = render_candles_png_base64(
                    candles,
                    width=320,
                    height=160,
                    max_candles=visible_limit,
                    overlays=symbol_overlays,
                )
            except Exception:
                continue
        if symbol_images:
            images[analysis.symbol] = symbol_images
    return images


def _load_chart_candles(
    symbol: str,
    timeframe: str,
    *,
    limit: int,
    client: OkxRestClient,
) -> list:
    if timeframe == "1W":
        try:
            return client.get_candles(symbol, timeframe, limit=limit)
        except Exception:
            return []

    cached: list = []
    try:
        cached = load_candle_cache(symbol, timeframe, limit=limit)
    except Exception:
        cached = []
    if len(cached) >= limit:
        return cached[-limit:]

    try:
        fetched = client.get_candles_history(symbol, timeframe, limit=limit)
    except Exception:
        return cached
    return fetched or cached


def _build_coin_chart_html(
    *,
    asset: str,
    symbol: str,
    chart_image_map: dict[str, dict[str, str]],
    overlay_legends: dict[str, str],
) -> str:
    symbol_images = chart_image_map.get(symbol, {})
    encoded_1h = symbol_images.get("1H", "").strip()
    encoded_4h = symbol_images.get("4H", "").strip()
    encoded_1d = symbol_images.get("1D", "").strip()
    encoded_1w = symbol_images.get("1W", "").strip()
    if not encoded_1h and not encoded_4h and not encoded_1d and not encoded_1w:
        return """
        <table width="100%" border="0" cellspacing="0" cellpadding="0" style="margin-bottom: 12px;">
            <tr>
                <td style="padding: 10px 12px; border: 1px dashed #d8e1ea; border-radius: 6px; background-color: #fbfcfe; font-size: 12px; color: #7f8c8d; text-align: center;">
                    缩略 K 线暂不可用
                </td>
            </tr>
        </table>
        """
    image_1h_html = _build_single_chart_cell(asset=asset, timeframe="1H", encoded=encoded_1h)
    image_4h_html = _build_single_chart_cell(asset=asset, timeframe="4H", encoded=encoded_4h)
    image_1d_html = _build_single_chart_cell(asset=asset, timeframe="1D", encoded=encoded_1d)
    image_1w_html = _build_single_chart_cell(asset=asset, timeframe="1W", encoded=encoded_1w)
    overlay_1h = overlay_legends.get("1H", build_overlay_legend_html(default_symbol_overlays()))
    overlay_4h = overlay_legends.get("4H", build_overlay_legend_html(default_4h_overlays()))
    overlay_1d = overlay_legends.get("1D", build_overlay_legend_html(default_4h_overlays()))
    overlay_1w = overlay_legends.get("1W", build_overlay_legend_html(default_4h_overlays()))
    return f"""
    <table width="100%" border="0" cellspacing="0" cellpadding="0" style="margin-bottom: 12px;">
        <tr>
            <td style="font-size: 12px; color: #7f8c8d; padding: 0 0 6px 0;">{escape(asset)} 最近 72 根 1H / 4H / 1D / 1W K 线</td>
        </tr>
        <tr>
            <td>
                <table width="100%" border="0" cellspacing="0" cellpadding="0">
                    <tr>
                        <td width="50%" valign="top" style="padding: 0 6px 0 0;">{image_1h_html.replace('__OVERLAY_LEGEND__', overlay_1h)}</td>
                        <td width="50%" valign="top" style="padding: 0 0 0 6px;">{image_4h_html.replace('__OVERLAY_LEGEND__', overlay_4h)}</td>
                    </tr>
                    <tr>
                        <td width="50%" valign="top" style="padding: 12px 6px 0 0;">{image_1d_html.replace('__OVERLAY_LEGEND__', overlay_1d)}</td>
                        <td width="50%" valign="top" style="padding: 12px 0 0 6px;">{image_1w_html.replace('__OVERLAY_LEGEND__', overlay_1w)}</td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
    """


def _build_single_chart_cell(*, asset: str, timeframe: str, encoded: str) -> str:
    if not encoded.strip():
        return f"""
        <table width="100%" border="0" cellspacing="0" cellpadding="0">
            <tr>
                <td style="font-size: 12px; color: #7f8c8d; padding: 0 0 6px 0;">{escape(timeframe)}</td>
            </tr>
            <tr>
                <td style="font-size: 11px; color: #94a3b8; padding: 0 0 6px 0;">__OVERLAY_LEGEND__</td>
            </tr>
            <tr>
                <td style="padding: 10px 12px; border: 1px dashed #d8e1ea; border-radius: 6px; background-color: #fbfcfe; font-size: 12px; color: #94a3b8; text-align: center;">
                    {escape(timeframe)} 图暂不可用
                </td>
            </tr>
        </table>
        """
    return f"""
    <table width="100%" border="0" cellspacing="0" cellpadding="0">
        <tr>
            <td style="font-size: 12px; color: #7f8c8d; padding: 0 0 6px 0;">{escape(timeframe)}</td>
        </tr>
        <tr>
            <td style="font-size: 11px; color: #94a3b8; padding: 0 0 6px 0;">__OVERLAY_LEGEND__</td>
        </tr>
        <tr>
            <td>
                <img
                    src="data:image/png;base64,{encoded}"
                    alt="{escape(asset)} recent {escape(timeframe)} candles"
                    style="display: block; width: 100%; max-width: 100%; height: auto; border: 1px solid #e0e6ed; border-radius: 6px; background-color: #f8fafc;"
                >
            </td>
        </tr>
    </table>
    """


def build_multi_coin_overlay_map(
    digest: MultiCoinMarketDigest,
    *,
    bundle_path: Path | None = None,
) -> dict[str, dict[str, tuple[MiniChartOverlay, ...]]]:
    resolved_bundle_path = bundle_path or (analysis_report_dir_path() / "packages" / "最佳参数组合包.json")
    overlays_by_symbol: dict[str, dict[str, list[MiniChartOverlay]]] = {
        analysis.symbol: {
            "1H": [],
            "4H": list(default_4h_overlays()),
            "1D": list(default_4h_overlays()),
            "1W": list(default_4h_overlays()),
        }
        for analysis in digest.analyses
    }
    if not resolved_bundle_path.exists():
        return {
            symbol: {
                "1H": tuple(items["1H"]) if items["1H"] else default_symbol_overlays(),
                "4H": tuple(items["4H"]),
                "1D": tuple(items["1D"]),
                "1W": tuple(items["1W"]),
            }
            for symbol, items in overlays_by_symbol.items()
        }
    try:
        bundle = read_strategy_bundle(resolved_bundle_path)
    except Exception:
        return {
            symbol: {
                "1H": tuple(items["1H"]) if items["1H"] else default_symbol_overlays(),
                "4H": tuple(items["4H"]),
                "1D": tuple(items["1D"]),
                "1W": tuple(items["1W"]),
            }
            for symbol, items in overlays_by_symbol.items()
        }
    for profile in bundle.profiles:
        symbol = profile.symbol.strip().upper()
        if symbol not in overlays_by_symbol:
            continue
        overlays_by_symbol[symbol]["1H"] = _merge_overlays(
            overlays_by_symbol[symbol]["1H"],
            _extract_symbol_overlays_from_snapshot(profile.config_snapshot),
        )
    return {
        symbol: {
            "1H": tuple(items["1H"]) if items["1H"] else default_symbol_overlays(),
            "4H": tuple(items["4H"]),
            "1D": tuple(items["1D"]),
            "1W": tuple(items["1W"]),
        }
        for symbol, items in overlays_by_symbol.items()
    }


def default_symbol_overlays() -> tuple[MiniChartOverlay, ...]:
    return (
        MiniChartOverlay(period=21, ma_type="ema"),
        MiniChartOverlay(period=55, ma_type="ema"),
    )


def default_4h_overlays() -> tuple[MiniChartOverlay, ...]:
    return default_symbol_overlays()


def _extract_symbol_overlays_from_snapshot(snapshot: dict[str, object]) -> tuple[MiniChartOverlay, ...]:
    rows: list[MiniChartOverlay] = []
    main_period = _to_positive_int(snapshot.get("ema_period"))
    if main_period > 0:
        rows.append(MiniChartOverlay(period=main_period, ma_type=str(snapshot.get("ema_type", "ema") or "ema")))
    trend_period = _to_positive_int(snapshot.get("trend_ema_period"))
    if trend_period > 0:
        rows.append(
            MiniChartOverlay(period=trend_period, ma_type=str(snapshot.get("trend_ema_type", "ema") or "ema"))
        )
    if not rows:
        return default_symbol_overlays()
    deduped: list[MiniChartOverlay] = []
    seen: set[tuple[str, int]] = set()
    for item in rows:
        key = (item.normalized_type, item.period)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return tuple(deduped)


def _merge_overlays(
    existing: list[MiniChartOverlay],
    incoming: tuple[MiniChartOverlay, ...],
) -> list[MiniChartOverlay]:
    merged = list(existing)
    seen = {(item.normalized_type, item.period) for item in merged}
    for item in incoming:
        key = (item.normalized_type, item.period)
        if key in seen:
            continue
        merged.append(item)
        seen.add(key)
    return sorted(merged, key=lambda item: (item.period, item.normalized_type))


def format_overlay_labels(overlays: tuple[MiniChartOverlay, ...]) -> str:
    if not overlays:
        overlays = default_symbol_overlays()
    return " / ".join(item.resolved_label for item in overlays)


def build_overlay_legend_html(overlays: tuple[MiniChartOverlay, ...]) -> str:
    if not overlays:
        overlays = default_symbol_overlays()
    parts = []
    for index, item in enumerate(overlays):
        color = _rgb_to_hex(item.color or LINE_COLORS[index % len(LINE_COLORS)])
        parts.append(
            f'<span style="display:inline-block; margin-right:8px; white-space:nowrap;">'
            f'<span style="display:inline-block; width:8px; height:8px; border-radius:999px; background:{color}; margin-right:4px; vertical-align:middle;"></span>'
            f'<span style="vertical-align:middle;">{escape(item.resolved_label)}</span>'
            f"</span>"
        )
    return "叠加：" + "".join(parts)


def _resolve_chart_overlays_for_timeframe(
    overlay_map: dict[str, tuple[MiniChartOverlay, ...]],
    timeframe: str,
) -> tuple[MiniChartOverlay, ...]:
    if timeframe == "1H":
        return overlay_map.get("1H", default_symbol_overlays())
    return overlay_map.get(timeframe, default_4h_overlays())


def _to_positive_int(value: object) -> int:
    try:
        resolved = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return max(resolved, 0)


def _rgb_to_hex(color: tuple[int, int, int]) -> str:
    return "#{:02x}{:02x}{:02x}".format(*color)


def _leader_payload(leader: DigestLeader) -> dict[str, object]:
    return {
        "symbol": leader.symbol,
        "label": leader.label,
        "summary": leader.summary,
        "score": leader.score,
        "explicit": leader.explicit,
    }


def _leader_headline(leader: DigestLeader) -> str:
    if leader.explicit:
        return f"{leader.label}。{leader.summary}"
    return leader.summary


def _direction_label(direction: str) -> str:
    return {"long": "多", "short": "空", "neutral": "震荡"}.get(direction, direction)


def _asset_name(symbol: str) -> str:
    return symbol.split("-")[0].upper()


def _pct(value) -> str:
    return f"{float(value) * 100:.0f}%"


def _parse_iso_datetime(raw_value: object) -> datetime | None:
    raw = str(raw_value or "").strip()
    if not raw:
        return None
    normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _format_generated_at_display(raw_value: str) -> str:
    parsed = _parse_iso_datetime(raw_value)
    if parsed is None:
        return raw_value
    china_time = _as_beijing_time(parsed)
    return china_time.strftime("%Y-%m-%d %H:%M UTC+8")


def _as_beijing_time(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone(timedelta(hours=8)))
