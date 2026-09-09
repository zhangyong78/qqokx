from __future__ import annotations

import json
import math
from html import escape
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterable

from okx_quant.btc_market_analyzer import (
    BtcMarketAnalysis,
    BtcMarketAnalyzerConfig,
    TimeframeAnalysis,
    analyze_btc_market_from_client,
    btc_market_analysis_payload,
    build_pattern_focus_events,
    load_btc_market_email_notifier,
)
from okx_quant.analysis_email_validation import (
    build_recent_email_validation_summary,
    load_latest_email_validation_payload,
    refresh_email_validation_report,
)
from okx_quant.candle_cache import load_candle_cache
from okx_quant.indicators import moving_average
from okx_quant.mini_chart import LINE_COLORS, MiniChartOverlay, render_candles_png_base64
from okx_quant.okx_client import OkxRestClient
from okx_quant.deribit_client import DeribitRestClient, DeribitVolatilityCandle
from okx_quant.models import Candle
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
BTC_EMA15_MA50_TIMEFRAMES: tuple[str, ...] = ("1H", "4H", "1D", "1W")
BTC_VOLATILITY_EMA15_MA50_TIMEFRAMES: tuple[str, ...] = ("1H", "4H", "1D", "1W")
BEIJING_TIMEZONE = timezone(timedelta(hours=8))
CHART_STALE_AFTER = {
    "1H": timedelta(hours=2),
    "4H": timedelta(hours=6),
    "1D": timedelta(hours=36),
    "1W": timedelta(days=8),
}
VALIDATION_STALE_AFTER = timedelta(hours=24)


@dataclass(frozen=True)
class DigestLeader:
    symbol: str
    label: str
    summary: str
    score: float
    explicit: bool = True


@dataclass(frozen=True)
class ChartDataStatus:
    timeframe: str
    candle_ts: int | None
    confirmed: bool | None
    source: str
    display_time: str
    age: timedelta | None
    is_stale: bool
    status_text: str


def build_chart_data_status(
    timeframe: str,
    *,
    candle_ts: int | None,
    confirmed: bool | None,
    source: str,
    generated_at: str | datetime,
) -> ChartDataStatus:
    generated_dt = generated_at if isinstance(generated_at, datetime) else _parse_iso_datetime(generated_at)
    if generated_dt is None:
        generated_dt = datetime.now(timezone.utc)
    if generated_dt.tzinfo is None:
        generated_dt = generated_dt.replace(tzinfo=timezone.utc)
    source_text = str(source or "").strip() or "来源未知"
    if candle_ts is None:
        return ChartDataStatus(
            timeframe=timeframe,
            candle_ts=None,
            confirmed=confirmed,
            source=source_text,
            display_time="-",
            age=None,
            is_stale=True,
            status_text=f"数据更新时间未知 | {source_text} | 数据已过期",
        )
    candle_dt = datetime.fromtimestamp(int(candle_ts) / 1000.0, tz=timezone.utc)
    age = max(generated_dt.astimezone(timezone.utc) - candle_dt, timedelta(0))
    threshold = CHART_STALE_AFTER.get(str(timeframe).strip().upper(), timedelta(hours=2))
    is_stale = age > threshold
    display_time = candle_dt.astimezone(BEIJING_TIMEZONE).strftime("%Y-%m-%d %H:%M")
    close_state = "已收盘" if confirmed is True else "进行中" if confirmed is False else "收盘状态未知"
    freshness = f"数据已过期（滞后 {_format_age(age)}）" if is_stale else "数据正常"
    return ChartDataStatus(
        timeframe=timeframe,
        candle_ts=int(candle_ts),
        confirmed=confirmed,
        source=source_text,
        display_time=display_time,
        age=age,
        is_stale=is_stale,
        status_text=f"数据更新至 {display_time} UTC+8 | {close_state} | {source_text} | {freshness}",
    )


def _format_age(value: timedelta) -> str:
    total_minutes = max(0, int(value.total_seconds() // 60))
    days, remaining_minutes = divmod(total_minutes, 24 * 60)
    hours, minutes = divmod(remaining_minutes, 60)
    if days:
        return f"{days}天{hours}小时"
    if hours:
        return f"{hours}小时{minutes}分"
    return f"{minutes}分"


@dataclass(frozen=True)
class BtcEma15Ma50TimeframeAnalysis:
    timeframe: str
    candle_ts: int | None
    last_close: Decimal | None
    ema15: Decimal | None
    ma50: Decimal | None
    direction: str
    structure: str
    summary: str
    candle_confirmed: bool | None = None


@dataclass(frozen=True)
class BtcEma15Ma50Supplement:
    symbol: str
    generated_at: str
    direction: str
    summary: str
    timeframes: tuple[BtcEma15Ma50TimeframeAnalysis, ...]
    candle_series: tuple[tuple[str, tuple[Candle, ...]], ...] = field(default_factory=tuple, repr=False, compare=False)


@dataclass(frozen=True)
class BtcVolatilityTimeframeAnalysis:
    timeframe: str
    candle_ts: int | None
    last_close: Decimal | None
    ema15: Decimal | None
    ma50: Decimal | None
    direction: str
    structure: str
    source: str
    summary: str
    candle_confirmed: bool | None = None


@dataclass(frozen=True)
class BtcVolatilitySupplement:
    symbol: str
    generated_at: str
    direction: str
    summary: str
    timeframes: tuple[BtcVolatilityTimeframeAnalysis, ...]
    candle_series: tuple[tuple[str, tuple[Candle, ...]], ...] = field(default_factory=tuple, repr=False, compare=False)


@dataclass(frozen=True)
class MultiCoinMarketDigest:
    generated_at: str
    symbols: tuple[str, ...]
    analyses: tuple[BtcMarketAnalysis, ...]
    strongest_long: DigestLeader
    weakest_short: DigestLeader
    best_trade_candidate: DigestLeader
    btc_ema15_ma50: BtcEma15Ma50Supplement | None = None
    btc_volatility_ema15_ma50: BtcVolatilitySupplement | None = None


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
    btc_analysis = next((item for item in analyses if item.symbol.strip().upper() == "BTC-USDT-SWAP"), None)
    btc_ema15_ma50 = (
        _build_btc_ema15_ma50_supplement(client, symbol=btc_analysis.symbol, generated_at=generated_at)
        if btc_analysis is not None
        else None
    )
    btc_volatility_ema15_ma50 = (
        _build_btc_volatility_ema15_ma50_supplement(client, symbol=btc_analysis.symbol, generated_at=generated_at)
        if btc_analysis is not None
        else None
    )
    return MultiCoinMarketDigest(
        generated_at=generated_at,
        symbols=tuple(item.symbol for item in analyses),
        analyses=analyses,
        strongest_long=_pick_strongest_long(analyses),
        weakest_short=_pick_weakest_short(analyses),
        best_trade_candidate=_pick_best_trade_candidate(analyses),
        btc_ema15_ma50=btc_ema15_ma50,
        btc_volatility_ema15_ma50=btc_volatility_ema15_ma50,
    )


def multi_coin_market_digest_payload(digest: MultiCoinMarketDigest) -> dict[str, object]:
    payload = {
        "generated_at": digest.generated_at,
        "symbols": list(digest.symbols),
        "leaders": {
            "strongest_long": _leader_payload(digest.strongest_long),
            "weakest_short": _leader_payload(digest.weakest_short),
            "best_trade_candidate": _leader_payload(digest.best_trade_candidate),
        },
        "analyses": [btc_market_analysis_payload(item) for item in digest.analyses],
    }
    if digest.btc_ema15_ma50 is not None:
        payload["btc_ema15_ma50"] = _btc_ema15_ma50_payload(digest.btc_ema15_ma50)
    if digest.btc_volatility_ema15_ma50 is not None:
        payload["btc_volatility_ema15_ma50"] = _btc_volatility_ema15_ma50_payload(digest.btc_volatility_ema15_ma50)
    return payload


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
        if (
            digest.btc_ema15_ma50 is not None
            and analysis.symbol.strip().upper() == digest.btc_ema15_ma50.symbol.strip().upper()
        ):
            lines.extend(_build_btc_ema15_ma50_text_section(digest.btc_ema15_ma50))
        if (
            digest.btc_volatility_ema15_ma50 is not None
            and analysis.symbol.strip().upper() == digest.btc_volatility_ema15_ma50.symbol.strip().upper()
        ):
            lines.extend(_build_btc_volatility_text_section(digest.btc_volatility_ema15_ma50))
    return "\n".join(lines)


def build_multi_coin_market_email_html(
    digest: MultiCoinMarketDigest,
    *,
    chart_image_map: dict[str, dict[str, str]] | None = None,
    chart_data_status_map: dict[str, dict[str, ChartDataStatus]] | None = None,
    overlay_legend_map: dict[str, dict[str, str]] | None = None,
    btc_ema15_ma50_chart_image_map: dict[str, dict[str, str]] | None = None,
    btc_ema15_ma50_chart_data_status_map: dict[str, dict[str, ChartDataStatus]] | None = None,
    btc_ema15_ma50_overlay_legend_map: dict[str, dict[str, str]] | None = None,
    btc_volatility_chart_image_map: dict[str, dict[str, str]] | None = None,
    btc_volatility_chart_data_status_map: dict[str, dict[str, ChartDataStatus]] | None = None,
    btc_volatility_overlay_legend_map: dict[str, dict[str, str]] | None = None,
) -> str:
    return _build_multi_coin_market_email_html(
        digest,
        chart_image_map=chart_image_map,
        chart_data_status_map=chart_data_status_map,
        overlay_legend_map=overlay_legend_map,
        btc_ema15_ma50_chart_image_map=btc_ema15_ma50_chart_image_map,
        btc_ema15_ma50_chart_data_status_map=btc_ema15_ma50_chart_data_status_map,
        btc_ema15_ma50_overlay_legend_map=btc_ema15_ma50_overlay_legend_map,
        btc_volatility_chart_image_map=btc_volatility_chart_image_map,
        btc_volatility_chart_data_status_map=btc_volatility_chart_data_status_map,
        btc_volatility_overlay_legend_map=btc_volatility_overlay_legend_map,
        validation_summary=None,
    )


def _build_multi_coin_market_email_html(
    digest: MultiCoinMarketDigest,
    *,
    chart_image_map: dict[str, dict[str, str]] | None = None,
    chart_data_status_map: dict[str, dict[str, ChartDataStatus]] | None = None,
    overlay_legend_map: dict[str, dict[str, str]] | None = None,
    btc_ema15_ma50_chart_image_map: dict[str, dict[str, str]] | None = None,
    btc_ema15_ma50_chart_data_status_map: dict[str, dict[str, ChartDataStatus]] | None = None,
    btc_ema15_ma50_overlay_legend_map: dict[str, dict[str, str]] | None = None,
    btc_volatility_chart_image_map: dict[str, dict[str, str]] | None = None,
    btc_volatility_chart_data_status_map: dict[str, dict[str, ChartDataStatus]] | None = None,
    btc_volatility_overlay_legend_map: dict[str, dict[str, str]] | None = None,
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
            chart_data_status_map=chart_data_status_map or {},
            overlay_legend_map=overlay_legend_map or {},
            btc_ema15_ma50=digest.btc_ema15_ma50,
            btc_ema15_ma50_chart_image_map=btc_ema15_ma50_chart_image_map or {},
            btc_ema15_ma50_chart_data_status_map=btc_ema15_ma50_chart_data_status_map or {},
            btc_ema15_ma50_overlay_legend_map=btc_ema15_ma50_overlay_legend_map or {},
            btc_volatility_ema15_ma50=digest.btc_volatility_ema15_ma50,
            btc_volatility_chart_image_map=btc_volatility_chart_image_map or {},
            btc_volatility_chart_data_status_map=btc_volatility_chart_data_status_map or {},
            btc_volatility_overlay_legend_map=btc_volatility_overlay_legend_map or {},
        )
        for analysis in digest.analyses
    )
    headline = build_multi_coin_market_email_subject(digest)
    generated_at = _format_generated_at_display(digest.generated_at)
    freshness_warning_html = _build_freshness_warning_html(
        chart_data_status_map or {},
        btc_ema15_ma50_chart_data_status_map or {},
        btc_volatility_chart_data_status_map or {},
    )
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
                    {freshness_warning_html}
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
    validation_summary = _load_recent_validation_summary(refresh_if_needed=True)
    subject = build_multi_coin_market_email_subject(digest)
    body = _build_multi_coin_market_email_body(digest, validation_summary=validation_summary)
    overlay_map = build_multi_coin_overlay_map(digest)
    chart_image_map, chart_data_status_map = _build_multi_coin_chart_assets(digest, overlay_map=overlay_map)
    btc_ema15_ma50_chart_image_map: dict[str, dict[str, str]] = {}
    btc_ema15_ma50_chart_data_status_map: dict[str, dict[str, ChartDataStatus]] = {}
    btc_ema15_ma50_overlay_legend_map: dict[str, dict[str, str]] = {}
    btc_volatility_chart_image_map: dict[str, dict[str, str]] = {}
    btc_volatility_chart_data_status_map: dict[str, dict[str, ChartDataStatus]] = {}
    btc_volatility_overlay_legend_map: dict[str, dict[str, str]] = {}
    if digest.btc_ema15_ma50 is not None:
        btc_symbol = digest.btc_ema15_ma50.symbol
        btc_ema15_ma50_overlays = _build_btc_ema15_ma50_overlay_map(btc_symbol)
        price_series_by_timeframe = _supplement_candle_series_map(digest.btc_ema15_ma50.candle_series)
        btc_ema15_ma50_chart_image_map = _build_chart_image_map_from_candle_series(
            btc_symbol,
            candles_by_timeframe=price_series_by_timeframe,
            overlays_by_timeframe=btc_ema15_ma50_overlays[btc_symbol],
            visible_limit=72,
        )
        btc_ema15_ma50_chart_data_status_map = _build_chart_data_status_map_from_candle_series(
            btc_symbol,
            candles_by_timeframe=price_series_by_timeframe,
            generated_at=digest.generated_at,
            source_by_timeframe={timeframe: "OKX" for timeframe in price_series_by_timeframe},
        )
        btc_ema15_ma50_overlay_legend_map = {
            btc_symbol: {
                timeframe: build_overlay_legend_html(overlays)
                for timeframe, overlays in btc_ema15_ma50_overlays[btc_symbol].items()
            }
        }
    if digest.btc_volatility_ema15_ma50 is not None:
        btc_symbol = digest.btc_volatility_ema15_ma50.symbol
        btc_volatility_overlays = _build_btc_ema15_ma50_overlay_map(btc_symbol)
        volatility_series_by_timeframe = _supplement_candle_series_map(digest.btc_volatility_ema15_ma50.candle_series)
        btc_volatility_chart_image_map = _build_chart_image_map_from_candle_series(
            btc_symbol,
            candles_by_timeframe=volatility_series_by_timeframe,
            overlays_by_timeframe=btc_volatility_overlays[btc_symbol],
            visible_limit=72,
        )
        btc_volatility_chart_data_status_map = _build_chart_data_status_map_from_candle_series(
            btc_symbol,
            candles_by_timeframe=volatility_series_by_timeframe,
            generated_at=digest.generated_at,
            source_by_timeframe={
                item.timeframe: item.source
                for item in digest.btc_volatility_ema15_ma50.timeframes
            },
        )
        btc_volatility_overlay_legend_map = {
            btc_symbol: {
                timeframe: build_overlay_legend_html(overlays)
                for timeframe, overlays in btc_volatility_overlays[btc_symbol].items()
            }
        }
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
        chart_data_status_map=chart_data_status_map,
        overlay_legend_map=overlay_legend_map,
        btc_ema15_ma50_chart_image_map=btc_ema15_ma50_chart_image_map,
        btc_ema15_ma50_chart_data_status_map=btc_ema15_ma50_chart_data_status_map,
        btc_ema15_ma50_overlay_legend_map=btc_ema15_ma50_overlay_legend_map,
        btc_volatility_chart_image_map=btc_volatility_chart_image_map,
        btc_volatility_chart_data_status_map=btc_volatility_chart_data_status_map,
        btc_volatility_overlay_legend_map=btc_volatility_overlay_legend_map,
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


def _load_recent_validation_summary(*, refresh_if_needed: bool = False) -> dict[str, object] | None:
    if refresh_if_needed:
        _refresh_recent_validation_summary_if_needed()
    payload = load_latest_email_validation_payload()
    if not payload:
        return None
    return build_recent_email_validation_summary(payload, recent_email_limit=20)


def _refresh_recent_validation_summary_if_needed(*, now: datetime | None = None) -> None:
    payload = load_latest_email_validation_payload()
    generated_at = _parse_iso_datetime(payload.get("generated_at")) if payload is not None else None
    resolved_now = now or datetime.now(timezone.utc)
    if resolved_now.tzinfo is None:
        resolved_now = resolved_now.replace(tzinfo=timezone.utc)
    if generated_at is not None and resolved_now.astimezone(timezone.utc) - generated_at.astimezone(timezone.utc) <= VALIDATION_STALE_AFTER:
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
    lines.extend(f"- {item}" for item in _recent_validation_time_lines(summary))
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


def _recent_validation_time_lines(
    summary: dict[str, object],
    *,
    now: datetime | None = None,
) -> list[str]:
    generated_at = _parse_iso_datetime(summary.get("generated_at"))
    sample_cutoff_at = _parse_iso_datetime(summary.get("sample_cutoff_at"))
    generated_text = _format_beijing_datetime(generated_at)
    cutoff_text = _format_beijing_datetime(sample_cutoff_at)
    lines = [
        f"复盘报告生成时间：{generated_text}",
        f"复盘样本截止时间：{cutoff_text}",
    ]
    resolved_now = now or datetime.now(timezone.utc)
    if resolved_now.tzinfo is None:
        resolved_now = resolved_now.replace(tzinfo=timezone.utc)
    is_stale = (
        generated_at is None
        or resolved_now.astimezone(timezone.utc) - generated_at.astimezone(timezone.utc) > VALIDATION_STALE_AFTER
    )
    if is_stale:
        lines.append("复盘数据已过期：邮件继续发送，但请勿把旧样本统计当作当前表现。")
    return lines


def _format_beijing_datetime(value: datetime | None) -> str:
    if value is None:
        return "-"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(BEIJING_TIMEZONE).strftime("%Y-%m-%d %H:%M UTC+8")


def _build_freshness_warning_html(
    *status_maps: dict[str, dict[str, ChartDataStatus]],
) -> str:
    stale_items = [
        f"{symbol.split('-')[0]} {status.timeframe}（{status.display_time}）"
        for status_map in status_maps
        for symbol, timeframe_map in status_map.items()
        for status in timeframe_map.values()
        if status.is_stale
    ]
    if not stale_items:
        return ""
    preview = "、".join(stale_items[:8])
    if len(stale_items) > 8:
        preview = f"{preview} 等 {len(stale_items)} 项"
    return f"""
                    <tr>
                        <td style="padding: 12px 24px; color: #b42318; background-color: #fff1f0; border-bottom: 1px solid #fecdca; font-size: 13px; line-height: 1.7; font-weight: 700;">
                            本邮件含过期数据：{escape(preview)}。邮件仍按计划发送，请先核对各图更新时间再使用结论。
                        </td>
                    </tr>
    """


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
    rows.extend(_recent_validation_time_lines(summary))
    summary_html = "".join(
        f"""
        <tr>
            <td style="padding: 6px 0; color: {'#b42318' if row.startswith('复盘数据已过期') else '#7c5f10'}; font-size: 14px; line-height: 1.7; font-weight: {'700' if row.startswith('复盘数据已过期') else '400'};">
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
        f"  4H：{_timeframe_line(tf4h)} | {_timeframe_status_text(tf4h, generated_at=analysis.generated_at, source='OKX')}",
        f"  1H：{_timeframe_line(tf1h)} | {_timeframe_status_text(tf1h, generated_at=analysis.generated_at, source='OKX')}",
        f"  跟踪：{_coin_tracking_summary(analysis, tf4h, tf1h)}",
    ]
    recent_events = _collect_recent_events((tf4h, tf1h), last_sent_at=last_sent_at)
    if recent_events:
        lines.append(f"  新形态：{'; '.join(recent_events[:3])}")
    else:
        lines.append("  新形态：上次发送后没有新的代表性K线")
    return lines


def _build_btc_ema15_ma50_text_section(supplement: BtcEma15Ma50Supplement) -> list[str]:
    lines = [f"  EMA15/MA50 四周期补充：{supplement.summary}"]
    for item in supplement.timeframes:
        lines.append(
            f"    {item.timeframe}：{item.summary} | "
            f"{_supplement_status_text(item, generated_at=supplement.generated_at, source='OKX')}"
        )
    return lines


def _build_btc_volatility_text_section(supplement: BtcVolatilitySupplement) -> list[str]:
    lines = [f"  波动率 EMA15/MA50 补充：{supplement.summary}"]
    for item in supplement.timeframes:
        lines.append(
            f"    {item.timeframe}：{item.summary} | "
            f"{_supplement_status_text(item, generated_at=supplement.generated_at, source=item.source)}"
        )
    return lines


def _timeframe_status_text(
    item: TimeframeAnalysis | None,
    *,
    generated_at: str,
    source: str,
) -> str:
    if item is None:
        return build_chart_data_status(
            "",
            candle_ts=None,
            confirmed=None,
            source=source,
            generated_at=generated_at,
        ).status_text
    return build_chart_data_status(
        item.timeframe,
        candle_ts=item.candle_ts,
        confirmed=item.candle_confirmed,
        source=source,
        generated_at=generated_at,
    ).status_text


def _supplement_status_text(
    item: BtcEma15Ma50TimeframeAnalysis | BtcVolatilityTimeframeAnalysis,
    *,
    generated_at: str,
    source: str,
) -> str:
    return build_chart_data_status(
        item.timeframe,
        candle_ts=item.candle_ts,
        confirmed=item.candle_confirmed,
        source=source,
        generated_at=generated_at,
    ).status_text


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
    chart_data_status_map: dict[str, dict[str, ChartDataStatus]],
    overlay_legend_map: dict[str, dict[str, str]],
    btc_ema15_ma50: BtcEma15Ma50Supplement | None,
    btc_ema15_ma50_chart_image_map: dict[str, dict[str, str]],
    btc_ema15_ma50_chart_data_status_map: dict[str, dict[str, ChartDataStatus]],
    btc_ema15_ma50_overlay_legend_map: dict[str, dict[str, str]],
    btc_volatility_ema15_ma50: BtcVolatilitySupplement | None,
    btc_volatility_chart_image_map: dict[str, dict[str, str]],
    btc_volatility_chart_data_status_map: dict[str, dict[str, ChartDataStatus]],
    btc_volatility_overlay_legend_map: dict[str, dict[str, str]],
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
        data_statuses=chart_data_status_map.get(analysis.symbol, {}),
        overlay_legends=overlay_legend_map.get(analysis.symbol, {}),
    )
    btc_extra_html = ""
    if (
        btc_ema15_ma50 is not None
        and analysis.symbol.strip().upper() == btc_ema15_ma50.symbol.strip().upper()
    ):
        btc_extra_html = _build_btc_ema15_ma50_html_block(
            btc_ema15_ma50,
            chart_image_map=btc_ema15_ma50_chart_image_map,
            data_statuses=btc_ema15_ma50_chart_data_status_map.get(analysis.symbol, {}),
            overlay_legends=btc_ema15_ma50_overlay_legend_map.get(analysis.symbol, {}),
        )
    btc_volatility_extra_html = ""
    if (
        btc_volatility_ema15_ma50 is not None
        and analysis.symbol.strip().upper() == btc_volatility_ema15_ma50.symbol.strip().upper()
    ):
        btc_volatility_extra_html = _build_btc_volatility_html_block(
            btc_volatility_ema15_ma50,
            chart_image_map=btc_volatility_chart_image_map,
            data_statuses=btc_volatility_chart_data_status_map.get(analysis.symbol, {}),
            overlay_legends=btc_volatility_overlay_legend_map.get(analysis.symbol, {}),
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
                        <td style="padding: 4px 0;"><strong>4H 周期：</strong>{escape(_timeframe_line(tf4h))}<br><span style="font-size: 11px; color: #667085;">{escape(_timeframe_status_text(tf4h, generated_at=analysis.generated_at, source='OKX'))}</span></td>
                    </tr>
                    <tr>
                        <td style="padding: 4px 0;"><strong>1H 周期：</strong>{escape(_timeframe_line(tf1h))}<br><span style="font-size: 11px; color: #667085;">{escape(_timeframe_status_text(tf1h, generated_at=analysis.generated_at, source='OKX'))}</span></td>
                    </tr>
                    <tr>
                        <td style="padding: 4px 0;"><strong>跟踪提示：</strong>{escape(_coin_tracking_summary(analysis, tf4h, tf1h))}</td>
                    </tr>
                    <tr>
                        <td style="padding: 4px 0;"><strong>关注新形态：</strong>{escape(recent_events_text)}</td>
                    </tr>
                </table>
                {btc_extra_html}
                {btc_volatility_extra_html}
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
    symbols: Iterable[str] | None = None,
    timeframes: tuple[str, ...] = ("1H", "4H", "1D", "1W"),
    visible_limit: int = 72,
) -> dict[str, dict[str, str]]:
    images, _ = _build_multi_coin_chart_assets(
        digest,
        client=client,
        overlay_map=overlay_map,
        symbols=symbols,
        timeframes=timeframes,
        visible_limit=visible_limit,
    )
    return images


def _build_multi_coin_chart_assets(
    digest: MultiCoinMarketDigest,
    *,
    client: OkxRestClient | None = None,
    overlay_map: dict[str, dict[str, tuple[MiniChartOverlay, ...]]] | None = None,
    symbols: Iterable[str] | None = None,
    timeframes: tuple[str, ...] = ("1H", "4H", "1D", "1W"),
    visible_limit: int = 72,
) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, ChartDataStatus]]]:
    resolved_client = client or OkxRestClient()
    resolved_overlay_map = overlay_map or {}
    symbol_filter = {str(item).strip().upper() for item in (symbols or ()) if str(item).strip()}
    images: dict[str, dict[str, str]] = {}
    statuses: dict[str, dict[str, ChartDataStatus]] = {}
    for analysis in digest.analyses:
        if symbol_filter and analysis.symbol.strip().upper() not in symbol_filter:
            continue
        symbol_images: dict[str, str] = {}
        symbol_statuses: dict[str, ChartDataStatus] = {}
        snapshot_map = {timeframe: list(candles) for timeframe, candles in analysis.chart_candles}
        for timeframe in timeframes:
            symbol_overlays = _resolve_chart_overlays_for_timeframe(
                resolved_overlay_map.get(analysis.symbol, {}),
                timeframe,
            )
            preload_limit = visible_limit + max((item.period for item in symbol_overlays), default=55)
            candles = snapshot_map.get(timeframe, [])
            if not candles:
                candles = _load_chart_candles(
                    analysis.symbol,
                    timeframe,
                    limit=preload_limit,
                    client=resolved_client,
                )
            if not candles:
                continue
            latest = candles[-1]
            symbol_statuses[timeframe] = build_chart_data_status(
                timeframe,
                candle_ts=int(latest.ts),
                confirmed=bool(latest.confirmed),
                source="OKX",
                generated_at=digest.generated_at,
            )
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
        if symbol_statuses:
            statuses[analysis.symbol] = symbol_statuses
    return images, statuses


def _build_chart_image_map_from_candle_series(
    symbol: str,
    *,
    candles_by_timeframe: dict[str, list],
    overlays_by_timeframe: dict[str, tuple[MiniChartOverlay, ...]],
    visible_limit: int,
) -> dict[str, dict[str, str]]:
    symbol_images: dict[str, str] = {}
    for timeframe, candles in candles_by_timeframe.items():
        if not candles:
            continue
        try:
            symbol_images[timeframe] = render_candles_png_base64(
                candles,
                width=320,
                height=160,
                max_candles=visible_limit,
                overlays=overlays_by_timeframe.get(timeframe, ()),
            )
        except Exception:
            continue
    return {symbol: symbol_images} if symbol_images else {}


def _supplement_candle_series_map(
    candle_series: tuple[tuple[str, tuple[Candle, ...]], ...],
) -> dict[str, list[Candle]]:
    return {timeframe: list(candles) for timeframe, candles in candle_series}


def _build_chart_data_status_map_from_candle_series(
    symbol: str,
    *,
    candles_by_timeframe: dict[str, list],
    generated_at: str,
    source_by_timeframe: dict[str, str],
) -> dict[str, dict[str, ChartDataStatus]]:
    statuses: dict[str, ChartDataStatus] = {}
    for timeframe, candles in candles_by_timeframe.items():
        if not candles:
            continue
        latest = candles[-1]
        statuses[timeframe] = build_chart_data_status(
            timeframe,
            candle_ts=int(latest.ts),
            confirmed=bool(latest.confirmed),
            source=source_by_timeframe.get(timeframe, ""),
            generated_at=generated_at,
        )
    return {symbol: statuses} if statuses else {}


def _load_chart_candles(
    symbol: str,
    timeframe: str,
    *,
    limit: int,
    client: OkxRestClient,
) -> list:
    cached: list = []
    try:
        cached = load_candle_cache(symbol, timeframe, limit=limit)
    except Exception:
        cached = []
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
    data_statuses: dict[str, ChartDataStatus],
    overlay_legends: dict[str, str],
    headline: str | None = None,
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
    image_1h_html = _build_single_chart_cell(
        asset=asset, timeframe="1H", encoded=encoded_1h, data_status=data_statuses.get("1H")
    )
    image_4h_html = _build_single_chart_cell(
        asset=asset, timeframe="4H", encoded=encoded_4h, data_status=data_statuses.get("4H")
    )
    image_1d_html = _build_single_chart_cell(
        asset=asset, timeframe="1D", encoded=encoded_1d, data_status=data_statuses.get("1D")
    )
    image_1w_html = _build_single_chart_cell(
        asset=asset, timeframe="1W", encoded=encoded_1w, data_status=data_statuses.get("1W")
    )
    overlay_1h = overlay_legends.get("1H", build_overlay_legend_html(default_symbol_overlays()))
    overlay_4h = overlay_legends.get("4H", build_overlay_legend_html(default_4h_overlays()))
    overlay_1d = overlay_legends.get("1D", build_overlay_legend_html(default_4h_overlays()))
    overlay_1w = overlay_legends.get("1W", build_overlay_legend_html(default_4h_overlays()))
    title = headline or f"{asset} 最近 72 根 1H / 4H / 1D / 1W K 线"
    return f"""
    <table width="100%" border="0" cellspacing="0" cellpadding="0" style="margin-bottom: 12px;">
        <tr>
            <td style="font-size: 12px; color: #7f8c8d; padding: 0 0 6px 0;">{escape(title)}</td>
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


def _build_coin_chart_html_with_headline(
    *,
    asset: str,
    symbol: str,
    chart_image_map: dict[str, dict[str, str]],
    data_statuses: dict[str, ChartDataStatus],
    overlay_legends: dict[str, str],
    headline: str,
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
    image_1h_html = _build_single_chart_cell(
        asset=asset, timeframe="1H", encoded=encoded_1h, data_status=data_statuses.get("1H")
    )
    image_4h_html = _build_single_chart_cell(
        asset=asset, timeframe="4H", encoded=encoded_4h, data_status=data_statuses.get("4H")
    )
    image_1d_html = _build_single_chart_cell(
        asset=asset, timeframe="1D", encoded=encoded_1d, data_status=data_statuses.get("1D")
    )
    image_1w_html = _build_single_chart_cell(
        asset=asset, timeframe="1W", encoded=encoded_1w, data_status=data_statuses.get("1W")
    )
    overlay_1h = overlay_legends.get("1H", build_overlay_legend_html(default_symbol_overlays()))
    overlay_4h = overlay_legends.get("4H", build_overlay_legend_html(default_4h_overlays()))
    overlay_1d = overlay_legends.get("1D", build_overlay_legend_html(default_4h_overlays()))
    overlay_1w = overlay_legends.get("1W", build_overlay_legend_html(default_4h_overlays()))
    return f"""
    <table width="100%" border="0" cellspacing="0" cellpadding="0" style="margin-bottom: 12px;">
        <tr>
            <td style="font-size: 12px; color: #7f8c8d; padding: 0 0 6px 0;">{escape(headline)}</td>
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


def _build_btc_ema15_ma50_html_block(
    supplement: BtcEma15Ma50Supplement,
    *,
    chart_image_map: dict[str, dict[str, str]],
    data_statuses: dict[str, ChartDataStatus],
    overlay_legends: dict[str, str],
) -> str:
    chart_html = _build_coin_chart_html_with_headline(
        asset="BTC",
        symbol=supplement.symbol,
        chart_image_map=chart_image_map,
        data_statuses=data_statuses,
        overlay_legends=overlay_legends,
        headline="BTC EMA15 + MA50 最近 72 根 1H / 4H / 1D / 1W K 线",
    )
    rows_html = "".join(
        f"""
        <tr>
            <td style="padding: 4px 0;"><strong>{escape(item.timeframe)}：</strong>{escape(item.summary)}<br><span style="font-size: 11px; color: {'#b42318' if data_statuses.get(item.timeframe) and data_statuses[item.timeframe].is_stale else '#667085'};">{escape(_supplement_status_text(item, generated_at=supplement.generated_at, source='OKX'))}</span></td>
        </tr>
        """
        for item in supplement.timeframes
    )
    return f"""
    <table width="100%" border="0" cellspacing="0" cellpadding="0" style="margin-top: 16px; border-top: 1px dashed #d7deea;">
        <tr>
            <td style="padding: 14px 0 10px 0; font-size: 14px; font-weight: 700; color: #175cd3;">BTC EMA15 + MA50 补充分析</td>
        </tr>
        <tr>
            <td style="padding: 0 0 10px 0; font-size: 13px; line-height: 1.7; color: #34495e;">
                <strong>结论：</strong>{escape(supplement.summary)}
            </td>
        </tr>
        <tr>
            <td>{chart_html}</td>
        </tr>
        <tr>
            <td>
                <table width="100%" border="0" cellspacing="0" cellpadding="0" style="font-size: 13px; color: #34495e; line-height: 1.7;">
                    {rows_html}
                </table>
            </td>
        </tr>
    </table>
    """


def _build_btc_volatility_html_block(
    supplement: BtcVolatilitySupplement,
    *,
    chart_image_map: dict[str, dict[str, str]],
    data_statuses: dict[str, ChartDataStatus],
    overlay_legends: dict[str, str],
) -> str:
    chart_html = _build_coin_chart_html_with_headline(
        asset="BTC波动率",
        symbol=supplement.symbol,
        chart_image_map=chart_image_map,
        data_statuses=data_statuses,
        overlay_legends=overlay_legends,
        headline="BTC 波动率 EMA15 + MA50 最近 72 根 1H / 4H / 1D / 1W K 线",
    )
    rows_html = "".join(
        f"""
        <tr>
            <td style="padding: 4px 0;"><strong>{escape(item.timeframe)}：</strong>{escape(item.summary)}<br><span style="font-size: 11px; color: {'#b42318' if data_statuses.get(item.timeframe) and data_statuses[item.timeframe].is_stale else '#667085'};">{escape(_supplement_status_text(item, generated_at=supplement.generated_at, source=item.source))}</span></td>
        </tr>
        """
        for item in supplement.timeframes
    )
    return f"""
    <table width="100%" border="0" cellspacing="0" cellpadding="0" style="margin-top: 16px; border-top: 1px dashed #d7deea;">
        <tr>
            <td style="padding: 14px 0 10px 0; font-size: 14px; font-weight: 700; color: #175cd3;">BTC 波动率 EMA15 + MA50 补充分析</td>
        </tr>
        <tr>
            <td style="padding: 0 0 10px 0; font-size: 13px; line-height: 1.7; color: #34495e;">
                <strong>结论：</strong>{escape(supplement.summary)}
            </td>
        </tr>
        <tr>
            <td>{chart_html}</td>
        </tr>
        <tr>
            <td>
                <table width="100%" border="0" cellspacing="0" cellpadding="0" style="font-size: 13px; color: #34495e; line-height: 1.7;">
                    {rows_html}
                </table>
            </td>
        </tr>
    </table>
    """


def _build_btc_ema15_ma50_supplement(
    client: OkxRestClient,
    *,
    symbol: str,
    generated_at: str,
) -> BtcEma15Ma50Supplement | None:
    timeframes: list[BtcEma15Ma50TimeframeAnalysis] = []
    candle_series: list[tuple[str, tuple[Candle, ...]]] = []
    for timeframe in BTC_EMA15_MA50_TIMEFRAMES:
        candles = _load_chart_candles(symbol, timeframe, limit=120, client=client)
        if candles:
            candle_series.append((timeframe, tuple(candles[-320:])))
        item = _build_btc_ema15_ma50_timeframe_analysis(timeframe, candles)
        if item is not None:
            timeframes.append(item)
    if not timeframes:
        return None
    direction, summary = _build_btc_ema15_ma50_overall_summary(tuple(timeframes))
    return BtcEma15Ma50Supplement(
        symbol=symbol,
        generated_at=generated_at,
        direction=direction,
        summary=summary,
        timeframes=tuple(timeframes),
        candle_series=tuple(candle_series),
    )


def _build_btc_volatility_ema15_ma50_supplement(
    client: OkxRestClient,
    *,
    symbol: str,
    generated_at: str,
) -> BtcVolatilitySupplement | None:
    volatility_series_by_timeframe, source_by_timeframe = _collect_btc_volatility_candle_series(
        symbol,
        client=client,
        timeframes=BTC_VOLATILITY_EMA15_MA50_TIMEFRAMES,
        limit=120,
    )
    timeframes: list[BtcVolatilityTimeframeAnalysis] = []
    for timeframe in BTC_VOLATILITY_EMA15_MA50_TIMEFRAMES:
        candles = volatility_series_by_timeframe.get(timeframe, [])
        source = source_by_timeframe.get(timeframe, "")
        item = _build_btc_volatility_timeframe_analysis(timeframe, candles, source=source)
        if item is not None:
            timeframes.append(item)
    if not timeframes:
        return None
    direction, summary = _build_btc_volatility_overall_summary(tuple(timeframes))
    return BtcVolatilitySupplement(
        symbol=symbol,
        generated_at=generated_at,
        direction=direction,
        summary=summary,
        timeframes=tuple(timeframes),
        candle_series=tuple(
            (timeframe, tuple(candles[-320:]))
            for timeframe, candles in volatility_series_by_timeframe.items()
            if candles
        ),
    )


def _collect_btc_volatility_candle_series(
    symbol: str,
    *,
    client: OkxRestClient,
    timeframes: tuple[str, ...],
    limit: int,
) -> tuple[dict[str, list], dict[str, str]]:
    deribit_client = DeribitRestClient()
    series_by_timeframe: dict[str, list] = {}
    source_by_timeframe: dict[str, str] = {}
    for timeframe in timeframes:
        price_candles = _load_chart_candles(symbol, timeframe, limit=limit, client=client)
        candles, source = _load_btc_volatility_timeframe_candles(
            symbol,
            timeframe,
            price_candles=price_candles,
            limit=limit,
            deribit_client=deribit_client,
        )
        if candles:
            series_by_timeframe[timeframe] = candles
            source_by_timeframe[timeframe] = source
    return series_by_timeframe, source_by_timeframe


def _load_btc_volatility_timeframe_candles(
    symbol: str,
    timeframe: str,
    *,
    price_candles: list,
    limit: int,
    deribit_client: DeribitRestClient | None = None,
) -> tuple[list, str]:
    deribit_candles = _load_deribit_volatility_timeframe_candles(
        symbol,
        timeframe,
        limit=limit,
        deribit_client=deribit_client,
    )
    if deribit_candles:
        return deribit_candles, "Deribit 波动率指数"
    realized = _build_realized_volatility_from_reference_for_digest(price_candles, bar=timeframe, lookback=20)
    if realized:
        return realized[-limit:], "降级：程序历史波动率"
    return [], ""


def _load_deribit_volatility_timeframe_candles(
    symbol: str,
    timeframe: str,
    *,
    limit: int,
    deribit_client: DeribitRestClient | None = None,
) -> list:
    client = deribit_client or DeribitRestClient()
    end_dt = datetime.now(timezone.utc)
    lookback_days = {"1H": 30, "4H": 90, "1D": 240, "1W": 840}.get(timeframe, 30)
    start_dt = end_dt - timedelta(days=lookback_days)
    hourly_records = min(20_000, max(limit * {"1H": 1, "4H": 8, "1D": 28, "1W": 168}.get(timeframe, 1), 500))
    try:
        hourly = client.get_volatility_index_candles(
            symbol.strip().upper().split("-", 1)[0] or "BTC",
            "3600",
            start_ts=int(start_dt.timestamp() * 1000),
            end_ts=int(end_dt.timestamp() * 1000),
            max_records=hourly_records,
        )
    except Exception:
        return []
    if not hourly:
        return []
    merged = _aggregate_deribit_volatility_candles_for_digest(
        hourly,
        {"1H": 3_600_000, "4H": 14_400_000, "1D": 86_400_000, "1W": 604_800_000}.get(timeframe, 3_600_000),
    )
    if limit > 0:
        merged = merged[-limit:]
    return [_candle_from_deribit_for_digest(item) for item in merged]


def _aggregate_deribit_volatility_candles_for_digest(
    candles: list[DeribitVolatilityCandle],
    resolution_ms: int,
) -> list[DeribitVolatilityCandle]:
    if not candles:
        return []
    buckets: dict[int, list[DeribitVolatilityCandle]] = {}
    for candle in sorted(candles, key=lambda item: item.ts):
        key = _deribit_volatility_bucket_start_ms_for_digest(int(candle.ts), resolution_ms)
        buckets.setdefault(key, []).append(candle)
    aggregated: list[DeribitVolatilityCandle] = []
    for key in sorted(buckets):
        group = buckets[key]
        aggregated.append(
            DeribitVolatilityCandle(
                ts=key,
                open=group[0].open,
                high=max(item.high for item in group),
                low=min(item.low for item in group),
                close=group[-1].close,
            )
        )
    return aggregated


def _deribit_volatility_bucket_start_ms_for_digest(ts_ms: int, resolution_ms: int) -> int:
    if resolution_ms not in (3_600_000, 14_400_000, 86_400_000, 604_800_000):
        return (ts_ms // resolution_ms) * resolution_ms
    dt_utc = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    local = dt_utc.astimezone(timezone(timedelta(hours=8)))
    if resolution_ms == 3_600_000:
        floored = local.replace(minute=0, second=0, microsecond=0)
    elif resolution_ms == 14_400_000:
        floored = local.replace(minute=0, second=0, microsecond=0)
        floored = floored.replace(hour=(floored.hour // 4) * 4)
    elif resolution_ms == 86_400_000:
        floored = local.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        start_of_day = local.replace(hour=0, minute=0, second=0, microsecond=0)
        floored = start_of_day - timedelta(days=start_of_day.weekday())
    return int(floored.astimezone(timezone.utc).timestamp() * 1000)


def _candle_from_deribit_for_digest(item: DeribitVolatilityCandle) -> Candle:
    return Candle(
        ts=item.ts,
        open=item.open,
        high=item.high,
        low=item.low,
        close=item.close,
        volume=Decimal("0"),
        confirmed=True,
    )


def _annualization_factor_for_digest_bar(bar: str) -> float:
    periods_per_year = {
        "1H": 365 * 24,
        "4H": 365 * 6,
        "1D": 365,
        "1W": 52,
    }.get(bar.strip())
    return math.sqrt(periods_per_year) if periods_per_year else 0.0


def _build_realized_volatility_from_reference_for_digest(reference_candles: list, *, bar: str, lookback: int) -> list[Candle]:
    confirmed = [item for item in reference_candles if getattr(item, "confirmed", False)]
    if len(confirmed) < lookback + 1:
        return []
    annualization = _annualization_factor_for_digest_bar(bar)
    if annualization <= 0:
        return []
    output: list[Candle] = []
    previous_close_vol: float | None = None
    for index in range(lookback, len(confirmed)):
        closes = [float(item.close) for item in confirmed[index - lookback : index + 1]]
        if any(value <= 0 for value in closes):
            continue
        returns = [math.log(closes[offset] / closes[offset - 1]) for offset in range(1, len(closes))]
        if not returns:
            continue
        mean_return = sum(returns) / len(returns)
        variance = sum((item - mean_return) ** 2 for item in returns) / len(returns)
        close_vol = math.sqrt(max(variance, 0.0)) * annualization * 100.0
        open_vol = previous_close_vol if previous_close_vol is not None else close_vol
        candle = confirmed[index]
        output.append(
            Candle(
                ts=candle.ts,
                open=Decimal(str(open_vol)),
                high=Decimal(str(max(open_vol, close_vol))),
                low=Decimal(str(min(open_vol, close_vol))),
                close=Decimal(str(close_vol)),
                volume=Decimal("0"),
                confirmed=True,
            )
        )
        previous_close_vol = close_vol
    return output


def _build_btc_volatility_timeframe_analysis(
    timeframe: str,
    candles: list,
    *,
    source: str,
) -> BtcVolatilityTimeframeAnalysis | None:
    if not candles:
        return None
    closes = [item.close for item in candles]
    ema15_values = moving_average(closes, 15, "ema")
    ma50_values = moving_average(closes, 50, "ma")
    if not ema15_values or not ma50_values:
        return None
    latest_close = closes[-1]
    latest_ema15 = ema15_values[-1]
    latest_ma50 = ma50_values[-1]
    if latest_ema15 is None or latest_ma50 is None:
        return None
    structure = _btc_ema15_ma50_structure(latest_close, latest_ema15, latest_ma50)
    shape_summary = _btc_ema15_ma50_shape_summary(
        timeframe,
        close=latest_close,
        ema15=latest_ema15,
        ma50=latest_ma50,
        ema15_values=ema15_values,
        ma50_values=ma50_values,
    )
    kline_pattern_summary = _btc_ema15_ma50_kline_pattern_summary(timeframe, candles)
    return BtcVolatilityTimeframeAnalysis(
        timeframe=timeframe,
        candle_ts=int(candles[-1].ts),
        candle_confirmed=bool(candles[-1].confirmed),
        last_close=latest_close,
        ema15=latest_ema15,
        ma50=latest_ma50,
        direction=_btc_ema15_ma50_direction(latest_close, latest_ema15, latest_ma50),
        structure=structure,
        source=source,
        summary=_btc_volatility_timeframe_summary(
            timeframe,
            latest_close,
            latest_ema15,
            latest_ma50,
            source_label=source,
            structure=structure,
            shape_summary=shape_summary,
            kline_pattern_summary=kline_pattern_summary,
        ),
    )


def _btc_volatility_timeframe_summary(
    timeframe: str,
    close: Decimal,
    ema15: Decimal,
    ma50: Decimal,
    *,
    source_label: str,
    structure: str,
    shape_summary: str,
    kline_pattern_summary: str,
) -> str:
    trade_conclusion = _btc_volatility_trade_conclusion(
        timeframe,
        close=close,
        ema15=ema15,
        ma50=ma50,
        source_label=source_label,
        structure=structure,
        shape_summary=shape_summary,
        kline_pattern_summary=kline_pattern_summary,
    )
    if trade_conclusion:
        return (
            f"{trade_conclusion} | vol close {_fmt_decimal(close)} / EMA15 {_fmt_decimal(ema15)} / MA50 {_fmt_decimal(ma50)}"
            f" | 距 EMA15 {_safe_pct_distance(close, ema15)} | 距 MA50 {_safe_pct_distance(close, ma50)}"
        )
    source_prefix = f"{timeframe} [{source_label}] " if source_label else f"{timeframe} "
    return (
        f"{source_prefix}{structure} | vol close {_fmt_decimal(close)} / EMA15 {_fmt_decimal(ema15)} / MA50 {_fmt_decimal(ma50)}"
        f" | 距 EMA15 {_safe_pct_distance(close, ema15)} | 距 MA50 {_safe_pct_distance(close, ma50)} | 先看波动率收敛"
    )


def _btc_volatility_trade_conclusion(
    timeframe: str,
    *,
    close: Decimal,
    ema15: Decimal,
    ma50: Decimal,
    source_label: str,
    structure: str,
    shape_summary: str,
    kline_pattern_summary: str,
) -> str:
    if timeframe not in {"4H", "1D"}:
        return ""
    prefix = f"{timeframe} [{source_label}] " if source_label else f"{timeframe} "
    below_both_averages = close < ema15 and close < ma50
    if structure in {"多头排列", "多头回踩"}:
        if below_both_averages:
            return f"{prefix}波动率高位回落后仍在降温，虽然中期均线还没完全转空，但短线风险释放更明显；操作上先别按波动扩张处理，等波动率重新站回 EMA15 再决定是否收紧仓位。"
        structure_text = "EMA15 维持在 MA50 上方" if "拉开 MA50" in shape_summary or "多头发散" in shape_summary else "均线仍偏向扩张"
        kline_text = (
            "波动放大后仍有承接"
            if "长下影" in kline_pattern_summary or "下探后被拉回" in kline_pattern_summary or "试底" in kline_pattern_summary
            else "波动扩张还在延续"
        )
        return f"{prefix}波动率继续抬升，{structure_text}，{kline_text}；操作上降低追价意愿，控制仓位，优先等波动率回落后再跟随。"
    if structure == "空头反抽" or "收敛" in shape_summary:
        kline_text = (
            "日线波动回拉但仍在收敛观察区"
            if "大阳线" in kline_pattern_summary or "买盘发力明显" in kline_pattern_summary
            else "波动率反抽但还没重新进入扩张"
        )
        return f"{prefix}波动率从高位回落后出现反抽，{kline_text}；操作上先看波动率是否重新上拐，再决定是否收紧仓位。"
    if structure == "空头排列":
        structure_text = "EMA15 压在 MA50 下方" if "空头发散" in shape_summary or "远离 MA50" in shape_summary else "波动回落仍占主导"
        return f"{prefix}波动率持续回落，{structure_text}，短线风险释放为主；操作上可按原趋势节奏跟踪，但仍防止波动率二次抬头。"
    return f"{prefix}波动率方向未完全明确，均线与K线信号仍在拉扯；操作上先控制频率，等波动率重新选方向。"


def _build_btc_ema15_ma50_timeframe_analysis(
    timeframe: str,
    candles: list,
) -> BtcEma15Ma50TimeframeAnalysis | None:
    if not candles:
        return None
    closes = [item.close for item in candles]
    ema15_values = moving_average(closes, 15, "ema")
    ma50_values = moving_average(closes, 50, "ma")
    if not ema15_values or not ma50_values:
        return None
    latest_close = closes[-1]
    latest_ema15 = ema15_values[-1]
    latest_ma50 = ma50_values[-1]
    if latest_ema15 is None or latest_ma50 is None:
        return None
    direction = _btc_ema15_ma50_direction(latest_close, latest_ema15, latest_ma50)
    structure = _btc_ema15_ma50_structure(latest_close, latest_ema15, latest_ma50)
    shape_summary = _btc_ema15_ma50_shape_summary(
        timeframe,
        close=latest_close,
        ema15=latest_ema15,
        ma50=latest_ma50,
        ema15_values=ema15_values,
        ma50_values=ma50_values,
    )
    kline_pattern_summary = _btc_ema15_ma50_kline_pattern_summary(timeframe, candles)
    return BtcEma15Ma50TimeframeAnalysis(
        timeframe=timeframe,
        candle_ts=int(candles[-1].ts),
        candle_confirmed=bool(candles[-1].confirmed),
        last_close=latest_close,
        ema15=latest_ema15,
        ma50=latest_ma50,
        direction=direction,
        structure=structure,
        summary=_btc_ema15_ma50_timeframe_summary(
            timeframe,
            latest_close,
            latest_ema15,
            latest_ma50,
            direction=direction,
            structure=structure,
            shape_summary=shape_summary,
            kline_pattern_summary=kline_pattern_summary,
        ),
    )


def _btc_ema15_ma50_direction(close: Decimal, ema15: Decimal, ma50: Decimal) -> str:
    if close > ema15 > ma50:
        return "long"
    if close < ema15 < ma50:
        return "short"
    return "neutral"


def _btc_ema15_ma50_structure(close: Decimal, ema15: Decimal, ma50: Decimal) -> str:
    if ema15 > ma50 and close >= ema15:
        return "多头排列"
    if ema15 < ma50 and close <= ema15:
        return "空头排列"
    if ema15 > ma50 and close < ema15:
        return "多头回踩"
    if ema15 < ma50 and close > ema15:
        return "空头反抽"
    if close >= ma50:
        return "围绕 MA50 偏强震荡"
    return "围绕 MA50 偏弱震荡"


def _btc_ema15_ma50_timeframe_summary(
    timeframe: str,
    close: Decimal,
    ema15: Decimal,
    ma50: Decimal,
    *,
    direction: str,
    structure: str,
    shape_summary: str,
    kline_pattern_summary: str,
) -> str:
    trade_conclusion = _btc_ema15_ma50_trade_conclusion(
        timeframe,
        direction=direction,
        structure=structure,
        shape_summary=shape_summary,
        kline_pattern_summary=kline_pattern_summary,
    )
    action_text = "顺势为主" if direction == "long" else "优先等反抽做空" if direction == "short" else "先等方向收敛"
    if trade_conclusion:
        return (
            f"{trade_conclusion} | close {_fmt_decimal(close)} / EMA15 {_fmt_decimal(ema15)} / MA50 {_fmt_decimal(ma50)}"
            f" | 距 EMA15 {_safe_pct_distance(close, ema15)} | 距 MA50 {_safe_pct_distance(close, ma50)}"
        )
    shape_text = f" | 形态{shape_summary}" if timeframe in {"4H", "1D"} and shape_summary else ""
    kline_text = f" | {kline_pattern_summary}" if timeframe in {"4H", "1D"} and kline_pattern_summary else ""
    return (
        f"{timeframe} {structure}{shape_text}{kline_text} | close {_fmt_decimal(close)} / EMA15 {_fmt_decimal(ema15)} / MA50 {_fmt_decimal(ma50)}"
        f" | 距 EMA15 {_safe_pct_distance(close, ema15)} | 距 MA50 {_safe_pct_distance(close, ma50)} | {action_text}"
    )


def _btc_ema15_ma50_trade_conclusion(
    timeframe: str,
    *,
    direction: str,
    structure: str,
    shape_summary: str,
    kline_pattern_summary: str,
) -> str:
    if timeframe not in {"4H", "1D"}:
        return ""
    if direction == "long" or structure in {"多头排列", "多头回踩"}:
        trend_text = f"{timeframe} 多头主导"
        structure_text = (
            "EMA15 持续拉开 MA50"
            if "拉开 MA50" in shape_summary or "多头发散" in shape_summary
            else "EMA15 仍压在 MA50 上方"
        )
        kline_text = (
            "回踩承接仍在"
            if "下探后被拉回" in kline_pattern_summary or "长下影" in kline_pattern_summary or "试底" in kline_pattern_summary
            else "K线配合仍偏强"
        )
        return f"{trend_text}，{structure_text}，{kline_text}，短线继续偏多；操作上优先等回踩 EMA15 再跟随，不追高。"
    if structure == "空头反抽" or (direction == "neutral" and "收敛" in shape_summary):
        trend_text = f"{timeframe} 空头结构出现反抽修复"
        kline_text = (
            "日线买盘回拉但仍以收敛观察为主"
            if "买盘发力明显" in kline_pattern_summary or "大阳线" in kline_pattern_summary
            else "反抽修复仍未改写空头结构"
        )
        return f"{trend_text}，{kline_text}；操作上先等 EMA15 与 MA50 进一步收敛后再决定是否跟进。"
    if direction == "short" or structure == "空头排列":
        trend_text = f"{timeframe} 空头主导"
        structure_text = (
            "EMA15 继续压在 MA50 下方"
            if "远离 MA50" in shape_summary or "空头发散" in shape_summary
            else "均线压制仍在"
        )
        kline_text = (
            "上方抛压没有明显松动"
            if "上影" in kline_pattern_summary or "冲高回落" in kline_pattern_summary
            else "K线配合仍偏弱"
        )
        return f"{trend_text}，{structure_text}，{kline_text}；操作上优先等反抽 EMA15 再处理空单，不低位追空。"
    return f"{timeframe} 方向仍在收敛，均线与K线信号暂未完全同向；操作上先观察，等结构进一步明确后再跟进。"


def _btc_ema15_ma50_kline_pattern_summary(timeframe: str, candles: list) -> str:
    if timeframe not in {"4H", "1D"}:
        return ""
    events = build_pattern_focus_events(candles, timeframe=timeframe, limit=1)
    if not events:
        return ""
    primary = events[0]
    return f"K线形态{primary.label}：{primary.summary}"


def _btc_ema15_ma50_shape_summary(
    timeframe: str,
    *,
    close: Decimal,
    ema15: Decimal,
    ma50: Decimal,
    ema15_values: list[Decimal | None],
    ma50_values: list[Decimal | None],
) -> str:
    if timeframe not in {"4H", "1D"}:
        return ""
    ema15_slope_3 = _series_change_ratio(ema15_values, 3)
    ema15_slope_5 = _series_change_ratio(ema15_values, 5)
    ma50_slope_5 = _series_change_ratio(ma50_values, 5)
    spread_now = _relative_spread_ratio(ema15, ma50)
    spread_prev = _relative_spread_ratio(
        _last_valid_series_value(ema15_values, offset=3),
        _last_valid_series_value(ma50_values, offset=3),
    )
    gap_expanding = spread_now is not None and spread_prev is not None and abs(spread_now) > abs(spread_prev)
    gap_contracting = spread_now is not None and spread_prev is not None and abs(spread_now) < abs(spread_prev)
    near_ema15 = _abs_pct_distance_ratio(close, ema15) <= Decimal("0.015")

    if ema15 > ma50:
        if ema15_slope_3 > 0 and ma50_slope_5 > 0 and gap_expanding:
            return "EMA15 上拐并继续拉开 MA50，多头发散，趋势延续。"
        if near_ema15 and gap_contracting:
            return "价格回到 EMA15 附近，两线开始收口，更像多头回踩确认。"
        if ema15_slope_3 <= 0 and ema15_slope_5 <= 0:
            return "EMA15 走平转弱，虽然仍在 MA50 上方，但多头推进减速。"
        return "EMA15 仍压在 MA50 上方，但两线扩张一般，偏强整理。"

    if ema15 < ma50:
        if ema15_slope_3 < 0 and ma50_slope_5 < 0 and gap_expanding:
            return "EMA15 下压并继续远离 MA50，空头发散，弱势延续。"
        if close > ema15 and gap_contracting:
            return "价格重新站回 EMA15 上方，EMA15 向 MA50 收敛，当前更像空头反抽后的收口观察。"
        if ema15_slope_3 > 0 and ema15_slope_5 > 0:
            return "EMA15 已上拐靠近 MA50，空头压制减弱，正在酝酿反抽或阶段性筑底。"
        return "EMA15 仍在 MA50 下方，但没有继续明显发散，暂时以反抽整理看待。"

    return "EMA15 与 MA50 明显贴近收口，处在临近变盘的均线挤压状态。"


def _series_change_ratio(values: list[Decimal | None], lookback: int) -> Decimal:
    current = _last_valid_series_value(values, offset=0)
    previous = _last_valid_series_value(values, offset=lookback)
    if current is None or previous is None or previous == 0:
        return Decimal("0")
    return (current - previous) / previous


def _last_valid_series_value(values: list[Decimal | None], *, offset: int) -> Decimal | None:
    index = len(values) - 1 - max(offset, 0)
    while index >= 0:
        value = values[index]
        if value is not None:
            return value
        index -= 1
    return None


def _relative_spread_ratio(left: Decimal | None, right: Decimal | None) -> Decimal | None:
    if left is None or right is None or right == 0:
        return None
    return (left - right) / right


def _abs_pct_distance_ratio(left: Decimal, right: Decimal) -> Decimal:
    if right == 0:
        return Decimal("999")
    return abs((left - right) / right)


def _build_btc_ema15_ma50_overall_summary(
    timeframes: tuple[BtcEma15Ma50TimeframeAnalysis, ...],
) -> tuple[str, str]:
    direction_counts = {"long": 0, "short": 0, "neutral": 0}
    for item in timeframes:
        direction_counts[item.direction] = direction_counts.get(item.direction, 0) + 1
    if direction_counts["long"] >= 3 and direction_counts["short"] == 0:
        return "long", "4 个周期里多数保持多头结构，BTC 额外观察口径偏多，优先等回踩 EMA15 后再跟随。"
    if direction_counts["short"] >= 3 and direction_counts["long"] == 0:
        return "short", "4 个周期里多数保持空头结构，BTC 额外观察口径偏空，优先等反抽 EMA15 后再处理空头。"
    if direction_counts["long"] > direction_counts["short"]:
        return "long", "短中周期偏多，但并非全周期一致，BTC 额外观察口径以偏多跟踪为主，避免直接追价。"
    if direction_counts["short"] > direction_counts["long"]:
        return "short", "短中周期偏空，但并非全周期一致，BTC 额外观察口径以偏空跟踪为主，避免在低位追空。"
    return "neutral", "EMA15 与 MA50 在 4 个周期里分歧较大，BTC 额外观察口径先看收敛，等待 1H/4H 重新同向。"


def _build_btc_volatility_overall_summary(
    timeframes: tuple[BtcVolatilityTimeframeAnalysis, ...],
) -> tuple[str, str]:
    direction_counts = {"long": 0, "short": 0, "neutral": 0}
    sources = sorted({item.source for item in timeframes if item.source})
    source_text = " / ".join(sources) if sources else "当前可用波动率序列"
    for item in timeframes:
        direction_counts[item.direction] = direction_counts.get(item.direction, 0) + 1
    if direction_counts["long"] >= 3 and direction_counts["short"] == 0:
        return "long", f"{source_text}多数周期仍在扩张，短线先把风险控制放在交易节奏前面，避免追价追单。"
    if direction_counts["short"] >= 3 and direction_counts["long"] == 0:
        return "short", f"{source_text}多数周期持续回落，风险释放为主，但仍要防止波动率二次抬头。"
    if direction_counts["long"] > direction_counts["short"]:
        return "long", f"{source_text}短中周期仍偏扩张，但不是全周期一致，操作上以控仓和等回落为主。"
    if direction_counts["short"] > direction_counts["long"]:
        return "short", f"{source_text}短中周期偏回落，市场风险在释放，但暂时不建议因为波动率回落就放松纪律。"
    return "neutral", f"{source_text}当前仍在收敛与反抽之间切换，先观察 4H/1D 是否重新同向。"


def _btc_ema15_ma50_payload(supplement: BtcEma15Ma50Supplement) -> dict[str, object]:
    return {
        "symbol": supplement.symbol,
        "generated_at": supplement.generated_at,
        "direction": supplement.direction,
        "summary": supplement.summary,
        "timeframes": [
            {
                "timeframe": item.timeframe,
                "candle_ts": item.candle_ts,
                "candle_confirmed": item.candle_confirmed,
                "last_close": str(item.last_close) if item.last_close is not None else None,
                "ema15": str(item.ema15) if item.ema15 is not None else None,
                "ma50": str(item.ma50) if item.ma50 is not None else None,
                "direction": item.direction,
                "structure": item.structure,
                "summary": item.summary,
            }
            for item in supplement.timeframes
        ],
    }


def _btc_volatility_ema15_ma50_payload(supplement: BtcVolatilitySupplement) -> dict[str, object]:
    return {
        "symbol": supplement.symbol,
        "generated_at": supplement.generated_at,
        "direction": supplement.direction,
        "summary": supplement.summary,
        "timeframes": [
            {
                "timeframe": item.timeframe,
                "candle_ts": item.candle_ts,
                "candle_confirmed": item.candle_confirmed,
                "last_close": str(item.last_close) if item.last_close is not None else None,
                "ema15": str(item.ema15) if item.ema15 is not None else None,
                "ma50": str(item.ma50) if item.ma50 is not None else None,
                "direction": item.direction,
                "structure": item.structure,
                "source": item.source,
                "summary": item.summary,
            }
            for item in supplement.timeframes
        ],
    }


def _build_btc_ema15_ma50_overlay_map(symbol: str) -> dict[str, dict[str, tuple[MiniChartOverlay, ...]]]:
    overlays = (
        MiniChartOverlay(period=15, ma_type="ema"),
        MiniChartOverlay(period=50, ma_type="ma"),
    )
    return {
        symbol: {
            "1H": overlays,
            "4H": overlays,
            "1D": overlays,
            "1W": overlays,
        }
    }


def _safe_pct_distance(left: Decimal, right: Decimal) -> str:
    if right == 0:
        return "-"
    return f"{((left - right) / right) * Decimal('100'):.2f}%"


def _fmt_decimal(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return format(value.quantize(Decimal("0.01")), "f")


def _build_single_chart_cell(
    *,
    asset: str,
    timeframe: str,
    encoded: str,
    data_status: ChartDataStatus | None = None,
) -> str:
    status_html = ""
    if data_status is not None:
        status_color = "#b42318" if data_status.is_stale else "#667085"
        status_html = f"""
            <tr>
                <td style="font-size: 11px; color: {status_color}; padding: 0 0 6px 0; font-weight: {'700' if data_status.is_stale else '400'};">
                    {escape(data_status.status_text)}
                </td>
            </tr>
        """
    if not encoded.strip():
        return f"""
        <table width="100%" border="0" cellspacing="0" cellpadding="0">
            <tr>
                <td style="font-size: 12px; color: #7f8c8d; padding: 0 0 6px 0;">{escape(timeframe)}</td>
            </tr>
            <tr>
                <td style="font-size: 11px; color: #94a3b8; padding: 0 0 6px 0;">__OVERLAY_LEGEND__</td>
            </tr>
            {status_html}
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
        {status_html}
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
