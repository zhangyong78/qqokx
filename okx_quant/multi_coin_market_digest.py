from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
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
from okx_quant.persistence import (
    analysis_report_dir_path,
    load_btc_market_email_state,
    save_btc_market_email_state,
)


DEFAULT_DIGEST_SYMBOLS: tuple[str, ...] = (
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "BNB-USDT-SWAP",
    "SOL-USDT-SWAP",
    "DOGE-USDT-SWAP",
)


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
    email_state = load_btc_market_email_state()
    last_sent_at = _parse_iso_datetime(email_state.get("last_sent_at", ""))
    lines = [
        "简明结论：",
        f"- 做多最强：{_leader_headline(digest.strongest_long)}",
        f"- 做空最弱：{_leader_headline(digest.weakest_short)}",
        f"- 最值得跟踪做单：{digest.best_trade_candidate.label}。{digest.best_trade_candidate.summary}",
        "",
        "分币摘要：",
    ]
    for analysis in digest.analyses:
        lines.extend(_build_coin_section(analysis, last_sent_at=last_sent_at))
    return "\n".join(lines)


def send_multi_coin_market_email(
    digest: MultiCoinMarketDigest,
    *,
    report_path: Path | None = None,
) -> bool:
    notifier = load_btc_market_email_notifier()
    if notifier is None or not notifier.enabled:
        return False
    subject = build_multi_coin_market_email_subject(digest)
    body = build_multi_coin_market_email_body(digest)
    sender = getattr(notifier, "_send", None)
    if callable(sender):
        sender(subject, body)
    else:
        notifier.notify_async(subject, body)
    save_btc_market_email_state(
        last_sent_at=datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        last_subject=subject,
        last_report_path=str(report_path) if report_path is not None else "",
    )
    return True


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
