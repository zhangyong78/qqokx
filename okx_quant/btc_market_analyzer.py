from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Literal

from okx_quant.candle_patterns import (
    PatternBias,
    SingleCandlePatternReport,
    analyze_single_candle_pattern_history,
    analyze_single_candle_patterns,
    single_candle_report_payload,
)
from okx_quant.btc_market_replay import ReplayValidation, build_replay_validation, replay_validation_payload
from okx_quant.indicators import bollinger_bands, ema, macd
from okx_quant.market_analysis import MarketAnalysisConfig, MarketAnalysisReport, build_market_analysis_report
from okx_quant.models import Candle, EmailNotificationConfig
from okx_quant.notifications import EmailNotifier
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import (
    analysis_report_dir_path,
    load_btc_market_email_state,
    load_notification_snapshot,
    save_btc_market_email_state,
)
from okx_quant.signal_replay_engine import SignalReplayConfig, build_signal_replay_dataset

AnalysisDirection = Literal["long", "short", "neutral"]
SignalBias = Literal["long", "short", "caution", "neutral"]

ZERO = Decimal("0")
ONE = Decimal("1")
FOCUS_EMAIL_TIMEFRAMES = {"4H", "1H"}
FOCUS_PATTERN_IDS = {
    "false_breakdown",
    "false_breakout",
    "top_fractal",
    "bottom_fractal",
    "big_bullish",
    "big_bearish",
    "long_upper_shadow",
    "long_lower_shadow",
    "inside_bar",
}
FOCUS_SINGLE_CANDLE_PATTERNS = {
    "hammer",
    "hanging_man",
    "inverted_hammer",
    "shooting_star",
    "bullish_marubozu",
    "bearish_marubozu",
    "dragonfly_doji",
    "gravestone_doji",
}


@dataclass(frozen=True)
class MarketSignal:
    name: str
    category: str
    timeframe: str | None
    bias: SignalBias
    score: int
    strength: Decimal
    trend_context: str
    reason: str


@dataclass(frozen=True)
class PatternFocusEvent:
    timeframe: str
    ts: int
    pattern_id: str
    label: str
    direction: SignalBias
    score: int
    candle_count: int
    source: str
    summary: str
    reason: str


@dataclass(frozen=True)
class TimeframeAnalysis:
    symbol: str
    timeframe: str
    candle_ts: int | None
    last_close: Decimal | None
    direction: AnalysisDirection
    score: int
    confidence: Decimal
    trend_context: str
    signals: tuple[MarketSignal, ...]
    reason: tuple[str, ...]
    probability: dict[str, object]
    indicators: dict[str, object]
    pattern: dict[str, object]
    focus_events: tuple[PatternFocusEvent, ...] = ()


@dataclass(frozen=True)
class ResonanceAnalysis:
    direction: AnalysisDirection
    aligned_timeframes: tuple[str, ...]
    score: int
    confidence: Decimal
    summary: str


@dataclass(frozen=True)
class BtcMarketAnalysis:
    symbol: str
    generated_at: str
    direction: AnalysisDirection
    score: int
    confidence: Decimal
    resonance: ResonanceAnalysis
    signals: tuple[MarketSignal, ...]
    reason: tuple[str, ...]
    timeframes: tuple[TimeframeAnalysis, ...]
    mode: str = "realtime"
    analysis_timezone: str | None = None
    analysis_point: str | None = None
    analysis_point_utc: str | None = None
    data_cutoff_rule: str | None = None
    validation: ReplayValidation | None = None


@dataclass(frozen=True)
class BtcMarketAnalyzerConfig:
    timeframes: tuple[str, ...] = ("1H", "4H", "1D")
    history_limits: tuple[tuple[str, int], ...] = (("1H", 5000), ("4H", 5000), ("1D", 0))
    default_history_limit: int = 0
    probability_config: MarketAnalysisConfig = field(default_factory=MarketAnalysisConfig)
    ema_periods: tuple[int, int, int] = (21, 55, 233)
    macd_fast_period: int = 12
    macd_slow_period: int = 26
    macd_signal_period: int = 9
    boll_period: int = 20
    boll_std_multiplier: Decimal = Decimal("2")
    timeframe_direction_threshold: int = 2
    aggregate_direction_threshold: int = 4
    timeframe_confidence_divisor: Decimal = Decimal("6")
    aggregate_confidence_divisor: Decimal = Decimal("12")
    resonance_full_alignment_score: int = 3
    resonance_partial_alignment_score: int = 1


def analyze_btc_market_from_client(
    client: OkxRestClient,
    *,
    symbol: str = "BTC-USDT-SWAP",
    config: BtcMarketAnalyzerConfig | None = None,
) -> BtcMarketAnalysis:
    config = config or BtcMarketAnalyzerConfig()
    candle_map: dict[str, list[Candle]] = {}
    for timeframe in config.timeframes:
        candle_map[timeframe] = client.get_candles_history(
            symbol,
            timeframe,
            limit=_history_limit_for_timeframe(config, timeframe),
        )
    return analyze_btc_market_from_candle_map(candle_map, symbol=symbol, config=config, mode="realtime")


def analyze_btc_market_at_time(
    client: OkxRestClient,
    *,
    symbol: str = "BTC-USDT-SWAP",
    analysis_dt: datetime,
    config: BtcMarketAnalyzerConfig | None = None,
    validation_windows_hours: tuple[int, ...] = (4, 12, 24),
) -> BtcMarketAnalysis:
    if analysis_dt.tzinfo is None:
        raise ValueError("analysis_dt must be timezone-aware")
    config = config or BtcMarketAnalyzerConfig()
    analysis_dt_utc = analysis_dt.astimezone(timezone.utc)
    end_ts = int(analysis_dt_utc.timestamp() * 1000)
    preload_count = _indicator_preload_count(config)
    candle_map: dict[str, list[Candle]] = {}
    for timeframe in config.timeframes:
        timeframe_ms = _timeframe_ms(timeframe)
        history_limit = _history_limit_for_timeframe(config, timeframe)
        selected_limit = history_limit if history_limit > 0 else max(preload_count + 240, 360)
        start_ts = max(0, end_ts - ((selected_limit - 1) * timeframe_ms))
        candle_map[timeframe] = client.get_candles_history_range(
            symbol,
            timeframe,
            start_ts=start_ts,
            end_ts=end_ts,
            limit=selected_limit,
            preload_count=preload_count,
        )
        candle_map[timeframe] = [item for item in candle_map[timeframe] if int(item.ts) <= end_ts]
    analysis = analyze_btc_market_from_candle_map(
        candle_map,
        symbol=symbol,
        config=config,
        mode="historical_replay",
        analysis_timezone=str(analysis_dt.tzinfo),
        analysis_point=analysis_dt.isoformat(timespec="seconds"),
        analysis_point_utc=analysis_dt_utc.isoformat(timespec="seconds").replace("+00:00", "Z"),
        data_cutoff_rule="close_time_lte_analysis_point",
    )
    primary = next((item for item in analysis.timeframes if item.candle_ts is not None and item.last_close is not None), None)
    if primary is None:
        return analysis
    max_window_hours = max((hours for hours in validation_windows_hours if hours > 0), default=0)
    future_candles: list[Candle] = []
    if max_window_hours > 0:
        future_candles = client.get_candles_history_range(
            symbol,
            primary.timeframe,
            start_ts=primary.candle_ts + 1,
            end_ts=primary.candle_ts + (max_window_hours * 3_600_000),
            limit=0,
            preload_count=0,
        )
        future_candles = [item for item in future_candles if int(item.ts) > int(primary.candle_ts)]
    validation = build_replay_validation(
        direction=analysis.direction,
        timeframe=primary.timeframe,
        entry_price=primary.last_close,
        analysis_candle_ts=primary.candle_ts,
        future_candles=future_candles,
        timeframe_ms=_timeframe_ms(primary.timeframe),
        windows_hours=validation_windows_hours,
    )
    return BtcMarketAnalysis(
        symbol=analysis.symbol,
        generated_at=analysis.generated_at,
        direction=analysis.direction,
        score=analysis.score,
        confidence=analysis.confidence,
        resonance=analysis.resonance,
        signals=analysis.signals,
        reason=analysis.reason,
        timeframes=analysis.timeframes,
        mode=analysis.mode,
        analysis_timezone=analysis.analysis_timezone,
        analysis_point=analysis.analysis_point,
        analysis_point_utc=analysis.analysis_point_utc,
        data_cutoff_rule=analysis.data_cutoff_rule,
        validation=validation,
    )


def analyze_btc_market_from_candle_map(
    candle_map: dict[str, list[Candle]],
    *,
    symbol: str = "BTC-USDT-SWAP",
    config: BtcMarketAnalyzerConfig | None = None,
    mode: str = "realtime",
    analysis_timezone: str | None = None,
    analysis_point: str | None = None,
    analysis_point_utc: str | None = None,
    data_cutoff_rule: str | None = None,
    validation: ReplayValidation | None = None,
) -> BtcMarketAnalysis:
    config = config or BtcMarketAnalyzerConfig()
    timeframe_results = tuple(
        _analyze_timeframe(
            candles=candle_map.get(timeframe, []),
            symbol=symbol,
            timeframe=timeframe,
            config=config,
        )
        for timeframe in config.timeframes
    )
    resonance = _build_resonance(timeframe_results, config)
    flattened_signals = _sorted_signals(
        [
            signal
            for result in timeframe_results
            for signal in result.signals
            if signal.score != 0
        ]
        + _resonance_signals(resonance)
    )
    aggregate_score = sum(result.score for result in timeframe_results) + resonance.score
    direction = _aggregate_direction(timeframe_results, aggregate_score, resonance, config)
    confidence = _confidence_from_score(aggregate_score, config.aggregate_confidence_divisor)
    reason = tuple(_top_reasons(flattened_signals))
    return BtcMarketAnalysis(
        symbol=symbol,
        generated_at=datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        direction=direction,
        score=aggregate_score,
        confidence=confidence,
        resonance=resonance,
        signals=tuple(flattened_signals),
        reason=reason,
        timeframes=timeframe_results,
        mode=mode,
        analysis_timezone=analysis_timezone,
        analysis_point=analysis_point,
        analysis_point_utc=analysis_point_utc,
        data_cutoff_rule=data_cutoff_rule,
        validation=validation,
    )


def btc_market_analysis_payload(analysis: BtcMarketAnalysis) -> dict[str, object]:
    payload = {
        "symbol": analysis.symbol,
        "generated_at": analysis.generated_at,
        "mode": analysis.mode,
        "analysis_timezone": analysis.analysis_timezone,
        "analysis_point": analysis.analysis_point,
        "analysis_point_utc": analysis.analysis_point_utc,
        "data_cutoff_rule": analysis.data_cutoff_rule,
        "direction": analysis.direction,
        "score": analysis.score,
        "confidence": _decimal_text(analysis.confidence),
        "signals": [_signal_payload(item) for item in analysis.signals],
        "reason": list(analysis.reason),
        "resonance": {
            "direction": analysis.resonance.direction,
            "aligned_timeframes": list(analysis.resonance.aligned_timeframes),
            "score": analysis.resonance.score,
            "confidence": _decimal_text(analysis.resonance.confidence),
            "summary": analysis.resonance.summary,
        },
        "timeframes": [
            {
                "symbol": item.symbol,
                "timeframe": item.timeframe,
                "candle_ts": item.candle_ts,
                "last_close": _decimal_text(item.last_close),
                "direction": item.direction,
                "score": item.score,
                "confidence": _decimal_text(item.confidence),
                "trend_context": item.trend_context,
                "signals": [_signal_payload(signal) for signal in item.signals],
                "reason": list(item.reason),
                "probability": item.probability,
                "indicators": item.indicators,
                "pattern": item.pattern,
                "focus_events": [_focus_event_payload(event) for event in item.focus_events],
            }
            for item in analysis.timeframes
        ],
    }
    validation_payload = replay_validation_payload(analysis.validation)
    if validation_payload is not None:
        payload["validation"] = validation_payload
    return payload


def btc_market_analysis_json(analysis: BtcMarketAnalysis) -> str:
    return json.dumps(btc_market_analysis_payload(analysis), ensure_ascii=False, indent=2)


def save_btc_market_analysis(
    analysis: BtcMarketAnalysis,
    *,
    path: Path | None = None,
    base_dir: Path | None = None,
) -> Path:
    if path is None:
        report_dir = analysis_report_dir_path(base_dir)
        report_dir.mkdir(parents=True, exist_ok=True)
        safe_symbol = _safe_file_component(analysis.symbol)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = report_dir / f"{safe_symbol}_btc_market_summary_{timestamp}.json"
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(btc_market_analysis_json(analysis), encoding="utf-8")
    return path


def build_btc_market_analysis_email_subject(analysis: BtcMarketAnalysis) -> str:
    confidence = f"{analysis.confidence * Decimal('100'):.0f}%"
    return f"[QQOKX] BTC 行情分析 | {analysis.symbol} | {analysis.direction} | {confidence}"


def build_btc_market_analysis_email_body(
    analysis: BtcMarketAnalysis,
    *,
    last_sent_at: datetime | None = None,
) -> str:
    lines = [
        "最简结论：",
        *_build_simple_email_summary(analysis, last_sent_at=last_sent_at),
        "",
        "重点变化：",
        *_build_focus_section(analysis, last_sent_at=last_sent_at),
        "",
        "当前快照：",
        *_build_current_snapshot_section(analysis),
    ]
    return "\n".join(lines)

    lines = [
        f"标的：{analysis.symbol}",
        f"生成时间(UTC)：{analysis.generated_at}",
        f"综合方向：{analysis.direction}",
        f"综合评分：{analysis.score}",
        f"综合置信度：{_format_pct(analysis.confidence)}",
        f"多周期共振：{analysis.resonance.summary}",
        "",
        "核心原因：",
    ]
    for reason in analysis.reason:
        lines.append(f"- {reason}")
    for item in analysis.timeframes:
        lines.extend(
            [
                "",
                f"[{item.timeframe}] 方向={item.direction} | 评分={item.score} | 置信度={_format_pct(item.confidence)}",
            ]
        )
        for reason in item.reason:
            lines.append(f"- {reason}")
    return "\n".join(lines)


def load_btc_market_email_notifier() -> EmailNotifier | None:
    snapshot = load_notification_snapshot()
    recipients = tuple(
        item.strip()
        for item in re.split(r"[,\n;]+", str(snapshot.get("recipient_emails", "")))
        if item.strip()
    )
    config = EmailNotificationConfig(
        enabled=bool(snapshot.get("enabled", False)),
        smtp_host=str(snapshot.get("smtp_host", "")),
        smtp_port=int(snapshot.get("smtp_port", 465)),
        smtp_username=str(snapshot.get("smtp_username", "")),
        smtp_password=str(snapshot.get("smtp_password", "")),
        sender_email=str(snapshot.get("sender_email", "")),
        recipient_emails=recipients,
        use_ssl=bool(snapshot.get("use_ssl", True)),
        notify_trade_fills=bool(snapshot.get("notify_trade_fills", True)),
        notify_signals=bool(snapshot.get("notify_signals", True)),
        notify_errors=bool(snapshot.get("notify_errors", True)),
    )
    notifier = EmailNotifier(config)
    return notifier if notifier.enabled else None


def send_btc_market_analysis_email(
    analysis: BtcMarketAnalysis,
    *,
    notifier: EmailNotifier | None = None,
    report_path: Path | None = None,
) -> bool:
    resolved_notifier = notifier or load_btc_market_email_notifier()
    if resolved_notifier is None or not resolved_notifier.enabled:
        return False
    email_state = load_btc_market_email_state()
    last_sent_at = _parse_iso_datetime(email_state.get("last_sent_at", ""))
    subject = build_btc_market_analysis_email_subject(analysis)
    body = build_btc_market_analysis_email_body(analysis, last_sent_at=last_sent_at)
    sender = getattr(resolved_notifier, "_send", None)
    if callable(sender):
        sender(subject, body)
        _save_email_delivery_state(subject=subject, report_path=report_path)
        return True
    resolved_notifier.notify_async(subject, body)
    _save_email_delivery_state(subject=subject, report_path=report_path)
    return True


def _build_simple_email_summary(
    analysis: BtcMarketAnalysis,
    *,
    last_sent_at: datetime | None,
) -> list[str]:
    lines = [f"- 大方向：{_direction_text(analysis.direction)}。"]
    for timeframe in ("4H", "1H"):
        item = _find_timeframe_analysis(analysis, timeframe)
        if item is None:
            continue
        recent_events = _recent_focus_events(item, last_sent_at=last_sent_at)
        tf_label = _timeframe_display_label(timeframe)
        if recent_events:
            lines.append(f"- {tf_label}：{_direction_text(item.direction)}，新出了{recent_events[0].label}。")
        else:
            lines.append(f"- {tf_label}：{_direction_text(item.direction)}，这段时间没有新的代表性K线。")
    return lines


def _build_focus_section(
    analysis: BtcMarketAnalysis,
    *,
    last_sent_at: datetime | None,
) -> list[str]:
    lines: list[str] = []
    if last_sent_at is None:
        lines.append("- 这是首次定时简报，下面列最近识别到的代表性形态。")
    else:
        lines.append(f"- 对比基线：{_format_local_datetime(last_sent_at)}")
    for timeframe in ("4H", "1H"):
        item = _find_timeframe_analysis(analysis, timeframe)
        if item is None:
            continue
        lines.append(f"- {_timeframe_display_label(timeframe)}：")
        recent_events = _recent_focus_events(item, last_sent_at=last_sent_at)
        if not recent_events:
            lines.append("  没有新的代表性K线。")
            continue
        for event in recent_events[:4]:
            lines.append(f"  {_format_local_timestamp(event.ts)} {event.label}：{event.summary}")
    return lines


def _build_current_snapshot_section(analysis: BtcMarketAnalysis) -> list[str]:
    lines = [
        f"- 标的：{analysis.symbol}",
        f"- 生成时间：{analysis.generated_at}",
        f"- 综合方向：{analysis.direction} | 评分={analysis.score} | 置信度={_format_pct(analysis.confidence)}",
        f"- 多周期共振：{analysis.resonance.summary}",
    ]
    for timeframe in ("4H", "1H", "1D"):
        item = _find_timeframe_analysis(analysis, timeframe)
        if item is None:
            continue
        lines.append(
            f"- [{item.timeframe}] {item.direction} | 评分={item.score} | 置信度={_format_pct(item.confidence)} | 核心：{_top_reason_text(item.reason)}"
        )
    return lines


def _build_focus_events(candles: list[Candle], *, timeframe: str) -> tuple[PatternFocusEvent, ...]:
    if timeframe not in FOCUS_EMAIL_TIMEFRAMES:
        return ()
    events = [
        *_replay_focus_events(candles, timeframe=timeframe),
        *_single_candle_focus_events(candles, timeframe=timeframe),
    ]
    unique: dict[tuple[int, str, str], PatternFocusEvent] = {}
    for event in events:
        key = (event.ts, event.pattern_id, event.source)
        current = unique.get(key)
        if current is None or event.score > current.score:
            unique[key] = event
    ordered = sorted(unique.values(), key=lambda item: (item.ts, item.score, item.candle_count, item.label), reverse=True)
    return tuple(ordered[:12])


def _replay_focus_events(candles: list[Candle], *, timeframe: str) -> list[PatternFocusEvent]:
    dataset = build_signal_replay_dataset(
        candles,
        config=SignalReplayConfig(
            include_long=False,
            include_short=False,
            enable_trend_filter=False,
            enable_pullback_trigger=False,
            enable_macd_filter=False,
            enable_volume_filter=False,
            enable_bias_filter=False,
            enable_near_ema_filter=False,
            enable_atr_filter=False,
            enable_large_move_gate=False,
        ),
    )
    events: list[PatternFocusEvent] = []
    for signal in dataset.signals:
        if signal.pattern_id not in FOCUS_PATTERN_IDS:
            continue
        events.append(
            PatternFocusEvent(
                timeframe=timeframe,
                ts=signal.ts,
                pattern_id=signal.pattern_id,
                label=_pattern_label(signal.pattern_id, signal.pattern_name),
                direction=_signal_direction_text(signal.direction),
                score=signal.score,
                candle_count=signal.candle_count,
                source="replay",
                summary=_pattern_summary(signal.pattern_id, signal.direction),
                reason=signal.reason,
            )
        )
    return events


def _single_candle_focus_events(candles: list[Candle], *, timeframe: str) -> list[PatternFocusEvent]:
    events: list[PatternFocusEvent] = []
    for report in analyze_single_candle_pattern_history(candles, inst_id="BTC-USDT-SWAP"):
        if not report.matches or report.candle_ts is None:
            continue
        primary = report.matches[0]
        if primary.pattern not in FOCUS_SINGLE_CANDLE_PATTERNS:
            continue
        direction = _signal_bias_from_pattern(primary.bias)
        events.append(
            PatternFocusEvent(
                timeframe=timeframe,
                ts=report.candle_ts,
                pattern_id=primary.pattern,
                label=_pattern_label(primary.pattern, primary.pattern),
                direction=direction,
                score=2,
                candle_count=1,
                source="single",
                summary=_single_pattern_summary(primary.pattern, direction),
                reason=primary.reason,
            )
        )
    return events


def _recent_focus_events(item: TimeframeAnalysis, *, last_sent_at: datetime | None) -> list[PatternFocusEvent]:
    if last_sent_at is None:
        return list(item.focus_events[:4])
    threshold_ms = int(last_sent_at.astimezone(timezone.utc).timestamp() * 1000)
    events = [event for event in item.focus_events if event.ts > threshold_ms]
    return events[:4]


def _find_timeframe_analysis(analysis: BtcMarketAnalysis, timeframe: str) -> TimeframeAnalysis | None:
    for item in analysis.timeframes:
        if item.timeframe == timeframe:
            return item
    return None


def _top_reason_text(reasons: tuple[str, ...]) -> str:
    return reasons[0] if reasons else "暂无明显主导信号"


def _direction_text(direction: str) -> str:
    return {
        "long": "偏多",
        "short": "偏空",
        "neutral": "震荡偏中性",
    }.get(str(direction), str(direction))


def _signal_direction_text(direction: str) -> SignalBias:
    if direction == "long":
        return "long"
    if direction == "short":
        return "short"
    return "neutral"


def _timeframe_display_label(timeframe: str) -> str:
    return {"4H": "4小时", "1H": "1小时", "1D": "日线"}.get(timeframe, timeframe)


def _pattern_label(pattern_id: str, fallback: str) -> str:
    return {
        "false_breakdown": "双线反转看多",
        "false_breakout": "双线反转看空",
        "top_fractal": "顶分型",
        "bottom_fractal": "底分型",
        "inside_bar": "孕线",
        "big_bullish": "大阳线",
        "big_bearish": "大阴线",
        "long_upper_shadow": "长上影",
        "long_lower_shadow": "长下影",
        "hammer": "锤子线",
        "hanging_man": "上吊线",
        "inverted_hammer": "倒锤子",
        "shooting_star": "流星线",
        "bullish_marubozu": "光头阳线",
        "bearish_marubozu": "光头阴线",
        "dragonfly_doji": "蜻蜓十字",
        "gravestone_doji": "墓碑十字",
    }.get(pattern_id, fallback)


def _pattern_summary(pattern_id: str, direction: str) -> str:
    return {
        "false_breakdown": "先跌破又收回，短线更像止跌反抽。",
        "false_breakout": "先冲高又收回，短线更像冲高回落。",
        "top_fractal": "三根K线做出局部高点，短线要防回落。",
        "bottom_fractal": "三根K线做出局部低点，短线先看反弹。",
        "inside_bar": "波动先收住了，后面容易选方向。",
        "big_bullish": "单根阳线很强，买盘发力明显。",
        "big_bearish": "单根阴线很强，卖盘发力明显。",
        "long_upper_shadow": "上去后被压回来了，上方抛压明显。",
        "long_lower_shadow": "下去后被拉回来了，下方承接明显。",
    }.get(pattern_id, f"出现了偏{_direction_text(direction)}的代表性K线。")


def _single_pattern_summary(pattern_id: str, direction: SignalBias) -> str:
    return {
        "hammer": "下探后被拉回，像是在试底。",
        "hanging_man": "冲高后留下下影，强势里开始有分歧。",
        "inverted_hammer": "下跌后向上试探，像是在试反转。",
        "shooting_star": "冲高后回落，短线偏弱。",
        "bullish_marubozu": "整根阳线很干净，买盘占优。",
        "bearish_marubozu": "整根阴线很干净，卖盘占优。",
        "dragonfly_doji": "下影很长，低位有承接。",
        "gravestone_doji": "上影很长，高位有压制。",
    }.get(pattern_id, f"出现了偏{_direction_text(direction)}的单K信号。")


def _format_local_timestamp(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone().strftime("%m-%d %H:%M")


def _format_local_datetime(value: datetime) -> str:
    return value.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def _parse_iso_datetime(raw_value: str) -> datetime | None:
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


def _save_email_delivery_state(*, subject: str, report_path: Path | None) -> None:
    save_btc_market_email_state(
        last_sent_at=datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        last_subject=subject,
        last_report_path=str(report_path) if report_path is not None else "",
    )


def _analyze_timeframe(
    *,
    candles: list[Candle],
    symbol: str,
    timeframe: str,
    config: BtcMarketAnalyzerConfig,
) -> TimeframeAnalysis:
    ordered = sorted(candles, key=lambda item: item.ts)
    if not ordered:
        return TimeframeAnalysis(
            symbol=symbol,
            timeframe=timeframe,
            candle_ts=None,
            last_close=None,
            direction="neutral",
            score=0,
            confidence=ZERO,
            trend_context="unknown",
            signals=(),
            reason=("未获取到K线数据。",),
            probability={},
            indicators={},
            pattern={},
            focus_events=(),
        )

    indicator_snapshot, indicator_signals, indicator_trend_context = _build_indicator_snapshot(
        ordered,
        timeframe=timeframe,
        config=config,
    )
    probability_report = build_market_analysis_report(
        ordered,
        inst_id=symbol,
        timeframe=timeframe,
        config=config.probability_config,
    )
    probability_snapshot, probability_signals = _build_probability_snapshot(
        probability_report,
        timeframe=timeframe,
    )
    pattern_report = analyze_single_candle_patterns(ordered, inst_id=symbol)
    pattern_snapshot, pattern_signals = _build_pattern_snapshot(pattern_report, timeframe=timeframe)
    focus_events = _build_focus_events(ordered, timeframe=timeframe)

    signals = _sorted_signals(indicator_signals + probability_signals + pattern_signals)
    score = sum(item.score for item in signals)
    direction = _direction_from_score(score, config.timeframe_direction_threshold)
    confidence = _confidence_from_score(score, config.timeframe_confidence_divisor)
    trend_context = _resolve_trend_context(indicator_trend_context, pattern_report.trend_context)
    reason = tuple(_top_reasons(signals))

    return TimeframeAnalysis(
        symbol=symbol,
        timeframe=timeframe,
        candle_ts=ordered[-1].ts,
        last_close=ordered[-1].close,
        direction=direction,
        score=score,
        confidence=confidence,
        trend_context=trend_context,
        signals=tuple(signals),
        reason=reason,
        probability=probability_snapshot,
        indicators=indicator_snapshot,
        pattern=pattern_snapshot,
        focus_events=focus_events,
    )


def _build_indicator_snapshot(
    candles: list[Candle],
    *,
    timeframe: str,
    config: BtcMarketAnalyzerConfig,
) -> tuple[dict[str, object], list[MarketSignal], str]:
    closes = [item.close for item in candles]
    fast_period, middle_period, slow_period = config.ema_periods
    ema_fast = ema(closes, fast_period)
    ema_middle = ema(closes, middle_period)
    ema_slow = ema(closes, slow_period)
    macd_line, macd_signal_line, macd_histogram = macd(
        closes,
        fast_period=config.macd_fast_period,
        slow_period=config.macd_slow_period,
        signal_period=config.macd_signal_period,
    )
    boll_middle, boll_upper, boll_lower = bollinger_bands(
        closes,
        period=config.boll_period,
        std_dev_multiplier=config.boll_std_multiplier,
    )

    close_price = closes[-1]
    fast_value = ema_fast[-1]
    middle_value = ema_middle[-1]
    slow_value = ema_slow[-1]
    fast_slope = _latest_delta(ema_fast)
    middle_slope = _latest_delta(ema_middle)
    slow_slope = _latest_delta(ema_slow)
    trend_context = "sideways"
    signals: list[MarketSignal] = []

    ema_state = "mixed"
    if close_price > fast_value > middle_value > slow_value and fast_slope > ZERO and middle_slope > ZERO:
        ema_state = "bullish_alignment"
        trend_context = "uptrend"
        signals.append(
            MarketSignal(
                name="ema_bullish_alignment",
                category="indicator",
                timeframe=timeframe,
                bias="long",
                score=2,
                strength=Decimal("0.86"),
                trend_context=trend_context,
                reason="EMA21/55/233 多头排列，且短中期均线斜率向上。",
            )
        )
    elif close_price < fast_value < middle_value < slow_value and fast_slope < ZERO and middle_slope < ZERO:
        ema_state = "bearish_alignment"
        trend_context = "downtrend"
        signals.append(
            MarketSignal(
                name="ema_bearish_alignment",
                category="indicator",
                timeframe=timeframe,
                bias="short",
                score=-2,
                strength=Decimal("0.86"),
                trend_context=trend_context,
                reason="EMA21/55/233 空头排列，且短中期均线斜率向下。",
            )
        )
    elif close_price > fast_value > middle_value:
        ema_state = "early_bullish_stack"
        trend_context = "uptrend"
        signals.append(
            MarketSignal(
                name="ema_structure_support",
                category="indicator",
                timeframe=timeframe,
                bias="long",
                score=1,
                strength=Decimal("0.58"),
                trend_context=trend_context,
                reason="价格位于 EMA21 与 EMA55 之上，趋势结构偏多。",
            )
        )
    elif close_price < fast_value < middle_value:
        ema_state = "early_bearish_stack"
        trend_context = "downtrend"
        signals.append(
            MarketSignal(
                name="ema_structure_pressure",
                category="indicator",
                timeframe=timeframe,
                bias="short",
                score=-1,
                strength=Decimal("0.58"),
                trend_context=trend_context,
                reason="价格位于 EMA21 与 EMA55 之下，趋势结构偏空。",
            )
        )

    macd_state = "neutral"
    if len(macd_line) >= 2 and len(macd_signal_line) >= 2:
        previous_line = macd_line[-2]
        previous_signal = macd_signal_line[-2]
        current_line = macd_line[-1]
        current_signal = macd_signal_line[-1]
        current_hist = macd_histogram[-1]
        if current_line > current_signal and previous_line <= previous_signal:
            macd_state = "bullish_cross"
            signals.append(
                MarketSignal(
                    name="macd_bullish_cross",
                    category="indicator",
                    timeframe=timeframe,
                    bias="long",
                    score=2,
                    strength=Decimal("0.80"),
                    trend_context=trend_context,
                    reason="MACD 最新一根出现上穿信号线的金叉。",
                )
            )
        elif current_line < current_signal and previous_line >= previous_signal:
            macd_state = "bearish_cross"
            signals.append(
                MarketSignal(
                    name="macd_bearish_cross",
                    category="indicator",
                    timeframe=timeframe,
                    bias="short",
                    score=-2,
                    strength=Decimal("0.80"),
                    trend_context=trend_context,
                    reason="MACD 最新一根出现下穿信号线的死叉。",
                )
            )
        elif current_hist > ZERO and current_line > current_signal:
            macd_state = "bullish_zone"
            signals.append(
                MarketSignal(
                    name="macd_positive_zone",
                    category="indicator",
                    timeframe=timeframe,
                    bias="long",
                    score=1,
                    strength=Decimal("0.55"),
                    trend_context=trend_context,
                    reason="MACD 位于多头区域，动能仍偏向上行。",
                )
            )
        elif current_hist < ZERO and current_line < current_signal:
            macd_state = "bearish_zone"
            signals.append(
                MarketSignal(
                    name="macd_negative_zone",
                    category="indicator",
                    timeframe=timeframe,
                    bias="short",
                    score=-1,
                    strength=Decimal("0.55"),
                    trend_context=trend_context,
                    reason="MACD 位于空头区域，动能仍偏向下行。",
                )
            )

    boll_state = "neutral"
    percent_b = _percent_b(close_price, boll_upper[-1], boll_lower[-1])
    bandwidth = _bandwidth(boll_middle[-1], boll_upper[-1], boll_lower[-1])
    if boll_upper[-1] is not None and close_price > boll_upper[-1]:
        boll_state = "upper_breakout"
        signals.append(
            MarketSignal(
                name="boll_upper_breakout",
                category="indicator",
                timeframe=timeframe,
                bias="long",
                score=1,
                strength=Decimal("0.64"),
                trend_context=trend_context,
                reason="价格突破布林上轨，短线强势延续概率上升。",
            )
        )
    elif boll_lower[-1] is not None and close_price < boll_lower[-1]:
        boll_state = "lower_breakdown"
        signals.append(
            MarketSignal(
                name="boll_lower_breakdown",
                category="indicator",
                timeframe=timeframe,
                bias="short",
                score=-1,
                strength=Decimal("0.64"),
                trend_context=trend_context,
                reason="价格跌破布林下轨，短线转弱迹象更明显。",
            )
        )
    elif bandwidth is not None and bandwidth <= Decimal("0.08"):
        boll_state = "squeeze"

    snapshot = {
        "close": _decimal_text(close_price),
        "ema": {
            "periods": list(config.ema_periods),
            "values": {
                str(fast_period): _decimal_text(fast_value),
                str(middle_period): _decimal_text(middle_value),
                str(slow_period): _decimal_text(slow_value),
            },
            "slopes": {
                str(fast_period): _decimal_text(fast_slope),
                str(middle_period): _decimal_text(middle_slope),
                str(slow_period): _decimal_text(slow_slope),
            },
            "state": ema_state,
        },
        "macd": {
            "line": _decimal_text(macd_line[-1]),
            "signal": _decimal_text(macd_signal_line[-1]),
            "histogram": _decimal_text(macd_histogram[-1]),
            "state": macd_state,
        },
        "boll": {
            "middle": _decimal_text(boll_middle[-1]),
            "upper": _decimal_text(boll_upper[-1]),
            "lower": _decimal_text(boll_lower[-1]),
            "percent_b": _decimal_text(percent_b),
            "bandwidth": _decimal_text(bandwidth),
            "state": boll_state,
        },
    }
    return snapshot, signals, trend_context


def _build_probability_snapshot(
    report: MarketAnalysisReport,
    *,
    timeframe: str,
) -> tuple[dict[str, object], list[MarketSignal]]:
    active_factors = []
    signals: list[MarketSignal] = []
    for item in report.active_factors:
        active_factors.append(
            {
                "key": item.key,
                "label": item.label,
                "direction_bias": item.direction_bias,
                "score": _decimal_text(item.score),
                "reason": item.reason,
            }
        )
        mapped_score = _score_from_factor(item.direction_bias, item.score)
        signals.append(
            MarketSignal(
                name=item.key,
                category="probability",
                timeframe=timeframe,
                bias=_signal_bias_from_factor(item.direction_bias),
                score=mapped_score,
                strength=item.score,
                trend_context="unknown",
                reason=item.reason,
            )
        )

    snapshot = {
        "direction_mode": report.direction_mode,
        "baseline_bullish_probability": _decimal_text(report.baseline_bullish_probability),
        "current_bullish_streak": report.snapshot.current_bullish_streak,
        "last_completed_bullish_streak": report.snapshot.last_completed_bullish_streak,
        "latest_pullback_bucket": report.snapshot.latest_pullback_bucket,
        "latest_support_break": report.snapshot.latest_support_break,
        "latest_volatility_regime": report.snapshot.latest_volatility_regime,
        "active_factors": active_factors,
    }
    return snapshot, signals


def _build_pattern_snapshot(
    report: SingleCandlePatternReport,
    *,
    timeframe: str,
) -> tuple[dict[str, object], list[MarketSignal]]:
    payload = single_candle_report_payload(report)
    signals: list[MarketSignal] = []
    primary_match = report.matches[0] if report.matches else None
    if primary_match is not None:
        score = _score_from_pattern(primary_match.pattern, primary_match.bias)
        if score != 0:
            signals.append(
                MarketSignal(
                    name=primary_match.pattern,
                    category="pattern",
                    timeframe=timeframe,
                    bias=_signal_bias_from_pattern(primary_match.bias),
                    score=score,
                    strength=Decimal("0.68") if abs(score) > 1 else Decimal("0.52"),
                    trend_context=report.trend_context,
                    reason=f"K线形态出现 {primary_match.pattern}，结合当前趋势语境偏向{_pattern_bias_label(primary_match.bias)}。",
                )
            )
    return payload, signals


def _build_resonance(
    timeframes: tuple[TimeframeAnalysis, ...],
    config: BtcMarketAnalyzerConfig,
) -> ResonanceAnalysis:
    long_timeframes = [item.timeframe for item in timeframes if item.direction == "long"]
    short_timeframes = [item.timeframe for item in timeframes if item.direction == "short"]

    if len(long_timeframes) == len(timeframes) and long_timeframes:
        return ResonanceAnalysis(
            direction="long",
            aligned_timeframes=tuple(long_timeframes),
            score=config.resonance_full_alignment_score,
            confidence=Decimal("0.95"),
            summary="1H / 4H / 1D 全部偏多，共振最强。",
        )
    if len(short_timeframes) == len(timeframes) and short_timeframes:
        return ResonanceAnalysis(
            direction="short",
            aligned_timeframes=tuple(short_timeframes),
            score=-config.resonance_full_alignment_score,
            confidence=Decimal("0.95"),
            summary="1H / 4H / 1D 全部偏空，共振最强。",
        )
    if len(long_timeframes) >= 2:
        return ResonanceAnalysis(
            direction="long",
            aligned_timeframes=tuple(long_timeframes),
            score=config.resonance_partial_alignment_score,
            confidence=Decimal("0.66"),
            summary="至少两个周期偏多，存在中等强度的多头共振。",
        )
    if len(short_timeframes) >= 2:
        return ResonanceAnalysis(
            direction="short",
            aligned_timeframes=tuple(short_timeframes),
            score=-config.resonance_partial_alignment_score,
            confidence=Decimal("0.66"),
            summary="至少两个周期偏空，存在中等强度的空头共振。",
        )
    return ResonanceAnalysis(
        direction="neutral",
        aligned_timeframes=(),
        score=0,
        confidence=Decimal("0.33"),
        summary="多周期方向分歧较大，共振不足。",
    )


def _aggregate_direction(
    timeframes: tuple[TimeframeAnalysis, ...],
    aggregate_score: int,
    resonance: ResonanceAnalysis,
    config: BtcMarketAnalyzerConfig,
) -> AnalysisDirection:
    long_count = sum(1 for item in timeframes if item.direction == "long")
    short_count = sum(1 for item in timeframes if item.direction == "short")
    if long_count > short_count and aggregate_score >= config.aggregate_direction_threshold:
        return "long"
    if short_count > long_count and aggregate_score <= -config.aggregate_direction_threshold:
        return "short"
    if resonance.direction == "long" and aggregate_score > 0:
        return "long"
    if resonance.direction == "short" and aggregate_score < 0:
        return "short"
    return _direction_from_score(aggregate_score, config.aggregate_direction_threshold)


def _resonance_signals(resonance: ResonanceAnalysis) -> list[MarketSignal]:
    if resonance.score == 0:
        return []
    return [
        MarketSignal(
            name="multi_timeframe_resonance",
            category="resonance",
            timeframe=None,
            bias="long" if resonance.direction == "long" else "short",
            score=resonance.score,
            strength=resonance.confidence,
            trend_context="multi_timeframe",
            reason=resonance.summary,
        )
    ]


def _history_limit_for_timeframe(config: BtcMarketAnalyzerConfig, timeframe: str) -> int:
    normalized = timeframe.strip().upper()
    for key, value in config.history_limits:
        if key.strip().upper() == normalized:
            return value
    return config.default_history_limit


def _indicator_preload_count(config: BtcMarketAnalyzerConfig) -> int:
    indicator_max = max(
        max(config.ema_periods),
        config.macd_slow_period + config.macd_signal_period,
        config.boll_period,
    )
    return max(indicator_max + 20, 80)


def _timeframe_ms(timeframe: str) -> int:
    normalized = timeframe.strip().upper()
    if normalized.endswith("H"):
        return int(normalized[:-1]) * 3_600_000
    if normalized.endswith("D"):
        return int(normalized[:-1]) * 86_400_000
    if normalized.endswith("M"):
        return int(normalized[:-1]) * 60_000
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def _direction_from_score(score: int, threshold: int) -> AnalysisDirection:
    if score >= threshold:
        return "long"
    if score <= -threshold:
        return "short"
    return "neutral"


def _confidence_from_score(score: int, divisor: Decimal) -> Decimal:
    if divisor <= 0:
        return ZERO
    confidence = Decimal(abs(score)) / divisor
    if confidence > ONE:
        return ONE
    return confidence


def _resolve_trend_context(indicator_context: str, pattern_context: str) -> str:
    if indicator_context in {"uptrend", "downtrend"}:
        return indicator_context
    if pattern_context in {"uptrend", "downtrend", "sideways"}:
        return pattern_context
    return "sideways"


def _sorted_signals(signals: list[MarketSignal]) -> list[MarketSignal]:
    return sorted(
        signals,
        key=lambda item: (abs(item.score), item.strength, item.name),
        reverse=True,
    )


def _top_reasons(signals: list[MarketSignal], limit: int = 5) -> list[str]:
    reasons: list[str] = []
    for item in signals:
        if item.score == 0 or item.reason in reasons:
            continue
        reasons.append(item.reason)
        if len(reasons) >= limit:
            break
    if not reasons:
        return ["当前没有足够强的共识信号，方向保持中性。"]
    return reasons


def _latest_delta(values: list[Decimal]) -> Decimal:
    if len(values) < 2:
        return ZERO
    return values[-1] - values[-2]


def _percent_b(
    close_price: Decimal,
    upper: Decimal | None,
    lower: Decimal | None,
) -> Decimal | None:
    if upper is None or lower is None or upper <= lower:
        return None
    return (close_price - lower) / (upper - lower)


def _bandwidth(
    middle: Decimal | None,
    upper: Decimal | None,
    lower: Decimal | None,
) -> Decimal | None:
    if middle is None or upper is None or lower is None or middle == ZERO:
        return None
    return (upper - lower) / middle


def _score_from_factor(direction_bias: str, strength: Decimal) -> int:
    base = 2 if strength >= Decimal("0.75") else 1
    if direction_bias == "bullish":
        return base
    if direction_bias == "bearish":
        return -base
    if direction_bias == "caution":
        return -1
    return 0


def _signal_bias_from_factor(direction_bias: str) -> SignalBias:
    if direction_bias == "bullish":
        return "long"
    if direction_bias == "bearish":
        return "short"
    if direction_bias == "caution":
        return "caution"
    return "neutral"


def _score_from_pattern(pattern: str, bias: PatternBias) -> int:
    if bias == "neutral":
        return 0
    strong_patterns = {
        "hammer",
        "hanging_man",
        "inverted_hammer",
        "shooting_star",
        "bullish_marubozu",
        "bearish_marubozu",
    }
    score = 2 if pattern in strong_patterns else 1
    if bias in {"bearish_reversal", "bearish_continuation"}:
        return -score
    return score


def _signal_bias_from_pattern(bias: PatternBias) -> SignalBias:
    if bias in {"bullish_reversal", "bullish_continuation"}:
        return "long"
    if bias in {"bearish_reversal", "bearish_continuation"}:
        return "short"
    return "neutral"


def _pattern_bias_label(bias: PatternBias) -> str:
    return {
        "bullish_reversal": "看多反转",
        "bearish_reversal": "看空反转",
        "bullish_continuation": "看多延续",
        "bearish_continuation": "看空延续",
        "neutral": "中性",
    }.get(bias, bias)


def _signal_payload(signal: MarketSignal) -> dict[str, object]:
    return {
        "timeframe": signal.timeframe,
        "name": signal.name,
        "category": signal.category,
        "bias": signal.bias,
        "score": signal.score,
        "strength": _decimal_text(signal.strength),
        "trend_context": signal.trend_context,
        "reason": signal.reason,
    }


def _focus_event_payload(event: PatternFocusEvent) -> dict[str, object]:
    return {
        "timeframe": event.timeframe,
        "ts": event.ts,
        "pattern_id": event.pattern_id,
        "label": event.label,
        "direction": event.direction,
        "score": event.score,
        "candle_count": event.candle_count,
        "source": event.source,
        "summary": event.summary,
        "reason": event.reason,
    }


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _format_pct(value: Decimal) -> str:
    return f"{value * Decimal('100'):.2f}%"


def _safe_file_component(value: str) -> str:
    compact = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return compact or "report"
