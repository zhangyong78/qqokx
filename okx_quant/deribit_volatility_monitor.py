from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Callable, Literal

from okx_quant.deribit_client import DeribitRestClient, DeribitVolatilityCandle
from okx_quant.indicators import ema
from okx_quant.pricing import format_decimal_fixed


Logger = Callable[[str], None]
VolatilitySignalType = Literal[
    "bearish_reversal_after_rally",
    "bullish_reversal_after_drop",
    "squeeze_breakout_up",
    "squeeze_breakout_down",
    "ema34_turn_up",
    "ema34_turn_down",
]
VolatilityDirection = Literal["up", "down"]
EventCallback = Callable[["VolatilitySignalEvent"], None]
DiagnosticCallback = Callable[["VolatilityMonitorRoundDiagnostic"], None]

VOL_SIGNAL_LABELS: dict[VolatilitySignalType, str] = {
    "bearish_reversal_after_rally": "连涨后大阴反转",
    "bullish_reversal_after_drop": "连跌后大阳反转",
    "squeeze_breakout_up": "窄幅后大阳突破",
    "squeeze_breakout_down": "窄幅后大阴突破",
    "ema34_turn_up": "EMA34转强",
    "ema34_turn_down": "EMA34转弱",
}

VOL_DIRECTION_LABELS: dict[VolatilityDirection, str] = {
    "up": "上行",
    "down": "下行",
}

DERIBIT_VOL_RESOLUTION_SECONDS: dict[str, int] = {
    "3600": 3_600,
    "43200": 43_200,
    "1D": 86_400,
}


@dataclass(frozen=True)
class VolatilityMonitorConfig:
    currencies: tuple[str, ...]
    resolution: str = "3600"
    buffer_seconds: float = 10.0
    enable_bearish_reversal_after_rally: bool = True
    enable_bullish_reversal_after_drop: bool = True
    enable_squeeze_breakout_up: bool = True
    enable_squeeze_breakout_down: bool = True
    enable_ema34_turn_up: bool = True
    enable_ema34_turn_down: bool = True
    ema_period: int = 34
    trend_streak_bars: int = 4
    squeeze_bars: int = 6
    lookback_candles: int = 180
    cumulative_change_threshold: Decimal = Decimal("0.06")
    reversal_body_multiplier: Decimal = Decimal("1.8")
    breakout_body_multiplier: Decimal = Decimal("2.0")
    squeeze_range_ratio: Decimal = Decimal("0.65")


@dataclass(frozen=True)
class VolatilitySignalEvent:
    currency: str
    signal_type: VolatilitySignalType
    direction: VolatilityDirection
    candle_ts: int
    trigger_value: Decimal
    reason: str
    decimal_places: int

    @property
    def symbol(self) -> str:
        return f"{self.currency} DVOL"


@dataclass(frozen=True)
class VolatilityMonitorSymbolDiagnostic:
    currency: str
    candle_ts: int | None
    matched_events: tuple[VolatilitySignalEvent, ...] = ()
    filtered_events: tuple[VolatilitySignalEvent, ...] = ()
    new_events: tuple[VolatilitySignalEvent, ...] = ()
    duplicate_events: tuple[VolatilitySignalEvent, ...] = ()
    note: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class VolatilityMonitorRoundDiagnostic:
    resolution: str
    checked_at: int
    reports: tuple[VolatilityMonitorSymbolDiagnostic, ...]


class DeribitVolatilityMonitor:
    def __init__(
        self,
        client: DeribitRestClient,
        logger: Logger,
        *,
        event_callback: EventCallback | None = None,
        diagnostic_callback: DiagnosticCallback | None = None,
        email_sender: Callable[[VolatilitySignalEvent, str], None] | None = None,
        monitor_name: str = "波动率监控",
    ) -> None:
        self._client = client
        self._logger = logger
        self._event_callback = event_callback
        self._diagnostic_callback = diagnostic_callback
        self._email_sender = email_sender
        self._monitor_name = monitor_name
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._thread is not None and self._thread.is_alive()

    def start(self, config: VolatilityMonitorConfig) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                raise RuntimeError("波动率监控已经在运行中")
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run,
                args=(config,),
                daemon=True,
                name="qqokx-deribit-vol-monitor",
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    def _run(self, config: VolatilityMonitorConfig) -> None:
        last_confirmed_ts: dict[str, int | None] = {}
        last_emitted: dict[tuple[str, VolatilitySignalType, VolatilityDirection], int] = {}
        lookback = max(config.lookback_candles, config.ema_period * 3, config.squeeze_bars + 30, 80)

        self._logger(
            f"启动波动率监控 | 币种={', '.join(config.currencies)} | 周期={config.resolution} | 收线缓冲={config.buffer_seconds}s"
        )

        baseline_reports = self._build_baseline_reports(config, lookback, last_confirmed_ts)
        if baseline_reports:
            self._emit_diagnostic(config.resolution, baseline_reports)

        while not self._stop_event.is_set():
            wait_seconds = _seconds_until_next_check(config.resolution, config.buffer_seconds)
            if self._stop_event.wait(wait_seconds):
                break

            round_reports: list[VolatilityMonitorSymbolDiagnostic] = []
            for currency in config.currencies:
                if self._stop_event.is_set():
                    break
                try:
                    candles = self._fetch_candles(currency, config.resolution, lookback)
                    if len(candles) < max(config.ema_period + 3, config.trend_streak_bars + 3, config.squeeze_bars + 25):
                        round_reports.append(
                            VolatilityMonitorSymbolDiagnostic(
                                currency=currency,
                                candle_ts=candles[-1].ts if candles else None,
                                note="历史波动率K线数量不足，暂不检测",
                            )
                        )
                        continue

                    latest_ts = candles[-1].ts
                    previous_ts = last_confirmed_ts.get(currency)
                    if previous_ts is not None and latest_ts <= previous_ts:
                        round_reports.append(
                            VolatilityMonitorSymbolDiagnostic(
                                currency=currency,
                                candle_ts=latest_ts,
                                note="新波动率K线未确认，继续等待",
                            )
                        )
                        continue

                    report = evaluate_volatility_signal_report(candles, currency, config)
                    last_confirmed_ts[currency] = latest_ts
                    new_events: list[VolatilitySignalEvent] = []
                    duplicate_events: list[VolatilitySignalEvent] = []
                    for event in report.matched_events:
                        key = (event.currency, event.signal_type, event.direction)
                        if last_emitted.get(key) == event.candle_ts:
                            duplicate_events.append(event)
                            continue
                        last_emitted[key] = event.candle_ts
                        new_events.append(event)
                        self._emit_event(event, config.resolution)

                    round_reports.append(
                        VolatilityMonitorSymbolDiagnostic(
                            currency=report.currency,
                            candle_ts=report.candle_ts,
                            matched_events=report.matched_events,
                            filtered_events=report.filtered_events,
                            new_events=tuple(new_events),
                            duplicate_events=tuple(duplicate_events),
                        )
                    )
                except Exception as exc:
                    self._logger(f"{currency} DVOL 监控失败：{exc}")
                    round_reports.append(
                        VolatilityMonitorSymbolDiagnostic(
                            currency=currency,
                            candle_ts=None,
                            error=str(exc),
                        )
                    )

            if round_reports:
                self._emit_diagnostic(config.resolution, round_reports)

        self._logger("波动率监控已停止。")

    def _fetch_candles(self, currency: str, resolution: str, limit: int) -> list[DeribitVolatilityCandle]:
        seconds = DERIBIT_VOL_RESOLUTION_SECONDS[resolution]
        end_ts = int(time.time() * 1000)
        start_ts = end_ts - (seconds * (limit + 5) * 1000)
        return self._client.get_volatility_index_candles(
            currency,
            resolution,
            start_ts=start_ts,
            end_ts=end_ts,
            max_records=limit,
        )

    def _build_baseline_reports(
        self,
        config: VolatilityMonitorConfig,
        lookback: int,
        last_confirmed_ts: dict[str, int | None],
    ) -> list[VolatilityMonitorSymbolDiagnostic]:
        reports: list[VolatilityMonitorSymbolDiagnostic] = []
        for currency in config.currencies:
            try:
                candles = self._fetch_candles(currency, config.resolution, lookback)
                latest_ts = candles[-1].ts if candles else None
                last_confirmed_ts[currency] = latest_ts
                reports.append(
                    VolatilityMonitorSymbolDiagnostic(
                        currency=currency,
                        candle_ts=latest_ts,
                        note="已建立基线，等待下一根新波动率K线",
                    )
                )
            except Exception as exc:
                last_confirmed_ts[currency] = None
                reports.append(
                    VolatilityMonitorSymbolDiagnostic(
                        currency=currency,
                        candle_ts=None,
                        error=str(exc),
                    )
                )
        return reports

    def _emit_diagnostic(self, resolution: str, reports: list[VolatilityMonitorSymbolDiagnostic]) -> None:
        if self._diagnostic_callback is None:
            return
        self._diagnostic_callback(
            VolatilityMonitorRoundDiagnostic(
                resolution=resolution,
                checked_at=int(time.time() * 1000),
                reports=tuple(reports),
            )
        )

    def _emit_event(self, event: VolatilitySignalEvent, resolution: str) -> None:
        text = (
            f"{_fmt_ts(event.candle_ts)} | {event.symbol} | {VOL_SIGNAL_LABELS[event.signal_type]} | "
            f"{VOL_DIRECTION_LABELS[event.direction]} | 收盘={_fmt_value(event.trigger_value, event.decimal_places)} | {event.reason}"
        )
        self._logger(text)
        if self._event_callback is not None:
            self._event_callback(event)
        if self._email_sender is not None:
            self._email_sender(event, resolution)


def evaluate_volatility_signal_report(
    candles: list[DeribitVolatilityCandle],
    currency: str,
    config: VolatilityMonitorConfig,
) -> VolatilityMonitorSymbolDiagnostic:
    matched_events: list[VolatilitySignalEvent] = []
    filtered_events: list[VolatilitySignalEvent] = []
    for event in _detect_all_volatility_signals(candles, currency, config):
        if _is_signal_enabled(config, event.signal_type):
            matched_events.append(event)
        else:
            filtered_events.append(event)
    return VolatilityMonitorSymbolDiagnostic(
        currency=currency,
        candle_ts=candles[-1].ts if candles else None,
        matched_events=tuple(matched_events),
        filtered_events=tuple(filtered_events),
    )


def _detect_all_volatility_signals(
    candles: list[DeribitVolatilityCandle],
    currency: str,
    config: VolatilityMonitorConfig,
) -> list[VolatilitySignalEvent]:
    events: list[VolatilitySignalEvent] = []

    for detector in (
        detect_bearish_reversal_after_rally,
        detect_bullish_reversal_after_drop,
        detect_squeeze_breakout_up,
        detect_squeeze_breakout_down,
        detect_ema34_turn_up,
        detect_ema34_turn_down,
    ):
        event = detector(currency, candles, config)
        if event is not None:
            events.append(event)
    return events


def _is_signal_enabled(config: VolatilityMonitorConfig, signal_type: VolatilitySignalType) -> bool:
    return {
        "bearish_reversal_after_rally": config.enable_bearish_reversal_after_rally,
        "bullish_reversal_after_drop": config.enable_bullish_reversal_after_drop,
        "squeeze_breakout_up": config.enable_squeeze_breakout_up,
        "squeeze_breakout_down": config.enable_squeeze_breakout_down,
        "ema34_turn_up": config.enable_ema34_turn_up,
        "ema34_turn_down": config.enable_ema34_turn_down,
    }[signal_type]


def detect_bearish_reversal_after_rally(
    currency: str,
    candles: list[DeribitVolatilityCandle],
    config: VolatilityMonitorConfig,
) -> VolatilitySignalEvent | None:
    streak = config.trend_streak_bars
    if len(candles) < streak + 3:
        return None
    previous = candles[-(streak + 1) : -1]
    last = candles[-1]
    if not _closes_strictly_increasing(previous):
        return None
    cumulative_change = _pct_change(previous[-1].close, previous[0].close)
    if cumulative_change < config.cumulative_change_threshold:
        return None
    if last.close >= last.open:
        return None
    if last.close >= previous[-1].low:
        return None
    avg_body = _average_body(previous)
    current_body = _body(last)
    if avg_body <= 0 or current_body < avg_body * config.reversal_body_multiplier:
        return None
    places = _infer_decimal_places(candles)
    return VolatilitySignalEvent(
        currency=currency,
        signal_type="bearish_reversal_after_rally",
        direction="down",
        candle_ts=last.ts,
        trigger_value=last.close,
        decimal_places=places,
        reason=(
            f"前{streak}根收盘连续上涨，累计涨幅={_fmt_pct(cumulative_change)} | "
            f"当前大阴实体={_fmt_value(current_body, places)}，跌破前一根低点"
        ),
    )


def detect_bullish_reversal_after_drop(
    currency: str,
    candles: list[DeribitVolatilityCandle],
    config: VolatilityMonitorConfig,
) -> VolatilitySignalEvent | None:
    streak = config.trend_streak_bars
    if len(candles) < streak + 3:
        return None
    previous = candles[-(streak + 1) : -1]
    last = candles[-1]
    if not _closes_strictly_decreasing(previous):
        return None
    cumulative_change = _pct_change(previous[-1].close, previous[0].close)
    if cumulative_change > -config.cumulative_change_threshold:
        return None
    if last.close <= last.open:
        return None
    if last.close <= previous[-1].high:
        return None
    avg_body = _average_body(previous)
    current_body = _body(last)
    if avg_body <= 0 or current_body < avg_body * config.reversal_body_multiplier:
        return None
    places = _infer_decimal_places(candles)
    return VolatilitySignalEvent(
        currency=currency,
        signal_type="bullish_reversal_after_drop",
        direction="up",
        candle_ts=last.ts,
        trigger_value=last.close,
        decimal_places=places,
        reason=(
            f"前{streak}根收盘连续下跌，累计跌幅={_fmt_pct(cumulative_change)} | "
            f"当前大阳实体={_fmt_value(current_body, places)}，突破前一根高点"
        ),
    )


def detect_squeeze_breakout_up(
    currency: str,
    candles: list[DeribitVolatilityCandle],
    config: VolatilityMonitorConfig,
) -> VolatilitySignalEvent | None:
    squeeze_bars = config.squeeze_bars
    if len(candles) < squeeze_bars + 25:
        return None
    squeeze_window = candles[-(squeeze_bars + 1) : -1]
    context = candles[-(squeeze_bars + 21) : -(squeeze_bars + 1)]
    last = candles[-1]
    if last.close <= last.open:
        return None
    squeeze_range = _average_range(squeeze_window)
    context_range = _average_range(context)
    if context_range <= 0 or squeeze_range > context_range * config.squeeze_range_ratio:
        return None
    avg_body = _average_body(candles[-11:-1])
    current_body = _body(last)
    if avg_body <= 0 or current_body < avg_body * config.breakout_body_multiplier:
        return None
    if last.close <= max(item.high for item in squeeze_window):
        return None
    places = _infer_decimal_places(candles)
    return VolatilitySignalEvent(
        currency=currency,
        signal_type="squeeze_breakout_up",
        direction="up",
        candle_ts=last.ts,
        trigger_value=last.close,
        decimal_places=places,
        reason=(
            f"前{squeeze_bars}根窄幅波动，平均波幅压缩至过去20根的{_fmt_pct(squeeze_range / context_range)} | "
            f"当前大阳实体={_fmt_value(current_body, places)}，收盘突破区间高点"
        ),
    )


def detect_squeeze_breakout_down(
    currency: str,
    candles: list[DeribitVolatilityCandle],
    config: VolatilityMonitorConfig,
) -> VolatilitySignalEvent | None:
    squeeze_bars = config.squeeze_bars
    if len(candles) < squeeze_bars + 25:
        return None
    squeeze_window = candles[-(squeeze_bars + 1) : -1]
    context = candles[-(squeeze_bars + 21) : -(squeeze_bars + 1)]
    last = candles[-1]
    if last.close >= last.open:
        return None
    squeeze_range = _average_range(squeeze_window)
    context_range = _average_range(context)
    if context_range <= 0 or squeeze_range > context_range * config.squeeze_range_ratio:
        return None
    avg_body = _average_body(candles[-11:-1])
    current_body = _body(last)
    if avg_body <= 0 or current_body < avg_body * config.breakout_body_multiplier:
        return None
    if last.close >= min(item.low for item in squeeze_window):
        return None
    places = _infer_decimal_places(candles)
    return VolatilitySignalEvent(
        currency=currency,
        signal_type="squeeze_breakout_down",
        direction="down",
        candle_ts=last.ts,
        trigger_value=last.close,
        decimal_places=places,
        reason=(
            f"前{squeeze_bars}根窄幅波动，平均波幅压缩至过去20根的{_fmt_pct(squeeze_range / context_range)} | "
            f"当前大阴实体={_fmt_value(current_body, places)}，收盘跌破区间低点"
        ),
    )


def detect_ema34_turn_up(
    currency: str,
    candles: list[DeribitVolatilityCandle],
    config: VolatilityMonitorConfig,
) -> VolatilitySignalEvent | None:
    if len(candles) < config.ema_period + 3:
        return None
    closes = [item.close for item in candles]
    ema_values = ema(closes, config.ema_period)
    previous_slope = ema_values[-2] - ema_values[-3]
    current_slope = ema_values[-1] - ema_values[-2]
    last = candles[-1]
    if previous_slope <= 0 and current_slope > 0 and last.close > ema_values[-1]:
        places = _infer_decimal_places(candles)
        return VolatilitySignalEvent(
            currency=currency,
            signal_type="ema34_turn_up",
            direction="up",
            candle_ts=last.ts,
            trigger_value=last.close,
            decimal_places=places,
            reason=(
                f"EMA{config.ema_period}斜率转正 | 前斜率={_fmt_value(previous_slope, places)} | "
                f"当前斜率={_fmt_value(current_slope, places)}"
            ),
        )
    return None


def detect_ema34_turn_down(
    currency: str,
    candles: list[DeribitVolatilityCandle],
    config: VolatilityMonitorConfig,
) -> VolatilitySignalEvent | None:
    if len(candles) < config.ema_period + 3:
        return None
    closes = [item.close for item in candles]
    ema_values = ema(closes, config.ema_period)
    previous_slope = ema_values[-2] - ema_values[-3]
    current_slope = ema_values[-1] - ema_values[-2]
    last = candles[-1]
    if previous_slope >= 0 and current_slope < 0 and last.close < ema_values[-1]:
        places = _infer_decimal_places(candles)
        return VolatilitySignalEvent(
            currency=currency,
            signal_type="ema34_turn_down",
            direction="down",
            candle_ts=last.ts,
            trigger_value=last.close,
            decimal_places=places,
            reason=(
                f"EMA{config.ema_period}斜率转负 | 前斜率={_fmt_value(previous_slope, places)} | "
                f"当前斜率={_fmt_value(current_slope, places)}"
            ),
        )
    return None


def _closes_strictly_increasing(candles: list[DeribitVolatilityCandle]) -> bool:
    return all(candles[index].close > candles[index - 1].close for index in range(1, len(candles)))


def _closes_strictly_decreasing(candles: list[DeribitVolatilityCandle]) -> bool:
    return all(candles[index].close < candles[index - 1].close for index in range(1, len(candles)))


def _body(candle: DeribitVolatilityCandle) -> Decimal:
    return abs(candle.close - candle.open)


def _range(candle: DeribitVolatilityCandle) -> Decimal:
    return candle.high - candle.low


def _average_body(candles: list[DeribitVolatilityCandle]) -> Decimal:
    if not candles:
        return Decimal("0")
    return sum((_body(item) for item in candles), Decimal("0")) / Decimal(len(candles))


def _average_range(candles: list[DeribitVolatilityCandle]) -> Decimal:
    if not candles:
        return Decimal("0")
    return sum((_range(item) for item in candles), Decimal("0")) / Decimal(len(candles))


def _pct_change(current: Decimal, baseline: Decimal) -> Decimal:
    if baseline == 0:
        return Decimal("0")
    return (current - baseline) / baseline


def _infer_decimal_places(candles: list[DeribitVolatilityCandle]) -> int:
    max_places = 2
    for candle in candles[-20:]:
        for value in (candle.open, candle.high, candle.low, candle.close):
            max_places = max(max_places, max(-value.as_tuple().exponent, 0))
    return min(max_places, 4)


def _fmt_value(value: Decimal, places: int) -> str:
    return format_decimal_fixed(value, places)


def _fmt_pct(value: Decimal) -> str:
    return f"{format_decimal_fixed(value * Decimal('100'), 2)}%"


def _fmt_ts(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")


def _seconds_until_next_check(resolution: str, buffer_seconds: float) -> float:
    bucket_seconds = DERIBIT_VOL_RESOLUTION_SECONDS.get(resolution, 3_600)
    now = time.time()
    current_bucket = int(now // bucket_seconds)
    next_bucket_open = (current_bucket + 1) * bucket_seconds
    wait_seconds = next_bucket_open + max(buffer_seconds, 0.0) - now
    return max(wait_seconds, 0.5)


def format_volatility_diagnostic_round(session_id: str, diagnostic: VolatilityMonitorRoundDiagnostic) -> str:
    header = f"[{datetime.fromtimestamp(diagnostic.checked_at / 1000).strftime('%Y-%m-%d %H:%M:%S')}] [{session_id}] 周期={diagnostic.resolution}"
    lines = [header]
    for report in diagnostic.reports:
        prefix = f"{report.currency} DVOL"
        if report.error:
            lines.append(f"- {prefix}: 失败 | {report.error}")
            continue
        if report.note:
            lines.append(f"- {prefix}: {report.note}")
        if report.new_events:
            text = " / ".join(
                f"{VOL_SIGNAL_LABELS[event.signal_type]}({VOL_DIRECTION_LABELS[event.direction]})"
                for event in report.new_events
            )
            lines.append(f"  新触发: {text}")
        if report.filtered_events:
            text = " / ".join(
                f"{VOL_SIGNAL_LABELS[event.signal_type]}({VOL_DIRECTION_LABELS[event.direction]})"
                for event in report.filtered_events
            )
            lines.append(f"  已过滤: {text}")
        if report.duplicate_events:
            text = " / ".join(
                f"{VOL_SIGNAL_LABELS[event.signal_type]}({VOL_DIRECTION_LABELS[event.direction]})"
                for event in report.duplicate_events
            )
            lines.append(f"  重复抑制: {text}")
        if not report.note and not report.new_events and not report.filtered_events and not report.duplicate_events:
            lines.append(f"- {prefix}: 本轮无明显信号")
    return "\n".join(lines)
