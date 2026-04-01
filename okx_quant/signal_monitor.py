from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Callable, Literal

from okx_quant.indicators import ema
from okx_quant.models import Candle
from okx_quant.okx_client import OkxRestClient
from okx_quant.pricing import format_decimal_by_increment


Logger = Callable[[str], None]
SignalType = Literal["ema21_55_cross", "ema55_slope_turn", "ema55_breakout", "candle_pattern"]
SignalDirection = Literal["long", "short"]
EventCallback = Callable[["MonitorSignalEvent"], None]
DiagnosticCallback = Callable[["MonitorRoundDiagnostic"], None]
DEFAULT_MONITOR_SYMBOLS: tuple[tuple[str, str], ...] = (
    ("BTCUSDT", "BTC-USDT-SWAP"),
    ("ETHUSDT", "ETH-USDT-SWAP"),
    ("SOLUSDT", "SOL-USDT-SWAP"),
    ("BNBUSDT", "BNB-USDT-SWAP"),
)


@dataclass(frozen=True)
class SignalMonitorConfig:
    symbols: tuple[str, ...]
    bar: str = "4H"
    poll_seconds: float = 10.0
    enable_ema21_55_cross: bool = True
    enable_ema55_slope_turn: bool = True
    enable_ema55_breakout: bool = True
    enable_candle_pattern: bool = True
    pattern_ema_period: int = 55
    ema_near_tolerance: Decimal = Decimal("0.001")
    body_ratio_threshold: Decimal = Decimal("0.5")
    wick_ratio_threshold: Decimal = Decimal("0.6")


@dataclass(frozen=True)
class MonitorSignalEvent:
    symbol: str
    signal_type: SignalType
    direction: SignalDirection
    candle_ts: int
    trigger_price: Decimal
    reason: str
    tick_size: Decimal | None = None


@dataclass(frozen=True)
class MonitorSymbolDiagnostic:
    symbol: str
    candle_ts: int | None
    matched_events: tuple[MonitorSignalEvent, ...] = ()
    filtered_events: tuple[MonitorSignalEvent, ...] = ()
    new_events: tuple[MonitorSignalEvent, ...] = ()
    duplicate_events: tuple[MonitorSignalEvent, ...] = ()
    note: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class MonitorRoundDiagnostic:
    bar: str
    checked_at: int
    reports: tuple[MonitorSymbolDiagnostic, ...]


class SignalMonitor:
    def __init__(
        self,
        client: OkxRestClient,
        logger: Logger,
        *,
        event_callback: EventCallback | None = None,
        diagnostic_callback: DiagnosticCallback | None = None,
        email_sender: Callable[[MonitorSignalEvent, str], None] | None = None,
        monitor_name: str = "多币种信号监控",
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
        self._symbol_tick_sizes: dict[str, Decimal | None] = {}

    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._thread is not None and self._thread.is_alive()

    def start(self, config: SignalMonitorConfig) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                raise RuntimeError("信号监控已经在运行中")
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run,
                args=(config,),
                daemon=True,
                name="qqokx-signal-monitor",
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    def _run(self, config: SignalMonitorConfig) -> None:
        last_confirmed_ts: dict[str, int | None] = {}
        last_emitted: dict[tuple[str, SignalType, SignalDirection], int] = {}
        lookback = max(config.pattern_ema_period * 4, 240)
        min_confirmed = max(config.pattern_ema_period + 2, 60)

        self._logger(
            f"启动信号监控 | 标的={', '.join(config.symbols)} | 周期={config.bar} | 收线缓冲={config.poll_seconds}s"
        )

        baseline_reports = self._build_baseline_reports(config, lookback, min_confirmed, last_confirmed_ts)
        if baseline_reports:
            self._emit_diagnostic(config.bar, baseline_reports)

        while not self._stop_event.is_set():
            wait_seconds = _seconds_until_next_check(config.bar, config.poll_seconds)
            if self._stop_event.wait(wait_seconds):
                break

            round_reports: list[MonitorSymbolDiagnostic] = []
            for symbol in config.symbols:
                if self._stop_event.is_set():
                    break
                try:
                    candles = self._client.get_candles(symbol, config.bar, limit=lookback)
                    confirmed = [item for item in candles if item.confirmed]
                    if len(confirmed) < min_confirmed:
                        message = "已收盘 K 线不足"
                        self._logger(f"{symbol} {message}，暂不检测。")
                        round_reports.append(
                            MonitorSymbolDiagnostic(
                                symbol=symbol,
                                candle_ts=confirmed[-1].ts if confirmed else None,
                                note=message,
                            )
                        )
                        continue

                    latest_ts = confirmed[-1].ts
                    previous_ts = last_confirmed_ts.get(symbol)
                    if previous_ts is not None and latest_ts <= previous_ts:
                        round_reports.append(
                            MonitorSymbolDiagnostic(
                                symbol=symbol,
                                candle_ts=latest_ts,
                                note="新K线未确认，继续等待",
                            )
                        )
                        continue

                    report = evaluate_monitor_signal_report(
                        confirmed,
                        symbol,
                        config,
                        tick_size=self._resolve_tick_size(symbol),
                    )
                    last_confirmed_ts[symbol] = latest_ts
                    new_events: list[MonitorSignalEvent] = []
                    duplicate_events: list[MonitorSignalEvent] = []
                    for event in report.matched_events:
                        key = (event.symbol, event.signal_type, event.direction)
                        if last_emitted.get(key) == event.candle_ts:
                            duplicate_events.append(event)
                            continue
                        last_emitted[key] = event.candle_ts
                        new_events.append(event)
                        self._emit_event(event, config.bar)

                    round_reports.append(
                        MonitorSymbolDiagnostic(
                            symbol=report.symbol,
                            candle_ts=report.candle_ts,
                            matched_events=report.matched_events,
                            filtered_events=report.filtered_events,
                            new_events=tuple(new_events),
                            duplicate_events=tuple(duplicate_events),
                        )
                    )
                except Exception as exc:
                    self._logger(f"{symbol} 信号监控失败：{exc}")
                    round_reports.append(
                        MonitorSymbolDiagnostic(
                            symbol=symbol,
                            candle_ts=None,
                            error=str(exc),
                        )
                    )

            if round_reports:
                self._emit_diagnostic(config.bar, round_reports)

        self._logger("信号监控已停止。")

    def _build_baseline_reports(
        self,
        config: SignalMonitorConfig,
        lookback: int,
        min_confirmed: int,
        last_confirmed_ts: dict[str, int | None],
    ) -> list[MonitorSymbolDiagnostic]:
        reports: list[MonitorSymbolDiagnostic] = []
        for symbol in config.symbols:
            try:
                candles = self._client.get_candles(symbol, config.bar, limit=lookback)
                confirmed = [item for item in candles if item.confirmed]
                latest_ts = confirmed[-1].ts if confirmed else None
                last_confirmed_ts[symbol] = latest_ts
                if len(confirmed) < min_confirmed:
                    reports.append(
                        MonitorSymbolDiagnostic(
                            symbol=symbol,
                            candle_ts=latest_ts,
                            note="已建立基线，但历史K线不足",
                        )
                    )
                    continue
                reports.append(
                    MonitorSymbolDiagnostic(
                        symbol=symbol,
                        candle_ts=latest_ts,
                        note="已建立基线，等待下一根新K线",
                    )
                )
            except Exception as exc:
                last_confirmed_ts[symbol] = None
                reports.append(
                    MonitorSymbolDiagnostic(
                        symbol=symbol,
                        candle_ts=None,
                        error=str(exc),
                    )
                )
        return reports

    def _emit_diagnostic(self, bar: str, reports: list[MonitorSymbolDiagnostic]) -> None:
        if self._diagnostic_callback is None:
            return
        self._diagnostic_callback(
            MonitorRoundDiagnostic(
                bar=bar,
                checked_at=int(time.time() * 1000),
                reports=tuple(reports),
            )
        )

    def _emit_event(self, event: MonitorSignalEvent, bar: str) -> None:
        text = (
            f"{_fmt_ts(event.candle_ts)} | {event.symbol} | {event.signal_type} | "
            f"{event.direction.upper()} | 参考价={format_decimal_by_increment(event.trigger_price, event.tick_size)} | {event.reason}"
        )
        self._logger(text)
        if self._event_callback is not None:
            self._event_callback(event)
        if self._email_sender is not None:
            self._email_sender(event, bar)

    def _resolve_tick_size(self, symbol: str) -> Decimal | None:
        normalized = symbol.strip().upper()
        if normalized in self._symbol_tick_sizes:
            return self._symbol_tick_sizes[normalized]
        try:
            tick_size = self._client.get_instrument(normalized).tick_size
        except Exception:
            tick_size = None
        self._symbol_tick_sizes[normalized] = tick_size
        return tick_size


def evaluate_monitor_signal_report(
    candles: list[Candle],
    symbol: str,
    config: SignalMonitorConfig,
    tick_size: Decimal | None = None,
) -> MonitorSymbolDiagnostic:
    matched_events: list[MonitorSignalEvent] = []
    filtered_events: list[MonitorSignalEvent] = []
    for event in _detect_all_monitor_signals(candles, symbol, config, tick_size=tick_size):
        if _is_signal_enabled(config, event.signal_type):
            matched_events.append(event)
        else:
            filtered_events.append(event)
    return MonitorSymbolDiagnostic(
        symbol=symbol,
        candle_ts=candles[-1].ts if candles else None,
        matched_events=tuple(matched_events),
        filtered_events=tuple(filtered_events),
    )


def evaluate_monitor_signals(
    candles: list[Candle],
    symbol: str,
    config: SignalMonitorConfig,
    tick_size: Decimal | None = None,
) -> list[MonitorSignalEvent]:
    return list(evaluate_monitor_signal_report(candles, symbol, config, tick_size=tick_size).matched_events)


def evaluate_monitor_signal_history(
    candles: list[Candle],
    symbol: str,
    config: SignalMonitorConfig,
    *,
    tick_size: Decimal | None = None,
    signal_type: SignalType | None = None,
    direction: SignalDirection | None = None,
) -> list[MonitorSignalEvent]:
    matched_events: list[MonitorSignalEvent] = []
    for index in range(len(candles)):
        prefix_candles = candles[: index + 1]
        for event in _detect_all_monitor_signals(
            prefix_candles,
            symbol,
            config,
            tick_size=tick_size,
        ):
            if not _is_signal_enabled(config, event.signal_type):
                continue
            if signal_type is not None and event.signal_type != signal_type:
                continue
            if direction is not None and event.direction != direction:
                continue
            matched_events.append(event)
    return matched_events


def _detect_all_monitor_signals(
    candles: list[Candle],
    symbol: str,
    config: SignalMonitorConfig,
    tick_size: Decimal | None = None,
) -> list[MonitorSignalEvent]:
    events: list[MonitorSignalEvent] = []
    closes = [item.close for item in candles]

    event = detect_ema_cross_signal(symbol, candles, closes, tick_size=tick_size)
    if event is not None:
        events.append(event)

    event = detect_ema55_slope_turn(symbol, candles, closes, tick_size=tick_size)
    if event is not None:
        events.append(event)

    event = detect_ema55_breakout(symbol, candles, closes, tick_size=tick_size)
    if event is not None:
        events.append(event)

    event = detect_candle_pattern_signal(symbol, candles, closes, config, tick_size=tick_size)
    if event is not None:
        events.append(event)

    return events


def _is_signal_enabled(config: SignalMonitorConfig, signal_type: SignalType) -> bool:
    if signal_type == "ema21_55_cross":
        return config.enable_ema21_55_cross
    if signal_type == "ema55_slope_turn":
        return config.enable_ema55_slope_turn
    if signal_type == "ema55_breakout":
        return config.enable_ema55_breakout
    if signal_type == "candle_pattern":
        return config.enable_candle_pattern
    return False


def detect_ema_cross_signal(
    symbol: str,
    candles: list[Candle],
    closes: list[Decimal],
    tick_size: Decimal | None = None,
) -> MonitorSignalEvent | None:
    if len(candles) < 56:
        return None
    ema21 = ema(closes, 21)
    ema55 = ema(closes, 55)
    previous_diff = ema21[-2] - ema55[-2]
    current_diff = ema21[-1] - ema55[-1]
    last_candle = candles[-1]
    if previous_diff <= 0 and current_diff > 0:
        return MonitorSignalEvent(
            symbol=symbol,
            signal_type="ema21_55_cross",
            direction="long",
            candle_ts=last_candle.ts,
            trigger_price=last_candle.close,
            reason=(
                f"EMA21 金叉 EMA55 | EMA21={format_decimal_by_increment(ema21[-1], tick_size)} | "
                f"EMA55={format_decimal_by_increment(ema55[-1], tick_size)}"
            ),
            tick_size=tick_size,
        )
    if previous_diff >= 0 and current_diff < 0:
        return MonitorSignalEvent(
            symbol=symbol,
            signal_type="ema21_55_cross",
            direction="short",
            candle_ts=last_candle.ts,
            trigger_price=last_candle.close,
            reason=(
                f"EMA21 死叉 EMA55 | EMA21={format_decimal_by_increment(ema21[-1], tick_size)} | "
                f"EMA55={format_decimal_by_increment(ema55[-1], tick_size)}"
            ),
            tick_size=tick_size,
        )
    return None


def detect_ema55_slope_turn(
    symbol: str,
    candles: list[Candle],
    closes: list[Decimal],
    tick_size: Decimal | None = None,
) -> MonitorSignalEvent | None:
    if len(candles) < 57:
        return None
    ema55 = ema(closes, 55)
    previous_slope = ema55[-2] - ema55[-3]
    current_slope = ema55[-1] - ema55[-2]
    last_candle = candles[-1]
    if previous_slope <= 0 and current_slope > 0:
        return MonitorSignalEvent(
            symbol=symbol,
            signal_type="ema55_slope_turn",
            direction="long",
            candle_ts=last_candle.ts,
            trigger_price=last_candle.close,
            reason=(
                f"EMA55 斜率转正 | 前斜率={format_decimal_by_increment(previous_slope, tick_size)} | "
                f"当前斜率={format_decimal_by_increment(current_slope, tick_size)}"
            ),
            tick_size=tick_size,
        )
    if previous_slope >= 0 and current_slope < 0:
        return MonitorSignalEvent(
            symbol=symbol,
            signal_type="ema55_slope_turn",
            direction="short",
            candle_ts=last_candle.ts,
            trigger_price=last_candle.close,
            reason=(
                f"EMA55 斜率转负 | 前斜率={format_decimal_by_increment(previous_slope, tick_size)} | "
                f"当前斜率={format_decimal_by_increment(current_slope, tick_size)}"
            ),
            tick_size=tick_size,
        )
    return None


def detect_ema55_breakout(
    symbol: str,
    candles: list[Candle],
    closes: list[Decimal],
    tick_size: Decimal | None = None,
) -> MonitorSignalEvent | None:
    if len(candles) < 56:
        return None
    ema55 = ema(closes, 55)
    previous_close = candles[-2].close
    last_candle = candles[-1]
    if previous_close <= ema55[-2] and last_candle.close > ema55[-1]:
        return MonitorSignalEvent(
            symbol=symbol,
            signal_type="ema55_breakout",
            direction="long",
            candle_ts=last_candle.ts,
            trigger_price=last_candle.close,
            reason=f"收盘价向上突破 EMA55 | EMA55={format_decimal_by_increment(ema55[-1], tick_size)}",
            tick_size=tick_size,
        )
    if previous_close >= ema55[-2] and last_candle.close < ema55[-1]:
        return MonitorSignalEvent(
            symbol=symbol,
            signal_type="ema55_breakout",
            direction="short",
            candle_ts=last_candle.ts,
            trigger_price=last_candle.close,
            reason=f"收盘价向下跌破 EMA55 | EMA55={format_decimal_by_increment(ema55[-1], tick_size)}",
            tick_size=tick_size,
        )
    return None


def detect_candle_pattern_signal(
    symbol: str,
    candles: list[Candle],
    closes: list[Decimal],
    config: SignalMonitorConfig,
    tick_size: Decimal | None = None,
) -> MonitorSignalEvent | None:
    if len(candles) < config.pattern_ema_period + 2:
        return None
    ema_values = ema(closes, config.pattern_ema_period)
    last_candle = candles[-1]
    ema_value = ema_values[-1]
    candle_range = last_candle.high - last_candle.low
    if candle_range <= 0:
        return None

    body = abs(last_candle.close - last_candle.open)
    body_ratio = body / candle_range
    lower_wick = min(last_candle.open, last_candle.close) - last_candle.low
    upper_wick = last_candle.high - max(last_candle.open, last_candle.close)
    lower_wick_ratio = lower_wick / candle_range
    upper_wick_ratio = upper_wick / candle_range
    near_ema = _is_near_ema(last_candle, ema_value, config.ema_near_tolerance)

    if near_ema and last_candle.close > last_candle.open:
        if body_ratio >= config.body_ratio_threshold or lower_wick_ratio >= config.wick_ratio_threshold:
            return MonitorSignalEvent(
                symbol=symbol,
                signal_type="candle_pattern",
                direction="long",
                candle_ts=last_candle.ts,
                trigger_price=last_candle.close,
                reason=(
                    f"K线形态做多 | EMA{config.pattern_ema_period}={format_decimal_by_increment(ema_value, tick_size)} | "
                    f"实体={format_decimal_by_increment(body, tick_size)} | "
                    f"下影线={format_decimal_by_increment(lower_wick, tick_size)} | "
                    f"实体比={format_decimal_by_increment(body_ratio, tick_size)} | "
                    f"下影比={format_decimal_by_increment(lower_wick_ratio, tick_size)}"
                ),
                tick_size=tick_size,
            )

    if near_ema and last_candle.close < last_candle.open:
        if body_ratio >= config.body_ratio_threshold or upper_wick_ratio >= config.wick_ratio_threshold:
            return MonitorSignalEvent(
                symbol=symbol,
                signal_type="candle_pattern",
                direction="short",
                candle_ts=last_candle.ts,
                trigger_price=last_candle.close,
                reason=(
                    f"K线形态做空 | EMA{config.pattern_ema_period}={format_decimal_by_increment(ema_value, tick_size)} | "
                    f"实体={format_decimal_by_increment(body, tick_size)} | "
                    f"上影线={format_decimal_by_increment(upper_wick, tick_size)} | "
                    f"实体比={format_decimal_by_increment(body_ratio, tick_size)} | "
                    f"上影比={format_decimal_by_increment(upper_wick_ratio, tick_size)}"
                ),
                tick_size=tick_size,
            )
    return None


def _is_near_ema(candle: Candle, ema_value: Decimal, tolerance: Decimal) -> bool:
    if candle.low <= ema_value <= candle.high:
        return True
    if ema_value <= 0:
        return False
    nearest = min(abs(candle.open - ema_value), abs(candle.close - ema_value), abs(candle.low - ema_value))
    return (nearest / ema_value) <= tolerance


def _bar_interval_seconds(bar: str) -> int:
    normalized = bar.strip()
    if len(normalized) < 2:
        raise ValueError(f"不支持的K线周期：{bar}")
    unit = normalized[-1].lower()
    value = int(normalized[:-1])
    if value <= 0:
        raise ValueError(f"不支持的K线周期：{bar}")
    if unit == "m":
        return value * 60
    if unit == "h":
        return value * 60 * 60
    if unit == "d":
        return value * 60 * 60 * 24
    raise ValueError(f"不支持的K线周期：{bar}")


def _seconds_until_next_check(bar: str, buffer_seconds: float, *, now_ts: float | None = None) -> float:
    current_ts = time.time() if now_ts is None else now_ts
    interval_seconds = _bar_interval_seconds(bar)
    next_close_ts = ((int(current_ts) // interval_seconds) + 1) * interval_seconds
    wait_seconds = (next_close_ts + float(buffer_seconds)) - current_ts
    return max(wait_seconds, 1.0)


def _fmt_ts(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
