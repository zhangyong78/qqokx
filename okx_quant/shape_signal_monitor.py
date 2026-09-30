"""Low-priority background monitor for subscribed multi-period candle shapes."""

from __future__ import annotations

import queue
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from PySide6.QtCore import QObject, Signal

from okx_quant.candle_cache import load_candle_cache
from okx_quant.candle_store import upsert_candles
from okx_quant.deribit_client import DeribitRestClient
from okx_quant.models import Candle, EmailNotificationConfig
from okx_quant.notifications import EmailNotifier
from okx_quant.okx_candle_ws import CandleStreamKey
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import load_notification_snapshot
from okx_quant.shape_signal_store import (
    SUPPORTED_PATTERNS,
    SUPPORTED_PERIODS,
    append_events,
    load_subscriptions,
    normalize_subscription,
)


SHANGHAI_TZ = timezone(timedelta(hours=8))


class ShapeSignalMonitor(QObject):
    """Owns public market subscriptions without doing work in the Qt event loop."""

    signal_detected = Signal(object)
    status_changed = Signal(str)

    def __init__(self, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self._lock = threading.RLock()
        self._subscriptions: list[dict[str, object]] = load_subscriptions()
        self._clients: dict[str, OkxRestClient] = {}
        self._unsubscribers: list[Any] = []
        self._states: dict[tuple[str, str, str], list[Candle]] = {}
        self._queue: queue.Queue[tuple[str, Candle] | tuple[str, None]] = queue.Queue(maxsize=5000)
        self._stop_event = threading.Event()
        self._worker: threading.Thread | None = None
        self._dirty: set[tuple[str, str, str]] = set()
        self._dirty_latest: dict[tuple[str, str, str], Candle] = {}
        self._last_startup_scan_day = ""

    def start(self) -> None:
        with self._lock:
            if self._worker is not None and self._worker.is_alive():
                return
            self._stop_event.clear()
            self._worker = threading.Thread(target=self._run, daemon=True, name="qqokx-shape-monitor")
            self._worker.start()

    def stop(self) -> None:
        self._stop_event.set()
        try:
            self._queue.put_nowait(("__stop__", None))
        except queue.Full:
            pass
        for unsubscribe in self._unsubscribers:
            try:
                unsubscribe()
            except Exception:
                pass
        self._unsubscribers.clear()
        worker = self._worker
        if worker is not None:
            worker.join(timeout=3.0)
        for client in self._clients.values():
            try:
                client.close()
            except Exception:
                pass

    def reload_subscriptions(self, items: list[dict[str, object]] | None = None) -> None:
        with self._lock:
            self._subscriptions = [normalize_subscription(item) for item in (items if items is not None else load_subscriptions())]
        try:
            self._queue.put_nowait(("__reload__", None))
        except queue.Full:
            pass

    def subscriptions(self) -> list[dict[str, object]]:
        with self._lock:
            return [dict(item) for item in self._subscriptions]

    def _run(self) -> None:
        self._status("启动形态后台监控")
        self._rebuild_connections()
        self._startup_reconcile()
        next_flush = time.monotonic() + 8.0
        next_volatility_sync = time.monotonic() + 30.0
        while not self._stop_event.wait(0.25):
            # Multiple symbols/periods push updates concurrently. One item per
            # tick caps consumption at 4/s and can bury closing candles.
            for _ in range(128):
                if self._stop_event.is_set():
                    break
                try:
                    key_text, candle = self._queue.get_nowait()
                except queue.Empty:
                    break
                if key_text == "__reload__":
                    self._rebuild_connections()
                    self._startup_reconcile()
                elif key_text == "__stop__":
                    break
                elif candle is not None:
                    try:
                        self._handle_candle(key_text, candle)
                    except Exception as exc:
                        self._status(f"形态行情处理失败：{exc}")
            if time.monotonic() >= next_flush:
                self._flush_cache()
                next_flush = time.monotonic() + 8.0
            if time.monotonic() >= next_volatility_sync:
                self._sync_volatility_cache()
                next_volatility_sync = time.monotonic() + 600.0
        self._flush_cache()
        self._status("形态后台监控已停止")

    def _rebuild_connections(self) -> None:
        for unsubscribe in self._unsubscribers:
            try:
                unsubscribe()
            except Exception:
                pass
        self._unsubscribers.clear()
        with self._lock:
            subscriptions = [dict(item) for item in self._subscriptions if bool(item.get("enabled", True))]
        keys: set[tuple[str, str, str]] = set()
        for subscription in subscriptions:
            symbol = str(subscription.get("symbol") or "").strip().upper()
            environment = str(subscription.get("environment") or "demo").strip().lower() or "demo"
            for period in subscription.get("periods", []):
                normalized_period = str(period).strip().upper()
                if symbol and normalized_period in SUPPORTED_PERIODS:
                    keys.add((symbol, normalized_period, environment))
        active_environments = {environment for _symbol, _period, environment in keys}
        for environment, client in list(self._clients.items()):
            if environment in active_environments:
                continue
            try:
                client.close()
            except Exception:
                pass
            self._clients.pop(environment, None)
        for symbol, period, environment in sorted(keys):
            client = self._clients.setdefault(environment, OkxRestClient())
            stream_key = CandleStreamKey(symbol, period, environment)
            unsubscribe = client.watch_candle(stream_key, lambda candle, _confirmed, text=f"{symbol}|{period}|{environment}": self._enqueue(text, candle))
            if unsubscribe is not None:
                self._unsubscribers.append(unsubscribe)
        self._status(f"形态后台订阅 {len(keys)} 路")

    def _startup_reconcile(self) -> None:
        with self._lock:
            subscriptions = [dict(item) for item in self._subscriptions if bool(item.get("enabled", True))]
        keys = sorted({
            (str(item.get("symbol") or "").strip().upper(), str(period).strip().upper(), str(item.get("environment") or "demo").strip().lower() or "demo")
            for item in subscriptions
            for period in item.get("periods", [])
            if str(item.get("symbol") or "").strip() and str(period).strip().upper() in SUPPORTED_PERIODS
        })
        for symbol, period, environment in keys:
            if self._stop_event.is_set():
                return
            client = self._clients.setdefault(environment, OkxRestClient())
            try:
                candles = client.get_candles_history(symbol, period, limit=240)
                self._states[(symbol, period, environment)] = list(candles[-240:])
                self._scan_state(
                    symbol,
                    period,
                    environment,
                    source="startup",
                    min_ts=int((time.time() - (48 * 3600)) * 1000),
                )
            except Exception as exc:  # noqa: BLE001
                self._status(f"{symbol} {period} 补算失败：{exc}")

    def _enqueue(self, key_text: str, candle: Candle) -> None:
        try:
            self._queue.put_nowait((key_text, candle))
        except queue.Full:
            self._status("形态行情队列已满，暂缓处理；套利模块不受影响")

    def _handle_candle(self, key_text: str, candle: Candle) -> None:
        parts = key_text.split("|")
        if len(parts) != 3:
            return
        symbol, period, environment = parts
        key = (symbol, period, environment)
        if key not in self._states:
            self._states[key] = list(load_candle_cache(symbol, period, limit=240))
        candles = self._states[key]
        replaced = False
        for index, existing in enumerate(candles):
            if existing.ts == candle.ts:
                candles[index] = candle
                replaced = True
                break
        if not replaced:
            candles.append(candle)
        candles.sort(key=lambda item: item.ts)
        self._states[key] = candles[-240:]
        self._dirty.add(key)
        self._dirty_latest[key] = candle
        if candle.confirmed:
            self._scan_state(symbol, period, environment, source="live", only_ts=candle.ts)

    def _flush_cache(self) -> None:
        dirty = list(self._dirty)
        self._dirty.clear()
        for symbol, period, _environment in dirty:
            candle = self._dirty_latest.pop((symbol, period, _environment), None)
            if candle is None:
                continue
            try:
                upsert_candles(symbol, period, [candle])
            except Exception as exc:  # noqa: BLE001
                self._status(f"{symbol} {period} 缓存写入失败：{exc}")

    def _sync_volatility_cache(self) -> None:
        with self._lock:
            symbols = {
                str(item.get("symbol") or "").strip().upper()
                for item in self._subscriptions
                if bool(item.get("enabled", True))
            }
        currencies = {"BTC" if symbol.startswith("BTC-") else "ETH" for symbol in symbols if symbol.startswith(("BTC-", "ETH-"))}
        if not currencies:
            return
        try:
            from okx_quant.option_strategy_ui import _load_deribit_hourly_series_from_cache, _save_deribit_hourly_series_to_cache

            client = DeribitRestClient()
            end_ts = int(time.time() * 1000)
            for currency in sorted(currencies):
                cached = _load_deribit_hourly_series_from_cache(currency)
                start_ts = max(0, (cached[-1].ts - 3 * 3_600_000) if cached else end_ts - 48 * 3_600_000)
                fetched = client.get_volatility_index_candles(currency, "3600", start_ts=start_ts, end_ts=end_ts, max_records=None)
                merged = {item.ts: item for item in cached}
                merged.update({item.ts: item for item in fetched})
                if merged:
                    _save_deribit_hourly_series_to_cache(currency, [merged[ts] for ts in sorted(merged)])
        except Exception as exc:  # noqa: BLE001
            self._status(f"波动率缓存刷新失败：{exc}")

    def _scan_state(
        self,
        symbol: str,
        period: str,
        environment: str,
        *,
        source: str,
        only_ts: int | None = None,
        min_ts: int | None = None,
    ) -> None:
        candles = [item for item in self._states.get((symbol, period, environment), []) if item.confirmed]
        if len(candles) < 10:
            return
        try:
            from roll_terminal_qt.kline_analysis_window import _build_replay_signal_markers, _to_ema, _to_sma

            closes = [float(item.close) for item in candles]
            markers = _build_replay_signal_markers(
                candles=candles,
                period=period,
                ema15_values=_to_ema(closes, 15),
                sma50_values=_to_sma(closes, 50),
            )
        except Exception as exc:  # noqa: BLE001
            self._status(f"{symbol} {period} 形态计算失败：{exc}")
            return
        with self._lock:
            subscriptions = [dict(item) for item in self._subscriptions if bool(item.get("enabled", True))]
        selected = [item for item in subscriptions if str(item.get("symbol") or "").strip().upper() == symbol and period in [str(value).upper() for value in item.get("periods", [])] and str(item.get("environment") or "demo").strip().lower() == environment]
        for subscription in selected:
            allowed = {str(item).strip().lower() for item in subscription.get("patterns", SUPPORTED_PATTERNS)}
            metric = str(subscription.get("metric") or "body").strip().lower()
            top_n = int(subscription.get("top_n", 4) or 4)
            for marker in markers:
                marker_ts = int(marker.get("time", 0) or 0) * 1000
                if only_ts is not None and marker_ts != int(only_ts):
                    continue
                if min_ts is not None and marker_ts < int(min_ts):
                    continue
                if str(marker.get("pattern_id") or "").strip().lower() not in allowed:
                    continue
                rank_key = "core_range_rank" if metric == "range" else "core_body_rank"
                rank = marker.get(rank_key)
                if rank is not None and int(rank) > top_n:
                    continue
                if bool(subscription.get("require_ma_touch", False)) and not bool(marker.get("core_ma_touch", False)):
                    continue
                event = {
                    "subscription_id": str(subscription.get("id") or ""),
                    "symbol": symbol,
                    "period": period,
                    "environment": environment,
                    "pattern_id": str(marker.get("pattern_id") or ""),
                    "pattern_name": str(marker.get("label") or marker.get("pattern_id") or ""),
                    "direction": str(marker.get("direction") or "neutral"),
                    "candle_ts": int(marker_ts),
                    "score": int(marker.get("score", 0) or 0),
                    "close": str(candles[-1].close if only_ts is None else next((item.close for item in candles if item.ts == marker_ts), "")),
                    "marker_text": str(marker.get("text") or ""),
                    "source": source,
                    "detected_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                }
                added = append_events([event])
                if added:
                    self._notify(subscription, added[0])

    def _notify(self, subscription: dict[str, object], event: dict[str, object]) -> None:
        event["popup_enabled"] = bool(subscription.get("popup_enabled", True))
        if bool(subscription.get("email_enabled", False)) and (event.get("source") != "startup" or bool(subscription.get("startup_email_enabled", False))):
            snapshot = load_notification_snapshot()
            recipients = tuple(item.strip() for item in str(snapshot.get("recipient_emails", "")).replace(";", ",").split(",") if item.strip())
            notifier = EmailNotifier(EmailNotificationConfig(
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
            ))
            if notifier.signal_notifications_enabled:
                subject = f"[QQOKX] 形态信号 | {event['symbol']} | {event['period']} | {event['pattern_name']}"
                body = "\n".join((
                    f"品种：{event['symbol']}", f"周期：{event['period']}", f"形态：{event['pattern_name']}",
                    f"方向：{event['direction']}", f"K线时间：{datetime.fromtimestamp(int(event['candle_ts']) / 1000, SHANGHAI_TZ).strftime('%Y-%m-%d %H:%M:%S')}",
                    f"收盘价：{event['close']}", f"说明：{event['marker_text']}", f"来源：{event['source']}",
                ))
                notifier.notify_async(subject, body)
                event["email_submitted"] = True
        self.signal_detected.emit(dict(event))

    def _status(self, text: str) -> None:
        self.status_changed.emit(str(text))
