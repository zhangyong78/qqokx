from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass
from typing import Callable

from okx_quant.models import Candle
from okx_quant.okx_client import OkxRestClient


Logger = Callable[[str], None]
_BAR_PATTERN = re.compile(r"^\s*(\d+)\s*([a-zA-Z]+)\s*$")


def _bar_seconds(bar: str) -> int:
    match = _BAR_PATTERN.match(str(bar or "").strip())
    if match is None:
        return 60
    value = max(int(match.group(1)), 1)
    unit = match.group(2).lower()
    if unit == "m":
        return value * 60
    if unit == "h":
        return value * 3600
    if unit in {"d", "day"}:
        return value * 86400
    if unit in {"w", "week"}:
        return value * 7 * 86400
    return 60


def _seconds_until_next_close(bar_seconds: int, now_ts: float | None = None) -> float:
    current = time.time() if now_ts is None else float(now_ts)
    interval = max(int(bar_seconds), 1)
    next_close_ts = ((int(current) // interval) + 1) * interval
    return max(float(next_close_ts) - current, 0.0)


def _refresh_interval_seconds(bar: str) -> float:
    bar_seconds = _bar_seconds(bar)
    until_close = _seconds_until_next_close(bar_seconds)
    if bar_seconds <= 300:
        return 2.0
    if bar_seconds <= 900:
        return 5.0 if until_close <= 60 else 15.0
    if bar_seconds <= 3600:
        return 10.0 if until_close <= 180 else 30.0
    if bar_seconds <= 14400:
        return 10.0 if until_close <= 300 else 60.0
    return 15.0 if until_close <= 600 else 120.0


@dataclass(frozen=True)
class CandleFeedKey:
    inst_id: str
    bar: str


class SharedCandleFeed:
    def __init__(
        self,
        client: OkxRestClient,
        key: CandleFeedKey,
        *,
        logger: Logger | None = None,
    ) -> None:
        self._client = client
        self._key = key
        self._logger = logger or (lambda _message: None)
        self._condition = threading.Condition()
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._candles: list[Candle] = []
        self._requested_limit = 0
        self._last_error = ""
        self._last_logged_error = ""

    def get_candles(self, *, limit: int, timeout: float = 15.0) -> list[Candle]:
        requested_limit = max(int(limit), 1)
        with self._condition:
            if requested_limit > self._requested_limit:
                self._requested_limit = requested_limit
                self._condition.notify_all()
            self._ensure_started_locked()
            deadline = time.monotonic() + max(float(timeout), 1.0)
            while not self._candles and not self._stop_event.is_set():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._condition.wait(timeout=remaining)
            if self._candles:
                return list(self._candles[-requested_limit:])
        return self._client.get_candles(self._key.inst_id, self._key.bar, limit=requested_limit)

    def stop(self) -> None:
        self._stop_event.set()
        with self._condition:
            self._condition.notify_all()
            thread = self._thread
        if thread is not None:
            thread.join(timeout=2.0)

    def _ensure_started_locked(self) -> None:
        thread = self._thread
        if thread is not None and thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_forever,
            daemon=True,
            name=f"market-data-{self._key.inst_id}-{self._key.bar}",
        )
        self._thread.start()

    def _run_forever(self) -> None:
        while not self._stop_event.is_set():
            self._refresh_once()
            wait_seconds = _refresh_interval_seconds(self._key.bar)
            with self._condition:
                if self._stop_event.is_set():
                    return
                self._condition.wait(timeout=wait_seconds)

    def _refresh_once(self) -> None:
        with self._condition:
            requested_limit = max(self._requested_limit, 1)
        try:
            candles = self._client.get_candles(self._key.inst_id, self._key.bar, limit=requested_limit)
        except Exception as exc:  # noqa: BLE001
            message = str(exc).strip() or exc.__class__.__name__
            self._last_error = message
            if message != self._last_logged_error:
                self._last_logged_error = message
                self._logger(
                    f"共享K线刷新失败 | {self._key.inst_id} {self._key.bar} | {message} | 已保留现有缓存并继续重试"
                )
            with self._condition:
                self._condition.notify_all()
            return
        with self._condition:
            self._candles = list(candles)
            self._last_error = ""
            self._last_logged_error = ""
            self._condition.notify_all()


class MarketDataHub:
    def __init__(
        self,
        client: OkxRestClient,
        *,
        logger: Logger | None = None,
    ) -> None:
        self._client = client
        self._logger = logger or (lambda _message: None)
        self._lock = threading.Lock()
        self._feeds: dict[CandleFeedKey, SharedCandleFeed] = {}

    def get_candles(self, inst_id: str, bar: str, *, limit: int) -> list[Candle]:
        key = CandleFeedKey(
            inst_id=str(inst_id or "").strip().upper(),
            bar=str(bar or "").strip() or "1H",
        )
        with self._lock:
            feed = self._feeds.get(key)
            if feed is None:
                feed = SharedCandleFeed(self._client, key, logger=self._logger)
                self._feeds[key] = feed
        return feed.get_candles(limit=limit)

    def stop(self) -> None:
        with self._lock:
            feeds = list(self._feeds.values())
        for feed in feeds:
            feed.stop()
