from __future__ import annotations

import asyncio
import json
import threading
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable

from okx_quant.models import Candle
from okx_quant.websockets_compat import connect_okx_websocket

try:
    import websockets
except Exception:  # noqa: BLE001
    websockets = None


@dataclass(frozen=True)
class CandleStreamKey:
    inst_id: str
    bar: str
    environment: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "inst_id", self.inst_id.strip().upper())
        object.__setattr__(self, "bar", self.bar.strip())
        object.__setattr__(self, "environment", self.environment.strip().lower() or "demo")

    @property
    def channel(self) -> str:
        return f"candle{self.bar}"


class CandleStreamState:
    def __init__(self, candles: list[Candle] | None = None) -> None:
        self._candles = list(candles or [])

    @property
    def candles(self) -> tuple[Candle, ...]:
        return tuple(self._candles)

    def apply(self, candle: Candle) -> None:
        for index, existing in enumerate(self._candles):
            if existing.ts == candle.ts:
                self._candles[index] = candle
                return
        self._candles.append(candle)
        self._candles.sort(key=lambda item: item.ts)


def _parse_okx_candle(row: object) -> Candle:
    if not isinstance(row, (list, tuple)) or len(row) < 9:
        raise ValueError("OKX candle payload is incomplete")
    return Candle(
        ts=int(str(row[0])),
        open=Decimal(str(row[1])),
        high=Decimal(str(row[2])),
        low=Decimal(str(row[3])),
        close=Decimal(str(row[4])),
        volume=Decimal(str(row[5])),
        confirmed=str(row[8]).strip() == "1",
    )


class OkxCandleWsConnectionUnavailable(RuntimeError):
    pass


class OkxCandleWsConnection:
    _BUSINESS_URL = "wss://ws.okx.com:8443/ws/v5/business"
    _DEMO_BUSINESS_URL = "wss://wspap.okx.com:8443/ws/v5/business"

    def __init__(self, *, environment: str, logger: Callable[[str], None] | None = None) -> None:
        self._environment = environment.strip().lower() or "demo"
        self._logger = logger or (lambda _message: None)
        self._lock = threading.RLock()
        self._listeners: dict[CandleStreamKey, set[Callable[[Candle, bool], None]]] = {}
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._socket: Any = None
        self._connected = False
        self._subscribed: set[CandleStreamKey] = set()

    def start(self) -> None:
        if websockets is None:
            raise OkxCandleWsConnectionUnavailable("websockets dependency is unavailable")
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run_forever, daemon=True, name=f"okx-candle-ws-{self._environment}")
            self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        loop = self._loop
        socket = self._socket
        if loop is not None and socket is not None:
            try:
                asyncio.run_coroutine_threadsafe(socket.close(), loop).result(timeout=2.0)
            except Exception:
                pass
        with self._lock:
            thread = self._thread
        if thread is not None:
            thread.join(timeout=3.0)

    def watch(self, key: CandleStreamKey, listener: Callable[[Candle, bool], None]) -> Callable[[], None]:
        with self._lock:
            listeners = self._listeners.setdefault(key, set())
            listeners.add(listener)
            loop = self._loop
            connected = self._connected
        if connected and loop is not None:
            asyncio.run_coroutine_threadsafe(self._ensure_subscription(key), loop)

        def _unsubscribe() -> None:
            with self._lock:
                active = self._listeners.get(key)
                if active is not None:
                    active.discard(listener)
                    if not active:
                        self._listeners.pop(key, None)

        return _unsubscribe

    def _run_forever(self) -> None:
        asyncio.run(self._run_forever_async())

    async def _run_forever_async(self) -> None:
        self._loop = asyncio.get_running_loop()
        while not self._stop_event.is_set():
            try:
                await self._run_connection_once()
            except Exception as exc:  # noqa: BLE001
                self._logger(f"OKX candle WS reconnect: {exc}")
                await asyncio.sleep(1)

    async def _run_connection_once(self) -> None:
        assert websockets is not None
        url = self._DEMO_BUSINESS_URL if self._environment == "demo" else self._BUSINESS_URL
        headers = {"x-simulated-trading": "1"} if self._environment == "demo" else None
        kwargs: dict[str, Any] = {"ping_interval": 20, "ping_timeout": 20, "open_timeout": 20}
        context = connect_okx_websocket(url, headers=headers, **kwargs)
        async with context as socket:
            with self._lock:
                self._socket = socket
                self._connected = True
                self._subscribed.clear()
                keys = tuple(self._listeners)
            for key in keys:
                await self._ensure_subscription(key)
            while not self._stop_event.is_set():
                await self._handle_message(await socket.recv())

    async def _ensure_subscription(self, key: CandleStreamKey) -> None:
        with self._lock:
            if key in self._subscribed or self._socket is None:
                return
            self._subscribed.add(key)
            socket = self._socket
        await socket.send(json.dumps({"op": "subscribe", "args": [{"channel": key.channel, "instId": key.inst_id}]}, separators=(",", ":")))

    async def _handle_message(self, message: object) -> None:
        if isinstance(message, bytes):
            message = message.decode("utf-8", errors="replace")
        payload = json.loads(str(message))
        arg = payload.get("arg")
        rows = payload.get("data")
        if not isinstance(arg, dict) or not isinstance(rows, list):
            return
        channel = str(arg.get("channel") or "")
        inst_id = str(arg.get("instId") or "").strip().upper()
        if not channel.startswith("candle") or not inst_id:
            return
        key = CandleStreamKey(inst_id, channel.removeprefix("candle"), self._environment)
        with self._lock:
            listeners = tuple(self._listeners.get(key, ()))
        for row in rows:
            candle = _parse_okx_candle(row)
            for listener in listeners:
                listener(candle, candle.confirmed)
