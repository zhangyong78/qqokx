from __future__ import annotations

import asyncio
import json
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

try:
    import websockets
except Exception:  # noqa: BLE001
    websockets = None


Logger = Callable[[str], None]


@dataclass(frozen=True)
class OkxPublicWsRecord:
    version: int
    payload: dict[str, Any]
    received_at: float


class OkxPublicWsConnectionUnavailable(RuntimeError):
    pass


class OkxPublicWsConnection:
    _PUBLIC_URL = "wss://ws.okx.com:8443/ws/v5/public"
    _DEMO_PUBLIC_URL = "wss://wspap.okx.com:8443/ws/v5/public"

    def __init__(
        self,
        *,
        environment: str,
        logger: Logger | None = None,
    ) -> None:
        self._environment = environment
        self._logger = logger or (lambda _message: None)
        self._lock = threading.Condition()
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._socket = None
        self._connected = False
        self._last_error = ""
        self._last_error_logged = ""
        self._version = 0
        self._watched_inst_ids: set[str] = set()
        self._subscribed_channels: set[tuple[str, str]] = set()
        self._ticker_by_inst_id: dict[str, OkxPublicWsRecord] = {}
        self._order_book_by_inst_id: dict[str, OkxPublicWsRecord] = {}

    def debug_status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "connected": self._connected,
                "last_error": self._last_error,
                "version": self._version,
                "watch_count": len(self._watched_inst_ids),
                "ticker_count": len(self._ticker_by_inst_id),
                "order_book_count": len(self._order_book_by_inst_id),
                "environment": self._environment,
            }

    def start(self) -> None:
        if websockets is None:
            raise OkxPublicWsConnectionUnavailable("websockets 依赖不可用，无法启用 OKX 公共 WS。")
        with self._lock:
            thread = self._thread
            if thread is not None and thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run_forever,
                daemon=True,
                name=f"okx-public-ws-{self._environment}",
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        loop = self._loop
        socket = self._socket
        if loop is not None and socket is not None:
            try:
                asyncio.run_coroutine_threadsafe(socket.close(), loop).result(timeout=2.0)
            except Exception:  # noqa: BLE001
                pass
        with self._lock:
            thread = self._thread
        if thread is not None:
            thread.join(timeout=3.0)

    def watch_inst_id(self, inst_id: str) -> None:
        normalized = inst_id.strip().upper()
        if not normalized:
            return
        with self._lock:
            self._watched_inst_ids.add(normalized)
            loop = self._loop
            connected = self._connected
        if connected and loop is not None:
            try:
                asyncio.run_coroutine_threadsafe(self._ensure_inst_subscription(normalized), loop)
            except Exception:  # noqa: BLE001
                pass

    def get_latest_ticker(self, inst_id: str) -> tuple[int, dict[str, Any]] | None:
        normalized = inst_id.strip().upper()
        with self._lock:
            record = self._ticker_by_inst_id.get(normalized)
            if record is None:
                return None
            return record.version, dict(record.payload)

    def get_latest_order_book(self, inst_id: str) -> tuple[int, dict[str, Any]] | None:
        normalized = inst_id.strip().upper()
        with self._lock:
            record = self._order_book_by_inst_id.get(normalized)
            if record is None:
                return None
            return record.version, dict(record.payload)

    def _run_forever(self) -> None:
        try:
            asyncio.run(self._run_forever_async())
        finally:
            with self._lock:
                self._connected = False
                self._loop = None
                self._socket = None
                self._lock.notify_all()

    async def _run_forever_async(self) -> None:
        self._loop = asyncio.get_running_loop()
        reconnect_delay = 1.0
        while not self._stop_event.is_set():
            try:
                await self._run_connection_once()
                reconnect_delay = 1.0
            except Exception as exc:  # noqa: BLE001
                self._set_last_error(str(exc).strip() or exc.__class__.__name__)
                self._log_error_once(f"OKX 公共 WS 断开，准备重连：{self._last_error}")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 15.0)

    async def _run_connection_once(self) -> None:
        assert websockets is not None
        url = self._DEMO_PUBLIC_URL if self._environment == "demo" else self._PUBLIC_URL
        headers: dict[str, str] = {}
        if self._environment == "demo":
            headers["x-simulated-trading"] = "1"
        connect_kwargs = {
            "ping_interval": 20,
            "ping_timeout": 20,
            "open_timeout": 20,
            "close_timeout": 5,
            "max_queue": 1000,
        }
        if headers:
            connect_kwargs["additional_headers"] = headers
        try:
            socket_context = websockets.connect(url, **connect_kwargs)
        except TypeError:
            if headers:
                connect_kwargs.pop("additional_headers", None)
                connect_kwargs["extra_headers"] = headers
            socket_context = websockets.connect(url, **connect_kwargs)
        async with socket_context as socket:
            with self._lock:
                self._socket = socket
                self._connected = True
                self._last_error = ""
                self._last_error_logged = ""
                self._subscribed_channels.clear()
                self._lock.notify_all()
            await self._ensure_all_subscriptions()
            while not self._stop_event.is_set():
                message = await socket.recv()
                if isinstance(message, bytes):
                    message = message.decode("utf-8", errors="replace")
                await self._handle_message(message)
        with self._lock:
            self._connected = False
            self._socket = None

    async def _handle_message(self, message: str) -> None:
        if message == "ping":
            socket = self._socket
            if socket is not None:
                await socket.send("pong")
            return
        payload = json.loads(message)
        event = str(payload.get("event") or "").strip().lower()
        if event == "error":
            raise RuntimeError(str(payload.get("msg") or payload.get("code") or "公共 WS 返回错误"))
        arg = payload.get("arg")
        if not isinstance(arg, dict):
            return
        channel = str(arg.get("channel") or "").strip().lower()
        inst_id = str(arg.get("instId") or "").strip().upper()
        if channel not in {"tickers", "books5"} or not inst_id:
            return
        rows = payload.get("data")
        if not isinstance(rows, list) or not rows:
            return
        first = rows[0]
        if not isinstance(first, dict):
            return
        with self._lock:
            self._version += 1
            record = OkxPublicWsRecord(
                version=self._version,
                payload=first,
                received_at=time.time(),
            )
            if channel == "tickers":
                self._ticker_by_inst_id[inst_id] = record
            else:
                self._order_book_by_inst_id[inst_id] = record
            self._lock.notify_all()

    async def _ensure_all_subscriptions(self) -> None:
        with self._lock:
            inst_ids = tuple(sorted(self._watched_inst_ids))
        for inst_id in inst_ids:
            await self._ensure_inst_subscription(inst_id)

    async def _ensure_inst_subscription(self, inst_id: str) -> None:
        socket = self._socket
        if socket is None:
            return
        args: list[dict[str, str]] = []
        for channel in ("tickers", "books5"):
            key = (channel, inst_id)
            with self._lock:
                if key in self._subscribed_channels:
                    continue
                self._subscribed_channels.add(key)
            args.append({"channel": channel, "instId": inst_id})
        if not args:
            return
        payload = {"op": "subscribe", "args": args}
        await socket.send(json.dumps(payload, separators=(",", ":")))

    def _set_last_error(self, message: str) -> None:
        with self._lock:
            self._last_error = message

    def _log_error_once(self, message: str) -> None:
        with self._lock:
            if message == self._last_error_logged:
                return
            self._last_error_logged = message
        self._logger(message)
