"""Opt-in, restart-applied fallback for public OKX REST market requests."""
from __future__ import annotations

import http.client
import json
import secrets
import ssl
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from urllib import error, parse, request

from okx_quant.app_paths import config_dir_path
from okx_quant.persistence import _decrypt_credential_text, _encrypt_credential_text, _write_json_atomic
from okx_quant import market_relay_protocol as protocol

RESTART_NOTE = "行情中转设置保存后重启所有 QQOKX 窗口生效。"
ALLOWED_PATHS = set(protocol.RULES)
DIRECT_TIMEOUT = 4.0
COOLDOWN_SECONDS = 30.0


@dataclass(frozen=True)
class Settings:
    enabled: bool = False
    url: str = ""
    token: str = field(default="", repr=False)


@dataclass
class _Circuit:
    failed_until: float = 0.0
    probing: bool = False


_lock = threading.RLock()
_runtime_settings: dict[Path, Settings | ValueError] = {}
_circuits: dict[Path, _Circuit] = {}


def config_path() -> Path:
    return config_dir_path() / "market_relay.json"


def validate(enabled: bool, url: str, token: str) -> Settings:
    if not isinstance(enabled, bool):
        raise ValueError("行情中转开关格式无效。")
    url, token = url.strip().rstrip("/"), token.strip()
    if enabled:
        try:
            parsed = parse.urlsplit(url)
            valid = (parsed.scheme == "https" and parsed.hostname and not parsed.username
                     and not parsed.password and not parsed.query and not parsed.fragment and not parsed.path
                     and (parsed.port is None or 1 <= parsed.port <= 65535)
                     and not any(char.isspace() or ord(char) < 32 for char in url)
                     and "\\" not in url and "%" not in parsed.netloc)
        except ValueError:
            valid = False
        if not valid:
            raise ValueError("行情中转地址必须为有效的 HTTPS 根地址。")
        if not protocol.valid_token(token):
            raise ValueError("中转密钥须为 32–128 位安全随机字符（字母、数字、短横线、下划线）。")
    return Settings(enabled, url, token)


def load() -> Settings:
    try:
        data = json.loads(config_path().read_text(encoding="utf-8"))
    except FileNotFoundError:
        return Settings()
    if not isinstance(data, dict) or not isinstance(data.get("url", ""), str):
        raise ValueError("行情中转配置格式无效。")
    return validate(data.get("enabled", False), data.get("url", ""), _decrypt_credential_text(data.get("token", "")))


def runtime() -> Settings:
    path = config_path()
    with _lock:
        if path not in _runtime_settings:
            try:
                _runtime_settings[path] = load()
            except (OSError, ValueError, TypeError):
                _runtime_settings[path] = ValueError("行情中转配置读取失败，请修复设置并重启。")
        settings = _runtime_settings[path]
        if isinstance(settings, ValueError):
            raise ValueError(str(settings)) from None
        return settings


def save(settings: Settings) -> Settings:
    settings = validate(settings.enabled, settings.url, settings.token)
    with _lock:
        # Freeze old settings even when saving before the first request.
        try:
            runtime()
        except ValueError:
            pass
        _write_json_atomic(config_path(), {"enabled": settings.enabled, "url": settings.url,
                                           "token": _encrypt_credential_text(settings.token)})
    return settings


def reset_runtime_for_tests():
    with _lock:
        _runtime_settings.clear()
        _circuits.clear()


class RelayError(RuntimeError):
    pass


def relay(path: str, query: str) -> dict:
    settings = runtime()
    if not settings.enabled:
        raise RelayError("行情中转未启用。")
    protocol.validate(path, query)
    request_id = secrets.token_hex(16)
    target = path + ("?" + query if query else "")
    req = request.Request(settings.url + protocol.PREFIX + target, headers={
        protocol.TOKEN_HEADER: settings.token, protocol.REQUEST_HEADER: request_id, "Accept": "application/json",
    })
    # Do not reuse a failing system proxy or follow a redirect with credentials.
    opener = request.build_opener(request.ProxyHandler({}), protocol.NoRedirect())
    try:
        with opener.open(req, timeout=8.0) as response:
            raw = response.read(protocol.MAX_RESPONSE_BYTES + 1)
        if len(raw) > protocol.MAX_RESPONSE_BYTES:
            raise ValueError("response too large")
        envelope = json.loads(raw)
        if (not isinstance(envelope, dict) or envelope.get("protocol") != 1
                or envelope.get("request_id") != request_id or envelope.get("target") != target):
            raise ValueError("request mismatch")
        stamp = envelope.get("served_at")
        if not isinstance(stamp, (float, int)) or not -5 <= time.time() - stamp <= 30:
            raise ValueError("expired relay response")
        payload = envelope.get("payload")
        if not isinstance(payload, dict) or not isinstance(payload.get("code"), str) or not isinstance(payload.get("data"), list):
            raise ValueError("invalid relay response")
        return payload
    except error.HTTPError as exc:
        raise RelayError(f"行情中转请求失败（HTTP {exc.code}），请检查鉴权、限流或服务器状态。") from None
    except Exception:
        # Never expose a URL, header, token or untrusted response in an error.
        raise RelayError("行情中转连接失败或响应无效，请检查服务器连接及两端时间。") from None


def network_failure(exc: BaseException) -> bool:
    if isinstance(exc, error.HTTPError):
        return False
    if isinstance(exc, (error.URLError, TimeoutError, ConnectionError, ssl.SSLError,
                        http.client.RemoteDisconnected, http.client.IncompleteRead)):
        return True
    return isinstance(exc, OSError) and any(marker in str(exc).lower() for marker in (
        "timeout", "timed out", "handshake", "connection reset", "connection aborted",
        "connection refused", "eof occurred", "remote end closed", "remotedisconnected",
    ))


def execute(path: str, query: str, direct, logger=None) -> dict:
    """Run direct(timeout); only transport failures use the relay.

    Healthy calls remain concurrent. After an outage, one call probes direct
    while other calls continue using the relay until that probe finishes.
    """
    key = config_path()
    with _lock:
        circuit = _circuits.setdefault(key, _Circuit())
        use_relay = circuit.failed_until > time.monotonic() or circuit.probing
        probe = not use_relay and circuit.failed_until > 0
        if probe:
            circuit.probing = True
    if use_relay:
        return relay(path, query)
    try:
        payload = direct(DIRECT_TIMEOUT)
    except Exception as exc:
        if not network_failure(exc):
            with _lock:
                circuit.failed_until = 0
            raise
        with _lock:
            first_failure = circuit.failed_until == 0
            circuit.failed_until = time.monotonic() + COOLDOWN_SECONDS
        if first_failure and logger:
            logger("行情直连网络故障，使用服务器中转；30 秒后探测直连恢复。")
        return relay(path, query)
    else:
        with _lock:
            recovered = circuit.failed_until > 0
            circuit.failed_until = 0
        if recovered and logger:
            logger("行情直连已恢复，切回原行情通道。")
        return payload
    finally:
        if probe:
            with _lock:
                circuit.probing = False
