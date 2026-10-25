"""Shared, restart-applied proxy settings for REST and WebSocket traffic."""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from okx_quant.app_paths import config_dir_path
from okx_quant.persistence import _decrypt_credential_text, _encrypt_credential_text, _write_json_atomic


PROXY_MODE_OPTIONS = {
    "不代理": "direct",
    "跟随系统代理": "system",
    "自定义 HTTP 代理地址": "custom",
}
PROXY_RESTART_NOTE = "所有 API 配置共用；保存后重启所有 QQOKX 窗口生效。"


@dataclass(frozen=True)
class NetworkProxySettings:
    mode: str = "system"
    url: str = ""


_runtime_lock = threading.RLock()
_runtime_settings: dict[Path, NetworkProxySettings | ValueError] = {}


def network_proxy_file_path() -> Path:
    return config_dir_path() / "network_proxy.json"


def validate_network_proxy_settings(mode: str, url: str) -> NetworkProxySettings:
    if mode not in PROXY_MODE_OPTIONS.values():
        raise ValueError("请选择有效的代理模式。")
    url = url.strip()
    if mode != "custom":
        return NetworkProxySettings(mode, "")
    try:
        parsed = urlsplit(url)
        valid = (
            parsed.scheme == "http" and parsed.hostname and parsed.port is not None
            and 1 <= parsed.port <= 65535 and parsed.path in ("", "/")
            and not parsed.query and not parsed.fragment
            and (parsed.username is None or parsed.password is not None)
            and not any(char.isspace() or ord(char) < 32 for char in url)
            and "\\" not in url and "%" not in parsed.netloc.rsplit("@", 1)[-1]
        )
    except ValueError:
        valid = False
    if not valid:
        raise ValueError("请输入有效的 HTTP 代理地址，例如 http://127.0.0.1:10808。")
    return NetworkProxySettings(mode, url.rstrip("/"))


def load_network_proxy_settings() -> NetworkProxySettings:
    try:
        payload = json.loads(network_proxy_file_path().read_text(encoding="utf-8"))
    except FileNotFoundError:
        return NetworkProxySettings()
    if not isinstance(payload, dict):
        raise ValueError("网络代理设置文件格式无效。")
    return validate_network_proxy_settings(
        str(payload.get("mode", "system")), _decrypt_credential_text(payload.get("url", "")),
    )


def runtime_network_proxy_settings() -> NetworkProxySettings:
    """Freeze one configuration per process/data directory until restart."""
    path = network_proxy_file_path()
    with _runtime_lock:
        if path not in _runtime_settings:
            try:
                _runtime_settings[path] = load_network_proxy_settings()
            except (OSError, ValueError):
                _runtime_settings[path] = ValueError("网络代理设置读取失败，请修正设置并重启 QQOKX。")
        settings = _runtime_settings[path]
        if isinstance(settings, ValueError):
            raise ValueError(str(settings)) from None
        return settings


def save_network_proxy_settings(mode: str, url: str) -> NetworkProxySettings:
    settings = validate_network_proxy_settings(mode, url)
    with _runtime_lock:
        # Saving before the first request must still leave the running process
        # on its old configuration. A repaired file takes effect after restart.
        try:
            runtime_network_proxy_settings()
        except ValueError:
            pass
        _write_json_atomic(network_proxy_file_path(), {
            "mode": settings.mode,
            "url": _encrypt_credential_text(settings.url),
        })
    return settings
