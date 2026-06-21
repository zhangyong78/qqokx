from __future__ import annotations

import queue
from tkinter import Tk

from okx_quant.arbitrage.models import ArbitrageTradeRuntime
from okx_quant.arbitrage_ui import ArbitrageWindow
from okx_quant.models import Credentials
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import load_credentials_profiles_snapshot, load_credentials_snapshot, load_notification_snapshot
from okx_quant.window_layout import apply_window_icon


def _environment_from_settings(label: object, fallback: str = "demo") -> str:
    text = str(label or "").strip().lower()
    if text.endswith("live") or "live" in text:
        return "live"
    if text.endswith("demo") or "demo" in text:
        return "demo"
    return fallback


def _trade_mode_from_settings(label: object) -> str:
    text = str(label or "").strip().lower()
    return "isolated" if "isolated" in text else "cross"


def _position_mode_from_settings(label: object) -> str:
    text = str(label or "").strip().lower()
    return "long_short" if "long/short" in text or "long_short" in text else "net"


class ArbitrageFastApp:
    def __init__(self) -> None:
        self.root = Tk()
        self.root.withdraw()
        apply_window_icon(self.root)
        self.client = OkxRestClient(logger=self._enqueue_log)
        self._log_queue: queue.Queue[str] = queue.Queue()
        self._selected_profile_name = self._load_selected_profile_name()
        self.window = ArbitrageWindow(
            self.root,
            self.client,
            runtime_config_provider=self._build_runtime_or_none,
            logger=self._enqueue_log,
            fast_mode=True,
        )
        self.window.window.protocol("WM_DELETE_WINDOW", self._on_close)

    def _enqueue_log(self, message: str) -> None:
        try:
            self._log_queue.put_nowait(message)
        except Exception:
            pass

    @staticmethod
    def _load_selected_profile_name() -> str:
        snapshot = load_credentials_profiles_snapshot()
        return str(snapshot.get("selected_profile", "") or "").strip()

    def _build_runtime_or_none(self) -> ArbitrageTradeRuntime | None:
        profile_name = self._selected_profile_name
        credentials_payload = load_credentials_snapshot(profile_name=profile_name or None)
        api_key = credentials_payload.get("api_key", "").strip()
        secret_key = credentials_payload.get("secret_key", "").strip()
        passphrase = credentials_payload.get("passphrase", "").strip()
        if not api_key or not secret_key or not passphrase:
            return None
        notification = load_notification_snapshot()
        profile_environment = str(credentials_payload.get("environment", "") or "").strip().lower()
        environment = profile_environment if profile_environment in {"demo", "live"} else _environment_from_settings(
            notification.get("environment_label"),
        )
        return ArbitrageTradeRuntime(
            credentials=Credentials(
                api_key=api_key,
                secret_key=secret_key,
                passphrase=passphrase,
                profile_name=profile_name,
            ),
            environment=environment,
            trade_mode=_trade_mode_from_settings(notification.get("trade_mode_label")),
            position_mode=_position_mode_from_settings(notification.get("position_mode_label")),
            credential_profile_name=profile_name,
        )

    def _on_close(self) -> None:
        self.window._on_close()
        self.root.after(0, self.root.destroy)


def run_app() -> None:
    app = ArbitrageFastApp()
    app.root.mainloop()
