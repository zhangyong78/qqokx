from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from okx_quant.models import Credentials, StrategyConfig

if TYPE_CHECKING:
    from okx_quant.engine import StrategyEngine


class EngineSessionRunner:
    def __init__(self, engine: StrategyEngine) -> None:
        self._engine = engine

    @property
    def is_running(self) -> bool:
        engine = self._engine
        with engine._lock:
            return engine._thread is not None and engine._thread.is_alive()

    def start(self, credentials: Credentials, config: StrategyConfig) -> None:
        engine = self._engine
        with engine._lock:
            if engine._thread is not None and engine._thread.is_alive():
                raise RuntimeError("策略已经在运行中")
            engine._stop_event.clear()
            resolved_profile_name = credentials.profile_name.strip()
            if resolved_profile_name:
                engine._api_name = resolved_profile_name
            engine._thread = threading.Thread(
                target=engine._run,
                args=(credentials, config),
                daemon=True,
                name=f"okx-{config.strategy_id}",
            )
            engine._thread.start()

    def stop(self) -> None:
        self._engine._stop_event.set()

    def wait_stopped(self, timeout: float | None = None) -> bool:
        engine = self._engine
        with engine._lock:
            thread = engine._thread
        if thread is None:
            return True
        thread.join(timeout=timeout)
        return not thread.is_alive()
