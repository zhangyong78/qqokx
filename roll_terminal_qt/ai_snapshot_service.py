from __future__ import annotations

from typing import Any, Iterable

from PySide6.QtCore import QThread, Signal

from okx_quant.ai_snapshot import AISnapshotBuilder
from okx_quant.arbitrage.models import ArbitrageTradeRuntime


class AISnapshotWorker(QThread):
    progress = Signal(str)
    succeeded = Signal(object)
    failed = Signal(str)

    def __init__(self, runtime: ArbitrageTradeRuntime, *, profile_name: str, watchlist: Iterable[str]) -> None:
        super().__init__()
        self._runtime = runtime
        self._profile_name = profile_name
        self._watchlist = tuple(watchlist)

    def run(self) -> None:
        try:
            builder = AISnapshotBuilder(progress_callback=self.progress.emit)
            payload = builder.build(
                credentials=self._runtime.credentials,
                profile_name=self._profile_name,
                environment=str(self._runtime.environment),
                watchlist=self._watchlist,
            )
        except Exception as exc:  # worker boundary: surface a readable error in the UI
            self.failed.emit(str(exc) or exc.__class__.__name__)
            return
        self.succeeded.emit(payload)

