from __future__ import annotations

from PySide6.QtCore import QThread, Signal

from okx_quant.arbitrage_ui import _roll_target_future_candidates
from okx_quant.okx_client import OkxRestClient


class TargetInstrumentThread(QThread):
    targets_ready = Signal(str, object)
    status_changed = Signal(str)

    def __init__(self, current_inst_id: str) -> None:
        super().__init__()
        self._current_inst_id = current_inst_id.strip().upper()
        self._client = OkxRestClient()

    def run(self) -> None:
        if not self._current_inst_id:
            self.targets_ready.emit("", [])
            return
        try:
            instruments = self._client.get_instruments("FUTURES", prefer_cached=True)
            if not instruments:
                instruments = self._client.get_instruments("FUTURES")
            targets = _roll_target_future_candidates(self._current_inst_id, instruments)
            self.targets_ready.emit(self._current_inst_id, targets)
            self.status_changed.emit(f"目标候选 {len(targets)} 个")
        except Exception as exc:  # noqa: BLE001
            self.status_changed.emit(f"目标候选读取异常：{exc}")
            self.targets_ready.emit(self._current_inst_id, [])

