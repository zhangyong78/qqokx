"""Local report loading off the GUI thread; never performs trading operations."""
from __future__ import annotations

from PySide6.QtCore import QObject, QRunnable, Signal

from okx_quant.persistence import load_account_equity_curve_records
from okx_quant.trade_analytics import position_history_trade
from roll_terminal_qt.history_service import load_local_fill_history, load_local_position_history_all
from roll_terminal_qt.bill_history_service import load_local_account_bills, load_local_asset_bills


class ReportLoadSignals(QObject):
    finished = Signal(int, object)


class ReportLoadTask(QRunnable):
    def __init__(self, generation: int, profile: str, environment: str, prices: dict):
        super().__init__()
        self.signals = ReportLoadSignals()
        self.generation, self.profile, self.environment, self.prices = generation, profile, environment, prices

    def run(self):
        try:
            items = load_local_position_history_all(self.profile, self.environment, persist_collapsed=False) if self.profile else []
            trades = [position_history_trade(item, profile_name=self.profile, environment=self.environment,
                                             usdt_prices=self.prices) for item in items]
            records = load_account_equity_curve_records(self.profile, self.environment) if self.profile else []
            fills = load_local_fill_history(self.profile, self.environment) if self.profile else []
            bills = load_local_account_bills(self.profile, self.environment) if self.profile else []
            asset_bills = load_local_asset_bills(self.profile, self.environment) if self.profile else []
            result = {"trades": trades, "items": items, "equity": records,
                      "fills": fills, "bills": bills, "asset_bills": asset_bills}
        except Exception as exc:
            result = exc
        self.signals.finished.emit(self.generation, result)
