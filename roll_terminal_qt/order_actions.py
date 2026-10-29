"""Shared actions used by both current-order views."""
from types import SimpleNamespace
from decimal import Decimal

from PySide6.QtCore import QObject, QThread, QTimer, Signal
from PySide6.QtWidgets import QPushButton, QSpinBox, QLabel, QMessageBox, QDialog

from okx_quant.okx_client import OkxRestClient
from okx_quant.manual_order_protection import prepare_break_even, amend_protection, matched_position, break_even_price, cached_fee


class _Task(QThread):
    completed = Signal(object)
    failed = Signal(str)

    def __init__(self, action, parent):
        super().__init__(parent)
        self.action = action

    def run(self):
        client = OkxRestClient()
        try:
            self.completed.emit(self.action(client))
        except Exception as exc:
            self.failed.emit(str(exc))
        finally:
            client.close()


class OrderActions(QObject):
    def __init__(self, parent, runtime_provider, selection_provider, refresh, context_provider=None):
        super().__init__(parent)
        self.view = parent
        self.runtime_provider, self.selection_provider = runtime_provider, selection_provider
        self.refresh, self.context_provider = refresh, context_provider
        self.workers = []
        self.chase_worker = None
        self.busy = False
        self.closing = False

    def add_chase_controls(self, layout):
        self.start_button = QPushButton("自动追单")
        self.stop_button = QPushButton("停止追单")
        self.interval = QSpinBox()
        self.interval.setRange(1, 60)
        self.interval.setValue(2)
        self.start_button.clicked.connect(self.start_chase)
        self.stop_button.clicked.connect(self.stop_chase)
        layout.addWidget(self.start_button)
        layout.addWidget(self.stop_button)
        layout.addWidget(QLabel("间隔（秒）"))
        layout.addWidget(self.interval)
        self.stop_button.setEnabled(False)

    def _item(self):
        from roll_terminal_qt.order_service import OrderStatusView, order_view_to_trade_order_item
        selected = self.selection_provider()
        if selected is None:
            raise ValueError("请先选中一条当前委托")
        return order_view_to_trade_order_item(selected) if isinstance(selected, OrderStatusView) else selected

    def _run(self, action, completed, failed=None):
        worker = _Task(action, self)
        self.workers.append(worker)
        worker.completed.connect(lambda result: completed(result) if not self.closing else None)
        worker.failed.connect(failed or self._error)
        def finished():
            self.workers.remove(worker)
            worker.deleteLater()
            self.busy = bool(self.workers)
        worker.finished.connect(finished)
        worker.start()

    def _error(self, message):
        self.busy = False
        if not self.closing:
            QMessageBox.warning(self.view, "委托操作", message)

    def edit(self):
        if self.busy or self.closing:
            return
        try:
            runtime, item = self.runtime_provider(), self._item()
            if runtime is None:
                raise ValueError("请先连接并解锁账户")
            if self.chase_worker is not None and self.chase_worker.isRunning():
                raise ValueError("请先停止追单再修改止盈止损")
        except Exception as exc:
            self._error(str(exc))
            return
        from roll_terminal_qt.account_positions_home import AccountPositionsHomeWidget
        from roll_terminal_qt.order_service import OrderFeedThread
        snapshot = getattr(self.view, "_snapshot", None)
        source = SimpleNamespace(
            _raw_positions=getattr(snapshot, "positions", getattr(self.view, "_raw_positions", ())),
            _position_instruments=getattr(snapshot, "position_instruments", None) or getattr(self.view, "_position_instruments", {}),
            _current_order_instruments=getattr(self.view, "_current_order_instruments", {}),
            _position_tickers=getattr(snapshot, "position_tickers", None) or getattr(self.view, "_position_tickers", {}),
        )
        feed = OrderFeedThread(None)
        try:
            view = feed._trade_order_to_view(item)
        finally:
            feed.stop()
        if not (source._position_instruments.get(item.inst_id) or source._current_order_instruments.get(item.inst_id)):
            cache_client = OkxRestClient()
            try:
                cached_instrument = cache_client._get_cached_instrument(item.inst_id)
                if cached_instrument is not None:
                    source._position_instruments = {**source._position_instruments, item.inst_id: cached_instrument}
            except Exception:
                pass
            finally:
                cache_client.close()
        context = self._preview_context(runtime, item, source, view)
        self._dialog(runtime, item, context, source=source, order_view=view)

    def _preview_context(self, runtime, item, source, view):
        from roll_terminal_qt.account_positions_home import AccountPositionsHomeWidget
        context = AccountPositionsHomeWidget._current_order_tp_sl_preview_context(source, view, item)
        try:
            from roll_terminal_qt.account_positions_home import _break_even_taker_fee_rate
            position, direction = matched_position(source._raw_positions, item)
            instrument = source._position_instruments.get(item.inst_id) or source._current_order_instruments.get(item.inst_id)
            tick = getattr(instrument, "tick_size", None)
            profile_name = str(getattr(runtime.credentials, "profile_name", "") or getattr(runtime, "credential_profile_name", ""))
            profiles = getattr(self.view, "_profile_snapshots", None)
            if profiles is None:
                provider = getattr(self.view, "profile_snapshots_provider", None)
                profiles = provider() if callable(provider) else {}
            profile = profiles.get(profile_name, {}) if isinstance(profiles, dict) else {}
            verified_fee = cached_fee(runtime, item.inst_id)
            fee = verified_fee if verified_fee is not None else _break_even_taker_fee_rate(profile, inst_type="SWAP")
            if item.source_kind != "algo" or item.ord_type not in {"oco", "conditional"} or not item.inst_id.endswith("-USDT-SWAP"):
                raise ValueError("不支持的保护单")
            if item.side != ("sell" if direction == "long" else "buy") or item.stop_loss_order_price != Decimal("-1"):
                raise ValueError("保护单方向或类型不符")
            target = break_even_price(position.avg_price, fee, tick, direction)
            context["cached_break_even"] = (target, direction, fee)
        except (ValueError, TypeError, AttributeError, ArithmeticError):
            pass
        return context

    def _dialog(self, runtime, item, context, *, source=None, order_view=None):
        from roll_terminal_qt.account_positions_home import _CurrentOrderTpSlDialog
        dialog = _CurrentOrderTpSlDialog(self.view, item, preview_context=context)
        pending = {"value": False}
        def show_cached():
            cached = context.get("cached_break_even")
            if not cached:
                return
            target, direction, fee = cached
            old = item.stop_loss_trigger_price
            if old is not None and ((direction == "long" and old >= target) or (direction == "short" and old <= target)):
                dialog._break_even_note.setText(f"预估保本价 {target}；当前止损已达到或优于保本。提交时以交易所数据为准。")
            else:
                dialog._break_even_note.setText(f"预估保本价 {target}（双向按每侧 {fee * 100}% 估算）；提交时核对交易所费率及当前价格。")
        show_cached()
        def prepare():
            cached = context.get("cached_break_even")
            if cached:
                target, direction, fee = cached
                old = item.stop_loss_trigger_price
                if old is not None and ((direction == "long" and old >= target) or (direction == "short" and old <= target)):
                    dialog._break_even_note.setText(f"当前止损已达到或优于预估保本价 {target}，无需降低保护。")
                    return
                dialog._stop_loss_edit.setText(str(target))
                dialog._take_profit_edit.clear()
                dialog._break_even_requested = True
                dialog._break_even_note.setText(f"已填入预估保本价 {target}；提交时核对交易所手续费、价格和保护单。")
                return
            if pending["value"]:
                return
            pending["value"] = True
            dialog._break_even_button.setEnabled(False)
            from PySide6.QtWidgets import QDialogButtonBox
            submit = dialog.findChild(QDialogButtonBox).button(QDialogButtonBox.StandardButton.Ok)
            submit.setEnabled(False)
            def ready(data):
                _current, target, direction, fee = data
                dialog._stop_loss_edit.setText(str(target))
                dialog._take_profit_edit.clear()
                dialog._break_even_requested = True
                submit.setEnabled(True)
                dialog._break_even_note.setText(f"已计算{'多头' if direction == 'long' else '空头'}保本价 {target}；双向按每侧 {fee * 100}% Taker费率估算，不含滑点及资金费。提交前再次检查。")
            def failed(message):
                pending["value"] = False
                dialog._break_even_button.setEnabled(True)
                submit.setEnabled(True)
                self._error(message)
            self._run(lambda client: prepare_break_even(client, runtime, item), ready, failed)
        dialog._break_even_button.clicked.connect(prepare)
        if source is not None and not (source._position_instruments.get(item.inst_id) or source._current_order_instruments.get(item.inst_id)):
            def load_instrument():
                if not dialog.isVisible() or self.closing:
                    return
                def ready(instrument):
                    if not dialog.isVisible() or self.closing:
                        return
                    source._position_instruments = {**source._position_instruments, item.inst_id: instrument}
                    context.update(self._preview_context(runtime, item, source, order_view))
                    dialog._preview_context = dict(context)
                    dialog._quantity_value.setText(str(context.get("quantity_text") or "暂无"))
                    dialog._holding_value.setText(str(context.get("holding_text") or "暂无"))
                    dialog._refresh_preview()
                    if not dialog._break_even_requested:
                        show_cached()
                self._run(lambda client: client.get_instrument(item.inst_id, prefer_cached=True), ready,
                          lambda _message: None)
            QTimer.singleShot(0, dialog, load_instrument)
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return
        try:
            tp, sl = dialog.values()
            parse = lambda text: Decimal(text) if text else None
            tp, sl = parse(tp), parse(sl)
            if not dialog._break_even_requested:
                tp = None if tp == item.take_profit_trigger_price else tp
                sl = None if sl == item.stop_loss_trigger_price else sl
        except Exception:
            self._error("请输入有效价格")
            return
        self.busy = True
        self._run(lambda client: amend_protection(client, runtime, item, tp, sl,
            break_even=dialog._break_even_requested), lambda result: self._amended(runtime, result))

    def _amended(self, runtime, result):
        from roll_terminal_qt.order_service import OrderFeedThread
        from roll_terminal_qt.shared_order_store import get_shared_order_store
        feed = OrderFeedThread(None)
        try:
            view = feed._trade_order_to_view(result)
        finally:
            feed.stop()
        profile = str(getattr(runtime.credentials, "profile_name", "") or getattr(runtime, "credential_profile_name", ""))
        get_shared_order_store().publish_order_updates(profile_name=profile, environment=runtime.environment, orders=[view])
        self.refresh()
        QMessageBox.information(self.view, "修改完成", "交易所回读已确认止盈止损修改生效")

    def start_chase(self):
        if self.closing or self.busy or (self.chase_worker is not None and self.chase_worker.isRunning()):
            return
        try:
            runtime, item = self.runtime_provider(), self._item()
            if runtime is None:
                raise ValueError("请先连接并解锁账户")
            context = self.context_provider(item) if self.context_provider else {}
            from roll_terminal_qt.kline_account_drawer import AccountDrawerChaseThread
            worker = AccountDrawerChaseThread(runtime=runtime, order=item, context=context, interval=self.interval.value())
            self.chase_worker = worker
            worker.updated.connect(lambda state: self.start_button.setToolTip(str(state.get("message", ""))))
            worker.failed.connect(self._error)
            worker.finished.connect(self._chase_finished)
            self.start_button.setEnabled(False)
            self.stop_button.setEnabled(True)
            worker.start()
        except Exception as exc:
            self._error(str(exc))

    def stop_chase(self):
        if self.chase_worker is not None:
            self.chase_worker.stop()

    def _chase_finished(self):
        self.chase_worker.deleteLater()
        self.chase_worker = None
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        if not self.closing:
            self.refresh()

    def shutdown(self):
        self.closing = True
        self.stop_chase()
        return [w for w in [*self.workers, self.chase_worker] if w is not None and w.isRunning()]
