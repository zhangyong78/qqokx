from dataclasses import replace
from decimal import Decimal as D
from types import SimpleNamespace as NS
from unittest.mock import Mock, patch

import pytest

from okx_quant.area_shape_execution_monitor import AreaShapeDemoExecutionMonitor
from okx_quant.area_shape_trade import AreaShapeConfig, AreaShapePlan
from okx_quant.kline_rr_execution import RRTradeExecutionService
from okx_quant.kline_rr_trade import RRTradeLedgerEntry, RRTradeOrderLink, build_rr_trade_plan
from roll_terminal_qt.kline_account_drawer import AccountDrawerChaseThread, KlineAccountDrawer
from roll_terminal_qt.kline_analysis_window import KlineAnalysisWindow
from tests.qt_test_case import QtWidgetTestCase
from tests.test_pending_order_chase import setup, start


def test_rr_adopts_authorized_price_size_and_does_not_cancel_replace(setup):
    registry, client, runtime = setup
    key = start(setup, {"owner": "rr", "risk_amount": 100, "stop_on_fill": True})
    registry.step(client=client, runtime=runtime, key=key)
    plan = build_rr_trade_plan(plan_id="rr1", profile_name="test", environment="demo", instrument=client.instrument,
        direction="long", entry_execution_mode="chase_best_quote", management_mode="fixed_tp", trigger_price_type="last",
        risk_amount=D(100), entry_price=D(100), stop_loss_price=D(90), direct_take_profit_r=D(2), round_trip_fee_rate=D(0))
    entry = RRTradeLedgerEntry(entry_id="rr1", status="entry_working", plan=plan, remaining_size=D(10),
        entry_order=RRTradeOrderLink(role="entry", channel="order", order_id="123", client_id="entry1", price=D(100), size=D(10)))
    service = RRTradeExecutionService()
    with patch("okx_quant.kline_rr_execution.get_pending_order_chase_service", return_value=registry):
        updated = service._reconcile_status(client=client, credentials=runtime.credentials, config=NS(position_mode="net"),
                                             entry=entry, exchange_status=client.status)
    assert updated.plan.entry_execution_mode == "limit"
    assert updated.entry_order.price == 102 and updated.entry_order.size == D("8.3")
    assert updated.plan.entry_price == 102
    assert updated.plan.stop_loss_price == 90 and updated.plan.take_profit_price == plan.take_profit_price
    assert len(client.amendments) == 1


def test_area_adopts_only_known_amend_and_keeps_original_r_reference(setup):
    registry, client, runtime = setup
    key = start(setup, {"owner": "area", "risk_amount": 100, "stop_on_fill": True})
    registry.step(client=client, runtime=runtime, key=key)
    monitor = AreaShapeDemoExecutionMonitor(store_load=lambda: [], store_save=lambda _: None)
    intent = dict(price="100", size="10", stop="90", entry_reference="100", take_profit_price="120", cl_ord_id="entry1")
    session = dict(symbol="BTC-USDT-SWAP", account_uid="uid", events=[], resting_entry_intent=intent)
    with patch("okx_quant.area_shape_execution_monitor.get_pending_order_chase_service", return_value=registry), patch.object(monitor, "_persist_locked") as save:
        monitor._adopt_authorized_entry_amendment(session, intent, client.status, runtime)
        assert intent["price"] == "102" and intent["size"] == "8.3"
        assert intent["entry_reference"] == "100" and intent["take_profit_price"] == "120"
        save.assert_called_once()
        unknown = replace(client.status, price=D(101))
        monitor._adopt_authorized_entry_amendment(session, intent, unknown, runtime)
        assert intent["price"] == "102"


def test_context_provider_matches_rr_and_blocks_owner_in_flight(setup):
    _, client, runtime = setup
    plan = build_rr_trade_plan(plan_id="rr1", profile_name="test", environment="demo", instrument=client.instrument,
        direction="long", entry_execution_mode="limit", management_mode="fixed_tp", trigger_price_type="last",
        risk_amount=D(100), entry_price=D(100), stop_loss_price=D(90), direct_take_profit_r=D(2), round_trip_fee_rate=D(0))
    entry = RRTradeLedgerEntry(entry_id="rr1", status="entry_working", plan=plan,
        entry_order=RRTradeOrderLink(role="entry", channel="order", order_id="123", client_id="entry1"))
    root = NS(_runtime=runtime, _account_drawer=NS(_profile_name="test", _environment="demo", _runtime=runtime),
        _active_profile_name=lambda: "test", _active_environment=lambda: "demo", _ensure_profile_access=lambda _: True,
        _all_rr_trade_ledger_entries=lambda: [entry], _rr_execution_in_flight=False)
    result = KlineAnalysisWindow._current_order_chase_context(root, client.row())
    assert result["owner"] == "rr" and result["risk_amount"] == 100
    root._rr_execution_in_flight = True
    with pytest.raises(ValueError, match="正在核对"):
        KlineAnalysisWindow._current_order_chase_context(root, client.row())


def test_context_provider_freezes_area_opposite_line_and_minimum_r(setup):
    _, client, runtime = setup
    plan = AreaShapePlan.create(profile_name="test", environment="demo", symbol="BTC-USDT-SWAP",
        support_price=D(100), pressure_price=D(130), config=AreaShapeConfig(direction="long", risk_amount=D(100)))
    session = dict(profile_name="test", environment="demo", status="armed", plan_snapshot=plan.to_dict(),
        resting_entry_intent=dict(cl_ord_id="entry1", side="buy", stop="90", take_profit_price="120"))
    root = NS(_runtime=runtime, _account_drawer=NS(_profile_name="test", _environment="demo", _runtime=runtime),
        _active_profile_name=lambda: "test", _active_environment=lambda: "demo", _ensure_profile_access=lambda _: True,
        _all_rr_trade_ledger_entries=lambda: [], _area_shape_execution_monitor=NS(sessions=lambda: [session]))
    result = KlineAnalysisWindow._current_order_chase_context(root, client.row())
    assert result["owner"] == "area" and result["target"] == 130 and result["min_space_r"] >= 2
    session["paused"] = True
    with pytest.raises(ValueError, match="暂停"):
        KlineAnalysisWindow._current_order_chase_context(root, client.row())


def test_unmanaged_order_can_start_without_area_monitor(setup):
    _, client, runtime = setup
    root = NS(_runtime=runtime, _account_drawer=NS(_profile_name="test", _environment="demo", _runtime=runtime),
        _active_profile_name=lambda: "test", _active_environment=lambda: "demo", _ensure_profile_access=lambda _: True,
        _all_rr_trade_ledger_entries=lambda: [], _area_shape_execution_monitor=None)
    assert KlineAnalysisWindow._current_order_chase_context(root, client.row()) == {}


class ChaseUiTests(QtWidgetTestCase):
    def test_current_orders_chase_buttons_and_algorithm_explanation(self):
        drawer = KlineAccountDrawer()
        try:
            drawer._runtime = object()
            order = NS(inst_id="BTC-USDT-SWAP", order_id="123", ord_type="limit", state="live", side="buy")
            drawer._populate_orders_table([order])
            drawer._orders_table.selectRow(0)
            drawer._sync_chase_button_state()
            self.assertTrue(drawer._chase_button.isEnabled())
            self.assertFalse(drawer._stop_chase_button.isEnabled())
            drawer._populate_orders_table([NS(inst_id="BTC-USDT-SWAP", order_id="", algo_id="algo", ord_type="oco", state="live", side="sell")])
            drawer._orders_table.selectRow(0)
            drawer._sync_chase_button_state()
            self.assertFalse(drawer._chase_button.isEnabled())
            self.assertIn("不追改触发价", drawer._chase_button.toolTip())
        finally:
            self.dispose_widget(drawer)

    def test_refresh_retains_selected_order_after_row_reorder(self):
        drawer = KlineAccountDrawer()
        try:
            a = NS(inst_id="BTC-USDT-SWAP", order_id="123", ord_type="limit", state="live", side="buy")
            b = NS(inst_id="ETH-USDT-SWAP", order_id="456", ord_type="limit", state="live", side="sell")
            drawer._populate_orders_table([a, b])
            drawer._orders_table.selectRow(0)
            drawer._populate_orders_table([b, a])
            self.assertEqual(drawer._selected_order().order_id, "123")
        finally:
            self.dispose_widget(drawer)

    def test_running_chase_stops_on_account_switch_and_shutdown(self):
        drawer = KlineAccountDrawer()
        worker = Mock()
        worker.isRunning.return_value = True
        worker.wait.return_value = True
        worker.key, worker.order_id, worker.profile_name, worker.environment = "", "123", "test", "demo"
        try:
            drawer._runtime, drawer._profile_name, drawer._environment = object(), "test", "demo"
            drawer._chase_thread = worker
            self.assertTrue(drawer.blocks_order_reconcile("123", "test", "demo"))
            drawer.set_context(runtime=None, profile_name="other", environment="demo", symbol="BTC-USDT-SWAP", refresh_if_visible=False)
            self.assertTrue(worker.stop.called)
            worker.stop.reset_mock()
            self.assertTrue(drawer.shutdown())
            worker.stop.assert_called_once()
        finally:
            drawer._chase_thread = None
            self.dispose_widget(drawer)

    def test_worker_hands_partial_fill_back_without_price_write(self):
        from tempfile import TemporaryDirectory
        from pathlib import Path
        from tests.test_pending_order_chase import Client
        from okx_quant.models import Credentials
        from okx_quant.pending_order_chase import PendingOrderChaseService
        with TemporaryDirectory() as directory:
            service = PendingOrderChaseService(Path(directory)/"chase.json")
            client = Client()
            client.close = Mock()
            client.status = replace(client.status, state="partially_filled", filled_size=D(2), avg_price=D(100))
            runtime = NS(credentials=Credentials("key", "secret", "pass"), environment="demo", credential_profile_name="test")
            thread = AccountDrawerChaseThread(runtime=runtime, order=client.row(), context={"owner": "area", "stop_on_fill": True},
                                              interval=1, service=service, client_factory=lambda: client)
            failures = []
            thread.failed.connect(failures.append)
            thread.run()
            self.assertFalse(failures)
            self.assertFalse(service.snapshot(thread.key)["active"])
            self.assertFalse(client.amendments)
            client.close.assert_called_once()
            thread.deleteLater()
