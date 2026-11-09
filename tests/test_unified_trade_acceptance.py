"""Supervisor acceptance: durable notification identity and delivery boundaries.

No exchange or SMTP calls: senders are injected in every delivery test.
"""
from concurrent.futures import ThreadPoolExecutor
import threading
from decimal import Decimal
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch
from hashlib import sha256

import pytest

from okx_quant.trade_notifications import (
    TradeNotificationQueue, merged_recipients, normalize_plan_notification_events,
    start_background_worker, stop_background_worker,
)
from okx_quant.kline_rr_trade import RRTradeLedgerEntry, RRTradeOrderLink, build_rr_trade_plan
from okx_quant.kline_rr_execution import RRTradeExecutionService
from okx_quant.models import Instrument
from okx_quant.models import EmailNotificationConfig
from okx_quant.notifications import EmailNotifier
from okx_quant.okx_client import OkxApiError
import okx_quant.kline_rr_execution as rr_execution
from okx_quant.area_shape_trade import AreaShapeConfig, AreaShapePlan
from roll_terminal_qt.kline_alerts import make_line_rule, normalize_workspace_entry

TEST_API_KEY = "offline-acceptance-api-key"
TEST_API_FINGERPRINT = sha256(TEST_API_KEY.encode("utf-8")).hexdigest()


def _enqueue(queue, event_id="api1:demo:BTC:plan1:order1:filled", kind="filled"):
    return queue.enqueue(
        event_id=event_id, plan_id="api1:demo:BTC:plan1", kind=kind,
        payload={"profile_name": "api1", "environment": "demo", "order_id": "order1"},
        recipients=["trader@example.com"],
    )


def _deliver(queue, sender, **switches):
    options = dict(enabled=True, notify_signals=True, notify_trade_fills=True, notify_errors=True)
    options.update(switches)
    return queue.deliver_pending(sender=sender, **options)


def _record(sent):
    def sender(event):
        sent.append(event)
        return True
    return sender


def test_repeated_fill_event_stays_sent_after_queue_restart(tmp_path):
    path = tmp_path / "events.json"
    sent = []
    first = TradeNotificationQueue(path=path)
    _enqueue(first)
    assert _deliver(first, _record(sent)) == 1
    restored = TradeNotificationQueue(path=path)
    row = _enqueue(restored)
    assert row["status"] == "sent"
    assert _deliver(restored, _record(sent)) == 0
    assert len(sent) == 1


def test_distinct_accounts_and_environments_do_not_share_deduplication(tmp_path):
    queue = TradeNotificationQueue(path=tmp_path / "events.json")
    sent = []
    for identity in ("api1:demo", "api1:live", "api2:demo"):
        _enqueue(queue, event_id=f"{identity}:BTC:plan1:order1:filled")
    assert _deliver(queue, _record(sent)) == 3


def test_sender_failure_is_not_success_and_explicit_retry_survives_restart(tmp_path):
    path = tmp_path / "events.json"
    queue = TradeNotificationQueue(path=path)
    _enqueue(queue)

    def unavailable(_event):
        raise ConnectionError("SMTP unavailable")

    assert _deliver(queue, unavailable) == 0
    restored = TradeNotificationQueue(path=path)
    assert _enqueue(restored)["status"] == "failed"
    assert restored.retry_failed() == 1
    sent = []
    assert _deliver(restored, _record(sent)) == 1
    assert len(sent) == 1
    assert _enqueue(restored)["status"] == "sent"


def test_corrupt_notification_store_is_preserved_and_not_replaced(tmp_path):
    path = tmp_path / "events.json"
    original = b'{"broken":'
    path.write_bytes(original)
    queue = TradeNotificationQueue(path=path)
    with pytest.raises((ValueError, RuntimeError)):
        _enqueue(queue)
    assert path.read_bytes() == original


def test_recipient_merge_supports_all_existing_separators_and_case_insensitive_dedup():
    assert merged_recipients(
        "A@example.com\nb@example.com; c@example.com",
        "a@EXAMPLE.com,d@example.com\r\nb@example.com",
    ) == ["A@example.com", "b@example.com", "c@example.com", "d@example.com"]


@pytest.mark.parametrize("content", ['{}', '[{}]', '["broken"]'])
def test_unknown_notification_schema_does_not_silently_reset_history(tmp_path, content):
    path = tmp_path / "events.json"
    path.write_text(content, encoding="utf-8")
    with pytest.raises((ValueError, RuntimeError)):
        _enqueue(TradeNotificationQueue(path=path))
    assert path.read_text(encoding="utf-8") == content


def test_global_categories_gate_the_matching_event_and_do_not_replay_disabled_history(tmp_path):
    queue = TradeNotificationQueue(path=tmp_path / "events.json")
    for kind in ("signal", "accepted", "filled", "error"):
        _enqueue(queue, event_id=f"plan1:order1:{kind}", kind=kind)
    sent = []
    assert _deliver(queue, _record(sent), notify_signals=False, notify_errors=False) == 1
    assert [row["kind"] for row in sent] == ["filled"]
    assert _deliver(queue, _record(sent)) == 0


def test_async_submission_without_confirmed_smtp_success_is_not_marked_sent(tmp_path):
    queue = TradeNotificationQueue(path=tmp_path / "events.json")
    _enqueue(queue)
    assert _deliver(queue, lambda _row: None) == 0
    assert queue.rows()[0]["status"] == "failed"


@pytest.mark.parametrize("legacy_event", ["signal", "accepted", "filled"])
def test_old_enabled_switch_preserves_original_event_only(legacy_event):
    result = normalize_plan_notification_events(None, legacy_enabled=True, legacy_event=legacy_event)
    assert result == {kind: kind == legacy_event for kind in ("signal", "accepted", "filled", "error")}


@pytest.mark.parametrize("legacy_enabled", [None, False])
def test_old_disabled_or_missing_switch_never_acquires_new_default(legacy_enabled):
    assert not any(normalize_plan_notification_events(
        None, legacy_enabled=legacy_enabled, legacy_event="accepted",
    ).values())


def test_new_plan_defaults_to_first_fill_and_error_but_explicit_settings_win():
    assert normalize_plan_notification_events(None, is_new_plan=True) == {
        "signal": False, "accepted": False, "filled": True, "error": True,
    }
    assert normalize_plan_notification_events(
        {"signal": True, "accepted": False, "filled": False, "error": False}, is_new_plan=True,
    ) == {"signal": True, "accepted": False, "filled": False, "error": False}


def test_disabled_plan_event_stays_suppressed_with_global_mail_enabled(tmp_path):
    queue = TradeNotificationQueue(path=tmp_path / "events.json")
    row = queue.enqueue(event_id="plan1:filled", plan_id="plan1", kind="filled", payload={}, eligible=False)
    assert row["status"] == "suppressed"
    sent = []
    assert _deliver(queue, _record(sent)) == 0
    assert queue.retry_failed() == 0
    assert sent == []


def test_two_workers_cannot_send_the_same_event_twice(tmp_path):
    path = tmp_path / "events.json"
    _enqueue(TradeNotificationQueue(path=path))
    sent = []
    started, release = threading.Event(), threading.Event()

    def blocked_sender(event):
        sent.append(event)
        started.set()
        assert release.wait(5)
        return True

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(_deliver, TradeNotificationQueue(path=path), blocked_sender)
        try:
            assert started.wait(2)
            second = pool.submit(_deliver, TradeNotificationQueue(path=path), _record(sent))
            second_result = second.result(timeout=2)
        finally:
            release.set()
        results = [first.result(timeout=5), second_result]
    assert sum(results) == 1
    assert len(sent) == 1


def test_slow_mail_delivery_does_not_block_other_event_persistence(tmp_path):
    queue = TradeNotificationQueue(path=tmp_path / "events.json")
    _enqueue(queue)
    started, release, persisted = threading.Event(), threading.Event(), threading.Event()

    def slow_sender(_event):
        started.set()
        assert release.wait(5), "acceptance sender was not released"
        return True

    def append_other_event():
        _enqueue(queue, event_id="api2:demo:BTC:plan2:order2:filled")
        persisted.set()

    with ThreadPoolExecutor(max_workers=2) as pool:
        delivery = pool.submit(_deliver, queue, slow_sender)
        try:
            assert started.wait(2)
            enqueue = pool.submit(append_other_event)
            assert persisted.wait(1), "SMTP blocked enqueue / foreground persistence"
        finally:
            release.set()
        delivery.result(timeout=5)
        enqueue.result(timeout=5)


def _bound_rr_entry():
    instrument = Instrument(
        inst_id="BTC-USDT-SWAP", inst_type="SWAP", tick_size=Decimal("0.1"),
        lot_size=Decimal("1"), min_size=Decimal("1"), state="live", settle_ccy="USDT",
        ct_val=Decimal("0.01"), ct_mult=Decimal("1"), ct_val_ccy="BTC",
        uly="BTC-USDT", inst_family="BTC-USDT",
    )
    plan = build_rr_trade_plan(
        plan_id="api1:BTC:rr1", profile_name="api1", environment="demo", instrument=instrument,
        direction="long", entry_execution_mode="limit", management_mode="fixed_tp",
        trigger_price_type="last", risk_amount=Decimal("100"), entry_price=Decimal("60000"),
        stop_loss_price=Decimal("59000"), direct_take_profit_r=Decimal("2"), round_trip_fee_rate=Decimal("0"),
        api_key_fingerprint=TEST_API_FINGERPRINT,
    )
    return RRTradeLedgerEntry(
        entry_id=plan.plan_id, status="entry_working", plan=plan,
        entry_order=RRTradeOrderLink(role="entry", channel="order", order_id="wanted", client_id="client1"),
    )


@pytest.mark.parametrize("wrong_field,wrong_value", [
    ("instId", "ETH-USDT-SWAP"), ("ordId", "foreign-order"), ("clOrdId", "foreign-client"),
])
def test_rr_foreign_order_readback_never_becomes_a_fill(wrong_field, wrong_value):
    entry = _bound_rr_entry()
    raw = {"instId": "BTC-USDT-SWAP", "ordId": "wanted", "clOrdId": "client1"}
    raw[wrong_field] = wrong_value
    status = SimpleNamespace(raw=raw, ord_id="wanted", client_order_id="client1", filled_size=Decimal("10"))
    service = RRTradeExecutionService()
    with patch.object(service, "_reconcile_status", side_effect=AssertionError("foreign order adopted")):
        result = service.reconcile(
            client=SimpleNamespace(get_order=lambda *_a, **_kw: status),
            credentials=SimpleNamespace(profile_name="api1", api_key=TEST_API_KEY),
            config=SimpleNamespace(environment="demo"), entry=entry,
        )
    assert result.status == "manual_review"
    assert result.filled_size == 0


def test_bound_plan_notification_policy_and_source_survive_ledger_reload():
    entry = _bound_rr_entry()
    entry = replace(entry, plan=replace(entry.plan, source="line", notification_events={
        "signal": True, "accepted": False, "filled": True, "error": False,
    }))
    restored = RRTradeLedgerEntry.from_dict(entry.to_dict())
    assert restored.plan.source == "line"
    assert restored.plan.profile_name == "api1"
    assert restored.plan.environment == "demo"
    assert restored.plan.api_key_fingerprint == TEST_API_FINGERPRINT
    assert restored.plan.notification_events == {
        "signal": True, "accepted": False, "filled": True, "error": False,
    }


@pytest.mark.parametrize("legacy_enabled", [False, True])
def test_area_config_migrates_old_checkbox_without_enabling_new_event_types(legacy_enabled):
    old = AreaShapeConfig().to_dict()
    old.pop("notification_events", None)
    old["email_enabled"] = legacy_enabled
    migrated = AreaShapeConfig.from_dict(old)
    assert migrated.notification_events == {
        "signal": False, "accepted": False, "filled": legacy_enabled, "error": False,
    }


def test_new_area_and_trade_line_have_first_fill_and_error_defaults():
    assert AreaShapeConfig().notification_events == {
        "signal": False, "accepted": False, "filled": True, "error": True,
    }
    line = make_line_rule(
        kind="horizontal", label="trade", trigger="down", action="short", trade_enabled=True,
        time_a=1, price_a=100, time_b=2, price_b=100,
    )
    assert line["notification_events"] == {
        "signal": False, "accepted": False, "filled": True, "error": True,
    }
    old = dict(line)
    old.pop("notification_events")
    old["email_enabled"] = True
    normalized = normalize_workspace_entry({"lines": [old]})["lines"][0]
    assert normalize_plan_notification_events(
        normalized.get("notification_events"), legacy_enabled=normalized["email_enabled"], legacy_event="signal",
    ) == {"signal": True, "accepted": False, "filled": False, "error": False}


def test_notification_storage_failure_cannot_interrupt_confirmed_fill_protection():
    from tests.test_area_shape_execution_monitor import _fixture

    monitor, session, client, order = _fixture(filled=Decimal("1"))
    session["plan_snapshot"]["config"]["email_enabled"] = True
    session["plan_snapshot"]["config"]["notification_events"] = {
        "signal": False, "accepted": False, "filled": True, "error": True,
    }
    client.accepted.add("entry-attached-stop")
    with patch.object(TradeNotificationQueue, "enqueue", side_effect=OSError("notification disk unavailable")):
        monitor._protect_fill(session["session_id"], order, 1_800_000_000_000)
    assert session["position"]["size"] == "1"
    assert "entry-attached-stop" in client.queried_ids


def test_synchronous_delivery_reports_smtp_success_with_explicit_api_recipients():
    notifier = EmailNotifier(EmailNotificationConfig(
        enabled=True, smtp_host="smtp.example.com", smtp_port=465,
        sender_email="sender@example.com", recipient_emails=("global@example.com",), use_ssl=True,
    ))
    with patch("okx_quant.notifications.smtplib.SMTP_SSL") as transport:
        smtp = transport.return_value.__enter__.return_value
        smtp.send_message.return_value = {}
        assert notifier.send_message_sync("subject", "body", recipients=("api@example.com",)) is True
        assert smtp.send_message.call_args.kwargs["to_addrs"] == ["api@example.com"]


def test_synchronous_delivery_reports_smtp_failure_instead_of_queued_success():
    notifier = EmailNotifier(EmailNotificationConfig(
        enabled=True, smtp_host="smtp.example.com", sender_email="sender@example.com",
        recipient_emails=("global@example.com",), use_ssl=True,
    ))
    with patch("okx_quant.notifications.smtplib.SMTP_SSL", side_effect=ConnectionError("offline SMTP")):
        assert notifier.send_message_sync("subject", "body") is False


def test_sender_merges_bound_api_recipient_without_leaking_other_api():
    from roll_terminal_qt.kline_analysis_window import _send_queued_trade_notification

    settings = {
        "enabled": True, "smtp_host": "smtp.example.com", "sender_email": "sender@example.com",
        "recipient_emails": "global@example.com", "api_sender_email_overrides": {
            "api1": "trader1@example.com;global@example.com", "api2": "trader2@example.com",
        },
    }
    row = {"kind": "filled", "plan_id": "api1:plan1", "payload": {"profile": "api1"}, "recipients": []}
    with patch.object(EmailNotifier, "send_message_sync", return_value=True) as smtp_send:
        assert _send_queued_trade_notification(row, settings) is True
        assert smtp_send.call_args.kwargs["recipients"] == ("global@example.com", "trader1@example.com")


def test_background_delivery_uses_injected_sender_off_foreground_thread(tmp_path):
    stop_background_worker()
    queue = TradeNotificationQueue(path=tmp_path / "events.json")
    _enqueue(queue)
    seen, done = [], threading.Event()
    main_thread = threading.get_ident()

    def sender(event, settings):
        seen.append((threading.get_ident(), event["kind"], settings["enabled"]))
        done.set()
        return True

    try:
        start_background_worker(lambda: dict(enabled=True, notify_signals=True, notify_trade_fills=True, notify_errors=True),
                                sender, queue=queue)
        assert done.wait(2), "background dispatcher did not consume pending event"
    finally:
        stop_background_worker()
    assert seen == [(seen[0][0], "filled", True)]
    assert seen[0][0] != main_thread
    assert queue.rows()[0]["status"] == "sent"


def _submission_fixture(monkeypatch, tmp_path):
    path = tmp_path / "intents.json"
    service = RRTradeExecutionService(intent_path=path)
    plan = _bound_rr_entry().plan
    credentials = SimpleNamespace(profile_name=plan.profile_name, api_key=TEST_API_KEY)
    config = SimpleNamespace(environment=plan.environment)
    return service, plan, credentials, config, path


def _recovered_order(plan, service):
    client_id = service._client_id(plan.plan_id, "entry", revision=0)
    return SimpleNamespace(
        raw={"instId": plan.inst_id, "clOrdId": client_id, "ordId": "accepted-before-timeout"},
        ord_id="accepted-before-timeout", client_order_id=client_id, state="partially_filled",
        price=plan.entry_price, filled_size=Decimal("1"),
    )


def test_unknown_post_is_recovered_by_original_client_id_without_second_post(monkeypatch, tmp_path):
    service, plan, credentials, config, path = _submission_fixture(monkeypatch, tmp_path)
    recovered = _recovered_order(plan, service)
    client = SimpleNamespace(get_order=lambda *_a, **kw: recovered)
    with patch.object(service, "_entry_price", return_value=plan.entry_price), \
         patch.object(service, "_order_plan", return_value=object()), \
         patch.object(service, "_submit_entry", side_effect=TimeoutError("response lost after POST")) as post:
        with pytest.raises(rr_execution.RRSubmissionUnknown):
            service.activate(client=client, credentials=credentials, config=config, plan=plan)
        assert path.exists(), "submission identity must survive unknown response"
        result = service.activate(client=client, credentials=credentials, config=config, plan=plan)
    assert post.call_count == 1
    assert result.entry_order.order_id == recovered.ord_id
    assert result.filled_size == 1


def test_not_found_during_unknown_recovery_cannot_authorize_a_second_post(monkeypatch, tmp_path):
    service, plan, credentials, config, _path = _submission_fixture(monkeypatch, tmp_path)

    def not_found(*_a, **_kw):
        raise OkxApiError("order not yet visible", code="51603", status=400)

    client = SimpleNamespace(get_order=not_found)
    with patch.object(service, "_entry_price", return_value=plan.entry_price), \
         patch.object(service, "_order_plan", return_value=object()), \
         patch.object(service, "_submit_entry", side_effect=TimeoutError("response lost")) as post:
        with pytest.raises(rr_execution.RRSubmissionUnknown):
            service.activate(client=client, credentials=credentials, config=config, plan=plan)
        with pytest.raises(RuntimeError):
            service.activate(client=client, credentials=credentials, config=config, plan=plan)
    assert post.call_count == 1


def test_unknown_submission_cannot_adopt_old_order_with_edited_stop(monkeypatch, tmp_path):
    service, plan, credentials, config, _path = _submission_fixture(monkeypatch, tmp_path)
    client = SimpleNamespace(get_order=lambda *_a, **_kw: _recovered_order(plan, service))
    with patch.object(service, "_entry_price", return_value=plan.entry_price), \
         patch.object(service, "_order_plan", return_value=object()), \
         patch.object(service, "_submit_entry", side_effect=TimeoutError("response lost")):
        with pytest.raises(rr_execution.RRSubmissionUnknown):
            service.activate(client=client, credentials=credentials, config=config, plan=plan)
        changed = replace(plan, stop_loss_price=plan.stop_loss_price - Decimal("100"))
        try:
            result = service.activate(client=client, credentials=credentials, config=config, plan=changed)
        except RuntimeError:
            return  # requiring manual reconciliation is also safe
        assert result.plan.stop_loss_price == plan.stop_loss_price


def test_definite_permission_rejection_is_not_left_as_an_unknown_post(monkeypatch, tmp_path):
    service, plan, credentials, config, _path = _submission_fixture(monkeypatch, tmp_path)
    with patch.object(service, "_entry_price", return_value=plan.entry_price), \
         patch.object(service, "_order_plan", return_value=object()), \
         patch.object(service, "_submit_entry", side_effect=OkxApiError("permission denied", code="50120", status=401)):
        with pytest.raises(rr_execution.RRSubmissionRejected):
            service.activate(client=SimpleNamespace(), credentials=credentials, config=config, plan=plan)
    assert plan.plan_id not in rr_execution._load_submission_intents(_path)


def test_quote_preflight_failure_does_not_leave_an_unknown_order(monkeypatch, tmp_path):
    service, plan, credentials, config, _path = _submission_fixture(monkeypatch, tmp_path)
    with patch.object(service, "_entry_price", side_effect=ValueError("missing quote")), \
         patch.object(service, "_submit_entry") as post:
        with pytest.raises(ValueError):
            service.activate(client=SimpleNamespace(), credentials=credentials, config=config, plan=plan)
    post.assert_not_called()
    assert plan.plan_id not in rr_execution._load_submission_intents(_path)


def test_business_timeout_over_http_success_is_still_an_unknown_post(monkeypatch, tmp_path):
    service, plan, credentials, config, path = _submission_fixture(monkeypatch, tmp_path)
    with patch.object(service, "_entry_price", return_value=plan.entry_price), \
         patch.object(service, "_order_plan", return_value=object()), \
         patch.object(service, "_submit_entry", side_effect=OkxApiError("request timed out", code="50004", status=200)):
        with pytest.raises(rr_execution.RRSubmissionUnknown):
            service.activate(client=SimpleNamespace(), credentials=credentials, config=config, plan=plan)
    assert plan.plan_id in rr_execution._load_submission_intents(path)


def test_rr_card_does_not_silently_replace_saved_api_missing_from_current_list():
    from PySide6.QtWidgets import QApplication
    from roll_terminal_qt.kline_analysis_window import RRCardDialog

    app = QApplication.instance() or QApplication([])
    item = {
        "rr_id": "legacy-bound", "trade_profile_name": "retired-api", "trade_environment": "demo",
        "side": "long", "price_entry": "100", "price_stop": "90", "r_multiple": "2",
        "risk_amount": "100", "fee_offset_enabled": False,
    }
    with patch("roll_terminal_qt.kline_analysis_window.profile_names", return_value=["different-api"]):
        dialog = RRCardDialog(parent=None, item=item, instrument=None, symbol="BTC-USDT-SWAP",
                              period="1H", price_increment=Decimal("0.1"), active_profile="different-api",
                              active_environment="live", show_binding=True)
        try:
            with patch("roll_terminal_qt.kline_analysis_window.QMessageBox.warning"):
                dialog.accept()
            result = dialog.result_payload()
            if result is None:
                return  # refusing an unavailable binding is safe
            assert result["trade_profile_name"] == "retired-api"
            assert result["trade_environment"] == "demo"
        finally:
            dialog.close()
            app.processEvents()


@pytest.fixture
def isolated_trade_window():
    from PySide6.QtWidgets import QApplication
    from roll_terminal_qt.kline_analysis_window import KlineAnalysisWindow
    from tests.test_kline_rr_execution import _instrument

    app = QApplication.instance() or QApplication([])
    with patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot"), \
         patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}), \
         patch("roll_terminal_qt.kline_analysis_window.load_kline_rr_trade_ledger_snapshot", return_value={"entries": []}), \
         patch("roll_terminal_qt.kline_analysis_window.save_kline_analysis_workspace_entries"):
        window = KlineAnalysisWindow(embedded=True, preview_mode=True)
        window._active_profile_name = lambda: "browsing-api2"
        window._active_environment = lambda: "live"
        window._selected_symbol = lambda: "BTC-USDT-SWAP"
        window._instrument_for_symbol = lambda *_a, **_kw: _instrument()
        try:
            yield window
        finally:
            window.begin_shutdown()
            window.close()
            app.processEvents()


def test_rr_bound_api_does_not_follow_browsing_account(isolated_trade_window):
    window = isolated_trade_window
    window._workspace_entry()["rr"] = [{
        "rr_id": "bound-rr", "trade_ref": "rr-ref", "trade_profile_name": "execution-api1",
        "trade_environment": "demo", "side": "long", "price_entry": "60000", "price_stop": "59000",
        "price_tp": "62000", "r_multiple": "2", "risk_amount": "100", "fee_offset_enabled": False,
        "notification_events": {"signal": False, "accepted": False, "filled": True, "error": True},
        "api_key_fingerprint": TEST_API_FINGERPRINT,
    }]
    window._selected_rr_index = 0
    window._control_tabs.setCurrentIndex(window._control_rr_tab_index)
    window._populate_rr_table(selected_index=0)
    assert "execution-api1" in window._plan_execution_context.text(), "selected plan account header was not refreshed"
    plan = window._build_selected_rr_trade_plan()
    assert (plan.profile_name, plan.environment) == ("execution-api1", "demo")


def test_legacy_rr_without_binding_requires_confirmation(isolated_trade_window):
    window = isolated_trade_window
    window._workspace_entry()["rr"] = [{
        "rr_id": "legacy", "side": "long", "price_entry": "60000", "price_stop": "59000",
        "price_tp": "62000", "r_multiple": "2", "risk_amount": "100",
    }]
    window._selected_rr_index = 0
    with pytest.raises(RuntimeError):
        window._build_selected_rr_trade_plan()
    assert not window._workspace_entry()["rr"][0].get("trade_profile_name")


def test_line_bound_api_does_not_follow_browsing_account(isolated_trade_window):
    window = isolated_trade_window
    line = make_line_rule(kind="horizontal", label="bound-line", trigger="down", action="short",
                          trade_enabled=True, time_a=1, price_a=100, time_b=2, price_b=100,
                          stop_loss_price=110, fee_offset_enabled=False)
    line.update(trade_profile_name="execution-api1", trade_environment="demo", api_key_fingerprint=TEST_API_FINGERPRINT)
    window._workspace_entry()["lines"] = [line]
    plan = window._build_line_trade_plan_from_event({"kind": "line_alert", "line_id": line["id"], "candle_time": 200})
    assert (plan.profile_name, plan.environment) == ("execution-api1", "demo")


def test_legacy_line_without_binding_cannot_silently_bind_current_api(isolated_trade_window):
    window = isolated_trade_window
    line = make_line_rule(kind="horizontal", label="legacy-line", trigger="down", action="short",
                          trade_enabled=True, time_a=1, price_a=100, time_b=2, price_b=100,
                          stop_loss_price=110, fee_offset_enabled=False)
    window._workspace_entry()["lines"] = [line]
    with pytest.raises(RuntimeError):
        window._build_line_trade_plan_from_event({"kind": "line_alert", "line_id": line["id"], "candle_time": 200})
    assert not line.get("trade_profile_name")


def test_first_partial_fill_then_full_fill_keeps_one_notification(isolated_trade_window, tmp_path):
    window = isolated_trade_window
    queue = TradeNotificationQueue(path=tmp_path / "notifications.json")
    entry = _bound_rr_entry()
    plan = replace(entry.plan, source="line", notification_events={"filled": True})
    with patch("roll_terminal_qt.kline_analysis_window.TradeNotificationQueue", return_value=queue), \
         patch("roll_terminal_qt.kline_analysis_window.save_kline_rr_trade_ledger_snapshot"), \
         patch("roll_terminal_qt.kline_analysis_window.clear_rr_submission_intent"), \
         patch.object(window, "_bind_rr_trade_entry"), patch.object(window, "_refresh_rr_trade_hint"):
        window._save_rr_trade_ledger_entry(replace(entry, plan=plan, filled_size=Decimal("1"),
                                                  remaining_size=Decimal("9"), status="entry_partially_filled"))
        window._save_rr_trade_ledger_entry(replace(entry, plan=plan, filled_size=Decimal("10"),
                                                  remaining_size=Decimal("0"), status="protected"))
    assert len(queue.rows()) == 1
    assert queue.rows()[0]["kind"] == "filled"
    assert queue.rows()[0]["payload"]["filled_size"] == "1"


def test_paper_entry_is_found_even_after_a_later_close_event(tmp_path):
    from okx_quant.area_shape_monitor import AreaShapeMonitor
    from roll_terminal_qt.kline_analysis_window import _send_queued_trade_notification

    queue = TradeNotificationQueue(path=tmp_path / "notifications.json")
    plan = AreaShapePlan.create(profile_name="api1", environment="demo", symbol="BTC-USDT-SWAP",
                                support_price=Decimal("90"), pressure_price=Decimal("120"))
    plan.events = [
        {"kind": "paper_closed", "candle_ts": 2000},
        {"kind": "paper_entry", "candle_ts": 1000, "entry": "100", "message": "simulated entry"},
    ]
    monitor = AreaShapeMonitor(store_load=lambda: [], store_save=lambda _rows: None)
    with patch("okx_quant.area_shape_monitor.TradeNotificationQueue", return_value=queue):
        monitor._emit_plan_updated(plan)
        monitor._emit_plan_updated(plan)
    rows = queue.rows()
    assert len(rows) == 1
    assert rows[0]["payload"]["simulated"] is True
    assert not rows[0]["payload"].get("order_id")
    with patch.object(EmailNotifier, "send_message_sync", return_value=True) as smtp:
        _send_queued_trade_notification(rows[0], {
            "enabled": True, "smtp_host": "smtp.example.com", "recipient_emails": "test@example.com",
        })
    assert "纸面" in smtp.call_args.args[0]
    assert "未向交易所报单" in smtp.call_args.args[0]


def test_area_repeated_error_is_deduplicated_but_distinct_protection_error_survives(tmp_path):
    from okx_quant.area_shape_execution_monitor import _enqueue_area_notification

    queue = TradeNotificationQueue(path=tmp_path / "notifications.json")
    session = {"plan_id": "p1", "session_id": "s1", "profile_name": "api1", "environment": "demo",
               "symbol": "BTC-USDT-SWAP", "plan_snapshot": {"config": {"notification_events": {"error": True}}}}
    with patch("okx_quant.area_shape_execution_monitor.TradeNotificationQueue", return_value=queue):
        for phase in ("submit_unknown", "submit_unknown", "stop_unverified"):
            _enqueue_area_notification(session, "error", {"client_id": "own-order", "phase": phase})
    assert len(queue.rows()) == 2


def test_corrupt_notification_store_does_not_break_trade_context_ui(isolated_trade_window):
    with patch.object(TradeNotificationQueue, "rows", side_effect=RuntimeError("corrupt queue")):
        isolated_trade_window._refresh_rr_trade_hint()
    assert "邮件队列存储异常" in isolated_trade_window._rr_trade_hint.text()


def test_switching_trade_tabs_always_shows_that_tabs_execution_account(isolated_trade_window):
    window = isolated_trade_window
    window._workspace_entry()["rr"] = [{"rr_id": "r1", "trade_profile_name": "rr-api", "trade_environment": "demo"}]
    window._workspace_entry()["lines"] = [{"id": "l1", "action": "short", "trade_profile_name": "line-api",
                                             "trade_environment": "live"}]
    window._selected_rr_index = 0
    window._selected_line_index = 0
    paper = AreaShapePlan.create(profile_name="paper-api", environment="demo", symbol="BTC-USDT-SWAP",
                                 support_price=Decimal("90"), pressure_price=Decimal("120"))
    window._current_area_plan = lambda: paper
    tabs = window._control_tabs
    for label, expected in (("RR交易", "rr-api"), ("智能辅助交易", "不提交API订单"),
                            ("线条触发", "line-api"), ("RR交易", "rr-api")):
        index = next(i for i in range(tabs.count()) if tabs.tabText(i) == label)
        tabs.setCurrentIndex(index)
        assert expected in window._plan_execution_context.text(), (label, window._plan_execution_context.text())


@pytest.mark.parametrize("operation", ["activate", "reconcile", "cancel"])
def test_same_profile_with_rotated_key_cannot_operate_frozen_plan(tmp_path, operation):
    entry = _bound_rr_entry()
    service = RRTradeExecutionService(intent_path=tmp_path / "intents.json")
    credentials = SimpleNamespace(profile_name=entry.plan.profile_name, api_key="a-different-real-account-key")
    config = SimpleNamespace(environment=entry.plan.environment)

    def forbidden(*_a, **_kw):
        raise AssertionError("changed account key reached exchange request")

    client = SimpleNamespace(get_order=forbidden)
    with patch.object(service, "_submit_entry", side_effect=forbidden):
        with pytest.raises(RuntimeError):
            if operation == "activate":
                service.activate(client=client, credentials=credentials, config=config, plan=entry.plan)
            elif operation == "reconcile":
                service.reconcile(client=client, credentials=credentials, config=config, entry=entry)
            else:
                service.cancel(client=client, credentials=credentials, config=config, entry=entry,
                               confirmed_for_filled=False)


def test_new_submission_without_key_identity_requires_rebinding(tmp_path):
    entry = _bound_rr_entry()
    service = RRTradeExecutionService(intent_path=tmp_path / "intents.json")
    with pytest.raises(RuntimeError):
        service.activate(client=SimpleNamespace(), credentials=SimpleNamespace(profile_name="api1", api_key=TEST_API_KEY),
                         config=SimpleNamespace(environment="demo"), plan=replace(entry.plan, api_key_fingerprint=""))


def test_rr_card_binding_requires_unlock_before_reading_api_identity(isolated_trade_window):
    from roll_terminal_qt.kline_analysis_window import RRCardDialog

    window = isolated_trade_window
    window._ensure_profile_access = lambda _profile: False
    item = {"rr_id": "draft", "side": "long", "price_entry": "100", "price_stop": "90", "r_multiple": "2"}
    with patch("roll_terminal_qt.kline_analysis_window.profile_names", return_value=["browsing-api2"]):
        card = RRCardDialog(parent=window, item=item, instrument=None, symbol="BTC-USDT-SWAP", period="1H",
                            price_increment=Decimal("0.1"), active_profile="browsing-api2",
                            active_environment="live", show_binding=True)
        try:
            with patch("roll_terminal_qt.kline_analysis_window.load_runtime") as runtime_load:
                card._bind_current_api()
                runtime_load.assert_not_called()
                assert not card._binding_api_key_fingerprint
            window._ensure_profile_access = lambda _profile: True
            runtime = SimpleNamespace(environment="live", credentials=SimpleNamespace(api_key=TEST_API_KEY))
            with patch("roll_terminal_qt.kline_analysis_window.load_runtime", return_value=runtime):
                card._bind_current_api()
            assert card._binding_api_key_fingerprint == TEST_API_FINGERPRINT
        finally:
            card.close()
