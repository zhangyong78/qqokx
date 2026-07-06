from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from roll_terminal_qt.account_positions_home import (
    AccountPositionsHomeWidget,
    _current_order_view_cancel_reference,
    _current_order_view_program_owner_label,
    _current_order_view_to_trade_order_item,
)
from okx_quant.okx_client import Instrument, OkxOrderResult
from roll_terminal_qt.order_service import OrderStatusView


class _ThreadRetireStub:
    def __init__(self, *, running: bool) -> None:
        self._running = running
        self.deleted = False
        self.terminated = False

    def isRunning(self) -> bool:
        return self._running

    def terminate(self) -> None:
        self.terminated = True

    def deleteLater(self) -> None:
        self.deleted = True


class _TimerStub:
    def __init__(self, *, active: bool = False) -> None:
        self._active = active
        self.started = False
        self.stopped = False

    def isActive(self) -> bool:
        return self._active

    def start(self) -> None:
        self.started = True
        self._active = True

    def stop(self) -> None:
        self.stopped = True
        self._active = False


class _ProfileComboStub:
    def __init__(self) -> None:
        self.enabled = True

    def setEnabled(self, enabled: bool) -> None:
        self.enabled = enabled


class AccountPositionsHomeQtHelpersTest(TestCase):
    def test_parse_positive_decimal_returns_decimal_without_widget_context(self) -> None:
        value = AccountPositionsHomeWidget._parse_positive_decimal("1.25", "平仓币数")

        self.assertEqual(value, Decimal("1.25"))

    def test_selected_position_close_display_amount_uses_coin_for_linear_swap(self) -> None:
        position = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            avail_position=Decimal("80"),
            position=Decimal("100"),
            pos_side="long",
            mark_price=Decimal("60000"),
            last_price=Decimal("60010"),
            avg_price=Decimal("59000"),
        )
        instrument = Instrument(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            ct_val=Decimal("0.01"),
            ct_mult=Decimal("1"),
            ct_val_ccy="BTC",
            state="live",
        )

        amount, unit = AccountPositionsHomeWidget._selected_position_close_display_amount(
            SimpleNamespace(_position_instruments={position.inst_id: instrument}),
            position,
            instrument,
        )

        self.assertEqual(amount, Decimal("0.8"))
        self.assertEqual(unit, "BTC")

    def test_convert_selected_position_close_coin_to_order_size_uses_linear_contract_value(self) -> None:
        position = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            avail_position=Decimal("500"),
            position=Decimal("500"),
            pos_side="long",
            mark_price=Decimal("60000"),
            last_price=Decimal("60010"),
            avg_price=Decimal("59000"),
        )
        instrument = Instrument(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            ct_val=Decimal("0.01"),
            ct_mult=Decimal("1"),
            ct_val_ccy="BTC",
            state="live",
        )

        size = AccountPositionsHomeWidget._convert_selected_position_close_coin_to_order_size(
            position,
            instrument,
            Decimal("1.23"),
        )

        self.assertEqual(size, Decimal("123"))

    def test_convert_selected_position_close_coin_to_order_size_uses_inverse_mark_price(self) -> None:
        position = SimpleNamespace(
            inst_id="BTC-USD-SWAP",
            inst_type="SWAP",
            avail_position=Decimal("500"),
            position=Decimal("500"),
            pos_side="long",
            mark_price=Decimal("50000"),
            last_price=Decimal("49950"),
            avg_price=Decimal("48000"),
        )
        instrument = Instrument(
            inst_id="BTC-USD-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            ct_val=Decimal("100"),
            ct_mult=Decimal("1"),
            ct_val_ccy="USD",
            state="live",
        )

        size = AccountPositionsHomeWidget._convert_selected_position_close_coin_to_order_size(
            position,
            instrument,
            Decimal("0.5"),
        )

        self.assertEqual(size, Decimal("250"))

    def test_submit_selected_position_manual_flatten_option_market_uses_aggressive_limit_order(self) -> None:
        shared_client = SimpleNamespace(
            place_aggressive_limit_order=MagicMock(
                return_value=OkxOrderResult(ord_id="opt-1", cl_ord_id=None, s_code="0", s_msg="accepted", raw={})
            ),
            place_simple_order=MagicMock(),
        )
        instrument = Instrument(
            inst_id="BTC-USD-260710-54000-P",
            inst_type="OPTION",
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("0.1"),
            min_size=Decimal("0.1"),
            state="live",
        )
        app = SimpleNamespace(
            _shared_client=shared_client,
            _prepare_selected_position_manual_flatten=lambda position, flatten_mode, close_size=None: (
                SimpleNamespace(),
                SimpleNamespace(),
                instrument,
                Decimal("1"),
                "sell",
                "long",
                "long",
                "market",
            ),
        )

        result, price, normalized_mode = AccountPositionsHomeWidget._submit_selected_position_manual_flatten(
            app,
            SimpleNamespace(inst_id="BTC-USD-260710-54000-P"),
            "market",
        )

        self.assertEqual(result.ord_id, "opt-1")
        self.assertIsNone(price)
        self.assertEqual(normalized_mode, "market")
        shared_client.place_aggressive_limit_order.assert_called_once()
        call_args = shared_client.place_aggressive_limit_order.call_args.args
        call_kwargs = shared_client.place_aggressive_limit_order.call_args.kwargs
        self.assertEqual(call_args[2], instrument)
        self.assertEqual(call_kwargs["side"], "sell")
        self.assertEqual(call_kwargs["size"], Decimal("1"))
        self.assertIsNone(call_kwargs["pos_side"])
        shared_client.place_simple_order.assert_not_called()

    def test_selected_position_manual_flatten_after_zero_delay_uses_ui_signal_dispatch(self) -> None:
        callback = MagicMock()
        emitted: list[object] = []
        app = SimpleNamespace(
            _selected_position_manual_flatten_callback=SimpleNamespace(emit=lambda payload: emitted.append(payload))
        )

        AccountPositionsHomeWidget._selected_position_manual_flatten_after(app, 0, callback)

        self.assertEqual(emitted, [callback])
        callback.assert_not_called()

    def test_selected_position_manual_flatten_after_positive_delay_uses_qtimer(self) -> None:
        callback = MagicMock()
        app = SimpleNamespace(
            _selected_position_manual_flatten_callback=SimpleNamespace(emit=MagicMock())
        )

        with patch("roll_terminal_qt.account_positions_home.QTimer.singleShot") as single_shot:
            AccountPositionsHomeWidget._selected_position_manual_flatten_after(app, 650, callback)

        single_shot.assert_called_once_with(650, callback)
        app._selected_position_manual_flatten_callback.emit.assert_not_called()

    def test_on_profile_changed_ignores_events_before_profile_switch_is_ready(self) -> None:
        app = SimpleNamespace(
            _profile_switch_guard=False,
            _profile_change_ready=False,
            _profile_switch_in_progress=False,
            _last_profile_name="159",
            _profile_change_serial=0,
            _current_profile_name=lambda: "2211",
            _set_profile_switch_in_progress=lambda value: None,
        )

        with patch("roll_terminal_qt.account_positions_home.QTimer.singleShot") as single_shot:
            AccountPositionsHomeWidget._on_profile_changed(app)

        self.assertEqual(app._profile_change_serial, 0)
        single_shot.assert_not_called()

    def test_on_profile_changed_schedules_profile_apply_after_startup(self) -> None:
        app = SimpleNamespace(
            _profile_switch_guard=False,
            _profile_change_ready=True,
            _profile_switch_in_progress=False,
            _last_profile_name="159",
            _profile_change_serial=0,
            _current_profile_name=lambda: "2211",
            _set_profile_switch_in_progress=lambda value: None,
        )

        with patch("roll_terminal_qt.account_positions_home.QTimer.singleShot") as single_shot:
            AccountPositionsHomeWidget._on_profile_changed(app)

        self.assertEqual(app._profile_change_serial, 1)
        self.assertEqual(single_shot.call_count, 1)

    def test_on_profile_changed_tolerates_broken_stdout(self) -> None:
        class _BrokenStdout:
            def write(self, _message: str) -> None:
                raise RuntimeError("stdout unavailable")

            def flush(self) -> None:
                raise RuntimeError("stdout unavailable")

        app = SimpleNamespace(
            _profile_switch_guard=False,
            _profile_change_ready=True,
            _profile_switch_in_progress=False,
            _last_profile_name="159",
            _profile_change_serial=0,
            _current_profile_name=lambda: "2211",
            _set_profile_switch_in_progress=lambda value: None,
        )

        with (
            patch("roll_terminal_qt.account_positions_home.sys.stdout", _BrokenStdout()),
            patch("roll_terminal_qt.account_positions_home.QTimer.singleShot") as single_shot,
        ):
            AccountPositionsHomeWidget._on_profile_changed(app)

        self.assertEqual(app._profile_change_serial, 1)
        single_shot.assert_called_once()

    def test_apply_profile_change_locked_profile_uses_async_unlock_flow(self) -> None:
        app = SimpleNamespace(
            _profile_change_serial=1,
            _profile_switch_guard=False,
            _current_profile_name=lambda: "2211",
            _last_profile_name="159",
            _profile_snapshots={"2211": {"switch_password_hash": "x"}},
            _unlocked_profiles=set(),
            _prompt_profile_unlock=MagicMock(),
        )

        with (
            patch("roll_terminal_qt.account_positions_home.profile_requires_password", return_value=True),
            patch(
                "roll_terminal_qt.account_positions_home.ensure_profile_unlocked",
                side_effect=AssertionError("sync unlock should not run during API switching"),
            ),
            patch("roll_terminal_qt.account_positions_home.load_runtime") as load_runtime,
        ):
            AccountPositionsHomeWidget._apply_profile_change(app, "2211", 1)

        app._prompt_profile_unlock.assert_called_once_with("2211", 1)
        load_runtime.assert_not_called()

    def test_apply_profile_change_starts_async_restart_flow_for_valid_runtime(self) -> None:
        runtime = SimpleNamespace(name="runtime-2211")
        app = SimpleNamespace(
            _profile_change_serial=1,
            _profile_switch_guard=False,
            _current_profile_name=lambda: "2211",
            _last_profile_name="159",
            _profile_snapshots={},
            _unlocked_profiles=set(),
            _begin_profile_switch_restart=MagicMock(),
            _clear_profile_switch_request=MagicMock(),
        )

        with (
            patch("roll_terminal_qt.account_positions_home.profile_requires_password", return_value=False),
            patch("roll_terminal_qt.account_positions_home.load_runtime", return_value=runtime),
        ):
            AccountPositionsHomeWidget._apply_profile_change(app, "2211", 1)

        app._begin_profile_switch_restart.assert_called_once_with("2211", runtime, 1)
        app._clear_profile_switch_request.assert_not_called()

    def test_dispatch_profile_change_waits_for_profile_combo_popup_to_close(self) -> None:
        app = SimpleNamespace(
            _profile_change_serial=2,
            _apply_profile_change=MagicMock(),
        )

        with (
            patch("roll_terminal_qt.account_positions_home.QApplication.activePopupWidget", return_value=object()),
            patch("roll_terminal_qt.account_positions_home.QTimer.singleShot") as single_shot,
        ):
            AccountPositionsHomeWidget._dispatch_profile_change(app, "2211", 2)

        app._apply_profile_change.assert_not_called()
        single_shot.assert_called_once()

    def test_poll_profile_switch_completion_waits_for_running_retired_threads(self) -> None:
        retired = _ThreadRetireStub(running=True)
        timer = _TimerStub(active=False)
        app = SimpleNamespace(
            _retired_threads=[retired],
            _profile_switch_deadline_monotonic=10**9,
            _profile_switch_force_terminate_sent=False,
            _profile_switch_poll_timer=timer,
            _ensure_profile_switch_poll_timer=lambda: timer,
            _profile_change_serial=3,
            _profile_switch_requested_serial=3,
            _profile_switch_requested_target="2211",
            _profile_switch_requested_runtime=SimpleNamespace(name="runtime-2211"),
            _last_profile_name="159",
            _start_private_threads=MagicMock(),
            _clear_profile_switch_request=MagicMock(),
        )

        AccountPositionsHomeWidget._poll_profile_switch_completion(app)

        app._start_private_threads.assert_not_called()
        app._clear_profile_switch_request.assert_not_called()
        self.assertTrue(timer.started)
        self.assertFalse(retired.deleted)

    def test_poll_profile_switch_completion_applies_runtime_after_retired_threads_finish(self) -> None:
        retired = _ThreadRetireStub(running=False)
        timer = _TimerStub(active=True)
        combo = _ProfileComboStub()
        runtime = SimpleNamespace(name="runtime-2211")
        app = SimpleNamespace(
            _retired_threads=[retired],
            _profile_switch_deadline_monotonic=10**9,
            _profile_switch_force_terminate_sent=False,
            _profile_switch_poll_timer=timer,
            _profile_change_serial=4,
            _profile_switch_requested_serial=4,
            _profile_switch_requested_target="2211",
            _profile_switch_requested_runtime=runtime,
            _last_profile_name="159",
            _runtime=None,
            _unlocked_profiles=set(),
            _profile_combo=combo,
            _profile_switch_in_progress=True,
            _set_profile_switch_in_progress=lambda value: AccountPositionsHomeWidget._set_profile_switch_in_progress(app, value),
            _clear_profile_switch_request=lambda: AccountPositionsHomeWidget._clear_profile_switch_request(app),
            _start_private_threads=MagicMock(),
        )

        AccountPositionsHomeWidget._poll_profile_switch_completion(app)

        self.assertIs(app._runtime, runtime)
        self.assertEqual(app._last_profile_name, "2211")
        self.assertIn("2211", app._unlocked_profiles)
        app._start_private_threads.assert_called_once_with()
        self.assertTrue(retired.deleted)
        self.assertEqual(app._profile_switch_requested_target, "")
        self.assertFalse(app._profile_switch_in_progress)
        self.assertTrue(combo.enabled)
        self.assertTrue(timer.stopped)

    def test_selected_position_manual_flatten_result_failed_uses_s_code(self) -> None:
        ok_result = OkxOrderResult(ord_id="1", cl_ord_id="a", s_code="0", s_msg="", raw={})
        failed_result = OkxOrderResult(ord_id="", cl_ord_id=None, s_code="50011", s_msg="no permission", raw={})

        self.assertFalse(AccountPositionsHomeWidget._selected_position_manual_flatten_result_failed(SimpleNamespace(), ok_result))
        self.assertTrue(
            AccountPositionsHomeWidget._selected_position_manual_flatten_result_failed(SimpleNamespace(), failed_result)
        )

    def test_selected_position_manual_flatten_result_error_message_contains_profile_and_okx_error(self) -> None:
        app = SimpleNamespace(
            _last_profile_name="159",
            _current_profile_name=lambda: "159",
            _selected_position_manual_flatten_result_reason_text=(
                lambda *, s_code, s_msg: AccountPositionsHomeWidget._selected_position_manual_flatten_result_reason_text(
                    SimpleNamespace(), s_code=s_code, s_msg=s_msg
                )
            ),
        )
        position = SimpleNamespace(inst_id="BTC-USD-260710-57000-P")
        result = OkxOrderResult(ord_id="", cl_ord_id=None, s_code="50011", s_msg="You are using a read-only API key", raw={})

        message = AccountPositionsHomeWidget._selected_position_manual_flatten_result_error_message(
            app,
            position=position,
            result=result,
            close_side_label="BUY 买入平仓",
            submit_size_text="1",
        )

        self.assertIn("API配置：159", message)
        self.assertIn("BTC-USD-260710-57000-P", message)
        self.assertIn("sCode=50011", message)
        self.assertIn("read-only API key", message)

    def test_selected_position_manual_flatten_result_error_message_explains_permission_error(self) -> None:
        app = SimpleNamespace(
            _last_profile_name="159",
            _current_profile_name=lambda: "159",
            _selected_position_manual_flatten_result_reason_text=(
                lambda *, s_code, s_msg: AccountPositionsHomeWidget._selected_position_manual_flatten_result_reason_text(
                    SimpleNamespace(), s_code=s_code, s_msg=s_msg
                )
            ),
        )
        position = SimpleNamespace(inst_id="BTC-USD-260710-60000-C")
        result = OkxOrderResult(
            ord_id="",
            cl_ord_id=None,
            s_code="50120",
            s_msg="This API key doesn't have permission to use this function",
            raw={},
        )

        message = AccountPositionsHomeWidget._selected_position_manual_flatten_result_error_message(
            app,
            position=position,
            result=result,
            close_side_label="SELL 卖出平仓",
            submit_size_text="100",
        )

        self.assertIn("原因解释：", message)
        self.assertIn("没有交易权限", message)
        self.assertIn("159", message)
        self.assertIn("Trade/交易权限", message)
