from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from PySide6.QtWidgets import QHeaderView

from tests.qt_test_case import QtWidgetTestCase
import roll_terminal_qt.account_positions_home as account_positions_module
from roll_terminal_qt.account_positions_home import (
    AccountPositionsHomeWidget,
    LegacyOptionToolsHost,
    PositionProtectionDialog,
    _position_break_even_price,
    _break_even_taker_fee_rate,
    _current_order_view_cancel_reference,
    _current_order_view_program_owner_label,
    _current_order_view_to_trade_order_item,
    _position_display_foreground_colors,
)
from okx_quant.okx_client import Instrument, OkxOrderResult
from okx_quant.position_protection import ProtectionSessionSnapshot
from roll_terminal_qt.order_service import OrderStatusView
from roll_terminal_qt.realtime_account_store import AccountRealtimeSnapshot
from roll_terminal_qt.shared_order_store import SharedOrderSnapshot


class PositionDisplayForegroundColorsTest(TestCase):
    def test_history_position_kline_markers_use_open_and_close_timestamps(self) -> None:
        history_item = SimpleNamespace(
            update_time=1_752_300_600_000,
            raw={"cTime": "1752210000000", "uTime": "1752300600000"},
        )

        markers = account_positions_module._position_history_kline_time_markers(history_item)

        self.assertEqual(markers, (("开仓", 1_752_210_000_000), ("平仓", 1_752_300_600_000)))

    def test_history_kline_limit_covers_the_oldest_time_marker(self) -> None:
        now_ms = 1_752_300_600_000
        markers = (("开仓", now_ms - (300 * 60 * 60 * 1000)), ("平仓", now_ms))

        limit = account_positions_module._position_kline_candle_limit("1H", markers, now_ms=now_ms)

        self.assertEqual(limit, 480)

    def test_current_position_kline_reuses_matching_history_markers(self) -> None:
        position = SimpleNamespace(inst_id="BTC-USD-260720-65000-C", raw={"cTime": "1752210000000"})
        history_item = SimpleNamespace(
            inst_id="BTC-USD-260720-65000-C",
            update_time=1_752_300_600_000,
            raw={"cTime": "1752210000000", "uTime": "1752300600000"},
        )

        markers = account_positions_module._current_position_kline_time_markers(position, [history_item])

        self.assertEqual(
            markers,
            (("开仓", 1_752_210_000_000), ("平仓", 1_752_300_600_000)),
        )

    def test_history_position_contract_click_opens_matching_kline(self) -> None:
        history_item = SimpleNamespace(inst_id="BTC-USD-260720-65000-C", inst_type="OPTION")
        app = SimpleNamespace(
            _visible_position_history_items=[history_item],
            _open_position_history_kline=MagicMock(),
        )

        AccountPositionsHomeWidget._on_position_history_table_clicked(app, 0, 2)
        AccountPositionsHomeWidget._on_position_history_table_clicked(app, 0, 1)

        app._open_position_history_kline.assert_called_once_with(history_item)

    def test_history_position_kline_uses_the_open_price_as_option_entry(self) -> None:
        history_item = SimpleNamespace(
            inst_id="BTC-USD-260720-65000-C",
            inst_type="OPTION",
            open_avg_price=Decimal("0.0012"),
            update_time=1_752_300_600_000,
            raw={"idxPx": "65000"},
        )
        app = SimpleNamespace(_open_position_kline=MagicMock())

        AccountPositionsHomeWidget._open_position_history_kline(app, history_item)

        opened_position = app._open_position_kline.call_args.args[0]
        self.assertEqual(opened_position.inst_id, history_item.inst_id)
        self.assertEqual(opened_position.inst_type, history_item.inst_type)
        self.assertEqual(opened_position.avg_price, history_item.open_avg_price)
        self.assertEqual(opened_position.raw, history_item.raw)
        self.assertEqual(app._open_position_kline.call_args.kwargs["time_markers"], (("平仓", history_item.update_time),))

    def test_quotes_are_fixed_colors_and_market_value_follows_unrealized_pnl(self) -> None:
        positive = _position_display_foreground_colors(
            time_value_text="B 0.0200",
            intrinsic_value_text="B 0.0100",
            bid_price_text="B 0.0200",
            ask_price_text="B 0.0205",
            mark_price_text="B 0.0202",
            avg_price_text="B 0.0190",
            break_even_text="B 0.0192",
            market_value_text="0.01 BTC",
            unrealized_pnl=Decimal("0.01"),
        )
        negative = _position_display_foreground_colors(
            time_value_text="B 0.0200",
            intrinsic_value_text="B 0.0100",
            bid_price_text="B 0.0200",
            ask_price_text="B 0.0205",
            mark_price_text="B 0.0202",
            avg_price_text="B 0.0190",
            break_even_text="B 0.0192",
            market_value_text="-0.01 BTC",
            unrealized_pnl=Decimal("-0.01"),
        )
        zero_or_missing = _position_display_foreground_colors(
            time_value_text="--",
            intrinsic_value_text="--",
            bid_price_text="--",
            ask_price_text="--",
            mark_price_text="--",
            avg_price_text="--",
            break_even_text="--",
            market_value_text="--",
            unrealized_pnl=Decimal("0"),
        )

        self.assertEqual(positive["time_value"].name(), "#7c3aed")
        self.assertEqual(positive["intrinsic_value"].name(), "#7c3aed")
        self.assertEqual(positive["bid_price"].name(), "#d97706")
        self.assertEqual(positive["ask_price"].name(), "#d97706")
        self.assertEqual(positive["mark"].name(), "#2563eb")
        self.assertEqual(positive["avg"].name(), "#2563eb")
        self.assertEqual(positive["break_even"].name(), "#13803d")
        self.assertEqual(positive["market_value"].name(), "#13803d")
        self.assertEqual(negative["break_even"].name(), "#c23b3b")
        self.assertEqual(negative["market_value"].name(), "#c23b3b")
        self.assertNotIn("market_value", zero_or_missing)
        self.assertNotIn("break_even", zero_or_missing)
        self.assertNotIn("time_value", zero_or_missing)
        self.assertNotIn("intrinsic_value", zero_or_missing)
        self.assertNotIn("bid_price", zero_or_missing)
        self.assertNotIn("ask_price", zero_or_missing)


class AccountPositionsHistoryTabWiringTest(QtWidgetTestCase):
    def test_active_history_table_click_is_wired_to_kline_handler(self) -> None:
        with (
            patch.object(AccountPositionsHomeWidget, "_start_private_threads"),
            patch.object(AccountPositionsHomeWidget, "_on_position_history_table_clicked") as handler,
        ):
            widget = AccountPositionsHomeWidget()
            try:
                widget._position_history_table.cellClicked.emit(0, 2)
                handler.assert_called_once_with(0, 2)
            finally:
                self.dispose_widget(widget)


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


class AccountPositionsWorkspaceProfileTest(TestCase):
    def test_apply_workspace_profile_reuses_async_switch_path_without_unlocking_again(self) -> None:
        app = SimpleNamespace(
            _last_profile_name="api1",
            _profile_change_serial=7,
            _unlocked_profiles=set(),
            _select_profile_without_signal=MagicMock(),
            _set_profile_switch_in_progress=MagicMock(),
            _apply_profile_change=MagicMock(),
        )
        with patch(
            "roll_terminal_qt.account_positions_home.QTimer.singleShot",
            side_effect=lambda _delay, callback: callback(),
        ):
            AccountPositionsHomeWidget.apply_workspace_profile(app, "api2")

        self.assertIn("api2", app._unlocked_profiles)
        app._select_profile_without_signal.assert_called_once_with("api2")
        app._set_profile_switch_in_progress.assert_called_once_with(True)
        app._apply_profile_change.assert_called_once_with("api2", 8)

    def test_set_workspace_managed_hides_duplicate_profile_controls(self) -> None:
        app = SimpleNamespace(_profile_label=MagicMock(), _profile_combo=MagicMock())

        AccountPositionsHomeWidget.set_workspace_managed(app, True)

        app._profile_label.setVisible.assert_called_once_with(False)
        app._profile_combo.setVisible.assert_called_once_with(False)


class _ProtectionSessionTableStub:
    def __init__(self) -> None:
        self.row_count = 0
        self.items: dict[tuple[int, int], object] = {}
        self.selected_row = -1

    def currentRow(self) -> int:
        return self.selected_row

    def setRowCount(self, count: int) -> None:
        self.row_count = count

    def setItem(self, row: int, column: int, item: object) -> None:
        self.items[(row, column)] = item

    def selectRow(self, row: int) -> None:
        self.selected_row = row


class _HeaderStub:
    def __init__(self) -> None:
        self.stretch_last_section = True
        self.resize_modes: dict[int, object] = {}

    def setStretchLastSection(self, enabled: bool) -> None:
        self.stretch_last_section = enabled

    def setSectionResizeMode(self, column: int, mode: object) -> None:
        self.resize_modes[column] = mode


class _ColumnConfigTableStub:
    def __init__(self) -> None:
        self._header = _HeaderStub()
        self.column_widths: dict[int, int] = {}
        self.scrollbar_policy = None

    def horizontalHeader(self) -> _HeaderStub:
        return self._header

    def setColumnWidth(self, column: int, width: int) -> None:
        self.column_widths[column] = width

    def setHorizontalScrollBarPolicy(self, policy: object) -> None:
        self.scrollbar_policy = policy


class AccountPositionsHomeQtHelpersTest(TestCase):
    def test_option_break_even_uses_strike_premium_and_two_way_fee(self) -> None:
        call = SimpleNamespace(
            inst_id="BTC-USD-260731-63000-C",
            inst_type="OPTION",
            pos_side="long",
            position=Decimal("1"),
            avg_price=Decimal("0.024"),
            margin_ccy="BTC",
        )

        value = _position_break_even_price(call, {"BTC": Decimal("64000")}, fee_rate=Decimal("0.0003"))

        self.assertEqual(value, Decimal("64536.9216"))

    def test_swap_break_even_only_adds_two_way_fee_in_position_direction(self) -> None:
        short_swap = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            pos_side="short",
            position=Decimal("-1"),
            avg_price=Decimal("64000"),
            margin_ccy="USDT",
        )

        value = _position_break_even_price(short_swap, {}, fee_rate=Decimal("0.00036"))

        self.assertEqual(value, Decimal("63953.92000"))

    def test_option_fee_rate_falls_back_to_contract_taker_fee(self) -> None:
        rate = _break_even_taker_fee_rate(
            {"futures_taker_fee_rate": "0.0360", "option_taker_fee_rate": ""},
            inst_type="OPTION",
        )

        self.assertEqual(rate, Decimal("0.00036"))
    @patch("roll_terminal_qt.account_positions_home.subprocess.Popen")
    def test_legacy_option_tools_launch_in_external_process(self, popen: MagicMock) -> None:
        host = LegacyOptionToolsHost(parent=SimpleNamespace(), runtime_provider=lambda: None)

        host.open_option_roll(position=object(), instrument=object(), ticker=object(), api_name="moni")

        popen.assert_called_once()
    def test_sync_order_watchlist_clears_symbol_restriction_for_current_orders(self) -> None:
        order_feed = MagicMock()
        app = SimpleNamespace(
            _order_feed=order_feed,
            _visible_positions=[SimpleNamespace(inst_id="BTC-USD-260731-60000-P")],
        )

        AccountPositionsHomeWidget._sync_order_watchlist(app)

        order_feed.set_watched_inst_ids.assert_called_once_with(set())

    def test_start_private_threads_uses_realtime_store_not_legacy_order_feed(self) -> None:
        runtime = SimpleNamespace(environment="demo")
        realtime_store = MagicMock()
        app = SimpleNamespace(
            _runtime=runtime,
            _private_thread_generation=0,
            _last_profile_name="moni",
            _realtime_store=realtime_store,
            _start_order_history_refresh=MagicMock(),
            _start_fill_history_refresh=MagicMock(),
            _start_position_history_refresh=MagicMock(),
        )

        AccountPositionsHomeWidget._start_private_threads(app)

        realtime_store.start.assert_called_once_with(runtime)
        app._start_order_history_refresh.assert_called_once_with(force_restart=False)
        app._start_fill_history_refresh.assert_called_once_with(force_restart=False)
        app._start_position_history_refresh.assert_called_once_with(force_restart=False)

    def test_unchanged_order_table_cell_is_not_replaced(self) -> None:
        cell = MagicMock()
        cell.text.return_value = "same"
        table = MagicMock()
        table.item.return_value = cell

        AccountPositionsHomeWidget._update_table_row(
            SimpleNamespace(),
            table,
            0,
            ("same",),
        )

        cell.setText.assert_not_called()
        table.setItem.assert_not_called()

    def test_realtime_order_only_snapshot_does_not_rebuild_positions(self) -> None:
        app = SimpleNamespace(
            _runtime=SimpleNamespace(environment="demo"),
            _last_profile_name="moni",
            _raw_positions=[],
            _apply_positions_payload=MagicMock(),
            _apply_positions_summary=MagicMock(),
            _apply_orders=MagicMock(),
        )

        AccountPositionsHomeWidget._apply_realtime_snapshot(
            app,
            AccountRealtimeSnapshot(
                profile_name="moni",
                environment="demo",
                positions=(),
                orders=(),
                account=None,
                generation=1,
                source="ws",
            ),
        )

        app._apply_positions_payload.assert_not_called()
        app._apply_orders.assert_called_once_with([])

    def test_realtime_position_snapshot_preserves_loaded_price_maps(self) -> None:
        next_position = SimpleNamespace(inst_id="BTC-USD-260731-63000-C")
        app = SimpleNamespace(
            _runtime=SimpleNamespace(environment="demo"),
            _last_profile_name="moni",
            _raw_positions=[],
            _position_instruments={"BTC-USD-260731-63000-C": object()},
            _position_tickers={"BTC-USD-260731-63000-C": object()},
            _upl_usdt_prices={"BTC": Decimal("64000")},
            _apply_positions_payload=MagicMock(),
            _apply_positions_summary=MagicMock(),
            _apply_orders=MagicMock(),
        )

        AccountPositionsHomeWidget._apply_realtime_snapshot(
            app,
            AccountRealtimeSnapshot(
                profile_name="moni",
                environment="demo",
                positions=(next_position,),
                orders=(),
                account=None,
                generation=1,
                source="ws",
                position_instruments={"BTC-USD-260731-63000-C": object()},
                position_tickers={"BTC-USD-260731-63000-C": object()},
                upl_usdt_prices={"BTC": Decimal("64000")},
            ),
        )

        payload = app._apply_positions_payload.call_args.args[0]
        self.assertEqual(payload["upl_usdt_prices"], {"BTC": Decimal("64000")})

    def test_unchanged_position_payload_does_not_rebuild_tree(self) -> None:
        position = SimpleNamespace(inst_id="BTC-USDT-SWAP")
        app = SimpleNamespace(
            _raw_positions=[position],
            _position_instruments={},
            _position_tickers={},
            _upl_usdt_prices={},
            _last_profile_name="",
            _render_positions_tree=MagicMock(),
        )

        AccountPositionsHomeWidget._apply_positions_payload(
            app,
            {
                "positions": [position],
                "position_instruments": {},
                "position_tickers": {},
                "upl_usdt_prices": {},
            },
        )

        app._render_positions_tree.assert_not_called()

    def test_apply_orders_keeps_pending_order_visible_even_without_matching_position(self) -> None:
        order = OrderStatusView(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            ord_id="algo-1",
            side="sell",
            pos_side="long",
            td_mode="cross",
            ord_type="conditional",
            state="effective",
            price=None,
            avg_price=None,
            size=Decimal("1"),
            filled_size=Decimal("0"),
            created_time=1,
            update_time=2,
            client_order_id="algo-client-1",
            reduce_only=None,
            raw={"_source_kind": "algo", "algoId": "algo-1"},
        )
        app = SimpleNamespace(
            _visible_positions=[SimpleNamespace(inst_id="BTC-USD-260731-60000-P")],
            _refresh_current_orders_table=MagicMock(),
        )

        AccountPositionsHomeWidget._apply_orders(app, [order])

        self.assertEqual(app._orders, [order])
        self.assertEqual(app._visible_orders, [order])
        app._refresh_current_orders_table.assert_called_once_with()

    def test_apply_orders_publishes_current_orders_to_shared_store(self) -> None:
        order = OrderStatusView(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            ord_id="algo-1",
            side="sell",
            pos_side="long",
            td_mode="cross",
            ord_type="conditional",
            state="effective",
            price=None,
            avg_price=None,
            size=Decimal("1"),
            filled_size=Decimal("0"),
            created_time=1,
            update_time=2,
            client_order_id="algo-client-1",
            reduce_only=None,
            raw={"_source_kind": "algo", "algoId": "algo-1"},
        )
        shared_order_store = MagicMock()
        app = SimpleNamespace(
            _visible_positions=[],
            _refresh_current_orders_table=MagicMock(),
            _shared_order_store=shared_order_store,
            _last_profile_name="moni",
            _runtime=SimpleNamespace(environment="demo"),
        )

        AccountPositionsHomeWidget._apply_orders(app, [order])

        shared_order_store.publish_current_orders.assert_called_once_with(
            profile_name="moni",
            environment="demo",
            orders=[order],
        )

    def test_refresh_current_orders_table_summary_no_longer_mentions_current_position_contract_scope(self) -> None:
        orders_summary_label = SimpleNamespace(setText=MagicMock())
        orders_table = SimpleNamespace(
            currentRow=lambda: -1,
            setRowCount=MagicMock(),
        )
        app = SimpleNamespace(
            _orders_table=orders_table,
            _filtered_current_orders=lambda: [],
            _orders_summary_label=orders_summary_label,
            _restore_table_selection=MagicMock(),
            _refresh_current_order_detail=MagicMock(),
        )

        AccountPositionsHomeWidget._refresh_current_orders_table(app)

        orders_summary_label.setText.assert_called_once_with("当前委托：0 条")

    def test_apply_order_history_payload_publishes_history_orders_to_shared_store(self) -> None:
        history_order = SimpleNamespace(inst_id="BTC-USDT-SWAP")
        shared_order_store = MagicMock()
        app = SimpleNamespace(
            _shared_order_store=shared_order_store,
            _last_profile_name="moni",
            _runtime=SimpleNamespace(environment="demo"),
            _refresh_order_history_table=MagicMock(),
        )

        AccountPositionsHomeWidget._apply_order_history_payload(
            app,
            {
                "items": [history_order],
                "usdt_prices": {"USDT": Decimal("1")},
            },
        )

        shared_order_store.publish_history_orders.assert_called_once_with(
            profile_name="moni",
            environment="demo",
            orders=[history_order],
            usdt_prices={"USDT": Decimal("1")},
        )
        app._refresh_order_history_table.assert_called_once_with()

    def test_apply_order_history_payload_skips_identical_cached_data(self) -> None:
        history_order = SimpleNamespace(inst_id="BTC-USDT-SWAP")
        shared_order_store = MagicMock()
        app = SimpleNamespace(
            _shared_order_store=shared_order_store,
            _last_profile_name="moni",
            _runtime=SimpleNamespace(environment="demo"),
            _order_history_items=[history_order],
            _order_history_usdt_prices={"USDT": Decimal("1")},
            _refresh_order_history_table=MagicMock(),
        )

        AccountPositionsHomeWidget._apply_order_history_payload(
            app,
            {
                "items": [history_order],
                "usdt_prices": {"USDT": Decimal("1")},
            },
        )

        app._refresh_order_history_table.assert_not_called()
        shared_order_store.publish_history_orders.assert_not_called()

    @patch("roll_terminal_qt.account_positions_home.QTableWidget")
    def test_build_history_table_uses_interactive_widths_for_non_stretch_columns(
        self,
        table_type: MagicMock,
    ) -> None:
        table = table_type.return_value
        header = table.horizontalHeader.return_value

        AccountPositionsHomeWidget._build_history_table(
            SimpleNamespace(),
            ("时间", "合约", "订单ID"),
            stretch_columns={1},
        )

        header.setSectionResizeMode.assert_any_call(0, QHeaderView.ResizeMode.Interactive)
        header.setSectionResizeMode.assert_any_call(1, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode.assert_any_call(2, QHeaderView.ResizeMode.Interactive)

    @patch("roll_terminal_qt.account_positions_home.QTableWidget")
    def test_build_history_table_enables_header_sorting(self, table_type: MagicMock) -> None:
        AccountPositionsHomeWidget._build_history_table(
            SimpleNamespace(),
            ("时间", "合约"),
            stretch_columns=set(),
        )

        table_type.return_value.setSortingEnabled.assert_called_once_with(True)

    def test_apply_shared_order_snapshot_updates_matching_profile_environment(self) -> None:
        current_order = OrderStatusView(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            ord_id="algo-1",
            side="sell",
            pos_side="long",
            td_mode="cross",
            ord_type="conditional",
            state="effective",
            price=None,
            avg_price=None,
            size=Decimal("1"),
            filled_size=Decimal("0"),
            created_time=1,
            update_time=2,
            client_order_id="algo-client-1",
            reduce_only=None,
            raw={"_source_kind": "algo", "algoId": "algo-1"},
        )
        history_order = SimpleNamespace(inst_id="BTC-USDT-SWAP")
        app = SimpleNamespace(
            _last_profile_name="moni",
            _runtime=SimpleNamespace(environment="demo"),
            _refresh_current_orders_table=MagicMock(),
            _refresh_order_history_table=MagicMock(),
        )

        AccountPositionsHomeWidget._apply_shared_order_snapshot(
            app,
            "moni",
            "demo",
            SharedOrderSnapshot(
                current_order_views=(current_order,),
                history_orders=(history_order,),
                history_order_usdt_prices={"USDT": Decimal("1")},
            ),
        )

        self.assertEqual(app._orders, [current_order])
        self.assertEqual(app._visible_orders, [current_order])
        self.assertEqual(app._order_history_items, [history_order])
        self.assertEqual(app._order_history_usdt_prices, {"USDT": Decimal("1")})
        app._refresh_current_orders_table.assert_called_once_with()
        app._refresh_order_history_table.assert_called_once_with()

    def test_apply_shared_order_snapshot_skips_identical_data(self) -> None:
        current_order = MagicMock()
        history_order = MagicMock()
        app = SimpleNamespace(
            _last_profile_name="moni",
            _runtime=SimpleNamespace(environment="demo"),
            _orders=[current_order],
            _visible_orders=[current_order],
            _order_history_items=[history_order],
            _order_history_usdt_prices={"USDT": Decimal("1")},
            _refresh_current_orders_table=MagicMock(),
            _refresh_order_history_table=MagicMock(),
        )

        AccountPositionsHomeWidget._apply_shared_order_snapshot(
            app,
            "moni",
            "demo",
            SharedOrderSnapshot(
                current_order_views=(current_order,),
                history_orders=(history_order,),
                history_order_usdt_prices={"USDT": Decimal("1")},
            ),
        )

        app._refresh_current_orders_table.assert_not_called()
        app._refresh_order_history_table.assert_not_called()

    def test_apply_shared_order_snapshot_refreshes_only_changed_history(self) -> None:
        current_order = MagicMock()
        old_history_order = MagicMock()
        new_history_order = MagicMock()
        app = SimpleNamespace(
            _last_profile_name="moni",
            _runtime=SimpleNamespace(environment="demo"),
            _orders=[current_order],
            _visible_orders=[current_order],
            _order_history_items=[old_history_order],
            _order_history_usdt_prices={},
            _refresh_current_orders_table=MagicMock(),
            _refresh_order_history_table=MagicMock(),
        )

        AccountPositionsHomeWidget._apply_shared_order_snapshot(
            app,
            "moni",
            "demo",
            SharedOrderSnapshot(
                current_order_views=(current_order,),
                history_orders=(new_history_order,),
            ),
        )

        app._refresh_current_orders_table.assert_not_called()
        app._refresh_order_history_table.assert_called_once_with()

    @patch("roll_terminal_qt.account_positions_home._reconcile_current_position_note_records", return_value=True)
    def test_apply_positions_payload_saves_changed_note_state(self, reconcile_notes: MagicMock) -> None:
        position = MagicMock()
        app = SimpleNamespace(
            _raw_positions=[],
            _position_instruments={},
            _position_tickers={},
            _upl_usdt_prices={},
            _last_profile_name="moni",
            _current_notes={},
            _note_environment=lambda: "demo",
            _save_position_notes=MagicMock(),
            _render_positions_tree=MagicMock(),
        )

        AccountPositionsHomeWidget._apply_positions_payload(
            app,
            {
                "positions": [position],
                "position_instruments": {},
                "position_tickers": {},
                "upl_usdt_prices": {},
            },
        )

        reconcile_notes.assert_called_once()
        app._save_position_notes.assert_called_once_with()

    def test_apply_position_history_payload_inherits_and_saves_notes(self) -> None:
        history_item = MagicMock()
        app = SimpleNamespace(
            _position_history_items=[],
            _position_history_instruments={},
            _position_history_usdt_prices={},
            _last_profile_name="moni",
            _current_notes={},
            _history_notes={},
            _note_environment=lambda: "demo",
            _save_position_notes=MagicMock(),
            _render_position_history_table=MagicMock(),
        )
        with (
            patch(
                "roll_terminal_qt.account_positions_home._inherit_position_history_notes",
                create=True,
                return_value=True,
            ) as inherit_notes,
            patch(
                "roll_terminal_qt.account_positions_home._prune_closed_current_position_notes",
                create=True,
                return_value=False,
            ),
            patch("roll_terminal_qt.account_positions_home.time.strftime", return_value="12:00:00"),
        ):
            AccountPositionsHomeWidget._apply_position_history_payload(
                app,
                {"items": [history_item], "instruments": {}, "usdt_prices": {}},
            )

        inherit_notes.assert_called_once()
        app._save_position_notes.assert_called_once_with()

    @patch("roll_terminal_qt.account_positions_home.OrderHistoryFeedThread")
    def test_force_order_history_refresh_reuses_running_thread_without_waiting(
        self,
        thread_type: MagicMock,
    ) -> None:
        running_thread = SimpleNamespace(isRunning=lambda: True)
        app = SimpleNamespace(
            _runtime=SimpleNamespace(),
            _order_history_feed=running_thread,
            _stop_order_history_thread=MagicMock(),
            _clear_order_history_thread=MagicMock(),
            _private_thread_generation=1,
        )

        AccountPositionsHomeWidget._start_order_history_refresh(app, force_restart=True)

        app._stop_order_history_thread.assert_not_called()
        thread_type.assert_not_called()

    @patch("roll_terminal_qt.account_positions_home.OrderHistoryFeedThread")
    def test_start_order_history_refresh_uses_shared_store(self, thread_type: MagicMock) -> None:
        store = MagicMock()
        runtime = SimpleNamespace(environment="demo")
        app = SimpleNamespace(
            _runtime=runtime,
            _last_profile_name="moni",
            _shared_order_store=store,
            _order_history_feed=None,
            _private_thread_generation=1,
            _clear_order_history_thread=MagicMock(),
        )

        AccountPositionsHomeWidget._start_order_history_refresh(app)

        store.request_refresh.assert_called_once_with(runtime=runtime, profile_name="moni")
        thread_type.assert_not_called()

    @patch("roll_terminal_qt.account_positions_home._validate_protection_live_price_availability")
    def test_start_selected_position_protection_sets_runtime_notifier(self, validate_live_price: MagicMock) -> None:
        notifier = object()
        protection = SimpleNamespace(name="protection")
        runtime = SimpleNamespace(credentials=SimpleNamespace(name="credentials"))
        position = SimpleNamespace(inst_type="OPTION")
        manager = SimpleNamespace(set_notifier=MagicMock(), start=MagicMock())
        dialog = SimpleNamespace(
            _runtime_provider=lambda: runtime,
            _current_position=lambda: position,
            _build_selected_position_protection=lambda current: protection,
            _client=SimpleNamespace(),
            _notifier_provider=lambda: notifier,
            _build_strategy_config=lambda **kwargs: "strategy-config",
            _manager=manager,
            _safe_refresh_sessions=MagicMock(),
        )

        PositionProtectionDialog._start_selected_position_protection(dialog)

        manager.set_notifier.assert_called_once_with(notifier)
        manager.start.assert_called_once_with(runtime.credentials, "strategy-config", protection)
        dialog._safe_refresh_sessions.assert_called_once_with(context="start")
        validate_live_price.assert_called_once_with(dialog._client, protection, position)

    def test_refresh_sessions_table_shows_protection_configuration_columns(self) -> None:
        table = _ProtectionSessionTableStub()
        session = ProtectionSessionSnapshot(
            session_id="P01",
            api_name="QQzhangyong",
            option_inst_id="BTC-USD-260717-57000-P",
            trigger_inst_id="BTC-USDT",
            trigger_label="BTC-USDT 最新价",
            trigger_price_type="last",
            direction="long",
            pos_side=None,
            take_profit_trigger=None,
            take_profit_order_mode="mark_with_slippage",
            take_profit_order_price=None,
            take_profit_slippage=Decimal("0"),
            stop_loss_trigger=Decimal("62900"),
            stop_loss_order_mode="mark_with_slippage",
            stop_loss_order_price=None,
            stop_loss_slippage=Decimal("0"),
            poll_seconds=2,
            status="运行中",
            started_at=datetime(2026, 7, 6, 18, 29, 1),
            last_message="监控中",
        )
        dialog = SimpleNamespace(
            _manager=SimpleNamespace(list_sessions=lambda: [session]),
            _session_status_label=SimpleNamespace(setText=MagicMock()),
            _sessions_table=table,
            _session_ids=[],
            _selected_session_id=lambda: "",
            _detail_text=SimpleNamespace(setPlainText=MagicMock()),
            _refresh_selected_session_detail=MagicMock(),
        )

        PositionProtectionDialog._refresh_sessions(dialog)

        values = [table.items.get((0, column), SimpleNamespace(text=lambda: "")).text() for column in range(13)]
        self.assertEqual(
            values,
            [
                "QQzhangyong",
                "BTC-USD-260717-57000-P",
                "BTC-USDT 最新价",
                "BTC-USDT",
                "最新价",
                "long",
                "-",
                "-",
                "62900",
                "止盈/止损: 标记价格加减滑点/标记价格加减滑点",
                "2s",
                "运行中",
                "18:29:01",
            ],
        )

    def test_protection_dialog_safe_refresh_sessions_reports_exception_without_raising(self) -> None:
        reporter = MagicMock()
        dialog = SimpleNamespace(
            _refresh_sessions=MagicMock(side_effect=RuntimeError("boom")),
            _report_refresh_exception=reporter,
        )

        result = PositionProtectionDialog._safe_refresh_sessions(dialog, context="timer")

        self.assertFalse(result)
        reporter.assert_called_once()

    def test_protection_dialog_safe_refresh_sessions_calls_refresh_once_on_success(self) -> None:
        refresh = MagicMock()
        reporter = MagicMock()
        dialog = SimpleNamespace(
            _refresh_sessions=refresh,
            _report_refresh_exception=reporter,
        )

        result = PositionProtectionDialog._safe_refresh_sessions(dialog, context="timer")

        self.assertTrue(result)
        refresh.assert_called_once_with()
        reporter.assert_not_called()

    def test_protection_dialog_timer_refresh_continues_after_session_refresh_failure(self) -> None:
        dialog = SimpleNamespace(
            _safe_refresh_sessions=MagicMock(return_value=False),
            _safe_refresh_from_selection=MagicMock(return_value=True),
        )

        PositionProtectionDialog._on_refresh_timer_timeout(dialog)

        dialog._safe_refresh_sessions.assert_called_once_with(context="timer")
        dialog._safe_refresh_from_selection.assert_called_once_with(force=False, context="timer")

    def test_protection_dialog_safe_refresh_selected_session_detail_reports_exception_without_raising(self) -> None:
        reporter = MagicMock()
        dialog = SimpleNamespace(
            _refresh_selected_session_detail=MagicMock(side_effect=RuntimeError("detail boom")),
            _report_refresh_exception=reporter,
        )

        result = PositionProtectionDialog._safe_refresh_selected_session_detail(dialog, context="selection")

        self.assertFalse(result)
        reporter.assert_called_once()

    def test_protection_dialog_safe_trigger_source_change_reports_exception_without_raising(self) -> None:
        reporter = MagicMock()
        dialog = SimpleNamespace(
            _on_trigger_source_changed=MagicMock(side_effect=RuntimeError("trigger boom")),
            _report_refresh_exception=reporter,
        )

        result = PositionProtectionDialog._safe_handle_trigger_source_changed(dialog)

        self.assertFalse(result)
        reporter.assert_called_once()

    def test_protection_dialog_current_position_returns_cached_selection_when_provider_raises(self) -> None:
        cached = object()
        reporter = MagicMock()
        dialog = SimpleNamespace(
            _selected_option_provider=MagicMock(side_effect=RuntimeError("provider boom")),
            _selected_position=cached,
            _report_refresh_exception=reporter,
        )

        result = PositionProtectionDialog._current_position(dialog)

        self.assertIs(result, cached)
        reporter.assert_called_once()

    def test_protection_dialog_autofills_spot_trigger_prices_by_option_direction_rules(self) -> None:
        cases = (
            ("BTC-USD-260717-60000-C", "long", "63050", "62650"),
            ("BTC-USD-260717-60000-C", "short", "62650", "63050"),
            ("BTC-USD-260717-60000-P", "long", "62650", "63050"),
            ("BTC-USD-260717-60000-P", "short", "63050", "62650"),
        )
        for inst_id, pos_side, expected_tp, expected_sl in cases:
            with self.subTest(inst_id=inst_id, pos_side=pos_side):
                tp_edit = SimpleNamespace(text=lambda: "", setText=MagicMock())
                sl_edit = SimpleNamespace(text=lambda: "", setText=MagicMock())
                dialog = SimpleNamespace(
                    _client=SimpleNamespace(get_trigger_price=MagicMock(return_value=Decimal("62850"))),
                    _spot_symbol_edit=SimpleNamespace(text=lambda: "BTC-USDT"),
                    _tp_trigger_edit=tp_edit,
                    _sl_trigger_edit=sl_edit,
                    _current_position=lambda inst_id=inst_id, pos_side=pos_side: SimpleNamespace(
                        inst_id=inst_id,
                        pos_side=pos_side,
                        position=Decimal("1"),
                    ),
                )

                PositionProtectionDialog._maybe_autofill_spot_trigger_prices(dialog)

                dialog._client.get_trigger_price.assert_called_once_with("BTC-USDT", "last")
                tp_edit.setText.assert_called_once_with(expected_tp)
                sl_edit.setText.assert_called_once_with(expected_sl)

    def test_protection_dialog_autofill_does_not_override_existing_spot_trigger_prices(self) -> None:
        tp_edit = SimpleNamespace(text=lambda: "63000", setText=MagicMock())
        sl_edit = SimpleNamespace(text=lambda: "62000", setText=MagicMock())
        dialog = SimpleNamespace(
            _client=SimpleNamespace(get_trigger_price=MagicMock(return_value=Decimal("62850"))),
            _spot_symbol_edit=SimpleNamespace(text=lambda: "BTC-USDT"),
            _tp_trigger_edit=tp_edit,
            _sl_trigger_edit=sl_edit,
            _current_position=lambda: SimpleNamespace(inst_id="BTC-USD-260717-57000-P"),
        )

        PositionProtectionDialog._maybe_autofill_spot_trigger_prices(dialog)

        dialog._client.get_trigger_price.assert_not_called()
        tp_edit.setText.assert_not_called()
        sl_edit.setText.assert_not_called()

    def test_protection_dialog_autofill_updates_only_blank_spot_trigger_price(self) -> None:
        tp_edit = SimpleNamespace(text=lambda: "63000", setText=MagicMock())
        sl_edit = SimpleNamespace(text=lambda: "", setText=MagicMock())
        dialog = SimpleNamespace(
            _client=SimpleNamespace(get_trigger_price=MagicMock(return_value=Decimal("62850"))),
            _spot_symbol_edit=SimpleNamespace(text=lambda: "BTC-USDT"),
            _tp_trigger_edit=tp_edit,
            _sl_trigger_edit=sl_edit,
            _current_position=lambda: SimpleNamespace(
                inst_id="BTC-USD-260717-60000-C",
                pos_side="short",
                position=Decimal("1"),
            ),
        )

        PositionProtectionDialog._maybe_autofill_spot_trigger_prices(dialog)

        dialog._client.get_trigger_price.assert_called_once_with("BTC-USDT", "last")
        tp_edit.setText.assert_not_called()
        sl_edit.setText.assert_called_once_with("63050")

    @patch("roll_terminal_qt.account_positions_home.QMessageBox.warning")
    def test_protection_dialog_refresh_detail_does_not_open_modal_for_abnormal_session(
        self,
        warning: MagicMock,
    ) -> None:
        session = ProtectionSessionSnapshot(
            session_id="P01",
            api_name="QQzhangyong",
            option_inst_id="BTC-USD-260717-57000-P",
            trigger_inst_id="BTC-USDT",
            trigger_label="BTC-USDT 最新价",
            trigger_price_type="last",
            direction="long",
            pos_side=None,
            take_profit_trigger=None,
            take_profit_order_mode="mark_with_slippage",
            take_profit_order_price=None,
            take_profit_slippage=Decimal("0"),
            stop_loss_trigger=Decimal("62900"),
            stop_loss_order_mode="mark_with_slippage",
            stop_loss_order_price=None,
            stop_loss_slippage=Decimal("0"),
            poll_seconds=2,
            status="异常",
            started_at=datetime(2026, 7, 6, 18, 29, 1),
            last_message="保护任务异常：boom",
        )
        detail_text = SimpleNamespace(setPlainText=MagicMock())
        dialog = SimpleNamespace(
            _manager=SimpleNamespace(list_sessions=lambda: [session]),
            _selected_session_id=lambda: "P01",
            _detail_text=detail_text,
            _last_abnormal_protection_alert={},
        )

        PositionProtectionDialog._refresh_selected_session_detail(dialog)

        warning.assert_not_called()
        self.assertEqual(dialog._last_abnormal_protection_alert, {"P01": "保护任务异常：boom"})
        detail_text.setPlainText.assert_called_once()

    def test_configure_sessions_table_columns_applies_default_widths_for_long_fields(self) -> None:
        table = _ColumnConfigTableStub()

        PositionProtectionDialog._configure_sessions_table_columns(SimpleNamespace(_sessions_table=table), 13)

        self.assertFalse(table.horizontalHeader().stretch_last_section)
        self.assertEqual(table.column_widths[0], 120)
        self.assertEqual(table.column_widths[1], 210)
        self.assertEqual(table.column_widths[2], 170)
        self.assertEqual(table.column_widths[3], 120)
        self.assertEqual(table.column_widths[9], 230)
        self.assertEqual(table.column_widths[12], 96)

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

    def test_restart_live_feeds_for_manual_refresh_restarts_private_threads_without_waiting(self) -> None:
        app = SimpleNamespace(
            _stop_private_threads=MagicMock(),
            _start_private_threads=MagicMock(),
        )

        AccountPositionsHomeWidget._restart_live_feeds_for_manual_refresh(app)

        app._stop_private_threads.assert_called_once_with(wait_ms=0)
        app._start_private_threads.assert_called_once_with(force_restart=False, start_history=False)

    def test_refresh_view_uses_lightweight_manual_refresh_path(self) -> None:
        app = SimpleNamespace(
            _ensure_runtime_ready=lambda force_unlock=False: True,
            _status_badge=SimpleNamespace(setText=MagicMock()),
            _restart_live_feeds_for_manual_refresh=MagicMock(),
        )

        AccountPositionsHomeWidget.refresh_view(app)

        app._status_badge.setText.assert_called_once()
        app._restart_live_feeds_for_manual_refresh.assert_called_once_with()

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
