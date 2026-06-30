from __future__ import annotations

import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from okx_quant.models import Instrument
from roll_terminal_qt.account_service import FuturesPositionView
from roll_terminal_qt.execution_service import ProfessionalCloseExecutionPlan, ProfessionalOpenExecutionPlan
from roll_terminal_qt.ui import RollTerminalWindow


class _TextField:
    def __init__(self, value: str = "") -> None:
        self._value = value

    def text(self) -> str:
        return self._value

    def setText(self, value: str) -> None:
        self._value = value


class _Label:
    def __init__(self) -> None:
        self.visible = None
        self.value = ""

    def setVisible(self, visible: bool) -> None:
        self.visible = visible

    def setText(self, value: str) -> None:
        self.value = value


class _NoopWidget:
    def setVisible(self, _visible: bool) -> None:
        pass

    def setEnabled(self, _enabled: bool) -> None:
        pass

    def setText(self, _value: str) -> None:
        pass


class _CheckStub(_NoopWidget):
    def __init__(self, checked: bool = False, enabled: bool = True) -> None:
        self._checked = checked
        self._enabled = enabled

    def isEnabled(self) -> bool:
        return self._enabled

    def isChecked(self) -> bool:
        return self._checked

    def setChecked(self, checked: bool) -> None:
        self._checked = checked

    def setEnabled(self, enabled: bool) -> None:
        self._enabled = enabled

    def blockSignals(self, _blocked: bool) -> None:
        pass


class _ComboValue:
    def __init__(self, *, text: str = "", data: str = "") -> None:
        self._text = text
        self._data = data

    def currentText(self) -> str:
        return self._text

    def currentData(self):
        return self._data


class _TableItem:
    def __init__(self, value: str = "") -> None:
        self._value = value

    def text(self) -> str:
        return self._value

    def setText(self, value: str) -> None:
        self._value = value


class _TableStub:
    def __init__(self, rows: list[list[_TableItem | None]]) -> None:
        self._rows = rows

    def rowCount(self) -> int:
        return len(self._rows)

    def item(self, row: int, column: int):
        return self._rows[row][column]


class RollTerminalUiTests(unittest.TestCase):
    def _build_window(self) -> RollTerminalWindow:
        return RollTerminalWindow.__new__(RollTerminalWindow)

    def test_default_open_qty_text_uses_requested_defaults(self) -> None:
        window = self._build_window()
        window._open_qty_unit_value = lambda: "coin"

        self.assertEqual(window._default_open_qty_text("coin"), "1")
        self.assertEqual(window._default_open_qty_text("usdt"), "10000")
        self.assertEqual(window._default_open_qty_text("contracts"), "100")
        self.assertEqual(window._default_open_qty_text(), "1")

    def test_apply_open_qty_default_updates_legacy_default_and_keeps_custom_value(self) -> None:
        window = self._build_window()
        window._is_open_mode = lambda: True
        window._open_qty_unit_value = lambda: "usdt"
        window._qty = _TextField("10")

        window._apply_open_qty_default(force=False)
        self.assertEqual(window._qty.text(), "10000")

        window._qty.setText("25000")
        window._apply_open_qty_default(force=False)
        self.assertEqual(window._qty.text(), "25000")

        window._apply_open_qty_default(force=True)
        self.assertEqual(window._qty.text(), "10000")

    def test_open_estimated_batch_text_uses_count_mode_contract_split(self) -> None:
        window = self._build_window()
        window._selected_batch_mode = lambda: "count"
        window._batch_count = _TextField("4")
        window._batch_qty = _TextField("")

        fake_instrument = Instrument(
            inst_id="BTC-USD-260925",
            inst_type="FUTURES",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            ct_val=Decimal("100"),
            ct_mult=Decimal("1"),
            ct_val_ccy="USD",
        )

        class _FakeClient:
            def get_instrument(self, inst_id: str, *, prefer_cached: bool = False) -> Instrument:
                self._last = (inst_id, prefer_cached)
                return fake_instrument

        with patch("roll_terminal_qt.ui.OkxRestClient", return_value=_FakeClient()):
            text = window._open_estimated_batch_text(Decimal("100"), "BTC-USD-260925")

        self.assertEqual(text, "预计拆单：按 4 批，预计拆成 4 批，每批 25 张。")

    def test_update_open_estimate_preview_shows_expected_size_and_batch_summary(self) -> None:
        window = self._build_window()
        window._open_estimate_title = _Label()
        window._open_estimate_text = _Label()
        window._qty = _TextField("10000")
        window._batch_count = _TextField("4")
        window._batch_qty = _TextField("")
        window._is_open_mode = lambda: True
        window._selected_professional_open_legs = lambda: ("BTC-USDT", "BTC-USD-260925")
        window._open_qty_unit_value = lambda: "usdt"
        window._snapshot_leg_mid = lambda _inst_id: Decimal("80000")
        window._preview_open_size = lambda **_kwargs: (Decimal("0.125"), Decimal("100"), Decimal("10000"))
        window._selected_batch_mode = lambda: "count"

        fake_instrument = Instrument(
            inst_id="BTC-USD-260925",
            inst_type="FUTURES",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            ct_val=Decimal("100"),
            ct_mult=Decimal("1"),
            ct_val_ccy="USD",
        )

        class _FakeClient:
            def get_instrument(self, inst_id: str, *, prefer_cached: bool = False) -> Instrument:
                self._last = (inst_id, prefer_cached)
                return fake_instrument

        with patch("roll_terminal_qt.ui.OkxRestClient", return_value=_FakeClient()):
            window._update_open_estimate_preview()

        self.assertTrue(window._open_estimate_title.visible)
        self.assertTrue(window._open_estimate_text.visible)
        self.assertIn("参考现货价：80000", window._open_estimate_text.value)
        self.assertIn("预计现货买入：0.125 BTC", window._open_estimate_text.value)
        self.assertIn("预计合约卖出：100 张", window._open_estimate_text.value)
        self.assertIn("预计名义金额：10000 U", window._open_estimate_text.value)
        self.assertIn("预计拆单：按 4 批，预计拆成 4 批，每批 25 张。", window._open_estimate_text.value)

    def test_update_open_estimate_preview_shows_default_guidance_when_size_missing(self) -> None:
        window = self._build_window()
        window._open_estimate_title = _Label()
        window._open_estimate_text = _Label()
        window._qty = _TextField("")
        window._is_open_mode = lambda: True
        window._selected_professional_open_legs = lambda: ("BTC-USDT", "BTC-USD-260925")
        window._open_qty_unit_value = lambda: "contracts"

        window._update_open_estimate_preview()

        self.assertIn("默认值：按币数 1，按金额(U) 10000，按合约张数 100。", window._open_estimate_text.value)

    def test_build_professional_open_plan_does_not_pass_close_only_field(self) -> None:
        window = self._build_window()
        window._selected_opportunity = type(
            "_Item",
            (),
            {
                "left_inst_id": "BTC-USDT",
                "right_inst_id": "BTC-USD-260925",
            },
        )()
        window._selected_professional_open_legs = lambda: ("BTC-USDT", "BTC-USD-260925")
        window._open_qty_unit_value = lambda: "coin"
        window._selected_batch_mode = lambda: "count"
        window._qty = _TextField("1")
        window._max_slippage = _TextField("0.15")
        window._batch_count = _TextField("10")
        window._batch_qty = _TextField("")
        window._maker_wait = _TextField("6")
        window._chase_limit = _TextField("3")
        window._current_limit_price = _TextField("")
        window._target_limit_price = _TextField("")
        window._mode = _ComboValue(text="双方挂单/先成交后市价", data="both_maker_first_taker")
        window._use_limit_orders = type("_Check", (), {"isChecked": lambda self: False})()

        plan = window._build_professional_open_plan()

        self.assertIsInstance(plan, ProfessionalOpenExecutionPlan)
        self.assertEqual(plan.size_value, Decimal("1"))
        self.assertEqual(plan.size_unit, "coin")

    def test_position_spot_text_prefers_cached_matching_spot_balance(self) -> None:
        window = self._build_window()
        window._spot_balance_lookup = {"BTC": "BTC-USDT | 可用 0.52 BTC | 余额 0.73 BTC"}
        position = FuturesPositionView(
            position_key="BTC-USD-260925|short",
            inst_id="BTC-USD-260925",
            inst_type="FUTURES",
            side="short",
            available=Decimal("17.6"),
            contracts=Decimal("17.6"),
            api_available=Decimal("17.6"),
            api_contracts=Decimal("17.6"),
            lot_size=Decimal("1"),
            notional_base=Decimal("0.02946736"),
            contract_value=Decimal("100"),
            contract_value_ccy="USD",
            notional_value=Decimal("1760"),
            label="demo",
        )

        text = window._position_spot_text(position)

        self.assertEqual(text, "对应现货：BTC-USDT | 可用 0.52 BTC | 余额 0.73 BTC")

    def test_position_spot_text_falls_back_to_waiting_when_cache_missing(self) -> None:
        window = self._build_window()
        window._spot_balance_lookup = {}
        position = FuturesPositionView(
            position_key="BTC-USD-260925|short",
            inst_id="BTC-USD-260925",
            inst_type="FUTURES",
            side="short",
            available=Decimal("17.6"),
            contracts=Decimal("17.6"),
            api_available=Decimal("17.6"),
            api_contracts=Decimal("17.6"),
            lot_size=Decimal("1"),
            notional_base=Decimal("0.02946736"),
            contract_value=Decimal("100"),
            contract_value_ccy="USD",
            notional_value=Decimal("1760"),
            label="demo",
        )

        text = window._position_spot_text(position)

        self.assertEqual(text, "对应现货：BTC-USDT 等待账户余额...")

    def test_apply_spot_balances_updates_selected_label_and_table_column(self) -> None:
        window = self._build_window()
        window._spot_balance_lookup = {}
        window._position_spot = _Label()
        window._positions_table = _TableStub(
            [[_TableItem("BTC-USD-260925"), None, None, None, _TableItem("BTC-USDT")]]
        )
        position = FuturesPositionView(
            position_key="BTC-USD-260925|short",
            inst_id="BTC-USD-260925",
            inst_type="FUTURES",
            side="short",
            available=Decimal("17.6"),
            contracts=Decimal("17.6"),
            api_available=Decimal("17.6"),
            api_contracts=Decimal("17.6"),
            lot_size=Decimal("1"),
            notional_base=Decimal("0.02946736"),
            contract_value=Decimal("100"),
            contract_value_ccy="USD",
            notional_value=Decimal("1760"),
            label="demo",
        )
        window._selected_position = lambda: position

        window._apply_spot_balances({"BTC": "BTC-USDT | 可用 0.52 BTC | 余额 0.73 BTC"})

        self.assertEqual(window._position_spot.value, "对应现货：BTC-USDT | 可用 0.52 BTC | 余额 0.73 BTC")
        assert window._positions_table.item(0, 4) is not None
        self.assertEqual(window._positions_table.item(0, 4).text(), "BTC-USDT | 可用 0.52 BTC | 余额 0.73 BTC")

    def test_auto_monitor_supported_allows_close_mode_when_context_exists(self) -> None:
        window = self._build_window()
        window._runtime = object()
        window._is_roll_template_active = lambda: False
        window._is_close_mode = lambda: True
        window._selected_supports_professional_close = lambda: True

        self.assertTrue(window._auto_monitor_supported())

    def test_current_auto_condition_text_uses_lte_for_close_mode(self) -> None:
        window = self._build_window()
        window._current = _ComboValue(text="BTC-USD-260925", data="")
        window._target = _ComboValue(text="BTC-USDT", data="")
        window._current_inst_id = lambda: "BTC-USD-260925"
        window._is_close_mode = lambda: True

        text = window._current_auto_condition_text(Decimal("450"))

        self.assertIn("<= 450", text)

    def test_auto_trigger_met_uses_lte_for_close_mode(self) -> None:
        window = self._build_window()
        window._is_close_mode = lambda: True

        self.assertTrue(window._auto_trigger_met(Decimal("400"), Decimal("450")))
        self.assertFalse(window._auto_trigger_met(Decimal("500"), Decimal("450")))

    def test_build_auto_live_text_shows_realtime_value_threshold_and_state(self) -> None:
        window = self._build_window()
        window._runtime = object()
        window._execution_thread = None
        window._auto_enabled = True
        window._auto_triggered = False
        window._auto_threshold_value = Decimal("450")
        window._auto_threshold = _TextField("450")
        window._is_roll_template_active = lambda: False
        window._is_close_mode = lambda: True
        window._selected_supports_professional_close = lambda: True
        window._current = _ComboValue(text="BTC-USD-260925", data="")
        window._target = _ComboValue(text="BTC-USDT", data="")
        window._current_inst_id = lambda: "BTC-USD-260925"
        snapshot = SimpleNamespace(
            current=SimpleNamespace(inst_id="BTC-USD-260925", bid=Decimal("59950"), ask=Decimal("60050"), last=None),
            target=SimpleNamespace(inst_id="BTC-USDT", bid=Decimal("60350"), ask=Decimal("60450"), last=None),
            spread_abs=Decimal("400"),
        )

        text = window._build_auto_live_text(snapshot)

        self.assertIn("当前价差 400", text)
        self.assertIn("目标阈值 <= 450", text)
        self.assertIn("状态：监控中（已满足）", text)

    def test_parse_batch_execution_log_extracts_batch_phase_and_message(self) -> None:
        parsed = RollTerminalWindow._parse_batch_execution_log("分批平仓：第 2/5 批，目标 0.3 张")

        self.assertEqual(parsed, ("批次 2/5", "目标 0.3 张"))

    def test_refresh_template_controls_close_mode_uses_lte_threshold_label(self) -> None:
        window = self._build_window()
        window._auto_enabled = False
        window._execution_thread = None
        window._runtime = object()
        window._selected_opportunity = None
        window._selected_professional_open_legs = lambda: None
        window._selected_professional_close_context = lambda: ("BTC-USDT", "BTC-USD-260925", object(), [object()])
        window._is_roll_template_active = lambda: False
        window._is_close_mode = lambda: True
        window._apply_execution_mode_options = lambda _options: None
        window._update_auto_controls_v2 = lambda: None
        window._update_qty_unit_hint = lambda: None
        window._apply_open_qty_default = lambda force=False: None
        window._update_open_estimate_preview = lambda: None
        window._apply_book_pair_lock_ui = lambda: None
        window._open_qty_unit_label = _NoopWidget()
        window._open_qty_unit = _NoopWidget()
        window._close_profit_spot = _CheckStub()
        window._current_label_widget = _Label()
        window._target_label_widget = _Label()
        window._mode_label_widget = _Label()
        window._manual_title = _Label()
        window._current_limit_price_label = _Label()
        window._target_limit_price_label = _Label()
        window._auto_title = _Label()
        window._auto_threshold_label = _Label()
        window._start_auto_button = _Label()
        window._stop_auto_button = _Label()
        window._auto_intro = _Label()
        window._auto_help = _Label()
        window._auto_hint = _Label()
        window._guide_text = _Label()
        window._current = _NoopWidget()
        window._target = _NoopWidget()
        window._switch_button = _NoopWidget()
        window._mode = _ComboValue(text="双腿吃单", data="dual_taker")
        window._use_limit_orders = _CheckStub()
        window._limit_order_help = _Label()
        window._limit_order_preference = False
        window._execution_table = type("_Table", (), {"setHorizontalHeaderLabels": lambda self, _labels: None})()
        window._manual_hint = _Label()
        window._execution_scope_hint = _Label()
        window._execute_button = _Label()

        window._refresh_template_controls_v2()

        self.assertEqual(window._auto_threshold_label.value, "触发价差<=")

    def test_build_professional_close_plan_allows_live_position_without_ledger_entries(self) -> None:
        window = self._build_window()
        window._selected_opportunity = type(
            "_Item",
            (),
            {
                "left_inst_id": "BTC-USD-260925",
                "right_inst_id": "BTC-USDT",
            },
        )()
        position = FuturesPositionView(
            position_key="BTC-USD-260925|short",
            inst_id="BTC-USD-260925",
            inst_type="FUTURES",
            side="short",
            available=Decimal("17.6"),
            contracts=Decimal("17.6"),
            api_available=Decimal("17.6"),
            api_contracts=Decimal("17.6"),
            lot_size=Decimal("0.1"),
            notional_base=Decimal("0.02946736"),
            contract_value=Decimal("100"),
            contract_value_ccy="USD",
            notional_value=Decimal("1760"),
            label="demo",
        )
        window._selected_professional_close_context = lambda: ("BTC-USDT", "BTC-USD-260925", position, [])
        window._selected_batch_mode = lambda: "count"
        window._qty = _TextField("10")
        window._max_slippage = _TextField("0.15")
        window._batch_count = _TextField("2")
        window._batch_qty = _TextField("")
        window._maker_wait = _TextField("6")
        window._chase_limit = _TextField("3")
        window._current_limit_price = _TextField("")
        window._target_limit_price = _TextField("")
        window._mode = _ComboValue(text="鍙岃吙鍚冨崟", data="dual_taker")
        window._use_limit_orders = type("_Check", (), {"isChecked": lambda self: False})()
        window._close_profit_spot = _CheckStub(checked=True)

        plan = window._build_professional_close_plan()

        self.assertIsInstance(plan, ProfessionalCloseExecutionPlan)
        self.assertEqual(plan.entry_ids, ())
        self.assertEqual(plan.qty_contracts, Decimal("10"))
        self.assertEqual(plan.position_available_contracts, Decimal("17.6"))
        self.assertFalse(plan.close_profit_spot)

    def test_batch_preview_text_for_close_plan_uses_qty_contracts(self) -> None:
        window = self._build_window()
        plan = ProfessionalCloseExecutionPlan(
            left_inst_id="BTC-USD-260925",
            right_inst_id="BTC-USDT",
            spot_inst_id="BTC-USDT",
            derivative_inst_id="BTC-USD-260925",
            entry_ids=("e1",),
            qty_contracts=Decimal("10"),
            execution_label="双腿吃单",
            batch_count=2,
            batch_contract_qty=None,
        )

        text = window._batch_preview_text_for_plan(plan)

        self.assertIn("2 批", text)

    def test_batch_preview_text_for_close_plan_uses_derivative_lot_size(self) -> None:
        window = self._build_window()
        plan = ProfessionalCloseExecutionPlan(
            left_inst_id="BTC-USD-260925",
            right_inst_id="BTC-USDT",
            spot_inst_id="BTC-USDT",
            derivative_inst_id="BTC-USD-260925",
            entry_ids=("e1",),
            qty_contracts=Decimal("0.3"),
            execution_label="双腿吃单",
            batch_count=3,
            batch_contract_qty=None,
            derivative_lot_size=Decimal("0.1"),
        )

        text = window._batch_preview_text_for_plan(plan)

        self.assertIn("3 批", text)
        self.assertIn("0.1", text)

    def test_batch_preview_text_for_open_contract_plan_uses_derivative_lot_size(self) -> None:
        window = self._build_window()
        plan = ProfessionalOpenExecutionPlan(
            left_inst_id="BTC-USDT",
            right_inst_id="BTC-USD-260925",
            spot_inst_id="BTC-USDT",
            derivative_inst_id="BTC-USD-260925",
            size_value=Decimal("0.3"),
            size_unit="contracts",
            execution_label="双腿吃单",
            batch_count=3,
            batch_contract_qty=None,
            derivative_lot_size=Decimal("0.1"),
        )

        text = window._batch_preview_text_for_plan(plan)

        self.assertIn("3 批", text)
        self.assertIn("0.1", text)

    def test_build_auto_execution_context_supports_close_mode(self) -> None:
        window = self._build_window()
        window._runtime = object()
        window._is_roll_template_active = lambda: False
        window._is_close_mode = lambda: True
        window._current = _ComboValue(text="BTC-USD-260925", data="")
        window._target = _ComboValue(text="BTC-USDT", data="")
        plan = ProfessionalCloseExecutionPlan(
            left_inst_id="BTC-USD-260925",
            right_inst_id="BTC-USDT",
            spot_inst_id="BTC-USDT",
            derivative_inst_id="BTC-USD-260925",
            entry_ids=("e1",),
            qty_contracts=Decimal("10"),
            execution_label="双腿吃单",
            batch_count=1,
            batch_contract_qty=None,
        )
        window._build_professional_close_plan = lambda: plan
        window._batch_preview_text_for_plan = lambda _plan: "1 批：每批 10 张"

        context = window._build_auto_execution_context(threshold=Decimal("450"), for_trigger=False)

        assert context is not None
        self.assertEqual(context["task_label"], "套利平仓")


if __name__ == "__main__":
    unittest.main()
