from __future__ import annotations

import unittest
from decimal import Decimal
from unittest.mock import patch

from okx_quant.models import Instrument
from roll_terminal_qt.account_service import FuturesPositionView
from roll_terminal_qt.execution_service import ProfessionalOpenExecutionPlan
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


class _ComboValue:
    def __init__(self, *, text: str = "", data: str = "") -> None:
        self._text = text
        self._data = data

    def currentText(self) -> str:
        return self._text

    def currentData(self):
        return self._data


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


if __name__ == "__main__":
    unittest.main()
