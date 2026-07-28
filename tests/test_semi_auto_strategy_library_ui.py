from unittest import TestCase
from unittest.mock import MagicMock, patch

from okx_quant.semi_auto_strategy_library_ui import (
    SemiAutoStrategyLibraryDialog,
    build_semi_auto_strategy_library_rows,
    build_semi_auto_strategy_parameter_payload,
)
from okx_quant.strategy_catalog import (
    STRATEGY_BODY_RETEST_SHORT_ID,
    STRATEGY_DEFINITIONS,
    STRATEGY_DYNAMIC_MTF_LONG_ID,
)


class _Value:
    def __init__(self, value: object) -> None:
        self.value = value

    def get(self) -> object:
        return self.value


class SemiAutoStrategyLibraryUiTest(TestCase):
    def test_strategy_library_rows_keep_full_builtin_order(self) -> None:
        rows = build_semi_auto_strategy_library_rows(STRATEGY_DEFINITIONS)

        self.assertEqual([strategy_id for strategy_id, _values in rows], [item.strategy_id for item in STRATEGY_DEFINITIONS])
        self.assertIn(STRATEGY_DYNAMIC_MTF_LONG_ID, [strategy_id for strategy_id, _values in rows])
        self.assertIn(STRATEGY_BODY_RETEST_SHORT_ID, [strategy_id for strategy_id, _values in rows])
        self.assertTrue(all(len(values) == 3 for _strategy_id, values in rows))

    def test_parameter_payload_keeps_selected_strategy_api_and_values(self) -> None:
        payload = build_semi_auto_strategy_parameter_payload(
            "ema_dynamic_order_long",
            api_name="real",
            values={"symbol": "BTC-USDT-SWAP", "bar": "1H"},
        )

        self.assertEqual(payload["strategy_id"], "ema_dynamic_order_long")
        self.assertEqual(payload["api_name"], "real")
        self.assertEqual(payload["symbol"], "BTC-USDT-SWAP")
        self.assertEqual(payload["bar"], "1H")

    def test_confirm_uses_independent_draft_and_pool_api(self) -> None:
        dialog = SemiAutoStrategyLibraryDialog.__new__(SemiAutoStrategyLibraryDialog)
        dialog._selected_strategy_id = "ema_dynamic_order_long"
        dialog._api_name = "real"
        dialog._draft_vars = {"symbol": _Value("ETH-USDT-SWAP"), "bar": _Value("1H")}
        dialog._template_builder = MagicMock(return_value={"record": "built"})
        dialog._on_confirm = MagicMock()
        dialog.window = MagicMock()

        dialog._confirm()

        dialog._template_builder.assert_called_once_with(
            "ema_dynamic_order_long",
            {"symbol": "ETH-USDT-SWAP", "bar": "1H"},
            "real",
        )
        dialog._on_confirm.assert_called_once_with({"record": "built"})
        dialog.window.destroy.assert_called_once_with()

    def test_confirm_validation_failure_shows_error_without_confirming(self) -> None:
        dialog = SemiAutoStrategyLibraryDialog.__new__(SemiAutoStrategyLibraryDialog)
        dialog._selected_strategy_id = "ema_dynamic_order_long"
        dialog._api_name = "real"
        dialog._draft_vars = {"symbol": _Value("")}
        dialog._template_builder = MagicMock(side_effect=ValueError("请选择交易标的。"))
        dialog._on_confirm = MagicMock()
        dialog.window = MagicMock()

        with patch("okx_quant.semi_auto_strategy_library_ui.messagebox.showerror") as showerror:
            dialog._confirm()

        showerror.assert_called_once()
        dialog._on_confirm.assert_not_called()
        dialog.window.destroy.assert_not_called()
