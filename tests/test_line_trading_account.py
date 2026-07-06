from __future__ import annotations

import unittest
from decimal import Decimal
from types import SimpleNamespace

from roll_terminal_qt.line_trading_account import (
    _fmt,
    build_rr_order_intent,
    build_runtime_from_profile_payload,
    position_row_cells,
)


class LineTradingAccountFormattingTests(unittest.TestCase):
    def test_position_row_cells_matches_legacy_tk_columns(self) -> None:
        position = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            pos_side="long",
            position=Decimal("2.5000"),
            avg_price=Decimal("100000.0000"),
            mark_price=Decimal("100100.1200"),
            upl=Decimal("25.5000"),
        )

        self.assertEqual(
            position_row_cells(position),
            ["BTC-USDT-SWAP", "long", "2.5", "100000", "100100.12", "25.5"],
        )

    def test_fmt_uses_dash_for_none(self) -> None:
        self.assertEqual(_fmt(None), "-")

    def test_fmt_strips_decimal_trailing_zeros_without_scientific_notation(self) -> None:
        self.assertEqual(_fmt(Decimal("1.2300")), "1.23")
        self.assertEqual(_fmt(Decimal("1E+3")), "1000")
        self.assertEqual(_fmt(Decimal("0E-8")), "0")
        self.assertEqual(_fmt(Decimal("-0.0000")), "0")
        self.assertEqual(_fmt(Decimal("123456789012345678901234567890.1234500")), "123456789012345678901234567890.12345")
        self.assertEqual(_fmt(Decimal("NaN")), "-")
        self.assertEqual(_fmt(Decimal("Infinity")), "-")

    def test_position_row_cells_supports_mapping_and_missing_fields(self) -> None:
        self.assertEqual(
            position_row_cells({"inst_id": "ETH-USDT-SWAP", "position": Decimal("-0.0000")}),
            ["ETH-USDT-SWAP", "-", "0", "-", "-", "-"],
        )

    def test_build_rr_order_intent_uses_selected_rr_for_long(self) -> None:
        intent = build_rr_order_intent(
            symbol="BTC-USDT-SWAP",
            side="long",
            entry_price=Decimal("60000"),
            stop_price=Decimal("59000"),
            take_profit=Decimal("62000"),
            risk_usdt=Decimal("100"),
            order_mode="limit",
        )
        self.assertEqual(intent["inst_id"], "BTC-USDT-SWAP")
        self.assertEqual(intent["direction"], "long")
        self.assertEqual(intent["entry_price"], Decimal("60000"))
        self.assertEqual(intent["stop_price"], Decimal("59000"))
        self.assertEqual(intent["take_profit"], Decimal("62000"))
        self.assertEqual(intent["risk_usdt"], Decimal("100"))
        self.assertEqual(intent["order_mode"], "limit")

    def test_build_rr_order_intent_rejects_invalid_inputs(self) -> None:
        with self.assertRaisesRegex(ValueError, "side"):
            build_rr_order_intent(
                symbol="BTC-USDT-SWAP",
                side="bad",
                entry_price=Decimal("60000"),
                stop_price=Decimal("59000"),
                take_profit=Decimal("62000"),
                risk_usdt=Decimal("100"),
                order_mode="limit",
            )
        with self.assertRaisesRegex(ValueError, "risk_usdt"):
            build_rr_order_intent(
                symbol="BTC-USDT-SWAP",
                side="long",
                entry_price=Decimal("60000"),
                stop_price=Decimal("59000"),
                take_profit=Decimal("62000"),
                risk_usdt=Decimal("0"),
                order_mode="limit",
            )

    def test_build_runtime_from_profile_payload_prefers_profile_environment(self) -> None:
        runtime = build_runtime_from_profile_payload(
            profile_name="demo-a",
            payload={
                "api_key": "k",
                "secret_key": "s",
                "passphrase": "p",
                "environment": "live",
            },
            notification_snapshot={
                "environment_label": "模拟盘 demo",
                "trade_mode_label": "逐仓 isolated",
                "position_mode_label": "双向 long_short",
            },
        )
        self.assertEqual(runtime.credentials.profile_name, "demo-a")
        self.assertEqual(runtime.environment, "live")
        self.assertEqual(runtime.trade_mode, "isolated")
        self.assertEqual(runtime.position_mode, "long_short")

    def test_build_runtime_from_profile_payload_requires_complete_credentials(self) -> None:
        with self.assertRaisesRegex(ValueError, "API"):
            build_runtime_from_profile_payload(
                profile_name="demo-a",
                payload={"api_key": "", "secret_key": "s", "passphrase": "p"},
                notification_snapshot={},
            )


if __name__ == "__main__":
    unittest.main()
