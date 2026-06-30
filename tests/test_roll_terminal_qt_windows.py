from __future__ import annotations

import unittest
from decimal import Decimal

from okx_quant.persistence import build_profile_switch_password_snapshot
from roll_terminal_qt.auto_channel_window import _safe_text as auto_safe_text
from roll_terminal_qt.line_trading_window import (
    _build_annotation_key,
    _compute_rr_target,
    _safe_text as line_safe_text,
    _split_annotation_key,
)
from roll_terminal_qt.profile_access import profile_requires_password
from roll_terminal_qt.smart_order_window import _safe_text as smart_safe_text


class RollTerminalQtWindowHelperTests(unittest.TestCase):
    def test_split_annotation_key_supports_standard_triplet(self) -> None:
        self.assertEqual(
            _split_annotation_key("api1|BTC-USDT-SWAP|1H"),
            ("api1", "BTC-USDT-SWAP", "1H"),
        )

    def test_build_annotation_key_normalizes_symbol(self) -> None:
        self.assertEqual(
            _build_annotation_key("api1", "btc-usdt-swap", "1H"),
            "api1|BTC-USDT-SWAP|1H",
        )

    def test_safe_text_normalizes_empty_values(self) -> None:
        for func in (line_safe_text, smart_safe_text, auto_safe_text):
            self.assertEqual(func(None), "-")
            self.assertEqual(func(""), "-")
            self.assertEqual(func("  ok  "), "ok")

    def test_compute_rr_target_supports_long_and_short(self) -> None:
        self.assertEqual(
            _compute_rr_target("long", Decimal("100"), Decimal("95"), Decimal("2")),
            Decimal("110"),
        )
        self.assertEqual(
            _compute_rr_target("short", Decimal("100"), Decimal("105"), Decimal("2")),
            Decimal("90"),
        )

    def test_profile_requires_password_detects_protected_payload(self) -> None:
        payload = build_profile_switch_password_snapshot("secret-1")
        self.assertTrue(profile_requires_password("api1", {"api1": payload}))
        self.assertFalse(profile_requires_password("api2", {"api1": payload}))


if __name__ == "__main__":
    unittest.main()
