from __future__ import annotations

from unittest import TestCase

from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_ID, STRATEGY_DYNAMIC_LONG_ID
from okx_quant.trader_desk_ui import (
    _draft_template_identity,
    _gate_condition_label,
    _gate_condition_value,
    _normalize_draft_form_values,
    _should_reload_draft_form,
    _validate_trader_desk_payload,
)


class TraderDeskHelpersTest(TestCase):
    @staticmethod
    def _payload(*, strategy_id: str = STRATEGY_DYNAMIC_LONG_ID, symbol: str = "BTC-USDT-SWAP") -> dict[str, object]:
        return {
            "strategy_id": strategy_id,
            "strategy_name": "EMA 动态委托做多",
            "api_name": "api1",
            "direction_label": "只做多",
            "run_mode_label": "交易并下单",
            "symbol": symbol,
            "exported_at": "2026-04-24T08:00:00",
            "app_version": "0.4.06",
            "config_snapshot": {
                "strategy_id": strategy_id,
                "inst_id": symbol,
                "trade_inst_id": symbol,
                "run_mode": "trade",
                "signal_mode": "long_only",
            },
        }

    def test_validate_trader_desk_payload_accepts_supported_strategy(self) -> None:
        payload = self._payload()

        validated = _validate_trader_desk_payload(payload)

        self.assertIs(validated, payload)

    def test_validate_trader_desk_payload_rejects_unsupported_strategy(self) -> None:
        payload = self._payload(strategy_id=STRATEGY_DYNAMIC_ID)

        with self.assertRaisesRegex(ValueError, "暂不支持加入交易员管理台"):
            _validate_trader_desk_payload(payload)

    def test_draft_template_identity_ignores_export_metadata_but_tracks_symbol(self) -> None:
        first = self._payload(symbol="BTC-USDT-SWAP")
        second = self._payload(symbol="BTC-USDT-SWAP")
        second["exported_at"] = "2026-04-24T08:15:00"
        second["app_version"] = "0.4.99"
        third = self._payload(symbol="ETH-USDT-SWAP")

        self.assertEqual(_draft_template_identity(first), _draft_template_identity(second))
        self.assertNotEqual(_draft_template_identity(first), _draft_template_identity(third))

    def test_normalize_draft_form_values_normalizes_valid_inputs(self) -> None:
        normalized = _normalize_draft_form_values(" 1.000 ", "0.2500", "05", " ready ")

        self.assertEqual(normalized, ("1", "0.25", "5", "ready"))

    def test_normalize_draft_form_values_rejects_unit_larger_than_total(self) -> None:
        with self.assertRaisesRegex(ValueError, "单次额度不能大于总额度"):
            _normalize_draft_form_values("1", "2", "3", "draft")

    def test_normalize_draft_form_values_rejects_invalid_step_count(self) -> None:
        with self.assertRaisesRegex(ValueError, "额度次数必须是正整数"):
            _normalize_draft_form_values("1", "0.1", "1.5", "draft")

    def test_gate_condition_label_round_trip_uses_chinese_symbols(self) -> None:
        label = _gate_condition_label("between")

        self.assertEqual(label, "区间内 [下限, 上限]")
        self.assertEqual(_gate_condition_value(label), "between")

    def test_should_reload_draft_form_skips_same_selection_while_dirty(self) -> None:
        should_reload = _should_reload_draft_form(
            explicit_select_id=None,
            selected_id="T001",
            loaded_trader_id="T001",
            form_dirty=True,
        )

        self.assertFalse(should_reload)

    def test_should_reload_draft_form_reloads_when_selection_changes(self) -> None:
        should_reload = _should_reload_draft_form(
            explicit_select_id=None,
            selected_id="T002",
            loaded_trader_id="T001",
            form_dirty=True,
        )

        self.assertTrue(should_reload)
