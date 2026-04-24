from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_ID, STRATEGY_DYNAMIC_LONG_ID
from okx_quant.trader_desk import TraderDeskSnapshot, TraderDraftRecord, TraderSlotRecord
from okx_quant.trader_desk_ui import (
    TraderDeskWindow,
    _build_trader_strategy_lines,
    _draft_status_label,
    _draft_status_value,
    _draft_template_identity,
    _gate_condition_label,
    _gate_condition_value,
    _normalize_draft_form_values,
    _payload_bar,
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
                "bar": "15m",
                "ema_period": 21,
                "trend_ema_period": 55,
                "big_ema_period": 233,
                "atr_period": 10,
                "atr_stop_multiplier": "2",
                "atr_take_multiplier": "4",
                "risk_amount": "10",
                "poll_seconds": 10,
                "run_mode": "trade",
                "signal_mode": "long_only",
                "take_profit_mode": "dynamic",
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

    def test_draft_status_label_round_trip_uses_chinese_labels(self) -> None:
        label = _draft_status_label("ready")

        self.assertEqual(label, "可启动")
        self.assertEqual(_draft_status_value(label), "ready")

    def test_payload_bar_reads_snapshot_bar(self) -> None:
        self.assertEqual(_payload_bar(self._payload()), "15m")

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

    def test_build_trader_strategy_lines_include_bar_and_runtime(self) -> None:
        draft = TraderDraftRecord(
            trader_id="T001",
            template_payload=self._payload(),
            total_quota=Decimal("1"),
            unit_quota=Decimal("0.1"),
            quota_steps=10,
        )

        lines = _build_trader_strategy_lines(
            draft,
            runtime_snapshot={
                "session_id": "S04",
                "runtime_status": "等待信号",
                "last_message": "当前无法生成挂单 | EMA21 仍在 EMA55 下方",
                "started_at": datetime(2026, 4, 24, 11, 0, 53),
                "is_running": True,
            },
        )
        text = "\n".join(lines)

        self.assertIn("K线周期：15m", text)
        self.assertIn("交易员固定数量：0.1", text)
        self.assertIn("当前 watcher：", text)
        self.assertIn("会话：S04 | 状态：等待信号 | 线程：运行中", text)

    def test_delete_selected_draft_auto_pauses_watching_trader_and_marks_pending_delete(self) -> None:
        draft = TraderDraftRecord(
            trader_id="T001",
            template_payload=self._payload(),
            total_quota=Decimal("1"),
            unit_quota=Decimal("0.1"),
            quota_steps=10,
        )
        watching_slot = TraderSlotRecord(
            slot_id="slot-1",
            trader_id="T001",
            session_id="S01",
            api_name="moni",
            strategy_name="EMA",
            symbol="BTC-USDT-SWAP",
            status="watching",
        )
        window = SimpleNamespace(
            window=object(),
            _snapshot=TraderDeskSnapshot(drafts=[draft], slots=[watching_slot]),
            _pending_delete_trader_id="",
            _selected_draft=lambda: draft,
            _selected_run_status=lambda trader_id: "running",
            _draft_deleter=MagicMock(side_effect=ValueError("该交易员仍有活动中的额度格，请先暂停或平仓。")),
            _trader_pauser=MagicMock(),
            _append_log=MagicMock(),
            _refresh_views=MagicMock(),
        )

        with patch("okx_quant.trader_desk_ui.messagebox.showinfo") as showinfo, patch(
            "okx_quant.trader_desk_ui.messagebox.showerror"
        ) as showerror:
            TraderDeskWindow.delete_selected_draft(window)

        window._trader_pauser.assert_called_once_with("T001")
        self.assertEqual(window._pending_delete_trader_id, "T001")
        window._refresh_views.assert_called_once_with(select_id="T001")
        showinfo.assert_called_once()
        showerror.assert_not_called()
