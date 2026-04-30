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
    _build_trader_book_ledger_rows,
    _build_trader_book_summary_rows,
    _build_trader_strategy_lines,
    _draft_status_label,
    _draft_status_value,
    _draft_template_identity,
    _gate_effective_price_inputs,
    _gate_field_ui_state,
    _gate_condition_label,
    _gate_condition_value,
    _normalize_draft_form_values,
    _format_optional_compact_price,
    _payload_bar,
    _replace_text_preserving_scroll,
    _run_status_label,
    _slot_status_label,
    _should_reload_draft_form,
    _trader_book_summary_text,
    _trader_current_session_label,
    _trader_primary_session_id,
    _symbol_asset_text,
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

    def test_gate_field_ui_state_for_above_only_enables_lower_input(self) -> None:
        lower_label, lower_state, upper_label, upper_state = _gate_field_ui_state("高于 >=")

        self.assertEqual(lower_label, "触发价 >=")
        self.assertEqual(lower_state, "normal")
        self.assertEqual(upper_label, "上限（不填）")
        self.assertEqual(upper_state, "disabled")

    def test_gate_effective_price_inputs_drop_irrelevant_bounds(self) -> None:
        self.assertEqual(_gate_effective_price_inputs("above", "95000", "99999"), ("95000", ""))
        self.assertEqual(_gate_effective_price_inputs("below", "90000", "88000"), ("", "88000"))
        self.assertEqual(_gate_effective_price_inputs("always", "1", "2"), ("", ""))

    def test_draft_status_label_round_trip_uses_chinese_labels(self) -> None:
        label = _draft_status_label("ready")

        self.assertEqual(label, "可启动")
        self.assertEqual(_draft_status_value(label), "ready")

    def test_run_status_label_uses_chinese_text(self) -> None:
        self.assertEqual(_run_status_label("paused_loss"), "亏损暂停")
        self.assertEqual(_run_status_label("quota_exhausted"), "额度耗尽")

    def test_slot_status_label_uses_chinese_text(self) -> None:
        self.assertEqual(_slot_status_label("watching"), "等待开仓")
        self.assertEqual(_slot_status_label("open"), "持仓中")
        self.assertEqual(_slot_status_label("closed_loss"), "亏损平仓")
        self.assertEqual(_slot_status_label("closed_profit"), "盈利平仓")
        self.assertEqual(_slot_status_label("closed_manual"), "人工结束")
        self.assertEqual(_slot_status_label("stopped"), "观察结束（未开仓）")
        self.assertEqual(_slot_status_label("failed"), "异常结束")
        self.assertEqual(
            _slot_status_label("closed_loss", close_reason="策略主动平仓", net_pnl=Decimal("-0.01")),
            "止盈净亏",
        )
        self.assertEqual(
            _slot_status_label("closed_profit", close_reason="策略主动平仓", net_pnl=Decimal("0.02")),
            "止盈净盈",
        )
        self.assertEqual(
            _slot_status_label("closed_loss", close_reason="OKX止损触发", net_pnl=Decimal("-0.08")),
            "止损平仓",
        )

    def test_payload_bar_reads_snapshot_bar(self) -> None:
        self.assertEqual(_payload_bar(self._payload()), "15m")

    def test_format_optional_compact_price_limits_repeating_decimals(self) -> None:
        self.assertEqual(_format_optional_compact_price(Decimal("2289.3833333333333333")), "2289.3833")
        self.assertEqual(_format_optional_compact_price(Decimal("85.8300")), "85.83")

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

    def test_trader_current_session_label_prefers_armed_session_and_shows_extra_count(self) -> None:
        snapshot = TraderDeskSnapshot(
            slots=[
                TraderSlotRecord(
                    slot_id="slot-1",
                    trader_id="T001",
                    session_id="S01",
                    api_name="moni",
                    strategy_name="EMA",
                    symbol="BTC-USDT-SWAP",
                    status="open",
                ),
                TraderSlotRecord(
                    slot_id="slot-2",
                    trader_id="T001",
                    session_id="S02",
                    api_name="moni",
                    strategy_name="EMA",
                    symbol="BTC-USDT-SWAP",
                    status="watching",
                ),
            ],
            runs=[SimpleNamespace(trader_id="T001", armed_session_id="S03")],
        )

        self.assertEqual(_trader_current_session_label(snapshot, "T001"), "S03 +2")
        self.assertEqual(_trader_current_session_label(snapshot, "T999"), "-")

    def test_trader_primary_session_id_prefers_armed_session(self) -> None:
        snapshot = TraderDeskSnapshot(
            slots=[
                TraderSlotRecord(
                    slot_id="slot-1",
                    trader_id="T001",
                    session_id="S01",
                    api_name="moni",
                    strategy_name="EMA",
                    symbol="BTC-USDT-SWAP",
                    status="open",
                ),
            ],
            runs=[SimpleNamespace(trader_id="T001", armed_session_id="S03")],
        )
        self.assertEqual(_trader_primary_session_id(snapshot, "T001"), "S03")
        self.assertEqual(_trader_primary_session_id(snapshot, "T999"), "")

    def test_symbol_asset_text_extracts_base_currency(self) -> None:
        self.assertEqual(_symbol_asset_text(self._payload(symbol="ETH-USDT-SWAP")), "ETH")

    def test_trader_book_summary_text_formats_global_totals(self) -> None:
        from okx_quant.trader_desk import TraderBookSummary

        text = _trader_book_summary_text(
            TraderBookSummary(
                trader_count=3,
                profitable_trader_count=1,
                losing_trader_count=1,
                flat_trader_count=1,
                realized_count=5,
                win_count=3,
                loss_count=1,
                manual_count=1,
                net_pnl=Decimal("1.23"),
            )
        )

        self.assertIn("交易员 3 名", text)
        self.assertIn("已平仓 5 单", text)
        self.assertIn("盈利交易员 1", text)
        self.assertIn("总净盈亏 1.23", text)

    def test_build_trader_book_summary_rows_include_direction_and_rate(self) -> None:
        draft = TraderDraftRecord(
            trader_id="T001",
            template_payload=self._payload(symbol="ETH-USDT-SWAP"),
            total_quota=Decimal("1"),
            unit_quota=Decimal("0.1"),
            quota_steps=10,
        )
        snapshot = TraderDeskSnapshot(
            drafts=[draft],
            slots=[
                TraderSlotRecord(
                    slot_id="slot-profit",
                    trader_id="T001",
                    session_id="S01",
                    api_name="moni",
                    strategy_name="EMA",
                    symbol="ETH-USDT-SWAP",
                    bar="1m",
                    direction_label="只做多",
                    status="closed_profit",
                    closed_at=datetime(2026, 4, 26, 8, 0, 0),
                    net_pnl=Decimal("0.25"),
                ),
                TraderSlotRecord(
                    slot_id="slot-loss",
                    trader_id="T001",
                    session_id="S02",
                    api_name="moni",
                    strategy_name="EMA",
                    symbol="ETH-USDT-SWAP",
                    bar="1m",
                    direction_label="只做多",
                    status="closed_loss",
                    closed_at=datetime(2026, 4, 26, 8, 5, 0),
                    net_pnl=Decimal("-0.10"),
                ),
            ],
        )

        rows = _build_trader_book_summary_rows(snapshot)

        self.assertEqual(len(rows), 1)
        _, values = rows[0]
        self.assertEqual(values[0], "T001")
        self.assertEqual(values[3], "只做多")
        self.assertEqual(values[5], 2)
        self.assertEqual(values[9], "50%")
        self.assertEqual(values[10], "0.15")

    def test_build_trader_book_ledger_rows_include_slot_metadata(self) -> None:
        snapshot = TraderDeskSnapshot(
            drafts=[
                TraderDraftRecord(
                    trader_id="T006",
                    template_payload=self._payload(symbol="ETH-USDT-SWAP"),
                    total_quota=Decimal("1"),
                    unit_quota=Decimal("0.1"),
                    quota_steps=10,
                )
            ],
            slots=[
                TraderSlotRecord(
                    slot_id="T006-slot-1",
                    trader_id="T006",
                    session_id="S25",
                    api_name="moni",
                    strategy_name="EMA",
                    symbol="ETH-USDT-SWAP",
                    bar="1m",
                    direction_label="只做多",
                    status="closed_loss",
                    opened_at=datetime(2026, 4, 26, 3, 10, 34),
                    closed_at=datetime(2026, 4, 26, 3, 29, 23),
                    entry_price=Decimal("2310.59"),
                    exit_price=Decimal("2308.49"),
                    size=Decimal("0.1"),
                    net_pnl=Decimal("-0.01"),
                    close_reason="策略主动平仓",
                )
            ],
        )

        rows = _build_trader_book_ledger_rows(snapshot)

        self.assertEqual(len(rows), 1)
        row_id, trader_id, values = rows[0]
        self.assertEqual(row_id, "T006-slot-1")
        self.assertEqual(trader_id, "T006")
        self.assertEqual(values[1], "T006")
        self.assertEqual(values[4], "只做多")
        self.assertEqual(values[5], "T006-slot-1")
        self.assertEqual(values[8], "止盈净亏")
        self.assertEqual(values[13], "-0.01")

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

    def test_flatten_selected_trader_asks_for_mode_and_dispatches_best_quote(self) -> None:
        draft = TraderDraftRecord(
            trader_id="T001",
            template_payload=self._payload(),
            total_quota=Decimal("1"),
            unit_quota=Decimal("0.1"),
            quota_steps=10,
        )
        captured: dict[str, object] = {}
        flattener = MagicMock()
        window = SimpleNamespace(
            window=object(),
            _selected_draft=lambda: draft,
            _trader_flattener=flattener,
            _run_action=lambda action, title, success_message: captured.update(
                action=action,
                title=title,
                success_message=success_message,
            ),
        )

        with patch("okx_quant.trader_desk_ui.messagebox.askyesnocancel", return_value=False) as chooser:
            TraderDeskWindow.flatten_selected_trader(window)

        chooser.assert_called_once()
        self.assertEqual(captured["title"], "平仓")
        self.assertIn("挂买一/卖一平仓", str(captured["success_message"]))
        captured["action"]("T001")
        flattener.assert_called_once_with("T001", "best_quote")

    def test_force_cleanup_selected_trader_confirms_then_runs_force_cleaner(self) -> None:
        draft = TraderDraftRecord(
            trader_id="T001",
            template_payload=self._payload(),
            total_quota=Decimal("1"),
            unit_quota=Decimal("0.1"),
            quota_steps=10,
        )
        open_slot = TraderSlotRecord(
            slot_id="slot-1",
            trader_id="T001",
            session_id="S01",
            api_name="moni",
            strategy_name="EMA",
            symbol="BTC-USDT-SWAP",
            status="open",
            quota_occupied=True,
        )
        window = SimpleNamespace(
            window=object(),
            _snapshot=TraderDeskSnapshot(drafts=[draft], slots=[open_slot]),
            _selected_draft=lambda: draft,
            _trader_force_cleaner=MagicMock(),
            _clear_pending_delete=MagicMock(),
            _append_log=MagicMock(),
            _refresh_views=MagicMock(),
        )

        with patch("okx_quant.trader_desk_ui.messagebox.askyesno", return_value=True) as askyesno, patch(
            "okx_quant.trader_desk_ui.messagebox.showerror"
        ) as showerror:
            TraderDeskWindow.force_cleanup_selected_trader(window)

        askyesno.assert_called_once()
        window._trader_force_cleaner.assert_called_once_with("T001")
        window._clear_pending_delete.assert_called_once()
        window._refresh_views.assert_called_once_with(select_id="T001")
        showerror.assert_not_called()

    def test_delete_selected_draft_auto_force_cleans_when_only_hidden_residual_state_blocks_delete(self) -> None:
        draft = TraderDraftRecord(
            trader_id="T004",
            template_payload=self._payload(),
            total_quota=Decimal("1"),
            unit_quota=Decimal("0.1"),
            quota_steps=10,
        )
        delete_calls = {"count": 0}

        def _delete(_trader_id: str) -> None:
            delete_calls["count"] += 1
            if delete_calls["count"] == 1:
                raise ValueError("该交易员仍有关联会话在运行，请先暂停或平仓。")

        window = SimpleNamespace(
            window=object(),
            _snapshot=TraderDeskSnapshot(drafts=[draft], slots=[]),
            _pending_delete_trader_id="",
            _selected_draft=lambda: draft,
            _selected_run_status=lambda trader_id: "paused_manual",
            _draft_deleter=MagicMock(side_effect=_delete),
            _trader_force_cleaner=MagicMock(),
            _trader_pauser=MagicMock(),
            _clear_pending_delete=MagicMock(),
            _append_log=MagicMock(),
            _refresh_views=MagicMock(),
        )

        with patch("okx_quant.trader_desk_ui.messagebox.showinfo") as showinfo, patch(
            "okx_quant.trader_desk_ui.messagebox.showerror"
        ) as showerror:
            TraderDeskWindow.delete_selected_draft(window)

        self.assertEqual(delete_calls["count"], 2)
        window._trader_force_cleaner.assert_called_once_with("T004")
        window._trader_pauser.assert_not_called()
        window._clear_pending_delete.assert_called_once_with("T004")
        window._refresh_views.assert_called_once_with()
        showinfo.assert_called_once()
        showerror.assert_not_called()

    def test_refresh_slot_tree_includes_exit_price_column_value(self) -> None:
        class _FakeTree:
            def __init__(self) -> None:
                self.rows: dict[str, tuple[object, ...]] = {}

            def get_children(self):
                return list(self.rows.keys())

            def delete(self, item_id: str) -> None:
                self.rows.pop(item_id, None)

            def insert(self, parent: str, index: object, iid: str, values: tuple[object, ...]) -> None:
                self.rows[iid] = values

        closed_slot = TraderSlotRecord(
            slot_id="slot-1",
            trader_id="T001",
            session_id="S11",
            api_name="moni",
            strategy_name="EMA",
            symbol="BTC-USDT-SWAP",
            status="closed_profit",
            opened_at=datetime(2026, 4, 25, 3, 1, 13),
            closed_at=datetime(2026, 4, 25, 6, 46, 28),
            entry_price=Decimal("77734.1"),
            exit_price=Decimal("78034.1"),
            size=Decimal("0.1"),
            net_pnl=Decimal("0.30"),
            close_reason="策略主动平仓",
        )
        window = SimpleNamespace(
            slot_tree=_FakeTree(),
            _selected_trader_id=lambda: "T001",
            _snapshot=TraderDeskSnapshot(slots=[closed_slot]),
        )

        TraderDeskWindow._refresh_slot_tree(window)

        values = window.slot_tree.rows["slot-1"]
        self.assertEqual(values[1], "止盈净盈")
        self.assertEqual(values[8], "78034.1")
        self.assertEqual(values[9], "0.30")

    def test_replace_text_preserving_scroll_restores_yview(self) -> None:
        class _FakeText:
            def __init__(self, y_position: float = 0.0) -> None:
                self.content = ""
                self.y_position = y_position

            def yview(self) -> tuple[float, float]:
                return (self.y_position, min(self.y_position + 0.2, 1.0))

            def yview_moveto(self, fraction: float) -> None:
                self.y_position = fraction

            def delete(self, _start: str, _end: str) -> None:
                self.content = ""
                self.y_position = 0.0

            def insert(self, _index: str, text: str) -> None:
                self.content = text
                self.y_position = 0.0

        widget = _FakeText(y_position=0.62)

        _replace_text_preserving_scroll(widget, "第一行\n第二行")

        self.assertEqual(widget.content, "第一行\n第二行")
        self.assertEqual(widget.y_position, 0.62)

    def test_refresh_event_text_preserves_scroll_position(self) -> None:
        class _FakeText:
            def __init__(self, y_position: float = 0.0) -> None:
                self.content = ""
                self.y_position = y_position

            def yview(self) -> tuple[float, float]:
                return (self.y_position, min(self.y_position + 0.2, 1.0))

            def yview_moveto(self, fraction: float) -> None:
                self.y_position = fraction

            def delete(self, _start: str, _end: str) -> None:
                self.content = ""
                self.y_position = 0.0

            def insert(self, _index: str, text: str) -> None:
                self.content = text
                self.y_position = 0.0

        event_text = _FakeText(y_position=0.55)
        window = SimpleNamespace(
            event_text=event_text,
            _selected_trader_id=lambda: "T001",
            _snapshot=TraderDeskSnapshot(
                events=[
                    SimpleNamespace(
                        trader_id="T001",
                        created_at=datetime(2026, 4, 26, 7, 46, 0),
                        level="info",
                        message="当前无法生成挂单",
                    )
                ]
            ),
        )

        TraderDeskWindow._refresh_event_text(window)

        self.assertEqual(event_text.y_position, 0.55)
        self.assertIn("当前无法生成挂单", event_text.content)

    def test_refresh_detail_text_preserves_scroll_position(self) -> None:
        class _FakeText:
            def __init__(self, y_position: float = 0.0) -> None:
                self.content = ""
                self.y_position = y_position

            def yview(self) -> tuple[float, float]:
                return (self.y_position, min(self.y_position + 0.2, 1.0))

            def yview_moveto(self, fraction: float) -> None:
                self.y_position = fraction

            def delete(self, _start: str, _end: str) -> None:
                self.content = ""
                self.y_position = 0.0

            def insert(self, _index: str, text: str) -> None:
                self.content = text
                self.y_position = 0.0

        class _FakeStringVar:
            def __init__(self) -> None:
                self.value = ""

            def set(self, value: str) -> None:
                self.value = value

        draft = TraderDraftRecord(
            trader_id="T001",
            template_payload=self._payload(),
            total_quota=Decimal("1"),
            unit_quota=Decimal("0.1"),
            quota_steps=10,
        )
        detail_text = _FakeText(y_position=0.48)
        summary_text = _FakeStringVar()
        window = SimpleNamespace(
            detail_text=detail_text,
            _summary_text=summary_text,
            _selected_draft=lambda: draft,
            _selected_run_status=lambda trader_id: "running",
            _selected_run_reason=lambda trader_id: "",
            _runtime_snapshot_provider=lambda trader_id: {
                "session_id": "S33",
                "runtime_status": "等待信号",
            },
            _refresh_slot_tree=MagicMock(),
            _snapshot=TraderDeskSnapshot(drafts=[draft], slots=[]),
        )

        TraderDeskWindow._refresh_detail_text(window)

        self.assertEqual(detail_text.y_position, 0.48)
        self.assertIn("交易员：T001", detail_text.content)
        self.assertIn("当前 watcher：", detail_text.content)
        self.assertIn("等待信号", detail_text.content)
