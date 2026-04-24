from __future__ import annotations

from decimal import Decimal
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from okx_quant.models import StrategyConfig
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_SHORT_ID
from okx_quant.trader_desk import TraderDraftRecord, TraderRunState, TraderSlotRecord
from okx_quant.ui import (
    ProfilePositionSnapshot,
    QuantApp,
    RefreshHealthState,
    StrategyHistoryRecord,
    StrategyStopCleanupResult,
    StrategyTradeReconciliationSnapshot,
    StrategyTradeRuntimeState,
    _build_strategy_template_payload,
    _coerce_log_file_path,
    _format_network_error_message,
    _infer_session_runtime_status,
    _mark_refresh_health_failure,
    _mark_refresh_health_success,
    _refresh_indicator_badge_text,
    _refresh_health_is_stale,
    _resolve_import_api_profile,
    _session_order_prefixes,
    _strategy_template_record_from_payload,
    _trade_order_belongs_to_session,
    _trade_order_session_role,
)


class UiHelpersTest(TestCase):
    def test_format_network_error_message_read_timeout(self) -> None:
        self.assertEqual(
            _format_network_error_message("The read operation timed out"),
            "\u7f51\u7edc\u8bfb\u53d6\u8d85\u65f6\uff0c\u8bf7\u7a0d\u540e\u91cd\u8bd5\u3002",
        )

    def test_format_network_error_message_handshake_timeout(self) -> None:
        self.assertEqual(
            _format_network_error_message("_ssl.c:983: The handshake operation timed out"),
            "\u7f51\u7edc\u63e1\u624b\u8d85\u65f6\uff0c\u8bf7\u7a0d\u540e\u91cd\u8bd5\u3002",
        )

    def test_format_network_error_message_remote_end_closed(self) -> None:
        self.assertEqual(
            _format_network_error_message("Remote end closed connection without response"),
            "\u4ea4\u6613\u6240\u63d0\u524d\u65ad\u5f00\u8fde\u63a5\uff0c\u8bf7\u7a0d\u540e\u91cd\u8bd5\u3002",
        )

    def test_format_network_error_message_summarizes_cloudflare_html_502(self) -> None:
        message = """
HTTP 502: <!DOCTYPE html>
<html lang="en-US">
<head><title>okx.com | 502: Bad gateway</title></head>
<body>
<span class="code-label">Error code 502</span>
<span class="leading-1.3 text-2xl text-red-error">Error</span>
<span class="cf-footer-item sm:block sm:mb-1">Cloudflare Ray ID: <strong class="font-semibold">9edcdbc80bb66da2</strong></span>
</body>
</html>
"""
        summary = _format_network_error_message(message)
        self.assertIn("HTTP 502", summary)
        self.assertIn("OKX\u6e90\u7ad9\u5f02\u5e38", summary)
        self.assertIn("RayID=9edcdbc80bb66da2", summary)
        self.assertNotIn("<!DOCTYPE html>", summary)

    def test_coerce_log_file_path_returns_none_for_blank_value(self) -> None:
        self.assertIsNone(_coerce_log_file_path("   "))

    def test_coerce_log_file_path_returns_path_for_non_blank_value(self) -> None:
        self.assertEqual(
            _coerce_log_file_path(r"D:\qqokx\logs\strategy_sessions\2026-04-17\session.log"),
            Path(r"D:\qqokx\logs\strategy_sessions\2026-04-17\session.log"),
        )

    def test_refresh_health_marks_stale_after_three_failures_with_cached_success(self) -> None:
        state = RefreshHealthState("\u6301\u4ed3", last_success_at=datetime(2026, 4, 18, 12, 0, 0))

        _mark_refresh_health_failure(state, "HTTP 502", at=datetime(2026, 4, 18, 12, 0, 10))
        self.assertFalse(_refresh_health_is_stale(state))
        _mark_refresh_health_failure(state, "HTTP 502", at=datetime(2026, 4, 18, 12, 0, 20))
        self.assertFalse(_refresh_health_is_stale(state))
        _mark_refresh_health_failure(state, "HTTP 502", at=datetime(2026, 4, 18, 12, 0, 30))

        self.assertTrue(_refresh_health_is_stale(state))
        self.assertEqual(state.consecutive_failures, 3)
        self.assertEqual(state.last_error_summary, "HTTP 502")
        self.assertEqual(state.stale_since, datetime(2026, 4, 18, 12, 0, 30))

    def test_refresh_health_success_resets_failure_and_stale_flags(self) -> None:
        state = RefreshHealthState(
            "\u5f53\u524d\u59d4\u6258",
            last_success_at=datetime(2026, 4, 18, 12, 0, 0),
            consecutive_failures=3,
            stale_since=datetime(2026, 4, 18, 12, 0, 30),
            last_error_summary="HTTP 502",
        )

        _mark_refresh_health_success(state, at=datetime(2026, 4, 18, 12, 1, 0))

        self.assertFalse(_refresh_health_is_stale(state))
        self.assertEqual(state.last_success_at, datetime(2026, 4, 18, 12, 1, 0))
        self.assertEqual(state.consecutive_failures, 0)
        self.assertIsNone(state.stale_since)
        self.assertIsNone(state.last_error_summary)

    def test_refresh_indicator_badge_text_transitions_from_idle_to_warning_to_stale(self) -> None:
        state = RefreshHealthState("\u6301\u4ed3")
        self.assertEqual(_refresh_indicator_badge_text(state), "\u672a\u8bfb")

        _mark_refresh_health_success(state, at=datetime(2026, 4, 18, 12, 0, 0))
        self.assertEqual(_refresh_indicator_badge_text(state), "\u6b63\u5e38")

        _mark_refresh_health_failure(state, "HTTP 502", at=datetime(2026, 4, 18, 12, 0, 10))
        self.assertEqual(_refresh_indicator_badge_text(state), "\u544a\u8b66 x1")

        _mark_refresh_health_failure(state, "HTTP 502", at=datetime(2026, 4, 18, 12, 0, 20))
        _mark_refresh_health_failure(state, "HTTP 502", at=datetime(2026, 4, 18, 12, 0, 30))
        self.assertEqual(_refresh_indicator_badge_text(state), "\u8fc7\u671f x3")

    def test_infer_session_runtime_status_maps_entry_and_position_phases(self) -> None:
        self.assertEqual(_infer_session_runtime_status("准备挂单 | 方向=LONG"), "开仓监控中")
        self.assertEqual(_infer_session_runtime_status("挂单已提交到 OKX | ordId=1"), "开仓监控中")
        self.assertEqual(_infer_session_runtime_status("开始监控 OKX 动态止损 | 标的=BTC-USDT-SWAP"), "持仓监控中")
        self.assertEqual(_infer_session_runtime_status("当前无法生成挂单 | 趋势未确认"), "等待信号")

    def test_infer_session_runtime_status_preserves_existing_phase_during_okx_retry(self) -> None:
        self.assertEqual(
            _infer_session_runtime_status("OKX 读取异常，准备重试 | 操作=读取持仓 SWAP", "持仓监控中"),
            "持仓监控中",
        )

    def test_infer_session_runtime_status_maps_startup_gate_and_round_completion_to_waiting(self) -> None:
        self.assertEqual(_infer_session_runtime_status("启动默认不追老信号 | 方向=LONG"), "等待信号")
        self.assertEqual(_infer_session_runtime_status("启动追单窗口已过期，当前不追单 | 方向=LONG"), "等待信号")
        self.assertEqual(_infer_session_runtime_status("本轮持仓已结束，继续监控下一次信号。"), "等待信号")

    def test_trade_order_session_role_matches_regular_strategy_order_prefix(self) -> None:
        session = SimpleNamespace(
            session_id="S01",
            strategy_id="ema_dynamic_order",
            strategy_name="EMA 动态委托做多",
        )
        prefix = _session_order_prefixes(session)[0]
        order = SimpleNamespace(
            client_order_id=f"{prefix}ent041816084800001",
            algo_client_order_id="",
        )

        self.assertEqual(_trade_order_session_role(order, session), "ent")
        self.assertTrue(_trade_order_belongs_to_session(order, session))

    def test_trade_order_session_role_matches_protective_algo_order_prefix(self) -> None:
        session = SimpleNamespace(
            session_id="S01",
            strategy_id="ema_dynamic_order",
            strategy_name="EMA 动态委托做多",
        )
        prefix = _session_order_prefixes(session)[0]
        order = SimpleNamespace(
            client_order_id="",
            algo_client_order_id=f"{prefix}slg041816084800001",
        )

        self.assertEqual(_trade_order_session_role(order, session), "slg")
        self.assertTrue(_trade_order_belongs_to_session(order, session))

    def test_trade_order_session_role_does_not_match_other_session(self) -> None:
        session = SimpleNamespace(
            session_id="S01",
            strategy_id="ema_dynamic_order",
            strategy_name="EMA 动态委托做多",
        )
        other_session = SimpleNamespace(
            session_id="S02",
            strategy_id="ema_dynamic_order",
            strategy_name="EMA 动态委托做多",
        )
        other_prefix = _session_order_prefixes(other_session)[0]
        order = SimpleNamespace(
            client_order_id=f"{other_prefix}ent041816084800001",
            algo_client_order_id="",
        )

        self.assertIsNone(_trade_order_session_role(order, session))
        self.assertFalse(_trade_order_belongs_to_session(order, session))

    def test_session_can_be_cleared_only_when_stopped_and_not_running(self) -> None:
        stopped = SimpleNamespace(status="\u5df2\u505c\u6b62", engine=SimpleNamespace(is_running=False))
        stopping = SimpleNamespace(status="\u505c\u6b62\u4e2d", engine=SimpleNamespace(is_running=False))
        running = SimpleNamespace(status="\u8fd0\u884c\u4e2d", engine=SimpleNamespace(is_running=True))

        self.assertTrue(QuantApp._session_can_be_cleared(stopped))
        self.assertFalse(QuantApp._session_can_be_cleared(stopping))
        self.assertFalse(QuantApp._session_can_be_cleared(running))

    def test_next_session_selection_after_clear_prefers_existing_selected_session(self) -> None:
        self.assertEqual(
            QuantApp._next_session_selection_after_clear("S02", ("S01", "S02", "S03")),
            "S02",
        )

    def test_next_session_selection_after_clear_falls_back_to_first_remaining(self) -> None:
        self.assertEqual(
            QuantApp._next_session_selection_after_clear("S02", ("S03", "S04")),
            "S03",
        )

    def test_next_session_selection_after_clear_returns_none_when_empty(self) -> None:
        self.assertIsNone(QuantApp._next_session_selection_after_clear("S02", ()))

    def test_session_blocks_history_deletion_when_running_or_stopping(self) -> None:
        running = SimpleNamespace(status="\u8fd0\u884c\u4e2d", engine=SimpleNamespace(is_running=True))
        stopping = SimpleNamespace(status="\u505c\u6b62\u4e2d", engine=SimpleNamespace(is_running=False))
        stopped = SimpleNamespace(status="\u5df2\u505c\u6b62", engine=SimpleNamespace(is_running=False))

        self.assertTrue(QuantApp._session_blocks_history_deletion(running))
        self.assertTrue(QuantApp._session_blocks_history_deletion(stopping))
        self.assertFalse(QuantApp._session_blocks_history_deletion(stopped))

    def test_next_history_selection_after_mutation_prefers_existing_selected_record(self) -> None:
        self.assertEqual(
            QuantApp._next_history_selection_after_mutation("R02", ("R01", "R02", "R03")),
            "R02",
        )

    def test_next_history_selection_after_mutation_falls_back_to_first_remaining(self) -> None:
        self.assertEqual(
            QuantApp._next_history_selection_after_mutation("R02", ("R03", "R04")),
            "R03",
        )

    def test_next_history_selection_after_mutation_returns_none_when_empty(self) -> None:
        self.assertIsNone(QuantApp._next_history_selection_after_mutation("R02", ()))


class StrategyTemplateImportExportTest(TestCase):
    def test_strategy_template_payload_round_trip_preserves_config(self) -> None:
        config = StrategyConfig(
            inst_id="SOL-USDT-SWAP",
            bar="1H",
            ema_period=21,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("58.82"),
            trade_mode="cross",
            signal_mode="short_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            trend_ema_period=55,
            big_ema_period=233,
            strategy_id=STRATEGY_DYNAMIC_SHORT_ID,
            poll_seconds=10.0,
            risk_amount=Decimal("10"),
            trade_inst_id="SOL-USDT-SWAP",
            tp_sl_mode="exchange",
            local_tp_sl_inst_id=None,
            entry_side_mode="follow_signal",
            run_mode="trade",
            take_profit_mode="dynamic",
            max_entries_per_trend=1,
            entry_reference_ema_period=55,
            dynamic_two_r_break_even=True,
            dynamic_fee_offset_enabled=True,
            startup_chase_window_seconds=300,
            time_stop_break_even_enabled=False,
            time_stop_break_even_bars=0,
        )
        session = SimpleNamespace(
            api_name="moni",
            strategy_id=STRATEGY_DYNAMIC_SHORT_ID,
            strategy_name="EMA 动态委托做空",
            direction_label="只做空",
            run_mode_label="交易并下单",
            symbol="SOL-USDT-SWAP",
            config=config,
        )

        payload = _build_strategy_template_payload(session)
        record = _strategy_template_record_from_payload(payload)

        self.assertIsNotNone(record)
        assert record is not None
        self.assertFalse(payload["includes_credentials"])
        self.assertEqual(record.api_name, "moni")
        self.assertEqual(record.strategy_id, STRATEGY_DYNAMIC_SHORT_ID)
        self.assertEqual(record.strategy_name, "EMA 动态委托做空")
        self.assertEqual(record.direction_label, "只做空")
        self.assertEqual(record.symbol, "SOL-USDT-SWAP")
        self.assertEqual(record.config.startup_chase_window_seconds, 300)
        self.assertEqual(record.config.trade_inst_id, "SOL-USDT-SWAP")
        self.assertEqual(record.config.take_profit_mode, "dynamic")

    def test_resolve_import_api_profile_prefers_matching_profile(self) -> None:
        self.assertEqual(
            _resolve_import_api_profile("moni", "local", {"local", "moni"}),
            ("moni", "已自动切换到导出文件里的 API：moni"),
        )

    def test_apply_strategy_template_record_keeps_current_api_when_source_profile_missing(self) -> None:
        payload = {
            "strategy_id": STRATEGY_DYNAMIC_SHORT_ID,
            "strategy_name": "EMA 动态委托做空",
            "api_name": "remote",
            "direction_label": "只做空",
            "run_mode_label": "交易并下单",
            "symbol": "SOL-USDT-SWAP",
            "config_snapshot": {
                "inst_id": "SOL-USDT-SWAP",
                "bar": "1H",
                "ema_period": 21,
                "atr_period": 10,
                "atr_stop_multiplier": "2",
                "atr_take_multiplier": "4",
                "order_size": "58.82",
                "trade_mode": "cross",
                "signal_mode": "short_only",
                "position_mode": "net",
                "environment": "demo",
                "tp_sl_trigger_type": "mark",
                "trend_ema_period": 55,
                "big_ema_period": 233,
                "strategy_id": STRATEGY_DYNAMIC_SHORT_ID,
                "poll_seconds": 10,
                "risk_amount": "10",
                "trade_inst_id": "SOL-USDT-SWAP",
                "tp_sl_mode": "exchange",
                "entry_side_mode": "follow_signal",
                "run_mode": "trade",
                "take_profit_mode": "dynamic",
                "max_entries_per_trend": 1,
                "entry_reference_ema_period": 55,
                "dynamic_two_r_break_even": True,
                "dynamic_fee_offset_enabled": True,
                "startup_chase_window_seconds": 0,
                "time_stop_break_even_enabled": False,
                "time_stop_break_even_bars": 0,
            },
        }
        record = _strategy_template_record_from_payload(payload)
        assert record is not None
        app = SimpleNamespace(
            _strategy_name_to_id={"EMA 动态委托做空": STRATEGY_DYNAMIC_SHORT_ID},
            _credential_profiles={"local": {"api_key": "k", "secret_key": "s", "passphrase": "p", "environment": "demo"}},
            _current_credential_profile=lambda: "local",
            _apply_credentials_profile=MagicMock(),
            _on_strategy_selected=MagicMock(),
            _sync_dynamic_take_profit_controls=MagicMock(),
            _ensure_importable_strategy_symbols=MagicMock(),
            strategy_name=_Var(),
            symbol=_Var(),
            trade_symbol=_Var(),
            local_tp_sl_symbol=_Var(),
            bar=_Var(),
            ema_period=_Var(),
            trend_ema_period=_Var(),
            big_ema_period=_Var(),
            entry_reference_ema_period=_Var(),
            atr_period=_Var(),
            stop_atr=_Var(),
            take_atr=_Var(),
            risk_amount=_Var(),
            order_size=_Var(),
            poll_seconds=_Var(),
            signal_mode_label=_Var(),
            take_profit_mode_label=_Var(),
            max_entries_per_trend=_Var(),
            startup_chase_window_seconds=_Var(),
            dynamic_two_r_break_even=_Var(False),
            dynamic_fee_offset_enabled=_Var(False),
            time_stop_break_even_enabled=_Var(False),
            time_stop_break_even_bars=_Var(),
            run_mode_label=_Var(),
            trade_mode_label=_Var(),
            position_mode_label=_Var(),
            trigger_type_label=_Var(),
            tp_sl_mode_label=_Var(),
            entry_side_mode_label=_Var(),
            environment_label=_Var("实盘 live"),
        )
        app._resolve_strategy_template_definition = lambda item: QuantApp._resolve_strategy_template_definition(app, item)

        definition, resolved_api_name, api_note = QuantApp._apply_strategy_template_record(app, record)

        self.assertEqual(definition.name, "EMA 动态委托做空")
        self.assertEqual(resolved_api_name, "local")
        self.assertIn("保留当前 API：local", api_note)
        self.assertEqual(app.strategy_name.get(), "EMA 动态委托做空")
        self.assertEqual(app.symbol.get(), "SOL-USDT-SWAP")
        self.assertEqual(app.risk_amount.get(), "10")
        self.assertEqual(app.tp_sl_mode_label.get(), "OKX 托管（仅同标的永续）")
        self.assertEqual(app.environment_label.get(), "模拟盘 demo")
        app._apply_credentials_profile.assert_not_called()
        app._ensure_importable_strategy_symbols.assert_called_once_with("SOL-USDT-SWAP", "")


class StrategyDuplicateLaunchGuardTest(TestCase):
    @staticmethod
    def _make_config(*, risk_amount: str = "10") -> StrategyConfig:
        return StrategyConfig(
            inst_id="ETH-USDT-SWAP",
            bar="1H",
            ema_period=21,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            trend_ema_period=55,
            big_ema_period=233,
            strategy_id="ema_dynamic_order_long",
            poll_seconds=10.0,
            risk_amount=Decimal(risk_amount),
            trade_inst_id="ETH-USDT-SWAP",
            tp_sl_mode="exchange",
            local_tp_sl_inst_id=None,
            entry_side_mode="follow_signal",
            run_mode="trade",
            take_profit_mode="dynamic",
            max_entries_per_trend=1,
            entry_reference_ema_period=55,
            dynamic_two_r_break_even=True,
            dynamic_fee_offset_enabled=True,
            startup_chase_window_seconds=0,
            time_stop_break_even_enabled=False,
            time_stop_break_even_bars=0,
        )

    def test_find_duplicate_strategy_session_blocks_same_api_same_config(self) -> None:
        config = self._make_config()
        active_session = SimpleNamespace(
            session_id="S01",
            api_name="moni",
            config=config,
            status="运行中",
            engine=SimpleNamespace(is_running=True),
        )
        app = SimpleNamespace(sessions={"S01": active_session})
        app._session_blocks_duplicate_launch = lambda session: QuantApp._session_blocks_duplicate_launch(session)

        duplicate = QuantApp._find_duplicate_strategy_session(app, api_name="moni", config=config)

        self.assertIs(duplicate, active_session)

    def test_find_duplicate_strategy_session_ignores_stopped_and_other_api(self) -> None:
        config = self._make_config()
        stopped_session = SimpleNamespace(
            session_id="S01",
            api_name="moni",
            config=config,
            status="已停止",
            engine=SimpleNamespace(is_running=False),
        )
        other_api_session = SimpleNamespace(
            session_id="S02",
            api_name="other",
            config=config,
            status="运行中",
            engine=SimpleNamespace(is_running=True),
        )
        app = SimpleNamespace(sessions={"S01": stopped_session, "S02": other_api_session})
        app._session_blocks_duplicate_launch = lambda session: QuantApp._session_blocks_duplicate_launch(session)

        duplicate = QuantApp._find_duplicate_strategy_session(app, api_name="moni", config=config)

        self.assertIsNone(duplicate)

    def test_find_duplicate_strategy_session_ignores_recoverable_session(self) -> None:
        config = self._make_config()
        recoverable_session = SimpleNamespace(
            session_id="S03",
            api_name="moni",
            config=config,
            status="待恢复",
            engine=SimpleNamespace(is_running=False),
        )
        app = SimpleNamespace(sessions={"S03": recoverable_session})
        app._session_blocks_duplicate_launch = lambda session: QuantApp._session_blocks_duplicate_launch(session)

        duplicate = QuantApp._find_duplicate_strategy_session(app, api_name="moni", config=config)

        self.assertIsNone(duplicate)

    def test_upsert_session_row_marks_duplicate_conflict_tag(self) -> None:
        config = self._make_config()
        session = SimpleNamespace(
            session_id="S01",
            api_name="moni",
            strategy_name="EMA 动态委托做多",
            symbol="ETH-USDT-SWAP",
            direction_label="只做多",
            run_mode_label="交易并下单",
            net_pnl_total=Decimal("0"),
            display_status="等待信号",
            started_at=datetime(2026, 4, 23, 22, 6, 41),
            status="运行中",
            engine=SimpleNamespace(is_running=True),
            config=config,
        )
        peer = SimpleNamespace(
            session_id="S02",
            api_name="moni",
            status="运行中",
            engine=SimpleNamespace(is_running=True),
            config=config,
        )
        app = SimpleNamespace(
            session_tree=_SessionTreeStub(),
            _session_live_pnl_snapshot=lambda _session: (None, None),
            sessions={"S01": session, "S02": peer},
        )

        QuantApp._upsert_session_row(app, session)

        self.assertEqual(app.session_tree.rows["S01"]["tags"], ("duplicate_conflict",))
        self.assertEqual(app.session_tree.rows["S01"]["values"][0], "S01")
        self.assertEqual(app.session_tree.rows["S01"]["values"][2], "普通量化")
        self.assertEqual(app.session_tree.rows["S01"]["values"][5], "1H")

    def test_render_strategy_history_view_includes_session_column(self) -> None:
        record = StrategyHistoryRecord(
            record_id="R01",
            session_id="S01",
            api_name="moni",
            strategy_id="ema_dynamic_short",
            strategy_name="EMA 动态委托做空",
            symbol="ETH-USDT-SWAP",
            direction_label="只做空",
            run_mode_label="交易并下单",
            status="已停止",
            started_at=datetime(2026, 4, 24, 13, 20, 0),
            stopped_at=datetime(2026, 4, 24, 13, 25, 0),
        )
        tree = _SessionTreeStub()
        app = SimpleNamespace(
            _strategy_history_tree=tree,
            _strategy_history_records=[record],
            _strategy_history_selected_record_id=None,
            _next_history_selection_after_mutation=lambda _selected_before, _remaining_ids: None,
            _refresh_selected_strategy_history_details=lambda: None,
        )

        QuantApp._render_strategy_history_view(app)

        self.assertEqual(tree.rows["R01"]["values"][0], "S01")
        self.assertEqual(tree.rows["R01"]["values"][2], "EMA 动态委托做空")

    def test_finish_strategy_template_import_warns_and_skips_start_when_duplicate_exists(self) -> None:
        duplicate_session = SimpleNamespace(
            session_id="S01",
            api_name="moni",
            display_status="等待信号",
            started_at=datetime(2026, 4, 23, 22, 4, 18),
            symbol="ETH-USDT-SWAP",
        )
        app = SimpleNamespace(
            _enqueue_log=MagicMock(),
            _find_duplicate_strategy_session=lambda *, api_name, config: duplicate_session,
            _focus_session_row=MagicMock(),
            _format_duplicate_launch_block_message=lambda session, imported: QuantApp._format_duplicate_launch_block_message(
                session, imported=imported
            ),
            start=MagicMock(),
        )

        with patch("okx_quant.ui.messagebox.showwarning") as showwarning, patch(
            "okx_quant.ui.messagebox.askyesno"
        ) as askyesno:
            QuantApp._finish_strategy_template_import(
                app,
                source=r"D:\qqokx\templates\eth.json",
                record=SimpleNamespace(config=self._make_config()),
                definition=SimpleNamespace(name="EMA 动态委托做多"),
                applied_api="moni",
                api_note="继续使用当前 API：moni",
            )

        showwarning.assert_called_once()
        self.assertIn("请先修改标的或切换 API 后再启动", showwarning.call_args.args[1])
        askyesno.assert_not_called()
        app.start.assert_not_called()
        app._focus_session_row.assert_called_once_with("S01")

    def test_finish_strategy_template_import_prompts_copy_guidance_before_start(self) -> None:
        app = SimpleNamespace(
            _enqueue_log=MagicMock(),
            _find_duplicate_strategy_session=lambda *, api_name, config: None,
            symbol=_Var("SOL-USDT-SWAP"),
            start=MagicMock(),
        )

        with patch("okx_quant.ui.messagebox.askyesno", return_value=False) as askyesno:
            QuantApp._finish_strategy_template_import(
                app,
                source=r"D:\qqokx\templates\sol.json",
                record=SimpleNamespace(config=self._make_config()),
                definition=SimpleNamespace(name="EMA 动态委托做多"),
                applied_api="moni",
                api_note="已自动切换到导出文件里的 API：moni",
            )

        askyesno.assert_called_once()
        self.assertIn("如需复制参数开新策略，请先改标的或切换 API，再启动。", askyesno.call_args.args[1])
        app.start.assert_not_called()


class StrategyTradeTrackingTest(TestCase):
    def _make_session(self) -> SimpleNamespace:
        return SimpleNamespace(
            session_id="S01",
            history_record_id="H01",
            api_name="moni",
            strategy_id="ema_dynamic_order_long",
            strategy_name="EMA 动态委托做多",
            symbol="ETH-USDT-SWAP",
            direction_label="只做多",
            run_mode_label="交易并下单",
            config=SimpleNamespace(
                trade_inst_id="ETH-USDT-SWAP",
                inst_id="ETH-USDT-SWAP",
                environment="demo",
            ),
            active_trade=None,
            trade_count=0,
            win_count=0,
            gross_pnl_total=Decimal("0"),
            fee_total=Decimal("0"),
            funding_total=Decimal("0"),
            net_pnl_total=Decimal("0"),
            last_close_reason="",
        )

    def _make_app_for_tracking(self) -> SimpleNamespace:
        app = SimpleNamespace()
        app._ensure_session_trade_runtime = lambda session, observed_at, signal_bar_at=None: QuantApp._ensure_session_trade_runtime(
            app,
            session,
            observed_at=observed_at,
            signal_bar_at=signal_bar_at,
        )
        app._start_session_trade_reconciliation = MagicMock()
        return app

    def test_track_session_trade_runtime_captures_entry_stop_and_close_trigger(self) -> None:
        session = self._make_session()
        app = self._make_app_for_tracking()

        QuantApp._track_session_trade_runtime(
            app,
            session,
            "2026-04-23 08:00:00 | 挂单已提交到 OKX | ordId=1001 | sCode=0 | sMsg=下单成功",
        )
        QuantApp._track_session_trade_runtime(
            app,
            session,
            "2026-04-23 08:00:00 | 委托追踪 | clOrdId=s01emaent042300000897343",
        )
        QuantApp._track_session_trade_runtime(
            app,
            session,
            "2026-04-23 08:00:00 | 挂单已成交 | ordId=1001 | 开仓价=2358.42 | 数量=0.1",
        )
        QuantApp._track_session_trade_runtime(
            app,
            session,
            "初始 OKX 止损已提交 | algoClOrdId=s01emaslg042300000897344 | 止损=2320.66 | 启动动态上移监控",
        )
        QuantApp._track_session_trade_runtime(
            app,
            session,
            "本轮持仓已结束，继续监控下一次信号。",
        )

        self.assertIsNotNone(session.active_trade)
        self.assertEqual(session.active_trade.entry_order_id, "1001")
        self.assertEqual(session.active_trade.entry_client_order_id, "s01emaent042300000897343")
        self.assertEqual(session.active_trade.entry_price, Decimal("2358.42"))
        self.assertEqual(session.active_trade.size, Decimal("0.1"))
        self.assertEqual(session.active_trade.protective_algo_cl_ord_id, "s01emaslg042300000897344")
        self.assertEqual(session.active_trade.current_stop_price, Decimal("2320.66"))
        app._start_session_trade_reconciliation.assert_called_once()

    def test_handle_stopped_watcher_pauses_trader_on_unexpected_stop(self) -> None:
        draft = TraderDraftRecord(
            trader_id="T001",
            template_payload={"strategy_id": "ema_dynamic_long"},
            total_quota=Decimal("1"),
            unit_quota=Decimal("0.1"),
            quota_steps=10,
        )
        run = TraderRunState(trader_id="T001", status="running")
        slot = TraderSlotRecord(
            slot_id="slot-1",
            trader_id="T001",
            session_id="S01",
            api_name="moni",
            strategy_name="EMA",
            symbol="BTC-USDT-SWAP",
            status="watching",
        )
        events: list[tuple[str, str, str]] = []
        app = SimpleNamespace(
            _trader_desk_slot_for_session=lambda session_id: slot,
            _trader_desk_run_by_id=lambda trader_id, create=False: run,
            _trader_desk_draft_by_id=lambda trader_id: draft,
            _session_stop_reason_text=lambda session: QuantApp._session_stop_reason_text(session),
            _expected_trader_stop_reason=lambda reason: QuantApp._expected_trader_stop_reason(reason),
            _trader_desk_add_event=lambda trader_id, message, level="info": events.append((trader_id, level, message)),
            _save_trader_desk_snapshot=MagicMock(),
            _ensure_trader_watcher=MagicMock(),
        )
        session = SimpleNamespace(
            session_id="S01",
            trader_id="T001",
            trader_slot_id="slot-1",
            stopped_at=datetime(2026, 4, 24, 10, 34, 21),
            ended_reason="",
            last_message="策略停止，原因：测试异常",
            history_record_id="H01",
        )

        QuantApp._trader_desk_handle_stopped_session(app, session)

        self.assertEqual(slot.status, "stopped")
        self.assertEqual(slot.close_reason, "测试异常")
        self.assertEqual(run.status, "stopped")
        self.assertEqual(run.paused_reason, "测试异常")
        self.assertEqual(draft.status, "paused")
        app._ensure_trader_watcher.assert_not_called()
        self.assertEqual(events[0][0], "T001")
        self.assertEqual(events[0][1], "error")
        self.assertIn("watcher 异常结束", events[0][2])

    def test_cleanup_stale_trader_watchers_marks_missing_session_as_stopped(self) -> None:
        run = TraderRunState(trader_id="T001", status="running", armed_session_id="S01")
        slot = TraderSlotRecord(
            slot_id="slot-1",
            trader_id="T001",
            session_id="S01",
            api_name="moni",
            strategy_name="EMA",
            symbol="BTC-USDT-SWAP",
            status="watching",
        )
        events: list[tuple[str, str, str]] = []
        app = SimpleNamespace(
            sessions={},
            _trader_desk_run_by_id=lambda trader_id, create=False: run,
            _trader_desk_slots_for_statuses=lambda trader_id, statuses: [slot] if "watching" in statuses else [],
            _trader_desk_add_event=lambda trader_id, message, level="info": events.append((trader_id, level, message)),
            _save_trader_desk_snapshot=MagicMock(),
        )

        QuantApp._cleanup_stale_trader_watchers(app, "T001")

        self.assertEqual(slot.status, "stopped")
        self.assertEqual(slot.close_reason, "watcher 会话不存在或已停止")
        self.assertEqual(run.armed_session_id, "")
        app._save_trader_desk_snapshot.assert_called_once()
        self.assertEqual(events[0][0], "T001")
        self.assertEqual(events[0][1], "warning")
        self.assertIn("检测到失效 watcher", events[0][2])

    def test_delete_trader_desk_draft_cleans_stale_watchers_before_delete(self) -> None:
        draft = TraderDraftRecord(
            trader_id="T001",
            template_payload={"strategy_id": "ema_dynamic_long"},
            total_quota=Decimal("1"),
            unit_quota=Decimal("0.1"),
            quota_steps=10,
        )
        slot = TraderSlotRecord(
            slot_id="slot-1",
            trader_id="T001",
            session_id="S01",
            api_name="moni",
            strategy_name="EMA",
            symbol="BTC-USDT-SWAP",
            status="watching",
        )

        def _cleanup(_trader_id: str) -> None:
            slot.status = "stopped"

        app = SimpleNamespace(
            _trader_desk_draft_by_id=lambda trader_id: draft,
            _cleanup_stale_trader_watchers=MagicMock(side_effect=_cleanup),
            sessions={},
            _trader_desk_slots_for_statuses=lambda trader_id, statuses: [slot] if slot.status in statuses else [],
            _trader_desk_drafts=[draft],
            _trader_desk_runs=[],
            _trader_desk_slots=[slot],
            _trader_desk_events=[],
            _save_trader_desk_snapshot=MagicMock(),
            _enqueue_log=MagicMock(),
        )

        QuantApp._delete_trader_desk_draft(app, "T001")

        app._cleanup_stale_trader_watchers.assert_called_once_with("T001")
        self.assertEqual(app._trader_desk_drafts, [])
        app._save_trader_desk_snapshot.assert_called_once()

    def test_build_strategy_trade_reconciliation_result_attributes_stop_loss_and_net_pnl(self) -> None:
        session = self._make_session()
        trade = StrategyTradeRuntimeState(
            round_id="round-1",
            signal_bar_at=datetime(2026, 4, 23, 8, 0, 0),
            opened_logged_at=datetime(2026, 4, 23, 8, 15, 13),
            entry_order_id="1001",
            entry_client_order_id="s01emaent042300000897343",
            entry_price=Decimal("2358.42"),
            size=Decimal("0.1"),
            protective_algo_cl_ord_id="s01emaslg042300000897344",
            current_stop_price=Decimal("2320.66"),
            reconciliation_started=True,
        )
        prefix = _session_order_prefixes(session)[0]
        open_ms = int(datetime(2026, 4, 23, 8, 15, 13).timestamp() * 1000)
        close_ms = int(datetime(2026, 4, 23, 17, 9, 20).timestamp() * 1000)
        snapshot = StrategyTradeReconciliationSnapshot(
            effective_environment="demo",
            order_history=[
                SimpleNamespace(
                    client_order_id=f"{prefix}ent042300000897343",
                    algo_client_order_id="",
                    order_id="1001",
                    algo_id="",
                    inst_id="ETH-USDT-SWAP",
                    side="buy",
                    pos_side="long",
                    filled_size=Decimal("0.1"),
                    actual_size=Decimal("0.1"),
                    avg_price=Decimal("2358.42"),
                    actual_price=Decimal("2358.42"),
                    price=Decimal("2358.42"),
                    fee=Decimal("-0.04"),
                    pnl=None,
                    state="filled",
                    update_time=open_ms,
                    created_time=open_ms,
                ),
                SimpleNamespace(
                    client_order_id="",
                    algo_client_order_id=f"{prefix}slg042300000897344",
                    order_id="2001",
                    algo_id="3001",
                    inst_id="ETH-USDT-SWAP",
                    side="sell",
                    pos_side="long",
                    filled_size=Decimal("0.1"),
                    actual_size=Decimal("0.1"),
                    avg_price=Decimal("2320.66"),
                    actual_price=Decimal("2320.66"),
                    price=Decimal("2320.66"),
                    fee=Decimal("-0.04"),
                    pnl=Decimal("-3.78"),
                    state="filled",
                    update_time=close_ms,
                    created_time=close_ms,
                ),
            ],
            fills=[
                SimpleNamespace(
                    inst_id="ETH-USDT-SWAP",
                    order_id="1001",
                    fill_time=open_ms,
                    fill_price=Decimal("2358.42"),
                    fill_size=Decimal("0.1"),
                    fill_fee=Decimal("-0.04"),
                    pnl=None,
                ),
                SimpleNamespace(
                    inst_id="ETH-USDT-SWAP",
                    order_id="2001",
                    fill_time=close_ms,
                    fill_price=Decimal("2320.66"),
                    fill_size=Decimal("0.1"),
                    fill_fee=Decimal("-0.04"),
                    pnl=Decimal("-3.78"),
                ),
            ],
            position_history=[
                SimpleNamespace(
                    inst_id="ETH-USDT-SWAP",
                    update_time=close_ms,
                    close_avg_price=Decimal("2320.66"),
                    pnl=Decimal("-3.78"),
                    realized_pnl=Decimal("-3.87"),
                )
            ],
            account_bills=[
                SimpleNamespace(
                    inst_id="ETH-USDT-SWAP",
                    bill_time=close_ms,
                    bill_sub_type="173",
                    bill_type="8",
                    business_type="",
                    event_type="",
                    amount=Decimal("-0.01"),
                    pnl=Decimal("-0.01"),
                    balance_change=Decimal("-0.01"),
                )
            ],
        )
        app = SimpleNamespace(
            _next_strategy_trade_ledger_record_id=lambda session_, closed_at: "T01",
            _is_funding_fee_bill=QuantApp._is_funding_fee_bill,
        )

        result = QuantApp._build_strategy_trade_reconciliation_result(app, session, trade, snapshot)

        self.assertEqual(result.ledger_record.record_id, "T01")
        self.assertEqual(result.ledger_record.close_reason, "OKX止损触发")
        self.assertEqual(result.ledger_record.gross_pnl, Decimal("-3.78"))
        self.assertEqual(result.ledger_record.net_pnl, Decimal("-3.87"))
        self.assertIn("原因=OKX止损触发", result.attribution_summary)
        self.assertIn("累计净盈亏=-3.87", result.cumulative_summary)

    def test_apply_financial_totals_keeps_decimal_zero_when_trade_ledger_is_empty(self) -> None:
        target = SimpleNamespace(
            trade_count=99,
            win_count=88,
            gross_pnl_total=Decimal("1"),
            fee_total=Decimal("1"),
            funding_total=Decimal("1"),
            net_pnl_total=Decimal("1"),
            last_close_reason="old",
        )

        QuantApp._apply_financial_totals(SimpleNamespace(), target, [])

        self.assertEqual(target.trade_count, 0)
        self.assertEqual(target.win_count, 0)
        self.assertEqual(target.gross_pnl_total, Decimal("0"))
        self.assertEqual(target.fee_total, Decimal("0"))
        self.assertEqual(target.funding_total, Decimal("0"))
        self.assertEqual(target.net_pnl_total, Decimal("0"))
        self.assertEqual(target.last_close_reason, "")


class _FastEvent:
    def wait(self, timeout: float | None = None) -> bool:
        return False


class _Var:
    def __init__(self, value: str = "") -> None:
        self._value = value

    def get(self) -> str:
        return self._value

    def set(self, value: str) -> None:
        self._value = value


class _SessionTreeStub:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, object]] = {}
        self._selection: list[str] = []
        self.focused: str | None = None
        self.seen: str | None = None

    @staticmethod
    def winfo_exists() -> bool:
        return True

    def exists(self, iid: str) -> bool:
        return iid in self.rows

    def item(self, iid: str, **kwargs: object) -> None:
        self.rows.setdefault(iid, {}).update(kwargs)

    def insert(self, _parent: str, _index: object, *, iid: str, values: tuple[object, ...], tags: tuple[str, ...] = ()) -> None:
        self.rows[iid] = {"values": values, "tags": tags}

    def delete(self, iid: str) -> None:
        self.rows.pop(iid, None)
        self._selection = [item for item in self._selection if item != iid]
        if self.focused == iid:
            self.focused = None
        if self.seen == iid:
            self.seen = None

    def get_children(self) -> tuple[str, ...]:
        return tuple(self.rows.keys())

    def selection(self) -> tuple[str, ...]:
        return tuple(self._selection)

    def selection_set(self, iid: str) -> None:
        self._selection = [iid]

    def focus(self, iid: str) -> None:
        self.focused = iid

    def see(self, iid: str) -> None:
        self.seen = iid


class SessionLivePnlSummaryTest(TestCase):
    def test_refresh_session_live_pnl_cache_allocates_same_position_by_trade_size(self) -> None:
        refreshed_at = datetime(2026, 4, 23, 19, 5, 0)
        snapshot = ProfilePositionSnapshot(
            api_name="moni",
            effective_environment="demo",
            positions=[
                SimpleNamespace(
                    inst_id="ETH-USDT-SWAP",
                    pos_side="long",
                    position=Decimal("3"),
                    unrealized_pnl=Decimal("30"),
                    margin_ccy="USDT",
                )
            ],
            upl_usdt_prices={"USDT": Decimal("1")},
            refreshed_at=refreshed_at,
        )
        session_one = SimpleNamespace(
            session_id="S01",
            api_name="moni",
            config=SimpleNamespace(trade_inst_id="ETH-USDT-SWAP", inst_id="ETH-USDT-SWAP", environment="demo", signal_mode="long_only"),
            active_trade=SimpleNamespace(size=Decimal("1")),
        )
        session_two = SimpleNamespace(
            session_id="S02",
            api_name="moni",
            config=SimpleNamespace(trade_inst_id="ETH-USDT-SWAP", inst_id="ETH-USDT-SWAP", environment="demo", signal_mode="long_only"),
            active_trade=SimpleNamespace(size=Decimal("2")),
        )
        app = SimpleNamespace(
            sessions={"S01": session_one, "S02": session_two},
            _positions_snapshot_by_profile={"moni": snapshot},
            _session_live_pnl_cache={},
        )
        app._positions_snapshot_for_session = lambda session: QuantApp._positions_snapshot_for_session(app, session)

        QuantApp._refresh_session_live_pnl_cache(app)

        self.assertEqual(app._session_live_pnl_cache["S01"], (Decimal("10"), refreshed_at))
        self.assertEqual(app._session_live_pnl_cache["S02"], (Decimal("20"), refreshed_at))

    def test_refresh_running_session_summary_includes_live_and_net_totals(self) -> None:
        refreshed_at = datetime(2026, 4, 23, 19, 6, 0)
        active_session = SimpleNamespace(
            session_id="S01",
            engine=SimpleNamespace(is_running=True),
            stop_cleanup_in_progress=False,
            status="运行中",
            net_pnl_total=Decimal("5.5"),
        )
        waiting_session = SimpleNamespace(
            session_id="S02",
            engine=SimpleNamespace(is_running=True),
            stop_cleanup_in_progress=False,
            status="运行中",
            net_pnl_total=Decimal("-1.25"),
        )
        app = SimpleNamespace(
            sessions={"S01": active_session, "S02": waiting_session},
            _session_live_pnl_cache={
                "S01": (Decimal("3.25"), refreshed_at),
                "S02": (None, None),
            },
            session_summary_text=_Var(),
            _refresh_session_live_pnl_cache=lambda: None,
            _session_live_pnl_snapshot=lambda session: app._session_live_pnl_cache.get(session.session_id, (None, None)),
            _session_counts_toward_running_summary=lambda session: QuantApp._session_counts_toward_running_summary(session),
        )

        QuantApp._refresh_running_session_summary(app)

        text = app.session_summary_text.get()
        self.assertIn("多策略合计：2 个策略", text)
        self.assertIn("实时浮盈亏=+3.25", text)
        self.assertIn("净盈亏=+4.25", text)
        self.assertIn("浮盈覆盖 1/2", text)
        self.assertIn("参考持仓 19:06:00", text)
 
    def test_refresh_running_session_summary_appends_visible_count_when_filtered(self) -> None:
        trader_session = SimpleNamespace(
            session_id="S01",
            engine=SimpleNamespace(is_running=True),
            stop_cleanup_in_progress=False,
            status="运行中",
            net_pnl_total=Decimal("2"),
            trader_id="TR001",
            config=SimpleNamespace(run_mode="trade"),
        )
        signal_session = SimpleNamespace(
            session_id="S02",
            engine=SimpleNamespace(is_running=True),
            stop_cleanup_in_progress=False,
            status="运行中",
            net_pnl_total=Decimal("0"),
            trader_id="",
            config=SimpleNamespace(run_mode="signal_only"),
        )
        app = SimpleNamespace(
            sessions={"S01": trader_session, "S02": signal_session},
            _session_live_pnl_cache={"S01": (None, None), "S02": (None, None)},
            session_summary_text=_Var(),
            running_session_filter=_Var("交易员策略"),
            _refresh_session_live_pnl_cache=lambda: None,
            _session_live_pnl_snapshot=lambda session: app._session_live_pnl_cache.get(session.session_id, (None, None)),
            _session_counts_toward_running_summary=lambda session: QuantApp._session_counts_toward_running_summary(session),
        )

        QuantApp._refresh_running_session_summary(app)

        self.assertIn("当前筛选 交易员策略 1条", app.session_summary_text.get())


class RunningSessionFilterTest(TestCase):
    def test_session_category_label_distinguishes_regular_trader_and_signal_watch(self) -> None:
        regular_session = SimpleNamespace(trader_id="", config=SimpleNamespace(run_mode="trade"))
        trader_session = SimpleNamespace(trader_id="TR001", config=SimpleNamespace(run_mode="trade"))
        signal_session = SimpleNamespace(trader_id="", config=SimpleNamespace(run_mode="signal_only"))

        self.assertEqual(QuantApp._session_category_label(regular_session), "普通量化")
        self.assertEqual(QuantApp._session_category_label(trader_session), "交易员策略")
        self.assertEqual(QuantApp._session_category_label(signal_session), "信号观察台")

    def test_upsert_session_row_hides_row_when_session_does_not_match_filter(self) -> None:
        session = SimpleNamespace(
            session_id="S01",
            api_name="moni",
            strategy_name="EMA 动态委托做多",
            symbol="ETH-USDT-SWAP",
            direction_label="只做多",
            run_mode_label="交易并下单",
            net_pnl_total=Decimal("0"),
            display_status="等待信号",
            started_at=datetime(2026, 4, 24, 11, 45, 26),
            status="运行中",
            engine=SimpleNamespace(is_running=True),
            config=SimpleNamespace(run_mode="trade"),
            trader_id="",
        )
        tree = _SessionTreeStub()
        tree.insert("", "end", iid="S01", values=("old",), tags=())
        app = SimpleNamespace(
            session_tree=tree,
            running_session_filter=_Var("交易员策略"),
            _session_live_pnl_snapshot=lambda _session: (None, None),
            sessions={"S01": session},
        )

        QuantApp._upsert_session_row(app, session)

        self.assertFalse(app.session_tree.exists("S01"))


class _AfterRoot:
    def __init__(self) -> None:
        self.calls: list[tuple[int, object]] = []
        self.canceled: list[object] = []

    def after(self, delay: int, callback: object) -> str:
        self.calls.append((delay, callback))
        return f"job-{len(self.calls)}"

    def after_cancel(self, job: object) -> None:
        self.canceled.append(job)


class CredentialProfileEnvironmentTest(TestCase):
    def test_startup_credential_profile_name_prefers_moni_and_otherwise_falls_back(self) -> None:
        app = SimpleNamespace(_credential_profiles={"real": {}, "moni": {}})
        app._credential_profile_names = lambda: QuantApp._credential_profile_names(app)

        self.assertEqual(QuantApp._startup_credential_profile_name(app, "real"), "moni")

        fallback_app = SimpleNamespace(_credential_profiles={"real": {}, "trade": {}})
        fallback_app._credential_profile_names = lambda: QuantApp._credential_profile_names(fallback_app)

        self.assertEqual(QuantApp._startup_credential_profile_name(fallback_app, "real"), "real")
        self.assertEqual(QuantApp._startup_credential_profile_name(fallback_app, "missing"), "real")

    def test_apply_credentials_profile_restores_environment_and_clears_effective_cache(self) -> None:
        app = SimpleNamespace(
            _credential_profiles={
                "moni": {
                    "api_key": "demo-key",
                    "secret_key": "demo-secret",
                    "passphrase": "demo-pass",
                    "environment": "demo",
                }
            },
            _loaded_credential_profile_name="real",
            api_profile_name=_Var("real"),
            api_key=_Var(),
            secret_key=_Var(),
            passphrase=_Var(),
            environment_label=_Var("\u5b9e\u76d8 live"),
            _default_environment_label="\u5b9e\u76d8 live",
            _positions_effective_environment="live",
            _sync_credential_profile_combo=MagicMock(),
            _update_settings_summary=MagicMock(),
            _enqueue_log=MagicMock(),
        )

        def _set_credentials_fields(snapshot: dict[str, str]) -> None:
            app.api_key.set(snapshot["api_key"])
            app.secret_key.set(snapshot["secret_key"])
            app.passphrase.set(snapshot["passphrase"])

        app._set_credentials_fields = _set_credentials_fields
        app._normalized_environment_label = lambda label, fallback=None: QuantApp._normalized_environment_label(
            app, label, fallback=fallback
        )
        app._environment_label_for_profile = lambda profile_name: QuantApp._environment_label_for_profile(app, profile_name)
        app._apply_profile_environment = lambda profile_name: QuantApp._apply_profile_environment(app, profile_name)

        QuantApp._apply_credentials_profile(app, "moni", log_change=True)

        self.assertEqual(app.api_profile_name.get(), "moni")
        self.assertEqual(app.environment_label.get(), "\u6a21\u62df\u76d8 demo")
        self.assertIsNone(app._positions_effective_environment)
        self.assertEqual(
            app._last_saved_credentials,
            ("moni", "demo-key", "demo-secret", "demo-pass", "demo"),
        )
        app._enqueue_log.assert_called_once_with("\u5df2\u5207\u6362 API \u914d\u7f6e\uff1amoni")

    def test_save_credentials_now_persists_environment_with_profile(self) -> None:
        app = SimpleNamespace(
            _credential_save_job=None,
            _current_credentials_state=lambda: ("real", "live-key", "live-secret", "live-pass", "live"),
            _last_saved_credentials=None,
            _credential_profiles={},
            _auto_save_notice_shown=False,
            _sync_credential_profile_combo=MagicMock(),
            _update_settings_summary=MagicMock(),
            _enqueue_log=MagicMock(),
        )

        with patch("okx_quant.ui.save_credentials_profiles_snapshot") as save_snapshot:
            QuantApp._save_credentials_now(app, silent=True)

        self.assertEqual(
            app._credential_profiles["real"],
            {
                "api_key": "live-key",
                "secret_key": "live-secret",
                "passphrase": "live-pass",
                "environment": "live",
            },
        )
        save_snapshot.assert_called_once()

    def test_on_environment_label_changed_schedules_profile_and_settings_save(self) -> None:
        root = _AfterRoot()
        save_credentials = MagicMock()
        save_settings = MagicMock()
        app = SimpleNamespace(
            environment_label=_Var("\u5b9e\u76d8 live"),
            _default_environment_label="\u6a21\u62df\u76d8 demo",
            _positions_effective_environment="demo",
            _update_settings_summary=MagicMock(),
            _credential_watch_enabled=True,
            _settings_watch_enabled=True,
            _credential_save_job="cred-old",
            _settings_save_job="settings-old",
            root=root,
            _save_credentials_now=save_credentials,
            _save_notification_settings_now=save_settings,
        )
        app._normalized_environment_label = lambda label, fallback=None: QuantApp._normalized_environment_label(
            app, label, fallback=fallback
        )

        QuantApp._on_environment_label_changed(app)

        self.assertEqual(app._default_environment_label, "\u5b9e\u76d8 live")
        self.assertIsNone(app._positions_effective_environment)
        self.assertEqual(root.canceled, ["cred-old", "settings-old"])
        self.assertEqual(root.calls, [(600, save_credentials), (600, save_settings)])
        self.assertEqual(app._credential_save_job, "job-1")
        self.assertEqual(app._settings_save_job, "job-2")


class StrategyStopCleanupTest(TestCase):
    def _make_session(self) -> SimpleNamespace:
        return SimpleNamespace(
            session_id="S01",
            strategy_id="ema_dynamic_order",
            strategy_name="EMA dynamic order long",
            symbol="ETH-USDT-SWAP",
            api_name="moni",
            status="\u505c\u6b62\u4e2d",
            stop_cleanup_in_progress=True,
            stopped_at=None,
            ended_reason="",
            engine=SimpleNamespace(is_running=False),
            config=SimpleNamespace(
                environment="demo",
                trade_inst_id="ETH-USDT-SWAP",
                inst_id="ETH-USDT-SWAP",
            ),
        )

    def _make_order(
        self,
        session: SimpleNamespace,
        *,
        role: str,
        source_kind: str = "ord",
        state: str = "live",
        filled_size: str = "0",
    ) -> SimpleNamespace:
        prefix = _session_order_prefixes(session)[0]
        client_order_id = f"{prefix}{role}041816084800001" if source_kind != "algo" else ""
        algo_client_order_id = f"{prefix}{role}041816084800001" if source_kind == "algo" else ""
        return SimpleNamespace(
            source_kind=source_kind,
            source_label="algo" if source_kind == "algo" else "\u59d4\u6258",
            inst_id="ETH-USDT-SWAP",
            side="buy",
            pos_side="long",
            client_order_id=client_order_id,
            algo_client_order_id=algo_client_order_id,
            order_id="7001",
            algo_id="9001" if source_kind == "algo" else "",
            filled_size=Decimal(filled_size),
            state=state,
            avg_price=Decimal("2500"),
            price=Decimal("2500"),
            inst_type="SWAP",
        )

    def _make_position(self, *, size: str = "1") -> SimpleNamespace:
        return SimpleNamespace(
            inst_id="ETH-USDT-SWAP",
            pos_side="long",
            position=Decimal(size),
            avg_price=Decimal("2495"),
        )

    def _make_snapshot(
        self,
        *,
        environment: str = "demo",
        pending_orders: list[SimpleNamespace] | None = None,
        order_history: list[SimpleNamespace] | None = None,
        positions: list[SimpleNamespace] | None = None,
        environment_note: str | None = None,
    ) -> SimpleNamespace:
        return SimpleNamespace(
            effective_environment=environment,
            pending_orders=list(pending_orders or []),
            order_history=list(order_history or []),
            positions=list(positions or []),
            environment_note=environment_note,
        )

    def test_perform_stop_session_cleanup_collects_successful_cancel(self) -> None:
        session = self._make_session()
        entry_order = self._make_order(session, role="ent")
        initial_snapshot = self._make_snapshot(pending_orders=[entry_order])
        final_snapshot = self._make_snapshot()
        cancel_requests: list[tuple[str, SimpleNamespace]] = []

        def cancel_pending_order_request(credentials, *, environment: str, item: SimpleNamespace):
            cancel_requests.append((environment, item))
            return SimpleNamespace(s_code="0", s_msg="")

        app = SimpleNamespace(
            _load_strategy_stop_cleanup_snapshot_with_fallback=lambda session_, credentials_: initial_snapshot,
            _load_strategy_stop_cleanup_snapshot=lambda session_, credentials_, environment: final_snapshot,
            _cancel_pending_order_request=cancel_pending_order_request,
        )

        result = QuantApp._perform_stop_session_cleanup(app, session, SimpleNamespace())

        self.assertEqual(cancel_requests, [("demo", entry_order)])
        self.assertEqual(len(result.cancel_requested_summaries), 1)
        self.assertFalse(result.cancel_failed_summaries)
        self.assertFalse(result.remaining_pending_summaries)
        self.assertFalse(result.needs_manual_review)
        self.assertEqual(result.final_reason, "\u7528\u6237\u624b\u52a8\u505c\u6b62")

    def test_perform_stop_session_cleanup_flags_failed_cancel_for_manual_review(self) -> None:
        session = self._make_session()
        entry_order = self._make_order(session, role="ent")
        initial_snapshot = self._make_snapshot(pending_orders=[entry_order])
        final_snapshot = self._make_snapshot(pending_orders=[entry_order])

        def cancel_pending_order_request(credentials, *, environment: str, item: SimpleNamespace):
            raise RuntimeError("The read operation timed out")

        app = SimpleNamespace(
            _load_strategy_stop_cleanup_snapshot_with_fallback=lambda session_, credentials_: initial_snapshot,
            _load_strategy_stop_cleanup_snapshot=lambda session_, credentials_, environment: final_snapshot,
            _cancel_pending_order_request=cancel_pending_order_request,
        )

        with patch("okx_quant.ui.threading.Event", return_value=_FastEvent()):
            result = QuantApp._perform_stop_session_cleanup(app, session, SimpleNamespace())

        self.assertFalse(result.cancel_requested_summaries)
        self.assertEqual(len(result.cancel_failed_summaries), 1)
        self.assertEqual(len(result.remaining_pending_summaries), 1)
        self.assertTrue(result.needs_manual_review)
        self.assertIn("\u64a4\u5355\u672a\u5b8c\u5168\u786e\u8ba4", result.final_reason)

    def test_perform_stop_session_cleanup_flags_filled_order_with_open_position(self) -> None:
        session = self._make_session()
        filled_order = self._make_order(session, role="ent", state="filled", filled_size="1")
        open_position = self._make_position()
        initial_snapshot = self._make_snapshot()
        final_snapshot = self._make_snapshot(order_history=[filled_order], positions=[open_position])

        app = SimpleNamespace(
            _load_strategy_stop_cleanup_snapshot_with_fallback=lambda session_, credentials_: initial_snapshot,
            _load_strategy_stop_cleanup_snapshot=lambda session_, credentials_, environment: final_snapshot,
            _cancel_pending_order_request=lambda credentials, *, environment, item: None,
        )

        result = QuantApp._perform_stop_session_cleanup(app, session, SimpleNamespace())

        self.assertFalse(result.cancel_requested_summaries)
        self.assertEqual(len(result.filled_order_summaries), 1)
        self.assertEqual(len(result.open_position_summaries), 1)
        self.assertTrue(result.needs_manual_review)
        self.assertIn("\u68c0\u6d4b\u5230\u5df2\u6210\u4ea4\u4ed3\u4f4d", result.final_reason)

    def test_apply_stop_session_cleanup_result_warns_for_manual_review(self) -> None:
        session = self._make_session()
        log_messages: list[str] = []
        app = SimpleNamespace(
            sessions={session.session_id: session},
            _remove_recoverable_strategy_session=MagicMock(),
            _upsert_session_row=MagicMock(),
            _refresh_selected_session_details=MagicMock(),
            _sync_strategy_history_from_session=MagicMock(),
            _log_session_message=lambda session_, message: log_messages.append(message),
            _current_credential_profile=lambda: "moni",
            refresh_positions=MagicMock(),
            refresh_order_views=MagicMock(),
        )
        result = StrategyStopCleanupResult(
            session_id=session.session_id,
            effective_environment="demo",
            cancel_failed_summaries=("entry cancel timeout",),
            remaining_pending_summaries=("entry still pending",),
            filled_order_summaries=("filled entry",),
            open_position_summaries=("ETH-USDT-SWAP [long] | size=1",),
            needs_manual_review=True,
            final_reason="\u7528\u6237\u624b\u52a8\u505c\u6b62\uff08\u68c0\u6d4b\u5230\u5df2\u6210\u4ea4\u4ed3\u4f4d\uff0c\u9700\u4eba\u5de5\u5224\u65ad\uff09",
        )

        with patch("okx_quant.ui.messagebox.showwarning") as showwarning, patch(
            "okx_quant.ui.messagebox.showinfo"
        ) as showinfo:
            QuantApp._apply_stop_session_cleanup_result(app, result)

        self.assertEqual(session.status, "\u5df2\u505c\u6b62")
        self.assertFalse(session.stop_cleanup_in_progress)
        self.assertEqual(session.ended_reason, result.final_reason)
        self.assertTrue(any("\u68c0\u6d4b\u5230\u5df2\u6210\u4ea4\u59d4\u6258" in message for message in log_messages))
        self.assertTrue(any("\u68c0\u6d4b\u5230\u4ecd\u6709\u4ed3\u4f4d" in message for message in log_messages))
        showwarning.assert_called_once()
        showinfo.assert_not_called()
        app.refresh_positions.assert_called_once()
        app.refresh_order_views.assert_called_once()

    def test_apply_stop_session_cleanup_result_shows_info_when_clean(self) -> None:
        session = self._make_session()
        log_messages: list[str] = []
        app = SimpleNamespace(
            sessions={session.session_id: session},
            _remove_recoverable_strategy_session=MagicMock(),
            _upsert_session_row=MagicMock(),
            _refresh_selected_session_details=MagicMock(),
            _sync_strategy_history_from_session=MagicMock(),
            _log_session_message=lambda session_, message: log_messages.append(message),
            _current_credential_profile=lambda: "moni",
            refresh_positions=MagicMock(),
            refresh_order_views=MagicMock(),
        )
        result = StrategyStopCleanupResult(
            session_id=session.session_id,
            effective_environment="demo",
            cancel_requested_summaries=("entry cancel accepted",),
            needs_manual_review=False,
            final_reason="\u7528\u6237\u624b\u52a8\u505c\u6b62",
        )

        with patch("okx_quant.ui.messagebox.showwarning") as showwarning, patch(
            "okx_quant.ui.messagebox.showinfo"
        ) as showinfo:
            QuantApp._apply_stop_session_cleanup_result(app, result)

        self.assertEqual(session.status, "\u5df2\u505c\u6b62")
        self.assertFalse(session.stop_cleanup_in_progress)
        self.assertTrue(any("\u5df2\u63d0\u4ea4\u64a4\u5355 1 \u6761" in message for message in log_messages))
        showwarning.assert_not_called()
        showinfo.assert_called_once()
