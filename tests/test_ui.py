from __future__ import annotations

from decimal import Decimal
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from okx_quant.models import StrategyConfig
from okx_quant.okx_client import Instrument, OkxOrderResult, OkxOrderStatus, OkxPosition
from okx_quant.strategy_catalog import (
    STRATEGY_DYNAMIC_LONG_ID,
    STRATEGY_DYNAMIC_SHORT_ID,
    STRATEGY_EMA5_EMA8_ID,
    STRATEGY_EMA_BREAKOUT_LONG_ID,
)
from okx_quant.trader_desk import TraderDraftRecord, TraderRunState, TraderSlotRecord
from okx_quant.ui import (
    NormalStrategyBookFilters,
    NormalStrategyBookSummary,
    ProfilePositionSnapshot,
    QuantApp,
    RefreshHealthState,
    StrategyHistoryRecord,
    StrategyTradeLedgerRecord,
    StrategyStopCleanupResult,
    StrategyTradeReconciliationSnapshot,
    StrategyTradeRuntimeState,
    _build_normal_strategy_book_filter_options,
    _build_normal_strategy_book_group_rows,
    _build_normal_strategy_book_ledger_rows,
    _build_normal_strategy_book_summary,
    _build_current_position_note_record,
    _build_group_row_values,
    _build_history_position_note_record,
    _build_launch_parameter_hint_text,
    _build_minimum_order_risk_hint_text,
    _build_order_size_mode_hint_text,
    _build_dynamic_protection_hint_text,
    _build_strategy_start_confirmation_message,
    _build_trend_parameter_hint_text,
    _build_strategy_template_payload,
    _build_fixed_order_size_hint_text,
    _coerce_log_file_path,
    _filter_position_history_items,
    _filter_positions,
    _format_network_error_message,
    _format_okx_ms_timestamp,
    _format_position_note_summary,
    _history_display_amount,
    _infer_session_runtime_status,
    _inherit_position_history_notes,
    _mark_refresh_health_failure,
    _mark_refresh_health_success,
    _merge_history_cache_records,
    _order_item_from_cache,
    _position_history_item_from_cache,
    _format_trade_order_coin_size,
    _position_history_note_key,
    _position_note_current_key,
    _position_realized_pnl_usdt,
    _position_tree_row_id,
    _prune_closed_current_position_notes,
    _reconcile_current_position_note_records,
    _refresh_indicator_badge_text,
    _refresh_health_is_stale,
    _resolve_import_api_profile,
    _session_order_prefixes,
    _strategy_template_record_from_payload,
    _trade_order_belongs_to_session,
    _trade_order_session_role,
)


class UiHelpersTest(TestCase):
    def test_ui_split_keeps_parse_positive_int_bound_to_app(self) -> None:
        descriptor = QuantApp.__dict__["_parse_positive_int"]

        self.assertFalse(isinstance(descriptor, staticmethod))
        self.assertEqual(descriptor.__get__(SimpleNamespace(), QuantApp)("2", "field"), 2)

    def test_order_item_from_cache_returns_none_for_invalid_record(self) -> None:
        self.assertIsNone(_order_item_from_cache({"inst_id": "", "created_time": None, "update_time": None}))

    def test_position_history_item_from_cache_returns_none_for_invalid_record(self) -> None:
        self.assertIsNone(_position_history_item_from_cache({"inst_id": "BTC-USDT-SWAP", "update_time": ""}))

    def test_format_trade_order_coin_size_uses_coin_amount(self) -> None:
        order = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            size=Decimal("10"),
            price=Decimal("100000"),
            avg_price=None,
        )
        instruments = {
            "BTC-USDT-SWAP": Instrument(
                inst_id="BTC-USDT-SWAP",
                inst_type="SWAP",
                tick_size=Decimal("0.1"),
                lot_size=Decimal("0.01"),
                min_size=Decimal("0.01"),
                state="live",
                ct_val=Decimal("0.01"),
                ct_mult=Decimal("1"),
                ct_val_ccy="BTC",
            )
        }

        text = _format_trade_order_coin_size(order, instruments)  # type: ignore[arg-type]
        self.assertIn("0.1 BTC", text)

    def test_format_trade_order_coin_size_without_instrument_falls_back_to_contracts(self) -> None:
        order = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            size=Decimal("20"),
            price=Decimal("100000"),
            avg_price=None,
        )
        text = _format_trade_order_coin_size(order, {})  # type: ignore[arg-type]
        self.assertIn("20 张", text)

    def test_history_display_amount_futures_without_instrument_prefers_coin_amount(self) -> None:
        amount, currency = _history_display_amount(
            inst_id="BTC-USDC-241227",
            inst_type="FUTURES",
            size=Decimal("100"),
            reference_price=Decimal("25000"),
            instruments={},
        )
        self.assertEqual(currency, "BTC")
        self.assertEqual(amount, Decimal("0.004"))

    def test_merge_history_cache_records_prefers_remote_duplicates(self) -> None:
        local_records = [
            {"order_id": "1001", "inst_id": "BTC-USDT-SWAP", "state": "live"},
            {"order_id": "1002", "inst_id": "BTC-USDT-SWAP", "state": "filled"},
        ]
        remote_records = [
            {"order_id": "1001", "inst_id": "BTC-USDT-SWAP", "state": "canceled"},
            {"order_id": "1003", "inst_id": "ETH-USDT-SWAP", "state": "live"},
        ]
        merged = _merge_history_cache_records(local_records, remote_records, ("order_id", "inst_id"))
        merged_by_key = {(item["order_id"], item["inst_id"]): item for item in merged}
        self.assertEqual(len(merged), 3)
        self.assertEqual(merged_by_key[("1001", "BTC-USDT-SWAP")]["state"], "canceled")
        self.assertEqual(merged_by_key[("1002", "BTC-USDT-SWAP")]["state"], "filled")
        self.assertEqual(merged_by_key[("1003", "ETH-USDT-SWAP")]["state"], "live")

    def test_line_trading_desk_refresh_order_history_tab_updates_filtered_rows(self) -> None:
        calls: list[str] = []
        state = SimpleNamespace(
            window=object(),
            symbol_var=_Var("BTC-USDT-SWAP"),
            api_profile_var=_Var("moni"),
            status_text=_Var(""),
            latest_order_history=[],
        )
        app = SimpleNamespace(
            _line_trading_desk_window=state,
            client=SimpleNamespace(
                get_order_history=lambda credentials, *, environment, limit: [
                    SimpleNamespace(inst_id="BTC-USDT-SWAP"),
                    SimpleNamespace(inst_id="ETH-USDT-SWAP"),
                    SimpleNamespace(inst_id="BTC-USDT-SWAP"),
                ]
            ),
            root=SimpleNamespace(after=lambda delay, callback: callback()),
            _credentials_for_profile_or_none=lambda profile: object(),
            _environment_label_for_profile=lambda profile: "模拟盘 demo",
            _current_credential_profile=lambda: "moni",
            _normalized_environment_label=lambda label: QuantApp._normalized_environment_label(SimpleNamespace(), label),
            _line_trading_desk_refresh_order_history_tree=lambda st: calls.append("tree"),
            _line_trading_desk_log_prefix=lambda st: "[desk]",
            _line_trading_desk_dual_log=lambda st, msg: calls.append(msg),
            _enqueue_log=lambda message: calls.append(message),
        )
        app._line_trading_desk_apply_order_history_only = lambda desk_ref, history, err: (
            QuantApp._line_trading_desk_apply_order_history_only(app, desk_ref, history, err)
        )

        with patch("okx_quant.ui_shell._widget_exists", return_value=True):
            QuantApp._line_trading_desk_refresh_order_history_tab(app)

        self.assertEqual(len(state.latest_order_history), 2)
        self.assertEqual(state.status_text.get(), "已刷新历史委托 | BTC-USDT-SWAP | 2 条")
        self.assertIn("tree", calls)
        # `_line_trading_desk_dual_log` receives the detail line; prefix is added inside that method via `_enqueue_log`.
        self.assertIn("已刷新历史委托 | BTC-USDT-SWAP | 2 条", calls)

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

    def test_format_okx_ms_timestamp_supports_second_and_microsecond(self) -> None:
        self.assertEqual(
            _format_okx_ms_timestamp(1710976500),
            _format_okx_ms_timestamp(1710976500000),
        )
        self.assertEqual(
            _format_okx_ms_timestamp(1710976500000000),
            _format_okx_ms_timestamp(1710976500000),
        )

    def test_format_okx_ms_timestamp_filters_implausible_old_time(self) -> None:
        self.assertEqual(_format_okx_ms_timestamp(1080000000000), "-")

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

    def test_build_fixed_order_size_hint_text_for_swap_includes_contract_examples(self) -> None:
        instrument = Instrument(
            inst_id="ETH-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
            ct_val=Decimal("0.1"),
            ct_mult=Decimal("1"),
            ct_val_ccy="ETH",
        )

        hint = _build_fixed_order_size_hint_text("ETH-USDT-SWAP", instrument)

        self.assertIn("不是USDT", hint)
        self.assertIn("风险金", hint)
        self.assertIn("1=0.1 ETH", hint)
        self.assertIn("10=1 ETH", hint)
        self.assertIn("最小步长=0.01", hint)

    def test_build_fixed_order_size_hint_text_without_instrument_falls_back_to_symbol_only(self) -> None:
        hint = _build_fixed_order_size_hint_text("SOL-USDT-SWAP", None)

        self.assertIn("SOL-USDT-SWAP", hint)
        self.assertIn("不是USDT", hint)

    def test_build_minimum_order_risk_hint_text_for_swap_under_threshold(self) -> None:
        instrument = Instrument(
            inst_id="BNB-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.001"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            ct_val=Decimal("0.01"),
            ct_mult=Decimal("1"),
            ct_val_ccy="BNB",
        )

        hint = _build_minimum_order_risk_hint_text(
            inst_id="BNB-USDT-SWAP",
            instrument=instrument,
            risk_amount_raw="0.2",
            minimum_risk_amount=Decimal("0.5"),
            note="按当前止损距离估算。",
        )

        self.assertIn("最小下单量 1张（折合0.01 BNB）", hint)
        self.assertIn("至少需要风险金 0.5", hint)
        self.assertIn("当前填写 0.2，还不够下最小一笔", hint)
        self.assertIn("按当前止损距离估算。", hint)

    def test_build_minimum_order_risk_hint_text_without_instrument_shows_loading(self) -> None:
        hint = _build_minimum_order_risk_hint_text(
            inst_id="ETH-USDT-SWAP",
            instrument=None,
            risk_amount_raw="",
        )

        self.assertIn("正在读取 ETH-USDT-SWAP 的最小下单规格", hint)

    def test_build_order_size_mode_hint_text_prefers_risk_amount_when_present(self) -> None:
        self.assertEqual(
            _build_order_size_mode_hint_text("10", "1"),
            "当前模式：风险金优先，固定数量仅作备用。",
        )

    def test_build_order_size_mode_hint_text_switches_to_fixed_quantity_when_risk_blank(self) -> None:
        self.assertEqual(
            _build_order_size_mode_hint_text("", "1"),
            "当前模式：若风险金留空，将按固定数量下单。",
        )

    def test_find_instrument_for_fixed_order_size_hint_uses_local_cache(self) -> None:
        instrument = Instrument(
            inst_id="ETH-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
        )
        app = SimpleNamespace(
            _fixed_order_size_hint_instrument_cache={"ETH-USDT-SWAP": instrument},
            instruments=[],
            _position_instruments={},
            _ensure_fixed_order_size_hint_instrument_async=MagicMock(),
        )

        found = QuantApp._find_instrument_for_fixed_order_size_hint(app, "ETH-USDT-SWAP")

        self.assertIs(found, instrument)
        app._ensure_fixed_order_size_hint_instrument_async.assert_not_called()

    def test_find_instrument_for_fixed_order_size_hint_defers_missing_fetch_async(self) -> None:
        app = SimpleNamespace(
            _fixed_order_size_hint_instrument_cache={},
            instruments=[],
            _position_instruments={},
            _ensure_fixed_order_size_hint_instrument_async=MagicMock(),
        )

        found = QuantApp._find_instrument_for_fixed_order_size_hint(
            app,
            "ETH-USDT-SWAP",
            fetch_if_missing=True,
        )

        self.assertIsNone(found)
        app._ensure_fixed_order_size_hint_instrument_async.assert_called_once_with("ETH-USDT-SWAP")

    def test_build_strategy_start_confirmation_message_includes_parameter_explanations(self) -> None:
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="4H",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            risk_amount=Decimal("20"),
            tp_sl_mode="exchange",
            entry_side_mode="follow_signal",
            run_mode="trade",
            take_profit_mode="dynamic",
            max_entries_per_trend=1,
            entry_reference_ema_period=21,
            dynamic_two_r_break_even=True,
            dynamic_fee_offset_enabled=True,
            startup_chase_window_seconds=0,
            time_stop_break_even_enabled=False,
            time_stop_break_even_bars=0,
        )
        instrument = Instrument(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
            ct_val=Decimal("0.01"),
            ct_mult=Decimal("1"),
            ct_val_ccy="BTC",
        )

        message = _build_strategy_start_confirmation_message(
            strategy_name="EMA 动态委托做多",
            rule_description="当快线高于慢线时，等待回踩后挂单。",
            strategy_symbol="BTC-USDT-SWAP",
            config=config,
            run_mode_label="交易并下单",
            environment_label="模拟盘 demo",
            trade_mode_label="全仓 cross",
            position_mode_label="净持仓 net",
            signal_mode_label="只做多",
            entry_side_mode_label="跟随信号",
            tp_sl_mode_label="OKX 托管（仅同标的永续）",
            trigger_type_label="标记价格 mark",
            take_profit_mode_label="动态止盈",
            risk_value="20",
            fixed_size="1",
            custom_trigger_symbol="",
            instrument=instrument,
        )

        self.assertIn("基础信息：", message)
        self.assertIn("信号方向：只做多（只接收多头信号）", message)
        self.assertIn("下单方向模式：跟随信号（多头买入，空头卖出）", message)
        self.assertIn("止损 ATR 倍数：2（止损距离 = 2 × ATR）", message)
        self.assertIn("止盈 ATR 倍数：4（当前为动态止盈，初始不直接挂止盈）", message)
        self.assertIn("风险金：20（按止损距离反推仓位）", message)
        self.assertIn("固定数量：1（OKX 下单数量 sz；当前已填写风险金，仅作备用；BTC-USDT-SWAP 下 1=0.01 BTC）", message)
        self.assertIn("启动追单窗口：关闭（启动不追老信号，只等新波）", message)

    def test_build_launch_parameter_hint_text_for_dynamic_take_profit(self) -> None:
        hint = _build_launch_parameter_hint_text(
            stop_atr_raw="2",
            take_atr_raw="4",
            take_profit_mode_label="动态止盈",
            max_entries_raw="1",
            startup_chase_window_raw="0",
        )

        self.assertIn("止损ATR倍数：2=止损距离是 2×ATR。", hint)
        self.assertIn("动态止盈下不用于初始挂止盈", hint)
        self.assertIn("每波最多开仓次数：1=同一波最多开 1 次。", hint)
        self.assertIn("启动追单窗口：0=启动不追老信号", hint)

    def test_build_trend_parameter_hint_text_for_dynamic_strategy(self) -> None:
        hint = _build_trend_parameter_hint_text(
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            ema_period_raw="21",
            trend_ema_period_raw="55",
            big_ema_period_raw="233",
            entry_reference_ema_period_raw="0",
        )

        self.assertIn("EMA小周期：21=快线", hint)
        self.assertIn("EMA中周期：55=趋势过滤线", hint)
        self.assertIn("挂单参考EMA：0=跟随EMA小周期，当前按 EMA21 作为挂单价格锚点。", hint)

    def test_build_dynamic_protection_hint_text_for_enabled_dynamic_mode(self) -> None:
        hint = _build_dynamic_protection_hint_text(
            take_profit_mode_label="动态止盈",
            dynamic_two_r_break_even_enabled=True,
            dynamic_fee_offset_enabled=True,
            time_stop_break_even_enabled=False,
            time_stop_break_even_bars_raw="10",
        )

        self.assertIn("2R保本：开启", hint)
        self.assertIn("手续费偏移：开启", hint)
        self.assertIn("时间保本：关闭（当前设定 10 根", hint)

    def test_build_normal_strategy_book_summary_filters_out_trader_history(self) -> None:
        history_records = [
            StrategyHistoryRecord(
                record_id="R-ordinary",
                session_id="S01",
                api_name="moni",
                strategy_id="ema_dynamic_long",
                strategy_name="EMA 动态委托做多",
                symbol="ETH-USDT-SWAP",
                direction_label="只做多",
                run_mode_label="交易并下单",
                status="已停止",
                started_at=datetime(2026, 4, 26, 8, 0, 0),
                config_snapshot={"run_mode": "trade", "bar": "1m"},
            ),
            StrategyHistoryRecord(
                record_id="R-trader",
                session_id="S02",
                api_name="moni",
                strategy_id="ema_dynamic_short",
                strategy_name="EMA 动态委托做空",
                symbol="SOL-USDT-SWAP",
                direction_label="只做空",
                run_mode_label="交易并下单",
                status="已停止",
                started_at=datetime(2026, 4, 26, 8, 5, 0),
                config_snapshot={"run_mode": "trade", "bar": "1m", "trader_virtual_stop_loss": True},
            ),
        ]
        ledger_records = [
            StrategyTradeLedgerRecord(
                record_id="L-ordinary",
                history_record_id="R-ordinary",
                session_id="S01",
                api_name="moni",
                strategy_id="ema_dynamic_long",
                strategy_name="EMA 动态委托做多",
                symbol="ETH-USDT-SWAP",
                direction_label="只做多",
                run_mode_label="交易并下单",
                environment="demo",
                closed_at=datetime(2026, 4, 26, 9, 0, 0),
                net_pnl=Decimal("1.25"),
                gross_pnl=Decimal("1.50"),
                entry_fee=Decimal("0.10"),
                exit_fee=Decimal("0.10"),
            ),
            StrategyTradeLedgerRecord(
                record_id="L-trader",
                history_record_id="R-trader",
                session_id="S02",
                api_name="moni",
                strategy_id="ema_dynamic_short",
                strategy_name="EMA 动态委托做空",
                symbol="SOL-USDT-SWAP",
                direction_label="只做空",
                run_mode_label="交易并下单",
                environment="demo",
                closed_at=datetime(2026, 4, 26, 9, 5, 0),
                net_pnl=Decimal("-0.50"),
            ),
        ]

        summary = _build_normal_strategy_book_summary(ledger_records, history_records)

        self.assertIsInstance(summary, NormalStrategyBookSummary)
        self.assertEqual(summary.strategy_count, 1)
        self.assertEqual(summary.history_count, 1)
        self.assertEqual(summary.trade_count, 1)
        self.assertEqual(summary.win_count, 1)
        self.assertEqual(summary.loss_count, 0)
        self.assertEqual(summary.net_pnl_total, Decimal("1.25"))

    def test_build_normal_strategy_book_group_rows_aggregates_by_api_symbol_bar_and_direction(self) -> None:
        history_records = [
            StrategyHistoryRecord(
                record_id="R01",
                session_id="S01",
                api_name="apiA",
                strategy_id="ema_dynamic_long",
                strategy_name="EMA 动态委托做多",
                symbol="ETH-USDT-SWAP",
                direction_label="只做多",
                run_mode_label="交易并下单",
                status="已停止",
                started_at=datetime(2026, 4, 26, 8, 0, 0),
                config_snapshot={"run_mode": "trade", "bar": "15m"},
            )
        ]
        ledger_records = [
            StrategyTradeLedgerRecord(
                record_id="L01",
                history_record_id="R01",
                session_id="S01",
                api_name="apiA",
                strategy_id="ema_dynamic_long",
                strategy_name="EMA 动态委托做多",
                symbol="ETH-USDT-SWAP",
                direction_label="只做多",
                run_mode_label="交易并下单",
                environment="demo",
                closed_at=datetime(2026, 4, 26, 9, 0, 0),
                net_pnl=Decimal("1.00"),
                gross_pnl=Decimal("1.20"),
                entry_fee=Decimal("0.10"),
                exit_fee=Decimal("0.10"),
            ),
            StrategyTradeLedgerRecord(
                record_id="L02",
                history_record_id="R01",
                session_id="S01",
                api_name="apiA",
                strategy_id="ema_dynamic_long",
                strategy_name="EMA 动态委托做多",
                symbol="ETH-USDT-SWAP",
                direction_label="只做多",
                run_mode_label="交易并下单",
                environment="demo",
                closed_at=datetime(2026, 4, 26, 10, 0, 0),
                net_pnl=Decimal("-0.40"),
                gross_pnl=Decimal("-0.20"),
                entry_fee=Decimal("0.10"),
                exit_fee=Decimal("0.10"),
            ),
        ]

        rows = _build_normal_strategy_book_group_rows(ledger_records, history_records)

        self.assertEqual(len(rows), 1)
        row_id, values = rows[0]
        self.assertEqual(row_id, "apiA||-||EMA 动态委托做多||ETH-USDT-SWAP||15m||只做多")
        self.assertEqual(values[0], "apiA")
        self.assertEqual(values[1], "-")
        self.assertEqual(values[4], "15m")
        self.assertEqual(values[7], 2)
        self.assertEqual(values[8], 1)
        self.assertEqual(values[9], 1)
        self.assertEqual(values[10], "50%")
        self.assertEqual(values[14], "+0.60")

    def test_build_normal_strategy_book_ledger_rows_keep_close_order(self) -> None:
        history_records = [
            StrategyHistoryRecord(
                record_id="R01",
                session_id="S01",
                api_name="apiA",
                strategy_id="ema_dynamic_short",
                strategy_name="EMA 动态委托做空",
                symbol="BTC-USDT-SWAP",
                direction_label="只做空",
                run_mode_label="交易并下单",
                status="已停止",
                started_at=datetime(2026, 4, 26, 7, 0, 0),
                config_snapshot={"run_mode": "trade", "bar": "4H"},
            )
        ]
        ledger_records = [
            StrategyTradeLedgerRecord(
                record_id="L01",
                history_record_id="R01",
                session_id="S01",
                api_name="apiA",
                strategy_id="ema_dynamic_short",
                strategy_name="EMA 动态委托做空",
                symbol="BTC-USDT-SWAP",
                direction_label="只做空",
                run_mode_label="交易并下单",
                environment="demo",
                opened_at=datetime(2026, 4, 26, 7, 30, 0),
                closed_at=datetime(2026, 4, 26, 8, 30, 0),
                entry_price=Decimal("90000"),
                exit_price=Decimal("88000"),
                size=Decimal("0.01"),
                gross_pnl=Decimal("20"),
                entry_fee=Decimal("1"),
                exit_fee=Decimal("1"),
                funding_fee=Decimal("0.50"),
                net_pnl=Decimal("18.50"),
                close_reason="ATR 止盈",
            )
        ]

        rows = _build_normal_strategy_book_ledger_rows(ledger_records, history_records)

        self.assertEqual(len(rows), 1)
        row_id, values = rows[0]
        self.assertEqual(row_id, "L01")
        self.assertEqual(values[2], "-")
        self.assertEqual(values[3], "EMA 动态委托做空")
        self.assertEqual(values[5], "4H")
        self.assertEqual(values[6], "只做空")
        self.assertEqual(values[13], "+20.00")
        self.assertEqual(values[14], "+2.00")
        self.assertEqual(values[15], "+0.50")
        self.assertEqual(values[16], "+18.50")

    def test_build_normal_strategy_book_filter_options_include_trader_and_status(self) -> None:
        history_records = [
            StrategyHistoryRecord(
                record_id="R01",
                session_id="S01",
                api_name="apiA",
                strategy_id="ema_dynamic_long",
                strategy_name="EMA Long",
                symbol="ETH-USDT-SWAP",
                direction_label="只做多",
                run_mode_label="交易并下单",
                status="已停止",
                started_at=datetime(2026, 4, 26, 8, 0, 0),
                config_snapshot={"run_mode": "trade", "bar": "15m", "trader_id": "T008"},
            )
        ]
        ledger_records = [
            StrategyTradeLedgerRecord(
                record_id="L01",
                history_record_id="R01",
                session_id="S01",
                api_name="apiA",
                strategy_id="ema_dynamic_long",
                strategy_name="EMA Long",
                symbol="ETH-USDT-SWAP",
                direction_label="只做多",
                run_mode_label="交易并下单",
                environment="demo",
                closed_at=datetime(2026, 4, 26, 9, 0, 0),
                net_pnl=Decimal("1.00"),
            )
        ]

        options = _build_normal_strategy_book_filter_options(ledger_records, history_records)

        self.assertIn("全部交易员", options["trader_label"])
        self.assertIn("T008", options["trader_label"])
        self.assertIn("全部状态", options["status"])
        self.assertIn("已停止", options["status"])

    def test_build_normal_strategy_book_rows_respect_filters(self) -> None:
        history_records = [
            StrategyHistoryRecord(
                record_id="R01",
                session_id="S01",
                api_name="apiA",
                strategy_id="ema_dynamic_long",
                strategy_name="EMA Long",
                symbol="ETH-USDT-SWAP",
                direction_label="只做多",
                run_mode_label="交易并下单",
                status="已停止",
                started_at=datetime(2026, 4, 26, 8, 0, 0),
                config_snapshot={"run_mode": "trade", "bar": "15m", "trader_id": "T001"},
            ),
            StrategyHistoryRecord(
                record_id="R02",
                session_id="S02",
                api_name="apiB",
                strategy_id="ema_dynamic_short",
                strategy_name="EMA Short",
                symbol="BTC-USDT-SWAP",
                direction_label="只做空",
                run_mode_label="交易并下单",
                status="运行中",
                started_at=datetime(2026, 4, 26, 8, 10, 0),
                config_snapshot={"run_mode": "trade", "bar": "1H"},
            ),
        ]
        ledger_records = [
            StrategyTradeLedgerRecord(
                record_id="L01",
                history_record_id="R01",
                session_id="S01",
                api_name="apiA",
                strategy_id="ema_dynamic_long",
                strategy_name="EMA Long",
                symbol="ETH-USDT-SWAP",
                direction_label="只做多",
                run_mode_label="交易并下单",
                environment="demo",
                closed_at=datetime(2026, 4, 26, 9, 0, 0),
                net_pnl=Decimal("1.00"),
            ),
            StrategyTradeLedgerRecord(
                record_id="L02",
                history_record_id="R02",
                session_id="S02",
                api_name="apiB",
                strategy_id="ema_dynamic_short",
                strategy_name="EMA Short",
                symbol="BTC-USDT-SWAP",
                direction_label="只做空",
                run_mode_label="交易并下单",
                environment="demo",
                closed_at=datetime(2026, 4, 26, 9, 5, 0),
                net_pnl=Decimal("-0.50"),
            ),
        ]
        filters = NormalStrategyBookFilters(
            api_name="apiA",
            trader_label="T001",
            strategy_name="EMA Long",
            symbol="ETH-USDT-SWAP",
            bar="15m",
            direction_label="只做多",
            status="已停止",
        )

        summary = _build_normal_strategy_book_summary(ledger_records, history_records, filters=filters)
        group_rows = _build_normal_strategy_book_group_rows(ledger_records, history_records, filters=filters)
        ledger_rows = _build_normal_strategy_book_ledger_rows(ledger_records, history_records, filters=filters)

        self.assertEqual(summary.trade_count, 1)
        self.assertEqual(summary.net_pnl_total, Decimal("1.00"))
        self.assertEqual(len(group_rows), 1)
        self.assertEqual(group_rows[0][1][0], "apiA")
        self.assertEqual(group_rows[0][1][1], "T001")
        self.assertEqual(len(ledger_rows), 1)
        self.assertEqual(ledger_rows[0][1][1], "apiA")
        self.assertEqual(ledger_rows[0][1][2], "T001")

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

    def test_infer_session_runtime_status_maps_trader_virtual_stop_messages_to_position_monitoring(self) -> None:
        self.assertEqual(_infer_session_runtime_status("交易员虚拟止损监控启动 | 标的=ETH-USDT-SWAP"), "持仓监控中")
        self.assertEqual(_infer_session_runtime_status("交易员虚拟止损已触发（不平仓） | 当前价=2300"), "持仓监控中")
        self.assertEqual(_infer_session_runtime_status("交易员动态止盈保护价已上移 | 新保护价=2310"), "持仓监控中")

    def test_infer_session_runtime_status_preserves_existing_phase_during_okx_retry(self) -> None:
        self.assertEqual(
            _infer_session_runtime_status("OKX 读取异常，准备重试 | 操作=读取持仓 SWAP", "持仓监控中"),
            "持仓监控中",
        )

    def test_infer_session_runtime_status_maps_startup_gate_and_round_completion_to_waiting(self) -> None:
        self.assertEqual(_infer_session_runtime_status("启动默认不追老信号 | 方向=LONG"), "等待信号")
        self.assertEqual(_infer_session_runtime_status("启动追单窗口已过期，当前不追单 | 方向=LONG"), "等待信号")
        self.assertEqual(_infer_session_runtime_status("本轮持仓已结束，继续监控下一次信号。"), "等待信号")

    def test_create_session_engine_carries_email_runtime_context(self) -> None:
        app = SimpleNamespace(
            client=MagicMock(),
            _make_session_logger=lambda *args, **kwargs: (lambda message: None),
        )

        engine = QuantApp._create_session_engine(
            app,
            strategy_id="ema_dynamic_order",
            strategy_name="EMA 动态委托做多",
            session_id="S06",
            symbol="ETH-USDT-SWAP",
            api_name="moni",
            log_file_path=None,
            notifier=MagicMock(),
            direction_label="只做多",
            run_mode_label="交易并下单",
            trader_id="T001",
        )

        self.assertEqual(engine._session_id, "S06")
        self.assertEqual(engine._direction_label, "只做多")
        self.assertEqual(engine._run_mode_label, "交易并下单")
        self.assertEqual(engine._trader_id, "T001")

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

    def test_strategy_live_chart_pending_entry_prices_deduplicate_same_price_orders(self) -> None:
        app = QuantApp.__new__(QuantApp)
        session = SimpleNamespace(
            session_id="S01",
            strategy_id="ema_dynamic_order",
            strategy_name="EMA dynamic",
        )
        prefix = _session_order_prefixes(session)[0]
        app._latest_pending_orders = [
            SimpleNamespace(
                price=Decimal("101.2"),
                order_price=None,
                trigger_price=None,
                client_order_id=f"{prefix}ent001",
                algo_client_order_id="",
            ),
            SimpleNamespace(
                price=Decimal("101.2"),
                order_price=None,
                trigger_price=None,
                client_order_id=f"{prefix}ent002",
                algo_client_order_id="",
            ),
            SimpleNamespace(
                price=None,
                order_price=Decimal("102.5"),
                trigger_price=None,
                client_order_id=f"{prefix}ent003",
                algo_client_order_id="",
            ),
        ]

        self.assertEqual(
            QuantApp._strategy_live_chart_pending_entry_prices(app, session),
            (Decimal("101.2"), Decimal("102.5")),
        )

    def test_strategy_live_chart_position_avg_price_weights_matching_positions(self) -> None:
        app = QuantApp.__new__(QuantApp)
        refreshed_at = datetime(2026, 4, 26, 12, 0, 0)
        app._positions_snapshot_by_profile = {
            "moni": ProfilePositionSnapshot(
                api_name="moni",
                effective_environment="demo",
                positions=[
                    SimpleNamespace(inst_id="ETH-USDT-SWAP", pos_side="long", position=Decimal("1"), avg_price=Decimal("100")),
                    SimpleNamespace(inst_id="ETH-USDT-SWAP", pos_side="long", position=Decimal("2"), avg_price=Decimal("130")),
                ],
                upl_usdt_prices={},
                refreshed_at=refreshed_at,
            )
        }
        session = SimpleNamespace(
            api_name="moni",
            symbol="ETH-USDT-SWAP",
            config=StrategyConfig(
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
            ),
        )

        avg_price, snapshot_at = QuantApp._strategy_live_chart_position_avg_price(app, session)

        self.assertEqual(avg_price, Decimal("120"))
        self.assertEqual(snapshot_at, refreshed_at)

    def test_create_session_engine_seeds_api_name_for_notification_fallback(self) -> None:
        app = QuantApp.__new__(QuantApp)
        app.client = MagicMock()
        app._make_session_logger = lambda *args, **kwargs: (lambda message: None)

        engine = QuantApp._create_session_engine(
            app,
            strategy_id="ema_dynamic_order",
            strategy_name="EMA 动态委托",
            session_id="S35",
            symbol="DOGE-USDT-SWAP",
            api_name="QQzhangyong",
            log_file_path=None,
            notifier=None,
            direction_label="只做空",
            run_mode_label="交易并下单",
        )

        self.assertEqual(engine._api_name, "QQzhangyong")

    def test_strategy_live_chart_event_time_markers_include_close_add_and_reduce(self) -> None:
        app = QuantApp.__new__(QuantApp)
        app._strategy_trade_ledger_records = [
            StrategyTradeLedgerRecord(
                record_id="L01",
                history_record_id="H01",
                session_id="S01",
                api_name="moni",
                strategy_id="ema_dynamic_order",
                strategy_name="EMA dynamic",
                symbol="SOL-USDT-SWAP",
                direction_label="只做多",
                run_mode_label="交易并下单",
                environment="demo",
                closed_at=datetime(2026, 4, 28, 10, 15),
                opened_at=datetime(2026, 4, 28, 9, 0),
                entry_order_id="ent001",
                exit_order_id="exi003",
            )
        ]
        app._credentials_for_profile_or_none = lambda profile_name: SimpleNamespace(profile_name=profile_name)
        app.client = SimpleNamespace(
            get_fills_history=lambda credentials, **kwargs: [
                SimpleNamespace(
                    fill_time=int(datetime(2026, 4, 28, 9, 0).timestamp() * 1000),
                    inst_id="SOL-USDT-SWAP",
                    side="buy",
                    order_id="ent001",
                    trade_id="t1",
                ),
                SimpleNamespace(
                    fill_time=int(datetime(2026, 4, 28, 9, 30).timestamp() * 1000),
                    inst_id="SOL-USDT-SWAP",
                    side="buy",
                    order_id="ent002",
                    trade_id="t2",
                ),
                SimpleNamespace(
                    fill_time=int(datetime(2026, 4, 28, 9, 45).timestamp() * 1000),
                    inst_id="SOL-USDT-SWAP",
                    side="sell",
                    order_id="red001",
                    trade_id="t3",
                ),
                SimpleNamespace(
                    fill_time=int(datetime(2026, 4, 28, 10, 15).timestamp() * 1000),
                    inst_id="SOL-USDT-SWAP",
                    side="sell",
                    order_id="exi003",
                    trade_id="t4",
                ),
            ]
        )
        session = SimpleNamespace(
            session_id="S01",
            history_record_id="H01",
            api_name="moni",
            active_trade=SimpleNamespace(opened_logged_at=datetime(2026, 4, 28, 9, 0), entry_order_id="ent001"),
            config=SimpleNamespace(environment="demo"),
        )

        markers = QuantApp._strategy_live_chart_event_time_markers(app, session, "SOL-USDT-SWAP")

        self.assertEqual([marker.key for marker in markers], ["close:L01", "add:ent002", "reduce:red001"])

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
            trader_virtual_stop_loss=True,
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
        self.assertTrue(record.config.trader_virtual_stop_loss)

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
            trader_id="",
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
            email_notifications_enabled=True,
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
            notify_enabled=_Var(True),
        )
        app._trader_desk_draft_by_id = lambda _trader_id: None

        QuantApp._upsert_session_row(app, session)

        self.assertEqual(app.session_tree.rows["S01"]["tags"], ("duplicate_conflict",))
        self.assertEqual(app.session_tree.rows["S01"]["values"][0], "S01")
        self.assertEqual(app.session_tree.rows["S01"]["values"][1], "-")
        self.assertEqual(app.session_tree.rows["S01"]["values"][2], "开")
        self.assertEqual(app.session_tree.rows["S01"]["values"][4], "普通量化")
        self.assertEqual(app.session_tree.rows["S01"]["values"][7], "1H")
    def test_session_trader_label_prefers_trader_id_and_falls_back_to_dash(self) -> None:
        trader_session = SimpleNamespace(trader_id="T001")
        plain_session = SimpleNamespace(trader_id="")
        app = SimpleNamespace(
            _trader_desk_draft_by_id=lambda trader_id: SimpleNamespace(trader_id=trader_id) if trader_id == "T001" else None
        )

        self.assertEqual(QuantApp._session_trader_label(app, trader_session), "T001")
        self.assertEqual(QuantApp._session_trader_label(app, plain_session), "-")

    def test_session_email_status_label_reflects_global_and_session_switches(self) -> None:
        session = SimpleNamespace(email_notifications_enabled=True)
        app = SimpleNamespace(notify_enabled=_Var(True))

        self.assertEqual(QuantApp._session_email_status_label(app, session), "开")

        session.email_notifications_enabled = False
        self.assertEqual(QuantApp._session_email_status_label(app, session), "关")

        app.notify_enabled.set(False)
        self.assertEqual(QuantApp._session_email_status_label(app, session), "总关")

    def test_toggle_global_email_notifications_updates_runtime_state(self) -> None:
        logs: list[str] = []
        app = SimpleNamespace(
            notify_enabled=_Var(True),
            global_email_toggle_text=_Var("发邮件：开"),
            _refresh_global_email_toggle_text=lambda: QuantApp._refresh_global_email_toggle_text(app),
            _refresh_running_session_tree=MagicMock(),
            _refresh_selected_session_details=MagicMock(),
            _save_notification_settings_now=MagicMock(),
            _enqueue_log=logs.append,
        )

        QuantApp.toggle_global_email_notifications(app)

        self.assertFalse(app.notify_enabled.get())
        self.assertEqual(app.global_email_toggle_text.get(), "发邮件：关")
        app._refresh_running_session_tree.assert_called_once()
        app._refresh_selected_session_details.assert_called_once()
        app._save_notification_settings_now.assert_called_once_with(silent=True)
        self.assertEqual(logs[-1], "已关闭全局发邮件。")

    def test_set_selected_session_email_notifications_updates_row_and_detail(self) -> None:
        session = SimpleNamespace(session_id="S01", email_notifications_enabled=True)
        logs: list[str] = []
        app = SimpleNamespace(
            root=object(),
            _selected_session=lambda: session,
            _upsert_session_row=MagicMock(),
            _refresh_selected_session_details=MagicMock(),
            _enqueue_log=logs.append,
        )

        QuantApp._set_selected_session_email_notifications(app, False)

        self.assertFalse(session.email_notifications_enabled)
        app._upsert_session_row.assert_called_once_with(session)
        app._refresh_selected_session_details.assert_called_once()
        self.assertEqual(logs[-1], "已关闭会话 S01 发邮件。")

    def test_session_tree_double_click_toggles_email_only_on_email_column(self) -> None:
        session = SimpleNamespace(session_id="S01", email_notifications_enabled=True)
        tree = _SessionTreeStub()
        tree.rows["S01"] = {"values": (), "tags": (), "text": ""}
        app = SimpleNamespace(
            session_tree=tree,
            sessions={"S01": session},
        )
        toggles: list[bool] = []
        app._set_selected_session_email_notifications = lambda enabled: toggles.append(enabled)

        tree.identify_column = lambda _x: "#3"
        tree.identify_row = lambda _y: "S01"
        result = QuantApp._on_session_tree_double_click(app, SimpleNamespace(x=24, y=12))

        self.assertEqual(result, "break")
        self.assertEqual(toggles, [False])

        toggles.clear()
        tree.identify_column = lambda _x: "#2"
        result = QuantApp._on_session_tree_double_click(app, SimpleNamespace(x=12, y=12))

        self.assertIsNone(result)
        self.assertEqual(toggles, [])

    def test_session_tree_double_click_opens_live_chart_on_symbol_column(self) -> None:
        session = SimpleNamespace(session_id="S01", email_notifications_enabled=True)
        tree = _SessionTreeStub()
        tree.rows["S01"] = {"values": (), "tags": (), "text": ""}
        opened: list[str] = []
        app = SimpleNamespace(
            session_tree=tree,
            sessions={"S01": session},
            open_strategy_live_chart_window=lambda session_id: opened.append(session_id),
        )

        tree.identify_column = lambda _x: "#7"
        tree.identify_row = lambda _y: "S01"
        result = QuantApp._on_session_tree_double_click(app, SimpleNamespace(x=48, y=12))

        self.assertEqual(result, "break")
        self.assertEqual(opened, ["S01"])

    def test_session_tree_double_click_opens_session_log_on_session_column(self) -> None:
        session = SimpleNamespace(session_id="S01", email_notifications_enabled=True, trader_id="")
        tree = _SessionTreeStub()
        tree.rows["S01"] = {"values": (), "tags": (), "text": ""}
        opened: list[str] = []
        app = SimpleNamespace(
            session_tree=tree,
            sessions={"S01": session},
            open_strategy_session_log=lambda session_id: opened.append(session_id),
        )

        tree.identify_column = lambda _x: "#1"
        tree.identify_row = lambda _y: "S01"
        result = QuantApp._on_session_tree_double_click(app, SimpleNamespace(x=8, y=12))

        self.assertEqual(result, "break")
        self.assertEqual(opened, ["S01"])

    def test_session_tree_double_click_opens_trader_desk_on_trader_column(self) -> None:
        session = SimpleNamespace(session_id="S01", email_notifications_enabled=True, trader_id="T001")
        tree = _SessionTreeStub()
        tree.rows["S01"] = {"values": (), "tags": (), "text": ""}
        opened: list[bool] = []
        app = SimpleNamespace(
            session_tree=tree,
            sessions={"S01": session},
            open_trader_desk_window_for_trader=lambda trader_id: opened.append(trader_id == "T001"),
        )

        tree.identify_column = lambda _x: "#2"
        tree.identify_row = lambda _y: "S01"
        result = QuantApp._on_session_tree_double_click(app, SimpleNamespace(x=16, y=12))

        self.assertEqual(result, "break")
        self.assertEqual(opened, [True])

    def test_open_trader_desk_window_for_trader_focuses_target_row(self) -> None:
        window = SimpleNamespace(
            _refresh_views=MagicMock(),
            _focus_trader_row=MagicMock(),
        )
        app = SimpleNamespace(
            _trader_desk_window=window,
            open_trader_desk_window=MagicMock(),
        )

        QuantApp.open_trader_desk_window_for_trader(app, "T001")

        app.open_trader_desk_window.assert_called_once_with()
        window._refresh_views.assert_called_once_with(select_id="T001")
        window._focus_trader_row.assert_called_once_with("T001")

    def test_session_tree_double_click_hint_maps_supported_columns(self) -> None:
        self.assertEqual(QuantApp._session_tree_double_click_hint("#1"), "双击打开这条会话的独立日志")
        self.assertEqual(QuantApp._session_tree_double_click_hint("#2"), "双击打开并定位对应交易员")
        self.assertEqual(QuantApp._session_tree_double_click_hint("#3"), "双击切换当前会话发邮件开关")
        self.assertEqual(QuantApp._session_tree_double_click_hint("#7"), "双击打开这条策略的实时K线图")
        self.assertEqual(QuantApp._session_tree_double_click_hint("#4"), "")

    def test_strategy_history_tree_double_click_hint_maps_supported_columns(self) -> None:
        self.assertEqual(QuantApp._strategy_history_tree_double_click_hint("#1"), "双击打开这条历史策略的独立日志")
        self.assertEqual(QuantApp._strategy_history_tree_double_click_hint("#4"), "双击打开对应会话的实时K线图（若仍存在）")
        self.assertEqual(QuantApp._strategy_history_tree_double_click_hint("#2"), "")


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

    def test_strategy_history_tree_double_click_opens_log_on_session_column(self) -> None:
        record = SimpleNamespace(record_id="R01")
        tree = _SessionTreeStub()
        tree.rows["R01"] = {"values": (), "tags": (), "text": ""}
        opened: list[bool] = []
        app = SimpleNamespace(
            _strategy_history_tree=tree,
            _strategy_history_by_id={"R01": record},
            _strategy_history_selected_record_id=None,
            open_selected_strategy_history_log=lambda: opened.append(True),
        )

        tree.identify_column = lambda _x: "#1"
        tree.identify_row = lambda _y: "R01"
        result = QuantApp._on_strategy_history_tree_double_click(app, SimpleNamespace(x=8, y=12))

        self.assertEqual(result, "break")
        self.assertEqual(opened, [True])
        self.assertEqual(app._strategy_history_selected_record_id, "R01")

    def test_strategy_history_tree_double_click_opens_live_chart_on_symbol_column_when_session_exists(self) -> None:
        record = SimpleNamespace(record_id="R01")
        session = SimpleNamespace(session_id="S01")
        tree = _SessionTreeStub()
        tree.rows["R01"] = {"values": (), "tags": (), "text": ""}
        opened: list[str] = []
        app = SimpleNamespace(
            _strategy_history_tree=tree,
            _strategy_history_by_id={"R01": record},
            _strategy_history_selected_record_id=None,
            _session_by_history_record_id=lambda record_id: session if record_id == "R01" else None,
            open_strategy_live_chart_window=lambda session_id: opened.append(session_id),
        )

        tree.identify_column = lambda _x: "#4"
        tree.identify_row = lambda _y: "R01"
        result = QuantApp._on_strategy_history_tree_double_click(app, SimpleNamespace(x=24, y=12))

        self.assertEqual(result, "break")
        self.assertEqual(opened, ["S01"])

    def test_strategy_book_tree_double_click_hint_maps_supported_columns(self) -> None:
        self.assertEqual(QuantApp._strategy_book_tree_double_click_hint("#2"), "双击打开并定位对应交易员")
        self.assertEqual(QuantApp._strategy_book_tree_double_click_hint("#4"), "双击打开对应会话的实时K线图（若仍存在）")
        self.assertEqual(QuantApp._strategy_book_tree_double_click_hint("#9"), "双击打开对应会话的独立日志（若仍存在）")
        self.assertEqual(QuantApp._strategy_book_tree_double_click_hint("#3"), "")

    def test_strategy_book_group_tree_double_click_opens_trader_and_symbol_actions(self) -> None:
        tree = _SessionTreeStub()
        tree.rows["G01"] = {
            "values": ("apiA", "T001", "EMA Long", "ETH-USDT-SWAP", "15m", "只做多"),
            "tags": (),
            "text": "",
        }
        opened_trader: list[str] = []
        opened_chart: list[str] = []
        session = SimpleNamespace(
            session_id="S01",
            strategy_name="EMA Long",
            symbol="ETH-USDT-SWAP",
            direction_label="只做多",
            config=SimpleNamespace(bar="15m"),
        )
        app = SimpleNamespace(
            _strategy_book_group_tree=tree,
            sessions={"S01": session},
            open_trader_desk_window_for_trader=lambda trader_id: opened_trader.append(trader_id),
            open_strategy_live_chart_window=lambda session_id: opened_chart.append(session_id),
        )

        tree.identify_row = lambda _y: "G01"
        tree.identify_column = lambda _x: "#2"
        result = QuantApp._on_strategy_book_group_tree_double_click(app, SimpleNamespace(x=10, y=10))
        self.assertEqual(result, "break")
        self.assertEqual(opened_trader, ["T001"])

        tree.identify_column = lambda _x: "#4"
        result = QuantApp._on_strategy_book_group_tree_double_click(app, SimpleNamespace(x=30, y=10))
        self.assertEqual(result, "break")
        self.assertEqual(opened_chart, ["S01"])

    def test_strategy_book_ledger_tree_double_click_opens_trader_chart_and_log(self) -> None:
        tree = _SessionTreeStub()
        tree.rows["L01"] = {
            "values": (
                "04-28 09:00:00",
                "apiA",
                "T001",
                "EMA Long",
                "ETH-USDT-SWAP",
                "15m",
                "只做多",
                "已停止",
                "S01",
            ),
            "tags": ("S01",),
            "text": "",
        }
        opened_trader: list[str] = []
        opened_chart: list[str] = []
        opened_log: list[str] = []
        app = SimpleNamespace(
            _strategy_book_ledger_tree=tree,
            open_trader_desk_window_for_trader=lambda trader_id: opened_trader.append(trader_id),
            open_strategy_live_chart_window=lambda session_id: opened_chart.append(session_id),
            open_strategy_session_log=lambda session_id: opened_log.append(session_id),
        )

        tree.identify_row = lambda _y: "L01"
        tree.identify_column = lambda _x: "#3"
        result = QuantApp._on_strategy_book_ledger_tree_double_click(app, SimpleNamespace(x=10, y=10))
        self.assertEqual(result, "break")
        self.assertEqual(opened_trader, ["T001"])

        tree.identify_column = lambda _x: "#5"
        result = QuantApp._on_strategy_book_ledger_tree_double_click(app, SimpleNamespace(x=20, y=10))
        self.assertEqual(result, "break")
        self.assertEqual(opened_chart, ["S01"])

        tree.identify_column = lambda _x: "#9"
        result = QuantApp._on_strategy_book_ledger_tree_double_click(app, SimpleNamespace(x=40, y=10))
        self.assertEqual(result, "break")
        self.assertEqual(opened_log, ["S01"])

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
            _trader_desk_slot_for_session=lambda session_id, trader_slot_id="": slot,
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

    def test_apply_trader_desk_reconciliation_persists_zero_net_pnl_when_missing(self) -> None:
        draft = TraderDraftRecord(
            trader_id="T001",
            template_payload={"strategy_id": "ema_dynamic_long"},
            total_quota=Decimal("1"),
            unit_quota=Decimal("0.1"),
            quota_steps=10,
            pause_on_stop_loss=True,
        )
        run = TraderRunState(trader_id="T001", status="running", armed_session_id="S01")
        slot = TraderSlotRecord(
            slot_id="slot-1",
            trader_id="T001",
            session_id="S01",
            api_name="moni",
            strategy_name="EMA",
            symbol="ETH-USDT-SWAP",
            status="open",
            quota_occupied=True,
            opened_at=datetime(2026, 4, 24, 15, 0, 0),
            entry_price=Decimal("2312.59"),
            size=Decimal("0.1"),
        )
        events: list[tuple[str, str, str]] = []
        app = SimpleNamespace(
            _trader_desk_slot_for_session=lambda session_id, trader_slot_id="": slot,
            _trader_desk_draft_by_id=lambda trader_id: draft,
            _trader_desk_run_by_id=lambda trader_id, create=False: run,
            _trader_desk_add_event=lambda trader_id, message, level="info": events.append((trader_id, level, message)),
            _save_trader_desk_snapshot=MagicMock(),
            _ensure_trader_watcher=MagicMock(),
        )
        session = SimpleNamespace(
            session_id="S01",
            trader_id="T001",
            trader_slot_id="slot-1",
            ended_reason="OKX止损触发",
            history_record_id="H01",
        )
        ledger_record = SimpleNamespace(
            close_reason="OKX止损触发",
            net_pnl=None,
            opened_at=datetime(2026, 4, 24, 15, 0, 0),
            closed_at=datetime(2026, 4, 24, 15, 6, 0),
            entry_price=Decimal("2312.59"),
            exit_price=Decimal("2311.10"),
            size=Decimal("0.1"),
            history_record_id="H01",
        )

        QuantApp._apply_trader_desk_reconciliation(app, session, ledger_record)

        self.assertEqual(slot.status, "closed_loss")
        self.assertEqual(slot.net_pnl, Decimal("0"))
        self.assertEqual(run.status, "paused_loss")
        self.assertIn("净盈亏=0.00", events[0][2])
        app._ensure_trader_watcher.assert_not_called()

    def test_trader_desk_slot_for_session_prefers_trader_slot_id_over_reused_session_id(self) -> None:
        old_slot = TraderSlotRecord(
            slot_id="slot-old",
            trader_id="T001",
            session_id="S02",
            api_name="moni",
            strategy_name="EMA",
            symbol="ETH-USDT-SWAP",
            status="open",
            created_at=datetime(2026, 4, 26, 12, 7, 21),
        )
        new_slot = TraderSlotRecord(
            slot_id="slot-new",
            trader_id="T001",
            session_id="S02",
            api_name="moni",
            strategy_name="EMA",
            symbol="ETH-USDT-SWAP",
            status="watching",
            created_at=datetime(2026, 4, 27, 8, 5, 56),
        )
        app = SimpleNamespace(_trader_desk_slots=[old_slot, new_slot])

        matched = QuantApp._trader_desk_slot_for_session(app, "S02", "slot-old")

        self.assertIs(matched, old_slot)

    def test_apply_trader_desk_reconciliation_overwrites_slot_open_fields_from_ledger(self) -> None:
        draft = TraderDraftRecord(
            trader_id="T001",
            template_payload={"strategy_id": "ema_dynamic_short"},
            total_quota=Decimal("1"),
            unit_quota=Decimal("0.1"),
            quota_steps=10,
            pause_on_stop_loss=False,
        )
        run = TraderRunState(trader_id="T001", status="running", armed_session_id="S02")
        slot = TraderSlotRecord(
            slot_id="slot-new",
            trader_id="T001",
            session_id="S02",
            api_name="moni",
            strategy_name="EMA",
            symbol="ETH-USDT-SWAP",
            status="open",
            quota_occupied=True,
            opened_at=datetime(2026, 4, 26, 12, 15, 28),
            entry_price=Decimal("2310.67"),
            size=Decimal("0.1"),
        )
        events: list[tuple[str, str, str]] = []
        app = SimpleNamespace(
            _trader_desk_slot_for_session=lambda session_id, trader_slot_id="": slot,
            _trader_desk_draft_by_id=lambda trader_id: draft,
            _trader_desk_run_by_id=lambda trader_id, create=False: run,
            _trader_desk_add_event=lambda trader_id, message, level="info": events.append((trader_id, level, message)),
            _save_trader_desk_snapshot=MagicMock(),
            _ensure_trader_watcher=MagicMock(),
        )
        session = SimpleNamespace(
            session_id="S02",
            trader_id="T001",
            trader_slot_id="slot-new",
            ended_reason="策略主动平仓",
            history_record_id="H02",
        )
        ledger_record = SimpleNamespace(
            close_reason="策略主动平仓",
            net_pnl=Decimal("-0.27"),
            opened_at=datetime(2026, 4, 27, 8, 37, 57),
            closed_at=datetime(2026, 4, 27, 13, 16, 52),
            entry_price=Decimal("2364.81"),
            exit_price=Decimal("2356.62"),
            size=Decimal("0.1"),
            history_record_id="H02",
        )

        QuantApp._apply_trader_desk_reconciliation(app, session, ledger_record)

        self.assertEqual(slot.opened_at, datetime(2026, 4, 27, 8, 37, 57))
        self.assertEqual(slot.entry_price, Decimal("2364.81"))
        self.assertEqual(slot.exit_price, Decimal("2356.62"))

    def test_repair_trader_desk_slots_from_trade_ledger_rewrites_closed_slot_fields(self) -> None:
        slot = TraderSlotRecord(
            slot_id="slot-1",
            trader_id="T002",
            session_id="S02",
            api_name="moni",
            strategy_name="EMA",
            symbol="ETH-USDT-SWAP",
            status="closed_loss",
            opened_at=datetime(2026, 4, 26, 12, 15, 28),
            closed_at=datetime(2026, 4, 27, 13, 16, 52),
            entry_price=Decimal("2310.67"),
            exit_price=Decimal("2356.62"),
            size=Decimal("0.1"),
            net_pnl=Decimal("-0.27"),
            close_reason="策略主动平仓",
            history_record_id="H02",
        )
        ledger_record = StrategyTradeLedgerRecord(
            record_id="L01",
            history_record_id="H02",
            session_id="S02",
            api_name="moni",
            strategy_id="ema_dynamic_order_short",
            strategy_name="EMA 动态委托做空",
            symbol="ETH-USDT-SWAP",
            direction_label="只做空",
            run_mode_label="交易并下单",
            environment="demo",
            opened_at=datetime(2026, 4, 27, 8, 37, 57),
            closed_at=datetime(2026, 4, 27, 13, 16, 52),
            entry_price=Decimal("2364.81"),
            exit_price=Decimal("2356.62"),
            size=Decimal("0.1"),
            net_pnl=Decimal("-0.270206047"),
            close_reason="策略主动平仓",
        )
        app = SimpleNamespace(
            _strategy_trade_ledger_records=[ledger_record],
            _trader_desk_slots=[slot],
            _save_trader_desk_snapshot=MagicMock(),
        )

        QuantApp._repair_trader_desk_slots_from_trade_ledger(app)

        self.assertEqual(slot.opened_at, datetime(2026, 4, 27, 8, 37, 57))
        self.assertEqual(slot.entry_price, Decimal("2364.81"))
        self.assertEqual(slot.net_pnl, Decimal("-0.270206047"))
        app._save_trader_desk_snapshot.assert_called_once()

    def test_update_session_counter_from_session_id_uses_max_seen_value(self) -> None:
        app = SimpleNamespace(_session_counter=2)

        QuantApp._update_session_counter_from_session_id(app, "S15")
        QuantApp._update_session_counter_from_session_id(app, "S03")
        QuantApp._update_session_counter_from_session_id(app, "bad-id")

        self.assertEqual(app._session_counter, 15)

    def test_trader_desk_start_slot_skips_when_armed_session_already_exists(self) -> None:
        draft = TraderDraftRecord(
            trader_id="T001",
            template_payload={"strategy_id": "ema_dynamic_short"},
            total_quota=Decimal("1"),
            unit_quota=Decimal("0.1"),
            quota_steps=10,
        )
        run = TraderRunState(trader_id="T001", status="running", armed_session_id="__starting__")
        app = SimpleNamespace(
            _trader_desk_draft_by_id=lambda trader_id: draft,
            _trader_desk_run_by_id=lambda trader_id, create=False: run,
        )

        started = QuantApp._trader_desk_start_slot(app, "T001")

        self.assertFalse(started)

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

    def test_force_clear_trader_draft_releases_local_slots_and_requests_stop(self) -> None:
        draft = TraderDraftRecord(
            trader_id="T001",
            template_payload={"strategy_id": "ema_dynamic_long"},
            total_quota=Decimal("1"),
            unit_quota=Decimal("0.1"),
            quota_steps=10,
            status="ready",
        )
        run = TraderRunState(trader_id="T001", status="running", armed_session_id="S01")
        watching_slot = TraderSlotRecord(
            slot_id="slot-1",
            trader_id="T001",
            session_id="S01",
            api_name="moni",
            strategy_name="EMA",
            symbol="BTC-USDT-SWAP",
            status="watching",
        )
        open_slot = TraderSlotRecord(
            slot_id="slot-2",
            trader_id="T001",
            session_id="S02",
            api_name="moni",
            strategy_name="EMA",
            symbol="BTC-USDT-SWAP",
            status="open",
            quota_occupied=True,
            entry_price=Decimal("2300"),
            size=Decimal("0.1"),
        )
        closed_slot = TraderSlotRecord(
            slot_id="slot-3",
            trader_id="T001",
            session_id="S03",
            api_name="moni",
            strategy_name="EMA",
            symbol="BTC-USDT-SWAP",
            status="closed_profit",
            quota_occupied=False,
            closed_at=datetime(2026, 4, 25, 10, 0, 0),
            released_at=datetime(2026, 4, 25, 10, 0, 0),
        )
        events: list[tuple[str, str, str]] = []
        stop_requests: list[str] = []
        app = SimpleNamespace(
            _trader_desk_draft_by_id=lambda trader_id: draft if trader_id == "T001" else None,
            _trader_desk_run_by_id=lambda trader_id, create=False: run if trader_id == "T001" else None,
            _trader_desk_slots=[watching_slot, open_slot, closed_slot],
            sessions={
                "S01": SimpleNamespace(session_id="S01", trader_id="T001", stop_cleanup_in_progress=False, engine=SimpleNamespace(is_running=True)),
                "S02": SimpleNamespace(session_id="S02", trader_id="T001", stop_cleanup_in_progress=False, engine=SimpleNamespace(is_running=False)),
            },
            _request_stop_strategy_session=lambda session_id, **kwargs: stop_requests.append(session_id) or True,
            _trader_desk_add_event=lambda trader_id, message, level="info": events.append((trader_id, level, message)),
            _save_trader_desk_snapshot=MagicMock(),
        )

        QuantApp.force_clear_trader_draft(app, "T001")

        self.assertEqual(draft.status, "paused")
        self.assertEqual(run.status, "paused_manual")
        self.assertEqual(run.paused_reason, "人工强制清格。")
        self.assertEqual(run.armed_session_id, "")
        self.assertEqual(stop_requests, ["S01"])
        self.assertEqual(watching_slot.status, "stopped")
        self.assertEqual(watching_slot.close_reason, "人工强制清格")
        self.assertIsNotNone(watching_slot.released_at)
        self.assertEqual(open_slot.status, "stopped")
        self.assertFalse(open_slot.quota_occupied)
        self.assertEqual(open_slot.close_reason, "人工强制清格（未同步平仓结果）")
        self.assertEqual(closed_slot.status, "closed_profit")
        self.assertEqual(events[0][0], "T001")
        self.assertEqual(events[0][1], "warning")
        self.assertIn("已强制清理 2 个额度格", events[0][2])
        app._save_trader_desk_snapshot.assert_called_once()
        app._save_trader_desk_snapshot.assert_called_once()

    def test_flatten_trader_draft_submits_market_close_orders_and_marks_slots_closed_manual(self) -> None:
        draft = TraderDraftRecord(
            trader_id="T001",
            template_payload={
                "api_name": "moni",
                "symbol": "ETH-USDT-SWAP",
                "config_snapshot": {
                    "inst_id": "ETH-USDT-SWAP",
                    "trade_inst_id": "ETH-USDT-SWAP",
                    "bar": "1m",
                    "ema_period": 21,
                    "atr_period": 10,
                    "atr_stop_multiplier": "1",
                    "atr_take_multiplier": "4",
                    "order_size": "0.1",
                    "trade_mode": "cross",
                    "signal_mode": "short_only",
                    "position_mode": "net",
                    "environment": "demo",
                    "tp_sl_trigger_type": "last",
                },
            },
            total_quota=Decimal("1"),
            unit_quota=Decimal("0.1"),
            quota_steps=10,
            status="ready",
        )
        run = TraderRunState(trader_id="T001", status="running", armed_session_id="S30")
        watching_slot = TraderSlotRecord(
            slot_id="slot-watch",
            trader_id="T001",
            session_id="S33",
            api_name="moni",
            strategy_name="EMA",
            symbol="ETH-USDT-SWAP",
            status="watching",
        )
        open_slot_one = TraderSlotRecord(
            slot_id="slot-open-1",
            trader_id="T001",
            session_id="S21",
            api_name="moni",
            strategy_name="EMA",
            symbol="ETH-USDT-SWAP",
            status="open",
            quota_occupied=True,
            size=Decimal("0.1"),
            entry_price=Decimal("2300"),
        )
        open_slot_two = TraderSlotRecord(
            slot_id="slot-open-2",
            trader_id="T001",
            session_id="S22",
            api_name="moni",
            strategy_name="EMA",
            symbol="ETH-USDT-SWAP",
            status="open",
            quota_occupied=True,
            size=Decimal("0.1"),
            entry_price=Decimal("2301"),
        )
        slots = [watching_slot, open_slot_one, open_slot_two]
        stop_requests: list[str] = []
        events: list[tuple[str, str, str]] = []

        class _StubClient:
            def __init__(self) -> None:
                self.orders: list[dict[str, object]] = []

            @staticmethod
            def get_instrument(inst_id: str) -> Instrument:
                return Instrument(
                    inst_id=inst_id,
                    inst_type="SWAP",
                    tick_size=Decimal("0.01"),
                    lot_size=Decimal("0.1"),
                    min_size=Decimal("0.1"),
                    state="live",
                )

            @staticmethod
            def get_positions(credentials, *, environment: str):  # noqa: ANN001
                return [
                    OkxPosition(
                        inst_id="ETH-USDT-SWAP",
                        inst_type="SWAP",
                        pos_side="net",
                        mgn_mode="cross",
                        position=Decimal("-0.2"),
                        avail_position=Decimal("-0.2"),
                        avg_price=Decimal("2300"),
                        mark_price=None,
                        unrealized_pnl=None,
                        unrealized_pnl_ratio=None,
                        liquidation_price=None,
                        leverage=None,
                        margin_ccy="USDT",
                        last_price=None,
                        realized_pnl=None,
                        margin_ratio=None,
                        initial_margin=None,
                        maintenance_margin=None,
                        delta=None,
                        gamma=None,
                        vega=None,
                        theta=None,
                        raw={},
                    )
                ]

            def place_simple_order(self, credentials, config, *, inst_id: str, side: str, size: Decimal, ord_type: str, pos_side=None, price=None, cl_ord_id=None):  # noqa: ANN001,E501
                self.orders.append(
                    {
                        "inst_id": inst_id,
                        "side": side,
                        "size": size,
                        "ord_type": ord_type,
                        "pos_side": pos_side,
                        "cl_ord_id": cl_ord_id,
                    }
                )
                return OkxOrderResult(
                    ord_id=f"ord-{len(self.orders)}",
                    cl_ord_id=str(cl_ord_id or ""),
                    s_code="0",
                    s_msg="accepted",
                    raw={},
                )

            @staticmethod
            def get_order(credentials, config, *, inst_id: str, ord_id=None, cl_ord_id=None):  # noqa: ANN001
                return OkxOrderStatus(
                    ord_id=str(ord_id or "ord-1"),
                    state="filled",
                    side="buy",
                    ord_type="market",
                    price=Decimal("2299.5"),
                    avg_price=Decimal("2299.5"),
                    size=Decimal("0.1"),
                    filled_size=Decimal("0.1"),
                    raw={},
                )

        client = _StubClient()
        app = SimpleNamespace(
            client=client,
            _trader_desk_draft_by_id=lambda trader_id: draft if trader_id == "T001" else None,
            _trader_desk_run_by_id=lambda trader_id, create=False: run if trader_id == "T001" else None,
            _trader_desk_slots_for_statuses=lambda trader_id, statuses: [slot for slot in slots if slot.status in statuses],
            _request_stop_strategy_session=lambda session_id, **kwargs: stop_requests.append(session_id) or True,
            _trader_desk_add_event=lambda trader_id, message, level="info": events.append((trader_id, level, message)),
            _save_trader_desk_snapshot=MagicMock(),
            _credentials_for_profile_or_none=lambda profile_name: SimpleNamespace(profile_name=profile_name),
            _submit_trader_manual_flatten_orders=lambda draft_record, open_slot_records, now, flatten_mode="market": QuantApp._submit_trader_manual_flatten_orders(
                app,
                draft_record,
                open_slot_records,
                now,
                flatten_mode=flatten_mode,
            ),
            _lookup_trader_manual_flatten_order_status=lambda credentials, config, inst_id, result: QuantApp._lookup_trader_manual_flatten_order_status(
                app,
                credentials,
                config,
                inst_id=inst_id,
                result=result,
            ),
            _build_trader_manual_flatten_cl_ord_id=QuantApp._build_trader_manual_flatten_cl_ord_id,
            _trader_manual_flatten_open_side=QuantApp._trader_manual_flatten_open_side,
            _trader_position_closeable_size=QuantApp._trader_position_closeable_size,
            _trader_slot_flatten_size=QuantApp._trader_slot_flatten_size,
            _normalize_trader_manual_flatten_mode=QuantApp._normalize_trader_manual_flatten_mode,
            _trader_manual_flatten_mode_label=QuantApp._trader_manual_flatten_mode_label,
            _resolve_trader_best_quote_flatten_price=lambda instrument, side: QuantApp._resolve_trader_best_quote_flatten_price(
                app,
                instrument,
                side=side,
            ),
            _clear_trader_manual_flatten_pending=QuantApp._clear_trader_manual_flatten_pending,
            _mark_trader_slot_manual_flatten_closed=lambda slot, now, exit_price, flatten_mode: QuantApp._mark_trader_slot_manual_flatten_closed(
                app,
                slot,
                now=now,
                exit_price=exit_price,
                flatten_mode=flatten_mode,
            ),
        )

        QuantApp.flatten_trader_draft(app, "T001")

        self.assertEqual(draft.status, "paused")
        self.assertEqual(run.status, "paused_manual")
        self.assertEqual(run.armed_session_id, "")
        self.assertEqual(stop_requests, ["S21", "S22", "S33"])
        self.assertEqual(watching_slot.status, "stopped")
        self.assertEqual(open_slot_one.status, "closed_manual")
        self.assertEqual(open_slot_two.status, "closed_manual")
        self.assertFalse(open_slot_one.quota_occupied)
        self.assertFalse(open_slot_two.quota_occupied)
        self.assertEqual(open_slot_one.close_reason, "人工手动平仓")
        self.assertEqual(open_slot_two.close_reason, "人工手动平仓")
        self.assertEqual(open_slot_one.exit_price, Decimal("2299.5"))
        self.assertEqual(len(client.orders), 2)
        self.assertEqual(client.orders[0]["side"], "buy")
        self.assertEqual(client.orders[1]["side"], "buy")
        self.assertTrue(any("手动平仓结果" in event[2] for event in events))
        app._save_trader_desk_snapshot.assert_called_once()

    def test_submit_trader_manual_flatten_orders_best_quote_keeps_slot_open_until_filled(self) -> None:
        draft = TraderDraftRecord(
            trader_id="T001",
            template_payload={
                "api_name": "moni",
                "symbol": "ETH-USDT-SWAP",
                "config_snapshot": {
                    "inst_id": "ETH-USDT-SWAP",
                    "trade_inst_id": "ETH-USDT-SWAP",
                    "bar": "1m",
                    "ema_period": 21,
                    "atr_period": 10,
                    "atr_stop_multiplier": "1",
                    "atr_take_multiplier": "4",
                    "order_size": "0.1",
                    "trade_mode": "cross",
                    "signal_mode": "short_only",
                    "position_mode": "net",
                    "environment": "demo",
                    "tp_sl_trigger_type": "last",
                },
            },
            total_quota=Decimal("1"),
            unit_quota=Decimal("0.1"),
            quota_steps=10,
            status="ready",
        )
        slot = TraderSlotRecord(
            slot_id="slot-open-1",
            trader_id="T001",
            session_id="S21",
            api_name="moni",
            strategy_name="EMA",
            symbol="ETH-USDT-SWAP",
            status="open",
            quota_occupied=True,
            size=Decimal("0.1"),
            entry_price=Decimal("2300"),
        )
        events: list[tuple[str, str, str]] = []

        class _StubClient:
            def __init__(self) -> None:
                self.orders: list[dict[str, object]] = []

            @staticmethod
            def get_instrument(inst_id: str) -> Instrument:
                return Instrument(
                    inst_id=inst_id,
                    inst_type="SWAP",
                    tick_size=Decimal("0.01"),
                    lot_size=Decimal("0.1"),
                    min_size=Decimal("0.1"),
                    state="live",
                )

            @staticmethod
            def get_positions(credentials, *, environment: str):  # noqa: ANN001
                return [
                    OkxPosition(
                        inst_id="ETH-USDT-SWAP",
                        inst_type="SWAP",
                        pos_side="net",
                        mgn_mode="cross",
                        position=Decimal("-0.1"),
                        avail_position=Decimal("-0.1"),
                        avg_price=Decimal("2300"),
                        mark_price=None,
                        unrealized_pnl=None,
                        unrealized_pnl_ratio=None,
                        liquidation_price=None,
                        leverage=None,
                        margin_ccy="USDT",
                        last_price=None,
                        realized_pnl=None,
                        margin_ratio=None,
                        initial_margin=None,
                        maintenance_margin=None,
                        delta=None,
                        gamma=None,
                        vega=None,
                        theta=None,
                        raw={},
                    )
                ]

            @staticmethod
            def get_order_book(inst_id: str, depth: int = 5):  # noqa: ANN001
                return SimpleNamespace(
                    inst_id=inst_id,
                    bids=((Decimal("2299.10"), Decimal("5")),),
                    asks=((Decimal("2299.20"), Decimal("5")),),
                    raw={},
                )

            @staticmethod
            def get_ticker(inst_id: str):  # noqa: ANN001
                return SimpleNamespace(inst_id=inst_id, bid=Decimal("2299.10"), ask=Decimal("2299.20"))

            def place_simple_order(self, credentials, config, *, inst_id: str, side: str, size: Decimal, ord_type: str, pos_side=None, price=None, cl_ord_id=None):  # noqa: ANN001,E501
                self.orders.append(
                    {
                        "inst_id": inst_id,
                        "side": side,
                        "size": size,
                        "ord_type": ord_type,
                        "price": price,
                        "pos_side": pos_side,
                        "cl_ord_id": cl_ord_id,
                    }
                )
                return OkxOrderResult(
                    ord_id="ord-1",
                    cl_ord_id=str(cl_ord_id or ""),
                    s_code="0",
                    s_msg="accepted",
                    raw={},
                )

            @staticmethod
            def get_order(credentials, config, *, inst_id: str, ord_id=None, cl_ord_id=None):  # noqa: ANN001
                return OkxOrderStatus(
                    ord_id=str(ord_id or "ord-1"),
                    state="live",
                    side="buy",
                    ord_type="limit",
                    price=Decimal("2299.10"),
                    avg_price=None,
                    size=Decimal("0.1"),
                    filled_size=Decimal("0"),
                    raw={},
                )

        client = _StubClient()
        app = SimpleNamespace(
            client=client,
            _credentials_for_profile_or_none=lambda profile_name: SimpleNamespace(profile_name=profile_name),
            _trader_desk_add_event=lambda trader_id, message, level="info": events.append((trader_id, level, message)),
            _trader_manual_flatten_open_side=QuantApp._trader_manual_flatten_open_side,
            _trader_position_closeable_size=QuantApp._trader_position_closeable_size,
            _trader_slot_flatten_size=QuantApp._trader_slot_flatten_size,
            _build_trader_manual_flatten_cl_ord_id=QuantApp._build_trader_manual_flatten_cl_ord_id,
            _lookup_trader_manual_flatten_order_status=lambda credentials, config, inst_id, result: QuantApp._lookup_trader_manual_flatten_order_status(
                app,
                credentials,
                config,
                inst_id=inst_id,
                result=result,
            ),
            _normalize_trader_manual_flatten_mode=QuantApp._normalize_trader_manual_flatten_mode,
            _trader_manual_flatten_mode_label=QuantApp._trader_manual_flatten_mode_label,
            _resolve_trader_best_quote_flatten_price=lambda instrument, side: QuantApp._resolve_trader_best_quote_flatten_price(
                app,
                instrument,
                side=side,
            ),
            _clear_trader_manual_flatten_pending=QuantApp._clear_trader_manual_flatten_pending,
            _mark_trader_slot_manual_flatten_closed=lambda opened_slot, now, exit_price, flatten_mode: QuantApp._mark_trader_slot_manual_flatten_closed(
                app,
                opened_slot,
                now=now,
                exit_price=exit_price,
                flatten_mode=flatten_mode,
            ),
        )

        submitted_count, stale_count, failed_count = QuantApp._submit_trader_manual_flatten_orders(
            app,
            draft,
            [slot],
            datetime(2026, 4, 28, 11, 0, 0),
            flatten_mode="best_quote",
        )

        self.assertEqual((submitted_count, stale_count, failed_count), (1, 0, 0))
        self.assertEqual(slot.status, "open")
        self.assertTrue(slot.quota_occupied)
        self.assertEqual(slot.pending_manual_exit_mode, "best_quote")
        self.assertEqual(slot.pending_manual_exit_inst_id, "ETH-USDT-SWAP")
        self.assertEqual(slot.pending_manual_exit_order_id, "ord-1")
        self.assertEqual(client.orders[0]["ord_type"], "limit")
        self.assertEqual(client.orders[0]["price"], Decimal("2299.10"))
        self.assertIn("挂单价=2299.1", slot.note)
        self.assertTrue(any("待成交" in event[2] for event in events))

    def test_refresh_trader_pending_manual_flatten_orders_marks_filled_slot_closed_manual(self) -> None:
        draft = TraderDraftRecord(
            trader_id="T001",
            template_payload={
                "api_name": "moni",
                "symbol": "ETH-USDT-SWAP",
                "config_snapshot": {
                    "inst_id": "ETH-USDT-SWAP",
                    "trade_inst_id": "ETH-USDT-SWAP",
                    "bar": "1m",
                    "ema_period": 21,
                    "atr_period": 10,
                    "atr_stop_multiplier": "1",
                    "atr_take_multiplier": "4",
                    "order_size": "0.1",
                    "trade_mode": "cross",
                    "signal_mode": "short_only",
                    "position_mode": "net",
                    "environment": "demo",
                    "tp_sl_trigger_type": "last",
                },
            },
            total_quota=Decimal("1"),
            unit_quota=Decimal("0.1"),
            quota_steps=10,
            status="paused",
        )
        slot = TraderSlotRecord(
            slot_id="slot-open-1",
            trader_id="T001",
            session_id="S21",
            api_name="moni",
            strategy_name="EMA",
            symbol="ETH-USDT-SWAP",
            status="open",
            quota_occupied=True,
            size=Decimal("0.1"),
            entry_price=Decimal("2300"),
            pending_manual_exit_mode="best_quote",
            pending_manual_exit_inst_id="ETH-USDT-SWAP",
            pending_manual_exit_order_id="ord-1",
            pending_manual_exit_cl_ord_id="cl-1",
        )
        events: list[tuple[str, str, str]] = []

        app = SimpleNamespace(
            _trader_desk_slots=[slot],
            client=SimpleNamespace(
                get_order=lambda credentials, config, *, inst_id, ord_id=None, cl_ord_id=None: OkxOrderStatus(
                    ord_id=str(ord_id or "ord-1"),
                    state="filled",
                    side="buy",
                    ord_type="limit",
                    price=Decimal("2299.10"),
                    avg_price=Decimal("2299.08"),
                    size=Decimal("0.1"),
                    filled_size=Decimal("0.1"),
                    raw={},
                )
            ),
            _trader_desk_draft_by_id=lambda trader_id: draft if trader_id == "T001" else None,
            _credentials_for_profile_or_none=lambda profile_name: SimpleNamespace(profile_name=profile_name),
            _trader_desk_add_event=lambda trader_id, message, level="info": events.append((trader_id, level, message)),
            _save_trader_desk_snapshot=MagicMock(),
            _mark_trader_slot_manual_flatten_closed=lambda opened_slot, now, exit_price, flatten_mode: QuantApp._mark_trader_slot_manual_flatten_closed(
                app,
                opened_slot,
                now=now,
                exit_price=exit_price,
                flatten_mode=flatten_mode,
            ),
            _normalize_trader_manual_flatten_mode=QuantApp._normalize_trader_manual_flatten_mode,
            _trader_manual_flatten_mode_label=QuantApp._trader_manual_flatten_mode_label,
            _clear_trader_manual_flatten_pending=QuantApp._clear_trader_manual_flatten_pending,
        )

        QuantApp._refresh_trader_pending_manual_flatten_orders(app, "T001")

        self.assertEqual(slot.status, "closed_manual")
        self.assertFalse(slot.quota_occupied)
        self.assertEqual(slot.exit_price, Decimal("2299.08"))
        self.assertEqual(slot.pending_manual_exit_order_id, "")
        self.assertTrue(any("人工平仓单已成交" in event[2] for event in events))
        app._save_trader_desk_snapshot.assert_called_once()

    def test_submit_selected_position_manual_flatten_best_quote_uses_ask1_for_long_position(self) -> None:
        position = OkxPosition(
            inst_id="ETH-USDT-SWAP",
            inst_type="SWAP",
            pos_side="net",
            mgn_mode="cross",
            position=Decimal("0.3"),
            avail_position=Decimal("0.2"),
            avg_price=Decimal("2300"),
            mark_price=None,
            unrealized_pnl=None,
            unrealized_pnl_ratio=None,
            liquidation_price=None,
            leverage=None,
            margin_ccy="USDT",
            last_price=None,
            realized_pnl=None,
            margin_ratio=None,
            initial_margin=None,
            maintenance_margin=None,
            delta=None,
            gamma=None,
            vega=None,
            theta=None,
            raw={},
        )

        class _StubClient:
            def __init__(self) -> None:
                self.orders: list[dict[str, object]] = []

            @staticmethod
            def get_instrument(inst_id: str) -> Instrument:
                return Instrument(
                    inst_id=inst_id,
                    inst_type="SWAP",
                    tick_size=Decimal("0.01"),
                    lot_size=Decimal("0.1"),
                    min_size=Decimal("0.1"),
                    state="live",
                )

            @staticmethod
            def get_order_book(inst_id: str, depth: int = 5):  # noqa: ANN001
                return SimpleNamespace(
                    inst_id=inst_id,
                    bids=((Decimal("2299.10"), Decimal("5")),),
                    asks=((Decimal("2299.20"), Decimal("5")),),
                    raw={},
                )

            @staticmethod
            def get_ticker(inst_id: str):  # noqa: ANN001
                return SimpleNamespace(inst_id=inst_id, bid=Decimal("2299.10"), ask=Decimal("2299.20"))

            def place_simple_order(self, credentials, config, *, inst_id: str, side: str, size: Decimal, ord_type: str, pos_side=None, price=None, cl_ord_id=None):  # noqa: ANN001,E501
                self.orders.append(
                    {
                        "inst_id": inst_id,
                        "side": side,
                        "size": size,
                        "ord_type": ord_type,
                        "pos_side": pos_side,
                        "price": price,
                        "cl_ord_id": cl_ord_id,
                    }
                )
                return OkxOrderResult(
                    ord_id="ord-position-1",
                    cl_ord_id=str(cl_ord_id or ""),
                    s_code="0",
                    s_msg="accepted",
                    raw={},
                )

        client = _StubClient()
        app = SimpleNamespace(
            client=client,
            _positions_context_profile_name="apiA",
            _positions_effective_environment="demo",
            environment_label=_Var("模拟盘"),
            trade_mode_label=_Var("全仓"),
            _credentials_for_profile_or_none=lambda profile_name: SimpleNamespace(profile_name=profile_name),
            _normalize_position_manual_flatten_mode=QuantApp._normalize_position_manual_flatten_mode,
            _build_selected_position_manual_flatten_config=lambda selected: QuantApp._build_selected_position_manual_flatten_config(
                app,
                selected,
            ),
            _selected_position_close_size=lambda selected: QuantApp._selected_position_close_size(app, selected),
            _resolve_trader_best_quote_flatten_price=lambda instrument, side: QuantApp._resolve_trader_best_quote_flatten_price(
                app,
                instrument,
                side=side,
            ),
        )

        result, price, normalized_mode = QuantApp._submit_selected_position_manual_flatten(
            app,
            position,
            "best_quote",
        )

        self.assertEqual(result.ord_id, "ord-position-1")
        self.assertEqual(price, Decimal("2299.20"))
        self.assertEqual(normalized_mode, "best_quote")
        self.assertEqual(client.orders[0]["side"], "sell")
        self.assertEqual(client.orders[0]["ord_type"], "limit")
        self.assertEqual(client.orders[0]["price"], Decimal("2299.20"))
        self.assertEqual(client.orders[0]["size"], Decimal("0.2"))

    def test_flatten_selected_position_best_quote_submits_and_refreshes(self) -> None:
        position = OkxPosition(
            inst_id="ETH-USDT-SWAP",
            inst_type="SWAP",
            pos_side="net",
            mgn_mode="cross",
            position=Decimal("-0.1"),
            avail_position=Decimal("-0.1"),
            avg_price=Decimal("2300"),
            mark_price=None,
            unrealized_pnl=None,
            unrealized_pnl_ratio=None,
            liquidation_price=None,
            leverage=None,
            margin_ccy="USDT",
            last_price=None,
            realized_pnl=None,
            margin_ratio=None,
            initial_margin=None,
            maintenance_margin=None,
            delta=None,
            gamma=None,
            vega=None,
            theta=None,
            raw={},
        )
        app = SimpleNamespace(
            _selected_position_item=lambda: position,
            _position_action_parent=lambda: "parent-window",
            _positions_context_profile_name=None,
            _current_credential_profile=lambda: "test-profile",
            _position_manual_flatten_mode_label=QuantApp._position_manual_flatten_mode_label,
            _submit_selected_position_manual_flatten=lambda selected, flatten_mode: (
                OkxOrderResult(
                    ord_id="ord-position-2",
                    cl_ord_id="cl-position-2",
                    s_code="0",
                    s_msg="accepted",
                    raw={},
                ),
                Decimal("2299.10"),
                "best_quote",
            ),
            _enqueue_log=MagicMock(),
            refresh_positions=MagicMock(),
            refresh_order_views=MagicMock(),
        )

        with patch("okx_quant.ui.messagebox.askyesnocancel", return_value=False) as askyesnocancel, patch(
            "okx_quant.ui.messagebox.showinfo"
        ) as showinfo:
            QuantApp.flatten_selected_position(app)

        askyesnocancel.assert_called_once()
        showinfo.assert_called_once()
        show_args, show_kwargs = showinfo.call_args
        self.assertEqual(show_args[0], "平仓已提交")
        self.assertIn("方式：挂买一/卖一平仓", show_args[1])
        self.assertIn("挂单价：2299.1", show_args[1])
        self.assertEqual(show_kwargs["parent"], "parent-window")
        app._enqueue_log.assert_called_once()
        app.refresh_positions.assert_called_once()
        app.refresh_order_views.assert_called_once()

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


class _LabelStub:
    def __init__(self, text: str = "") -> None:
        self.text = text

    def configure(self, **kwargs: object) -> None:
        if "text" in kwargs:
            self.text = str(kwargs["text"])


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

    @staticmethod
    def identify_column(_x: int) -> str:
        return "#1"

    @staticmethod
    def identify_row(_y: int) -> str:
        return ""

    def item(self, iid: str, option: str | None = None, **kwargs: object):
        row = self.rows.setdefault(iid, {})
        if kwargs:
            row.update(kwargs)
            return None
        if option is None:
            return row
        return row.get(option)

    def insert(
        self,
        _parent: str,
        _index: object,
        *,
        iid: str,
        values: tuple[object, ...],
        tags: tuple[str, ...] = (),
        text: str = "",
    ) -> None:
        self.rows[iid] = {"values": values, "tags": tags, "text": text}

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

    def focus(self, iid: str | None = None):
        if iid is None:
            return self.focused or ""
        self.focused = iid

    def see(self, iid: str) -> None:
        self.seen = iid


class TraderWaveLockTest(TestCase):
    def test_trader_wave_lock_signal_from_session_uses_signal_mode(self) -> None:
        session = SimpleNamespace(config=SimpleNamespace(signal_mode="long_only"), direction_label="双向")

        resolved = QuantApp._trader_wave_lock_signal_from_session(session)

        self.assertEqual(resolved, "long")

    def test_trader_desk_sync_open_trade_state_sets_wave_lock_signal(self) -> None:
        run = TraderRunState(trader_id="T002", status="running", armed_session_id="S01")
        slot = TraderSlotRecord(
            slot_id="slot-1",
            trader_id="T002",
            session_id="S01",
            api_name="moni",
            strategy_name="EMA",
            symbol="BTC-USDT-SWAP",
            status="watching",
        )
        trade = StrategyTradeRuntimeState(
            round_id="S01-1",
            opened_logged_at=datetime(2026, 4, 30, 8, 37, 0),
            entry_price=Decimal("95200"),
            size=Decimal("0.01"),
        )
        session = SimpleNamespace(
            session_id="S01",
            trader_id="T002",
            trader_slot_id="slot-1",
            direction_label="只做多",
            config=SimpleNamespace(signal_mode="long_only"),
            active_trade=trade,
        )
        app = SimpleNamespace(
            _trader_desk_slot_for_session=lambda session_id, trader_slot_id="": slot,
            _trader_desk_run_by_id=lambda trader_id, create=False: run,
            _trader_desk_add_event=lambda trader_id, message, level="info": None,
            _save_trader_desk_snapshot=MagicMock(),
            _ensure_trader_watcher=MagicMock(),
        )

        QuantApp._trader_desk_sync_open_trade_state(app, session)

        self.assertEqual(run.armed_session_id, "")
        self.assertEqual(run.wave_lock_signal, "long")
        app._ensure_trader_watcher.assert_called_once_with("T002")

    def test_ensure_trader_watcher_skips_start_when_wave_lock_active(self) -> None:
        draft = TraderDraftRecord(
            trader_id="T002",
            template_payload={"strategy_id": "ema_dynamic_long"},
            total_quota=Decimal("1"),
            unit_quota=Decimal("0.1"),
            quota_steps=10,
        )
        run = TraderRunState(trader_id="T002", status="running", wave_lock_signal="long")
        app = SimpleNamespace(
            _trader_desk_draft_by_id=lambda trader_id: draft,
            _trader_desk_run_by_id=lambda trader_id: run,
            _trader_desk_slots=[],
            _cleanup_stale_trader_watchers=MagicMock(),
            _is_trader_wave_lock_active=lambda _draft, _run: True,
            _trader_desk_start_slot=MagicMock(),
            _enqueue_log=MagicMock(),
        )

        QuantApp._ensure_trader_watcher(app, "T002")

        app._trader_desk_start_slot.assert_not_called()


class PositionRealizedUsdtColumnTest(TestCase):
    def test_position_realized_pnl_usdt_converts_native_currency(self) -> None:
        position = SimpleNamespace(realized_pnl=Decimal("0.001"), margin_ccy="BTC", inst_id="BTC-USDT", inst_type="SPOT")

        self.assertEqual(_position_realized_pnl_usdt(position, {"BTC": Decimal("100000")}), Decimal("100"))

    def test_build_group_row_values_includes_realized_usdt_slot(self) -> None:
        values = _build_group_row_values(
            "组合",
            {
                "count": 2,
                "size_display": "1.5 BTC",
                "option_side_display": "--",
                "upl": Decimal("0.012"),
                "upl_usdt": Decimal("1200"),
                "realized": Decimal("0.001"),
                "realized_usdt": Decimal("100"),
                "market_value_usdt": Decimal("50000"),
                "pnl_currency": "BTC",
                "imr": None,
                "mmr": None,
                "delta": None,
                "gamma": None,
                "vega": None,
                "theta": None,
                "theta_usdt": None,
            },
        )

        self.assertEqual(values[19], "+0.00100")
        self.assertEqual(values[20], "+100")

    def test_insert_position_row_includes_realized_usdt_value(self) -> None:
        app = SimpleNamespace(
            position_tree=_SessionTreeStub(),
            _upl_usdt_prices={"BTC": Decimal("100000")},
            _position_instruments={},
            _position_tickers={},
            _position_row_payloads={},
            _current_position_note_summary=lambda _position: "减仓观察",
        )
        position = SimpleNamespace(
            inst_id="BTC-USDT",
            inst_type="SPOT",
            pos_side="net",
            position=Decimal("0.5"),
            avail_position=Decimal("0.5"),
            mgn_mode="cross",
            leverage=None,
            avg_price=Decimal("100000"),
            mark_price=Decimal("101000"),
            last_price=Decimal("101000"),
            unrealized_pnl=Decimal("0.01"),
            unrealized_pnl_ratio=None,
            realized_pnl=Decimal("0.001"),
            liquidation_price=None,
            margin_ratio=None,
            initial_margin=None,
            maintenance_margin=None,
            delta=None,
            gamma=None,
            vega=None,
            theta=None,
            margin_ccy="BTC",
        )

        QuantApp._insert_position_row(app, "", position, "P01")

        self.assertEqual(app.position_tree.rows["P01"]["values"][19], "+0.00100")
        self.assertEqual(app.position_tree.rows["P01"]["values"][20], "+100")
        self.assertEqual(app.position_tree.rows["P01"]["values"][-1], "减仓观察")


class PositionNotesLifecycleTest(TestCase):
    def _make_position(self, *, inst_id: str = "BTC-USD-260501-77000-C", pos_side: str = "short", mgn_mode: str = "cross"):
        return SimpleNamespace(inst_id=inst_id, inst_type="OPTION", pos_side=pos_side, mgn_mode=mgn_mode)

    def _make_position_history(
        self,
        *,
        inst_id: str = "BTC-USD-260501-77000-C",
        update_time: int = 2_000,
        pos_side: str = "short",
        direction: str = "short",
        mgn_mode: str = "cross",
        close_size: Decimal = Decimal("0.2"),
        close_avg_price: Decimal = Decimal("0.03"),
    ):
        return SimpleNamespace(
            inst_id=inst_id,
            inst_type="OPTION",
            update_time=update_time,
            pos_side=pos_side,
            direction=direction,
            mgn_mode=mgn_mode,
            close_size=close_size,
            close_avg_price=close_avg_price,
        )

    def test_reconcile_current_position_note_records_marks_missing_after_successful_refresh(self) -> None:
        position = self._make_position()
        current_key = _position_note_current_key("moni", "demo", position)
        current_notes = {
            current_key: _build_current_position_note_record(
                profile_name="moni",
                environment="demo",
                position=position,
                note="观察仓位",
                now_ms=1_000,
            )
        }

        changed = _reconcile_current_position_note_records(
            current_notes,
            profile_name="moni",
            environment="demo",
            positions=[],
            now_ms=3_000,
        )

        self.assertTrue(changed)
        self.assertEqual(current_notes[current_key]["missing_success_count"], 1)
        self.assertEqual(current_notes[current_key]["missing_started_at_ms"], 3_000)

    def test_inherit_position_history_notes_copies_current_note_for_partial_close(self) -> None:
        position = self._make_position()
        current_key = _position_note_current_key("moni", "demo", position)
        current_notes = {
            current_key: _build_current_position_note_record(
                profile_name="moni",
                environment="demo",
                position=position,
                note="周五前减 gamma",
                now_ms=1_000,
            )
        }
        history_item = self._make_position_history(update_time=2_000)
        history_notes: dict[str, dict[str, object]] = {}

        changed = _inherit_position_history_notes(
            current_notes,
            history_notes,
            profile_name="moni",
            environment="demo",
            position_history=[history_item],
            now_ms=2_500,
        )

        history_key = _position_history_note_key("moni", "demo", history_item)
        self.assertTrue(changed)
        self.assertEqual(history_notes[history_key]["note"], "周五前减 gamma")
        self.assertEqual(current_notes[current_key]["linked_history_keys"], [history_key])

    def test_prune_closed_current_position_notes_waits_for_history_after_missing(self) -> None:
        position = self._make_position()
        current_key = _position_note_current_key("moni", "demo", position)
        current_notes = {
            current_key: _build_current_position_note_record(
                profile_name="moni",
                environment="demo",
                position=position,
                note="平仓后复盘",
                now_ms=1_000,
            )
        }
        current_notes[current_key]["missing_success_count"] = 2
        current_notes[current_key]["missing_started_at_ms"] = 4_000
        history_item = self._make_position_history(update_time=3_500)
        history_key = _position_history_note_key("moni", "demo", history_item)
        history_notes = {
            history_key: _build_history_position_note_record(
                profile_name="moni",
                environment="demo",
                item=history_item,
                note="旧快照",
                now_ms=3_500,
                source_current_key=current_key,
            )
        }
        current_notes[current_key]["linked_history_keys"] = [history_key]

        changed = _prune_closed_current_position_notes(
            current_notes,
            history_notes,
            profile_name="moni",
            environment="demo",
        )
        self.assertFalse(changed)
        self.assertIn(current_key, current_notes)

        history_notes[history_key]["update_time"] = 4_500
        changed = _prune_closed_current_position_notes(
            current_notes,
            history_notes,
            profile_name="moni",
            environment="demo",
        )

        self.assertTrue(changed)
        self.assertNotIn(current_key, current_notes)

    def test_filter_positions_matches_note_keyword(self) -> None:
        position = self._make_position(inst_id="BTC-USD-260501-77000-C", pos_side="short", mgn_mode="cross")
        filtered = _filter_positions(
            [position],
            inst_type="",
            keyword="gamma",
            note_texts={_position_tree_row_id(position): "观察 gamma"},
        )
        self.assertEqual(len(filtered), 1)

    def test_filter_position_history_items_matches_note_keyword(self) -> None:
        history_item = self._make_position_history()

        filtered = _filter_position_history_items(
            [history_item],
            keyword="复盘",
            note_texts_by_index={0: "晚点复盘这一腿"},
        )

        self.assertEqual(len(filtered), 1)

    def test_format_position_note_summary_truncates_multi_line_notes(self) -> None:
        summary = _format_position_note_summary("第一行\n第二行 gamma 观察", limit=10)

        self.assertTrue(summary.endswith("…"))


class PositionZoomSelectionSyncTest(TestCase):
    def test_on_position_selected_ignores_programmatic_main_sync(self) -> None:
        tree = _SessionTreeStub()
        tree.selection_set("P01")
        app = SimpleNamespace(
            position_tree=tree,
            _position_selection_syncing=False,
            _positions_view_rendering=False,
            _position_selection_suppressed_item_id="P01",
            _refresh_position_detail_panel=MagicMock(),
        )

        QuantApp._on_position_selected(app)

        self.assertIsNone(app._position_selection_suppressed_item_id)
        app._refresh_position_detail_panel.assert_not_called()

    def test_on_positions_zoom_selected_ignores_reentrant_sync(self) -> None:
        tree = _SessionTreeStub()
        tree.selection_set("P01")
        app = SimpleNamespace(
            _positions_zoom_tree=tree,
            _positions_view_rendering=False,
            _position_selection_syncing=True,
            _positions_zoom_selection_suppressed_item_id=None,
            _positions_zoom_selected_item_id=None,
            _refresh_positions_zoom_detail=MagicMock(),
            _update_positions_zoom_search_shortcuts=MagicMock(),
            position_tree=_SessionTreeStub(),
        )

        QuantApp._on_positions_zoom_selected(app)

        self.assertIsNone(app._positions_zoom_selected_item_id)
        app._refresh_positions_zoom_detail.assert_not_called()
        app._update_positions_zoom_search_shortcuts.assert_not_called()

    def test_on_positions_zoom_selected_ignores_programmatic_zoom_sync(self) -> None:
        zoom_tree = _SessionTreeStub()
        zoom_tree.selection_set("P01")
        app = SimpleNamespace(
            _positions_zoom_tree=zoom_tree,
            _positions_view_rendering=False,
            _position_selection_syncing=False,
            _positions_zoom_selection_suppressed_item_id="P01",
            _positions_zoom_selected_item_id=None,
            _refresh_positions_zoom_detail=MagicMock(),
            _update_positions_zoom_search_shortcuts=MagicMock(),
            position_tree=_SessionTreeStub(),
        )

        QuantApp._on_positions_zoom_selected(app)

        self.assertEqual(app._positions_zoom_selected_item_id, "P01")
        self.assertIsNone(app._positions_zoom_selection_suppressed_item_id)
        app._refresh_positions_zoom_detail.assert_not_called()
        app._update_positions_zoom_search_shortcuts.assert_not_called()

    def test_on_positions_zoom_selected_syncs_main_selection_and_refreshes_detail(self) -> None:
        zoom_tree = _SessionTreeStub()
        zoom_tree.rows["P01"] = {"values": (), "tags": (), "text": ""}
        zoom_tree.selection_set("P01")
        main_tree = _SessionTreeStub()
        main_tree.rows["P01"] = {"values": (), "tags": (), "text": ""}
        app = SimpleNamespace(
            _positions_zoom_tree=zoom_tree,
            _positions_view_rendering=False,
            _position_selection_syncing=False,
            _positions_zoom_selection_suppressed_item_id=None,
            _positions_zoom_selected_item_id=None,
            _refresh_positions_zoom_detail=MagicMock(),
            _refresh_position_detail_panel=MagicMock(),
            _update_positions_zoom_search_shortcuts=MagicMock(),
            position_tree=main_tree,
        )
        app._sync_position_tree_selection = lambda item_id: QuantApp._sync_position_tree_selection(app, item_id)

        QuantApp._on_positions_zoom_selected(app)

        self.assertEqual(app._positions_zoom_selected_item_id, "P01")
        self.assertEqual(main_tree.selection(), ("P01",))
        self.assertEqual(app._position_selection_suppressed_item_id, "P01")
        app._refresh_position_detail_panel.assert_called_once()
        app._update_positions_zoom_search_shortcuts.assert_called_once()

    def test_refresh_position_detail_panel_skips_zoom_reselect_when_already_selected(self) -> None:
        zoom_tree = _SessionTreeStub()
        zoom_tree.rows["P01"] = {"values": (), "tags": (), "text": ""}
        zoom_tree.selection_set("P01")
        main_tree = _SessionTreeStub()
        main_tree.selection_set("P01")
        position = SimpleNamespace(inst_id="BTC-USD-260501-77000-C", inst_type="OPTION")
        app = SimpleNamespace(
            position_tree=main_tree,
            _positions_zoom_tree=zoom_tree,
            _position_selection_syncing=False,
            _positions_view_rendering=False,
            _positions_zoom_selection_suppressed_item_id=None,
            _upl_usdt_prices={},
            _position_instruments={},
            position_detail_text=_Var(""),
            _position_detail_panel=SimpleNamespace(),
            _position_row_payloads={"P01": {"kind": "position", "item": position, "label": "", "metrics": None}},
            _selected_position_payload=lambda: {"kind": "position", "item": position, "label": "", "metrics": None},
            _current_position_note_text=lambda _position: "",
            _set_readonly_text=MagicMock(),
            _refresh_positions_zoom_detail=MagicMock(),
            _refresh_protection_window_view=MagicMock(),
        )
        app._sync_positions_zoom_selection = lambda item_id: QuantApp._sync_positions_zoom_selection(app, item_id)

        with patch("okx_quant.ui._build_position_detail_text", return_value="detail"):
            QuantApp._refresh_position_detail_panel(app)

        self.assertEqual(zoom_tree.selection(), ("P01",))
        app._refresh_positions_zoom_detail.assert_called_once()
        app._refresh_protection_window_view.assert_called_once()
        app._set_readonly_text.assert_called_once()


class SelectedSessionDetailRefreshTest(TestCase):
    def test_set_readonly_text_preserves_scroll_when_requested(self) -> None:
        class _FakeText:
            def __init__(self, y_position: float = 0.0) -> None:
                self.content = ""
                self.y_position = y_position
                self.state = "disabled"

            def winfo_exists(self) -> bool:
                return True

            def yview(self) -> tuple[float, float]:
                return (self.y_position, min(self.y_position + 0.2, 1.0))

            def yview_moveto(self, fraction: float) -> None:
                self.y_position = fraction

            def configure(self, **kwargs: object) -> None:
                state = kwargs.get("state")
                if isinstance(state, str):
                    self.state = state

            def delete(self, _start: str, _end: str) -> None:
                self.content = ""
                self.y_position = 0.0

            def insert(self, _index: str, text: str) -> None:
                self.content = text
                self.y_position = 0.0

        widget = _FakeText(y_position=0.58)

        QuantApp._set_readonly_text(SimpleNamespace(), widget, "updated", preserve_scroll=True)

        self.assertEqual(widget.content, "updated")
        self.assertEqual(widget.y_position, 0.58)
        self.assertEqual(widget.state, "disabled")

    def test_refresh_selected_session_details_preserves_scroll_for_same_session(self) -> None:
        session = SimpleNamespace(
            session_id="S01",
            api_name="moni",
            status="running",
            runtime_status="等待信号",
            strategy_id="ema",
            strategy_name="EMA 动态委托做多",
            symbol="ETH-USDT-SWAP",
            direction_label="只做多",
            run_mode_label="交易并下单",
            started_at=datetime(2026, 4, 26, 8, 0, 0),
            stopped_at=None,
            ended_reason="",
            config=object(),
            log_file_path="",
            last_message="最新日志",
            trade_count=1,
            win_count=1,
            gross_pnl_total=Decimal("1"),
            fee_total=Decimal("0.1"),
            funding_total=Decimal("0"),
            net_pnl_total=Decimal("0.9"),
            last_close_reason="",
        )
        app = SimpleNamespace(
            _selected_session=lambda: session,
            selected_session_text=_Var(""),
            _selected_session_detail=SimpleNamespace(),
            _selected_session_detail_session_id="S01",
            _session_live_pnl_snapshot=lambda _session: (None, None),
            _build_strategy_detail_text=MagicMock(return_value="detail"),
            _set_readonly_text=MagicMock(),
            notify_enabled=_Var(True),
        )

        with patch("okx_quant.ui._serialize_strategy_config_snapshot", return_value={}), patch.object(
            QuantApp,
            "_build_duplicate_launch_conflict_warning",
            return_value="",
        ), patch.object(
            QuantApp,
            "_duplicate_launch_conflicts_for",
            return_value=[],
        ):
            QuantApp._refresh_selected_session_details(app)

        app._set_readonly_text.assert_called_once_with(
            app._selected_session_detail,
            "detail",
            preserve_scroll=True,
        )
        self.assertEqual(app._selected_session_detail_session_id, "S01")

    def test_refresh_selected_session_details_resets_scroll_for_new_session(self) -> None:
        session = SimpleNamespace(
            session_id="S02",
            api_name="moni",
            status="running",
            runtime_status="等待信号",
            strategy_id="ema",
            strategy_name="EMA 动态委托做多",
            symbol="ETH-USDT-SWAP",
            direction_label="只做多",
            run_mode_label="交易并下单",
            started_at=datetime(2026, 4, 26, 8, 0, 0),
            stopped_at=None,
            ended_reason="",
            config=object(),
            log_file_path="",
            last_message="最新日志",
            trade_count=1,
            win_count=1,
            gross_pnl_total=Decimal("1"),
            fee_total=Decimal("0.1"),
            funding_total=Decimal("0"),
            net_pnl_total=Decimal("0.9"),
            last_close_reason="",
        )
        app = SimpleNamespace(
            _selected_session=lambda: session,
            selected_session_text=_Var(""),
            _selected_session_detail=SimpleNamespace(),
            _selected_session_detail_session_id="S01",
            _session_live_pnl_snapshot=lambda _session: (None, None),
            _build_strategy_detail_text=MagicMock(return_value="detail"),
            _set_readonly_text=MagicMock(),
            notify_enabled=_Var(True),
        )

        with patch("okx_quant.ui._serialize_strategy_config_snapshot", return_value={}), patch.object(
            QuantApp,
            "_build_duplicate_launch_conflict_warning",
            return_value="",
        ), patch.object(
            QuantApp,
            "_duplicate_launch_conflicts_for",
            return_value=[],
        ):
            QuantApp._refresh_selected_session_details(app)

        app._set_readonly_text.assert_called_once_with(
            app._selected_session_detail,
            "detail",
            preserve_scroll=False,
        )
        self.assertEqual(app._selected_session_detail_session_id, "S02")


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

    def test_apply_stop_session_cleanup_result_suppresses_dialog_when_requested(self) -> None:
        session = self._make_session()
        session.stop_result_show_dialog = False
        app = SimpleNamespace(
            sessions={session.session_id: session},
            _remove_recoverable_strategy_session=MagicMock(),
            _upsert_session_row=MagicMock(),
            _refresh_selected_session_details=MagicMock(),
            _sync_strategy_history_from_session=MagicMock(),
            _log_session_message=MagicMock(),
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

        showwarning.assert_not_called()
        showinfo.assert_not_called()
        self.assertTrue(session.stop_result_show_dialog)

    def test_apply_stop_session_cleanup_error_suppresses_dialog_when_requested(self) -> None:
        session = self._make_session()
        session.stop_result_show_dialog = False
        app = SimpleNamespace(
            sessions={session.session_id: session},
            _remove_recoverable_strategy_session=MagicMock(),
            _upsert_session_row=MagicMock(),
            _refresh_selected_session_details=MagicMock(),
            _sync_strategy_history_from_session=MagicMock(),
            _log_session_message=MagicMock(),
        )

        with patch("okx_quant.ui.messagebox.showwarning") as showwarning:
            QuantApp._apply_stop_session_cleanup_error(app, session.session_id, "HTTP 500")

        showwarning.assert_not_called()
        self.assertTrue(session.stop_result_show_dialog)


class StrategyParameterDraftRestoreTest(TestCase):
    def _make_parameter_stub(self) -> SimpleNamespace:
        app = SimpleNamespace(
            _strategy_parameter_drafts={"launcher": {}},
            _strategy_parameter_scope="launcher",
            bar=_Var(""),
            signal_mode_label=_Var(""),
            ema_period=_Var(""),
            trend_ema_period=_Var(""),
            big_ema_period=_Var(""),
            atr_period=_Var(""),
            stop_atr=_Var(""),
            take_atr=_Var(""),
            entry_reference_ema_period=_Var(""),
            take_profit_mode_label=_Var(""),
            max_entries_per_trend=_Var(""),
            dynamic_two_r_break_even=_Var(False),
            dynamic_fee_offset_enabled=_Var(False),
            time_stop_break_even_enabled=_Var(False),
            time_stop_break_even_bars=_Var(""),
            startup_chase_window_seconds=_Var(""),
        )
        app._strategy_parameter_scope_drafts = lambda: QuantApp._strategy_parameter_scope_drafts(app)
        app._strategy_parameter_bindings = lambda: QuantApp._strategy_parameter_bindings(app)
        return app

    def test_restore_strategy_parameter_draft_applies_fixed_ema5_8_values(self) -> None:
        app = self._make_parameter_stub()

        QuantApp._restore_strategy_parameter_draft(app, STRATEGY_EMA5_EMA8_ID)

        self.assertEqual(app.bar.get(), "4H")
        self.assertEqual(app.ema_period.get(), 5)
        self.assertEqual(app.trend_ema_period.get(), 8)
        self.assertEqual(app.big_ema_period.get(), 233)

    def test_restore_strategy_parameter_draft_prefers_saved_cross_values(self) -> None:
        app = self._make_parameter_stub()
        app._strategy_parameter_drafts["launcher"][STRATEGY_EMA_BREAKOUT_LONG_ID] = {
            "bar": "1H",
            "ema_period": "34",
            "trend_ema_period": "89",
            "big_ema_period": "233",
            "atr_period": "14",
        }

        QuantApp._restore_strategy_parameter_draft(app, STRATEGY_EMA_BREAKOUT_LONG_ID)

        self.assertEqual(app.bar.get(), "1H")
        self.assertEqual(app.ema_period.get(), "34")
        self.assertEqual(app.trend_ema_period.get(), "89")
        self.assertEqual(app.atr_period.get(), "14")


class StrategyParameterFixedLabelTest(TestCase):
    def _make_label_stub(self) -> SimpleNamespace:
        return SimpleNamespace(
            _bar_label=_LabelStub("K线周期"),
            _signal_label=_LabelStub("信号方向"),
            _ema_label=_LabelStub("EMA小周期"),
            _trend_ema_label=_LabelStub("EMA中周期"),
            _big_ema_label=_LabelStub("EMA大周期"),
        )

    def test_apply_strategy_parameter_fixed_labels_marks_ema5_8_fixed_fields(self) -> None:
        app = self._make_label_stub()

        QuantApp._apply_strategy_parameter_fixed_labels(app, STRATEGY_EMA5_EMA8_ID)

        self.assertEqual(app._bar_label.text, "K线周期（本策略固定）")
        self.assertEqual(app._ema_label.text, "EMA小周期（本策略固定）")
        self.assertEqual(app._trend_ema_label.text, "EMA中周期（本策略固定）")
        self.assertEqual(app._big_ema_label.text, "EMA大周期（本策略固定）")
        self.assertEqual(app._signal_label.text, "信号方向")

    def test_apply_strategy_parameter_fixed_labels_marks_dynamic_direction_only(self) -> None:
        app = self._make_label_stub()

        QuantApp._apply_strategy_parameter_fixed_labels(app, STRATEGY_DYNAMIC_LONG_ID)

        self.assertEqual(app._signal_label.text, "信号方向（本策略固定）")
        self.assertEqual(app._bar_label.text, "K线周期")
        self.assertEqual(app._ema_label.text, "EMA小周期")
