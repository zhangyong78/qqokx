from dataclasses import replace
from decimal import Decimal
import smtplib
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from okx_quant.engine import StrategyEngine, _classify_live_dynamic_close_reason
from okx_quant.models import Credentials, EmailNotificationConfig, Instrument, StrategyConfig
from okx_quant.notifications import EmailNotifier
from okx_quant.okx_client import OkxPositionHistoryItem
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID, STRATEGY_EMA5_EMA8_ID


def _make_strategy_config(*, run_mode: str = "trade") -> StrategyConfig:
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
        trade_inst_id="ETH-USDT-SWAP",
        run_mode=run_mode,
    )


class EmailNotifierTest(TestCase):
    def _make_notifier(self) -> EmailNotifier:
        notifier = EmailNotifier(
            EmailNotificationConfig(
                enabled=True,
                smtp_host="smtp.example.com",
                recipient_emails=("receiver@example.com",),
            )
        )
        notifier.notify_async = MagicMock()
        return notifier

    def test_send_trade_fill_includes_api_name_in_subject_and_body(self) -> None:
        notifier = self._make_notifier()

        notifier.send_trade_fill(
            strategy_name="EMA 动态委托",
            config=_make_strategy_config(),
            title="开仓成交",
            symbol="ETH-USDT-SWAP",
            side="buy",
            size="1",
            price="2500",
            reason="测试成交",
            api_name="moni",
            session_id="S08",
            trader_id="T001",
            direction_label="只做多",
            run_mode_label="交易并下单",
        )

        subject, body = notifier.notify_async.call_args.args
        self.assertIn("API=moni", subject)
        self.assertIn("会话=S08", subject)
        self.assertIn("交易员=T001", subject)
        self.assertIn("会话：S08", body)
        self.assertIn("交易员：T001", body)
        self.assertIn("API配置：moni", body)
        self.assertIn("策略ID：ema_dynamic_order", body)
        self.assertIn("规则方向：只做多", body)
        self.assertIn("运行模式：交易并下单", body)
        self.assertIn("K线周期：1H", body)
        self.assertIn("成交方向：买入", body)

    def test_send_signal_includes_session_and_rule_context(self) -> None:
        notifier = self._make_notifier()

        notifier.send_signal(
            strategy_name="EMA 动态委托",
            config=_make_strategy_config(),
            signal="short",
            trigger_symbol="BTC-USDT-SWAP",
            entry_reference="95000",
            reason="趋势确认",
            api_name="moni",
            session_id="S02",
            trader_id="T009",
            direction_label="只做空",
            run_mode_label="交易并下单",
        )

        subject, body = notifier.notify_async.call_args.args
        self.assertIn("API=moni", subject)
        self.assertIn("会话=S02", subject)
        self.assertIn("交易员=T009", subject)
        self.assertIn("做空", subject)
        self.assertIn("当前信号：做空", body)
        self.assertIn("规则方向：只做空", body)
        self.assertIn("触发标的：BTC-USDT-SWAP", body)
        self.assertIn("参考价：95000", body)

    def test_send_signal_keeps_tick_sized_entry_reference_text(self) -> None:
        notifier = self._make_notifier()

        notifier.send_signal(
            strategy_name="EMA 动态委托",
            config=_make_strategy_config(),
            signal="short",
            trigger_symbol="SOL-USDT-SWAP",
            entry_reference="84.5766",
            reason="趋势确认",
            api_name="moni",
            session_id="S02",
            trader_id="T009",
            direction_label="只做空",
            run_mode_label="交易并下单",
        )

        _, body = notifier.notify_async.call_args.args
        self.assertIn("参考价：84.5766", body)

    def test_send_error_includes_api_name_in_subject_and_body(self) -> None:
        notifier = self._make_notifier()

        notifier.send_error(
            strategy_name="EMA 动态委托",
            config=_make_strategy_config(),
            message="下单失败",
            api_name="real-1",
            session_id="S03",
            trader_id="T002",
            direction_label="只做多",
            run_mode_label="交易并下单",
        )

        subject, body = notifier.notify_async.call_args.args
        self.assertIn("API=real-1", subject)
        self.assertIn("会话=S03", subject)
        self.assertIn("交易员=T002", subject)
        self.assertIn("API配置：real-1", body)
        self.assertIn("会话：S03", body)
        self.assertIn("交易员：T002", body)
        self.assertIn("规则方向：只做多", body)

    def test_send_trade_fill_omits_api_name_when_blank(self) -> None:
        notifier = self._make_notifier()

        notifier.send_trade_fill(
            strategy_name="EMA 动态委托",
            config=_make_strategy_config(),
            title="开仓成交",
            symbol="ETH-USDT-SWAP",
            side="buy",
            size="1",
            price="2500",
            reason="测试成交",
            api_name="",
        )

        subject, body = notifier.notify_async.call_args.args
        self.assertNotIn("API=", subject)
        self.assertNotIn("API配置：", body)

    def test_send_trade_fill_includes_trade_pnl_when_present(self) -> None:
        notifier = self._make_notifier()

        notifier.send_trade_fill(
            strategy_name="EMA 动态委托",
            config=_make_strategy_config(),
            title="止盈平仓成交",
            symbol="ETH-USDT-SWAP",
            side="sell",
            size="1",
            price="2550",
            reason="本地止盈触发后平仓成交",
            trade_pnl="+50",
            api_name="moni",
        )

        _, body = notifier.notify_async.call_args.args
        self.assertIn("本笔盈亏：+50", body)

    def test_send_trade_close_includes_trigger_reason_and_prices(self) -> None:
        notifier = self._make_notifier()

        notifier.send_trade_close(
            strategy_name="EMA 动态委托",
            config=_make_strategy_config(),
            symbol="ETH-USDT-SWAP",
            side="sell",
            size="1",
            entry_price="2500",
            exit_price="2550",
            trigger_reason="3R",
            detail="动态止损保护价触发后平仓成交",
            trade_pnl="+50",
            api_name="moni",
            session_id="S18",
        )

        subject, body = notifier.notify_async.call_args.args
        self.assertIn("平仓通知", subject)
        self.assertIn("3R", subject)
        self.assertIn("触发原因：3R", body)
        self.assertIn("开仓价格：2500", body)
        self.assertIn("平仓价格：2550", body)
        self.assertIn("本笔盈亏：+50", body)

    def test_login_uses_sender_email_when_username_blank(self) -> None:
        notifier = EmailNotifier(
            EmailNotificationConfig(
                enabled=True,
                smtp_host="smtp.example.com",
                smtp_password="app-password",
                sender_email="sender@example.com",
                recipient_emails=("receiver@example.com",),
            )
        )
        smtp = MagicMock()

        notifier._login_and_send(
            smtp,
            "sender@example.com",
            ["receiver@example.com"],
            MagicMock(),
        )

        smtp.login.assert_called_once_with("sender@example.com", "app-password")
        smtp.send_message.assert_called_once()

    def test_send_logs_auth_guidance_for_smtp_535(self) -> None:
        logger = MagicMock()
        notifier = EmailNotifier(
            EmailNotificationConfig(
                enabled=True,
                smtp_host="smtp.example.com",
                smtp_port=465,
                smtp_username="alerts@example.com",
                smtp_password="bad-password",
                sender_email="sender@example.com",
                recipient_emails=("receiver@example.com",),
                use_ssl=True,
            ),
            logger=logger,
        )
        auth_error = smtplib.SMTPAuthenticationError(535, b"Error: authentication failed")
        smtp_context = MagicMock()
        smtp_context.__enter__.return_value = MagicMock()
        smtp_context.__exit__.return_value = False
        notifier._login_and_send = MagicMock(side_effect=auth_error)

        with patch("okx_quant.notifications.smtplib.SMTP_SSL", return_value=smtp_context):
            notifier._send("[QQOKX] test", "body")

        logged_message = logger.call_args.args[0]
        self.assertIn("SMTP认证失败(535)", logged_message)
        self.assertIn("SMTP 密码/授权码", logged_message)
        self.assertIn("SSL=开", logged_message)
        self.assertIn("用户名=al***@example.com", logged_message)


class StrategyEngineNotificationTest(TestCase):
    def _make_credentials(self) -> Credentials:
        return Credentials(api_key="key", secret_key="secret", passphrase="pass")

    def _make_trade_instrument(self) -> Instrument:
        return Instrument(
            inst_id="ETH-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
        )

    def test_trade_fill_notification_passes_runtime_context_to_notifier(self) -> None:
        notifier = MagicMock()
        engine = StrategyEngine(
            MagicMock(),
            lambda message: None,
            notifier=notifier,
            strategy_name="EMA 动态委托",
            session_id="S06",
            direction_label="只做空",
            run_mode_label="交易并下单",
            trader_id="T005",
        )
        engine._api_name = "moni"

        engine._notify_trade_fill(
            _make_strategy_config(),
            title="开仓成交",
            symbol="ETH-USDT-SWAP",
            side="buy",
            size=Decimal("1"),
            price=Decimal("2500"),
            reason="测试成交",
        )

        self.assertEqual(notifier.send_trade_fill.call_args.kwargs["api_name"], "moni")
        self.assertEqual(notifier.send_trade_fill.call_args.kwargs["session_id"], "S06")
        self.assertEqual(notifier.send_trade_fill.call_args.kwargs["trader_id"], "T005")
        self.assertEqual(notifier.send_trade_fill.call_args.kwargs["direction_label"], "只做空")
        self.assertEqual(notifier.send_trade_fill.call_args.kwargs["run_mode_label"], "交易并下单")

    def test_trade_fill_notification_keeps_constructor_api_name_when_runtime_credentials_blank(self) -> None:
        notifier = MagicMock()
        engine = StrategyEngine(
            MagicMock(),
            lambda message: None,
            notifier=notifier,
            strategy_name="EMA 动态委托",
            session_id="S06",
            api_name="QQzhangyong",
        )

        engine._notify_trade_fill(
            _make_strategy_config(),
            title="开仓成交",
            symbol="ETH-USDT-SWAP",
            side="buy",
            size=Decimal("1"),
            price=Decimal("2500"),
            reason="测试成交",
        )

        self.assertEqual(notifier.send_trade_fill.call_args.kwargs["api_name"], "QQzhangyong")

    def test_signal_notification_passes_runtime_context_to_notifier(self) -> None:
        notifier = MagicMock()
        engine = StrategyEngine(
            MagicMock(),
            lambda message: None,
            notifier=notifier,
            strategy_name="EMA 动态委托",
            session_id="S09",
            direction_label="只做多",
            run_mode_label="交易并下单",
            trader_id="T003",
        )
        engine._api_name = "moni"

        engine._notify_signal(
            _make_strategy_config(),
            signal="long",
            trigger_symbol="ETH-USDT-SWAP",
            entry_reference=Decimal("2500"),
            reason="突破确认",
        )

        self.assertEqual(notifier.send_signal.call_args.kwargs["api_name"], "moni")
        self.assertEqual(notifier.send_signal.call_args.kwargs["session_id"], "S09")
        self.assertEqual(notifier.send_signal.call_args.kwargs["trader_id"], "T003")
        self.assertEqual(notifier.send_signal.call_args.kwargs["direction_label"], "只做多")
        self.assertEqual(notifier.send_signal.call_args.kwargs["run_mode_label"], "交易并下单")

    def test_signal_only_notification_appends_take_profit_mode_line(self) -> None:
        notifier = MagicMock()
        engine = StrategyEngine(
            MagicMock(),
            lambda message: None,
            notifier=notifier,
            strategy_name="EMA 突破做多",
            session_id="S11",
        )
        cfg = replace(
            _make_strategy_config(run_mode="signal_only"),
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            take_profit_mode="dynamic",
        )
        engine._notify_signal(
            cfg,
            signal="long",
            trigger_symbol="ETH-USDT-SWAP",
            entry_reference=Decimal("2500"),
            reason="EMA 动态委托参考价已更新",
        )
        reason = notifier.send_signal.call_args.kwargs["reason"]
        self.assertIn("EMA 动态委托参考价已更新", reason)
        self.assertIn("止盈方式：动态止盈", reason)
        self.assertIn("首档触发R=", reason)
        self.assertIn("nR保本=", reason)

    def test_signal_only_ema5_email_includes_distinct_take_profit_note(self) -> None:
        notifier = MagicMock()
        engine = StrategyEngine(
            MagicMock(),
            lambda message: None,
            notifier=notifier,
            strategy_name="EMA5/8",
            session_id="S12",
        )
        cfg = replace(_make_strategy_config(run_mode="signal_only"), strategy_id=STRATEGY_EMA5_EMA8_ID)
        engine._notify_signal(
            cfg,
            signal="long",
            trigger_symbol="ETH-USDT-SWAP",
            entry_reference=Decimal("2500"),
            reason="金叉",
        )
        reason = notifier.send_signal.call_args.kwargs["reason"]
        self.assertIn("金叉", reason)
        self.assertIn("慢线 EMA", reason)

    def test_trade_mode_signal_reason_not_appended_with_take_profit_block(self) -> None:
        notifier = MagicMock()
        engine = StrategyEngine(
            MagicMock(),
            lambda message: None,
            notifier=notifier,
            strategy_name="EMA 动态委托",
            session_id="S13",
        )
        engine._notify_signal(
            _make_strategy_config(run_mode="trade"),
            signal="long",
            trigger_symbol="ETH-USDT-SWAP",
            entry_reference=Decimal("2500"),
            reason="突破确认",
        )
        self.assertEqual(notifier.send_signal.call_args.kwargs["reason"], "突破确认")

    def test_signal_notification_formats_entry_reference_by_tick_size(self) -> None:
        notifier = MagicMock()
        engine = StrategyEngine(
            MagicMock(),
            lambda message: None,
            notifier=notifier,
            strategy_name="EMA 动态委托",
            session_id="S09",
        )

        engine._notify_signal(
            _make_strategy_config(),
            signal="long",
            trigger_symbol="ETH-USDT-SWAP",
            entry_reference=Decimal("2500.127"),
            tick_size=Decimal("0.1"),
            reason="突破确认",
        )

        self.assertEqual(notifier.send_signal.call_args.kwargs["entry_reference"], "2500.1")

    def test_trade_fill_notification_formats_price_by_tick_size(self) -> None:
        notifier = MagicMock()
        engine = StrategyEngine(
            MagicMock(),
            lambda message: None,
            notifier=notifier,
            strategy_name="EMA 动态委托",
            session_id="S09",
        )

        engine._notify_trade_fill(
            _make_strategy_config(),
            title="开仓成交",
            symbol="ETH-USDT-SWAP",
            side="buy",
            size=Decimal("1"),
            price=Decimal("2366.9692729338256184"),
            tick_size=Decimal("0.01"),
            reason="测试成交",
        )

        self.assertEqual(notifier.send_trade_fill.call_args.kwargs["price"], "2366.97")

    def test_trade_error_notification_passes_api_name_to_notifier(self) -> None:
        notifier = MagicMock()
        engine = StrategyEngine(
            MagicMock(),
            lambda message: None,
            notifier=notifier,
            strategy_name="EMA 动态委托",
            session_id="S10",
            direction_label="只做多",
            run_mode_label="交易并下单",
            trader_id="T001",
        )
        engine._api_name = "real-1"

        engine._notify_error(_make_strategy_config(), "下单失败")

        self.assertEqual(notifier.send_error.call_args.kwargs["api_name"], "real-1")
        self.assertEqual(notifier.send_error.call_args.kwargs["session_id"], "S10")
        self.assertEqual(notifier.send_error.call_args.kwargs["trader_id"], "T001")

    def test_trade_fill_notification_passes_trade_pnl_to_notifier(self) -> None:
        notifier = MagicMock()
        engine = StrategyEngine(
            MagicMock(),
            lambda message: None,
            notifier=notifier,
            strategy_name="EMA 动态委托",
            session_id="S12",
        )

        engine._notify_trade_fill(
            _make_strategy_config(),
            title="止损平仓成交",
            symbol="ETH-USDT-SWAP",
            side="sell",
            size=Decimal("1"),
            price=Decimal("2450"),
            reason="本地止损触发后平仓成交",
            trade_pnl="-50",
        )

        self.assertEqual(notifier.send_trade_fill.call_args.kwargs["trade_pnl"], "-50")

    def test_trade_close_notification_passes_trigger_reason_to_notifier(self) -> None:
        notifier = MagicMock()
        engine = StrategyEngine(
            MagicMock(),
            lambda message: None,
            notifier=notifier,
            strategy_name="EMA 动态委托",
            session_id="S19",
        )

        engine._notify_trade_close(
            _make_strategy_config(),
            symbol="ETH-USDT-SWAP",
            side="sell",
            size=Decimal("1"),
            entry_price=Decimal("2500"),
            exit_price=Decimal("2550"),
            trigger_reason="保本",
            detail="本地保本触发后平仓成交",
            trade_pnl="50",
        )

        self.assertEqual(notifier.send_trade_close.call_args.kwargs["trigger_reason"], "保本")
        self.assertEqual(notifier.send_trade_close.call_args.kwargs["trade_pnl"], "50")

    def test_exchange_dynamic_stop_close_notification_uses_position_history_pnl(self) -> None:
        client = MagicMock()
        client.get_positions_history.return_value = [
            OkxPositionHistoryItem(
                update_time=None,
                inst_id="ETH-USDT-SWAP",
                inst_type="SWAP",
                mgn_mode="cross",
                pos_side="long",
                direction="net",
                open_avg_price=Decimal("2500"),
                close_avg_price=Decimal("2550"),
                close_size=Decimal("1"),
                pnl=Decimal("47.5"),
                realized_pnl=Decimal("48.5"),
                settle_pnl=Decimal("0"),
                raw={},
                fee=Decimal("-1"),
                fee_currency="USDT",
            )
        ]
        notifier = MagicMock()
        engine = StrategyEngine(client, lambda message: None, notifier=notifier, strategy_name="EMA 动态委托")

        engine._notify_exchange_dynamic_stop_close(
            self._make_credentials(),
            _make_strategy_config(),
            trade_instrument=self._make_trade_instrument(),
            position=SimpleNamespace(
                inst_id="ETH-USDT-SWAP",
                side="buy",
                close_side="sell",
                pos_side="long",
                size=Decimal("1"),
                entry_price=Decimal("2500"),
            ),
            initial_stop_loss=Decimal("2400"),
            current_stop_loss=Decimal("2550"),
            risk_per_unit=Decimal("100"),
            next_trigger_r=3,
            detail="OKX 动态止损平仓",
        )

        self.assertEqual(notifier.send_trade_close.call_args.kwargs["trade_pnl"], "+48.5")
        client.get_positions_history.assert_called_once()

    def test_exchange_dynamic_stop_close_notification_skips_unmatched_history_pnl(self) -> None:
        client = MagicMock()
        client.get_positions_history.return_value = [
            OkxPositionHistoryItem(
                update_time=None,
                inst_id="BTC-USDT-SWAP",
                inst_type="SWAP",
                mgn_mode="cross",
                pos_side="short",
                direction="net",
                open_avg_price=Decimal("70000"),
                close_avg_price=Decimal("69000"),
                close_size=Decimal("1"),
                pnl=Decimal("100"),
                realized_pnl=Decimal("100"),
                settle_pnl=Decimal("0"),
                raw={},
                fee=Decimal("-1"),
                fee_currency="USDT",
            )
        ]
        notifier = MagicMock()
        engine = StrategyEngine(client, lambda message: None, notifier=notifier, strategy_name="EMA 动态委托")

        engine._notify_exchange_dynamic_stop_close(
            self._make_credentials(),
            _make_strategy_config(),
            trade_instrument=self._make_trade_instrument(),
            position=SimpleNamespace(
                inst_id="ETH-USDT-SWAP",
                side="buy",
                close_side="sell",
                pos_side="long",
                size=Decimal("1"),
                entry_price=Decimal("2500"),
            ),
            initial_stop_loss=Decimal("2400"),
            current_stop_loss=Decimal("2550"),
            risk_per_unit=Decimal("100"),
            next_trigger_r=3,
            detail="OKX 动态止损平仓",
        )

        self.assertEqual(notifier.send_trade_close.call_args.kwargs["trade_pnl"], "")

    def test_classify_live_dynamic_close_reason_returns_locked_r(self) -> None:
        reason = _classify_live_dynamic_close_reason(
            direction="long",
            entry_price=Decimal("100"),
            initial_stop_loss=Decimal("90"),
            current_stop_loss=Decimal("120"),
            risk_per_unit=Decimal("10"),
            next_trigger_r=4,
            tick_size=Decimal("0.1"),
            two_r_break_even=True,
            dynamic_fee_offset_enabled=False,
            time_stop_break_even_enabled=False,
        )

        self.assertEqual(reason, "2R")

    def test_trade_fill_pnl_text_for_close_uses_entry_direction(self) -> None:
        long_position = SimpleNamespace(
            side="buy",
            size=Decimal("2"),
            entry_price=Decimal("2500"),
            price_delta_multiplier=Decimal("1"),
        )
        short_position = SimpleNamespace(
            side="sell",
            size=Decimal("2"),
            entry_price=Decimal("2500"),
            price_delta_multiplier=Decimal("1"),
        )

        self.assertEqual(
            StrategyEngine._trade_fill_pnl_text_for_close(
                long_position,
                fill_size=Decimal("1"),
                fill_price=Decimal("2550"),
            ),
            "50",
        )
        self.assertEqual(
            StrategyEngine._trade_fill_pnl_text_for_close(
                short_position,
                fill_size=Decimal("1"),
                fill_price=Decimal("2450"),
            ),
            "50",
        )

    def test_signal_only_error_notification_keeps_api_name(self) -> None:
        notifier = MagicMock()
        engine = StrategyEngine(
            MagicMock(),
            lambda message: None,
            notifier=notifier,
            strategy_name="EMA 动态委托",
            session_id="S11",
            direction_label="只做多",
            run_mode_label="仅信号",
        )
        engine._api_name = "real-1"

        engine._notify_error(_make_strategy_config(run_mode="signal_only"), "读取失败")

        self.assertEqual(notifier.send_error.call_args.kwargs["api_name"], "real-1")
        self.assertEqual(notifier.send_error.call_args.kwargs["session_id"], "S11")
