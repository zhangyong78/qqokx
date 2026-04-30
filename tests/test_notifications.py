from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock

from okx_quant.engine import StrategyEngine
from okx_quant.models import EmailNotificationConfig, StrategyConfig
from okx_quant.notifications import EmailNotifier


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


class StrategyEngineNotificationTest(TestCase):
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
