from decimal import Decimal
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
        )

        subject, body = notifier.notify_async.call_args.args
        self.assertIn("API=moni", subject)
        self.assertIn("API配置：moni", body)

    def test_send_error_includes_api_name_in_subject_and_body(self) -> None:
        notifier = self._make_notifier()

        notifier.send_error(
            strategy_name="EMA 动态委托",
            config=_make_strategy_config(),
            message="下单失败",
            api_name="real-1",
        )

        subject, body = notifier.notify_async.call_args.args
        self.assertIn("API=real-1", subject)
        self.assertIn("API配置：real-1", body)

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


class StrategyEngineNotificationTest(TestCase):
    def test_trade_fill_notification_passes_api_name_to_notifier(self) -> None:
        notifier = MagicMock()
        engine = StrategyEngine(MagicMock(), lambda message: None, notifier=notifier, strategy_name="EMA 动态委托")
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

    def test_trade_error_notification_passes_api_name_to_notifier(self) -> None:
        notifier = MagicMock()
        engine = StrategyEngine(MagicMock(), lambda message: None, notifier=notifier, strategy_name="EMA 动态委托")
        engine._api_name = "real-1"

        engine._notify_error(_make_strategy_config(), "下单失败")

        self.assertEqual(notifier.send_error.call_args.kwargs["api_name"], "real-1")

    def test_signal_only_error_notification_omits_api_name(self) -> None:
        notifier = MagicMock()
        engine = StrategyEngine(MagicMock(), lambda message: None, notifier=notifier, strategy_name="EMA 动态委托")
        engine._api_name = "real-1"

        engine._notify_error(_make_strategy_config(run_mode="signal_only"), "读取失败")

        self.assertEqual(notifier.send_error.call_args.kwargs["api_name"], "")
