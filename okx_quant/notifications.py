from __future__ import annotations

import smtplib
import threading
from email.message import EmailMessage
from typing import Callable

from okx_quant.log_utils import append_log_line, ensure_log_timestamp
from okx_quant.models import EmailNotificationConfig, StrategyConfig


Logger = Callable[[str], None]


class EmailNotifier:
    def __init__(self, config: EmailNotificationConfig, logger: Logger | None = None) -> None:
        self._config = config
        self._logger = logger

    @property
    def enabled(self) -> bool:
        return self._config.enabled and bool(self._config.smtp_host.strip()) and bool(self._recipients())

    @property
    def signal_notifications_enabled(self) -> bool:
        return self.enabled and self._config.notify_signals

    @staticmethod
    def _clean_api_name(api_name: str | None) -> str:
        return (api_name or "").strip()

    def _subject_with_api(self, subject: str, api_name: str | None) -> str:
        resolved_api_name = self._clean_api_name(api_name)
        if not resolved_api_name:
            return subject
        return f"{subject} | API={resolved_api_name}"

    def _lines_with_api(self, lines: list[str], api_name: str | None) -> list[str]:
        resolved_api_name = self._clean_api_name(api_name)
        if not resolved_api_name:
            return lines
        if not lines:
            return [f"API配置：{resolved_api_name}"]
        return [lines[0], f"API配置：{resolved_api_name}", *lines[1:]]

    def send_signal(
        self,
        *,
        strategy_name: str,
        config: StrategyConfig,
        signal: str,
        trigger_symbol: str,
        entry_reference: str,
        reason: str,
    ) -> None:
        if not self._config.notify_signals:
            return
        subject = f"[QQOKX] 信号提醒 | {strategy_name} | {trigger_symbol} | {signal.upper()}"
        body = "\n".join(
            [
                f"策略：{strategy_name}",
                f"运行模式：{config.run_mode}",
                f"信号标的：{config.inst_id}",
                f"下单标的：{config.trade_inst_id or config.inst_id}",
                f"K线周期：{config.bar}",
                f"信号方向：{signal}",
                f"触发标的：{trigger_symbol}",
                f"参考价：{entry_reference}",
                f"原因：{reason}",
            ]
        )
        self.notify_async(subject, body)

    def send_trade_fill(
        self,
        *,
        strategy_name: str,
        config: StrategyConfig,
        title: str,
        symbol: str,
        side: str,
        size: str,
        price: str,
        reason: str,
        api_name: str = "",
    ) -> None:
        if not self._config.notify_trade_fills:
            return
        subject = self._subject_with_api(f"[QQOKX] 成交通知 | {title} | {symbol}", api_name)
        body = "\n".join(
            self._lines_with_api(
                [
                    f"策略：{strategy_name}",
                    f"运行模式：{config.run_mode}",
                    f"信号标的：{config.inst_id}",
                    f"下单标的：{config.trade_inst_id or config.inst_id}",
                    f"成交标的：{symbol}",
                    f"方向：{side}",
                    f"数量：{size}",
                    f"价格：{price}",
                    f"说明：{reason}",
                ],
                api_name,
            )
        )
        self.notify_async(subject, body)

    def send_error(
        self,
        *,
        strategy_name: str,
        config: StrategyConfig | None,
        message: str,
        api_name: str = "",
    ) -> None:
        if not self._config.notify_errors:
            return
        subject = self._subject_with_api(f"[QQOKX] 异常提醒 | {strategy_name}", api_name)
        lines = [f"策略：{strategy_name}", f"异常：{message}"]
        if config is not None:
            lines.extend(
                [
                    f"运行模式：{config.run_mode}",
                    f"信号标的：{config.inst_id}",
                    f"下单标的：{config.trade_inst_id or config.inst_id}",
                    f"K线周期：{config.bar}",
                ]
            )
        self.notify_async(subject, "\n".join(self._lines_with_api(lines, api_name)))

    def notify_async(self, subject: str, body: str) -> None:
        if not self.enabled:
            return
        threading.Thread(
            target=self._send,
            args=(subject, body),
            daemon=True,
            name="qqokx-email-notifier",
        ).start()

    def _send(self, subject: str, body: str) -> None:
        sender = (self._config.sender_email or self._config.smtp_username).strip()
        recipients = self._recipients()
        if not sender or not recipients:
            return

        message = EmailMessage()
        message["From"] = sender
        message["To"] = ", ".join(recipients)
        message["Subject"] = subject
        message.set_content(body)

        try:
            if self._config.use_ssl:
                with smtplib.SMTP_SSL(self._config.smtp_host, self._config.smtp_port, timeout=20) as smtp:
                    self._login_and_send(smtp, sender, recipients, message)
            else:
                with smtplib.SMTP(self._config.smtp_host, self._config.smtp_port, timeout=20) as smtp:
                    smtp.starttls()
                    self._login_and_send(smtp, sender, recipients, message)
            self._log(f"邮件已发送 | {subject}")
        except Exception as exc:
            self._log(f"邮件发送失败 | {subject} | {exc}")

    def _login_and_send(
        self,
        smtp: smtplib.SMTP,
        sender: str,
        recipients: list[str],
        message: EmailMessage,
    ) -> None:
        if self._config.smtp_username.strip():
            smtp.login(self._config.smtp_username.strip(), self._config.smtp_password)
        smtp.send_message(message, from_addr=sender, to_addrs=recipients)

    def _recipients(self) -> list[str]:
        return [item.strip() for item in self._config.recipient_emails if item.strip()]

    def _log(self, message: str) -> None:
        if self._logger is not None:
            self._logger(ensure_log_timestamp(message))
            return
        append_log_line(message)
