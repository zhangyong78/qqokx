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

    @staticmethod
    def _clean_text(value: str | None) -> str:
        return (value or "").strip()

    def _subject_with_context(
        self,
        subject: str,
        *,
        api_name: str | None = None,
        session_id: str | None = None,
        trader_id: str | None = None,
    ) -> str:
        current = self._subject_with_api(subject, api_name)
        resolved_session_id = self._clean_text(session_id)
        resolved_trader_id = self._clean_text(trader_id)
        if resolved_session_id:
            current = f"{current} | 会话={resolved_session_id}"
        if resolved_trader_id:
            current = f"{current} | 交易员={resolved_trader_id}"
        return current

    @staticmethod
    def _resolve_run_mode_label(config: StrategyConfig | None, run_mode_label: str | None) -> str:
        resolved = (run_mode_label or "").strip()
        if resolved:
            return resolved
        if config is None:
            return ""
        return {
            "trade": "交易并下单",
            "signal_only": "仅信号",
        }.get(config.run_mode, config.run_mode)

    @staticmethod
    def _resolve_strategy_direction(config: StrategyConfig | None, direction_label: str | None) -> str:
        resolved = (direction_label or "").strip()
        if resolved:
            return resolved
        if config is None:
            return ""
        return {
            "long_only": "只做多",
            "short_only": "只做空",
            "both": "双向",
        }.get(config.signal_mode, config.signal_mode)

    def _build_base_lines(
        self,
        *,
        strategy_name: str,
        config: StrategyConfig | None,
        api_name: str | None = None,
        session_id: str | None = None,
        trader_id: str | None = None,
        direction_label: str | None = None,
        run_mode_label: str | None = None,
    ) -> list[str]:
        lines: list[str] = []
        resolved_session_id = self._clean_text(session_id)
        resolved_trader_id = self._clean_text(trader_id)
        resolved_api_name = self._clean_api_name(api_name)
        resolved_run_mode_label = self._resolve_run_mode_label(config, run_mode_label)
        resolved_direction_label = self._resolve_strategy_direction(config, direction_label)
        if resolved_session_id:
            lines.append(f"会话：{resolved_session_id}")
        if resolved_trader_id:
            lines.append(f"交易员：{resolved_trader_id}")
        if resolved_api_name:
            lines.append(f"API配置：{resolved_api_name}")
        lines.append(f"策略：{strategy_name}")
        if config is None:
            return lines
        lines.extend(
            [
                f"策略ID：{config.strategy_id}",
                f"运行模式：{resolved_run_mode_label or config.run_mode}",
                f"规则方向：{resolved_direction_label or config.signal_mode}",
                f"信号标的：{config.inst_id}",
                f"下单标的：{config.trade_inst_id or config.inst_id}",
                f"K线周期：{config.bar}",
            ]
        )
        return lines

    @staticmethod
    def _signal_label(signal: str) -> str:
        normalized = (signal or "").strip().lower()
        return {
            "long": "做多",
            "short": "做空",
        }.get(normalized, signal)

    @staticmethod
    def _trade_side_label(side: str) -> str:
        normalized = (side or "").strip().lower()
        return {
            "buy": "买入",
            "sell": "卖出",
        }.get(normalized, side)

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
        api_name: str = "",
        session_id: str = "",
        trader_id: str = "",
        direction_label: str = "",
        run_mode_label: str = "",
    ) -> None:
        if not self._config.notify_signals:
            return
        subject = self._subject_with_context(
            f"[QQOKX] 信号提醒 | {strategy_name} | {trigger_symbol} | {self._signal_label(signal)}",
            api_name=api_name,
            session_id=session_id,
            trader_id=trader_id,
        )
        body = "\n".join(
            [
                *self._build_base_lines(
                    strategy_name=strategy_name,
                    config=config,
                    api_name=api_name,
                    session_id=session_id,
                    trader_id=trader_id,
                    direction_label=direction_label,
                    run_mode_label=run_mode_label,
                ),
                f"当前信号：{self._signal_label(signal)}",
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
        trade_pnl: str = "",
        api_name: str = "",
        session_id: str = "",
        trader_id: str = "",
        direction_label: str = "",
        run_mode_label: str = "",
    ) -> None:
        if not self._config.notify_trade_fills:
            return
        subject = self._subject_with_context(
            f"[QQOKX] 成交通知 | {title} | {symbol}",
            api_name=api_name,
            session_id=session_id,
            trader_id=trader_id,
        )
        body = "\n".join(
            [
                *self._build_base_lines(
                    strategy_name=strategy_name,
                    config=config,
                    api_name=api_name,
                    session_id=session_id,
                    trader_id=trader_id,
                    direction_label=direction_label,
                    run_mode_label=run_mode_label,
                ),
                f"成交标的：{symbol}",
                f"成交方向：{self._trade_side_label(side)}",
                f"成交数量：{size}",
                f"成交价格：{price}",
                *([f"本笔盈亏：{trade_pnl}"] if trade_pnl.strip() else []),
                f"说明：{reason}",
            ]
        )
        self.notify_async(subject, body)

    def send_error(
        self,
        *,
        strategy_name: str,
        config: StrategyConfig | None,
        message: str,
        api_name: str = "",
        session_id: str = "",
        trader_id: str = "",
        direction_label: str = "",
        run_mode_label: str = "",
    ) -> None:
        if not self._config.notify_errors:
            return
        subject = self._subject_with_context(
            f"[QQOKX] 异常提醒 | {strategy_name}",
            api_name=api_name,
            session_id=session_id,
            trader_id=trader_id,
        )
        lines = self._build_base_lines(
            strategy_name=strategy_name,
            config=config,
            api_name=api_name,
            session_id=session_id,
            trader_id=trader_id,
            direction_label=direction_label,
            run_mode_label=run_mode_label,
        )
        lines.append(f"异常：{message}")
        self.notify_async(subject, "\n".join(lines))

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
