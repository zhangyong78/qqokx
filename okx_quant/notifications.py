from __future__ import annotations

import smtplib
import threading
from decimal import Decimal, InvalidOperation
from email.message import EmailMessage
from typing import Callable, Literal

from okx_quant.log_utils import append_log_line, ensure_log_timestamp
from okx_quant.models import EmailNotificationConfig, StrategyConfig


Logger = Callable[[str], None]
DeliveryPolicy = Callable[[Literal["signal", "trade_fill", "error"]], bool]


class EmailNotifier:
    def __init__(
        self,
        config: EmailNotificationConfig,
        logger: Logger | None = None,
        delivery_policy: DeliveryPolicy | None = None,
    ) -> None:
        self._config = config
        self._logger = logger
        self._delivery_policy = delivery_policy

    @property
    def enabled(self) -> bool:
        return self._config.enabled and bool(self._config.smtp_host.strip()) and bool(self._recipients())

    @property
    def signal_notifications_enabled(self) -> bool:
        return self.enabled and self._kind_enabled("signal")

    def _delivery_allowed(self, kind: Literal["signal", "trade_fill", "error"]) -> bool:
        if self._delivery_policy is None:
            return True
        try:
            return bool(self._delivery_policy(kind))
        except Exception:
            return False

    def _base_delivery_disabled_reason(self) -> str:
        if not self._config.enabled:
            return "全局邮件通知未启用"
        if not self._config.smtp_host.strip():
            return "SMTP 主机为空"
        if not self._recipients():
            return "收件邮箱为空"
        return ""

    def _kind_disabled_reason(self, kind: Literal["signal", "trade_fill", "error"]) -> str:
        base_reason = self._base_delivery_disabled_reason()
        if base_reason:
            return base_reason
        if not self._delivery_allowed(kind):
            return "全局/会话邮件开关或该类型邮件开关已关闭"
        if kind == "signal" and not self._config.notify_signals:
            return "信号邮件开关已关闭"
        if kind == "trade_fill" and not self._config.notify_trade_fills:
            return "成交邮件开关已关闭"
        if kind == "error" and not self._config.notify_errors:
            return "异常邮件开关已关闭"
        return ""

    def _kind_enabled(self, kind: Literal["signal", "trade_fill", "error"]) -> bool:
        return not self._kind_disabled_reason(kind)

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

    @staticmethod
    def _position_direction_from_trade_side(side: str, *, closing: bool) -> str:
        normalized = (side or "").strip().lower()
        if closing:
            return {"sell": "做多", "buy": "做空"}.get(normalized, "")
        return {"buy": "做多", "sell": "做空"}.get(normalized, "")

    @staticmethod
    def _trade_event_label(text: str, *, closing: bool) -> str:
        if not closing:
            return "开仓"
        normalized = (text or "").strip()
        if "止损" in normalized:
            return "平仓-止损"
        if "止盈" in normalized:
            return "平仓-止盈"
        return "平仓"

    @staticmethod
    def _trade_title_is_close(title: str) -> bool:
        normalized = (title or "").strip()
        return any(marker in normalized for marker in ("平仓", "止损", "止盈", "离场"))

    @staticmethod
    def _format_signal_entry_reference(entry_reference: str, trigger_symbol: str) -> str:
        raw = str(entry_reference or "").strip()
        if not raw:
            return "-"
        try:
            value = Decimal(raw)
        except (InvalidOperation, ValueError):
            return raw
        if "e" in raw.lower():
            return format(value, "f")
        return raw

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
        subject = self._subject_with_context(
            f"[QQOKX] 信号提醒 | {strategy_name} | {trigger_symbol} | {self._signal_label(signal)}",
            api_name=api_name,
            session_id=session_id,
            trader_id=trader_id,
        )
        skip_reason = self._kind_disabled_reason("signal")
        if skip_reason:
            self._log(f"邮件未发送 | 类型=信号 | {subject} | 原因={skip_reason}")
            return
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
                f"参考价：{self._format_signal_entry_reference(entry_reference, trigger_symbol)}",
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
        closing = self._trade_title_is_close(title)
        direction = self._position_direction_from_trade_side(side, closing=closing)
        event = self._trade_event_label(title, closing=closing)
        subject = self._subject_with_context(
            f"[QQOKX] {strategy_name} | {direction or '-'} | {event} | {symbol}",
            api_name=api_name,
            session_id=session_id,
            trader_id=trader_id,
        )
        skip_reason = self._kind_disabled_reason("trade_fill")
        if skip_reason:
            self._log(f"邮件未发送 | 类型=成交 | {subject} | 原因={skip_reason}")
            return
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
                *([f"本笔净盈亏：{trade_pnl}"] if trade_pnl.strip() else []),
                f"说明：{reason}",
            ]
        )
        self.notify_async(subject, body)

    def send_trade_close(
        self,
        *,
        strategy_name: str,
        config: StrategyConfig,
        symbol: str,
        side: str,
        size: str,
        entry_price: str = "",
        exit_price: str = "",
        trigger_reason: str,
        detail: str,
        trade_pnl: str = "",
        api_name: str = "",
        session_id: str = "",
        trader_id: str = "",
        direction_label: str = "",
        run_mode_label: str = "",
        price_label: str = "平仓价格",
        stop_execution_status: str = "",
        stop_execution_summary: str = "",
    ) -> None:
        direction = self._position_direction_from_trade_side(side, closing=True)
        event = self._trade_event_label(trigger_reason, closing=True)
        status = str(stop_execution_status or "").strip().lower()
        status_prefix = {"warning": "⚠️ 止损超额", "critical": "🚨 止损严重超额"}.get(status, "")
        subject_text = f"[QQOKX] {status_prefix} | {strategy_name} | {direction or '-'} | {event} | {symbol}" if status_prefix else f"[QQOKX] {strategy_name} | {direction or '-'} | {event} | {symbol}"
        subject = self._subject_with_context(
            subject_text,
            api_name=api_name,
            session_id=session_id,
            trader_id=trader_id,
        )
        skip_reason = self._kind_disabled_reason("trade_fill")
        if skip_reason:
            self._log(f"邮件未发送 | 类型=平仓 | {subject} | 原因={skip_reason}")
            return
        lines = [
            *self._build_base_lines(
                strategy_name=strategy_name,
                config=config,
                api_name=api_name,
                session_id=session_id,
                trader_id=trader_id,
                direction_label=direction_label,
                run_mode_label=run_mode_label,
            ),
            f"平仓标的：{symbol}",
            f"平仓方向：{self._trade_side_label(side)}",
            f"平仓数量：{size}",
            f"触发原因：{trigger_reason}",
        ]
        if stop_execution_summary.strip():
            lines.extend(["", "【止损执行归因】", stop_execution_summary])
        if entry_price.strip():
            lines.append(f"开仓价格：{entry_price}")
        if exit_price.strip():
            lines.append(f"{price_label}：{exit_price}")
        if trade_pnl.strip():
            lines.append(f"本笔净盈亏：{trade_pnl}")
        lines.append(f"说明：{detail}")
        self.notify_async(subject, "\n".join(lines))

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
        subject = self._subject_with_context(
            f"[QQOKX] 异常提醒 | {strategy_name}",
            api_name=api_name,
            session_id=session_id,
            trader_id=trader_id,
        )
        skip_reason = self._kind_disabled_reason("error")
        if skip_reason:
            self._log(f"邮件未发送 | 类型=异常 | {subject} | 原因={skip_reason}")
            return
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

    def notify_async(self, subject: str, body: str, html_body: str | None = None) -> None:
        skip_reason = self._base_delivery_disabled_reason()
        if skip_reason:
            self._log(f"邮件未发送 | {subject} | 原因={skip_reason}")
            return
        self._log(f"邮件发送任务已提交 | {subject}")
        threading.Thread(
            target=self._send,
            args=(subject, body, html_body),
            daemon=True,
            name="qqokx-email-notifier",
        ).start()

    def _send(self, subject: str, body: str, html_body: str | None = None) -> None:
        sender = (self._config.sender_email or self._config.smtp_username).strip()
        recipients = self._recipients()
        if not sender or not recipients:
            self._log(f"邮件发送失败 | {subject} | 原因=发件邮箱或收件邮箱为空")
            return

        message = EmailMessage()
        message["From"] = sender
        message["To"] = ", ".join(recipients)
        message["Subject"] = subject
        message.set_content(body)
        if html_body and html_body.strip():
            message.add_alternative(html_body, subtype="html")

        try:
            if self._config.use_ssl:
                with smtplib.SMTP_SSL(self._config.smtp_host, self._config.smtp_port, timeout=20) as smtp:
                    self._login_and_send(smtp, sender, recipients, message)
            else:
                with smtplib.SMTP(self._config.smtp_host, self._config.smtp_port, timeout=20) as smtp:
                    smtp.ehlo()
                    smtp.starttls()
                    smtp.ehlo()
                    self._login_and_send(smtp, sender, recipients, message)
            self._log(f"邮件已发送 | {subject}")
        except smtplib.SMTPAuthenticationError as exc:
            self._log(f"邮件发送失败 | {subject} | {self._format_auth_error(exc, sender)}")
        except Exception as exc:
            self._log(f"邮件发送失败 | {subject} | {exc}")

    def _login_and_send(
        self,
        smtp: smtplib.SMTP,
        sender: str,
        recipients: list[str],
        message: EmailMessage,
    ) -> None:
        login_username = (self._config.smtp_username or sender).strip()
        if login_username and self._config.smtp_password:
            smtp.login(login_username, self._config.smtp_password)
        smtp.send_message(message, from_addr=sender, to_addrs=recipients)

    @staticmethod
    def _mask_mailbox(value: str) -> str:
        text = (value or "").strip()
        if not text:
            return "-"
        if "@" in text:
            local_part, domain = text.split("@", 1)
            if len(local_part) <= 2:
                masked_local = f"{local_part[:1]}***"
            else:
                masked_local = f"{local_part[:2]}***"
            return f"{masked_local}@{domain}"
        if len(text) <= 3:
            return "*" * len(text)
        return f"{text[:2]}***{text[-1]}"

    def _format_auth_error(self, exc: smtplib.SMTPAuthenticationError, sender: str) -> str:
        smtp_code = getattr(exc, "smtp_code", "")
        smtp_error = getattr(exc, "smtp_error", "")
        if isinstance(smtp_error, bytes):
            detail = smtp_error.decode("utf-8", errors="ignore").strip()
        else:
            detail = str(smtp_error).strip()
        if not detail:
            detail = str(exc)
        login_username = (self._config.smtp_username or sender).strip()
        return (
            f"SMTP认证失败({smtp_code})：{detail}；请检查 SMTP 用户名、SMTP 密码/授权码，"
            f"以及 SSL/端口组合是否匹配（常见为 SSL=465，STARTTLS=587）。"
            f" 当前主机={self._config.smtp_host}:{self._config.smtp_port}"
            f"，SSL={'开' if self._config.use_ssl else '关'}"
            f"，用户名={self._mask_mailbox(login_username)}"
            f"，发件邮箱={self._mask_mailbox(sender)}"
        )

    def _recipients(self) -> list[str]:
        return [item.strip() for item in self._config.recipient_emails if item.strip()]

    def _log(self, message: str) -> None:
        if self._logger is not None:
            self._logger(ensure_log_timestamp(message))
            return
        append_log_line(message)
