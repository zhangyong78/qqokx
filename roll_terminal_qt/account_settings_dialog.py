from __future__ import annotations

from PySide6.QtWidgets import (
    QCheckBox, QComboBox, QDialog, QDialogButtonBox, QFormLayout,
    QGridLayout, QGroupBox, QHBoxLayout, QInputDialog, QLabel,
    QLineEdit, QMessageBox, QPushButton, QScrollArea, QVBoxLayout, QWidget,
)

from okx_quant.persistence import (
    build_profile_switch_password_snapshot, credentials_file_path, load_credentials_profiles_snapshot,
    load_notification_snapshot, save_credentials_profiles_snapshot,
    save_notification_snapshot, settings_file_path, verify_profile_switch_password,
)
from okx_quant.network_proxy import (
    PROXY_MODE_OPTIONS, PROXY_RESTART_NOTE, load_network_proxy_settings,
    save_network_proxy_settings, validate_network_proxy_settings,
)
from roll_terminal_qt.profile_access import ensure_profile_unlocked


class AccountSettingsDialog(QDialog):
    """Edit the same credential and notification files as the Tk settings page."""

    _FEES = (
        "spot_maker_fee_rate", "spot_taker_fee_rate",
        "futures_maker_fee_rate", "futures_taker_fee_rate",
        "option_maker_fee_rate", "option_taker_fee_rate",
    )

    def __init__(self, *, profile_name: str = "", parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("API 与通知设置")
        self.resize(850, 690)
        self._profiles: dict[str, dict[str, str]] = {}
        self._selected_profile = ""
        self._loaded_profile: dict[str, str] = {}
        self._displayed_fields: dict[str, str] = {}
        self._loaded_notification: dict[str, object] = {}
        self._unlocked: set[str] = set()

        root = QVBoxLayout(self)
        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        page = QWidget(scroll)
        layout = QVBoxLayout(page)
        scroll.setWidget(page)
        root.addWidget(scroll)

        account = QGroupBox("API 与交易设置", page)
        form = QFormLayout(account)
        profile_row = QHBoxLayout()
        self.profile_combo = QComboBox(account)
        self.profile_combo.currentTextChanged.connect(self._switch_profile)
        profile_row.addWidget(self.profile_combo, 1)
        new_button = QPushButton("新建配置", account)
        new_button.clicked.connect(self._create_profile)
        profile_row.addWidget(new_button)
        rename_button = QPushButton("重命名", account)
        rename_button.clicked.connect(self._rename_profile)
        profile_row.addWidget(rename_button)
        delete_button = QPushButton("删除当前", account)
        delete_button.clicked.connect(self._delete_profile)
        profile_row.addWidget(delete_button)
        form.addRow("API 配置", profile_row)
        password_row = QHBoxLayout()
        self.password_status = QLabel(account)
        password_row.addWidget(self.password_status, 1)
        password_button = QPushButton("设置/更新密码", account)
        password_button.clicked.connect(self._set_password)
        password_row.addWidget(password_button)
        clear_password_button = QPushButton("清除密码", account)
        clear_password_button.clicked.connect(self._clear_password)
        password_row.addWidget(clear_password_button)
        form.addRow("切换密码", password_row)
        self.api_key = QLineEdit(account)
        self.secret_key = QLineEdit(account)
        self.secret_key.setEchoMode(QLineEdit.EchoMode.Password)
        self.passphrase = QLineEdit(account)
        self.passphrase.setEchoMode(QLineEdit.EchoMode.Password)
        form.addRow("API Key", self.api_key)
        form.addRow("Secret Key", self.secret_key)
        form.addRow("Passphrase", self.passphrase)
        self.environment = QComboBox(account)
        self.environment.addItem("模拟盘 demo", "demo")
        self.environment.addItem("实盘 live", "live")
        form.addRow("环境", self.environment)
        self.trade_mode = QComboBox(account)
        self.trade_mode.addItem("全仓 cross", "cross")
        self.trade_mode.addItem("逐仓 isolated", "isolated")
        form.addRow("Trade Mode", self.trade_mode)
        self.position_mode = QComboBox(account)
        self.position_mode.addItem("净持仓 net", "net")
        self.position_mode.addItem("双向持仓 long_short", "long_short")
        form.addRow("Position Mode", self.position_mode)
        fee_grid = QGridLayout()
        self.fees: dict[str, QLineEdit] = {}
        for row, (label, prefix) in enumerate((("Spot", "spot"), ("Futures", "futures"), ("Option", "option")), 1):
            fee_grid.addWidget(QLabel(label), row, 0)
            for column, kind in enumerate(("maker", "taker"), 1):
                key = f"{prefix}_{kind}_fee_rate"
                field = QLineEdit(account)
                self.fees[key] = field
                fee_grid.addWidget(field, row, column)
        fee_grid.addWidget(QLabel("Maker %"), 0, 1)
        fee_grid.addWidget(QLabel("Taker %"), 0, 2)
        form.addRow("Custom Fee (%)", fee_grid)
        form.addRow(QLabel(f"凭证加密保存：{credentials_file_path()}"))
        layout.addWidget(account)

        network = QGroupBox("网络代理（交易接口与行情）", page)
        network_form = QFormLayout(network)
        self.proxy_mode = QComboBox(network)
        for label, mode in PROXY_MODE_OPTIONS.items():
            self.proxy_mode.addItem(label, mode)
        self.proxy_url = QLineEdit(network)
        self.proxy_url.setPlaceholderText("http://127.0.0.1:10808")
        self.proxy_mode.currentIndexChanged.connect(
            lambda: self.proxy_url.setEnabled(self.proxy_mode.currentData() == "custom")
        )
        network_form.addRow("代理模式", self.proxy_mode)
        network_form.addRow("HTTP 代理地址", self.proxy_url)
        note = QLabel(PROXY_RESTART_NOTE, network)
        note.setWordWrap(True)
        network_form.addRow(note)
        proxy_save = QPushButton("保存代理设置", network)
        proxy_save.clicked.connect(self._save_proxy)
        network_form.addRow(proxy_save)
        layout.addWidget(network)
        self._loaded_proxy = None
        try:
            self._loaded_proxy = load_network_proxy_settings()
            self._set_combo(self.proxy_mode, self._loaded_proxy.mode)
            self.proxy_url.setText(self._loaded_proxy.url)
        except (OSError, ValueError):
            self._set_combo(self.proxy_mode, "system")
            note.setText("网络代理设置读取失败，请重新保存。" + PROXY_RESTART_NOTE)
        self.proxy_url.setEnabled(self.proxy_mode.currentData() == "custom")

        mail = QGroupBox("邮件通知设置", page)
        mail_form = QFormLayout(mail)
        self.mail_enabled = QCheckBox("启用邮件通知", mail)
        mail_form.addRow(self.mail_enabled)
        self.trigger_type = QComboBox(mail)
        for label in ("标记价格 mark", "最新价格 last", "指数价格 index"):
            self.trigger_type.addItem(label)
        mail_form.addRow("止盈止损触发价格类型", self.trigger_type)
        self.mail_fields: dict[str, QLineEdit] = {}
        for key, label in (
            ("smtp_host", "SMTP 主机"), ("smtp_port", "SMTP 端口"),
            ("smtp_username", "SMTP 用户名"), ("smtp_password", "SMTP 密码"),
            ("sender_email", "发件邮箱"), ("recipient_emails", "收件邮箱"),
        ):
            field = QLineEdit(mail)
            if key == "smtp_password":
                field.setEchoMode(QLineEdit.EchoMode.Password)
            self.mail_fields[key] = field
            mail_form.addRow(label, field)
        self.recipient_override = QLineEdit(mail)
        mail_form.addRow("当前 API 专属收件邮箱", self.recipient_override)
        self.ssl = QCheckBox("使用 SSL", mail)
        self.fills = QCheckBox("成交邮件", mail)
        self.signals = QCheckBox("信号邮件", mail)
        self.errors = QCheckBox("异常邮件", mail)
        checks = QHBoxLayout()
        for widget in (self.ssl, self.fills, self.signals, self.errors):
            checks.addWidget(widget)
        mail_form.addRow(checks)
        mail_form.addRow(QLabel(f"通知设置：{settings_file_path()}"))
        layout.addWidget(mail)
        layout.addStretch()

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Close, self)
        buttons.button(QDialogButtonBox.StandardButton.Save).setText("保存")
        buttons.button(QDialogButtonBox.StandardButton.Close).setText("关闭")
        buttons.accepted.connect(self._save)
        buttons.rejected.connect(self.reject)
        root.addWidget(buttons)
        self._load(profile_name)

    def _load(self, requested: str) -> None:
        snapshot = load_credentials_profiles_snapshot()
        self._profiles = dict(snapshot.get("profiles", {}))
        self._selected_profile = str(snapshot.get("selected_profile", "") or "")
        self.profile_combo.blockSignals(True)
        self.profile_combo.clear()
        self.profile_combo.addItems(self._profiles)
        target = requested if requested in self._profiles else self._selected_profile
        self.profile_combo.setCurrentText(target)
        self.profile_combo.blockSignals(False)
        self._show_profile(target)
        self._loaded_notification = load_notification_snapshot()
        notification = self._loaded_notification
        self.mail_enabled.setChecked(bool(notification["enabled"]))
        self.trigger_type.setCurrentText(str(notification.get("trigger_type_label", "标记价格 mark")))
        self.recipient_override.setText(str(notification.get("api_sender_email_overrides", {}).get(target, "")))
        for key, field in self.mail_fields.items():
            field.setText(str(notification.get(key, "") or ""))
        for widget, key in ((self.ssl, "use_ssl"), (self.fills, "notify_trade_fills"),
                            (self.signals, "notify_signals"), (self.errors, "notify_errors")):
            widget.setChecked(bool(notification[key]))

    def _show_profile(self, name: str) -> None:
        profile = self._profiles.get(name)
        if not isinstance(profile, dict):
            return
        if not ensure_profile_unlocked(self, name, self._profiles, self._unlocked):
            self.profile_combo.blockSignals(True)
            self.profile_combo.setCurrentText(self._selected_profile)
            self.profile_combo.blockSignals(False)
            return
        self._selected_profile = name
        self._loaded_profile = dict(profile)
        self.password_status.setText("已设置" if profile.get("switch_password_hash") else "未设置")
        if self._loaded_notification:
            self.recipient_override.setText(str(self._loaded_notification.get("api_sender_email_overrides", {}).get(name, "")))
        self.api_key.setText(profile.get("api_key", ""))
        self.secret_key.setText(profile.get("secret_key", ""))
        self.passphrase.setText(profile.get("passphrase", ""))
        self._set_combo(self.environment, profile.get("environment", "demo"))
        notification = load_notification_snapshot()
        self._set_combo(self.trade_mode, profile.get("trade_mode", "isolated" if "isolated" in str(notification["trade_mode_label"]) else "cross"))
        self._set_combo(self.position_mode, profile.get("position_mode", "long_short" if "long_short" in str(notification["position_mode_label"]) else "net"))
        for key, field in self.fees.items():
            field.setText(str(profile.get(key, "") or ""))
        self._displayed_fields = self._profile_fields()

    @staticmethod
    def _set_combo(combo: QComboBox, value: str) -> None:
        index = combo.findData(value)
        combo.setCurrentIndex(max(index, 0))

    def _switch_profile(self, name: str) -> None:
        if not name or name == self._selected_profile:
            return
        if self._profile_fields() != self._displayed_fields:
            answer = QMessageBox.question(self, "未保存的修改", "当前账户有未保存的修改。先保存再切换账户？")
            if answer != QMessageBox.StandardButton.Yes or not self._save():
                self.profile_combo.blockSignals(True)
                self.profile_combo.setCurrentText(self._selected_profile)
                self.profile_combo.blockSignals(False)
                return
        self._show_profile(name)

    def _create_profile(self) -> None:
        if self._profile_fields() != self._displayed_fields and not self._save():
            return
        name, accepted = QInputDialog.getText(self, "新建 API 配置", "配置名称")
        name = name.strip()
        if not accepted or not name:
            return
        if name in self._profiles:
            QMessageBox.warning(self, "新建配置", "该名称已存在。")
            return
        snapshot = load_credentials_profiles_snapshot()
        profiles = dict(snapshot.get("profiles", {}))
        if name in profiles:
            QMessageBox.warning(self, "新建配置", "该名称已被另一窗口使用。")
            return
        profiles[name] = {"api_key": "", "secret_key": "", "passphrase": "", "environment": "demo"}
        save_credentials_profiles_snapshot(selected_profile=str(snapshot.get("selected_profile") or name), profiles=profiles)
        self._load(name)

    def _account_mutation_allowed(self) -> bool:
        summary = getattr(self.parent(), "_local_task_summary", None)
        if callable(summary) and any(int(value or 0) for value in summary().values()):
            QMessageBox.warning(self, "账户配置", "当前有本地任务运行，请先结束任务再重命名或删除 API 配置。")
            return False
        return True

    def _rename_profile(self) -> None:
        if not self._account_mutation_allowed() or self._profile_fields() != self._displayed_fields:
            QMessageBox.warning(self, "重命名", "请先保存当前修改。")
            return
        old = self._selected_profile
        name, accepted = QInputDialog.getText(self, "重命名 API 配置", "新名称", text=old)
        name = name.strip()
        if not accepted or not name or name == old:
            return
        snapshot = load_credentials_profiles_snapshot()
        profiles = dict(snapshot.get("profiles", {}))
        if name in profiles or old not in profiles:
            QMessageBox.warning(self, "重命名", "名称已存在或原配置已变化。")
            return
        profiles[name] = profiles.pop(old)
        selected = name if snapshot.get("selected_profile") == old else str(snapshot.get("selected_profile") or name)
        save_credentials_profiles_snapshot(selected_profile=selected, profiles=profiles)
        self._load(name)

    def _delete_profile(self) -> None:
        if not self._account_mutation_allowed():
            return
        name = self._selected_profile
        if QMessageBox.question(self, "删除 API 配置", f"删除 {name}？此操作不会撤销交易所订单。") != QMessageBox.StandardButton.Yes:
            return
        snapshot = load_credentials_profiles_snapshot()
        profiles = dict(snapshot.get("profiles", {}))
        if name not in profiles or len(profiles) < 2:
            QMessageBox.warning(self, "删除配置", "至少保留一个 API 配置。")
            return
        profiles.pop(name)
        selected = next(iter(profiles)) if snapshot.get("selected_profile") == name else str(snapshot.get("selected_profile"))
        save_credentials_profiles_snapshot(selected_profile=selected, profiles=profiles)
        self._load(selected)

    def _set_password(self) -> None:
        name = self._selected_profile
        if not name:
            return
        password, accepted = QInputDialog.getText(self, "设置切换密码", f"API 配置 {name} 的新密码", QLineEdit.EchoMode.Password)
        if not accepted or not password:
            return
        confirm, accepted = QInputDialog.getText(self, "确认密码", "再次输入新密码", QLineEdit.EchoMode.Password)
        if not accepted or password != confirm:
            QMessageBox.warning(self, "设置密码", "两次密码不一致。")
            return
        self._save_password_snapshot(build_profile_switch_password_snapshot(password))

    def _clear_password(self) -> None:
        name = self._selected_profile
        if not self._loaded_profile.get("switch_password_hash"):
            return
        password, accepted = QInputDialog.getText(self, "清除切换密码", f"输入 {name} 的现有密码", QLineEdit.EchoMode.Password)
        if not accepted or not verify_profile_switch_password(self._loaded_profile, password):
            QMessageBox.warning(self, "清除密码", "密码不正确。")
            return
        self._save_password_snapshot({"switch_password_hash": "", "switch_password_salt": "", "switch_password_iterations": ""})

    def _save_password_snapshot(self, password_fields: dict[str, str]) -> None:
        snapshot = load_credentials_profiles_snapshot()
        profiles = dict(snapshot.get("profiles", {}))
        name = self._selected_profile
        if name not in profiles:
            QMessageBox.warning(self, "切换密码", "账户配置已变化，请重新打开。")
            return
        profiles[name] = {**profiles[name], **password_fields}
        save_credentials_profiles_snapshot(selected_profile=str(snapshot.get("selected_profile") or name), profiles=profiles)
        self._profiles = profiles
        self._loaded_profile = dict(profiles[name])
        self.password_status.setText("已设置" if password_fields["switch_password_hash"] else "未设置")
        self._unlocked.add(name)

    def _profile_fields(self) -> dict[str, str]:
        return {
            "api_key": self.api_key.text().strip(), "secret_key": self.secret_key.text().strip(),
            "passphrase": self.passphrase.text().strip(),
            "environment": str(self.environment.currentData()),
            "trade_mode": str(self.trade_mode.currentData()),
            "position_mode": str(self.position_mode.currentData()),
            **{key: field.text().strip() for key, field in self.fees.items()},
        }

    def _save_proxy_settings(self) -> None:
        settings = validate_network_proxy_settings(str(self.proxy_mode.currentData()), self.proxy_url.text())
        if settings != self._loaded_proxy:
            if self._loaded_proxy is not None:
                current = load_network_proxy_settings()
                if current not in (self._loaded_proxy, settings):
                    raise ValueError("代理设置已被另一窗口修改，请重新打开设置。")
            self._loaded_proxy = save_network_proxy_settings(settings.mode, settings.url)

    def _save_proxy(self) -> None:
        try:
            self._save_proxy_settings()
        except Exception as exc:
            QMessageBox.warning(self, "代理设置保存失败", str(exc))
            return
        QMessageBox.information(self, "代理设置已保存", PROXY_RESTART_NOTE)

    def _save(self) -> bool:
        name = self._selected_profile
        if not name:
            return False
        fields = self._profile_fields()
        if any(fields[key] for key in ("api_key", "secret_key", "passphrase")) and not all(
            fields[key] for key in ("api_key", "secret_key", "passphrase")
        ):
            QMessageBox.warning(self, "保存失败", "API Key、Secret Key 和 Passphrase 必须同时填写。")
            return False
        try:
            validate_network_proxy_settings(str(self.proxy_mode.currentData()), self.proxy_url.text())
            for key in self._FEES:
                if fields[key] and float(fields[key]) < 0:
                    raise ValueError(f"{key} 不能为负数")
            port = int(self.mail_fields["smtp_port"].text().strip())
            if not 1 <= port <= 65535:
                raise ValueError("SMTP 端口无效")
            current = load_credentials_profiles_snapshot()
            profiles = dict(current.get("profiles", {}))
            latest = profiles.get(name, {})
            if name in profiles and any(
                str(latest.get(key, "") or "").strip() != str(self._loaded_profile.get(key, "") or "").strip()
                for key in fields
            ):
                raise ValueError("此账户配置已被另一窗口修改，请关闭后重新打开。")
            current_mail = load_notification_snapshot()
            mail_changes = {key: field.text().strip() for key, field in self.mail_fields.items()}
            mail_changes.update({
                "enabled": self.mail_enabled.isChecked(), "use_ssl": self.ssl.isChecked(),
                "trigger_type_label": self.trigger_type.currentText(),
                "notify_trade_fills": self.fills.isChecked(), "notify_signals": self.signals.isChecked(),
                "notify_errors": self.errors.isChecked(),
            })
            for key, value in mail_changes.items():
                if current_mail.get(key) != self._loaded_notification.get(key) and current_mail.get(key) != value:
                    raise ValueError("通知设置已被另一窗口修改，请重新打开。")
            overrides = dict(current_mail.get("api_sender_email_overrides", {}))
            recipient = self.recipient_override.text().strip()
            old_recipient = str(self._loaded_notification.get("api_sender_email_overrides", {}).get(name, ""))
            current_recipient = str(overrides.get(name, ""))
            if current_recipient != old_recipient and current_recipient != recipient:
                raise ValueError("当前 API 的收件邮箱已被另一窗口修改，请重新打开。")
            if recipient:
                overrides[name] = recipient
            else:
                overrides.pop(name, None)
            mail_changes["api_sender_email_overrides"] = overrides
            profiles[name] = {**latest, **fields}
            merged = {**current_mail, **mail_changes, "smtp_port": port}
            save_credentials_profiles_snapshot(selected_profile=str(current.get("selected_profile") or name), profiles=profiles)
            save_notification_snapshot(**merged)
            self._save_proxy_settings()
            self._profiles = profiles
            self._loaded_profile = dict(profiles[name])
            self._displayed_fields = fields
            self._loaded_notification = load_notification_snapshot()
        except Exception as exc:
            QMessageBox.warning(self, "保存失败", str(exc))
            return False
        QMessageBox.information(self, "已保存", f"API 配置 {name} 已保存；新启动的任务会读取新参数。\n代理设置需重启所有 QQOKX 窗口生效。")
        return True
