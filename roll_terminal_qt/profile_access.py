from __future__ import annotations

from PySide6.QtWidgets import QInputDialog, QLineEdit, QMessageBox, QWidget

from okx_quant.persistence import (
    credential_profile_has_switch_password,
    load_credentials_profiles_snapshot,
    verify_profile_switch_password,
)


def load_profile_snapshots() -> tuple[dict[str, dict[str, str]], str]:
    snapshot = load_credentials_profiles_snapshot()
    profiles = snapshot.get("profiles", {}) if isinstance(snapshot, dict) else {}
    selected_profile = str(snapshot.get("selected_profile", "") or "").strip() if isinstance(snapshot, dict) else ""
    normalized = (
        {
            str(name).strip(): dict(payload)
            for name, payload in profiles.items()
            if str(name).strip() and isinstance(payload, dict)
        }
        if isinstance(profiles, dict)
        else {}
    )
    return normalized, selected_profile


def profile_requires_password(profile_name: str, profile_snapshots: dict[str, dict[str, str]]) -> bool:
    target = profile_name.strip()
    if not target:
        return False
    return credential_profile_has_switch_password(profile_snapshots.get(target, {}))


def ensure_profile_unlocked(
    parent: QWidget,
    profile_name: str,
    profile_snapshots: dict[str, dict[str, str]],
    unlocked_profiles: set[str],
) -> bool:
    target = profile_name.strip()
    if not target:
        return False
    if not profile_requires_password(target, profile_snapshots):
        unlocked_profiles.add(target)
        return True
    if target in unlocked_profiles:
        return True
    password, accepted = QInputDialog.getText(
        parent,
        "输入 API 切换密码",
        f"API 配置 {target} 已设置切换密码，请输入密码后继续。",
        QLineEdit.EchoMode.Password,
    )
    if not accepted:
        return False
    if verify_profile_switch_password(profile_snapshots.get(target, {}), password):
        unlocked_profiles.add(target)
        return True
    QMessageBox.warning(parent, "密码错误", f"API 配置 {target} 的切换密码不正确。")
    return False
