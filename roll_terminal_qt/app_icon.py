from __future__ import annotations

import ctypes
import sys
from functools import lru_cache
from pathlib import Path

from PySide6.QtGui import QGuiApplication, QIcon
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QWidget


def qt_app_icon_path() -> Path:
    return Path(__file__).resolve().parent.parent / "okx_quant" / "assets" / "quant_console.ico"


@lru_cache(maxsize=1)
def qt_app_icon() -> QIcon:
    return QIcon(str(qt_app_icon_path()))


def apply_qt_application_identity(app: QApplication | QGuiApplication) -> None:
    if sys.platform.startswith("win"):
        try:
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("qqokx.quant.console")
        except Exception:
            pass
    icon_path = qt_app_icon_path()
    if icon_path.exists():
        app.setWindowIcon(qt_app_icon())


def apply_qt_window_icon(window: QWidget) -> None:
    icon_path = qt_app_icon_path()
    if icon_path.exists():
        window.setWindowIcon(qt_app_icon())
    if window.isWindow():
        try:
            window.setWindowFlag(Qt.WindowType.WindowMinimizeButtonHint, True)
            window.setWindowFlag(Qt.WindowType.WindowCloseButtonHint, True)
        except Exception:
            pass
