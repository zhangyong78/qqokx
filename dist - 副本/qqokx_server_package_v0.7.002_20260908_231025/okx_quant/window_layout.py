from __future__ import annotations

from pathlib import Path
from tkinter import BaseWidget


def _app_icon_path() -> Path:
    return Path(__file__).resolve().parent / "assets" / "btc.ico"


def apply_window_icon(window: BaseWidget) -> bool:
    icon_path = _app_icon_path()
    if not icon_path.exists():
        return False
    try:
        window.iconbitmap(str(icon_path))
        return True
    except Exception:
        return False


def apply_adaptive_window_geometry(
    window: BaseWidget,
    *,
    width_ratio: float,
    height_ratio: float,
    min_width: int,
    min_height: int,
    max_width: int | None = None,
    max_height: int | None = None,
    margin: int = 48,
    top_reserve: int = 56,
    center: bool = True,
) -> tuple[int, int]:
    apply_window_icon(window)
    try:
        window.update_idletasks()
    except Exception:
        pass

    screen_width = max(int(window.winfo_screenwidth()), min_width)
    screen_height = max(int(window.winfo_screenheight()), min_height)
    usable_width = max(screen_width - margin, min_width)
    usable_height = max(screen_height - margin - top_reserve, min_height)

    width = max(int(screen_width * width_ratio), min_width)
    height = max(int(screen_height * height_ratio), min_height)

    width = min(width, usable_width)
    height = min(height, usable_height)
    if max_width is not None:
        width = min(width, max_width)
    if max_height is not None:
        height = min(height, max_height)

    window.minsize(min_width, min_height)
    if center:
        x = max((screen_width - width) // 2, 12)
        y = max((screen_height - height) // 2 - 16, 12)
    else:
        x = max(margin // 2, 12)
        y = max(top_reserve // 2, 12)
    window.geometry(f"{width}x{height}+{x}+{y}")
    return width, height


def toggle_toplevel_maximize(window: BaseWidget) -> bool:
    """在系统标题栏无最大化（如 transient 子窗）时，用程序切换铺满屏幕。

    优先使用 Windows ``state('zoomed')``；否则尝试 ``attributes('-zoomed')``。
    返回是否使用了已知可用的 API（失败时返回 False，由调用方决定是否几何铺满兜底）。"""
    try:
        st = str(window.state() or "")
        if st == "zoomed":
            window.state("normal")
            return True
        window.state("zoomed")
        return True
    except Exception:
        pass
    try:
        cur = bool(window.attributes("-zoomed"))
        window.attributes("-zoomed", not cur)
        return True
    except Exception:
        pass
    return False


def apply_fill_window_geometry(
    window: BaseWidget,
    *,
    min_width: int = 1200,
    min_height: int = 800,
    margin: int = 30,
    top_reserve: int = 56,
) -> tuple[int, int]:
    apply_window_icon(window)
    try:
        window.update_idletasks()
    except Exception:
        pass

    screen_width = max(int(window.winfo_screenwidth()), min_width)
    screen_height = max(int(window.winfo_screenheight()), min_height)
    width = max(screen_width - margin, min_width)
    height = max(screen_height - margin - top_reserve, min_height)
    x = max((screen_width - width) // 2, 8)
    y = max((screen_height - height) // 2 - 12, 8)
    window.minsize(min_width, min_height)
    window.geometry(f"{width}x{height}+{x}+{y}")
    return width, height
