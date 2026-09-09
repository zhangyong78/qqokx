from __future__ import annotations

import base64
import struct
import zlib
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, Sequence

from okx_quant.models import Candle

RGB = tuple[int, int, int]

BACKGROUND: RGB = (248, 250, 252)
GRID: RGB = (226, 232, 240)
BORDER: RGB = (203, 213, 225)
BULL: RGB = (39, 174, 96)
BEAR: RGB = (231, 76, 60)
NEUTRAL: RGB = (100, 116, 139)
LINE_COLORS: tuple[RGB, ...] = (
    (59, 130, 246),
    (245, 158, 11),
    (168, 85, 247),
    (16, 185, 129),
    (239, 68, 68),
    (20, 184, 166),
)


@dataclass(frozen=True)
class MiniChartOverlay:
    period: int
    ma_type: str = "ema"
    label: str = ""
    color: RGB | None = None

    @property
    def normalized_type(self) -> str:
        return "ma" if str(self.ma_type or "").strip().lower() == "ma" else "ema"

    @property
    def resolved_label(self) -> str:
        prefix = "MA" if self.normalized_type == "ma" else "EMA"
        return self.label or f"{prefix}{self.period}"


def render_candles_png_base64(
    candles: Sequence[Candle] | Iterable[Candle],
    *,
    width: int = 320,
    height: int = 160,
    max_candles: int = 72,
    overlays: Sequence[MiniChartOverlay] = (),
) -> str:
    return base64.b64encode(
        render_candles_png(candles, width=width, height=height, max_candles=max_candles, overlays=overlays)
    ).decode("ascii")


def render_candles_png(
    candles: Sequence[Candle] | Iterable[Candle],
    *,
    width: int = 320,
    height: int = 160,
    max_candles: int = 72,
    overlays: Sequence[MiniChartOverlay] = (),
) -> bytes:
    all_rows = list(candles)
    if not all_rows:
        return render_placeholder_png(width=width, height=height)
    all_rows = sorted(all_rows, key=lambda item: item.ts)
    rows = all_rows[-max(8, max_candles) :]

    pixels = [[BACKGROUND for _ in range(width)] for _ in range(height)]
    _draw_rect(pixels, 0, 0, width - 1, height - 1, BORDER)

    left = 8
    right = width - 8
    top = 8
    bottom = height - 8
    plot_width = max(10, right - left)
    plot_height = max(10, bottom - top)

    highs = [float(item.high) for item in rows]
    lows = [float(item.low) for item in rows]
    max_price = max(highs)
    min_price = min(lows)
    if max_price <= min_price:
        pad = max(abs(max_price) * 0.01, 1.0)
        max_price += pad
        min_price -= pad
    else:
        pad = (max_price - min_price) * 0.05
        max_price += pad
        min_price -= pad

    for ratio in (0.25, 0.5, 0.75):
        y = top + int(plot_height * ratio)
        _draw_hline(pixels, left, right, y, GRID)

    count = len(rows)
    step = plot_width / max(count, 1)
    candle_width = max(2, min(8, int(step * 0.6)))

    for index, candle in enumerate(rows):
        center_x = left + int((index + 0.5) * step)
        wick_top = _price_to_y(float(candle.high), min_price, max_price, top, bottom)
        wick_bottom = _price_to_y(float(candle.low), min_price, max_price, top, bottom)
        open_y = _price_to_y(float(candle.open), min_price, max_price, top, bottom)
        close_y = _price_to_y(float(candle.close), min_price, max_price, top, bottom)
        color = BULL if candle.close > candle.open else BEAR if candle.close < candle.open else NEUTRAL
        _draw_vline(pixels, center_x, wick_top, wick_bottom, color)
        body_top = min(open_y, close_y)
        body_bottom = max(open_y, close_y)
        if body_bottom - body_top <= 1:
            _draw_hline(pixels, center_x - candle_width // 2, center_x + candle_width // 2, body_top, color)
        else:
            _fill_rect(
                pixels,
                center_x - candle_width // 2,
                body_top,
                center_x + candle_width // 2,
                body_bottom,
                color,
            )

    if overlays:
        close_values = [item.close for item in all_rows]
        x_positions = [left + int((index + 0.5) * step) for index in range(len(rows))]
        start_index = len(all_rows) - len(rows)
        for overlay_index, overlay in enumerate(overlays):
            values = _moving_average_values(close_values, overlay)
            visible_values = values[start_index:]
            line_color = overlay.color or LINE_COLORS[overlay_index % len(LINE_COLORS)]
            previous_point: tuple[int, int] | None = None
            for point_index, value in enumerate(visible_values):
                if value is None:
                    previous_point = None
                    continue
                x = x_positions[point_index]
                y = _price_to_y(float(value), min_price, max_price, top, bottom)
                if previous_point is not None:
                    _draw_line(pixels, previous_point[0], previous_point[1], x, y, line_color)
                previous_point = (x, y)

    return _encode_png(pixels)


def render_placeholder_png(*, width: int = 320, height: int = 160) -> bytes:
    pixels = [[BACKGROUND for _ in range(width)] for _ in range(height)]
    _draw_rect(pixels, 0, 0, width - 1, height - 1, BORDER)
    for offset in range(0, min(width, height), 12):
        _draw_line(pixels, 0, offset, offset, 0, GRID)
        _draw_line(pixels, width - 1 - offset, height - 1, width - 1, height - 1 - offset, GRID)
    return _encode_png(pixels)


def _price_to_y(price: float, min_price: float, max_price: float, top: int, bottom: int) -> int:
    usable = max(1, bottom - top)
    ratio = (price - min_price) / max(max_price - min_price, 1e-9)
    return bottom - int(ratio * usable)


def _draw_rect(pixels: list[list[RGB]], x1: int, y1: int, x2: int, y2: int, color: RGB) -> None:
    _draw_hline(pixels, x1, x2, y1, color)
    _draw_hline(pixels, x1, x2, y2, color)
    _draw_vline(pixels, x1, y1, y2, color)
    _draw_vline(pixels, x2, y1, y2, color)


def _fill_rect(pixels: list[list[RGB]], x1: int, y1: int, x2: int, y2: int, color: RGB) -> None:
    width = len(pixels[0])
    height = len(pixels)
    start_x = max(0, min(x1, x2))
    end_x = min(width - 1, max(x1, x2))
    start_y = max(0, min(y1, y2))
    end_y = min(height - 1, max(y1, y2))
    for y in range(start_y, end_y + 1):
        row = pixels[y]
        for x in range(start_x, end_x + 1):
            row[x] = color


def _draw_hline(pixels: list[list[RGB]], x1: int, x2: int, y: int, color: RGB) -> None:
    if y < 0 or y >= len(pixels):
        return
    width = len(pixels[0])
    start_x = max(0, min(x1, x2))
    end_x = min(width - 1, max(x1, x2))
    row = pixels[y]
    for x in range(start_x, end_x + 1):
        row[x] = color


def _draw_vline(pixels: list[list[RGB]], x: int, y1: int, y2: int, color: RGB) -> None:
    width = len(pixels[0])
    height = len(pixels)
    if x < 0 or x >= width:
        return
    start_y = max(0, min(y1, y2))
    end_y = min(height - 1, max(y1, y2))
    for y in range(start_y, end_y + 1):
        pixels[y][x] = color


def _draw_line(pixels: list[list[RGB]], x1: int, y1: int, x2: int, y2: int, color: RGB) -> None:
    dx = abs(x2 - x1)
    sx = 1 if x1 < x2 else -1
    dy = -abs(y2 - y1)
    sy = 1 if y1 < y2 else -1
    err = dx + dy
    while True:
        if 0 <= y1 < len(pixels) and 0 <= x1 < len(pixels[0]):
            pixels[y1][x1] = color
        if x1 == x2 and y1 == y2:
            break
        e2 = 2 * err
        if e2 >= dy:
            err += dy
            x1 += sx
        if e2 <= dx:
            err += dx
            y1 += sy


def _encode_png(pixels: list[list[RGB]]) -> bytes:
    height = len(pixels)
    width = len(pixels[0]) if height else 0
    raw = bytearray()
    for row in pixels:
        raw.append(0)
        for red, green, blue in row:
            raw.extend((red, green, blue))
    compressed = zlib.compress(bytes(raw), level=9)
    parts = [
        b"\x89PNG\r\n\x1a\n",
        _png_chunk(b"IHDR", struct.pack("!IIBBBBB", width, height, 8, 2, 0, 0, 0)),
        _png_chunk(b"IDAT", compressed),
        _png_chunk(b"IEND", b""),
    ]
    return b"".join(parts)


def _png_chunk(chunk_type: bytes, data: bytes) -> bytes:
    return (
        struct.pack("!I", len(data))
        + chunk_type
        + data
        + struct.pack("!I", zlib.crc32(chunk_type + data) & 0xFFFFFFFF)
    )


def _moving_average_values(values: Sequence[Decimal], overlay: MiniChartOverlay) -> list[float | None]:
    period = max(int(overlay.period), 1)
    normalized = overlay.normalized_type
    numeric = [float(item) for item in values]
    if normalized == "ma":
        result: list[float | None] = []
        rolling_sum = 0.0
        for index, value in enumerate(numeric):
            rolling_sum += value
            if index >= period:
                rolling_sum -= numeric[index - period]
            if index + 1 < period:
                result.append(None)
            else:
                result.append(rolling_sum / period)
        return result
    result = []
    multiplier = 2.0 / (period + 1.0)
    ema_value: float | None = None
    for value in numeric:
        ema_value = value if ema_value is None else ((value - ema_value) * multiplier) + ema_value
        result.append(ema_value)
    return result
