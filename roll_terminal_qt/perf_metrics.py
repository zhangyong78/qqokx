from __future__ import annotations

from contextlib import contextmanager
from time import perf_counter
from typing import Iterator

from okx_quant.log_utils import append_log_line


@contextmanager
def measure_ui_step(name: str, **fields: object) -> Iterator[None]:
    """Record one UI operation duration without changing its execution flow."""
    started = perf_counter()
    try:
        yield
    finally:
        elapsed_ms = (perf_counter() - started) * 1000.0
        suffix = " | ".join(f"{key}={value}" for key, value in fields.items())
        append_log_line(
            f"[qt_perf] {name} | elapsed_ms={elapsed_ms:.3f}"
            + (f" | {suffix}" if suffix else "")
        )
