from __future__ import annotations

import argparse
from pathlib import Path
import sys
import tkinter as tk

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.auto_channel_preview import build_auto_channel_preview_snapshot
from okx_quant.strategy_live_chart import render_strategy_live_chart


def main() -> None:
    parser = argparse.ArgumentParser(description="Preview the auto channel chart overlay")
    parser.add_argument("--sample", choices=("channel", "box"), default="channel")
    args = parser.parse_args()

    root = tk.Tk()
    root.title("QQOKX 自动通道预览")
    root.geometry("1180x720")
    canvas = tk.Canvas(root, width=1180, height=680, highlightthickness=0)
    canvas.pack(fill=tk.BOTH, expand=True)
    snapshot = build_auto_channel_preview_snapshot(args.sample)

    def redraw(_event: object | None = None) -> None:
        render_strategy_live_chart(canvas, snapshot)

    canvas.bind("<Configure>", redraw)
    root.after(100, redraw)
    root.mainloop()


if __name__ == "__main__":
    main()
