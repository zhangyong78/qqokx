from __future__ import annotations

import base64
from io import BytesIO
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def save_equity_curve_html(curves: dict[str, pd.DataFrame], output_path: Path) -> None:
    output_path.write_text(_build_curve_html(curves, "Equity Curve", "equity"), encoding="utf-8")


def save_drawdown_curve_html(curves: dict[str, pd.DataFrame], output_path: Path) -> None:
    output_path.write_text(_build_curve_html(curves, "Drawdown Curve", "drawdown"), encoding="utf-8")


def _build_curve_html(curves: dict[str, pd.DataFrame], title: str, value_column: str) -> str:
    fig, ax = plt.subplots(figsize=(12, 6))
    for name, curve in curves.items():
        if curve.empty:
            continue
        ax.plot(pd.to_datetime(curve["timestamp"]), curve[value_column], label=name, linewidth=1.6)
    ax.set_title(title)
    ax.grid(alpha=0.2)
    ax.legend(loc="best", fontsize=8)
    fig.autofmt_xdate()
    plt.tight_layout()

    buffer = BytesIO()
    plt.savefig(buffer, format="png", dpi=150)
    plt.close(fig)
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        f"<title>{title}</title></head><body><h1>{title}</h1>"
        f"<img src='data:image/png;base64,{encoded}' alt='{title}' style='max-width:100%;'>"
        "</body></html>"
    )


def save_trade_chart(frame: pd.DataFrame, trade: pd.Series, output_path: Path) -> None:
    entry_index = int(trade["entry_bar_index"])
    exit_index = int(trade["exit_bar_index"])
    start = max(0, entry_index - 30)
    end = min(len(frame), exit_index + 11)
    window = frame.iloc[start:end].copy().reset_index(drop=True)
    dates = mdates.date2num(pd.to_datetime(window["timestamp"]).tolist())

    fig, ax = plt.subplots(figsize=(12, 6))
    width = 0.55
    for date_value, row in zip(dates, window.itertuples(index=False)):
        color = "#16a34a" if row.close >= row.open else "#dc2626"
        ax.vlines(date_value, row.low, row.high, color=color, linewidth=1.0)
        body_low = min(row.open, row.close)
        body_high = max(row.open, row.close)
        ax.add_patch(
            plt.Rectangle(
                (date_value - width / 2, body_low),
                width,
                max(body_high - body_low, 1e-6),
                facecolor=color,
                edgecolor=color,
                alpha=0.75,
            )
        )

    ax.plot(dates, window["ema21"], color="#2563eb", linewidth=1.4, label="EMA21")
    ax.plot(dates, window["ema55"], color="#7c3aed", linewidth=1.4, label="EMA55")

    entry_time = mdates.date2num(pd.to_datetime(trade["entry_time"]).to_pydatetime())
    exit_time = mdates.date2num(pd.to_datetime(trade["exit_time"]).to_pydatetime())
    ax.axhline(float(trade["entry_price"]), color="#0891b2", linestyle="--", linewidth=1.1, label="Entry")
    ax.axhline(float(trade["stop_loss_price"]), color="#ef4444", linestyle=":", linewidth=1.1, label="Stop")
    ax.scatter([entry_time], [float(trade["entry_price"])], color="#0891b2", marker="v", s=70, zorder=5)
    ax.scatter([exit_time], [float(trade["exit_price"])], color="#111827", marker="x", s=70, zorder=5)
    ax.set_title(f"{trade['strategy_name']} | R={float(trade['R_multiple']):.2f} | {trade['exit_reason']}")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax.grid(alpha=0.2)
    ax.legend(loc="best", fontsize=8)
    fig.autofmt_xdate()
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=140)
    plt.close(fig)
