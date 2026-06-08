from __future__ import annotations

import base64
import html
import io
import json
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from shutil import copyfile

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.persistence import analysis_report_dir_path
from scripts.run_btc_daily_ma_direction_filter_research import ENTRY_BAR, INITIAL_CAPITAL


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASENAME = f"bnb_doge_old_style_long_combo_study_10u_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASENAME}.html"
CSV_PATH = REPORT_DIR / f"{BASENAME}.csv"
JSON_PATH = REPORT_DIR / f"{BASENAME}.json"
PROJECT_HTML_PATH = PROJECT_REPORT_DIR / "bnb_doge_old_style_long_combo_study_10u.html"

SYMBOLS = ("BNB-USDT-SWAP", "DOGE-USDT-SWAP")
SYMBOL_LABELS = {
    "BNB-USDT-SWAP": "BNB",
    "DOGE-USDT-SWAP": "DOGE",
}


@dataclass(frozen=True)
class Combo:
    fast_type: str
    trend_type: str
    entry_period: int
    entry_type: str

    @property
    def key(self) -> str:
        return f"fast_{self.fast_type}_trend_{self.trend_type}_entry_{self.entry_type}{self.entry_period}"

    @property
    def label(self) -> str:
        return f"{self.fast_type.upper()}21 / {self.trend_type.upper()}55 / 挂单 {self.entry_type.upper()}{self.entry_period}"


COMBOS = tuple(
    Combo(fast_type=fast_type, trend_type=trend_type, entry_period=entry_period, entry_type=entry_type)
    for fast_type in ("ema", "ma")
    for trend_type in ("ema", "ma")
    for entry_period in (21, 55)
    for entry_type in ("ema", "ma")
)


def run_one(symbol: str, combo: Combo) -> dict[str, object]:
    sys.path.insert(0, str(ROOT))
    from okx_quant.backtest import _run_backtest_with_loaded_data
    from okx_quant.candle_cache import load_candle_cache
    from okx_quant.models import StrategyConfig
    from okx_quant.okx_client import OkxRestClient
    from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID
    from scripts.run_btc_daily_ma_direction_filter_research import (
        LONG_MAKER_FEE_RATE,
        LONG_TAKER_FEE_RATE,
        RISK_AMOUNT,
        build_metrics,
        build_split_bounds,
        filter_split_trades,
        format_ts,
    )

    client = OkxRestClient()
    candles = [candle for candle in load_candle_cache(symbol, ENTRY_BAR, limit=None) if candle.confirmed]
    bounds = build_split_bounds(len(candles))
    config = StrategyConfig(
        inst_id=symbol,
        bar=ENTRY_BAR,
        ema_period=21,
        ema_type=combo.fast_type,
        trend_ema_period=55,
        trend_ema_type=combo.trend_type,
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=Decimal("1"),
        atr_take_multiplier=Decimal("4"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        risk_amount=RISK_AMOUNT,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        entry_reference_ema_period=combo.entry_period,
        entry_reference_ema_type=combo.entry_type,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=0,
    )
    try:
        result = _run_backtest_with_loaded_data(
            candles,
            client.get_instrument(symbol),
            config,
            data_source_note=f"local candle_cache full history | {symbol} {ENTRY_BAR} old-style combo {combo.label}",
            maker_fee_rate=LONG_MAKER_FEE_RATE,
            taker_fee_rate=LONG_TAKER_FEE_RATE,
        )
        trades = list(result.trades)
        test_trades = filter_split_trades(trades, bounds["test"])
        all_metrics = build_metrics(trades)
        test_metrics = build_metrics(test_trades)
        error = ""
    except RuntimeError as exc:
        trades = []
        test_trades = []
        all_metrics = None
        test_metrics = None
        error = str(exc)
    return {
        "symbol": SYMBOL_LABELS[symbol],
        "combo_key": combo.key,
        "combo_label": combo.label,
        "start_utc": format_ts(candles[0].ts),
        "end_utc": format_ts(candles[-1].ts),
        "candles": len(candles),
        "all_pnl_u": None if all_metrics is None else float(all_metrics.pnl),
        "all_trades": None if all_metrics is None else all_metrics.trades,
        "all_win_rate_pct": None if all_metrics is None else float(all_metrics.win_rate),
        "all_profit_factor": None if all_metrics is None or all_metrics.profit_factor is None else float(all_metrics.profit_factor),
        "all_avg_r": None if all_metrics is None else float(all_metrics.avg_r),
        "all_drawdown_u": None if all_metrics is None else float(all_metrics.max_drawdown),
        "test_pnl_u": None if test_metrics is None else float(test_metrics.pnl),
        "test_trades": None if test_metrics is None else test_metrics.trades,
        "test_win_rate_pct": None if test_metrics is None else float(test_metrics.win_rate),
        "test_profit_factor": None if test_metrics is None or test_metrics.profit_factor is None else float(test_metrics.profit_factor),
        "test_avg_r": None if test_metrics is None else float(test_metrics.avg_r),
        "test_drawdown_u": None if test_metrics is None else float(test_metrics.max_drawdown),
        "error": error,
    }


def fig_to_base64(fig) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def build_chart(frame: pd.DataFrame):
    ranked = (
        frame.sort_values(["symbol", "test_pnl_u"], ascending=[True, False])
        .groupby("symbol", as_index=False)
        .head(5)
        .copy()
    )
    labels = [f"{row.symbol} {row.combo_label}" for row in ranked.itertuples()]
    fig, ax = plt.subplots(figsize=(13, 6.5))
    x = range(len(ranked))
    width = 0.36
    ax.bar([i - width / 2 for i in x], ranked["all_pnl_u"], width=width, label="全样本", color="#2563eb")
    ax.bar([i + width / 2 for i in x], ranked["test_pnl_u"], width=width, label="测试段", color="#16a34a")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=35, ha="right")
    ax.set_ylabel("PnL (U)")
    ax.axhline(0, color="#475467", linewidth=1)
    ax.grid(axis="y", alpha=0.2)
    ax.legend()
    fig.tight_layout()
    return fig


def dataframe_to_html(frame: pd.DataFrame) -> str:
    display = frame.copy()
    for column in display.columns:
        if pd.api.types.is_float_dtype(display[column]):
            display[column] = display[column].map(lambda value: "" if pd.isna(value) else f"{value:,.2f}")
    return display.to_html(index=False, escape=False)


def main() -> None:
    rows: list[dict[str, object]] = []
    with ProcessPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(run_one, symbol, combo) for symbol in SYMBOLS for combo in COMBOS]
        for future in as_completed(futures):
            rows.append(future.result())

    frame = pd.DataFrame(rows)
    valid_frame = frame[frame["error"] == ""].copy()
    valid_frame = valid_frame.sort_values(["symbol", "test_pnl_u", "all_pnl_u"], ascending=[True, False, False]).reset_index(drop=True)
    frame.sort_values(["symbol", "combo_label"], ascending=[True, True]).to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

    best_test = (
        valid_frame.sort_values(["symbol", "test_pnl_u", "all_pnl_u"], ascending=[True, False, False])
        .groupby("symbol", as_index=False)
        .head(1)
        .reset_index(drop=True)
    )
    best_all = (
        valid_frame.sort_values(["symbol", "all_pnl_u", "test_pnl_u"], ascending=[True, False, False])
        .groupby("symbol", as_index=False)
        .head(1)
        .reset_index(drop=True)
    )
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "scope": "BNB + DOGE | old-style EMA dynamic long combo study | full history | 10U",
        "assumption": {
            "bar": ENTRY_BAR,
            "risk_amount": "10U",
            "atr_period": 10,
            "atr_stop_multiplier": "1",
            "atr_take_multiplier": "4",
            "take_profit_mode": "dynamic",
            "max_entries_per_trend": 1,
            "dynamic_two_r_break_even": True,
            "dynamic_fee_offset_enabled": True,
            "sweep": "快线类型 EMA/MA × 趋势线类型 EMA/MA × 挂单 21/55 × 挂单类型 EMA/MA，共 16 组",
        },
        "best_test": best_test.to_dict("records"),
        "best_all": best_all.to_dict("records"),
        "rows": frame.sort_values(["symbol", "combo_label"], ascending=[True, True]).to_dict("records"),
    }
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    chart = fig_to_base64(build_chart(valid_frame))
    bnb_best_test = best_test.loc[best_test["symbol"] == "BNB"]
    doge_best_test = best_test.loc[best_test["symbol"] == "DOGE"]
    bnb_label = "无有效组合" if bnb_best_test.empty else bnb_best_test["combo_label"].iloc[0]
    bnb_test_pnl = "" if bnb_best_test.empty else f'{bnb_best_test["test_pnl_u"].iloc[0]:,.2f}U'
    doge_label = "无有效组合" if doge_best_test.empty else doge_best_test["combo_label"].iloc[0]
    doge_test_pnl = "" if doge_best_test.empty else f'{doge_best_test["test_pnl_u"].iloc[0]:,.2f}U'
    invalid_summary = (
        frame[frame["error"] != ""]
        .groupby("symbol")["error"]
        .count()
        .reset_index(name="invalid_combo_count")
    )
    html_text = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>BNB / DOGE 老风格做多组合研究</title>
  <style>
    body {{ margin: 0; font-family: "Microsoft YaHei", "Segoe UI", sans-serif; background: #f6f7f3; color: #1f2937; }}
    .wrap {{ max-width: 1450px; margin: 0 auto; padding: 28px 24px 48px; }}
    .hero {{ background: #17202a; color: white; padding: 24px 28px; border-radius: 12px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; margin-top: 18px; }}
    .card {{ background: white; border: 1px solid #dde3dc; border-radius: 10px; padding: 16px; }}
    .label {{ color: #667085; font-size: 13px; }}
    .value {{ font-size: 26px; font-weight: 700; margin-top: 6px; }}
    h1, h2, h3 {{ margin: 0 0 12px; }}
    h2 {{ margin-top: 28px; }}
    table {{ width: 100%; border-collapse: collapse; background: white; margin-top: 12px; border-radius: 8px; overflow: hidden; }}
    th, td {{ padding: 9px 10px; border-bottom: 1px solid #e5e7eb; text-align: right; white-space: nowrap; font-size: 13px; }}
    th:first-child, td:first-child {{ text-align: left; }}
    th {{ background: #e9eee8; color: #344054; }}
    img {{ width: 100%; border-radius: 8px; border: 1px solid #dde3dc; background: white; }}
    .note {{ color: #667085; line-height: 1.8; }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>BNB / DOGE 老风格 EMA 动态做多组合研究</h1>
      <p>口径：1H、全历史、10U 固定风险、ATR10、SL1、动态止盈、2R 保本、手续费偏移开启、每波最多开仓 1 次。</p>
      <p>扫描范围：快线类型 EMA/MA × 趋势线类型 EMA/MA × 挂单 21/55 × 挂单类型 EMA/MA，共 16 组。</p>
      <p>目标：补齐 5 月前后没被纳入正式候选的 BNB / DOGE 做多组合，看它们当时为什么没被选中。</p>
    </section>

    <div class="grid">
      <div class="card"><div class="label">BNB 测试段最优</div><div class="value">{html.escape(bnb_label)}</div></div>
      <div class="card"><div class="label">BNB 测试段PnL</div><div class="value">{bnb_test_pnl}</div></div>
      <div class="card"><div class="label">DOGE 测试段最优</div><div class="value">{html.escape(doge_label)}</div></div>
      <div class="card"><div class="label">DOGE 测试段PnL</div><div class="value">{doge_test_pnl}</div></div>
    </div>

    <h2>测试段最佳组合</h2>
    {dataframe_to_html(best_test)}

    <h2>全样本最佳组合</h2>
    {dataframe_to_html(best_all)}

    <h2>Top 组合图</h2>
    <img alt="combo chart" src="data:image/png;base64,{chart}">

    <h2>全部组合明细</h2>
    {dataframe_to_html(valid_frame)}

    <h2>无效组合统计</h2>
    {dataframe_to_html(invalid_summary)}

    <h2>说明</h2>
    <div class="card note">
      <p>这次只扫结构组合，不额外混入 SL1.5 / SL2 或不同开仓次数，目的是先判断 BNB / DOGE 在 5 月那套老做多体系里有没有“能打的骨架”。</p>
      <p>如果其中某个币已经明显跑不赢，通常说明它不是“参数没调到”，而是币性和这套做多结构匹配度本身就不高。</p>
    </div>
  </div>
</body>
</html>"""
    HTML_PATH.write_text(html_text, encoding="utf-8")
    copyfile(HTML_PATH, PROJECT_HTML_PATH)
    print(HTML_PATH)
    print(PROJECT_HTML_PATH)


if __name__ == "__main__":
    main()
