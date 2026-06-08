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


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASENAME = f"bnb_doge_long_refine_followup_10u_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASENAME}.html"
CSV_PATH = REPORT_DIR / f"{BASENAME}.csv"
JSON_PATH = REPORT_DIR / f"{BASENAME}.json"
PROJECT_HTML_PATH = PROJECT_REPORT_DIR / "bnb_doge_long_refine_followup_10u.html"


@dataclass(frozen=True)
class Scenario:
    symbol: str
    label: str
    bar: str
    ema_period: int
    ema_type: str
    trend_ema_period: int
    trend_ema_type: str
    entry_reference_ema_period: int
    entry_reference_ema_type: str
    atr_stop_multiplier: Decimal

    @property
    def atr_take_multiplier(self) -> Decimal:
        return self.atr_stop_multiplier * Decimal("4")


SCENARIOS = (
    Scenario("BNB-USDT-SWAP", "BNB A | MA21 / MA55 / 挂单 MA55 | SL1", "1H", 21, "ma", 55, "ma", 55, "ma", Decimal("1")),
    Scenario("BNB-USDT-SWAP", "BNB A | MA21 / MA55 / 挂单 MA55 | SL1.5", "1H", 21, "ma", 55, "ma", 55, "ma", Decimal("1.5")),
    Scenario("BNB-USDT-SWAP", "BNB A | MA21 / MA55 / 挂单 MA55 | SL2", "1H", 21, "ma", 55, "ma", 55, "ma", Decimal("2")),
    Scenario("BNB-USDT-SWAP", "BNB B | EMA21 / MA55 / 挂单 EMA55 | SL1", "1H", 21, "ema", 55, "ma", 55, "ema", Decimal("1")),
    Scenario("BNB-USDT-SWAP", "BNB B | EMA21 / MA55 / 挂单 EMA55 | SL1.5", "1H", 21, "ema", 55, "ma", 55, "ema", Decimal("1.5")),
    Scenario("BNB-USDT-SWAP", "BNB B | EMA21 / MA55 / 挂单 EMA55 | SL2", "1H", 21, "ema", 55, "ma", 55, "ema", Decimal("2")),
    Scenario("DOGE-USDT-SWAP", "DOGE 1H | EMA21 / EMA55 / 挂单 EMA21 | SL1.5", "1H", 21, "ema", 55, "ema", 21, "ema", Decimal("1.5")),
    Scenario("DOGE-USDT-SWAP", "DOGE 1H | EMA21 / EMA55 / 挂单 EMA21 | SL2", "1H", 21, "ema", 55, "ema", 21, "ema", Decimal("2")),
    Scenario("DOGE-USDT-SWAP", "DOGE 1H | EMA21 / EMA55 / 挂单 EMA55 | SL1.5", "1H", 21, "ema", 55, "ema", 55, "ema", Decimal("1.5")),
    Scenario("DOGE-USDT-SWAP", "DOGE 1H | EMA21 / EMA55 / 挂单 EMA55 | SL2", "1H", 21, "ema", 55, "ema", 55, "ema", Decimal("2")),
    Scenario("DOGE-USDT-SWAP", "DOGE 15m | EMA8 / EMA21 / 挂单 EMA8 | SL1.5", "15m", 8, "ema", 21, "ema", 8, "ema", Decimal("1.5")),
    Scenario("DOGE-USDT-SWAP", "DOGE 15m | EMA8 / EMA21 / 挂单 EMA8 | SL2", "15m", 8, "ema", 21, "ema", 8, "ema", Decimal("2")),
    Scenario("DOGE-USDT-SWAP", "DOGE 15m | EMA13 / EMA34 / 挂单 EMA13 | SL1.5", "15m", 13, "ema", 34, "ema", 13, "ema", Decimal("1.5")),
    Scenario("DOGE-USDT-SWAP", "DOGE 15m | EMA13 / EMA34 / 挂单 EMA13 | SL2", "15m", 13, "ema", 34, "ema", 13, "ema", Decimal("2")),
)


def run_one(scenario: Scenario) -> dict[str, object]:
    sys.path.insert(0, str(ROOT))
    from okx_quant.backtest import _run_backtest_with_loaded_data
    from okx_quant.candle_cache import load_candle_cache
    from okx_quant.models import StrategyConfig
    from okx_quant.okx_client import OkxRestClient
    from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID
    from scripts.run_btc_daily_ma_direction_filter_research import (
        INITIAL_CAPITAL,
        LONG_MAKER_FEE_RATE,
        LONG_TAKER_FEE_RATE,
        RISK_AMOUNT,
        build_metrics,
        build_split_bounds,
        filter_split_trades,
        format_ts,
    )

    client = OkxRestClient()
    candles = [candle for candle in load_candle_cache(scenario.symbol, scenario.bar, limit=None) if candle.confirmed]
    bounds = build_split_bounds(len(candles))
    config = StrategyConfig(
        inst_id=scenario.symbol,
        bar=scenario.bar,
        ema_period=scenario.ema_period,
        ema_type=scenario.ema_type,
        trend_ema_period=scenario.trend_ema_period,
        trend_ema_type=scenario.trend_ema_type,
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=scenario.atr_stop_multiplier,
        atr_take_multiplier=scenario.atr_take_multiplier,
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
        entry_reference_ema_period=scenario.entry_reference_ema_period,
        entry_reference_ema_type=scenario.entry_reference_ema_type,
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
            client.get_instrument(scenario.symbol),
            config,
            data_source_note=f"local candle_cache full history | {scenario.symbol} {scenario.bar} | {scenario.label}",
            maker_fee_rate=LONG_MAKER_FEE_RATE,
            taker_fee_rate=LONG_TAKER_FEE_RATE,
        )
        trades = list(result.trades)
        test_trades = filter_split_trades(trades, bounds["test"])
        all_metrics = build_metrics(trades)
        test_metrics = build_metrics(test_trades)
        return {
            "symbol": "BNB" if scenario.symbol == "BNB-USDT-SWAP" else "DOGE",
            "label": scenario.label,
            "bar": scenario.bar,
            "all_pnl_u": float(all_metrics.pnl),
            "all_trades": all_metrics.trades,
            "all_win_rate_pct": float(all_metrics.win_rate),
            "all_profit_factor": None if all_metrics.profit_factor is None else float(all_metrics.profit_factor),
            "all_avg_r": float(all_metrics.avg_r),
            "all_drawdown_u": float(all_metrics.max_drawdown),
            "test_pnl_u": float(test_metrics.pnl),
            "test_trades": test_metrics.trades,
            "test_win_rate_pct": float(test_metrics.win_rate),
            "test_profit_factor": None if test_metrics.profit_factor is None else float(test_metrics.profit_factor),
            "test_avg_r": float(test_metrics.avg_r),
            "test_drawdown_u": float(test_metrics.max_drawdown),
            "start_utc": format_ts(candles[0].ts),
            "end_utc": format_ts(candles[-1].ts),
            "candles": len(candles),
            "error": "",
        }
    except RuntimeError as exc:
        return {
            "symbol": "BNB" if scenario.symbol == "BNB-USDT-SWAP" else "DOGE",
            "label": scenario.label,
            "bar": scenario.bar,
            "all_pnl_u": None,
            "all_trades": None,
            "all_win_rate_pct": None,
            "all_profit_factor": None,
            "all_avg_r": None,
            "all_drawdown_u": None,
            "test_pnl_u": None,
            "test_trades": None,
            "test_win_rate_pct": None,
            "test_profit_factor": None,
            "test_avg_r": None,
            "test_drawdown_u": None,
            "start_utc": "",
            "end_utc": "",
            "candles": None,
            "error": str(exc),
        }


def fig_to_base64(fig) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def build_chart(frame: pd.DataFrame):
    ranked = frame.sort_values(["symbol", "test_pnl_u"], ascending=[True, False]).copy()
    fig, ax = plt.subplots(figsize=(14, 6.8))
    x = range(len(ranked))
    width = 0.36
    ax.bar([i - width / 2 for i in x], ranked["all_pnl_u"], width=width, label="全样本", color="#2563eb")
    ax.bar([i + width / 2 for i in x], ranked["test_pnl_u"], width=width, label="测试段", color="#16a34a")
    ax.set_xticks(list(x))
    ax.set_xticklabels(ranked["label"], rotation=35, ha="right")
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
        futures = [executor.submit(run_one, scenario) for scenario in SCENARIOS]
        for future in as_completed(futures):
            rows.append(future.result())

    all_frame = pd.DataFrame(rows).sort_values(["symbol", "label"], ascending=[True, True]).reset_index(drop=True)
    valid_frame = all_frame[all_frame["error"] == ""].copy()
    valid_frame = valid_frame.sort_values(["symbol", "test_pnl_u", "all_pnl_u"], ascending=[True, False, False]).reset_index(drop=True)
    all_frame.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

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
    invalid_summary = (
        all_frame[all_frame["error"] != ""]
        .groupby("symbol")["error"]
        .count()
        .reset_index(name="invalid_count")
    )
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "scope": "BNB refine + DOGE alternate-style full-history study | 10U",
        "assumption": {
            "risk_amount": "10U",
            "take_profit_mode": "dynamic",
            "dynamic_two_r_break_even": True,
            "dynamic_fee_offset_enabled": True,
            "max_entries_per_trend": 1,
            "bnb_note": "围绕 BNB 两套最优老骨架，只补 SL1 / 1.5 / 2。",
            "doge_note": "DOGE 不再硬套老 1H 骨架，增加 15m 快节奏和更宽止损的尝试。",
        },
        "best_test": best_test.to_dict("records"),
        "best_all": best_all.to_dict("records"),
        "invalid_summary": invalid_summary.to_dict("records"),
        "rows": all_frame.to_dict("records"),
    }
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    chart = fig_to_base64(build_chart(valid_frame))
    html_text = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>BNB / DOGE 做多补测</title>
  <style>
    body {{ margin: 0; font-family: "Microsoft YaHei", "Segoe UI", sans-serif; background: #f6f7f3; color: #1f2937; }}
    .wrap {{ max-width: 1450px; margin: 0 auto; padding: 28px 24px 48px; }}
    .hero {{ background: #17202a; color: white; padding: 24px 28px; border-radius: 12px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; margin-top: 18px; }}
    .card {{ background: white; border: 1px solid #dde3dc; border-radius: 10px; padding: 16px; }}
    .label {{ color: #667085; font-size: 13px; }}
    .value {{ font-size: 24px; font-weight: 700; margin-top: 6px; }}
    h1, h2 {{ margin: 0 0 12px; }}
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
      <h1>BNB / DOGE 做多补测</h1>
      <p>BNB：围绕前一轮最优骨架，只补 `SL1 / 1.5 / 2`。DOGE：不再硬套旧 `1H` 骨架，转去测更宽止损和更快周期风格。</p>
      <p>统一口径：全历史、10U 固定风险、动态止盈、2R 保本、手续费偏移开启、每波最多开仓 1 次。</p>
    </section>

    <div class="grid">
      <div class="card"><div class="label">BNB 测试段最优</div><div class="value">{html.escape(best_test.loc[best_test["symbol"] == "BNB", "label"].iloc[0])}</div></div>
      <div class="card"><div class="label">BNB 测试段PnL</div><div class="value">{best_test.loc[best_test["symbol"] == "BNB", "test_pnl_u"].iloc[0]:,.2f}U</div></div>
      <div class="card"><div class="label">DOGE 测试段最优</div><div class="value">{html.escape(best_test.loc[best_test["symbol"] == "DOGE", "label"].iloc[0])}</div></div>
      <div class="card"><div class="label">DOGE 测试段PnL</div><div class="value">{best_test.loc[best_test["symbol"] == "DOGE", "test_pnl_u"].iloc[0]:,.2f}U</div></div>
    </div>

    <h2>测试段最佳</h2>
    {dataframe_to_html(best_test)}

    <h2>全样本最佳</h2>
    {dataframe_to_html(best_all)}

    <h2>图表</h2>
    <img alt="followup chart" src="data:image/png;base64,{chart}">

    <h2>有效组合明细</h2>
    {dataframe_to_html(valid_frame)}

    <h2>无效组合统计</h2>
    {dataframe_to_html(invalid_summary)}

    <h2>说明</h2>
    <div class="card note">
      <p>如果 DOGE 在换成更快周期或更宽止损后仍明显弱于 BNB，就基本可以判断：它不适合放进这类 EMA 动态委托做多候选池。</p>
      <p>如果 BNB 的最优组合和上一轮相比变化不大，说明它的问题不在于止损倍数没有扫到，而在于币性上限本来就有限。</p>
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
