from __future__ import annotations

import html
import sys
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import _run_backtest_with_loaded_data
from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.strategy_catalog import STRATEGY_EMA55_SLOPE_SHORT_ID
from scripts.run_btc_daily_ma_direction_filter_research import (
    SHORT_TAKER_FEE_RATE,
    build_metrics,
    build_split_bounds,
    filter_split_trades,
    format_ts,
)


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASENAME = f"eth_short_daily_filter_drawdown_study_100u_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASENAME}.html"
SUMMARY_CSV_PATH = REPORT_DIR / f"{BASENAME}_summary.csv"
YEARLY_CSV_PATH = REPORT_DIR / f"{BASENAME}_yearly.csv"
MONTHLY_2023_CSV_PATH = REPORT_DIR / f"{BASENAME}_monthly_2023.csv"
PROJECT_HTML_PATH = PROJECT_REPORT_DIR / "eth_short_daily_filter_drawdown_study_100u.html"

SYMBOL = "ETH-USDT-SWAP"
RISK_AMOUNT = Decimal("100")
INITIAL_CAPITAL = Decimal("10000")


@dataclass(frozen=True)
class FilterSpec:
    key: str
    label: str
    enabled: bool
    ma_type: str
    period: int


FILTER_SPECS = (
    FilterSpec("none", "不加日线过滤", False, "ema", 21),
    FilterSpec("ema21", "北京时间8点日线 EMA21", True, "ema", 21),
    FilterSpec("ma20", "北京时间8点日线 MA20", True, "ma", 20),
    FilterSpec("ema34", "北京时间8点日线 EMA34", True, "ema", 34),
    FilterSpec("ma34", "北京时间8点日线 MA34", True, "ma", 34),
    FilterSpec("ema55", "北京时间8点日线 EMA55", True, "ema", 55),
    FilterSpec("ma55", "北京时间8点日线 MA55", True, "ma", 55),
)


def build_config(spec: FilterSpec) -> StrategyConfig:
    return StrategyConfig(
        inst_id=SYMBOL,
        bar="1H",
        ema_period=34,
        ema_type="ma",
        trend_ema_period=34,
        trend_ema_type="ma",
        big_ema_period=233,
        atr_period=14,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("4"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="short_only",
        position_mode="net",
        environment="live",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
        risk_amount=RISK_AMOUNT,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        take_profit_mode="dynamic",
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        ema55_slope_exit_enabled=False,
        atr_percentile_filter_max=Decimal("0.5"),
        trend_ema_slope_filter_min_ratio=Decimal("-0.0005"),
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=10,
        daily_filter_enabled=spec.enabled,
        daily_filter_bar="1D" if spec.enabled else None,
        daily_filter_boundary="bjt_08",
        daily_filter_mode="close_vs_ma" if spec.enabled else "disabled",
        daily_filter_scope="short_only",
        daily_filter_ma_type=spec.ma_type,
        daily_filter_period=spec.period,
    )


def none_or_float(value) -> float | None:
    if value is None:
        return None
    return float(value)


def format_u(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:,.2f}U"


def format_pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:.2f}%"


def format_float(value: float | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:.{digits}f}"


def dataframe_to_html(df: pd.DataFrame, *, money_cols: set[str] | None = None, pct_cols: set[str] | None = None, float_cols: set[str] | None = None) -> str:
    money_cols = money_cols or set()
    pct_cols = pct_cols or set()
    float_cols = float_cols or set()
    headers = "".join(f"<th>{html.escape(str(col))}</th>" for col in df.columns)
    body_rows: list[str] = []
    for _, row in df.iterrows():
        cells: list[str] = []
        for col in df.columns:
            value = row[col]
            if col in money_cols:
                text = format_u(float(value) if value is not None else None)
            elif col in pct_cols:
                text = format_pct(float(value) if value is not None else None)
            elif col in float_cols:
                text = format_float(float(value) if value is not None else None)
            elif pd.isna(value):
                text = "-"
            else:
                text = str(value)
            cells.append(f"<td>{html.escape(text)}</td>")
        body_rows.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{headers}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"


def build_year_frame(trades: list, year: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for trade in trades:
        exit_dt = pd.to_datetime(int(trade.exit_ts), unit="ms", utc=True)
        if exit_dt.strftime("%Y") != year:
            continue
        rows.append(
            {
                "month": exit_dt.strftime("%Y-%m"),
                "pnl_u": float(trade.pnl),
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    candles = [c for c in load_candle_cache(SYMBOL, "1H", limit=None) if c.confirmed]
    if not candles:
        raise RuntimeError(f"missing local candles for {SYMBOL} 1H")
    client = OkxRestClient()
    instrument = client.get_instrument(SYMBOL)
    bounds = build_split_bounds(len(candles))["test"]

    summary_rows: list[dict[str, object]] = []
    yearly_rows: list[dict[str, object]] = []
    monthly_2023_rows: list[dict[str, object]] = []

    for spec in FILTER_SPECS:
        result = _run_backtest_with_loaded_data(
            candles,
            instrument,
            build_config(spec),
            data_source_note=f"local candle_cache full history | {SYMBOL} MA34 short daily filter study 100u",
            taker_fee_rate=SHORT_TAKER_FEE_RATE,
        )
        trades = list(result.trades)
        test_trades = filter_split_trades(trades, bounds)
        all_metrics = build_metrics(trades)
        test_metrics = build_metrics(test_trades)

        for year in ("2022", "2023", "2024", "2025", "2026"):
            year_trades = [t for t in trades if pd.to_datetime(int(t.exit_ts), unit="ms", utc=True).strftime("%Y") == year]
            year_metrics = build_metrics(year_trades)
            yearly_rows.append(
                {
                    "日线过滤": spec.label,
                    "年份": year,
                    "利润": float(year_metrics.pnl),
                    "交易数": year_metrics.trades,
                    "胜率": float(year_metrics.win_rate),
                    "PF": none_or_float(year_metrics.profit_factor),
                    "AvgR": float(year_metrics.avg_r),
                    "回撤": float(year_metrics.max_drawdown),
                }
            )

        monthly_2023 = build_year_frame(trades, "2023")
        if not monthly_2023.empty:
            month_group = (
                monthly_2023.groupby("month", as_index=False)
                .agg(利润=("pnl_u", "sum"), 交易数=("pnl_u", "size"))
                .sort_values("month")
                .reset_index(drop=True)
            )
            month_group["日线过滤"] = spec.label
            monthly_2023_rows.extend(month_group[["日线过滤", "month", "利润", "交易数"]].rename(columns={"month": "月份"}).to_dict("records"))

        year_2023 = next(row for row in yearly_rows[::-1] if row["日线过滤"] == spec.label and row["年份"] == "2023")
        summary_rows.append(
            {
                "日线过滤": spec.label,
                "开始时间": format_ts(candles[0].ts),
                "结束时间": format_ts(candles[-1].ts),
                "K线数": len(candles),
                "全样本利润": float(all_metrics.pnl),
                "全样本交易数": all_metrics.trades,
                "全样本胜率": float(all_metrics.win_rate),
                "全样本PF": none_or_float(all_metrics.profit_factor),
                "全样本AvgR": float(all_metrics.avg_r),
                "全样本回撤": float(all_metrics.max_drawdown),
                "测试段利润": float(test_metrics.pnl),
                "测试段交易数": test_metrics.trades,
                "测试段胜率": float(test_metrics.win_rate),
                "测试段PF": none_or_float(test_metrics.profit_factor),
                "测试段AvgR": float(test_metrics.avg_r),
                "测试段回撤": float(test_metrics.max_drawdown),
                "2023利润": year_2023["利润"],
                "2023交易数": year_2023["交易数"],
                "2023胜率": year_2023["胜率"],
                "2023PF": year_2023["PF"],
                "2023AvgR": year_2023["AvgR"],
                "2023回撤": year_2023["回撤"],
            }
        )

    summary_frame = pd.DataFrame(summary_rows)
    yearly_frame = pd.DataFrame(yearly_rows)
    monthly_2023_frame = pd.DataFrame(monthly_2023_rows)

    summary_frame.to_csv(SUMMARY_CSV_PATH, index=False, encoding="utf-8-sig")
    yearly_frame.to_csv(YEARLY_CSV_PATH, index=False, encoding="utf-8-sig")
    monthly_2023_frame.to_csv(MONTHLY_2023_CSV_PATH, index=False, encoding="utf-8-sig")

    identical_2023 = summary_frame["2023回撤"].round(10).nunique() == 1 and summary_frame["2023利润"].round(10).nunique() == 1 and summary_frame["2023交易数"].nunique() == 1
    html_text = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>ETH 做空日线过滤回撤研究 100U</title>
  <style>
    body {{
      margin: 0;
      padding: 24px;
      background: #f7f4ee;
      color: #1f2328;
      font-family: "Microsoft YaHei", sans-serif;
      line-height: 1.6;
    }}
    .card {{
      background: #fffdf8;
      border: 1px solid #e6dccb;
      border-radius: 14px;
      padding: 18px 20px;
      margin-bottom: 18px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: #fff;
    }}
    th, td {{
      border: 1px solid #e8dfd2;
      padding: 10px 12px;
      text-align: right;
      white-space: nowrap;
    }}
    th:first-child, td:first-child {{ text-align: left; }}
    thead th {{ background: #f1e7d7; }}
  </style>
</head>
<body>
  <div class="card">
    <h1>ETH 做空日线过滤回撤研究</h1>
    <p>研究对象：<strong>ETH MA34 斜率做空</strong>，统一口径为 <strong>1H / 100U 固定风险 / ATR14 止损2 / 动态止盈 / 2R保本</strong>。</p>
    <p>这轮重点只看一个问题：不同日线过滤是否能减少 <strong>2023 年</strong> 的回撤。</p>
    <p><strong>结论：</strong>{"2023 年利润、交易数、回撤在所有过滤版本下完全一致，当前实现下日线过滤对 ETH 做空没有减回撤作用。" if identical_2023 else "不同过滤版本之间存在差异，请以下表为准。"}</p>
  </div>
  <div class="card">
    <h2>总览</h2>
    {dataframe_to_html(summary_frame, money_cols={"全样本利润", "全样本回撤", "测试段利润", "测试段回撤", "2023利润", "2023回撤"}, pct_cols={"全样本胜率", "测试段胜率", "2023胜率"}, float_cols={"全样本PF", "全样本AvgR", "测试段PF", "测试段AvgR", "2023PF", "2023AvgR"})}
  </div>
  <div class="card">
    <h2>年度明细</h2>
    {dataframe_to_html(yearly_frame, money_cols={"利润", "回撤"}, pct_cols={"胜率"}, float_cols={"PF", "AvgR"})}
  </div>
  <div class="card">
    <h2>2023 月度明细</h2>
    {dataframe_to_html(monthly_2023_frame, money_cols={"利润"})}
  </div>
</body>
</html>
"""
    HTML_PATH.write_text(html_text, encoding="utf-8")
    PROJECT_HTML_PATH.write_text(html_text, encoding="utf-8")
    print(HTML_PATH)


if __name__ == "__main__":
    main()
