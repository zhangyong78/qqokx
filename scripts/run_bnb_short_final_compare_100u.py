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
from okx_quant.strategy_catalog import STRATEGY_BODY_RETEST_SHORT_ID, STRATEGY_EMA55_SLOPE_SHORT_ID
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
BASENAME = f"bnb_short_final_compare_100u_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASENAME}.html"
SUMMARY_CSV_PATH = REPORT_DIR / f"{BASENAME}_summary.csv"
YEARLY_CSV_PATH = REPORT_DIR / f"{BASENAME}_yearly.csv"
MONTHLY_CSV_PATH = REPORT_DIR / f"{BASENAME}_monthly.csv"
PROJECT_HTML_PATH = PROJECT_REPORT_DIR / "bnb_short_final_compare_100u.html"

SYMBOL = "BNB-USDT-SWAP"
RISK_AMOUNT = Decimal("100")
INITIAL_CAPITAL = Decimal("10000")


@dataclass(frozen=True)
class Variant:
    key: str
    label: str
    strategy_id: str
    ema_type: str
    ema_period: int
    trend_ema_type: str
    trend_ema_period: int
    daily_filter_enabled: bool
    daily_filter_mode: str
    daily_filter_ma_type: str
    daily_filter_period: int
    body_retest_enabled: bool = False


VARIANTS = (
    Variant(
        key="slope_ma34_no_filter",
        label="MA34 斜率做空 | 不加日线过滤",
        strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
        ema_type="ma",
        ema_period=34,
        trend_ema_type="ma",
        trend_ema_period=34,
        daily_filter_enabled=False,
        daily_filter_mode="disabled",
        daily_filter_ma_type="ema",
        daily_filter_period=21,
    ),
    Variant(
        key="body_retest_no_filter",
        label="Body/ATR 回抽做空 | 不加日线过滤",
        strategy_id=STRATEGY_BODY_RETEST_SHORT_ID,
        ema_type="ma",
        ema_period=20,
        trend_ema_type="ma",
        trend_ema_period=20,
        daily_filter_enabled=False,
        daily_filter_mode="disabled",
        daily_filter_ma_type="ema",
        daily_filter_period=21,
        body_retest_enabled=True,
    ),
    Variant(
        key="body_retest_weak_day",
        label="Body/ATR 回抽做空 | Weak Day",
        strategy_id=STRATEGY_BODY_RETEST_SHORT_ID,
        ema_type="ma",
        ema_period=20,
        trend_ema_type="ma",
        trend_ema_period=20,
        daily_filter_enabled=True,
        daily_filter_mode="weak_day",
        daily_filter_ma_type="ema",
        daily_filter_period=5,
        body_retest_enabled=True,
    ),
)


def build_config(variant: Variant) -> StrategyConfig:
    return StrategyConfig(
        inst_id=SYMBOL,
        bar="1H",
        ema_period=variant.ema_period,
        ema_type=variant.ema_type,
        trend_ema_period=variant.trend_ema_period,
        trend_ema_type=variant.trend_ema_type,
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
        strategy_id=variant.strategy_id,
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
        daily_filter_enabled=variant.daily_filter_enabled,
        daily_filter_bar="1D" if variant.daily_filter_enabled else None,
        daily_filter_boundary="bjt_08",
        daily_filter_mode=variant.daily_filter_mode,
        daily_filter_scope="short_only",
        daily_filter_ma_type=variant.daily_filter_ma_type,
        daily_filter_period=variant.daily_filter_period,
        body_retest_breakdown_atr_multiplier=Decimal("0.2") if variant.body_retest_enabled else Decimal("0.2"),
        body_retest_retest_atr_multiplier=Decimal("0.3") if variant.body_retest_enabled else Decimal("0.3"),
        body_retest_stop_buffer_atr_multiplier=Decimal("0.3") if variant.body_retest_enabled else Decimal("0.3"),
        body_retest_body_atr_limit=Decimal("1.0") if variant.body_retest_enabled else Decimal("1.0"),
        body_retest_watch_bars=6 if variant.body_retest_enabled else 6,
    )


def none_or_float(value) -> float | None:
    if value is None:
        return None
    return float(value)


def build_yearly_rows(label: str, trades: list) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    by_year: dict[str, list] = {}
    for trade in trades:
        year = pd.to_datetime(int(trade.exit_ts), unit="ms", utc=True).strftime("%Y")
        by_year.setdefault(year, []).append(trade)
    for year in sorted(by_year):
        metrics = build_metrics(by_year[year])
        rows.append(
            {
                "方案": label,
                "年份": year,
                "利润": float(metrics.pnl),
                "交易数": metrics.trades,
                "胜率": float(metrics.win_rate),
                "PF": none_or_float(metrics.profit_factor),
                "AvgR": float(metrics.avg_r),
                "回撤": float(metrics.max_drawdown),
            }
        )
    return rows


def build_monthly_rows(label: str, trades: list) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    month_map: dict[str, list] = {}
    for trade in trades:
        month = pd.to_datetime(int(trade.exit_ts), unit="ms", utc=True).strftime("%Y-%m")
        month_map.setdefault(month, []).append(trade)
    for month in sorted(month_map):
        metrics = build_metrics(month_map[month])
        rows.append(
            {
                "方案": label,
                "月份": month,
                "利润": float(metrics.pnl),
                "交易数": metrics.trades,
                "胜率": float(metrics.win_rate),
                "PF": none_or_float(metrics.profit_factor),
                "AvgR": float(metrics.avg_r),
                "回撤": float(metrics.max_drawdown),
            }
        )
    return rows


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


def main() -> None:
    candles = [c for c in load_candle_cache(SYMBOL, "1H", limit=None) if c.confirmed]
    if not candles:
        raise RuntimeError(f"missing local candles for {SYMBOL} 1H")
    client = OkxRestClient()
    instrument = client.get_instrument(SYMBOL)
    bounds = build_split_bounds(len(candles))["test"]

    summary_rows: list[dict[str, object]] = []
    yearly_rows: list[dict[str, object]] = []
    monthly_rows: list[dict[str, object]] = []

    for variant in VARIANTS:
        result = _run_backtest_with_loaded_data(
            candles,
            instrument,
            build_config(variant),
            data_source_note=f"local candle_cache full history | {SYMBOL} short final compare 100u",
            taker_fee_rate=SHORT_TAKER_FEE_RATE,
        )
        trades = list(result.trades)
        test_trades = filter_split_trades(trades, bounds)
        all_metrics = build_metrics(trades)
        test_metrics = build_metrics(test_trades)
        yearly_rows.extend(build_yearly_rows(variant.label, trades))
        monthly_rows.extend(build_monthly_rows(variant.label, trades))
        summary_rows.append(
            {
                "方案": variant.label,
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
            }
        )

    summary_frame = pd.DataFrame(summary_rows)
    yearly_frame = pd.DataFrame(yearly_rows)
    monthly_frame = pd.DataFrame(monthly_rows)

    summary_frame.to_csv(SUMMARY_CSV_PATH, index=False, encoding="utf-8-sig")
    yearly_frame.to_csv(YEARLY_CSV_PATH, index=False, encoding="utf-8-sig")
    monthly_frame.to_csv(MONTHLY_CSV_PATH, index=False, encoding="utf-8-sig")

    best_test = summary_frame.sort_values(["测试段利润", "全样本利润"], ascending=False).iloc[0]["方案"]
    best_all = summary_frame.sort_values(["全样本利润", "测试段利润"], ascending=False).iloc[0]["方案"]
    best_dd = summary_frame.sort_values(["全样本回撤", "测试段回撤"], ascending=True).iloc[0]["方案"]
    html_text = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>BNB 做空最终对比 100U</title>
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
    <h1>BNB 做空最终对比</h1>
    <p>口径：<strong>1H / 100U 固定风险 / 全历史 / 正式主回测接口</strong>。这次只比较 BNB 当前最值得保留的做空候选。</p>
    <p><strong>结论提示：</strong>测试段利润最高的是 <strong>{html.escape(str(best_test))}</strong>；全样本利润最高的是 <strong>{html.escape(str(best_all))}</strong>；回撤最低的是 <strong>{html.escape(str(best_dd))}</strong>。</p>
  </div>
  <div class="card">
    <h2>总览</h2>
    {dataframe_to_html(summary_frame, money_cols={"全样本利润", "全样本回撤", "测试段利润", "测试段回撤"}, pct_cols={"全样本胜率", "测试段胜率"}, float_cols={"全样本PF", "全样本AvgR", "测试段PF", "测试段AvgR"})}
  </div>
  <div class="card">
    <h2>年度明细</h2>
    {dataframe_to_html(yearly_frame, money_cols={"利润", "回撤"}, pct_cols={"胜率"}, float_cols={"PF", "AvgR"})}
  </div>
  <div class="card">
    <h2>月度明细</h2>
    {dataframe_to_html(monthly_frame, money_cols={"利润", "回撤"}, pct_cols={"胜率"}, float_cols={"PF", "AvgR"})}
  </div>
</body>
</html>
"""
    HTML_PATH.write_text(html_text, encoding="utf-8")
    PROJECT_HTML_PATH.write_text(html_text, encoding="utf-8")
    print(HTML_PATH)


if __name__ == "__main__":
    main()
