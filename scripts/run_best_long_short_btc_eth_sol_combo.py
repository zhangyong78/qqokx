from __future__ import annotations

import base64
import html
import io
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import _run_backtest_with_loaded_data
from okx_quant.candle_cache import load_candle_cache
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from scripts.build_best_parameter_bundle import (
    SHORT_SPECS,
    SPECS,
    build_dynamic_long_config,
    build_slope_short_config,
)
from scripts.run_btc_daily_ma_direction_filter_research import (
    INITIAL_CAPITAL,
    LONG_MAKER_FEE_RATE,
    LONG_TAKER_FEE_RATE,
    SHORT_TAKER_FEE_RATE,
    build_metrics,
    build_split_bounds,
    filter_split_trades,
    format_ts,
)


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASENAME = f"best_long_short_btc_eth_sol_combo_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASENAME}.html"
SUMMARY_CSV_PATH = REPORT_DIR / f"{BASENAME}_summary.csv"
COIN_COMBO_CSV_PATH = REPORT_DIR / f"{BASENAME}_coin_combo.csv"
YEARLY_CSV_PATH = REPORT_DIR / f"{BASENAME}_yearly.csv"
MONTHLY_CSV_PATH = REPORT_DIR / f"{BASENAME}_monthly.csv"
YEARLY_PIVOT_CSV_PATH = REPORT_DIR / f"{BASENAME}_yearly_pivot.csv"
MONTHLY_PIVOT_CSV_PATH = REPORT_DIR / f"{BASENAME}_monthly_pivot.csv"
TRADES_CSV_PATH = REPORT_DIR / f"{BASENAME}_trades.csv"
PROJECT_HTML_PATH = PROJECT_REPORT_DIR / "best_long_short_btc_eth_sol_combo.html"

TARGET_SYMBOLS = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP")

DISPLAY_NAME_MAP = {
    "coin": "币种",
    "symbol": "交易对",
    "side": "方向",
    "profile_name": "参数名称",
    "config_label": "小时参数",
    "daily_filter_label": "日线过滤",
    "combo_label": "组合说明",
    "range_start": "开始时间",
    "range_end": "结束时间",
    "candles": "K线数",
    "all_pnl_u": "全样本利润",
    "all_trades": "全样本交易数",
    "all_win_rate_pct": "全样本胜率",
    "all_profit_factor": "全样本PF",
    "all_avg_r": "全样本AvgR",
    "all_drawdown_u": "全样本回撤",
    "test_pnl_u": "测试段利润",
    "test_trades": "测试段交易数",
    "test_win_rate_pct": "测试段胜率",
    "test_profit_factor": "测试段PF",
    "test_avg_r": "测试段AvgR",
    "test_drawdown_u": "测试段回撤",
    "period": "周期",
    "trades": "交易数",
    "wins": "盈利笔数",
    "losses": "亏损笔数",
    "win_rate_pct": "胜率",
    "pnl_u": "利润",
    "avg_pnl_u": "平均利润",
    "cumulative_pnl_u": "累计利润",
    "end_capital_u": "期末资金",
    "TOTAL": "合计",
}


@dataclass(frozen=True)
class Study:
    symbol: str
    coin: str
    side: str
    profile_name: str
    config_label: str
    daily_filter_label: str
    trades: list
    test_trades: list
    all_metrics: object
    test_metrics: object
    start_utc: str
    end_utc: str
    candle_count: int


def rename_display(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns=DISPLAY_NAME_MAP)


def selected_long_specs():
    return [spec for spec in SPECS if spec.symbol in TARGET_SYMBOLS]


def selected_short_specs():
    return [spec for spec in SHORT_SPECS if spec.symbol in TARGET_SYMBOLS]


def build_long_label(spec) -> str:
    fast = f"{spec.ema_type.upper()}{spec.ema_period}"
    trend = f"{spec.trend_ema_type.upper()}{spec.trend_ema_period}"
    ref = f"{spec.entry_reference_ema_type.upper()}{spec.entry_reference_ema_period}"
    return f"{fast} / {trend} / 挂单 {ref}"


def build_short_label(spec) -> str:
    line = f"{spec.ema_type.upper()}{spec.ema_period}"
    return f"{line} 斜率做空"


def build_short_filter_label(spec) -> str:
    return f"北京时间8点日线 {spec.daily_filter_ma_type.upper()}{spec.daily_filter_period}"


def run_long_study(client: OkxRestClient, spec) -> Study:
    candles = [candle for candle in load_candle_cache(spec.symbol, "1H", limit=None) if candle.confirmed]
    instrument = client.get_instrument(spec.symbol)
    bounds = build_split_bounds(len(candles))["test"]
    result = _run_backtest_with_loaded_data(
        candles,
        instrument,
        build_dynamic_long_config(spec),
        data_source_note=f"local candle_cache full history | {spec.symbol} best dynamic long config",
        maker_fee_rate=LONG_MAKER_FEE_RATE,
        taker_fee_rate=LONG_TAKER_FEE_RATE,
    )
    trades = list(result.trades)
    test_trades = filter_split_trades(trades, bounds)
    coin = spec.symbol.replace("-USDT-SWAP", "")
    return Study(
        symbol=spec.symbol,
        coin=coin,
        side="long",
        profile_name=spec.profile_name,
        config_label=build_long_label(spec),
        daily_filter_label="不加日线过滤",
        trades=trades,
        test_trades=test_trades,
        all_metrics=build_metrics(trades),
        test_metrics=build_metrics(test_trades),
        start_utc=format_ts(candles[0].ts),
        end_utc=format_ts(candles[-1].ts),
        candle_count=len(candles),
    )


def run_short_study(client: OkxRestClient, spec) -> Study:
    candles = [candle for candle in load_candle_cache(spec.symbol, "1H", limit=None) if candle.confirmed]
    instrument = client.get_instrument(spec.symbol)
    bounds = build_split_bounds(len(candles))["test"]
    result = _run_backtest_with_loaded_data(
        candles,
        instrument,
        build_slope_short_config(spec),
        data_source_note=f"local candle_cache full history | {spec.symbol} best short final config",
        taker_fee_rate=SHORT_TAKER_FEE_RATE,
    )
    trades = list(result.trades)
    test_trades = filter_split_trades(trades, bounds)
    coin = spec.symbol.replace("-USDT-SWAP", "")
    return Study(
        symbol=spec.symbol,
        coin=coin,
        side="short",
        profile_name=spec.profile_name,
        config_label=build_short_label(spec),
        daily_filter_label=build_short_filter_label(spec),
        trades=trades,
        test_trades=test_trades,
        all_metrics=build_metrics(trades),
        test_metrics=build_metrics(test_trades),
        start_utc=format_ts(candles[0].ts),
        end_utc=format_ts(candles[-1].ts),
        candle_count=len(candles),
    )


def build_trade_frame(studies: list[Study]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for study in studies:
        for trade in study.trades:
            exit_dt = pd.to_datetime(int(trade.exit_ts), unit="ms", utc=True)
            rows.append(
                {
                    "coin": study.coin,
                    "symbol": study.symbol,
                    "side": study.side,
                    "profile_name": study.profile_name,
                    "config_label": study.config_label,
                    "daily_filter_label": study.daily_filter_label,
                    "entry_ts": int(trade.entry_ts),
                    "exit_ts": int(trade.exit_ts),
                    "entry_time": pd.to_datetime(int(trade.entry_ts), unit="ms", utc=True),
                    "exit_time": exit_dt,
                    "year": exit_dt.strftime("%Y"),
                    "month": exit_dt.strftime("%Y-%m"),
                    "pnl_u": float(trade.pnl),
                    "r_multiple": float(trade.r_multiple),
                    "exit_reason": str(trade.exit_reason),
                }
            )
    return pd.DataFrame(rows).sort_values(["exit_ts", "entry_ts", "coin", "side"]).reset_index(drop=True)


def build_summary_frame(studies: list[Study]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for study in studies:
        rows.append(
            {
                "coin": study.coin,
                "side": "做多" if study.side == "long" else "做空",
                "profile_name": study.profile_name,
                "config_label": study.config_label,
                "daily_filter_label": study.daily_filter_label,
                "range_start": study.start_utc,
                "range_end": study.end_utc,
                "candles": study.candle_count,
                "all_pnl_u": float(study.all_metrics.pnl),
                "all_trades": study.all_metrics.trades,
                "all_win_rate_pct": float(study.all_metrics.win_rate),
                "all_profit_factor": none_or_float(study.all_metrics.profit_factor),
                "all_avg_r": float(study.all_metrics.avg_r),
                "all_drawdown_u": float(study.all_metrics.max_drawdown),
                "test_pnl_u": float(study.test_metrics.pnl),
                "test_trades": study.test_metrics.trades,
                "test_win_rate_pct": float(study.test_metrics.win_rate),
                "test_profit_factor": none_or_float(study.test_metrics.profit_factor),
                "test_avg_r": float(study.test_metrics.avg_r),
                "test_drawdown_u": float(study.test_metrics.max_drawdown),
            }
        )
    return pd.DataFrame(rows)


def build_coin_combo_frame(studies: list[Study]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for coin in sorted({study.coin for study in studies}):
        coin_studies = [study for study in studies if study.coin == coin]
        all_trades = sorted(
            [trade for study in coin_studies for trade in study.trades],
            key=lambda trade: (trade.exit_ts, trade.entry_ts),
        )
        test_trades = sorted(
            [trade for study in coin_studies for trade in study.test_trades],
            key=lambda trade: (trade.exit_ts, trade.entry_ts),
        )
        all_metrics = build_metrics(all_trades)
        test_metrics = build_metrics(test_trades)
        rows.append(
            {
                "coin": coin,
                "combo_label": "最佳做多 + 最佳做空",
                "all_pnl_u": float(all_metrics.pnl),
                "all_trades": all_metrics.trades,
                "all_win_rate_pct": float(all_metrics.win_rate),
                "all_profit_factor": none_or_float(all_metrics.profit_factor),
                "all_avg_r": float(all_metrics.avg_r),
                "all_drawdown_u": float(all_metrics.max_drawdown),
                "test_pnl_u": float(test_metrics.pnl),
                "test_trades": test_metrics.trades,
                "test_win_rate_pct": float(test_metrics.win_rate),
                "test_profit_factor": none_or_float(test_metrics.profit_factor),
                "test_avg_r": float(test_metrics.avg_r),
                "test_drawdown_u": float(test_metrics.max_drawdown),
            }
        )
    return pd.DataFrame(rows)


def build_period_frame(trade_frame: pd.DataFrame, period: str) -> pd.DataFrame:
    field = "month" if period == "month" else "year"
    grouped = (
        trade_frame.groupby(["coin", field], as_index=False)
        .agg(
            trades=("pnl_u", "size"),
            wins=("pnl_u", lambda s: int((s > 0).sum())),
            pnl_u=("pnl_u", "sum"),
        )
        .rename(columns={field: "period"})
        .sort_values(["coin", "period"])
        .reset_index(drop=True)
    )
    grouped["losses"] = grouped["trades"] - grouped["wins"]
    grouped["win_rate_pct"] = grouped["wins"] / grouped["trades"] * 100.0
    grouped["avg_pnl_u"] = grouped["pnl_u"] / grouped["trades"]
    grouped["cumulative_pnl_u"] = grouped.groupby("coin")["pnl_u"].cumsum()
    grouped["end_capital_u"] = float(INITIAL_CAPITAL) + grouped["cumulative_pnl_u"]
    return grouped


def build_profit_pivot(period_frame: pd.DataFrame) -> pd.DataFrame:
    pivot = (
        period_frame.pivot(index="period", columns="coin", values="pnl_u")
        .fillna(0.0)
        .sort_index()
        .reset_index()
    )
    coin_cols = [col for col in pivot.columns if col != "period"]
    pivot["TOTAL"] = pivot[coin_cols].sum(axis=1)
    return pivot


def none_or_float(value) -> float | None:
    if value is None:
        return None
    return float(value)


def format_u(value: float) -> str:
    return f"{value:,.2f}U"


def format_pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:.2f}%"


def format_float(value: float | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:.{digits}f}"


def dataframe_to_html(
    df: pd.DataFrame,
    *,
    money_cols: set[str] | None = None,
    pct_cols: set[str] | None = None,
    float_cols: set[str] | None = None,
) -> str:
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
                text = format_u(float(value))
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


def fig_to_base64(fig) -> str:
    buffer = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buffer, format="png", dpi=160, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def build_yearly_chart(yearly_pivot: pd.DataFrame) -> str:
    fig, ax = plt.subplots(figsize=(10.8, 4.8))
    chart = yearly_pivot.set_index("period")[[col for col in yearly_pivot.columns if col not in {"period", "TOTAL"}]]
    chart.plot(kind="bar", ax=ax)
    ax.axhline(0, color="#475467", linewidth=1)
    ax.set_title("BTC / ETH / SOL 多空最佳参数年度利润横向对比")
    ax.set_ylabel("PnL (U)")
    ax.grid(axis="y", alpha=0.2)
    ax.legend(title="币种")
    return fig_to_base64(fig)


def build_monthly_total_chart(monthly_pivot: pd.DataFrame) -> str:
    fig, ax = plt.subplots(figsize=(12.2, 4.8))
    ax.plot(monthly_pivot["period"], monthly_pivot["TOTAL"], color="#9A3412", linewidth=2.2)
    ax.axhline(0, color="#475467", linewidth=1)
    ax.tick_params(axis="x", rotation=70)
    ax.set_title("BTC / ETH / SOL 多空最佳参数月度合计利润")
    ax.set_ylabel("PnL (U)")
    ax.grid(axis="y", alpha=0.2)
    return fig_to_base64(fig)


def build_html(
    studies: list[Study],
    summary_frame: pd.DataFrame,
    coin_combo_frame: pd.DataFrame,
    yearly_frame: pd.DataFrame,
    monthly_frame: pd.DataFrame,
    yearly_pivot: pd.DataFrame,
    monthly_pivot: pd.DataFrame,
) -> str:
    total_all_pnl = float(coin_combo_frame["all_pnl_u"].sum()) if not coin_combo_frame.empty else 0.0
    total_test_pnl = float(coin_combo_frame["test_pnl_u"].sum()) if not coin_combo_frame.empty else 0.0
    total_trades = int(coin_combo_frame["all_trades"].sum()) if not coin_combo_frame.empty else 0
    yearly_chart = build_yearly_chart(yearly_pivot)
    monthly_chart = build_monthly_total_chart(monthly_pivot)

    parameter_frame = pd.DataFrame(
        [
            {
                "coin": study.coin,
                "side": "做多" if study.side == "long" else "做空",
                "profile_name": study.profile_name,
                "config_label": study.config_label,
                "daily_filter_label": study.daily_filter_label,
            }
            for study in studies
        ]
    )

    display_parameter = rename_display(parameter_frame)
    display_summary = rename_display(summary_frame)
    display_coin_combo = rename_display(coin_combo_frame)
    display_yearly = rename_display(yearly_frame)
    display_monthly = rename_display(monthly_frame)
    display_yearly_pivot = rename_display(yearly_pivot)
    display_monthly_pivot = rename_display(monthly_pivot)

    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>BTC ETH SOL 多空最佳参数组合报告</title>
  <style>
    :root {{
      --bg: #f5f1e8;
      --panel: #fffdf8;
      --ink: #1f1b16;
      --muted: #6c6257;
      --line: #d9cfc1;
      --accent: #9a3412;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: radial-gradient(circle at top left, rgba(154,52,18,0.08), transparent 32%), linear-gradient(180deg, #f8f3ea 0%, var(--bg) 100%);
      color: var(--ink);
      font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
      line-height: 1.55;
    }}
    .wrap {{ width: min(1500px, calc(100vw - 48px)); margin: 0 auto; padding: 28px 0 42px; }}
    .hero, .section {{ background: rgba(255,255,255,0.82); border: 1px solid rgba(217,207,193,0.9); border-radius: 24px; padding: 24px; }}
    .section {{ margin-top: 24px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; margin-top: 18px; }}
    .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 18px; padding: 16px 18px; }}
    .label {{ color: var(--muted); font-size: 13px; }}
    .value {{ font-size: 28px; font-weight: 700; margin-top: 6px; }}
    .note {{ padding: 12px 14px; border-left: 4px solid var(--accent); background: rgba(154,52,18,0.06); border-radius: 12px; color: #4a3829; }}
    .img-box {{ margin-top: 16px; background: var(--panel); border: 1px solid var(--line); border-radius: 18px; padding: 10px; }}
    img {{ display: block; width: 100%; height: auto; border-radius: 12px; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 14px; background: var(--panel); }}
    th, td {{ padding: 10px 12px; border-bottom: 1px solid var(--line); text-align: right; white-space: nowrap; font-size: 13px; }}
    th:first-child, td:first-child {{ text-align: left; }}
    thead th {{ background: #efe5d8; }}
    tr:last-child td {{ border-bottom: none; }}
    h1, h2 {{ margin: 0 0 12px; }}
    p {{ margin: 8px 0; color: var(--muted); }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>BTC / ETH / SOL 多空最佳参数组合报告</h1>
      <p>口径：只使用当前“最佳参数组合包”里的 <code>BTC / ETH / SOL</code> 最终最佳做多与最佳做空参数，用正式主回测接口按全历史重跑，再把多空结果合并统计。</p>
      <div class="grid">
        <div class="card"><div class="label">三币多空全样本利润</div><div class="value">{format_u(total_all_pnl)}</div></div>
        <div class="card"><div class="label">三币多空测试段利润</div><div class="value">{format_u(total_test_pnl)}</div></div>
        <div class="card"><div class="label">三币多空全样本交易数</div><div class="value">{total_trades}</div></div>
        <div class="card"><div class="label">参数条目数</div><div class="value">{len(studies)}</div></div>
      </div>
    </section>

    <section class="section">
      <h2>参数清单</h2>
      <div class="note">这份报告只保留 6 条最终参数：BTC / ETH / SOL 各 1 条做多最佳参数 + 各 1 条做空最佳参数，不混入 BNB / DOGE，也不混入旧研究参数。</div>
      {dataframe_to_html(display_parameter)}
    </section>

    <section class="section">
      <h2>单策略总览</h2>
      {dataframe_to_html(display_summary, money_cols={"全样本利润", "全样本回撤", "测试段利润", "测试段回撤"}, pct_cols={"全样本胜率", "测试段胜率"}, float_cols={"全样本PF", "全样本AvgR", "测试段PF", "测试段AvgR"})}
    </section>

    <section class="section">
      <h2>按币种合并后的多空结果</h2>
      {dataframe_to_html(display_coin_combo, money_cols={"全样本利润", "全样本回撤", "测试段利润", "测试段回撤"}, pct_cols={"全样本胜率", "测试段胜率"}, float_cols={"全样本PF", "全样本AvgR", "测试段PF", "测试段AvgR"})}
    </section>

    <section class="section">
      <h2>年度利润横向对比</h2>
      <div class="img-box"><img alt="yearly_chart" src="data:image/png;base64,{yearly_chart}" /></div>
      {dataframe_to_html(display_yearly_pivot, money_cols=set(display_yearly_pivot.columns) - {"周期"})}
    </section>

    <section class="section">
      <h2>月度利润横向对比</h2>
      <div class="img-box"><img alt="monthly_chart" src="data:image/png;base64,{monthly_chart}" /></div>
      {dataframe_to_html(display_monthly_pivot, money_cols=set(display_monthly_pivot.columns) - {"周期"})}
    </section>

    <section class="section">
      <h2>年度明细</h2>
      {dataframe_to_html(display_yearly, money_cols={"利润", "平均利润", "累计利润", "期末资金"}, pct_cols={"胜率"})}
    </section>

    <section class="section">
      <h2>月度明细</h2>
      {dataframe_to_html(display_monthly, money_cols={"利润", "平均利润", "累计利润", "期末资金"}, pct_cols={"胜率"})}
    </section>
  </div>
</body>
</html>
"""


def main() -> None:
    client = OkxRestClient()
    studies = [run_long_study(client, spec) for spec in selected_long_specs()] + [
        run_short_study(client, spec) for spec in selected_short_specs()
    ]
    trade_frame = build_trade_frame(studies)
    summary_frame = build_summary_frame(studies)
    coin_combo_frame = build_coin_combo_frame(studies)
    yearly_frame = build_period_frame(trade_frame, "year")
    monthly_frame = build_period_frame(trade_frame, "month")
    yearly_pivot = build_profit_pivot(yearly_frame)
    monthly_pivot = build_profit_pivot(monthly_frame)

    trade_frame.to_csv(TRADES_CSV_PATH, index=False, encoding="utf-8-sig")
    rename_display(summary_frame).to_csv(SUMMARY_CSV_PATH, index=False, encoding="utf-8-sig")
    rename_display(coin_combo_frame).to_csv(COIN_COMBO_CSV_PATH, index=False, encoding="utf-8-sig")
    rename_display(yearly_frame).to_csv(YEARLY_CSV_PATH, index=False, encoding="utf-8-sig")
    rename_display(monthly_frame).to_csv(MONTHLY_CSV_PATH, index=False, encoding="utf-8-sig")
    rename_display(yearly_pivot).to_csv(YEARLY_PIVOT_CSV_PATH, index=False, encoding="utf-8-sig")
    rename_display(monthly_pivot).to_csv(MONTHLY_PIVOT_CSV_PATH, index=False, encoding="utf-8-sig")

    html_text = build_html(studies, summary_frame, coin_combo_frame, yearly_frame, monthly_frame, yearly_pivot, monthly_pivot)
    HTML_PATH.write_text(html_text, encoding="utf-8")
    PROJECT_HTML_PATH.write_text(html_text, encoding="utf-8")
    print(HTML_PATH)


if __name__ == "__main__":
    main()
