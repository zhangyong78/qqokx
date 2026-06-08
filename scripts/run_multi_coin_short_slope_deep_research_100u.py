from __future__ import annotations

import base64
import html
import io
import sys
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import matplotlib.pyplot as plt
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


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASENAME = f"multi_coin_short_slope_deep_research_100u_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASENAME}.html"
RUNS_CSV_PATH = REPORT_DIR / f"{BASENAME}_runs.csv"
LINE_BEST_CSV_PATH = REPORT_DIR / f"{BASENAME}_line_best.csv"
RECOMMEND_CSV_PATH = REPORT_DIR / f"{BASENAME}_recommend.csv"
TRADES_CSV_PATH = REPORT_DIR / f"{BASENAME}_trades.csv"
PROJECT_HTML_PATH = PROJECT_REPORT_DIR / "multi_coin_short_slope_deep_research_100u.html"

SYMBOLS = (
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "SOL-USDT-SWAP",
    "BNB-USDT-SWAP",
    "DOGE-USDT-SWAP",
)

RISK_AMOUNT = Decimal("100")
INITIAL_CAPITAL = Decimal("10000")
ENTRY_BAR = "1H"
FILTER_BAR = "1D"


@dataclass(frozen=True)
class LineVariant:
    key: str
    label: str
    ma_type: str
    period: int


@dataclass(frozen=True)
class FilterVariant:
    key: str
    label: str
    ma_type: str | None
    period: int | None


LINE_VARIANTS = (
    LineVariant("ema21", "EMA21 斜率做空", "ema", 21),
    LineVariant("ma20", "MA20 斜率做空", "ma", 20),
    LineVariant("ema34", "EMA34 斜率做空", "ema", 34),
    LineVariant("ma34", "MA34 斜率做空", "ma", 34),
    LineVariant("ema55", "EMA55 斜率做空", "ema", 55),
    LineVariant("ma55", "MA55 斜率做空", "ma", 55),
    LineVariant("ema89", "EMA89 斜率做空", "ema", 89),
    LineVariant("ma89", "MA89 斜率做空", "ma", 89),
)

FILTER_VARIANTS = (
    FilterVariant("none", "不加日线过滤", None, None),
    FilterVariant("ema21", "北京时间8点日线 EMA21", "ema", 21),
    FilterVariant("ma20", "北京时间8点日线 MA20", "ma", 20),
    FilterVariant("ema34", "北京时间8点日线 EMA34", "ema", 34),
    FilterVariant("ma34", "北京时间8点日线 MA34", "ma", 34),
    FilterVariant("ema55", "北京时间8点日线 EMA55", "ema", 55),
    FilterVariant("ma55", "北京时间8点日线 MA55", "ma", 55),
)

DISPLAY_NAME_MAP = {
    "coin": "币种",
    "symbol": "交易对",
    "line_label": "斜率线参数",
    "line_type": "均线类型",
    "line_period": "均线周期",
    "filter_label": "日线过滤",
    "filter_type": "过滤均线类型",
    "filter_period": "过滤周期",
    "range_start": "开始时间",
    "range_end": "结束时间",
    "candles": "K线数",
    "min_size_bind_count": "最小单量触发数",
    "min_size_bind_pct": "最小单量触发占比",
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
    "recommend_rank": "推荐排序",
    "period": "周期",
    "TOTAL": "合计",
}


def rename_display(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns=DISPLAY_NAME_MAP)


def build_config(symbol: str, line: LineVariant, gate: FilterVariant) -> StrategyConfig:
    daily_enabled = gate.key != "none"
    return StrategyConfig(
        inst_id=symbol,
        bar=ENTRY_BAR,
        ema_period=line.period,
        ema_type=line.ma_type,
        trend_ema_period=line.period,
        trend_ema_type=line.ma_type,
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
        daily_filter_enabled=daily_enabled,
        daily_filter_bar=FILTER_BAR if daily_enabled else None,
        daily_filter_boundary="bjt_08",
        daily_filter_mode="close_vs_ma" if daily_enabled else "disabled",
        daily_filter_scope="short_only",
        daily_filter_ma_type=gate.ma_type or "ema",
        daily_filter_period=gate.period or 21,
    )


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


def build_top_test_chart(recommend_frame: pd.DataFrame) -> str:
    fig, ax = plt.subplots(figsize=(10.6, 4.8))
    ax.bar(recommend_frame["coin"], recommend_frame["test_pnl_u"], color="#9A3412")
    ax.axhline(0, color="#475467", linewidth=1)
    ax.set_title("5币推荐组合测试段利润")
    ax.set_ylabel("PnL (U)")
    ax.grid(axis="y", alpha=0.2)
    return fig_to_base64(fig)


def build_line_compare_chart(line_best_frame: pd.DataFrame) -> str:
    fig, ax = plt.subplots(figsize=(12.8, 5.0))
    chart = (
        line_best_frame.pivot(index="line_label", columns="coin", values="test_pnl_u")
        .fillna(0.0)
        .loc[[item.label for item in LINE_VARIANTS]]
    )
    chart.plot(kind="bar", ax=ax)
    ax.axhline(0, color="#475467", linewidth=1)
    ax.set_title("各币不同斜率线的最佳测试段利润")
    ax.set_ylabel("PnL (U)")
    ax.grid(axis="y", alpha=0.2)
    ax.legend(title="币种")
    return fig_to_base64(fig)


def build_runs(client: OkxRestClient) -> tuple[pd.DataFrame, pd.DataFrame]:
    run_rows: list[dict[str, object]] = []
    trade_rows: list[dict[str, object]] = []
    for symbol in SYMBOLS:
        candles = [candle for candle in load_candle_cache(symbol, ENTRY_BAR, limit=None) if candle.confirmed]
        instrument = client.get_instrument(symbol)
        bounds = build_split_bounds(len(candles))["test"]
        coin = symbol.replace("-USDT-SWAP", "")
        for line in LINE_VARIANTS:
            for gate in FILTER_VARIANTS:
                config = build_config(symbol, line, gate)
                try:
                    result = _run_backtest_with_loaded_data(
                        candles,
                        instrument,
                        config,
                        data_source_note=f"{symbol} {line.key} {gate.key} deep research 100u",
                        taker_fee_rate=SHORT_TAKER_FEE_RATE,
                    )
                    trades = list(result.trades)
                    test_trades = filter_split_trades(trades, bounds)
                    all_metrics = build_metrics(trades)
                    test_metrics = build_metrics(test_trades)
                    min_size_bind_count = sum(1 for trade in trades if trade.size == instrument.min_size)
                    min_size_bind_pct = 0.0 if not trades else (min_size_bind_count / len(trades)) * 100.0
                    error_text = ""
                except Exception as exc:  # pragma: no cover - research robustness
                    trades = []
                    test_trades = []
                    all_metrics = build_metrics(trades)
                    test_metrics = build_metrics(test_trades)
                    min_size_bind_count = 0
                    min_size_bind_pct = 0.0
                    error_text = str(exc)

                run_rows.append(
                    {
                        "coin": coin,
                        "symbol": symbol,
                        "line_key": line.key,
                        "line_label": line.label,
                        "line_type": line.ma_type.upper(),
                        "line_period": line.period,
                        "filter_key": gate.key,
                        "filter_label": gate.label,
                        "filter_type": "" if gate.ma_type is None else gate.ma_type.upper(),
                        "filter_period": "" if gate.period is None else gate.period,
                        "range_start": format_ts(candles[0].ts),
                        "range_end": format_ts(candles[-1].ts),
                        "candles": len(candles),
                        "min_size_bind_count": min_size_bind_count,
                        "min_size_bind_pct": min_size_bind_pct,
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
                        "error_text": error_text,
                    }
                )
                for trade in trades:
                    trade_rows.append(
                        {
                            "coin": coin,
                            "symbol": symbol,
                            "line_label": line.label,
                            "filter_label": gate.label,
                            "entry_ts": int(trade.entry_ts),
                            "exit_ts": int(trade.exit_ts),
                            "entry_time": pd.to_datetime(int(trade.entry_ts), unit="ms", utc=True),
                            "exit_time": pd.to_datetime(int(trade.exit_ts), unit="ms", utc=True),
                            "pnl_u": float(trade.pnl),
                            "r_multiple": float(trade.r_multiple),
                            "size": float(trade.size),
                            "exit_reason": str(trade.exit_reason),
                        }
                    )
    return pd.DataFrame(run_rows), pd.DataFrame(trade_rows)


def choose_recommendations(run_frame: pd.DataFrame) -> pd.DataFrame:
    picks: list[pd.DataFrame] = []
    valid_frame = run_frame[run_frame["error_text"] == ""].copy()
    for coin, group in valid_frame.groupby("coin", sort=True):
        ordered = group.sort_values(
            ["test_pnl_u", "test_profit_factor", "test_drawdown_u", "all_pnl_u", "all_profit_factor"],
            ascending=[False, False, True, False, False],
        ).reset_index(drop=True)
        ordered["recommend_rank"] = ordered.index + 1
        picks.append(ordered.iloc[[0]])
        source_indexes = group.sort_values(
            ["test_pnl_u", "test_profit_factor", "test_drawdown_u", "all_pnl_u", "all_profit_factor"],
            ascending=[False, False, True, False, False],
        ).index
        run_frame.loc[source_indexes, "recommend_rank"] = ordered["recommend_rank"].to_numpy()
    return pd.concat(picks, ignore_index=True)


def build_line_best_frame(run_frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[pd.Series] = []
    valid_frame = run_frame[run_frame["error_text"] == ""].copy()
    for (coin, line_label), group in valid_frame.groupby(["coin", "line_label"], sort=True):
        best = group.sort_values(
            ["test_pnl_u", "test_profit_factor", "test_drawdown_u", "all_pnl_u"],
            ascending=[False, False, True, False],
        ).iloc[0]
        rows.append(best)
    return pd.DataFrame(rows).sort_values(["coin", "test_pnl_u"], ascending=[True, False]).reset_index(drop=True)


def build_total_row(recommend_frame: pd.DataFrame) -> pd.DataFrame:
    row = {
        "coin": "合计",
        "line_label": "按各币推荐组合",
        "filter_label": "-",
        "all_pnl_u": float(recommend_frame["all_pnl_u"].sum()),
        "all_trades": int(recommend_frame["all_trades"].sum()),
        "all_drawdown_u": float(recommend_frame["all_drawdown_u"].sum()),
        "test_pnl_u": float(recommend_frame["test_pnl_u"].sum()),
        "test_trades": int(recommend_frame["test_trades"].sum()),
        "test_drawdown_u": float(recommend_frame["test_drawdown_u"].sum()),
        "min_size_bind_count": int(recommend_frame["min_size_bind_count"].sum()),
        "min_size_bind_pct": float(recommend_frame["min_size_bind_pct"].mean()),
    }
    return pd.DataFrame([row])


def build_html(
    run_frame: pd.DataFrame,
    recommend_frame: pd.DataFrame,
    line_best_frame: pd.DataFrame,
) -> str:
    recommend_total = build_total_row(recommend_frame)
    top_test_chart = build_top_test_chart(recommend_frame)
    line_chart = build_line_compare_chart(line_best_frame)
    summary_cards = {
        "回测口径": "1H 全历史 / 100U 固定风险 / 正式主回测接口",
        "参数矩阵": f"{len(LINE_VARIANTS)} 条斜率线 × {len(FILTER_VARIANTS)} 种日线过滤 × 5 币",
        "总组合数": str(len(run_frame)),
        "推荐组合测试段利润": format_u(float(recommend_frame["test_pnl_u"].sum())),
    }

    recommend_display = rename_display(
        recommend_frame[
            [
                "coin",
                "line_label",
                "filter_label",
                "min_size_bind_count",
                "min_size_bind_pct",
                "all_pnl_u",
                "all_trades",
                "all_profit_factor",
                "all_drawdown_u",
                "test_pnl_u",
                "test_trades",
                "test_profit_factor",
                "test_drawdown_u",
            ]
        ]
    )
    total_display = rename_display(recommend_total)
    line_display = rename_display(
        line_best_frame[
            [
                "coin",
                "line_label",
                "filter_label",
                "min_size_bind_count",
                "all_pnl_u",
                "all_profit_factor",
                "all_drawdown_u",
                "test_pnl_u",
                "test_profit_factor",
                "test_drawdown_u",
            ]
        ]
    )
    top_by_coin_sections: list[str] = []
    for coin in sorted(run_frame["coin"].unique()):
        top_coin = run_frame[run_frame["coin"] == coin].sort_values(
            ["test_pnl_u", "test_profit_factor", "test_drawdown_u", "all_pnl_u"],
            ascending=[False, False, True, False],
        ).head(10)
        display = rename_display(
            top_coin[
                [
                    "line_label",
                    "filter_label",
                    "min_size_bind_count",
                    "all_pnl_u",
                    "all_profit_factor",
                    "all_drawdown_u",
                    "test_pnl_u",
                    "test_profit_factor",
                    "test_drawdown_u",
                ]
            ]
        )
        top_by_coin_sections.append(
            f"""
            <section class="section">
              <h3>{html.escape(coin)} 前10组合</h3>
              {dataframe_to_html(display, money_cols={"全样本利润", "全样本回撤", "测试段利润", "测试段回撤"}, float_cols={"全样本PF", "测试段PF"})}
            </section>
            """
        )

    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>5币斜率做空 100U 深度研究报告</title>
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
    .wrap {{ width: min(1560px, calc(100vw - 48px)); margin: 0 auto; padding: 28px 0 42px; }}
    .hero, .section {{ background: rgba(255,255,255,0.82); border: 1px solid rgba(217,207,193,0.9); border-radius: 24px; padding: 24px; }}
    .section {{ margin-top: 24px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; margin-top: 18px; }}
    .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 18px; padding: 16px 18px; }}
    .label {{ color: var(--muted); font-size: 13px; }}
    .value {{ font-size: 22px; font-weight: 700; margin-top: 6px; }}
    .note {{ padding: 12px 14px; border-left: 4px solid var(--accent); background: rgba(154,52,18,0.06); border-radius: 12px; color: #4a3829; }}
    .img-box {{ margin-top: 16px; background: var(--panel); border: 1px solid var(--line); border-radius: 18px; padding: 10px; }}
    img {{ display: block; width: 100%; height: auto; border-radius: 12px; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 14px; background: var(--panel); }}
    th, td {{ padding: 10px 12px; border-bottom: 1px solid var(--line); text-align: right; white-space: nowrap; font-size: 13px; }}
    th:first-child, td:first-child {{ text-align: left; }}
    thead th {{ background: #efe5d8; }}
    tr:last-child td {{ border-bottom: none; }}
    h1, h2, h3 {{ margin: 0 0 12px; }}
    p {{ margin: 8px 0; color: var(--muted); }}
    code {{ background: rgba(31,27,22,0.06); padding: 1px 6px; border-radius: 6px; color: #5c2411; }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>5币斜率做空 100U 深度研究报告</h1>
      <p>这次只研究 <code>EMA / MA 斜率做空</code> 这条线，不混入 Body/ATR 做空。所有组合统一按 <code>1H 全历史 + 100U 固定风险 + ATR14 止损2 + 动态止盈 + 2R保本 + ATR分位≤0.5 + 北京时间8点日线过滤</code> 重跑。</p>
      <div class="grid">
        {''.join(f'<div class="card"><div class="label">{html.escape(k)}</div><div class="value">{html.escape(v)}</div></div>' for k, v in summary_cards.items())}
      </div>
    </section>

    <section class="section">
      <h2>结论先看</h2>
      <div class="note">
        推荐排序规则：优先看 <code>测试段利润</code>，再看 <code>测试段PF</code>，然后看 <code>测试段回撤</code> 与 <code>全样本利润</code>。<br/>
        这份报告重点回答：<code>BTC / ETH / SOL / BNB / DOGE</code> 在 100U 口径下，到底是更适合 <code>EMA55</code>、<code>EMA34</code>、<code>MA20</code> 还是其他斜率线。
      </div>
      {dataframe_to_html(recommend_display, money_cols={"全样本利润", "全样本回撤", "测试段利润", "测试段回撤"}, pct_cols={"最小单量触发占比"}, float_cols={"全样本PF", "测试段PF"})}
      {dataframe_to_html(total_display, money_cols={"全样本利润", "全样本回撤", "测试段利润", "测试段回撤"}, pct_cols={"最小单量触发占比"})}
    </section>

    <section class="section">
      <h2>各斜率线最佳结果</h2>
      <div class="img-box"><img alt="line_chart" src="data:image/png;base64,{line_chart}" /></div>
      {dataframe_to_html(line_display, money_cols={"全样本利润", "全样本回撤", "测试段利润", "测试段回撤"}, float_cols={"全样本PF", "测试段PF"})}
    </section>

    <section class="section">
      <h2>推荐组合测试段利润</h2>
      <div class="img-box"><img alt="top_test_chart" src="data:image/png;base64,{top_test_chart}" /></div>
    </section>

    {''.join(top_by_coin_sections)}
  </div>
</body>
</html>
"""


def main() -> None:
    client = OkxRestClient()
    run_frame, trade_frame = build_runs(client)
    if run_frame.empty:
        raise RuntimeError("未生成任何回测结果")

    run_frame["recommend_rank"] = 0
    recommend_frame = choose_recommendations(run_frame)
    line_best_frame = build_line_best_frame(run_frame)

    trade_frame.to_csv(TRADES_CSV_PATH, index=False, encoding="utf-8-sig")
    rename_display(run_frame).to_csv(RUNS_CSV_PATH, index=False, encoding="utf-8-sig")
    rename_display(line_best_frame).to_csv(LINE_BEST_CSV_PATH, index=False, encoding="utf-8-sig")
    rename_display(recommend_frame).to_csv(RECOMMEND_CSV_PATH, index=False, encoding="utf-8-sig")

    html_text = build_html(run_frame, recommend_frame, line_best_frame)
    HTML_PATH.write_text(html_text, encoding="utf-8")
    PROJECT_HTML_PATH.write_text(html_text, encoding="utf-8")
    print(HTML_PATH)


if __name__ == "__main__":
    main()
