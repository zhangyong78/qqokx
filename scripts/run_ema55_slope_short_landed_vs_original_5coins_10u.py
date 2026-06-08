from __future__ import annotations

import base64
import html
import io
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import BacktestTrade, _run_backtest_with_loaded_data
from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.strategy_catalog import STRATEGY_EMA55_SLOPE_SHORT_ID
from scripts.run_btc_daily_ma_direction_filter_research import (
    ENTRY_BAR,
    INITIAL_CAPITAL,
    RISK_AMOUNT,
    SHORT_TAKER_FEE_RATE,
    SplitMetrics,
    build_metrics,
    build_split_bounds,
    filter_split_trades,
    format_ts,
)


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASENAME = f"ema55_slope_short_landed_vs_original_5coins_10u_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASENAME}.html"
CSV_PATH = REPORT_DIR / f"{BASENAME}.csv"
COIN_CSV_PATH = REPORT_DIR / f"{BASENAME}_by_coin.csv"
JSON_PATH = REPORT_DIR / f"{BASENAME}.json"

SYMBOLS = (
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "SOL-USDT-SWAP",
    "BNB-USDT-SWAP",
    "DOGE-USDT-SWAP",
)
SYMBOL_LABELS = {symbol: symbol.split("-")[0] for symbol in SYMBOLS}


@dataclass(frozen=True)
class Variant:
    key: str
    label: str
    note: str
    locked_reentry_ema21_near: bool = False
    locked_reentry_min_r: int = 0
    locked_reentry_max_r: int = 0


@dataclass(frozen=True)
class CoinRun:
    symbol: str
    label: str
    trades: list[BacktestTrade]
    test_trades: list[BacktestTrade]
    all_metrics: SplitMetrics
    test_metrics: SplitMetrics
    start_ts: int
    end_ts: int
    candle_count: int


@dataclass(frozen=True)
class VariantRun:
    variant: Variant
    coin_runs: list[CoinRun]
    trades: list[BacktestTrade]
    test_trades: list[BacktestTrade]
    all_metrics: SplitMetrics
    test_metrics: SplitMetrics


VARIANTS = (
    Variant(
        key="original",
        label="原策略",
        note="主回测原始逻辑：动态锁盈后若斜率条件仍成立，可直接再次开空。",
    ),
    Variant(
        key="landed",
        label="落地版",
        note="仅当因 locked_2r_stop 出场时，必须先反抽接近 EMA21（<= 0.3 ATR），再跌回 EMA21 下方，才允许再次开空。",
        locked_reentry_ema21_near=True,
        locked_reentry_min_r=2,
        locked_reentry_max_r=2,
    ),
)


def build_config(symbol: str, variant: Variant) -> StrategyConfig:
    return StrategyConfig(
        inst_id=symbol,
        bar=ENTRY_BAR,
        ema_period=55,
        ema_type="ema",
        trend_ema_period=55,
        trend_ema_type="ema",
        big_ema_period=233,
        atr_period=14,
        atr_stop_multiplier=Decimal("2"),
        # Dynamic TP is used here; set fixed TP multiplier to 0 to avoid legacy
        # protection-plan negatives on very old low-price candles.
        atr_take_multiplier=Decimal("0"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="short_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
        risk_amount=RISK_AMOUNT,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        take_profit_mode="dynamic",
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        ema55_slope_exit_enabled=False,
        ema55_slope_same_bar_reentry_block=False,
        ema55_slope_dynamic_exit_requires_ema_reclaim=False,
        ema55_slope_locked_reentry_requires_ema21_near=variant.locked_reentry_ema21_near,
        ema55_slope_locked_reentry_min_r=variant.locked_reentry_min_r,
        ema55_slope_locked_reentry_max_r=variant.locked_reentry_max_r,
        atr_percentile_filter_max=Decimal("0.5"),
        trend_ema_slope_filter_min_ratio=Decimal("-0.0005"),
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=10,
    )


def build_data_note(symbol: str, candle_count: int) -> str:
    return f"local candle_cache full history | {symbol} {ENTRY_BAR} candles={candle_count}"


def run_coin_variant(client: OkxRestClient, symbol: str, variant: Variant) -> CoinRun:
    candles = [candle for candle in load_candle_cache(symbol, ENTRY_BAR, limit=None) if candle.confirmed]
    if not candles:
        raise RuntimeError(f"missing local candles for {symbol} {ENTRY_BAR}")
    instrument = client.get_instrument(symbol)
    test_bounds = build_split_bounds(len(candles))["test"]
    result = _run_backtest_with_loaded_data(
        candles,
        instrument,
        build_config(symbol, variant),
        data_source_note=build_data_note(symbol, len(candles)),
        taker_fee_rate=SHORT_TAKER_FEE_RATE,
    )
    trades = list(result.trades)
    test_trades = filter_split_trades(trades, test_bounds)
    return CoinRun(
        symbol=symbol,
        label=SYMBOL_LABELS[symbol],
        trades=trades,
        test_trades=test_trades,
        all_metrics=build_metrics(trades),
        test_metrics=build_metrics(test_trades),
        start_ts=candles[0].ts,
        end_ts=candles[-1].ts,
        candle_count=len(candles),
    )


def combine_runs(coin_runs: list[CoinRun]) -> tuple[list[BacktestTrade], list[BacktestTrade], SplitMetrics, SplitMetrics]:
    trades: list[BacktestTrade] = []
    test_trades: list[BacktestTrade] = []
    for item in coin_runs:
        trades.extend(item.trades)
        test_trades.extend(item.test_trades)
    trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
    test_trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
    return trades, test_trades, build_metrics(trades), build_metrics(test_trades)


def run_variant(client: OkxRestClient, variant: Variant) -> VariantRun:
    coin_runs = [run_coin_variant(client, symbol, variant) for symbol in SYMBOLS]
    trades, test_trades, all_metrics, test_metrics = combine_runs(coin_runs)
    return VariantRun(
        variant=variant,
        coin_runs=coin_runs,
        trades=trades,
        test_trades=test_trades,
        all_metrics=all_metrics,
        test_metrics=test_metrics,
    )


def summary_rows(runs: list[VariantRun]) -> list[dict[str, object]]:
    original = next(item for item in runs if item.variant.key == "original")
    rows: list[dict[str, object]] = []
    for run in runs:
        rows.append(
            {
                "variant_key": run.variant.key,
                "variant_label": run.variant.label,
                "all_pnl_u": float(run.all_metrics.pnl),
                "all_trades": run.all_metrics.trades,
                "all_win_rate": float(run.all_metrics.win_rate),
                "all_avg_r": float(run.all_metrics.avg_r),
                "all_profit_factor": None if run.all_metrics.profit_factor is None else float(run.all_metrics.profit_factor),
                "all_drawdown_u": float(run.all_metrics.max_drawdown),
                "test_pnl_u": float(run.test_metrics.pnl),
                "test_trades": run.test_metrics.trades,
                "test_win_rate": float(run.test_metrics.win_rate),
                "test_avg_r": float(run.test_metrics.avg_r),
                "test_profit_factor": None if run.test_metrics.profit_factor is None else float(run.test_metrics.profit_factor),
                "test_drawdown_u": float(run.test_metrics.max_drawdown),
                "test_delta_vs_original_u": float(run.test_metrics.pnl - original.test_metrics.pnl),
                "all_delta_vs_original_u": float(run.all_metrics.pnl - original.all_metrics.pnl),
                "note": run.variant.note,
            }
        )
    return rows


def coin_rows(runs: list[VariantRun]) -> list[dict[str, object]]:
    original = next(item for item in runs if item.variant.key == "original")
    original_map = {coin.symbol: coin for coin in original.coin_runs}
    rows: list[dict[str, object]] = []
    for run in runs:
        for coin in run.coin_runs:
            base_coin = original_map[coin.symbol]
            rows.append(
                {
                    "variant_label": run.variant.label,
                    "coin": coin.label,
                    "all_pnl_u": float(coin.all_metrics.pnl),
                    "test_pnl_u": float(coin.test_metrics.pnl),
                    "test_delta_vs_original_u": float(coin.test_metrics.pnl - base_coin.test_metrics.pnl),
                    "all_delta_vs_original_u": float(coin.all_metrics.pnl - base_coin.all_metrics.pnl),
                    "test_trades": coin.test_metrics.trades,
                    "all_trades": coin.all_metrics.trades,
                }
            )
    return rows


def build_html(summary: pd.DataFrame, coin_frame: pd.DataFrame, runs: list[VariantRun]) -> str:
    original = summary[summary["variant_key"] == "original"].iloc[0]
    landed = summary[summary["variant_key"] == "landed"].iloc[0]
    total_chart = render_bar_chart(summary, "all_pnl_u", "全样本总盈亏", "#145c54")
    test_chart = render_bar_chart(summary, "test_pnl_u", "测试段总盈亏", "#b86b3f")
    dd_chart = render_bar_chart(summary, "test_drawdown_u", "测试段最大回撤", "#667085")
    heatmap = render_coin_heatmap(coin_frame, "test_pnl_u", "各币种测试段 PnL")
    summary_table = dataframe_to_html(
        summary[
            [
                "variant_label",
                "all_pnl_u",
                "all_trades",
                "all_profit_factor",
                "all_drawdown_u",
                "test_pnl_u",
                "test_trades",
                "test_profit_factor",
                "test_drawdown_u",
                "test_delta_vs_original_u",
            ]
        ],
        float_cols={
            "all_pnl_u": 1,
            "all_profit_factor": 2,
            "all_drawdown_u": 1,
            "test_pnl_u": 1,
            "test_profit_factor": 2,
            "test_drawdown_u": 1,
            "test_delta_vs_original_u": 1,
        },
    )
    coin_table = dataframe_to_html(
        coin_frame.sort_values(["coin", "variant_label"]),
        float_cols={
            "all_pnl_u": 1,
            "test_pnl_u": 1,
            "test_delta_vs_original_u": 1,
            "all_delta_vs_original_u": 1,
        },
    )
    data_lines = "".join(
        f"<li><strong>{html.escape(coin.label)}</strong>: {html.escape(format_ts(coin.start_ts))} -> {html.escape(format_ts(coin.end_ts))}, 1H={coin.candle_count}</li>"
        for coin in runs[0].coin_runs
    )
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>EMA55 斜率做空 落地版 vs 原策略</title>
  <style>
    :root {{
      --bg:#f6f7fb; --panel:#fff; --ink:#1a2333; --muted:#667085; --line:rgba(26,35,51,.10);
      --accent:#145c54; --accent2:#b86b3f; --shadow:0 18px 38px rgba(15,23,42,.08);
    }}
    * {{ box-sizing:border-box; }}
    body {{
      margin:0; color:var(--ink); font-family:"Microsoft YaHei","Segoe UI",sans-serif;
      background:
        radial-gradient(circle at 12% 0%, rgba(20,92,84,.11), transparent 28%),
        radial-gradient(circle at 88% 8%, rgba(184,107,63,.10), transparent 24%),
        linear-gradient(180deg,#fbfcfe 0%,var(--bg) 100%);
    }}
    .wrap {{ width:min(1220px,calc(100vw - 30px)); margin:0 auto; padding:28px 0 56px; }}
    .hero {{ border-radius:28px; padding:30px; color:#fff; background:linear-gradient(135deg,#145c54,#263445); box-shadow:var(--shadow); }}
    .hero h1 {{ margin:10px 0 8px; font-size:34px; line-height:1.08; }}
    .hero p {{ margin:8px 0 0; max-width:940px; line-height:1.72; color:rgba(255,255,255,.9); }}
    .eyebrow {{ font-size:12px; text-transform:uppercase; letter-spacing:.16em; opacity:.82; }}
    .grid {{ display:grid; grid-template-columns:repeat(12,1fr); gap:18px; margin-top:20px; }}
    .card {{ background:var(--panel); border:1px solid var(--line); border-radius:22px; box-shadow:var(--shadow); padding:22px; }}
    .stat {{ grid-column:span 3; }} .wide {{ grid-column:span 6; }} .full {{ grid-column:1/-1; }}
    .k {{ color:var(--muted); font-size:13px; }} .v {{ font-size:28px; font-weight:800; margin-top:8px; color:var(--accent); }}
    .s,.note {{ color:var(--muted); font-size:13px; line-height:1.65; margin-top:8px; }}
    h2 {{ margin:0 0 12px; font-size:20px; }} p {{ margin:0; line-height:1.7; }} ul {{ margin:0; padding-left:18px; line-height:1.8; }}
    img {{ width:100%; border-radius:16px; border:1px solid var(--line); background:#fff; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; }}
    th,td {{ text-align:left; padding:10px 8px; border-bottom:1px solid rgba(26,35,51,.08); white-space:nowrap; }}
    th {{ color:var(--muted); background:#f7fafc; position:sticky; top:0; }} .scroll {{ overflow:auto; }}
    @media(max-width:960px) {{ .stat,.wide {{ grid-column:1/-1; }} .hero h1 {{ font-size:28px; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div class="eyebrow">Slope Short / Landed Compare</div>
      <h1>落地版 vs 原策略</h1>
      <p>正式主回测逻辑已经落地：仅当 `locked_2r_stop` 出场时，必须先反抽接近 EMA21（<= 0.3 ATR），再跌回 EMA21 下方，才允许再次开空。这里用正式回测接口，重新跑 5 币种全样本，并和原策略直接对比。</p>
    </section>
    <section class="grid">
      <div class="card stat"><div class="k">原策略测试段</div><div class="v">{original['test_pnl_u']:.1f}U</div><div class="s">回撤 {original['test_drawdown_u']:.1f}U</div></div>
      <div class="card stat"><div class="k">落地版测试段</div><div class="v">{landed['test_pnl_u']:.1f}U</div><div class="s">回撤 {landed['test_drawdown_u']:.1f}U</div></div>
      <div class="card stat"><div class="k">测试段净差</div><div class="v">{landed['test_delta_vs_original_u']:+.1f}U</div><div class="s">全样本净差 {landed['all_delta_vs_original_u']:+.1f}U</div></div>
      <div class="card stat"><div class="k">核心结论</div><div class="v">{'更稳' if landed['test_drawdown_u'] < original['test_drawdown_u'] else '不更稳'}</div><div class="s">看收益和回撤是否一起改善</div></div>

      <div class="card wide"><h2>落地规则</h2><ul>
        <li>只处理 `locked_2r_stop` 这一档锁盈后再入场</li>
        <li>出场后必须先接近 `EMA21`，阈值是 `0.3 ATR`</li>
        <li>之后只有再次跌回 `EMA21` 下方，才允许下一次做空</li>
      </ul></div>
      <div class="card wide"><h2>数据覆盖</h2><ul>{data_lines}</ul></div>

      <div class="card wide"><h2>全样本总盈亏</h2><img src="data:image/png;base64,{total_chart}" alt="all_pnl"></div>
      <div class="card wide"><h2>测试段总盈亏</h2><img src="data:image/png;base64,{test_chart}" alt="test_pnl"></div>
      <div class="card wide"><h2>测试段最大回撤</h2><img src="data:image/png;base64,{dd_chart}" alt="drawdown"></div>
      <div class="card wide"><h2>各币种测试段 PnL</h2><img src="data:image/png;base64,{heatmap}" alt="heatmap"></div>

      <div class="card full"><h2>模式总表</h2><div class="scroll">{summary_table}</div></div>
      <div class="card full"><h2>币种明细</h2><div class="scroll">{coin_table}</div></div>
    </section>
  </div>
</body>
</html>"""


def render_bar_chart(frame: pd.DataFrame, column: str, title: str, color: str) -> str:
    plot_frame = frame.sort_values(column, ascending=True)
    fig, ax = plt.subplots(figsize=(9.8, 4.8))
    ax.barh(plot_frame["variant_label"], plot_frame[column], color=color)
    ax.set_title(title)
    ax.set_xlabel("PnL (U)" if "pnl" in column else "Drawdown (U)")
    fig.tight_layout()
    return figure_to_base64(fig)


def render_coin_heatmap(frame: pd.DataFrame, column: str, title: str) -> str:
    pivot = frame.pivot(index="coin", columns="variant_label", values=column).reindex(index=[SYMBOL_LABELS[s] for s in SYMBOLS])
    fig, ax = plt.subplots(figsize=(8.8, 4.4))
    image = ax.imshow(pivot.to_numpy(dtype=float), cmap="RdYlGn", aspect="auto")
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=0)
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    ax.set_title(title)
    for row_index in range(len(pivot.index)):
        for col_index in range(len(pivot.columns)):
            value = pivot.iloc[row_index, col_index]
            ax.text(col_index, row_index, f"{float(value):.1f}", ha="center", va="center", fontsize=9, color="#111827")
    fig.colorbar(image, ax=ax, shrink=0.8)
    fig.tight_layout()
    return figure_to_base64(fig)


def dataframe_to_html(frame: pd.DataFrame, *, float_cols: dict[str, int]) -> str:
    headers = "".join(f"<th>{html.escape(str(col))}</th>" for col in frame.columns)
    rows = []
    for _, row in frame.iterrows():
        cells = []
        for col in frame.columns:
            value = row[col]
            if col in float_cols:
                text = "-" if pd.isna(value) else f"{float(value):.{float_cols[col]}f}"
            else:
                text = "-" if pd.isna(value) else str(value)
            cells.append(f"<td>{html.escape(text)}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{headers}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def figure_to_base64(fig: plt.Figure) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=160, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def main() -> None:
    client = OkxRestClient()
    runs = [run_variant(client, variant) for variant in VARIANTS]
    summary = pd.DataFrame(summary_rows(runs))
    coin_frame = pd.DataFrame(coin_rows(runs))
    CSV_PATH.write_text(summary.to_csv(index=False), encoding="utf-8-sig")
    COIN_CSV_PATH.write_text(coin_frame.to_csv(index=False), encoding="utf-8-sig")
    JSON_PATH.write_text(
        json.dumps(
            {
                "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                "summary_rows": summary.to_dict("records"),
                "coin_rows": coin_frame.to_dict("records"),
                "variants": [variant.__dict__ for variant in VARIANTS],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    HTML_PATH.write_text(build_html(summary, coin_frame, runs), encoding="utf-8")
    print(HTML_PATH)


if __name__ == "__main__":
    main()
