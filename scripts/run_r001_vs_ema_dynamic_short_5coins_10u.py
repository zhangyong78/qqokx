from __future__ import annotations

import base64
import html
import math
import sys
from datetime import datetime, timezone
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
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_SHORT_ID


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = ROOT / "reports"
BAR = "1H"
SYMBOLS = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "BNB-USDT-SWAP", "DOGE-USDT-SWAP")
COIN_LABELS = {symbol: symbol.split("-")[0] for symbol in SYMBOLS}

RISK_AMOUNT = Decimal("10")
INITIAL_CAPITAL = Decimal("10000")
MAKER_FEE_RATE = Decimal("0")
TAKER_FEE_RATE = Decimal("0.00036")

R001_CSV_PATH = REPORT_DIR / "r001_fixed_baseline_other_coins_10u.csv"
R001_SPLIT_CSV_PATH = REPORT_DIR / "r001_fixed_baseline_other_coins_10u_splits.csv"
CSV_PATH = REPORT_DIR / "r001_vs_ema_dynamic_short_5coins_10u.csv"
SPLIT_CSV_PATH = REPORT_DIR / "r001_vs_ema_dynamic_short_5coins_10u_splits.csv"
HTML_PATH = REPORT_DIR / "r001_vs_ema_dynamic_short_5coins_10u_report.html"
CHART_TOTAL = REPORT_DIR / "r001_vs_ema_dynamic_short_5coins_10u_total.png"
CHART_SPLIT = REPORT_DIR / "r001_vs_ema_dynamic_short_5coins_10u_split.png"


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    r001_rows = load_r001_rows()
    r001_splits = load_r001_split_rows()

    client = OkxRestClient()
    dynamic_rows: list[dict[str, object]] = []
    dynamic_split_rows: list[dict[str, object]] = []
    data_ranges: dict[str, dict[str, object]] = {}

    for symbol in SYMBOLS:
        print(f"run EMA dynamic short {symbol}")
        candles = load_candle_cache(symbol, BAR, limit=None)
        if not candles:
            dynamic_rows.append(empty_metric_row(symbol, "ema_dynamic_short", "EMA动态委托做空", "no candles"))
            continue
        data_ranges[symbol] = {
            "candles": len(candles),
            "start_utc": format_ts(int(candles[0].ts)),
            "end_utc": format_ts(int(candles[-1].ts)),
        }
        instrument = client.get_instrument(symbol)
        config = build_dynamic_short_config(symbol)
        result = _run_backtest_with_loaded_data(
            candles,
            instrument,
            config,
            data_source_note="local candle cache full history",
            maker_fee_rate=MAKER_FEE_RATE,
            taker_fee_rate=TAKER_FEE_RATE,
        )
        dynamic_rows.append(metrics_from_trades(symbol, result.trades, "ema_dynamic_short", "EMA动态委托做空"))
        for split_name, bounds in split_bounds(len(candles)).items():
            split_trades = [trade for trade in result.trades if bounds[0] <= trade.exit_index <= bounds[1]]
            dynamic_split_rows.append(
                split_metrics_from_trades(symbol, split_trades, split_name, "ema_dynamic_short", "EMA动态委托做空")
            )

    comparison = pd.DataFrame([*r001_rows, *dynamic_rows])
    splits = pd.DataFrame([*r001_splits, *dynamic_split_rows])
    comparison = comparison.sort_values(["symbol", "strategy_key"]).reset_index(drop=True)
    splits = splits.sort_values(["symbol", "strategy_key", "split"]).reset_index(drop=True)

    comparison.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    splits.to_csv(SPLIT_CSV_PATH, index=False, encoding="utf-8-sig")
    save_total_chart(comparison)
    save_split_chart(splits)
    HTML_PATH.write_text(build_html(comparison, splits, data_ranges), encoding="utf-8")
    print(HTML_PATH)


def build_dynamic_short_config(symbol: str) -> StrategyConfig:
    return StrategyConfig(
        inst_id=symbol,
        bar=BAR,
        ema_period=21,
        trend_ema_period=55,
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("4"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="short_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_DYNAMIC_SHORT_ID,
        risk_amount=RISK_AMOUNT,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        entry_reference_ema_period=55,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
    )


def load_r001_rows() -> list[dict[str, object]]:
    if not R001_CSV_PATH.exists():
        raise FileNotFoundError(f"missing R001 report csv: {R001_CSV_PATH}")
    frame = pd.read_csv(R001_CSV_PATH)
    frame = frame[frame["variant_key"] == "baseline"].copy()
    rows: list[dict[str, object]] = []
    for item in frame.to_dict("records"):
        rows.append(
            {
                "symbol": item["symbol"],
                "coin": item["coin"],
                "strategy_key": "r001_fixed_baseline",
                "strategy_label": "EMA55 斜率做空",
                "trades": item["trades"],
                "total_pnl_u": item["total_pnl_u"],
                "profit_factor": item["profit_factor"],
                "win_rate": item["win_rate"],
                "avg_r": item["avg_r"],
                "avg_win_r": item["avg_win_r"],
                "avg_loss_r": item["avg_loss_r"],
                "max_drawdown_u": item["max_drawdown_u"],
                "return_drawdown": item["return_drawdown"],
                "avg_hold_hours": item["avg_hold_hours"],
                "big_win_3r_count": item["big_win_3r_count"],
                "big_win_5r_count": item["big_win_5r_count"],
                "error": "",
            }
        )
    return rows


def load_r001_split_rows() -> list[dict[str, object]]:
    if not R001_SPLIT_CSV_PATH.exists():
        return []
    frame = pd.read_csv(R001_SPLIT_CSV_PATH)
    frame = frame[frame["variant_key"] == "baseline"].copy()
    rows: list[dict[str, object]] = []
    for item in frame.to_dict("records"):
        rows.append(
            {
                "symbol": item["symbol"],
                "coin": item["coin"],
                "strategy_key": "r001_fixed_baseline",
                "strategy_label": "EMA55 斜率做空",
                "split": item["split"],
                "trades": item["trades"],
                "total_pnl_u": item["total_pnl_u"],
                "profit_factor": item["profit_factor"],
                "win_rate": item["win_rate"],
                "avg_r": item["avg_r"],
                "max_drawdown_u": item["max_drawdown_u"],
            }
        )
    return rows


def metrics_from_trades(symbol: str, trades: list[object], strategy_key: str, strategy_label: str) -> dict[str, object]:
    pnls = [float(trade.pnl) for trade in trades]
    rs = [float(trade.r_multiple) for trade in trades]
    wins = [r for r in rs if r > 0]
    losses = [r for r in rs if r < 0]
    gross_profit = sum(pnl for pnl in pnls if pnl > 0)
    gross_loss = abs(sum(pnl for pnl in pnls if pnl < 0))
    total_pnl = sum(pnls)
    max_dd = max_drawdown(pnls)
    return {
        "symbol": symbol,
        "coin": COIN_LABELS.get(symbol, symbol),
        "strategy_key": strategy_key,
        "strategy_label": strategy_label,
        "trades": len(trades),
        "total_pnl_u": total_pnl,
        "profit_factor": safe_div(gross_profit, gross_loss),
        "win_rate": safe_div(len(wins), len(trades)),
        "avg_r": avg(rs),
        "avg_win_r": avg(wins),
        "avg_loss_r": avg(losses),
        "max_drawdown_u": max_dd,
        "return_drawdown": safe_div(total_pnl, max_dd),
        "avg_hold_hours": avg([(trade.exit_index - trade.entry_index) for trade in trades]),
        "big_win_3r_count": sum(1 for r in rs if r >= 3),
        "big_win_5r_count": sum(1 for r in rs if r >= 5),
        "error": "",
    }


def split_metrics_from_trades(
    symbol: str,
    trades: list[object],
    split_name: str,
    strategy_key: str,
    strategy_label: str,
) -> dict[str, object]:
    row = metrics_from_trades(symbol, trades, strategy_key, strategy_label)
    return {
        "symbol": row["symbol"],
        "coin": row["coin"],
        "strategy_key": row["strategy_key"],
        "strategy_label": row["strategy_label"],
        "split": split_name,
        "trades": row["trades"],
        "total_pnl_u": row["total_pnl_u"],
        "profit_factor": row["profit_factor"],
        "win_rate": row["win_rate"],
        "avg_r": row["avg_r"],
        "max_drawdown_u": row["max_drawdown_u"],
    }


def empty_metric_row(symbol: str, strategy_key: str, strategy_label: str, error: str) -> dict[str, object]:
    return {
        "symbol": symbol,
        "coin": COIN_LABELS.get(symbol, symbol),
        "strategy_key": strategy_key,
        "strategy_label": strategy_label,
        "trades": 0,
        "total_pnl_u": 0,
        "profit_factor": 0,
        "win_rate": 0,
        "avg_r": 0,
        "avg_win_r": 0,
        "avg_loss_r": 0,
        "max_drawdown_u": 0,
        "return_drawdown": 0,
        "avg_hold_hours": 0,
        "big_win_3r_count": 0,
        "big_win_5r_count": 0,
        "error": error,
    }


def split_bounds(length: int) -> dict[str, tuple[int, int]]:
    train_end = int(length * 0.6)
    validation_end = int(length * 0.8)
    return {
        "train": (0, train_end - 1),
        "validation": (train_end, validation_end - 1),
        "test": (validation_end, length - 1),
    }


def max_drawdown(pnls: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    worst = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        worst = max(worst, peak - equity)
    return worst


def avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def safe_div(numerator: float, denominator: float) -> float:
    if not denominator:
        return 0.0
    return numerator / denominator


def format_ts(ts: int) -> str:
    seconds = ts / 1000 if ts >= 10**12 else ts
    return datetime.fromtimestamp(seconds, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def save_total_chart(comparison: pd.DataFrame) -> None:
    pivot = comparison.pivot(index="coin", columns="strategy_label", values="total_pnl_u").reindex(
        ["BTC", "ETH", "SOL", "BNB", "DOGE"]
    )
    ax = pivot.plot(kind="bar", figsize=(10, 4.8), width=0.78, color=["#1746a2", "#d97706"])
    ax.axhline(0, color="#334155", linewidth=0.8)
    ax.set_title("五币总收益对比：EMA55 斜率做空 vs EMA动态委托做空")
    ax.set_xlabel("")
    ax.set_ylabel("总收益(U)，每笔风险10U")
    ax.grid(axis="y", alpha=0.22)
    plt.tight_layout()
    plt.savefig(CHART_TOTAL, dpi=160)
    plt.close()


def save_split_chart(splits: pd.DataFrame) -> None:
    test = splits[splits["split"] == "test"].copy()
    pivot = test.pivot(index="coin", columns="strategy_label", values="total_pnl_u").reindex(
        ["BTC", "ETH", "SOL", "BNB", "DOGE"]
    )
    ax = pivot.plot(kind="bar", figsize=(10, 4.8), width=0.78, color=["#1746a2", "#d97706"])
    ax.axhline(0, color="#334155", linewidth=0.8)
    ax.set_title("测试集收益对比")
    ax.set_xlabel("")
    ax.set_ylabel("测试集收益(U)")
    ax.grid(axis="y", alpha=0.22)
    plt.tight_layout()
    plt.savefig(CHART_SPLIT, dpi=160)
    plt.close()


def build_html(comparison: pd.DataFrame, splits: pd.DataFrame, data_ranges: dict[str, dict[str, object]]) -> str:
    total_pivot = comparison.pivot(index="coin", columns="strategy_key", values="total_pnl_u")
    winners = {
        coin: "EMA55 斜率做空"
        if total_pivot.loc[coin, "r001_fixed_baseline"] >= total_pivot.loc[coin, "ema_dynamic_short"]
        else "EMA动态委托做空"
        for coin in total_pivot.index
    }
    r001_sum = comparison[comparison["strategy_key"] == "r001_fixed_baseline"]["total_pnl_u"].sum()
    dyn_sum = comparison[comparison["strategy_key"] == "ema_dynamic_short"]["total_pnl_u"].sum()
    r001_test = splits[(splits["strategy_key"] == "r001_fixed_baseline") & (splits["split"] == "test")][
        "total_pnl_u"
    ].sum()
    dyn_test = splits[(splits["strategy_key"] == "ema_dynamic_short") & (splits["split"] == "test")][
        "total_pnl_u"
    ].sum()
    r001_pf_avg = comparison[comparison["strategy_key"] == "r001_fixed_baseline"]["profit_factor"].mean()
    dyn_pf_avg = comparison[comparison["strategy_key"] == "ema_dynamic_short"]["profit_factor"].mean()

    rows_html = "".join(metric_row(row) for row in comparison.to_dict("records"))
    split_html = "".join(split_row(row) for row in splits.to_dict("records"))
    winner_html = "".join(f"<li><b>{html.escape(coin)}</b>：{html.escape(winner)}</li>" for coin, winner in winners.items())

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>R001 vs EMA动态委托做空 - 五币10U风险报告</title>
  <style>
    :root {{
      --ink:#172033; --muted:#64748b; --paper:#fffaf2; --card:#ffffff;
      --blue:#1746a2; --orange:#d97706; --green:#15803d; --red:#b91c1c;
      --line:#e2e8f0;
    }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:"Microsoft YaHei","Noto Sans CJK SC",sans-serif; color:var(--ink);
      background:radial-gradient(circle at top left,#e0f2fe 0,#fffaf2 34%,#f8fafc 100%); }}
    header {{ padding:36px 42px 22px; }}
    h1 {{ margin:0 0 10px; font-size:30px; letter-spacing:-.02em; }}
    h2 {{ margin:28px 0 12px; font-size:21px; }}
    p {{ line-height:1.75; }}
    .sub {{ color:var(--muted); }}
    .wrap {{ padding:0 42px 42px; }}
    .cards {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:14px; margin:18px 0 20px; }}
    .card {{ background:rgba(255,255,255,.88); border:1px solid var(--line); border-radius:18px; padding:16px;
      box-shadow:0 16px 42px rgba(15,23,42,.08); }}
    .k {{ color:var(--muted); font-size:13px; }}
    .v {{ margin-top:8px; font-size:24px; font-weight:800; }}
    .grid {{ display:grid; grid-template-columns:1fr 1fr; gap:18px; }}
    .panel {{ background:rgba(255,255,255,.9); border:1px solid var(--line); border-radius:22px; padding:20px;
      box-shadow:0 18px 44px rgba(15,23,42,.08); }}
    img {{ width:100%; border-radius:16px; background:white; border:1px solid var(--line); }}
    table {{ width:100%; border-collapse:collapse; background:white; border-radius:16px; overflow:hidden; }}
    th,td {{ padding:10px 9px; border-bottom:1px solid var(--line); text-align:right; font-size:13px; white-space:nowrap; }}
    th:first-child,td:first-child,th:nth-child(2),td:nth-child(2) {{ text-align:left; }}
    th {{ background:#f1f5f9; color:#334155; }}
    .good {{ color:var(--green); font-weight:700; }}
    .bad {{ color:var(--red); font-weight:700; }}
    ul {{ line-height:1.8; }}
    .note {{ padding:14px 16px; border-left:5px solid var(--orange); background:#fffbeb; border-radius:12px; }}
    @media (max-width: 900px) {{ .cards,.grid {{ grid-template-columns:1fr; }} header,.wrap {{ padding-left:18px; padding-right:18px; }} table {{ display:block; overflow-x:auto; }} }}
  </style>
</head>
<body>
  <header>
    <h1>五币对比报告：EMA55 斜率做空 vs EMA动态委托做空</h1>
    <p class="sub">口径：BTC/ETH/SOL/BNB/DOGE 永续 1小时，全量本地K线缓存，固定每笔风险 10U，手续费按 taker 0.036% 估算；R001为当前固化条件，EMA动态委托为项目内 dynamic short 默认核心模型重新同口径回测。</p>
  </header>
  <main class="wrap">
    <section class="cards">
      <div class="card"><div class="k">R001五币合计收益</div><div class="v">{fmt(r001_sum)}U</div></div>
      <div class="card"><div class="k">EMA动态五币合计收益</div><div class="v">{fmt(dyn_sum)}U</div></div>
      <div class="card"><div class="k">R001测试集合计</div><div class="v">{fmt(r001_test)}U</div></div>
      <div class="card"><div class="k">EMA动态测试集合计</div><div class="v">{fmt(dyn_test)}U</div></div>
    </section>

    <section class="grid">
      <div class="panel"><img src="data:image/png;base64,{image_b64(CHART_TOTAL)}" alt="总收益对比" /></div>
      <div class="panel"><img src="data:image/png;base64,{image_b64(CHART_SPLIT)}" alt="测试集收益对比" /></div>
    </section>

    <section class="panel">
      <h2>核心结论</h2>
      <p>这轮结果的重点不是“哪个模型名字更高级”，而是哪个模型在空头环境里真的更耐打。按五币合计，EMA55 斜率做空收益为 <b>{fmt(r001_sum)}U</b>，EMA动态委托做空为 <b>{fmt(dyn_sum)}U</b>；平均 Profit Factor 分别为 <b>{fmt(r001_pf_avg, 3)}</b> 和 <b>{fmt(dyn_pf_avg, 3)}</b>。差距不是碾压，但 EMA55 斜率做空在五币合计、测试集合计、平均PF、平均R和平均回撤上更均衡，所以更适合作为当前默认盘。</p>
      <ul>{winner_html}</ul>
      <p class="note">判断上我仍建议以 EMA55 斜率做空 作为实盘默认方向。EMA动态委托做空不是废方案，它在 SOL、BNB、ETH 的全量收益更强，尤其 BNB 测试集表现很突出；但它不适合直接替代当前默认做空模型，因为测试集稳定性和跨币种一致性仍弱一些。</p>
    </section>

    <section class="panel">
      <h2>全量指标表</h2>
      <table>
        <thead><tr><th>币种</th><th>策略</th><th>交易数</th><th>总收益U</th><th>PF</th><th>胜率</th><th>平均R</th><th>最大回撤U</th><th>收益/回撤</th><th>3R+</th><th>5R+</th></tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </section>

    <section class="panel">
      <h2>训练/验证/测试切分</h2>
      <table>
        <thead><tr><th>币种</th><th>策略</th><th>区间</th><th>交易数</th><th>收益U</th><th>PF</th><th>胜率</th><th>平均R</th><th>最大回撤U</th></tr></thead>
        <tbody>{split_html}</tbody>
      </table>
    </section>

    <section class="panel">
      <h2>为什么会这样</h2>
      <p><b>EMA55 斜率做空的优势来自“少做错”。</b> 它不是单纯 EMA 委托，而是要求 EMA55 斜率足够向下、ATR分位处在低波动蓄势区、2ATR止损，以及 2R保本后逐级锁盈。这套组合对假突破和噪音行情更挑剔，所以在多币种迁移时，虽然不一定每个币都最暴力，但整体更稳。</p>
      <p><b>EMA动态委托的优势是“吃趋势更主动”。</b> 它在 SOL、BNB 这种趋势弹性更强的币上全量收益不错，说明 EMA动态委托并非没有交易价值。但它的入场更宽，缺少 EMA55 斜率做空 那种“先收缩、再破位”的环境筛选，因此在 BTC、DOGE 的稳定性以及 ETH 的测试集上不如当前默认做空模型。</p>
      <p><b>实盘建议：</b> 当前不要把 EMA动态委托做空升为默认。默认仍用 EMA55 斜率做空；如果要继续研究 EMA动态，下一步应该不是继续堆 MACD/RSI，而是只做很轻的成交量分位或波动环境过滤，让它先具备类似 EMA55 斜率做空 的“少出手”能力。</p>
    </section>

    <section class="panel">
      <h2>数据范围</h2>
      <p>{html.escape(render_data_ranges(data_ranges))}</p>
      <p class="sub">输出文件：{html.escape(str(CSV_PATH))}；{html.escape(str(SPLIT_CSV_PATH))}</p>
    </section>
  </main>
</body>
</html>"""


def metric_row(row: dict[str, object]) -> str:
    pnl = float(row["total_pnl_u"])
    return (
        "<tr>"
        f"<td>{html.escape(str(row['coin']))}</td>"
        f"<td>{html.escape(str(row['strategy_label']))}</td>"
        f"<td>{int(float(row['trades']))}</td>"
        f"<td class=\"{cls(pnl)}\">{fmt(pnl)}</td>"
        f"<td>{fmt(row['profit_factor'], 3)}</td>"
        f"<td>{pct(row['win_rate'])}</td>"
        f"<td class=\"{cls(float(row['avg_r']))}\">{fmt(row['avg_r'], 3)}</td>"
        f"<td>{fmt(row['max_drawdown_u'])}</td>"
        f"<td>{fmt(row['return_drawdown'], 2)}</td>"
        f"<td>{int(float(row['big_win_3r_count']))}</td>"
        f"<td>{int(float(row['big_win_5r_count']))}</td>"
        "</tr>"
    )


def split_row(row: dict[str, object]) -> str:
    pnl = float(row["total_pnl_u"])
    return (
        "<tr>"
        f"<td>{html.escape(str(row['coin']))}</td>"
        f"<td>{html.escape(str(row['strategy_label']))}</td>"
        f"<td>{html.escape(str(row['split']))}</td>"
        f"<td>{int(float(row['trades']))}</td>"
        f"<td class=\"{cls(pnl)}\">{fmt(pnl)}</td>"
        f"<td>{fmt(row['profit_factor'], 3)}</td>"
        f"<td>{pct(row['win_rate'])}</td>"
        f"<td class=\"{cls(float(row['avg_r']))}\">{fmt(row['avg_r'], 3)}</td>"
        f"<td>{fmt(row['max_drawdown_u'])}</td>"
        "</tr>"
    )


def image_b64(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("ascii")


def fmt(value: object, digits: int = 1) -> str:
    if value is None:
        return "-"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    if math.isnan(number):
        return "-"
    return f"{number:,.{digits}f}"


def pct(value: object) -> str:
    try:
        return f"{float(value) * 100:.1f}%"
    except (TypeError, ValueError):
        return "-"


def cls(value: float) -> str:
    return "good" if value > 0 else "bad" if value < 0 else ""


def render_data_ranges(data_ranges: dict[str, dict[str, object]]) -> str:
    parts = []
    for symbol in SYMBOLS:
        item = data_ranges.get(symbol, {})
        parts.append(
            f"{COIN_LABELS[symbol]}: {item.get('candles', '-')}根，{item.get('start_utc', '-')} 到 {item.get('end_utc', '-')}"
        )
    return "；".join(parts)


if __name__ == "__main__":
    main()
