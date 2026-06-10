from __future__ import annotations

import csv
import html
import sys
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import (
    BacktestResult,
    _run_backtest_with_loaded_data,
    summarize_trade_exit_reasons,
)
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.pricing import format_decimal, format_decimal_fixed
from okx_quant.strategy_catalog import STRATEGY_BTC_EMA55_SLOPE_SHORT_ID


SYMBOL = "BTC-USDT-SWAP"
BAR = "1H"
CANDLE_LIMIT = 10_000
INITIAL_CAPITAL = Decimal("10000")
RISK_AMOUNT = Decimal("100")
SHORT_TAKER_FEE_RATE = Decimal("0.00036")
SLOPE_THRESHOLDS = (
    Decimal("-0.0001"),
    Decimal("-0.0002"),
    Decimal("-0.0003"),
    Decimal("-0.0004"),
    Decimal("-0.0005"),
)
NEGATIVE_BARS_OPTIONS = tuple(range(1, 9))

REPORT_DIR = ROOT / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASENAME = f"btc_ema55_slope_short_threshold_matrix_100u_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASENAME}.html"
CSV_PATH = REPORT_DIR / f"{BASENAME}.csv"
LATEST_HTML_PATH = REPORT_DIR / "btc_ema55_slope_short_threshold_matrix_100u_latest.html"
LATEST_CSV_PATH = REPORT_DIR / "btc_ema55_slope_short_threshold_matrix_100u_latest.csv"


@dataclass(frozen=True)
class RunRow:
    slope_threshold: Decimal
    negative_bars: int
    trades: int
    wins: int
    losses: int
    breakeven: int
    win_rate_pct: Decimal
    total_pnl: Decimal
    return_pct: Decimal
    ending_equity: Decimal
    max_drawdown: Decimal
    max_drawdown_pct: Decimal
    profit_factor: Decimal | None
    avg_r: Decimal
    avg_pnl: Decimal
    average_win: Decimal
    average_loss: Decimal
    stop_loss_hits: int
    slope_turn_positive_hits: int
    exit_summary: str


def main() -> None:
    client = OkxRestClient()
    instrument = client.get_instrument(SYMBOL)
    candles = client.get_candles_history(SYMBOL, BAR, limit=CANDLE_LIMIT)
    if not candles:
        raise RuntimeError(f"未取到 {SYMBOL} {BAR} K 线数据")

    stats = getattr(client, "last_candle_history_stats", {}) or {}
    data_source_note = build_data_source_note(stats, returned_count=len(candles))
    range_start = format_ts(candles[0].ts)
    range_end = format_ts(candles[-1].ts)

    rows: list[RunRow] = []
    for slope_threshold in SLOPE_THRESHOLDS:
        for negative_bars in NEGATIVE_BARS_OPTIONS:
            config = build_config(slope_threshold, negative_bars)
            result = _run_backtest_with_loaded_data(
                candles,
                instrument,
                config,
                data_source_note=data_source_note,
                taker_fee_rate=SHORT_TAKER_FEE_RATE,
            )
            rows.append(build_row(result, slope_threshold=slope_threshold, negative_bars=negative_bars))

    rows_sorted = sorted(
        rows,
        key=lambda item: (
            item.total_pnl,
            -item.max_drawdown_pct,
            item.profit_factor if item.profit_factor is not None else Decimal("-1"),
            item.trades,
        ),
        reverse=True,
    )
    best_return = rows_sorted[0]
    low_drawdown_rows = [row for row in rows if row.max_drawdown_pct <= Decimal("1.00")]
    best_low_drawdown = max(low_drawdown_rows or rows, key=lambda item: (item.total_pnl, item.trades))
    most_trades = max(rows, key=lambda item: (item.trades, item.total_pnl))

    write_csv(rows_sorted)
    html_text = build_html(
        rows=rows,
        rows_sorted=rows_sorted,
        range_start=range_start,
        range_end=range_end,
        candle_count=len(candles),
        data_source_note=data_source_note,
        best_return=best_return,
        best_low_drawdown=best_low_drawdown,
        most_trades=most_trades,
    )
    HTML_PATH.write_text(html_text, encoding="utf-8")
    LATEST_HTML_PATH.write_text(html_text, encoding="utf-8")

    print(HTML_PATH)
    print(CSV_PATH)
    print(f"BEST_RETURN={best_return.slope_threshold},{best_return.negative_bars},{best_return.total_pnl}")
    print(
        f"BEST_LOW_DD={best_low_drawdown.slope_threshold},"
        f"{best_low_drawdown.negative_bars},{best_low_drawdown.total_pnl}"
    )
    print(f"MOST_TRADES={most_trades.slope_threshold},{most_trades.negative_bars},{most_trades.trades}")


def build_config(slope_threshold: Decimal, negative_bars: int) -> StrategyConfig:
    return StrategyConfig(
        inst_id=SYMBOL,
        bar=BAR,
        ema_period=55,
        trend_ema_period=55,
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("2"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="short_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_BTC_EMA55_SLOPE_SHORT_ID,
        take_profit_mode="fixed",
        risk_amount=RISK_AMOUNT,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        backtest_compounding=False,
        trend_ema_slope_filter_min_ratio=slope_threshold,
        ema55_slope_negative_entry_bars=negative_bars,
        ema55_slope_exit_enabled=True,
        ema55_slope_lock_profit_enabled=False,
        ema55_slope_lock_profit_trigger_r=2,
        backtest_entry_slippage_rate=Decimal("0"),
        backtest_exit_slippage_rate=Decimal("0"),
        backtest_funding_rate=Decimal("0"),
    )


def build_row(result: BacktestResult, *, slope_threshold: Decimal, negative_bars: int) -> RunRow:
    report = result.report
    exit_counts = dict(summarize_trade_exit_reasons(result.trades))
    return RunRow(
        slope_threshold=slope_threshold,
        negative_bars=negative_bars,
        trades=report.total_trades,
        wins=report.win_trades,
        losses=report.loss_trades,
        breakeven=report.breakeven_trades,
        win_rate_pct=report.win_rate,
        total_pnl=report.total_pnl,
        return_pct=report.total_return_pct,
        ending_equity=report.ending_equity,
        max_drawdown=report.max_drawdown,
        max_drawdown_pct=report.max_drawdown_pct,
        profit_factor=report.profit_factor,
        avg_r=report.average_r_multiple,
        avg_pnl=report.average_pnl,
        average_win=report.average_win,
        average_loss=report.average_loss,
        stop_loss_hits=report.stop_loss_hits,
        slope_turn_positive_hits=int(exit_counts.get("斜率转正平仓", 0)),
        exit_summary=" / ".join(f"{label}:{count}" for label, count in summarize_trade_exit_reasons(result.trades)),
    )


def build_data_source_note(stats: dict[str, object], *, returned_count: int) -> str:
    cache_hit = int(stats.get("cache_hit_count", 0) or 0)
    latest_fetch = int(stats.get("latest_fetch_count", 0) or 0)
    older_fetch = int(stats.get("older_fetch_count", 0) or 0)
    parts = [
        f"本地缓存命中 {cache_hit} 根",
        f"补拉最新 {latest_fetch} 根",
    ]
    if older_fetch > 0:
        parts.append(f"补拉更早 {older_fetch} 根")
    parts.append(f"本次回测使用 {returned_count} 根")
    return " | ".join(parts)


def write_csv(rows_sorted: list[RunRow]) -> None:
    fieldnames = [
        "slope_threshold",
        "negative_bars",
        "trades",
        "wins",
        "losses",
        "breakeven",
        "win_rate_pct",
        "total_pnl",
        "return_pct",
        "ending_equity",
        "max_drawdown",
        "max_drawdown_pct",
        "profit_factor",
        "avg_r",
        "avg_pnl",
        "average_win",
        "average_loss",
        "stop_loss_hits",
        "slope_turn_positive_hits",
        "exit_summary",
    ]
    for path in (CSV_PATH, LATEST_CSV_PATH):
        with path.open("w", newline="", encoding="utf-8-sig") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows_sorted:
                writer.writerow(
                    {
                        "slope_threshold": format_decimal_fixed(row.slope_threshold, 4),
                        "negative_bars": row.negative_bars,
                        "trades": row.trades,
                        "wins": row.wins,
                        "losses": row.losses,
                        "breakeven": row.breakeven,
                        "win_rate_pct": format_decimal_fixed(row.win_rate_pct, 2),
                        "total_pnl": format_decimal(row.total_pnl),
                        "return_pct": format_decimal_fixed(row.return_pct, 2),
                        "ending_equity": format_decimal(row.ending_equity),
                        "max_drawdown": format_decimal(row.max_drawdown),
                        "max_drawdown_pct": format_decimal_fixed(row.max_drawdown_pct, 2),
                        "profit_factor": "" if row.profit_factor is None else format_decimal_fixed(row.profit_factor, 2),
                        "avg_r": format_decimal_fixed(row.avg_r, 3),
                        "avg_pnl": format_decimal(row.avg_pnl),
                        "average_win": format_decimal(row.average_win),
                        "average_loss": format_decimal(row.average_loss),
                        "stop_loss_hits": row.stop_loss_hits,
                        "slope_turn_positive_hits": row.slope_turn_positive_hits,
                        "exit_summary": row.exit_summary,
                    }
                )


def build_html(
    *,
    rows: list[RunRow],
    rows_sorted: list[RunRow],
    range_start: str,
    range_end: str,
    candle_count: int,
    data_source_note: str,
    best_return: RunRow,
    best_low_drawdown: RunRow,
    most_trades: RunRow,
) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>BTC EMA55 斜率做空参数矩阵回测 100U</title>
  <style>
    :root {{
      --bg: #f4efe7;
      --paper: #fffaf2;
      --ink: #1f2937;
      --muted: #5f6b7a;
      --line: #d9cfbe;
      --accent: #8f2d17;
      --accent-soft: #f4d8c8;
      --good: #1f7a4d;
      --bad: #b42318;
      --gold: #b7791f;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, #fff7e8 0, transparent 32rem),
        linear-gradient(180deg, #f6f1e8 0%, #efe7da 100%);
    }}
    .wrap {{
      max-width: 1500px;
      margin: 0 auto;
      padding: 28px;
    }}
    .hero {{
      background: linear-gradient(135deg, #fffdf8 0%, #f8efe3 55%, #f3ddcf 100%);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 28px 30px;
      box-shadow: 0 12px 32px rgba(90, 54, 23, 0.08);
    }}
    h1, h2, h3 {{
      margin: 0 0 10px;
    }}
    h1 {{
      font-size: 32px;
      letter-spacing: 0.02em;
    }}
    h2 {{
      margin-top: 30px;
      font-size: 22px;
    }}
    p {{
      margin: 8px 0;
      line-height: 1.6;
    }}
    .muted {{
      color: var(--muted);
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 16px;
      margin-top: 18px;
    }}
    .card {{
      background: var(--paper);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 18px;
      box-shadow: 0 8px 22px rgba(90, 54, 23, 0.06);
    }}
    .tag {{
      display: inline-block;
      padding: 4px 10px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.03em;
    }}
    .assumptions {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px 22px;
      margin-top: 16px;
    }}
    .assumptions div {{
      background: rgba(255,255,255,0.55);
      border-radius: 12px;
      padding: 10px 12px;
      border: 1px solid rgba(217, 207, 190, 0.75);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: var(--paper);
      border: 1px solid var(--line);
      border-radius: 16px;
      overflow: hidden;
      margin-top: 14px;
    }}
    th, td {{
      padding: 10px 12px;
      border-bottom: 1px solid #eadfce;
      text-align: left;
      vertical-align: top;
      font-size: 14px;
    }}
    th {{
      background: #f3e8da;
      font-weight: 700;
    }}
    tr:hover td {{
      background: rgba(255, 249, 241, 0.85);
    }}
    .right {{ text-align: right; }}
    .good {{ color: var(--good); font-weight: 700; }}
    .bad {{ color: var(--bad); font-weight: 700; }}
    .matrix {{
      display: grid;
      grid-template-columns: 140px repeat(8, minmax(0, 1fr));
      gap: 8px;
      margin-top: 16px;
    }}
    .matrix .head,
    .matrix .cell,
    .matrix .rowhead {{
      border-radius: 12px;
      padding: 10px;
      border: 1px solid var(--line);
      background: rgba(255, 250, 242, 0.92);
      min-height: 88px;
    }}
    .matrix .head,
    .matrix .rowhead {{
      font-weight: 700;
      background: #f0e3d0;
    }}
    .mini {{
      font-size: 12px;
      line-height: 1.45;
      color: var(--muted);
      margin-top: 6px;
    }}
    .footer {{
      margin-top: 28px;
      color: var(--muted);
      font-size: 13px;
    }}
    @media (max-width: 1100px) {{
      .grid {{
        grid-template-columns: 1fr;
      }}
      .assumptions {{
        grid-template-columns: 1fr;
      }}
      .matrix {{
        overflow-x: auto;
        display: flex;
        flex-direction: column;
      }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <span class="tag">BTC EMA55 斜率做空 · 100U 固定风险矩阵</span>
      <h1>BTC EMA55 斜率做空回测报告</h1>
      <p>本次扫描参数为 <strong>开空斜率阈值 -0.0001 到 -0.0005</strong>，以及 <strong>连续负数斜率根数 1 到 8</strong>，共 40 组组合。</p>
      <p class="muted">生成时间：{html.escape(generated_at)} ｜ 数据区间：{html.escape(range_start)} 至 {html.escape(range_end)} ｜ K 线周期：1H ｜ 样本数量：{candle_count} 根</p>
      <div class="assumptions">
        <div><strong>标的</strong><br>{html.escape(SYMBOL)}</div>
        <div><strong>回测 sizing</strong><br>固定风险金 100U / 初始资金 10000U / 不复利</div>
        <div><strong>止损与平仓</strong><br>ATR10 + 2ATR 止损；关闭锁盈利；开启“斜率转正平仓”</div>
        <div><strong>费用口径</strong><br>Short taker 手续费 0.036%；滑点 0；资金费 0</div>
        <div><strong>策略逻辑</strong><br>仅做空；等待 EMA55 斜率满足阈值与连续负斜率条件后开仓</div>
        <div><strong>数据来源说明</strong><br>{html.escape(data_source_note)}</div>
      </div>
    </section>

    <section>
      <h2>结论摘要</h2>
      <div class="grid">
        {summary_card("收益最高", best_return, tone="good")}
        {summary_card("低回撤代表（DD≤1%）", best_low_drawdown, tone="gold")}
        {summary_card("交易最频繁", most_trades, tone="bad")}
      </div>
    </section>

    <section>
      <h2>收益矩阵</h2>
      <p class="muted">每个格子展示：收益率 / 交易数 / 利润因子。颜色越深代表收益率越高。</p>
      <div class="matrix">
        <div class="head">开空斜率 \\ 连续负数根数</div>
        {''.join(f'<div class="head">N={bars}</div>' for bars in NEGATIVE_BARS_OPTIONS)}
        {build_matrix_rows(rows)}
      </div>
    </section>

    <section>
      <h2>Top 12 组合</h2>
      <table>
        <thead>
          <tr>
            <th>排名</th>
            <th>阈值</th>
            <th>连续负数根数</th>
            <th class="right">交易数</th>
            <th class="right">胜率</th>
            <th class="right">净利润(U)</th>
            <th class="right">收益率</th>
            <th class="right">最大回撤</th>
            <th class="right">PF</th>
            <th class="right">Avg R</th>
            <th>平仓分布</th>
          </tr>
        </thead>
        <tbody>
          {''.join(build_top_row(index + 1, row) for index, row in enumerate(rows_sorted[:12]))}
        </tbody>
      </table>
    </section>

    <section>
      <h2>完整 40 组明细</h2>
      <table>
        <thead>
          <tr>
            <th>阈值</th>
            <th>连续负数根数</th>
            <th class="right">交易数</th>
            <th class="right">胜/负/平</th>
            <th class="right">胜率</th>
            <th class="right">净利润(U)</th>
            <th class="right">期末权益(U)</th>
            <th class="right">收益率</th>
            <th class="right">最大回撤(U)</th>
            <th class="right">最大回撤%</th>
            <th class="right">PF</th>
            <th class="right">Avg R</th>
            <th class="right">止损</th>
            <th class="right">斜率转正</th>
          </tr>
        </thead>
        <tbody>
          {''.join(build_full_row(row) for row in sorted(rows, key=lambda item: (item.slope_threshold, item.negative_bars)))}
        </tbody>
      </table>
    </section>

    <div class="footer">
      报告假设说明：这里的“只有斜率重新转正时平仓”解释为 <strong>关闭锁盈利/动态止盈，仅保留 ATR 止损与斜率转正平仓</strong>。如果你想把 ATR 止损也去掉，结果会是另一套风险口径，需要单独再跑。
    </div>
  </div>
</body>
</html>
"""


def summary_card(title: str, row: RunRow, *, tone: str) -> str:
    tone_class = {
        "good": "good",
        "gold": "",
        "bad": "bad",
    }.get(tone, "")
    return f"""
<article class="card">
  <div class="tag">{html.escape(title)}</div>
  <h3>阈值 {format_decimal_fixed(row.slope_threshold, 4)} · 连续负数 {row.negative_bars} 根</h3>
  <p class="{tone_class}">净利润 {format_decimal(row.total_pnl)}U ｜ 收益率 {format_decimal_fixed(row.return_pct, 2)}%</p>
  <p>交易数 {row.trades} ｜ 胜率 {format_decimal_fixed(row.win_rate_pct, 2)}% ｜ PF {format_optional(row.profit_factor, 2)} ｜ Avg R {format_decimal_fixed(row.avg_r, 3)}</p>
  <p class="muted">最大回撤 {format_decimal(row.max_drawdown)}U ({format_decimal_fixed(row.max_drawdown_pct, 2)}%) ｜ 平仓分布：{html.escape(row.exit_summary or "-")}</p>
</article>
"""


def build_matrix_rows(rows: list[RunRow]) -> str:
    lookup = {(row.slope_threshold, row.negative_bars): row for row in rows}
    returns = [row.return_pct for row in rows]
    min_return = min(returns)
    max_return = max(returns)
    blocks: list[str] = []
    for slope_threshold in SLOPE_THRESHOLDS:
        blocks.append(f'<div class="rowhead">阈值 {format_decimal_fixed(slope_threshold, 4)}</div>')
        for negative_bars in NEGATIVE_BARS_OPTIONS:
            row = lookup[(slope_threshold, negative_bars)]
            color = return_cell_color(row.return_pct, min_return=min_return, max_return=max_return)
            pnl_class = "good" if row.total_pnl > 0 else ("bad" if row.total_pnl < 0 else "")
            blocks.append(
                f'<div class="cell" style="background:{color}">'
                f'<div class="{pnl_class}">{format_decimal_fixed(row.return_pct, 2)}%</div>'
                f'<div class="mini">净利润 {format_decimal(row.total_pnl)}U<br>'
                f'交易 {row.trades} 笔 ｜ PF {format_optional(row.profit_factor, 2)}<br>'
                f'回撤 {format_decimal_fixed(row.max_drawdown_pct, 2)}%</div>'
                "</div>"
            )
    return "".join(blocks)


def build_top_row(rank: int, row: RunRow) -> str:
    pnl_class = "good" if row.total_pnl > 0 else ("bad" if row.total_pnl < 0 else "")
    return (
        "<tr>"
        f"<td>{rank}</td>"
        f"<td>{format_decimal_fixed(row.slope_threshold, 4)}</td>"
        f"<td>{row.negative_bars}</td>"
        f'<td class="right">{row.trades}</td>'
        f'<td class="right">{format_decimal_fixed(row.win_rate_pct, 2)}%</td>'
        f'<td class="right {pnl_class}">{format_decimal(row.total_pnl)}</td>'
        f'<td class="right {pnl_class}">{format_decimal_fixed(row.return_pct, 2)}%</td>'
        f'<td class="right">{format_decimal(row.max_drawdown)}U / {format_decimal_fixed(row.max_drawdown_pct, 2)}%</td>'
        f'<td class="right">{format_optional(row.profit_factor, 2)}</td>'
        f'<td class="right">{format_decimal_fixed(row.avg_r, 3)}</td>'
        f"<td>{html.escape(row.exit_summary or '-')}</td>"
        "</tr>"
    )


def build_full_row(row: RunRow) -> str:
    pnl_class = "good" if row.total_pnl > 0 else ("bad" if row.total_pnl < 0 else "")
    return (
        "<tr>"
        f"<td>{format_decimal_fixed(row.slope_threshold, 4)}</td>"
        f"<td>{row.negative_bars}</td>"
        f'<td class="right">{row.trades}</td>'
        f'<td class="right">{row.wins}/{row.losses}/{row.breakeven}</td>'
        f'<td class="right">{format_decimal_fixed(row.win_rate_pct, 2)}%</td>'
        f'<td class="right {pnl_class}">{format_decimal(row.total_pnl)}</td>'
        f'<td class="right">{format_decimal(row.ending_equity)}</td>'
        f'<td class="right {pnl_class}">{format_decimal_fixed(row.return_pct, 2)}%</td>'
        f'<td class="right">{format_decimal(row.max_drawdown)}</td>'
        f'<td class="right">{format_decimal_fixed(row.max_drawdown_pct, 2)}%</td>'
        f'<td class="right">{format_optional(row.profit_factor, 2)}</td>'
        f'<td class="right">{format_decimal_fixed(row.avg_r, 3)}</td>'
        f'<td class="right">{row.stop_loss_hits}</td>'
        f'<td class="right">{row.slope_turn_positive_hits}</td>'
        "</tr>"
    )


def return_cell_color(value: Decimal, *, min_return: Decimal, max_return: Decimal) -> str:
    if max_return == min_return:
        ratio = Decimal("0.5")
    else:
        ratio = (value - min_return) / (max_return - min_return)
    ratio = min(max(ratio, Decimal("0")), Decimal("1"))
    start = (248, 240, 226)
    end = (182, 66, 33)
    rgb = [
        int(start[index] + (end[index] - start[index]) * float(ratio))
        for index in range(3)
    ]
    return f"rgb({rgb[0]}, {rgb[1]}, {rgb[2]})"


def format_optional(value: Decimal | None, places: int) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(value, places)


def format_ts(ts: int) -> str:
    if ts >= 10**12:
        return datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M")
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


if __name__ == "__main__":
    main()
