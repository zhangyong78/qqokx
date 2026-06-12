from __future__ import annotations

import csv
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import BacktestTrade, _run_backtest_with_loaded_data, format_trade_exit_reason
from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import Instrument, StrategyConfig
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal_fixed
from scripts.run_btc_daily_ma_direction_filter_research import LONG_MAKER_FEE_RATE, LONG_TAKER_FEE_RATE


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASE_NAME = f"btc_long_three_config_full_compare_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASE_NAME}.html"
CSV_PATH = REPORT_DIR / f"{BASE_NAME}.csv"
JSON_PATH = REPORT_DIR / f"{BASE_NAME}.json"
PROJECT_HTML_PATH = PROJECT_REPORT_DIR / "btc_long_three_config_full_compare_latest.html"

INITIAL_CAPITAL = Decimal("10000")
RISK_AMOUNT = Decimal("100")


@dataclass(frozen=True)
class ConfigCase:
    key: str
    title: str
    config: StrategyConfig


BTC_SWAP_INSTRUMENT = Instrument(
    inst_id="BTC-USDT-SWAP",
    inst_type="SWAP",
    tick_size=Decimal("0.1"),
    lot_size=Decimal("0.01"),
    min_size=Decimal("0.01"),
    state="live",
    settle_ccy="USDT",
    ct_val=Decimal("0.01"),
    ct_mult=Decimal("1"),
    ct_val_ccy="BTC",
    uly="BTC-USDT",
    inst_family="BTC-USDT",
)


def _html_text(text: object) -> str:
    escaped = (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )
    return "".join(f"&#x{ord(ch):X};" if ord(ch) > 127 else ch for ch in escaped)


def _fmt(value: Decimal | int | float | None, digits: int = 2) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(Decimal(str(value)), digits)


def _fmt_pf(value: Decimal | None) -> str:
    return "-" if value is None else format_decimal_fixed(value, 4)


def _base_config() -> dict[str, object]:
    return {
        "inst_id": "BTC-USDT-SWAP",
        "bar": "1H",
        "atr_period": 10,
        "order_size": Decimal("0"),
        "trade_mode": "cross",
        "signal_mode": "long_only",
        "position_mode": "net",
        "environment": "demo",
        "tp_sl_trigger_type": "mark",
        "strategy_id": "ema_dynamic_order_long",
        "poll_seconds": 10.0,
        "risk_amount": RISK_AMOUNT,
        "tp_sl_mode": "exchange",
        "entry_side_mode": "follow_signal",
        "run_mode": "trade",
        "take_profit_mode": "dynamic",
        "max_entries_per_trend": 1,
        "dynamic_two_r_break_even": True,
        "dynamic_fee_offset_enabled": True,
        "ema55_slope_exit_enabled": True,
        "ema55_slope_lock_profit_enabled": False,
        "ema55_slope_negative_entry_bars": 1,
        "trend_ema_slope_filter_enabled": True,
        "trend_ema_slope_filter_lookback_bars": 5,
        "trend_ema_slope_filter_min_ratio": Decimal("0"),
        "atr_percentile_filter_max": Decimal("0"),
        "time_stop_break_even_enabled": False,
        "time_stop_break_even_bars": 0,
        "hold_close_exit_bars": 0,
        "backtest_initial_capital": INITIAL_CAPITAL,
        "backtest_sizing_mode": "fixed_risk",
        "backtest_compounding": False,
        "backtest_entry_slippage_rate": Decimal("0"),
        "backtest_exit_slippage_rate": Decimal("0"),
        "backtest_slippage_rate": Decimal("0"),
        "backtest_funding_rate": Decimal("0"),
    }


def build_cases() -> tuple[ConfigCase, ...]:
    base = _base_config()
    return (
        ConfigCase(
            key="ma21_ma50_r2",
            title="1. EMA21 / MA50 / MA50 / ATR10 / SLx2 / TPx2 / 首档触发R2",
            config=StrategyConfig(
                **base,
                ema_type="ema",
                ema_period=21,
                trend_ema_type="ma",
                trend_ema_period=50,
                entry_reference_ema_type="ma",
                entry_reference_ema_period=50,
                atr_stop_multiplier=Decimal("2"),
                atr_take_multiplier=Decimal("2"),
                ema55_slope_lock_profit_trigger_r=2,
            ),
        ),
        ConfigCase(
            key="ma21_ma50_r6",
            title="2. EMA21 / MA50 / MA50 / ATR10 / SLx2 / TPx2 / 首档触发R6",
            config=StrategyConfig(
                **base,
                ema_type="ema",
                ema_period=21,
                trend_ema_type="ma",
                trend_ema_period=50,
                entry_reference_ema_type="ma",
                entry_reference_ema_period=50,
                atr_stop_multiplier=Decimal("2"),
                atr_take_multiplier=Decimal("2"),
                ema55_slope_lock_profit_trigger_r=6,
            ),
        ),
        ConfigCase(
            key="ema5_ema13_r6",
            title="3. EMA5 / EMA13 / EMA5 / ATR10 / SLx1 / TPx2 / 首档触发R6",
            config=StrategyConfig(
                **base,
                ema_type="ema",
                ema_period=5,
                trend_ema_type="ema",
                trend_ema_period=13,
                entry_reference_ema_type="ema",
                entry_reference_ema_period=5,
                atr_stop_multiplier=Decimal("1"),
                atr_take_multiplier=Decimal("2"),
                ema55_slope_lock_profit_trigger_r=6,
            ),
        ),
    )


def _exit_reason_summary(trades: list[BacktestTrade], limit: int = 6) -> str:
    counts: dict[str, int] = {}
    for trade in trades:
        label = format_trade_exit_reason(trade.exit_reason)
        counts[label] = counts.get(label, 0) + 1
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return " | ".join(f"{label} {count}" for label, count in ordered[:limit])


def _rows_from_cases(cases: tuple[ConfigCase, ...], full_candles: list) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for case in cases:
        result = _run_backtest_with_loaded_data(
            full_candles,
            BTC_SWAP_INSTRUMENT,
            case.config,
            data_source_note=f"local candle_cache full history | {case.key}",
            maker_fee_rate=LONG_MAKER_FEE_RATE,
            taker_fee_rate=LONG_TAKER_FEE_RATE,
        )
        report = result.report
        rows.append(
            {
                "key": case.key,
                "title": case.title,
                "ema_type": case.config.ema_type,
                "ema_period": case.config.ema_period,
                "trend_ema_type": case.config.trend_ema_type,
                "trend_ema_period": case.config.trend_ema_period,
                "entry_reference_ema_type": case.config.entry_reference_ema_type,
                "entry_reference_ema_period": case.config.entry_reference_ema_period,
                "atr_period": case.config.atr_period,
                "atr_stop_multiplier": float(case.config.atr_stop_multiplier),
                "atr_take_multiplier": float(case.config.atr_take_multiplier),
                "trigger_r": int(case.config.ema55_slope_lock_profit_trigger_r),
                "candle_count": len(full_candles),
                "start_ts": full_candles[0].ts,
                "end_ts": full_candles[-1].ts,
                "trades": report.total_trades,
                "win_rate_pct": float(report.win_rate),
                "pnl_u": float(report.total_pnl),
                "return_pct": float(report.total_return_pct),
                "profit_factor": None if report.profit_factor is None else float(report.profit_factor),
                "avg_r": float(report.average_r_multiple),
                "max_drawdown_u": float(report.max_drawdown),
                "max_drawdown_pct": float(report.max_drawdown_pct),
                "gross_profit_u": float(report.gross_profit),
                "gross_loss_u": float(report.gross_loss),
                "fees_u": float(report.total_fees),
                "maker_fees_u": float(report.maker_fees),
                "taker_fees_u": float(report.taker_fees),
                "win_trades": report.win_trades,
                "loss_trades": report.loss_trades,
                "profit_loss_ratio": None if report.profit_loss_ratio is None else float(report.profit_loss_ratio),
                "exit_reason_top": _exit_reason_summary(result.trades),
            }
        )
    return rows


def _analysis_lines(rows: list[dict[str, object]]) -> list[str]:
    lines: list[str] = []
    by_pnl = max(rows, key=lambda row: row["pnl_u"])
    by_pf = max(rows, key=lambda row: row["profit_factor"] if row["profit_factor"] is not None else -1)
    by_dd = min(rows, key=lambda row: row["max_drawdown_u"])
    lines.append(f"按净利看，最佳是 {by_pnl['title']}，净利 {_fmt(by_pnl['pnl_u'], 4)}，收益率 {_fmt(by_pnl['return_pct'])}%。")
    lines.append(f"按 PF 看，最佳是 {by_pf['title']}，PF {_fmt(by_pf['profit_factor'], 4)}，单笔质量更好。")
    lines.append(f"按最大回撤看，最稳的是 {by_dd['title']}，最大回撤 {_fmt(by_dd['max_drawdown_u'], 4)}。")
    row1 = next(row for row in rows if row["key"] == "ma21_ma50_r2")
    row2 = next(row for row in rows if row["key"] == "ma21_ma50_r6")
    row3 = next(row for row in rows if row["key"] == "ema5_ema13_r6")
    lines.append(
        "同样是 MA21/MA50/MA50 结构，首档触发R从 2 提到 6 后，交易会变少，"
        f"净利从 {_fmt(row1['pnl_u'], 4)} 变为 {_fmt(row2['pnl_u'], 4)}，"
        f"PF 从 {_fmt(row1['profit_factor'], 4)} 变为 {_fmt(row2['profit_factor'], 4)}。"
    )
    lines.append(
        "EMA5/EMA13/EMA5 + SL1 + 6R 会显著提高信号密度和趋势持有时间，"
        f"交易 {row3['trades']} 笔，但回撤也会放大到 {_fmt(row3['max_drawdown_u'], 4)}。"
    )
    return lines


def _write_csv(rows: list[dict[str, object]]) -> None:
    with CSV_PATH.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_json(rows: list[dict[str, object]]) -> None:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "data_range": {
            "start": datetime.fromtimestamp(rows[0]["start_ts"] / 1000).strftime("%Y-%m-%d %H:%M"),
            "end": datetime.fromtimestamp(rows[0]["end_ts"] / 1000).strftime("%Y-%m-%d %H:%M"),
            "candle_count": rows[0]["candle_count"],
        },
        "rows": rows,
        "analysis": _analysis_lines(rows),
    }
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_html(rows: list[dict[str, object]]) -> None:
    cards_html = []
    for row in rows:
        cards_html.append(
            f"""
            <div class="card">
              <div class="eyebrow">{_html_text(row['key'])}</div>
              <h2>{_html_text(row['key'])}</h2>
              <div class="title">{_html_text(row['title'])}</div>
              <div class="metric">{_html_text(_fmt(row['pnl_u'], 4))}</div>
              <div class="caption">净利 / 收益率 {_html_text(_fmt(row['return_pct']))}%</div>
              <div class="detail">交易 {_html_text(row['trades'])} 笔 | 胜率 {_html_text(_fmt(row['win_rate_pct']))}% | PF {_html_text(_fmt(row['profit_factor'], 4))}</div>
              <div class="detail">最大回撤 {_html_text(_fmt(row['max_drawdown_u'], 4))} | 平均R {_html_text(_fmt(row['avg_r'], 4))}</div>
            </div>
            """
        )
    table_rows = []
    for row in rows:
        table_rows.append(
            f"""
            <tr>
              <td>{_html_text(row['key'])}</td>
              <td>{_html_text(row['title'])}</td>
              <td>{_html_text(row['trades'])}</td>
              <td>{_html_text(_fmt(row['win_rate_pct']))}%</td>
              <td>{_html_text(_fmt(row['pnl_u'], 4))}</td>
              <td>{_html_text(_fmt(row['return_pct']))}%</td>
              <td>{_html_text(_fmt(row['profit_factor'], 4))}</td>
              <td>{_html_text(_fmt(row['avg_r'], 4))}</td>
              <td>{_html_text(_fmt(row['max_drawdown_u'], 4))}</td>
              <td>{_html_text(_fmt(row['max_drawdown_pct']))}%</td>
              <td>{_html_text(_fmt(row['fees_u'], 4))}</td>
              <td>{_html_text(row['exit_reason_top'])}</td>
            </tr>
            """
        )
    analysis_html = "".join(f"<li>{_html_text(line)}</li>" for line in _analysis_lines(rows))
    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>BTC 做多三组参数全量对比</title>
  <style>
    :root {{
      --bg: #f6efe3;
      --card: rgba(255,255,255,0.78);
      --ink: #1f2a2e;
      --muted: #5e676b;
      --line: rgba(88, 79, 58, 0.16);
      --accent: #0d7a68;
      --warn: #d2872c;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Microsoft YaHei UI", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(244, 209, 122, 0.34), transparent 32%),
        radial-gradient(circle at top right, rgba(136, 194, 170, 0.28), transparent 30%),
        linear-gradient(135deg, #f8f0db 0%, #f7f5ef 45%, #eef6f1 100%);
    }}
    .wrap {{ max-width: 1520px; margin: 0 auto; padding: 28px 28px 40px; }}
    .hero, .section, .card {{
      background: var(--card);
      border: 1px solid var(--line);
      box-shadow: 0 16px 40px rgba(48, 53, 41, 0.08);
    }}
    .hero {{
      border-radius: 26px;
      padding: 28px 30px;
      background: linear-gradient(120deg, rgba(255, 233, 184, 0.88), rgba(227, 242, 233, 0.84));
    }}
    h1 {{ margin: 0 0 10px; font-size: 44px; line-height: 1.05; }}
    .sub {{ color: var(--muted); font-size: 16px; line-height: 1.75; }}
    .cards {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 18px; margin-top: 24px; }}
    .card {{ border-radius: 22px; padding: 20px 22px; }}
    .eyebrow {{ color: var(--muted); font-size: 13px; text-transform: uppercase; letter-spacing: .06em; }}
    .card h2 {{ margin: 8px 0; font-size: 32px; color: var(--accent); }}
    .title {{ line-height: 1.7; min-height: 54px; }}
    .metric {{ margin-top: 14px; font-size: 30px; font-weight: 700; }}
    .caption {{ color: var(--muted); margin-top: 4px; }}
    .detail {{ margin-top: 10px; line-height: 1.7; }}
    .section {{ margin-top: 24px; border-radius: 24px; padding: 22px 24px; }}
    .section h3 {{ margin: 0 0 14px; font-size: 24px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ border-bottom: 1px solid var(--line); padding: 10px; text-align: left; vertical-align: top; }}
    th {{ background: rgba(255,255,255,0.7); }}
    ul {{ margin: 0; padding-left: 20px; line-height: 1.8; }}
    .path {{ font-family: Consolas, monospace; color: var(--muted); font-size: 13px; }}
    @media (max-width: 1100px) {{
      .cards {{ grid-template-columns: 1fr; }}
      .wrap {{ padding: 16px; }}
      h1 {{ font-size: 34px; }}
      table {{ font-size: 12px; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <h1>BTC 做多三组参数全量对比</h1>
      <div class="sub">
        口径：BTC-USDT-SWAP / 1H / 本地全量已收盘缓存 / 风险金100 / 本金10000 / 不复利 / Maker 0.015% / Taker 0.036%<br>
        时间范围：{_html_text(datetime.fromtimestamp(rows[0]['start_ts'] / 1000).strftime("%Y-%m-%d %H:%M"))} -> {_html_text(datetime.fromtimestamp(rows[0]['end_ts'] / 1000).strftime("%Y-%m-%d %H:%M"))} / {_html_text(rows[0]['candle_count'])} 根
      </div>
    </div>
    <div class="cards">{''.join(cards_html)}</div>
    <div class="section">
      <h3>结果表</h3>
      <table>
        <thead>
          <tr>
            <th>编号</th>
            <th>参数</th>
            <th>交易</th>
            <th>胜率</th>
            <th>净利</th>
            <th>收益率</th>
            <th>PF</th>
            <th>平均R</th>
            <th>最大回撤</th>
            <th>回撤比例</th>
            <th>手续费</th>
            <th>主要平仓</th>
          </tr>
        </thead>
        <tbody>{''.join(table_rows)}</tbody>
      </table>
    </div>
    <div class="section">
      <h3>分析</h3>
      <ul>{analysis_html}</ul>
    </div>
    <div class="section">
      <h3>文件</h3>
      <div class="path">{_html_text(str(HTML_PATH))}</div>
      <div class="path">{_html_text(str(CSV_PATH))}</div>
      <div class="path">{_html_text(str(JSON_PATH))}</div>
    </div>
  </div>
</body>
</html>"""
    HTML_PATH.write_text(html, encoding="utf-8")
    PROJECT_HTML_PATH.write_text(html, encoding="utf-8")


def main() -> None:
    full_candles = [candle for candle in load_candle_cache("BTC-USDT-SWAP", "1H", limit=None) if candle.confirmed]
    if not full_candles:
        raise RuntimeError("missing local full candles for BTC-USDT-SWAP 1H")
    cases = build_cases()
    rows = _rows_from_cases(cases, full_candles)
    _write_csv(rows)
    _write_json(rows)
    _write_html(rows)
    print(HTML_PATH)
    print(PROJECT_HTML_PATH)


if __name__ == "__main__":
    main()
