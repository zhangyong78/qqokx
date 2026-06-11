from __future__ import annotations

import csv
import json
import sys
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import okx_quant.backtest as backtest_module
from okx_quant.backtest import BacktestTrade, _run_backtest_with_loaded_data
from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal_fixed
from scripts.build_best_parameter_bundle import build_specs
from scripts.run_btc_daily_ma_direction_filter_research import (
    LONG_MAKER_FEE_RATE,
    LONG_TAKER_FEE_RATE,
    SHORT_TAKER_FEE_RATE,
)


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASE_NAME = f"best_parameter_bundle_overall_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASE_NAME}.html"
CSV_PATH = REPORT_DIR / f"{BASE_NAME}.csv"
JSON_PATH = REPORT_DIR / f"{BASE_NAME}.json"
PROJECT_HTML_PATH = PROJECT_REPORT_DIR / "best_parameter_bundle_overall_latest.html"

LONG_SIDE = "\u505a\u591a"
SHORT_SIDE = "\u505a\u7a7a"
INITIAL_CAPITAL = Decimal("10000")


@dataclass(frozen=True)
class StrategyRun:
    side: str
    symbol: str
    coin: str
    strategy_label: str
    core_label: str
    protection_label: str
    config: StrategyConfig
    candle_count: int
    start_ts: int
    end_ts: int
    trades: tuple[BacktestTrade, ...]


def _html_text(text: object) -> str:
    escaped = (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )
    return "".join(f"&#x{ord(ch):X};" if ord(ch) > 127 else ch for ch in escaped)


def _fmt(value: Decimal | int | float, digits: int = 2) -> str:
    return format_decimal_fixed(Decimal(str(value)), digits)


def _fmt_pf(value: Decimal | None) -> str:
    return "-" if value is None else format_decimal_fixed(value, 4)


def _dt(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M")


@contextmanager
def patched_dynamic_trigger_r(config: StrategyConfig):
    original = backtest_module._create_open_position
    trigger_r = max(int(config.ema55_slope_lock_profit_trigger_r), 2)

    def wrapped_create_open_position(*args, **kwargs):
        if kwargs.get("dynamic_take_profit_enabled"):
            kwargs.setdefault("next_dynamic_trigger_r", trigger_r)
        return original(*args, **kwargs)

    backtest_module._create_open_position = wrapped_create_open_position
    try:
        yield
    finally:
        backtest_module._create_open_position = original


def profit_factor(trades: list[BacktestTrade]) -> Decimal | None:
    gross_profit = sum((trade.pnl for trade in trades if trade.pnl > 0), Decimal("0"))
    gross_loss = sum((-trade.pnl for trade in trades if trade.pnl < 0), Decimal("0"))
    if gross_loss == 0:
        return None if gross_profit == 0 else Decimal("999999")
    return gross_profit / gross_loss


def max_drawdown(trades: list[BacktestTrade]) -> Decimal:
    equity = Decimal("0")
    peak = Decimal("0")
    drawdown = Decimal("0")
    for trade in sorted(trades, key=lambda item: (item.exit_ts, item.entry_ts, item.signal)):
        equity += trade.pnl
        if equity > peak:
            peak = equity
        drawdown = max(drawdown, peak - equity)
    return drawdown


def metrics(trades: list[BacktestTrade]) -> dict[str, object]:
    total = len(trades)
    pnl = sum((trade.pnl for trade in trades), Decimal("0"))
    wins = sum(1 for trade in trades if trade.pnl > 0)
    losses = sum(1 for trade in trades if trade.pnl < 0)
    return {
        "trades": total,
        "wins": wins,
        "losses": losses,
        "pnl_u": pnl,
        "return_pct": (pnl / INITIAL_CAPITAL * Decimal("100")) if INITIAL_CAPITAL else Decimal("0"),
        "win_rate_pct": (Decimal(wins) / Decimal(total) * Decimal("100")) if total else Decimal("0"),
        "profit_factor": profit_factor(trades),
        "avg_r": (sum((trade.r_multiple for trade in trades), Decimal("0")) / Decimal(total)) if total else Decimal("0"),
        "max_drawdown_u": max_drawdown(trades),
        "fees_u": sum((trade.total_fee for trade in trades), Decimal("0")),
        "slippage_u": sum((trade.slippage_cost for trade in trades), Decimal("0")),
    }


def run_strategy(client: OkxRestClient, bundle_spec) -> StrategyRun:
    config = replace(bundle_spec.config, environment="demo")
    candles = [candle for candle in load_candle_cache(bundle_spec.symbol, config.bar, limit=None) if candle.confirmed]
    if not candles:
        raise RuntimeError(f"missing candles for {bundle_spec.symbol} {config.bar}")
    kwargs = {}
    if bundle_spec.side == LONG_SIDE:
        kwargs = {
            "maker_fee_rate": LONG_MAKER_FEE_RATE,
            "taker_fee_rate": LONG_TAKER_FEE_RATE,
        }
    else:
        kwargs = {"taker_fee_rate": SHORT_TAKER_FEE_RATE}
    with patched_dynamic_trigger_r(config):
        result = _run_backtest_with_loaded_data(
            candles,
            client.get_instrument(bundle_spec.symbol),
            config,
            data_source_note=f"local candle_cache full history | {bundle_spec.symbol} | best bundle",
            **kwargs,
        )
    return StrategyRun(
        side=bundle_spec.side,
        symbol=bundle_spec.symbol,
        coin=bundle_spec.symbol.replace("-USDT-SWAP", ""),
        strategy_label=bundle_spec.strategy_label,
        core_label=bundle_spec.core_label,
        protection_label=bundle_spec.protection_label,
        config=config,
        candle_count=len(candles),
        start_ts=candles[0].ts,
        end_ts=candles[-1].ts,
        trades=tuple(result.trades),
    )


def group_metrics(runs: list[StrategyRun]) -> dict[str, dict[str, object]]:
    all_trades = [trade for run in runs for trade in run.trades]
    output = {"ALL": metrics(all_trades)}
    for side in (LONG_SIDE, SHORT_SIDE):
        side_trades = [trade for run in runs if run.side == side for trade in run.trades]
        output[side] = metrics(side_trades)
    for run in runs:
        output[f"{run.side}-{run.coin}"] = metrics(list(run.trades))
    return output


def period_rows(trades: list[BacktestTrade], *, fmt: str) -> list[dict[str, object]]:
    buckets: dict[str, list[BacktestTrade]] = {}
    for trade in trades:
        key = datetime.fromtimestamp(trade.exit_ts / 1000).strftime(fmt)
        buckets.setdefault(key, []).append(trade)
    rows = []
    cumulative = Decimal("0")
    for key in sorted(buckets):
        bucket = buckets[key]
        pnl = sum((trade.pnl for trade in bucket), Decimal("0"))
        cumulative += pnl
        rows.append(
            {
                "period": key,
                "trades": len(bucket),
                "pnl_u": pnl,
                "cumulative_pnl_u": cumulative,
                "long_pnl_u": sum((trade.pnl for trade in bucket if trade.signal == "long"), Decimal("0")),
                "short_pnl_u": sum((trade.pnl for trade in bucket if trade.signal == "short"), Decimal("0")),
            }
        )
    return rows


def trade_rows(runs: list[StrategyRun]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for run in runs:
        for trade in run.trades:
            rows.append(
                {
                    "side": run.side,
                    "symbol": run.symbol,
                    "strategy": run.strategy_label,
                    "entry_ts": trade.entry_ts,
                    "exit_ts": trade.exit_ts,
                    "entry_time": _dt(trade.entry_ts),
                    "exit_time": _dt(trade.exit_ts),
                    "signal": trade.signal,
                    "entry_price": str(trade.entry_price),
                    "exit_price": str(trade.exit_price),
                    "pnl_u": str(trade.pnl),
                    "r_multiple": str(trade.r_multiple),
                    "exit_reason": trade.exit_reason,
                    "fee_u": str(trade.total_fee),
                    "slippage_u": str(trade.slippage_cost),
                }
            )
    rows.sort(key=lambda item: (int(item["exit_ts"]), int(item["entry_ts"]), str(item["symbol"])))
    cumulative = Decimal("0")
    for row in rows:
        cumulative += Decimal(str(row["pnl_u"]))
        row["cumulative_pnl_u"] = str(cumulative)
    return rows


def write_csv(rows: list[dict[str, object]]) -> None:
    fieldnames = [
        "side",
        "symbol",
        "strategy",
        "entry_time",
        "exit_time",
        "signal",
        "entry_price",
        "exit_price",
        "pnl_u",
        "cumulative_pnl_u",
        "r_multiple",
        "exit_reason",
        "fee_u",
        "slippage_u",
    ]
    with CSV_PATH.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def metric_table(title: str, rows: list[tuple[str, dict[str, object]]]) -> str:
    body = []
    for label, row in rows:
        body.append(
            "<tr>"
            f"<td>{_html_text(label)}</td>"
            f"<td>{row['trades']}</td>"
            f"<td>{_html_text(_fmt(row['pnl_u'], 2))}</td>"
            f"<td>{_html_text(_fmt(row['return_pct'], 2))}</td>"
            f"<td>{_html_text(_fmt(row['win_rate_pct'], 2))}</td>"
            f"<td>{_html_text(_fmt_pf(row['profit_factor']))}</td>"
            f"<td>{_html_text(_fmt(row['avg_r'], 4))}</td>"
            f"<td>{_html_text(_fmt(row['max_drawdown_u'], 2))}</td>"
            "</tr>"
        )
    return (
        f"<section class=\"card\"><h2>{_html_text(title)}</h2>"
        "<table><thead><tr><th>\u5206\u7ec4</th><th>\u4ea4\u6613</th><th>PnL(U)</th>"
        "<th>\u6536\u76ca%</th><th>\u80dc\u7387%</th><th>PF</th><th>\u5e73\u5747R</th><th>\u6700\u5927\u56de\u64a4(U)</th></tr></thead>"
        f"<tbody>{''.join(body)}</tbody></table></section>"
    )


def period_table(title: str, rows: list[dict[str, object]], limit: int = 24) -> str:
    display = rows[-limit:] if len(rows) > limit else rows
    body = []
    for row in display:
        body.append(
            "<tr>"
            f"<td>{_html_text(row['period'])}</td>"
            f"<td>{row['trades']}</td>"
            f"<td>{_html_text(_fmt(row['pnl_u'], 2))}</td>"
            f"<td>{_html_text(_fmt(row['cumulative_pnl_u'], 2))}</td>"
            f"<td>{_html_text(_fmt(row['long_pnl_u'], 2))}</td>"
            f"<td>{_html_text(_fmt(row['short_pnl_u'], 2))}</td>"
            "</tr>"
        )
    return (
        f"<section class=\"card\"><h2>{_html_text(title)}</h2>"
        "<table><thead><tr><th>\u5468\u671f</th><th>\u4ea4\u6613</th><th>PnL(U)</th><th>\u7d2f\u8ba1PnL(U)</th><th>\u591a\u5934(U)</th><th>\u7a7a\u5934(U)</th></tr></thead>"
        f"<tbody>{''.join(body)}</tbody></table></section>"
    )


def render_html(payload: dict[str, object]) -> str:
    grouped = payload["grouped_metrics"]
    runs = payload["runs"]
    month_rows = payload["monthly_rows"]
    year_rows = payload["yearly_rows"]
    generated_at = _html_text(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    all_metrics = grouped["ALL"]
    side_rows = [(LONG_SIDE, grouped[LONG_SIDE]), (SHORT_SIDE, grouped[SHORT_SIDE]), ("\u603b\u7ec4\u5408", all_metrics)]
    symbol_rows = [
        (f"{run['side']} {run['coin']}", grouped[f"{run['side']}-{run['coin']}"])
        for run in runs
    ]
    run_body = []
    for run in runs:
        cfg = run["config"]
        trigger_r = cfg["ema55_slope_lock_profit_trigger_r"]
        run_body.append(
            "<tr>"
            f"<td>{_html_text(run['side'])}</td>"
            f"<td>{_html_text(run['coin'])}</td>"
            f"<td>{_html_text(run['strategy_label'])}</td>"
            f"<td>{_html_text(run['core_label'])}</td>"
            f"<td>{_html_text(run['protection_label'])}</td>"
            f"<td>{_html_text(trigger_r)}R</td>"
            f"<td>{run['candle_count']}</td>"
            f"<td>{_html_text(run['date_range'])}</td>"
            "</tr>"
        )
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>{_html_text('最佳参数组合包整体回测报告')}</title>
  <style>
    :root {{
      --bg: #eef3ef;
      --panel: #fbfcf8;
      --line: #ccd8cc;
      --ink: #18231d;
      --green: #0f766e;
      --red: #a13b2b;
      --gold: #8a6a12;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      padding: 28px;
      background:
        linear-gradient(135deg, rgba(15,118,110,0.12), transparent 32%),
        linear-gradient(315deg, rgba(161,59,43,0.10), transparent 30%),
        var(--bg);
      color: var(--ink);
      font-family: "Microsoft YaHei UI", "Microsoft YaHei", sans-serif;
      line-height: 1.55;
    }}
    .hero, .card {{
      max-width: 1480px;
      margin: 0 auto 18px auto;
      background: rgba(251,252,248,0.96);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 20px 22px;
      box-shadow: 0 14px 38px rgba(24,35,29,0.08);
    }}
    h1, h2 {{ margin: 0 0 12px 0; }}
    h1 {{ color: var(--green); font-size: 34px; }}
    h2 {{ color: var(--red); font-size: 22px; }}
    .chips {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(230px, 1fr)); gap: 12px; }}
    .chip {{ background: #eef7f3; border: 1px solid #c9e4dc; border-radius: 8px; padding: 11px 12px; }}
    .big {{ font-size: 24px; font-weight: 700; color: var(--green); }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid var(--line); padding: 8px 7px; text-align: left; vertical-align: top; }}
    th {{ background: #e7f0eb; color: #174a43; }}
    code {{ background: #eef0e8; padding: 1px 5px; border-radius: 5px; }}
  </style>
</head>
<body>
  <section class="hero">
    <h1>{_html_text('最佳参数组合包整体回测报告')}</h1>
    <p>{_html_text('口径：当前最佳参数组合包 4 多 + 4 空，本地全量 1H K 线，每笔固定风险 100U，不复利；手续费按多头 maker/taker、空头 taker 口径计入。')}</p>
    <div class="chips">
      <div class="chip"><strong>{_html_text('生成时间')}</strong><br>{generated_at}</div>
      <div class="chip"><strong>{_html_text('总收益')}</strong><br><span class="big">{_html_text(_fmt(all_metrics['pnl_u'], 2))}U</span></div>
      <div class="chip"><strong>{_html_text('PF / 胜率')}</strong><br><span class="big">{_html_text(_fmt_pf(all_metrics['profit_factor']))} / {_html_text(_fmt(all_metrics['win_rate_pct'], 2))}%</span></div>
      <div class="chip"><strong>{_html_text('最大回撤')}</strong><br><span class="big">{_html_text(_fmt(all_metrics['max_drawdown_u'], 2))}U</span></div>
    </div>
  </section>
  {metric_table('组合总览', side_rows)}
  {metric_table('分币种明细', symbol_rows)}
  <section class="card">
    <h2>{_html_text('策略参数')}</h2>
    <table><thead><tr><th>{_html_text('方向')}</th><th>{_html_text('币种')}</th><th>{_html_text('策略')}</th><th>{_html_text('核心')}</th><th>{_html_text('保护')}</th><th>{_html_text('首档R')}</th><th>{_html_text('K线数')}</th><th>{_html_text('区间')}</th></tr></thead>
    <tbody>{''.join(run_body)}</tbody></table>
  </section>
  {period_table('年度盈亏', year_rows, limit=50)}
  {period_table('最近24个月盈亏', month_rows, limit=24)}
</body>
</html>
"""


def build_payload(runs: list[StrategyRun]) -> dict[str, object]:
    trades = [trade for run in runs for trade in run.trades]
    trades.sort(key=lambda item: (item.exit_ts, item.entry_ts, item.signal))
    grouped = group_metrics(runs)
    run_payload = []
    for run in runs:
        run_payload.append(
            {
                "side": run.side,
                "symbol": run.symbol,
                "coin": run.coin,
                "strategy_label": run.strategy_label,
                "core_label": run.core_label,
                "protection_label": run.protection_label,
                "candle_count": run.candle_count,
                "date_range": f"{_dt(run.start_ts)} ~ {_dt(run.end_ts)}",
                "config": {
                    "ema_type": run.config.ema_type,
                    "ema_period": run.config.ema_period,
                    "trend_ema_type": run.config.trend_ema_type,
                    "trend_ema_period": run.config.trend_ema_period,
                    "atr_stop_multiplier": str(run.config.atr_stop_multiplier),
                    "ema55_slope_lock_profit_trigger_r": run.config.ema55_slope_lock_profit_trigger_r,
                    "dynamic_two_r_break_even": run.config.dynamic_two_r_break_even,
                    "dynamic_fee_offset_enabled": run.config.dynamic_fee_offset_enabled,
                    "ema55_slope_exit_enabled": run.config.ema55_slope_exit_enabled,
                },
            }
        )
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "initial_capital": str(INITIAL_CAPITAL),
        "grouped_metrics": grouped,
        "runs": run_payload,
        "monthly_rows": period_rows(trades, fmt="%Y-%m"),
        "yearly_rows": period_rows(trades, fmt="%Y"),
    }


def _json_default(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"{type(value)!r} is not JSON serializable")


def main() -> None:
    client = OkxRestClient()
    runs: list[StrategyRun] = []
    for spec in build_specs():
        print(f"run {spec.side} {spec.symbol} {spec.core_label}")
        runs.append(run_strategy(client, spec))
    rows = trade_rows(runs)
    write_csv(rows)
    payload = build_payload(runs)
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8-sig")
    html_text = render_html(payload)
    HTML_PATH.write_text(html_text, encoding="utf-8-sig")
    PROJECT_HTML_PATH.write_text(html_text, encoding="utf-8-sig")
    print(HTML_PATH)
    print(CSV_PATH)
    print(JSON_PATH)
    print(PROJECT_HTML_PATH)


if __name__ == "__main__":
    main()
