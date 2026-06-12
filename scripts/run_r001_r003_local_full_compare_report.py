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

from okx_quant.backtest import BacktestReport, _run_backtest_with_loaded_data
from okx_quant.backtest_ui import _deserialize_strategy_config
from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import Instrument, StrategyConfig
from okx_quant.persistence import analysis_report_dir_path, backtest_history_file_path
from okx_quant.pricing import format_decimal_fixed
from scripts.run_btc_daily_ma_direction_filter_research import LONG_MAKER_FEE_RATE, LONG_TAKER_FEE_RATE


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASE_NAME = f"r001_r003_local_full_compare_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASE_NAME}.html"
CSV_PATH = REPORT_DIR / f"{BASE_NAME}.csv"
JSON_PATH = REPORT_DIR / f"{BASE_NAME}.json"
PROJECT_HTML_PATH = PROJECT_REPORT_DIR / "r001_r003_local_full_compare_latest.html"


@dataclass(frozen=True)
class CaseSpec:
    label: str
    snapshot_id: str
    export_file_name: str


@dataclass(frozen=True)
class CaseResult:
    label: str
    snapshot_id: str
    original_export_path: str
    config: StrategyConfig
    original_report: BacktestReport
    original_candle_count: int
    original_start_ts: int
    original_end_ts: int
    rerun_report: BacktestReport
    rerun_candle_count: int
    rerun_start_ts: int
    rerun_end_ts: int


CASES: tuple[CaseSpec, ...] = (
    CaseSpec(
        label="R001",
        snapshot_id="S125",
        export_file_name="single_20260611_085801_ema_dynamic_order_long_BTC-USDT-SWAP_1H_long_only",
    ),
    CaseSpec(
        label="R002",
        snapshot_id="S164",
        export_file_name="single_20260611_120007_ema_dynamic_order_long_BTC-USDT-SWAP_1H_long_only",
    ),
    CaseSpec(
        label="R003",
        snapshot_id="S174",
        export_file_name="single_20260611_182755_ema_dynamic_order_long_BTC-USDT-SWAP_1H_long_only",
    ),
)

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
    if value is None:
        return "-"
    return format_decimal_fixed(value, 4)


def _fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M")


def _resolved_reference_label(config: StrategyConfig) -> str:
    ma_type = (config.resolved_entry_reference_ema_type() or "ema").upper()
    period = config.resolved_entry_reference_ema_period()
    return f"{ma_type}{period}"


def _config_summary(config: StrategyConfig) -> str:
    trigger_r = max(int(config.ema55_slope_lock_profit_trigger_r), 2)
    return (
        f"{config.resolved_ema_type().upper()}{config.ema_period} / "
        f"{config.resolved_trend_ema_type().upper()}{config.trend_ema_period} / "
        f"{_resolved_reference_label(config)} / "
        f"ATR{config.atr_period} / "
        f"SLx{format_decimal_fixed(config.atr_stop_multiplier, 2)} / "
        f"TPx{format_decimal_fixed(config.atr_take_multiplier, 2)} / "
        f"首档触发R{trigger_r}"
    )


def _load_history_records() -> dict[str, dict[str, object]]:
    payload = json.loads(backtest_history_file_path().read_text(encoding="utf-8"))
    return {str(item["snapshot_id"]): item for item in payload.get("records", [])}


def _load_audit_payload(base_name: str) -> dict[str, object]:
    path = Path(rf"D:\qqokx_data\reports\backtest_exports\{base_name}.audit.json")
    return json.loads(path.read_text(encoding="utf-8"))


def _report_from_audit(audit_payload: dict[str, object]) -> BacktestReport:
    summary = audit_payload["report_summary"]
    return BacktestReport(
        total_trades=int(summary["total_trades"]),
        win_trades=int(summary["win_trades"]),
        loss_trades=int(summary["loss_trades"]),
        breakeven_trades=int(summary["breakeven_trades"]),
        win_rate=Decimal(str(summary["win_rate"])),
        total_pnl=Decimal(str(summary["total_pnl"])),
        average_pnl=Decimal(str(summary["average_pnl"])),
        gross_profit=Decimal(str(summary["gross_profit"])),
        gross_loss=Decimal(str(summary["gross_loss"])),
        profit_factor=None if summary.get("profit_factor") in (None, "") else Decimal(str(summary["profit_factor"])),
        average_win=Decimal(str(summary["gross_profit"])) / Decimal(str(summary["win_trades"]))
        if int(summary["win_trades"]) > 0
        else Decimal("0"),
        average_loss=Decimal(str(summary["gross_loss"])) / Decimal(str(summary["loss_trades"]))
        if int(summary["loss_trades"]) > 0
        else Decimal("0"),
        profit_loss_ratio=(
            (Decimal(str(summary["gross_profit"])) / Decimal(str(summary["win_trades"])))
            / (Decimal(str(summary["gross_loss"])) / Decimal(str(summary["loss_trades"])))
        )
        if int(summary["win_trades"]) > 0 and int(summary["loss_trades"]) > 0 and Decimal(str(summary["gross_loss"])) > 0
        else None,
        average_r_multiple=Decimal(str(summary["average_r_multiple"])),
        max_drawdown=Decimal(str(summary["max_drawdown"])),
        max_drawdown_pct=Decimal(str(summary["max_drawdown_pct"])),
        ending_equity=Decimal(str(summary["ending_equity"])),
        total_return_pct=Decimal(str(summary["total_return_pct"])),
        maker_fees=Decimal(str(summary["maker_fees"])),
        taker_fees=Decimal(str(summary["taker_fees"])),
        total_fees=Decimal(str(summary["total_fees"])),
        slippage_costs=Decimal(str(summary["slippage_costs"])),
        funding_costs=Decimal(str(summary["funding_costs"])),
    )


def _load_case_result(spec: CaseSpec, history_records: dict[str, dict[str, object]], full_candles: list) -> CaseResult:
    history_item = history_records[spec.snapshot_id]
    audit_payload = _load_audit_payload(spec.export_file_name)
    config = _deserialize_strategy_config(history_item["config"])
    rerun = _run_backtest_with_loaded_data(
        full_candles,
        BTC_SWAP_INSTRUMENT,
        config,
        data_source_note=f"local candle_cache full history | {spec.label}",
        maker_fee_rate=LONG_MAKER_FEE_RATE,
        taker_fee_rate=LONG_TAKER_FEE_RATE,
    )
    time_range = audit_payload["time_range"]
    counts = audit_payload["counts"]
    return CaseResult(
        label=spec.label,
        snapshot_id=spec.snapshot_id,
        original_export_path=str(history_item["export_path"]),
        config=config,
        original_report=_report_from_audit(audit_payload),
        original_candle_count=int(counts["candle_count"]),
        original_start_ts=int(time_range["start_ts"]),
        original_end_ts=int(time_range["end_ts"]),
        rerun_report=rerun.report,
        rerun_candle_count=len(full_candles),
        rerun_start_ts=full_candles[0].ts,
        rerun_end_ts=full_candles[-1].ts,
    )


def _build_rows(results: list[CaseResult]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for item in results:
        rows.append(
            {
                "label": item.label,
                "snapshot_id": item.snapshot_id,
                "config": _config_summary(item.config),
                "original_start": _fmt_ts(item.original_start_ts),
                "original_end": _fmt_ts(item.original_end_ts),
                "original_candles": item.original_candle_count,
                "original_trades": item.original_report.total_trades,
                "original_win_rate_pct": float(item.original_report.win_rate),
                "original_pnl_u": float(item.original_report.total_pnl),
                "original_return_pct": float(item.original_report.total_return_pct),
                "original_pf": None
                if item.original_report.profit_factor is None
                else float(item.original_report.profit_factor),
                "original_max_dd_u": float(item.original_report.max_drawdown),
                "rerun_start": _fmt_ts(item.rerun_start_ts),
                "rerun_end": _fmt_ts(item.rerun_end_ts),
                "rerun_candles": item.rerun_candle_count,
                "rerun_trades": item.rerun_report.total_trades,
                "rerun_win_rate_pct": float(item.rerun_report.win_rate),
                "rerun_pnl_u": float(item.rerun_report.total_pnl),
                "rerun_return_pct": float(item.rerun_report.total_return_pct),
                "rerun_pf": None if item.rerun_report.profit_factor is None else float(item.rerun_report.profit_factor),
                "rerun_max_dd_u": float(item.rerun_report.max_drawdown),
                "delta_trades": item.rerun_report.total_trades - item.original_report.total_trades,
                "delta_pnl_u": float(item.rerun_report.total_pnl - item.original_report.total_pnl),
                "delta_return_pct": float(item.rerun_report.total_return_pct - item.original_report.total_return_pct),
            }
        )
    return rows


def _write_csv(rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    with CSV_PATH.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_json(rows: list[dict[str, object]], results: list[CaseResult]) -> None:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "full_data_range": {
            "start": _fmt_ts(results[0].rerun_start_ts) if results else "",
            "end": _fmt_ts(results[0].rerun_end_ts) if results else "",
            "candle_count": results[0].rerun_candle_count if results else 0,
        },
        "rows": rows,
    }
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_conclusion_lines(results: list[CaseResult]) -> list[str]:
    lines: list[str] = []
    if len(results) >= 2:
        first, second = results[0], results[1]
        if _config_summary(first.config) == _config_summary(second.config):
            lines.append("R001 与 R002 的参数实际上相同，因此改为本地全量数据后，两者重跑结果也应一致。")
    lines.append("R003 使用的是 EMA5 / EMA13 / EMA5 / SLx1 / 首档触发R6；修复 nR 透传 bug 后，全量回测结果会明显不同于旧导出。")
    lines.append("本报告统一使用 BTC 1H 本地全量已收盘缓存，不再受原始回测窗口长度影响。")
    return lines


def _write_html(results: list[CaseResult]) -> None:
    rows_html = []
    for item in results:
        original = item.original_report
        rerun = item.rerun_report
        rows_html.append(
            """
            <tr>
              <td>{label}</td>
              <td>{snapshot_id}</td>
              <td>{config}</td>
              <td>{orig_range}<br>{orig_candles} 根</td>
              <td>{orig_trades}</td>
              <td>{orig_win}</td>
              <td>{orig_pnl}</td>
              <td>{orig_ret}</td>
              <td>{orig_pf}</td>
              <td>{orig_dd}</td>
              <td>{rerun_range}<br>{rerun_candles} 根</td>
              <td>{rerun_trades}</td>
              <td>{rerun_win}</td>
              <td>{rerun_pnl}</td>
              <td>{rerun_ret}</td>
              <td>{rerun_pf}</td>
              <td>{rerun_dd}</td>
              <td>{delta_trades}</td>
              <td>{delta_pnl}</td>
            </tr>
            """.format(
                label=_html_text(item.label),
                snapshot_id=_html_text(item.snapshot_id),
                config=_html_text(_config_summary(item.config)),
                orig_range=_html_text(f"{_fmt_ts(item.original_start_ts)} -> {_fmt_ts(item.original_end_ts)}"),
                orig_candles=_html_text(item.original_candle_count),
                orig_trades=_html_text(original.total_trades),
                orig_win=_html_text(f"{_fmt(original.win_rate)}%"),
                orig_pnl=_html_text(_fmt(original.total_pnl, 4)),
                orig_ret=_html_text(f"{_fmt(original.total_return_pct)}%"),
                orig_pf=_html_text(_fmt_pf(original.profit_factor)),
                orig_dd=_html_text(_fmt(original.max_drawdown, 4)),
                rerun_range=_html_text(f"{_fmt_ts(item.rerun_start_ts)} -> {_fmt_ts(item.rerun_end_ts)}"),
                rerun_candles=_html_text(item.rerun_candle_count),
                rerun_trades=_html_text(rerun.total_trades),
                rerun_win=_html_text(f"{_fmt(rerun.win_rate)}%"),
                rerun_pnl=_html_text(_fmt(rerun.total_pnl, 4)),
                rerun_ret=_html_text(f"{_fmt(rerun.total_return_pct)}%"),
                rerun_pf=_html_text(_fmt_pf(rerun.profit_factor)),
                rerun_dd=_html_text(_fmt(rerun.max_drawdown, 4)),
                delta_trades=_html_text(rerun.total_trades - original.total_trades),
                delta_pnl=_html_text(_fmt(rerun.total_pnl - original.total_pnl, 4)),
            )
        )
    conclusion_html = "".join(f"<li>{_html_text(line)}</li>" for line in _build_conclusion_lines(results))
    generated_at = _html_text(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>R001-R003 本地全量重跑对比</title>
  <style>
    :root {{
      --bg: #f7f2e8;
      --card: rgba(255,255,255,0.78);
      --ink: #1f2a2e;
      --muted: #5f6a6f;
      --line: rgba(102, 90, 62, 0.16);
      --accent: #0d7a68;
      --warm: #d2872c;
      --danger: #c5523f;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Microsoft YaHei UI", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(243, 208, 120, 0.35), transparent 32%),
        radial-gradient(circle at top right, rgba(140, 195, 168, 0.28), transparent 30%),
        linear-gradient(135deg, #f8f0db 0%, #f7f5ef 45%, #eef6f1 100%);
    }}
    .wrap {{
      max-width: 1600px;
      margin: 0 auto;
      padding: 28px 28px 40px;
    }}
    .hero {{
      background: linear-gradient(120deg, rgba(255, 233, 184, 0.88), rgba(227, 242, 233, 0.84));
      border: 1px solid var(--line);
      border-radius: 26px;
      padding: 28px 30px;
      box-shadow: 0 18px 50px rgba(89, 74, 35, 0.10);
    }}
    h1 {{
      margin: 0 0 10px;
      font-size: 46px;
      line-height: 1.05;
    }}
    .sub {{
      color: var(--muted);
      font-size: 17px;
      line-height: 1.7;
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 18px;
      margin-top: 24px;
    }}
    .card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 22px;
      padding: 20px 22px;
      box-shadow: 0 14px 32px rgba(45, 53, 44, 0.08);
    }}
    .eyebrow {{
      color: var(--muted);
      font-size: 13px;
      letter-spacing: .06em;
      text-transform: uppercase;
    }}
    .card h2 {{
      margin: 8px 0 8px;
      font-size: 34px;
      color: var(--accent);
    }}
    .metric {{
      font-size: 28px;
      font-weight: 700;
      margin: 10px 0 6px;
    }}
    .section {{
      margin-top: 24px;
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 24px;
      padding: 22px 24px;
      box-shadow: 0 14px 32px rgba(45, 53, 44, 0.08);
    }}
    .section h3 {{
      margin: 0 0 14px;
      font-size: 24px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 10px 10px;
      text-align: left;
      vertical-align: top;
    }}
    th {{
      background: rgba(255,255,255,0.7);
      position: sticky;
      top: 0;
      z-index: 1;
    }}
    .delta-pos {{ color: var(--accent); font-weight: 700; }}
    .delta-neg {{ color: var(--danger); font-weight: 700; }}
    ul {{ margin: 0; padding-left: 20px; line-height: 1.8; }}
    .path {{ font-family: Consolas, monospace; font-size: 13px; color: var(--muted); }}
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
      <h1>R001-R003 本地全量重跑对比</h1>
      <div class="sub">
        生成时间：{generated_at}<br>
        口径：保留 R001 / R002 / R003 的原始参数；时间统一改为 BTC 1H 本地全量已收盘缓存；对比原导出结果与修复后的全量重跑结果。
      </div>
    </div>
    <div class="cards">
      {''.join(
        f'''
        <div class="card">
          <div class="eyebrow">{_html_text(item.label)} / {_html_text(item.snapshot_id)}</div>
          <h2>{_html_text(item.label)}</h2>
          <div>{_html_text(_config_summary(item.config))}</div>
          <div class="metric">{_html_text(_fmt(item.rerun_report.total_pnl, 4))}</div>
          <div>全量重跑净利</div>
          <div style="margin-top:8px;">交易 {_html_text(item.rerun_report.total_trades)} 笔 / PF {_html_text(_fmt_pf(item.rerun_report.profit_factor))} / 回撤 {_html_text(_fmt(item.rerun_report.max_drawdown, 4))}</div>
        </div>
        '''
        for item in results
      )}
    </div>
    <div class="section">
      <h3>对比表</h3>
      <table>
        <thead>
          <tr>
            <th>编号</th>
            <th>快线 / 趋势 / 挂单线 / ATR / R</th>
            <th>原始区间</th>
            <th>原始交易</th>
            <th>原始胜率</th>
            <th>原始净利</th>
            <th>原始收益率</th>
            <th>原始PF</th>
            <th>原始回撤</th>
            <th>全量区间</th>
            <th>全量交易</th>
            <th>全量胜率</th>
            <th>全量净利</th>
            <th>全量收益率</th>
            <th>全量PF</th>
            <th>全量回撤</th>
            <th>交易差值</th>
            <th>净利差值</th>
          </tr>
        </thead>
        <tbody>
          {''.join(rows_html)}
        </tbody>
      </table>
    </div>
    <div class="section">
      <h3>说明</h3>
      <ul>{conclusion_html}</ul>
    </div>
    <div class="section">
      <h3>原始报告路径</h3>
      {''.join(f'<div class="path">{_html_text(item.label)}: {_html_text(item.original_export_path)}</div>' for item in results)}
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
    history_records = _load_history_records()
    results = [_load_case_result(spec, history_records, full_candles) for spec in CASES]
    rows = _build_rows(results)
    _write_csv(rows)
    _write_json(rows, results)
    _write_html(results)
    print(HTML_PATH)
    print(PROJECT_HTML_PATH)


if __name__ == "__main__":
    main()
