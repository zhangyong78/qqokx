from __future__ import annotations

import csv
import html
import json
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import BacktestReport, BacktestResult, BacktestTrade, _build_report
from okx_quant.models import Instrument, StrategyConfig
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID, STRATEGY_EMA55_SLOPE_SHORT_ID
from okx_quant.strategy_profiles import (
    STRATEGY_PROFILE_SCHEMA_VERSION,
    StrategyBundle,
    build_strategy_profile_from_config,
    write_strategy_bundle,
)
from okx_quant.strategy_symbol_defaults import get_strategy_symbol_parameter_defaults


BUNDLE_NAME = "最佳参数组合包"
HTML_NAME = "最佳参数组合包说明.html"
PACKAGE_DIR = analysis_report_dir_path() / "packages"
PACKAGE_DIR.mkdir(parents=True, exist_ok=True)
JSON_PATH = PACKAGE_DIR / f"{BUNDLE_NAME}.json"
HTML_PATH = PACKAGE_DIR / HTML_NAME
REPORTS_DIR = ROOT / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
LEGACY_HTML_PATH = REPORTS_DIR / HTML_NAME

STANDARD_REPORT_DIR = ROOT / "reports" / "best_parameter_bundle_1h_standard_100u"
STANDARD_TRADES_CSV = STANDARD_REPORT_DIR / "trades.csv"

BUNDLE_INITIAL_CAPITAL = Decimal("10000")

_DECIMAL_FIELDS = {
    "atr_stop_multiplier",
    "atr_take_multiplier",
    "order_size",
    "risk_amount",
    "atr_percentile_filter_max",
    "trend_ema_slope_filter_min_ratio",
    "body_retest_breakdown_atr_multiplier",
    "body_retest_retest_atr_multiplier",
    "body_retest_stop_buffer_atr_multiplier",
    "body_retest_body_atr_limit",
}

_LONG_NOTE_MAP: dict[str, str] = {
    "BTC-USDT-SWAP": "BTC 做多默认参数已同步到参数包、UI 与实盘；下方统计按固定风险 100U、初始资金 10000U、非复利标准口径展示。",
    "ETH-USDT-SWAP": "ETH 做多默认参数已同步到参数包、UI 与实盘；下方统计按固定风险 100U、初始资金 10000U、非复利标准口径展示。",
    "SOL-USDT-SWAP": "SOL 做多默认参数已同步到参数包、UI 与实盘；下方统计按固定风险 100U、初始资金 10000U、非复利标准口径展示。",
    "DOGE-USDT-SWAP": "DOGE 做多默认参数已同步到参数包、UI 与实盘；下方统计按固定风险 100U、初始资金 10000U、非复利标准口径展示。",
}

_SHORT_NOTE_MAP: dict[str, str] = {
    "BTC-USDT-SWAP": "BTC 做空默认参数已同步到参数包、UI 与实盘；下方统计按固定风险 100U、初始资金 10000U、非复利标准口径展示。",
    "ETH-USDT-SWAP": "ETH 做空默认参数已同步到参数包、UI 与实盘；下方统计按固定风险 100U、初始资金 10000U、非复利标准口径展示。",
    "SOL-USDT-SWAP": "SOL 做空默认参数已同步到参数包、UI 与实盘；下方统计按固定风险 100U、初始资金 10000U、非复利标准口径展示。",
    "DOGE-USDT-SWAP": "DOGE 做空默认参数已同步到参数包、UI 与实盘；Bear-Reentry 复核未带来足够大的综合提升，当前定稿暂不调整。下方统计按固定风险 100U、初始资金 10000U、非复利标准口径展示。",
}


@dataclass(frozen=True)
class BundleSpec:
    side: str
    symbol: str
    profile_id: str
    profile_name: str
    strategy_id: str
    strategy_label: str
    core_label: str
    protection_label: str
    note: str
    config: StrategyConfig


@dataclass(frozen=True)
class BundleRun:
    spec: BundleSpec
    result: BacktestResult
    data_source_note: str

    @property
    def coin(self) -> str:
        return self.spec.symbol.replace("-USDT-SWAP", "")

    @property
    def candle_count(self) -> int:
        return len(self.result.candles)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _fmt_decimal(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _fmt_fixed(value: object, places: str = "0.0000") -> str:
    if value in (None, ""):
        value = Decimal("0")
    return str(Decimal(str(value)).quantize(Decimal(places)))


def _html_text(text: str) -> str:
    escaped = html.escape(text, quote=True)
    return "".join(f"&#x{ord(ch):X};" if ord(ch) > 127 else ch for ch in escaped)


def _coerce_value(key: str, value: object) -> object:
    if value in (None, ""):
        return None if key == "risk_amount" else value
    if key in _DECIMAL_FIELDS:
        return Decimal(str(value))
    return value


def _build_config_from_defaults(strategy_id: str, symbol: str, signal_mode: str) -> StrategyConfig:
    defaults = get_strategy_symbol_parameter_defaults(strategy_id, symbol, "launcher")
    kwargs: dict[str, object] = {
        "inst_id": symbol,
        "bar": str(defaults.get("bar", "1H")),
        "ema_period": int(defaults.get("ema_period", 21)),
        "atr_period": int(defaults.get("atr_period", 10)),
        "atr_stop_multiplier": Decimal(str(defaults.get("atr_stop_multiplier", "2"))),
        "atr_take_multiplier": Decimal(str(defaults.get("atr_take_multiplier", "4"))),
        "order_size": Decimal("0"),
        "trade_mode": "cross",
        "signal_mode": signal_mode,
        "position_mode": "net",
        "environment": "live",
        "tp_sl_trigger_type": "mark",
        "strategy_id": strategy_id,
        "trade_inst_id": symbol,
        "tp_sl_mode": "exchange",
        "entry_side_mode": "follow_signal",
        "run_mode": "trade",
        "backtest_initial_capital": BUNDLE_INITIAL_CAPITAL,
        "backtest_sizing_mode": "fixed_risk",
        "take_profit_mode": str(defaults.get("take_profit_mode", "dynamic")),
    }
    for key, value in defaults.items():
        kwargs[key] = _coerce_value(key, value)
    return StrategyConfig(**kwargs)


def _format_rule(rule: dict[str, object], *, fee_offset_enabled: bool) -> str:
    trigger_r = int(rule.get("trigger_r", 0) or 0)
    action = str(rule.get("action", "") or "")
    fee_text = " + 双向手续费" if fee_offset_enabled else ""
    if action == "break_even":
        return f"{trigger_r}R 保本{fee_text}"
    lock_r = int(rule.get("lock_r", 0) or 0)
    if str(rule.get("trail_mode", "") or "") == "step":
        every_r = int(rule.get("trail_every_r", 1) or 1)
        add_r = int(rule.get("trail_add_r", 1) or 1)
        return f"{trigger_r}R 锁 {lock_r}R{fee_text}，之后每 {every_r}R 再上移 {add_r}R"
    return f"{trigger_r}R 锁 {lock_r}R{fee_text}"


def _dynamic_rules_payload(config: StrategyConfig) -> tuple[dict[str, object], ...]:
    payload = config.dynamic_protection_rules
    if not payload:
        return ()
    return tuple(dict(item) for item in payload)


def _build_core_label(config: StrategyConfig, *, include_entry_reference: bool) -> str:
    fast = config.ema_label()
    trend = config.trend_ema_label()
    if not include_entry_reference:
        return f"{fast} / {trend}"
    reference = config.entry_reference_line_label().replace("跟随快线(", "入场跟随 ").replace(")", "")
    if reference.startswith("EMA") or reference.startswith("MA"):
        reference = f"入场 {reference}"
    return f"{fast} / {trend} / {reference}"


def _build_protection_label(config: StrategyConfig, *, include_entries: bool, append_exit_rule: bool) -> str:
    parts = [f"ATR{config.atr_period} / SL{_fmt_decimal(config.atr_stop_multiplier)}"]
    rules = _dynamic_rules_payload(config)
    fee_enabled = bool(config.dynamic_fee_offset_enabled)
    if rules:
        parts.extend(_format_rule(rule, fee_offset_enabled=fee_enabled) for rule in rules)
    if append_exit_rule and bool(config.ema55_slope_exit_enabled):
        parts.append("斜率转正平仓")
    if include_entries:
        entries_text = "不限" if config.max_entries_per_trend <= 0 else str(config.max_entries_per_trend)
        parts.append(f"每波 {entries_text} 次")
    return " / ".join(parts)


def build_specs() -> tuple[BundleSpec, ...]:
    specs: list[BundleSpec] = []
    symbols = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "DOGE-USDT-SWAP")

    for symbol in symbols:
        config = _build_config_from_defaults(STRATEGY_DYNAMIC_LONG_ID, symbol, "long_only")
        coin = symbol.replace("-USDT-SWAP", "")
        specs.append(
            BundleSpec(
                side="做多",
                symbol=symbol,
                profile_id=f"dynamic_long_best_{coin.lower()}_v2",
                profile_name=f"{coin} 动态委托做多 最佳参数",
                strategy_id=STRATEGY_DYNAMIC_LONG_ID,
                strategy_label="EMA 动态委托做多",
                core_label=_build_core_label(config, include_entry_reference=True),
                protection_label=_build_protection_label(config, include_entries=True, append_exit_rule=False),
                note=_LONG_NOTE_MAP[symbol],
                config=config,
            )
        )

    for symbol in symbols:
        config = _build_config_from_defaults(STRATEGY_EMA55_SLOPE_SHORT_ID, symbol, "short_only")
        coin = symbol.replace("-USDT-SWAP", "")
        specs.append(
            BundleSpec(
                side="做空",
                symbol=symbol,
                profile_id=f"slope_short_best_{coin.lower()}_v2",
                profile_name=f"{coin} 均线斜率做空 最佳参数",
                strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
                strategy_label="均线斜率做空",
                core_label=_build_core_label(config, include_entry_reference=False),
                protection_label=_build_protection_label(config, include_entries=False, append_exit_rule=True),
                note=_SHORT_NOTE_MAP[symbol],
                config=config,
            )
        )

    return tuple(specs)


def build_bundle(specs: tuple[BundleSpec, ...]) -> StrategyBundle:
    profiles = [
        build_strategy_profile_from_config(
            profile_id=spec.profile_id,
            profile_name=spec.profile_name,
            strategy_id=spec.strategy_id,
            symbol=spec.symbol,
            config=spec.config,
            direction_label=spec.side,
            run_mode_label="交易并下单",
            enabled=True,
            tags=("best-parameter-bundle", "2026-06-16", "long" if spec.side == "做多" else "short"),
            notes=spec.note,
            source_report=str(Path(__file__).resolve()),
        )
        for spec in specs
    ]
    return StrategyBundle(
        bundle_version=STRATEGY_PROFILE_SCHEMA_VERSION,
        bundle_name=BUNDLE_NAME,
        profiles=tuple(profiles),
        created_at=_utc_now(),
        source_report=str(Path(__file__).resolve()),
        auto_start_on_import=True,
    )


_BACKTEST_RANGE_CLAUSE_RE = re.compile(r"(?:[；。]\s*)?回测区间.*$", re.S)


def _strip_backtest_range_clause(note: str) -> str:
    cleaned = _BACKTEST_RANGE_CLAUSE_RE.sub("", note).strip(" ；。")
    if cleaned and cleaned[-1] not in "。！？":
        cleaned += "。"
    return cleaned or note.strip()


def _note_takeaway_text(note: str) -> str:
    cleaned = _strip_backtest_range_clause(note)
    parts = [part.strip(" 。；;") for part in re.split(r"[；。]", cleaned) if part.strip(" 。；;")]
    if len(parts) <= 1:
        return ""
    takeaway = parts[-1]
    if takeaway and takeaway[-1] not in "。！？":
        takeaway += "。"
    return takeaway


def _fmt_note_report_line(report: BacktestReport) -> str:
    pf_text = "-" if report.profit_factor is None else _fmt_fixed(report.profit_factor)
    return (
        f"PnL {_fmt_fixed(report.total_pnl)} / DD {_fmt_fixed(report.max_drawdown)} / "
        f"PF {pf_text} / Trades {report.total_trades}"
    )


def _slice_trades_from(trades: tuple[BacktestTrade, ...], *, start: datetime) -> tuple[BacktestTrade, ...]:
    start_ms = int(start.timestamp() * 1000)
    return tuple(trade for trade in trades if int(trade.exit_ts) >= start_ms)


def _format_bundle_ts(ts: int) -> str:
    return datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def _bundle_run_backtest_range_text(run: BundleRun) -> str:
    candles = tuple(run.result.candles)
    if candles:
        return f"回测区间：{_format_bundle_ts(candles[0].ts)} -> {_format_bundle_ts(candles[-1].ts)}；样本：{len(candles)} 根（全量）。"
    trades = tuple(run.result.trades)
    if trades:
        start_ts = min(int(trade.entry_ts) for trade in trades)
        end_ts = max(int(trade.exit_ts) for trade in trades)
        return f"回测区间：{_format_bundle_ts(start_ts)} -> {_format_bundle_ts(end_ts)}；样本：{len(trades)} 笔（按成交统计）。"
    return "暂无回测区间。"


def _strategy_detail_note_html(spec: BundleSpec, run: BundleRun | None) -> str:
    if run is None:
        return (
            '<div class="note-stack">'
            f'<div class="note-block note-block-primary"><span class="note-copy">{_html_text(_strip_backtest_range_clause(spec.note))}</span></div>'
            "</div>"
        )

    oos_start = datetime(2022, 1, 1, tzinfo=timezone.utc)
    oos_trades = _slice_trades_from(tuple(run.result.trades), start=oos_start)
    oos_report = _build_report(list(oos_trades), initial_capital=BUNDLE_INITIAL_CAPITAL)
    lines = [
        '<div class="note-stack">',
        (
            f'<div class="note-block note-block-primary"><strong class="note-tag">{_html_text("定稿结论")}</strong>'
            f'<span class="note-copy">{_html_text(f"{run.coin}{spec.side}当前默认采用 {spec.core_label}；保护口径为 {spec.protection_label}。")}</span></div>'
        ),
        (
            f'<div class="note-block note-block-stats"><strong class="note-tag">{_html_text("全样本")}</strong>'
            f'<span class="note-copy">{_html_text(_fmt_note_report_line(run.result.report))}</span></div>'
        ),
        (
            f'<div class="note-block note-block-stats"><strong class="note-tag">{_html_text("样本外")}</strong>'
            f'<span class="note-copy">{_html_text(f"2022-01-01 之后 {_fmt_note_report_line(oos_report)}")}</span></div>'
        ),
    ]
    takeaway = _note_takeaway_text(spec.note)
    if takeaway:
        lines.append(
            f'<div class="note-block note-block-research"><strong class="note-tag">{_html_text("研究备注")}</strong>'
            f'<span class="note-copy">{_html_text(takeaway)}</span></div>'
        )
    lines.append(f'<div class="note-meta">{_html_text(_bundle_run_backtest_range_text(run))}</div>')
    lines.append("</div>")
    return "".join(lines)


def _combined_period_report(
    long_trades: tuple[BacktestTrade, ...],
    short_trades: tuple[BacktestTrade, ...],
) -> BacktestReport:
    all_trades = tuple(sorted((*long_trades, *short_trades), key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal)))
    return _build_report(list(all_trades), initial_capital=BUNDLE_INITIAL_CAPITAL)


def _period_key(exit_ts: int, by: str) -> str:
    dt = datetime.fromtimestamp(int(exit_ts) / 1000, tz=timezone.utc)
    if by == "year":
        return dt.strftime("%Y")
    return dt.strftime("%Y-%m")


def _period_pnl_percent(period_pnl: Decimal, start_equity: Decimal) -> str:
    if start_equity == 0:
        return "0.00%"
    return _fmt_fixed((period_pnl * Decimal("100")) / start_equity, "0.01") + "%"


def _drawdown_percent(drawdown: Decimal, peak_equity: Decimal) -> str:
    if peak_equity == 0:
        return "0.00%"
    return _fmt_fixed((drawdown * Decimal("100")) / peak_equity, "0.01") + "%"


def _combined_period_rows(
    long_trades: tuple[BacktestTrade, ...],
    short_trades: tuple[BacktestTrade, ...],
    *,
    by: str,
) -> tuple[tuple[object, ...], ...]:
    if by not in {"year", "month"}:
        raise ValueError("by must be 'year' or 'month'")

    all_trades = tuple(sorted((*long_trades, *short_trades), key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal)))
    if not all_trades:
        return ()

    bucket_keys: list[str] = []
    bucket_map: dict[str, list[BacktestTrade]] = {}
    for trade in all_trades:
        key = _period_key(int(trade.exit_ts), by)
        if key not in bucket_map:
            bucket_keys.append(key)
            bucket_map[key] = []
        bucket_map[key].append(trade)

    equity = BUNDLE_INITIAL_CAPITAL
    peak_equity = BUNDLE_INITIAL_CAPITAL
    rows: list[tuple[object, ...]] = []
    for key in bucket_keys:
        bucket = tuple(bucket_map[key])
        long_bucket = tuple(trade for trade in bucket if trade.signal == "long")
        short_bucket = tuple(trade for trade in bucket if trade.signal == "short")
        long_pnl = sum((trade.pnl for trade in long_bucket), Decimal("0"))
        short_pnl = sum((trade.pnl for trade in short_bucket), Decimal("0"))
        period_pnl = long_pnl + short_pnl
        start_equity = equity
        max_bucket_drawdown = Decimal("0")
        for trade in bucket:
            equity += trade.pnl
            peak_equity = max(peak_equity, equity)
            max_bucket_drawdown = max(max_bucket_drawdown, peak_equity - equity)
        win_trades = sum(1 for trade in bucket if trade.pnl > 0)
        total_trades = len(bucket)
        win_rate = Decimal("0") if total_trades <= 0 else (Decimal(win_trades) * Decimal("100") / Decimal(total_trades))
        rows.append(
            (
                key,
                len(long_bucket),
                _fmt_fixed(long_pnl),
                len(short_bucket),
                _fmt_fixed(short_pnl),
                total_trades,
                _fmt_fixed(win_rate, "0.01") + "%",
                _fmt_fixed(period_pnl),
                _period_pnl_percent(period_pnl, start_equity),
                _fmt_fixed(max_bucket_drawdown),
                _drawdown_percent(max_bucket_drawdown, peak_equity),
                _fmt_fixed(equity),
            )
        )
    return tuple(rows)


def _json_ready(value: object) -> object:
    if isinstance(value, Decimal):
        return _fmt_decimal(value)
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    return value


def _config_json(config: StrategyConfig) -> str:
    return json.dumps({key: _json_ready(value) for key, value in asdict(config).items()}, ensure_ascii=False, indent=2)


def _simple_table_html(headers: tuple[str, ...], rows: tuple[tuple[object, ...], ...]) -> str:
    head = "".join(f"<th>{_html_text(str(item))}</th>" for item in headers)
    body = "".join(
        "<tr>" + "".join(f"<td>{_html_text(str(item))}</td>" for item in row) + "</tr>"
        for row in rows
    )
    return f'<table class="subtable"><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>'


def _empty_period_row(label: str) -> tuple[object, ...]:
    return (label, 0, "0.0000", 0, "0.0000", 0, "0.00%", "0.0000", "0.00%", "0.0000", "0.00%", "10000.0000")


def _period_overview_html(
    *,
    title: str,
    report: BacktestReport,
    yearly_rows: tuple[tuple[object, ...], ...],
    monthly_rows: tuple[tuple[object, ...], ...],
    long_trades: int,
    short_trades: int,
) -> str:
    year_start = str(yearly_rows[0][0]) if yearly_rows else "-"
    year_end = str(yearly_rows[-1][0]) if yearly_rows else "-"
    latest_month = str(monthly_rows[-1][0]) if monthly_rows else "-"
    chips = (
        ("PnL", _fmt_fixed(report.total_pnl)),
        ("Max DD", _fmt_fixed(report.max_drawdown)),
        ("Trades", str(report.total_trades)),
        ("Ending Equity", _fmt_fixed(report.ending_equity)),
    )
    chips_html = "".join(
        '<div class="metric-chip">'
        f'<span class="metric-label">{_html_text(label)}</span>'
        f'<strong class="metric-value">{_html_text(value)}</strong>'
        "</div>"
        for label, value in chips
    )
    summary_text = f"Coverage {year_start} -> {year_end}; latest month {latest_month}; long {long_trades} / short {short_trades}"
    return (
        '<div class="period-overview">'
        f"<h3>{_html_text(title)}</h3>"
        f'<p class="period-summary">{_html_text(summary_text)}</p>'
        f'<div class="metric-row">{chips_html}</div>'
        "</div>"
    )


def _parse_decimal(text: str) -> Decimal:
    cleaned = str(text or "0").replace(",", "").strip()
    return Decimal(cleaned or "0")


def _parse_report_ts(text: str) -> int:
    dt = datetime.strptime(text.strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _placeholder_instrument(symbol: str) -> Instrument:
    return Instrument(
        inst_id=symbol,
        inst_type="SWAP",
        tick_size=Decimal("0.0001"),
        lot_size=Decimal("1"),
        min_size=Decimal("1"),
        state="live",
    )


def _trade_from_report_row(row: dict[str, str]) -> BacktestTrade:
    direction = row.get("方向", "")
    signal = "long" if direction == "多头" else "short"
    pnl = _parse_decimal(row.get("盈亏", "0"))
    r_multiple = _parse_decimal(row.get("R倍数", "0"))
    return BacktestTrade(
        signal=signal,
        entry_index=0,
        exit_index=0,
        entry_ts=_parse_report_ts(row["开仓时间"]),
        exit_ts=_parse_report_ts(row["平仓时间"]),
        entry_price=_parse_decimal(row.get("开仓价", "0")),
        exit_price=_parse_decimal(row.get("平仓价", "0")),
        stop_loss=Decimal("0"),
        take_profit=Decimal("0"),
        size=_parse_decimal(row.get("成交数量", "0")),
        gross_pnl=pnl,
        pnl=pnl,
        risk_value=_parse_decimal(row.get("风险金额", "0")),
        r_multiple=r_multiple,
        exit_reason=row.get("平仓原因", "") or "",
    )


def _load_bundle_runs(specs: tuple[BundleSpec, ...]) -> tuple[BundleRun, ...]:
    if not STANDARD_TRADES_CSV.exists():
        return ()

    grouped: dict[tuple[str, str], list[BacktestTrade]] = {}
    with STANDARD_TRADES_CSV.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            coin = row.get("币种", "").strip()
            direction = row.get("方向", "").strip()
            if not coin or direction not in {"多头", "空头"}:
                continue
            side = "做多" if direction == "多头" else "做空"
            grouped.setdefault((coin, side), []).append(_trade_from_report_row(row))

    runs: list[BundleRun] = []
    for spec in specs:
        coin = spec.symbol.replace("-USDT-SWAP", "")
        trades = sorted(grouped.get((coin, spec.side), []), key=lambda item: (item.exit_ts, item.entry_ts, item.signal))
        report = _build_report(trades, initial_capital=BUNDLE_INITIAL_CAPITAL)
        result = BacktestResult(
            candles=[],
            trades=trades,
            report=report,
            instrument=_placeholder_instrument(spec.symbol),
        )
        runs.append(BundleRun(spec=spec, result=result, data_source_note=str(STANDARD_TRADES_CSV)))
    return tuple(runs)


def _coin_order(specs: tuple[BundleSpec, ...]) -> tuple[str, ...]:
    ordered: list[str] = []
    for spec in specs:
        coin = spec.symbol.replace("-USDT-SWAP", "")
        if coin not in ordered:
            ordered.append(coin)
    return tuple(ordered)


def _coin_period_tables_html(specs: tuple[BundleSpec, ...], runs: tuple[BundleRun, ...]) -> str:
    if not runs:
        return (
            '<div class="strategy-card">'
            f'<p>{_html_text(f"未找到标准 100U 组合回测明细：{STANDARD_TRADES_CSV}。先生成标准 100U 组合报告后，再重建本说明即可自动带出年度/月度统计。")}</p>'
            "</div>"
        )

    headers = (
        "周期",
        "多交易数",
        "多盈亏",
        "空交易数",
        "空盈亏",
        "合计交易数",
        "胜率",
        "总盈亏",
        "收益率",
        "最大回撤",
        "回撤比例",
        "期末权益",
    )
    run_map = {(run.coin, run.spec.side): run for run in runs}
    cards: list[str] = []

    for coin in _coin_order(specs):
        long_run = run_map.get((coin, "做多"))
        short_run = run_map.get((coin, "做空"))
        long_trades = tuple(long_run.result.trades) if long_run else ()
        short_trades = tuple(short_run.result.trades) if short_run else ()
        yearly_rows = _combined_period_rows(long_trades, short_trades, by="year")
        monthly_rows = _combined_period_rows(long_trades, short_trades, by="month")
        report = _combined_period_report(long_trades, short_trades)
        cards.append(
            '<div class="strategy-card">'
            + _period_overview_html(
                title=f"{coin} 年度 / 月度统计",
                report=report,
                yearly_rows=yearly_rows,
                monthly_rows=monthly_rows,
                long_trades=len(long_trades),
                short_trades=len(short_trades),
            )
            + f'<details open><summary>{_html_text("年度统计")}</summary><div class="table-scroll">{_simple_table_html(headers, yearly_rows or (_empty_period_row("无数据"),))}</div></details>'
            + f'<details><summary>{_html_text("月度统计")}</summary><div class="table-scroll">{_simple_table_html(headers, monthly_rows or (_empty_period_row("无数据"),))}</div></details>'
            + "</div>"
        )

    long_all = tuple(trade for run in runs if run.spec.side == "做多" for trade in run.result.trades)
    short_all = tuple(trade for run in runs if run.spec.side == "做空" for trade in run.result.trades)
    overall_yearly_rows = _combined_period_rows(long_all, short_all, by="year")
    overall_monthly_rows = _combined_period_rows(long_all, short_all, by="month")
    overall_report = _combined_period_report(long_all, short_all)
    cards.append(
        '<div class="strategy-card">'
        + _period_overview_html(
            title="全组合统计",
            report=overall_report,
            yearly_rows=overall_yearly_rows,
            monthly_rows=overall_monthly_rows,
            long_trades=len(long_all),
            short_trades=len(short_all),
        )
        + f'<p>{_html_text("口径：固定风险 100U、初始资金 10000U、非复利；多/空列展示各方向交易数与盈亏，合计列按该币或全组合的合并资金曲线统计收益率、回撤与期末权益。")}</p>'
        + f'<details open><summary>{_html_text("年度统计")}</summary><div class="table-scroll">{_simple_table_html(headers, overall_yearly_rows or (_empty_period_row("无数据"),))}</div></details>'
        + f'<details><summary>{_html_text("月度统计")}</summary><div class="table-scroll">{_simple_table_html(headers, overall_monthly_rows or (_empty_period_row("无数据"),))}</div></details>'
        + f'<p><small>{_html_text(f"K线来源：{STANDARD_TRADES_CSV}；由标准 100U 组合回测成交明细重建。")}</small></p>'
        + "</div>"
    )
    return "".join(cards)


def build_html(specs: tuple[BundleSpec, ...]) -> str:
    runs = _load_bundle_runs(specs)
    run_map = {run.spec.profile_id: run for run in runs}
    rows: list[str] = []
    config_cards: list[str] = []
    for spec in specs:
        symbol_label = spec.symbol.replace("-USDT-SWAP", "")
        rows.append(
            "<tr>"
            f"<td>{_html_text(spec.side)}</td>"
            f"<td>{_html_text(symbol_label)}</td>"
            f"<td>{_html_text(spec.strategy_label)}</td>"
            f"<td>{_html_text(spec.core_label)}</td>"
            f"<td>{_html_text(spec.protection_label)}</td>"
            f'<td class="note-cell">{_strategy_detail_note_html(spec, run_map.get(spec.profile_id))}</td>'
            "</tr>"
        )
        config_title = f"{spec.side} {symbol_label} | {spec.strategy_label}"
        config_cards.append(
            '<details class="config-card">'
            f"<summary>{_html_text(config_title)}</summary>"
            f"<p>{_html_text(spec.note)}</p>"
            f"<pre>{_html_text(_config_json(spec.config))}</pre>"
            "</details>"
        )

    title = _html_text(BUNDLE_NAME + "说明")
    generated_at = _html_text(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    json_path = _html_text(str(JSON_PATH))
    intro = _html_text("这份说明直接取当前默认模板生成，并与组合包 JSON、回测导入参数、UI 默认值和实盘默认值保持同一口径。")
    rows_html = "".join(rows)
    configs_html = "".join(config_cards)
    coin_period_tables = _coin_period_tables_html(specs, runs)

    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>{title}</title>
  <style>
    :root {{
      --bg: #f6efe3;
      --panel: rgba(255, 250, 242, 0.94);
      --line: #d7c4a4;
      --ink: #18242a;
      --accent: #0f766e;
      --warm: #9a3412;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      padding: 28px;
      background:
        radial-gradient(circle at top right, rgba(15, 118, 110, 0.12), transparent 28%),
        radial-gradient(circle at top left, rgba(154, 52, 18, 0.10), transparent 24%),
        var(--bg);
      color: var(--ink);
      font-family: "Microsoft YaHei UI", "Microsoft YaHei", sans-serif;
      line-height: 1.6;
    }}
    .panel {{
      max-width: 1360px;
      margin: 0 auto 18px auto;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 22px 24px;
      box-shadow: 0 18px 50px rgba(44, 38, 26, 0.08);
    }}
    h1, h2 {{ margin: 0 0 12px 0; }}
    h1 {{ font-size: 38px; color: #0b5d58; }}
    h2 {{ font-size: 22px; color: var(--warm); }}
    h3 {{ margin: 0 0 10px 0; color: #0b5d58; }}
    p {{ margin: 0 0 10px 0; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 15px;
    }}
    .strategy-table {{
      table-layout: fixed;
    }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 12px 10px;
      text-align: left;
      vertical-align: top;
      word-break: break-word;
    }}
    th {{
      background: rgba(230, 244, 239, 0.88);
      color: #0b5d58;
    }}
    .meta {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 12px;
      margin-top: 14px;
    }}
    .chip {{
      background: rgba(230, 244, 239, 0.85);
      border: 1px solid rgba(15, 118, 110, 0.18);
      border-radius: 14px;
      padding: 12px 14px;
    }}
    .strategy-card {{
      margin-top: 14px;
      border: 1px solid rgba(215, 196, 164, 0.9);
      border-radius: 16px;
      padding: 14px 16px;
      background: rgba(255, 255, 255, 0.72);
    }}
    details {{
      margin-top: 12px;
      border: 1px solid rgba(215, 196, 164, 0.9);
      border-radius: 14px;
      padding: 12px 14px;
      background: rgba(255, 255, 255, 0.7);
    }}
    summary {{
      cursor: pointer;
      font-weight: 700;
      color: #0b5d58;
    }}
    pre {{
      margin: 10px 0 0 0;
      padding: 14px;
      border-radius: 12px;
      background: #f6fbf9;
      border: 1px solid rgba(15, 118, 110, 0.12);
      overflow-x: auto;
      white-space: pre-wrap;
      word-break: break-word;
      font-size: 12px;
      line-height: 1.55;
    }}
    .subtable {{
      margin-top: 12px;
      font-size: 14px;
      min-width: 1040px;
    }}
    .table-scroll {{
      overflow-x: auto;
      padding-bottom: 4px;
    }}
    .period-overview {{
      margin-bottom: 12px;
    }}
    .period-summary {{
      color: #6b7280;
      font-size: 13px;
    }}
    .metric-row {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 10px;
      margin-top: 10px;
    }}
    .metric-chip {{
      border: 1px solid rgba(15, 118, 110, 0.15);
      border-radius: 12px;
      padding: 10px 12px;
      background: rgba(246, 251, 249, 0.95);
    }}
    .metric-label {{
      display: block;
      font-size: 12px;
      color: #6b7280;
      margin-bottom: 4px;
    }}
    .metric-value {{
      display: block;
      font-size: 16px;
      color: #0b5d58;
    }}
    .strategy-table th:nth-child(1),
    .strategy-table td:nth-child(1) {{ width: 7%; }}
    .strategy-table th:nth-child(2),
    .strategy-table td:nth-child(2) {{ width: 8%; }}
    .strategy-table th:nth-child(3),
    .strategy-table td:nth-child(3) {{ width: 12%; }}
    .strategy-table th:nth-child(4),
    .strategy-table td:nth-child(4) {{ width: 13%; }}
    .strategy-table th:nth-child(5),
    .strategy-table td:nth-child(5) {{ width: 23%; }}
    .strategy-table th:nth-child(6),
    .strategy-table td:nth-child(6) {{ width: 37%; }}
    .note-cell {{
      padding-top: 10px;
      padding-bottom: 10px;
    }}
    .note-stack {{
      display: grid;
      gap: 8px;
    }}
    .note-block {{
      border: 1px solid rgba(220, 207, 183, 0.95);
      border-radius: 12px;
      padding: 10px 12px;
      background: rgba(255, 255, 255, 0.76);
    }}
    .note-block-primary {{
      border-left: 4px solid var(--accent);
      background: rgba(255, 255, 255, 0.94);
    }}
    .note-block-stats {{
      background: rgba(230, 244, 239, 0.52);
    }}
    .note-block-research {{
      background: rgba(255, 248, 235, 0.92);
    }}
    .note-tag {{
      display: block;
      margin-bottom: 4px;
      font-size: 12px;
      color: #7c2d12;
      letter-spacing: 0.02em;
    }}
    .note-copy {{
      display: block;
      font-size: 13px;
      line-height: 1.58;
    }}
    .note-meta {{
      margin-top: 2px;
      padding: 8px 10px;
      font-size: 13px;
      color: #6b7280;
      background: rgba(246, 251, 249, 0.88);
      border: 1px dashed rgba(15, 118, 110, 0.18);
      border-radius: 10px;
    }}
  </style>
</head>
<body>
  <section class="panel">
    <h1>{title}</h1>
    <p>{intro}</p>
    <div class="meta">
      <div class="chip"><strong>{_html_text("生成时间")}</strong><br>{generated_at}</div>
      <div class="chip"><strong>{_html_text("JSON 路径")}</strong><br>{json_path}</div>
    </div>
  </section>
  <section class="panel">
    <h2>{_html_text("参数总览")}</h2>
    <table class="strategy-table">
      <thead>
        <tr>
          <th>{_html_text("方向")}</th>
          <th>{_html_text("币种")}</th>
          <th>{_html_text("策略")}</th>
          <th>{_html_text("核心参数")}</th>
          <th>{_html_text("保护逻辑")}</th>
          <th>{_html_text("备注")}</th>
        </tr>
      </thead>
      <tbody>{rows_html}</tbody>
    </table>
  </section>
  <section class="panel">
    <h2>{_html_text("年度 / 月度统计")}</h2>
    <p>{_html_text("按最佳参数包当前标准 100U 组合回测口径，逐币展示年度、多空、合计与月度统计，并附全组合总表。")}</p>
    {coin_period_tables}
  </section>
  <section class="panel">
    <h2>{_html_text("配置快照")}</h2>
    <p>{_html_text("下面每张配置卡都直接来自当前 StrategyConfig，适合作为 UI / 回测 / 实盘对照。")}</p>
    {configs_html}
  </section>
</body>
</html>
"""


def write_outputs() -> tuple[Path, Path, Path]:
    specs = build_specs()
    bundle = build_bundle(specs)
    write_strategy_bundle(bundle, JSON_PATH)
    html_text = build_html(specs)
    HTML_PATH.write_text(html_text, encoding="utf-8-sig")
    LEGACY_HTML_PATH.write_text(html_text, encoding="utf-8-sig")
    return JSON_PATH, HTML_PATH, LEGACY_HTML_PATH


def main() -> None:
    json_path, html_path, legacy_html_path = write_outputs()
    print(json_path)
    print(html_path)
    print(legacy_html_path)


if __name__ == "__main__":
    main()
