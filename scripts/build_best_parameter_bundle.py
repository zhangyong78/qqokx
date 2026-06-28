from __future__ import annotations

import csv
import html
import json
import re
import sys
from dataclasses import asdict, dataclass, replace
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
LEGACY_HTML_PATH = ROOT / "reports" / HTML_NAME

STANDARD_REPORT_DIR = ROOT / "reports" / "best_parameter_bundle_1h_standard_100u"
STANDARD_TRADES_CSV = STANDARD_REPORT_DIR / "trades.csv"
SLOPE_FILTER_COMPARE_CSV = ROOT / "reports" / "dynamic_long_slope_filter_compare" / "compare.csv"
ANALYSIS_REPORT_DIR = analysis_report_dir_path()
OVERALL_TRADES_GLOB = "best_parameter_bundle_overall_*.csv"
CLEANUP_BACKUPS_DIR = ANALYSIS_REPORT_DIR.parents[1] / "cleanup_backups"

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
    "BTC-USDT-SWAP": "BTC 做多默认参数已同步到参数包、UI 与实盘。本轮补做趋势线斜率过滤开/关对比后，关闭过滤总收益略高但最大回撤同步放大，因此最佳参数包当前默认关闭，并继续观察回撤扩张是否可接受。下方统计按固定风险 100U、初始资金 10000U、非复利标准口径展示。",
    "ETH-USDT-SWAP": "ETH 做多默认参数已同步到参数包、UI 与实盘。本轮补做趋势线斜率过滤开/关对比后，开启过滤的收益与回撤都略优于关闭方案，因此最佳参数包当前保留开启。下方统计按固定风险 100U、初始资金 10000U、非复利标准口径展示。",
    "SOL-USDT-SWAP": "SOL 做多默认参数已同步到参数包、UI 与实盘。本轮补做趋势线斜率过滤开/关对比后，关闭过滤同时带来更高收益和更低回撤，因此最佳参数包当前默认关闭。下方统计按固定风险 100U、初始资金 10000U、非复利标准口径展示。",
    "DOGE-USDT-SWAP": "DOGE 做多本轮已复核 S652 / S653 / S654 / S655，并把主候选从先前误推的 S652 更正为 S653；同时补做趋势线斜率过滤开/关对比，结果显示开启过滤的收益与回撤都优于关闭方案，因此最佳参数包当前保留开启。下方统计按固定风险 100U、初始资金 10000U、非复利标准口径展示。",
}

_SHORT_NOTE_MAP: dict[str, str] = {
    "BTC-USDT-SWAP": "BTC 做空默认参数已同步到参数包、UI 与实盘；下方统计按固定风险 100U、初始资金 10000U、非复利标准口径展示。",
    "ETH-USDT-SWAP": "ETH 做空默认参数已同步到参数包、UI 与实盘；下方统计按固定风险 100U、初始资金 10000U、非复利标准口径展示。",
    "SOL-USDT-SWAP": "SOL 做空默认参数已同步到参数包、UI 与实盘；下方统计按固定风险 100U、初始资金 10000U、非复利标准口径展示。",
    "DOGE-USDT-SWAP": "DOGE 做空默认参数已同步到参数包、UI 与实盘；Bear-Reentry 复核未带来足够大的综合提升，当前定稿暂不调整。下方统计按固定风险 100U、初始资金 10000U、非复利标准口径展示。",
}


_LIVE_CAPITAL_PLAN: tuple[tuple[str, str, Decimal], ...] = (
    ("BTC", "BTC-USDT-SWAP", Decimal("100")),
    ("ETH", "ETH-USDT-SWAP", Decimal("50")),
    ("SOL", "SOL-USDT-SWAP", Decimal("50")),
    ("DOGE", "DOGE-USDT-SWAP", Decimal("20")),
)
_LIVE_TRIAL_COMBO_PLAN: tuple[tuple[str, str, Decimal], ...] = (
    ("BTC", "BTC-USDT-SWAP", Decimal("30")),
    ("ETH", "ETH-USDT-SWAP", Decimal("20")),
    ("SOL", "SOL-USDT-SWAP", Decimal("10")),
    ("DOGE", "DOGE-USDT-SWAP", Decimal("10")),
)
_LIVE_IMPORT_TRIAL_RISK_MAP: dict[tuple[str, str], Decimal] = {
    ("BTC-USDT-SWAP", "long_only"): Decimal("20"),
    ("BTC-USDT-SWAP", "short_only"): Decimal("10"),
    ("ETH-USDT-SWAP", "long_only"): Decimal("12"),
    ("ETH-USDT-SWAP", "short_only"): Decimal("8"),
    ("SOL-USDT-SWAP", "long_only"): Decimal("4"),
    ("SOL-USDT-SWAP", "short_only"): Decimal("6"),
    ("DOGE-USDT-SWAP", "long_only"): Decimal("4"),
    ("DOGE-USDT-SWAP", "short_only"): Decimal("6"),
}
_LIVE_CAPITAL_STOP_LOSS_COUNT = Decimal("20")


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
    config = StrategyConfig(**kwargs)
    import_risk = _LIVE_IMPORT_TRIAL_RISK_MAP.get((symbol, signal_mode))
    if import_risk is not None:
        config = replace(config, risk_amount=import_risk, order_size=Decimal("0"))
    return config


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
    if include_entries and config.uses_reentry_confirmation():
        parts.append(
            f"再开仓确认：第{config.resolved_reentry_confirmation_min_sequence()}次起收盘站上"
            f"{config.reentry_confirmation_line_label()}"
        )
    return " / ".join(parts)


def build_specs() -> tuple[BundleSpec, ...]:
    specs: list[BundleSpec] = []
    symbols = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "DOGE-USDT-SWAP")

    for symbol in symbols:
        config = _build_config_from_defaults(STRATEGY_DYNAMIC_LONG_ID, symbol, "long_only")
        coin = symbol.replace("-USDT-SWAP", "")
        note = _LONG_NOTE_MAP[symbol]
        if symbol == "SOL-USDT-SWAP":
            note = (
                "SOL 做多本轮围绕 S656 基线补测 5R / 6R / 7R / 8R 首档锁盈，"
                "并对每波 1 / 2 / 3 次做样本外与滚动窗口复核；"
                "最终确认 3R 保本 + 5R 锁 1R + 11R 锁 10R / 每波 2 次优于原 7R 锁 1R，"
                "同时补做趋势线斜率过滤开/关对比，关闭过滤后总收益更高且最大回撤更低，"
                "因此最佳参数包当前默认关闭该过滤。已同步到参数包、UI 与实盘。下方统计按固定风险 100U、初始资金 10000U、"
                "非复利标准口径展示。"
            )
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
                note=note,
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


def build_slope_short_config(
    *,
    symbol: str,
    ema_period: int,
    ema_type: str,
    trend_ema_period: int,
    trend_ema_type: str,
    daily_filter_period: int = 5,
    daily_filter_boundary: str = "bjt_08",
    daily_filter_scope: str = "short_only",
    daily_filter_mode: str = "close_vs_ma",
    atr_period: int = 14,
    dynamic_break_even_trigger_r: int = 2,
    dynamic_first_lock_r: int = 0,
    dynamic_trailing_step_r: int = 1,
    dynamic_protection_rules: tuple[dict[str, object], ...] | None = None,
    ema55_slope_lock_profit_trigger_r: int = 5,
    ema55_slope_exit_enabled: bool = True,
    time_stop_break_even_bars: int = 10,
    environment: str = "live",
) -> StrategyConfig:
    import_risk = _LIVE_IMPORT_TRIAL_RISK_MAP.get((symbol, "short_only"), Decimal("10"))
    return StrategyConfig(
        inst_id=symbol,
        bar="1H",
        ema_period=ema_period,
        ema_type=ema_type,
        trend_ema_period=trend_ema_period,
        trend_ema_type=trend_ema_type,
        big_ema_period=233,
        atr_period=atr_period,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("4"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="short_only",
        position_mode="net",
        environment=environment,
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
        risk_amount=import_risk,
        trade_inst_id=symbol,
        tp_sl_mode="exchange",
        entry_side_mode="follow_signal",
        run_mode="trade",
        backtest_initial_capital=BUNDLE_INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        take_profit_mode="dynamic",
        dynamic_two_r_break_even=True,
        dynamic_break_even_trigger_r=dynamic_break_even_trigger_r,
        dynamic_fee_offset_enabled=True,
        dynamic_protection_rules=tuple(dynamic_protection_rules or ()),
        ema55_slope_exit_enabled=ema55_slope_exit_enabled,
        ema55_slope_lock_profit_trigger_r=ema55_slope_lock_profit_trigger_r,
        dynamic_first_lock_r=dynamic_first_lock_r,
        dynamic_trailing_step_r=dynamic_trailing_step_r,
        ema55_slope_same_bar_reentry_block=True,
        ema55_slope_dynamic_exit_requires_bear_reentry=False,
        ema55_slope_dynamic_exit_bear_reentry_break_prev_low=False,
        atr_percentile_filter_max=Decimal("0.5"),
        trend_ema_slope_filter_min_ratio=Decimal("-0.0005"),
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=time_stop_break_even_bars,
        daily_filter_enabled=daily_filter_mode != "disabled",
        daily_filter_bar="1D",
        daily_filter_boundary=daily_filter_boundary,
        daily_filter_mode=daily_filter_mode,
        daily_filter_scope=daily_filter_scope,
        daily_filter_ma_type="ema",
        daily_filter_period=daily_filter_period,
    )


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


def _note_primary_text(note: str) -> str:
    cleaned = _strip_backtest_range_clause(note)
    parts = [part.strip(" 。；;") for part in re.split(r"[；。]", cleaned) if part.strip(" 。；;")]
    filtered = [part for part in parts if not part.startswith("下方统计按固定风险")]
    if not filtered:
        return cleaned
    primary = "；".join(filtered)
    if primary and primary[-1] not in "。！？":
        primary += "。"
    return primary


def _fmt_note_report_line(report: BacktestReport) -> str:
    pf_text = "-" if report.profit_factor is None else _fmt_fixed(report.profit_factor)
    win_rate_text = _fmt_fixed(report.win_rate, "0.01") + "%"
    return (
        f"PnL {_fmt_fixed(report.total_pnl)} / DD {_fmt_fixed(report.max_drawdown)} / "
        f"WinRate {win_rate_text} / PF {pf_text} / Trades {report.total_trades}"
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
            f'<span class="note-copy">{_html_text(_note_primary_text(spec.note))}</span></div>'
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
    technical_summary = f"当前默认采用 {spec.core_label}；保护口径为 {spec.protection_label}。"
    research_copy = takeaway or technical_summary
    if takeaway and takeaway != technical_summary:
        research_copy = technical_summary
    lines.append(
        f'<div class="note-block note-block-research"><strong class="note-tag">{_html_text("研究备注")}</strong>'
        f'<span class="note-copy">{_html_text(research_copy)}</span></div>'
    )
    lines.append(f'<div class="note-meta">{_html_text(_bundle_run_backtest_range_text(run))}</div>')
    lines.append("</div>")
    return "".join(lines)


def _live_capital_plan_html() -> str:
    def _plan_rows(plan: tuple[tuple[str, str, Decimal], ...]) -> tuple[str, Decimal, Decimal]:
        rows: list[str] = []
        total_risk = Decimal("0")
        total_stop_line = Decimal("0")
        for coin, _symbol, risk_amount in plan:
            stop_line = risk_amount * _LIVE_CAPITAL_STOP_LOSS_COUNT
            total_risk += risk_amount
            total_stop_line += stop_line
            rows.append(
                "<tr>"
                f"<td>{_html_text(coin)}</td>"
                f"<td>{_html_text(_fmt_decimal(risk_amount) + 'U')}</td>"
                f"<td>{_html_text(_fmt_decimal(stop_line) + 'U')}</td>"
                f"<td>{_html_text('单币种多空组合；累计亏满 20 次固定风险即暂停')}</td>"
                "</tr>"
            )
        return "".join(rows), total_risk, total_stop_line

    standard_rows_html, standard_total_risk, standard_total_stop_line = _plan_rows(_LIVE_CAPITAL_PLAN)
    trial_rows_html, trial_total_risk, trial_total_stop_line = _plan_rows(_LIVE_TRIAL_COMBO_PLAN)
    import_rows: list[str] = []
    for coin, symbol, _combo_risk in _LIVE_TRIAL_COMBO_PLAN:
        long_risk = _LIVE_IMPORT_TRIAL_RISK_MAP[(symbol, "long_only")]
        short_risk = _LIVE_IMPORT_TRIAL_RISK_MAP[(symbol, "short_only")]
        import_rows.append(
            "<tr>"
            f"<td>{_html_text(coin)}</td>"
            f"<td>{_html_text(_fmt_decimal(long_risk) + 'U')}</td>"
            f"<td>{_html_text(_fmt_decimal(short_risk) + 'U')}</td>"
            f"<td>{_html_text(_fmt_decimal(long_risk + short_risk) + 'U')}</td>"
            "</tr>"
        )
    return (
        '<div class="live-plan">'
        f'<h2>{_html_text("实盘资金建议")}</h2>'
        f'<p>{_html_text("下面资金分配按单一币种的多空组合口径整理。选择依据不是追求收益最大，而是优先兼顾历史稳定性、流动性、连续亏损承受度和你当前先缩规模验证实盘执行的阶段目标。")}</p>'
        '<ul class="live-plan-list">'
        f'<li>{_html_text("BTC 历史稳定性和流动性最好，所以放在第一档。")}</li>'
        f'<li>{_html_text("ETH 整体稳定性次于 BTC，但通常好于 SOL / DOGE，所以放在中档。")}</li>'
        f'<li>{_html_text("SOL、DOGE 历史上更容易出现低胜率和更长的回撤段，初期不建议和 BTC 同额度。")}</li>'
        f'<li>{_html_text("试运行档的目标是先验证滑点、止损执行、参数一致性和你的心理承受度，而不是一上来把收益做大。")}</li>'
        "</ul>"
        f'<h3>{_html_text("实盘试运行档")}</h3>'
        f'<p>{_html_text("建议先用这档开始，按组合累计亏满 20 次固定风险即停止；四个币种总测试资金更轻，更适合刚开始联机验证。")}</p>'
        '<table class="live-plan-table">'
        "<thead><tr>"
        f"<th>{_html_text('币种')}</th>"
        f"<th>{_html_text('组合单次风险')}</th>"
        f"<th>{_html_text('20 次止损停线')}</th>"
        f"<th>{_html_text('备注')}</th>"
        "</tr></thead>"
        f"<tbody>{trial_rows_html}</tbody>"
        "</table>"
        f'<p class="live-plan-summary">{_html_text(f"试运行档合计：单次总风险 {_fmt_decimal(trial_total_risk)}U；四个币种总启动资金 {_fmt_decimal(trial_total_stop_line)}U。")}</p>'
        f'<h3>{_html_text("参数包导入默认风险金")}</h3>'
        f'<p>{_html_text("由于最佳参数包是按 8 个独立策略导入，不是按 4 个组合导入，所以默认风险金已按试运行档拆分预填；强的一侧多给，弱的一侧少给，导入后同币种多空合计正好对应试运行档。")}</p>'
        '<table class="live-plan-table">'
        "<thead><tr>"
        f"<th>{_html_text('币种')}</th>"
        f"<th>{_html_text('做多默认风险金')}</th>"
        f"<th>{_html_text('做空默认风险金')}</th>"
        f"<th>{_html_text('同币种合计')}</th>"
        "</tr></thead>"
        f"<tbody>{''.join(import_rows)}</tbody>"
        "</table>"
        f'<h3>{_html_text("实盘标准档")}</h3>'
        f'<p>{_html_text("当试运行阶段确认执行、滑点和心理承受都稳定后，再放大到标准档。")}</p>'
        '<table class="live-plan-table">'
        "<thead><tr>"
        f"<th>{_html_text('币种')}</th>"
        f"<th>{_html_text('组合单次风险')}</th>"
        f"<th>{_html_text('20 次止损停线')}</th>"
        f"<th>{_html_text('备注')}</th>"
        "</tr></thead>"
        f"<tbody>{standard_rows_html}</tbody>"
        "</table>"
        f'<p class="live-plan-summary">{_html_text(f"标准档合计：单次总风险 {_fmt_decimal(standard_total_risk)}U；四个币种总启动资金 {_fmt_decimal(standard_total_stop_line)}U。")}</p>'
        "</div>"
    )


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


def _direction_period_equity_rows(
    long_trades: tuple[BacktestTrade, ...],
    short_trades: tuple[BacktestTrade, ...],
    *,
    by: str,
) -> tuple[tuple[str, Decimal, Decimal], ...]:
    if by not in {"year", "month"}:
        raise ValueError("by must be 'year' or 'month'")

    bucket_keys: list[str] = []
    bucket_map: dict[str, dict[str, Decimal]] = {}
    for trade in long_trades:
        key = _period_key(int(trade.exit_ts), by)
        if key not in bucket_map:
            bucket_keys.append(key)
            bucket_map[key] = {"long": Decimal("0"), "short": Decimal("0")}
        bucket_map[key]["long"] += trade.pnl
    for trade in short_trades:
        key = _period_key(int(trade.exit_ts), by)
        if key not in bucket_map:
            bucket_keys.append(key)
            bucket_map[key] = {"long": Decimal("0"), "short": Decimal("0")}
        bucket_map[key]["short"] += trade.pnl

    bucket_keys.sort()
    long_equity = BUNDLE_INITIAL_CAPITAL
    short_equity = BUNDLE_INITIAL_CAPITAL
    rows: list[tuple[str, Decimal, Decimal]] = []
    for key in bucket_keys:
        payload = bucket_map[key]
        long_equity += payload["long"]
        short_equity += payload["short"]
        rows.append((key, long_equity, short_equity))
    return tuple(rows)


def _monthly_equity_chart_html(
    title: str,
    monthly_rows: tuple[tuple[object, ...], ...],
    *,
    direction_rows: tuple[tuple[str, Decimal, Decimal], ...] = (),
) -> str:
    if not monthly_rows and not direction_rows:
        return (
            '<div class="chart-empty">'
            f"{_html_text(f'{title}：暂无月度数据')}"
            "</div>"
        )

    if direction_rows:
        labels = [row[0] for row in direction_rows]
        long_equities = [row[1] for row in direction_rows]
        short_equities = [row[2] for row in direction_rows]
        total_equities = [_parse_decimal(str(row[11])) for row in monthly_rows] if monthly_rows else [
            long_equities[index] + short_equities[index] - BUNDLE_INITIAL_CAPITAL for index in range(len(labels))
        ]
    else:
        labels = [str(row[0]) for row in monthly_rows]
        long_equities = [_parse_decimal(str(row[11])) for row in monthly_rows]
        short_equities = list(long_equities)
        total_equities = list(long_equities)

    all_equities = [*long_equities, *short_equities, *total_equities]
    min_equity = min(all_equities)
    max_equity = max(all_equities)
    if min_equity == max_equity:
        min_equity -= Decimal("1")
        max_equity += Decimal("1")

    width = 820
    height = 280
    pad_left = 56
    pad_right = 20
    pad_top = 18
    pad_bottom = 46
    plot_width = width - pad_left - pad_right
    plot_height = height - pad_top - pad_bottom
    denominator = max_equity - min_equity

    def _x(index: int) -> float:
        if len(labels) <= 1:
            return float(pad_left + plot_width / 2)
        return float(pad_left + (plot_width * index / (len(labels) - 1)))

    def _y(value: Decimal) -> float:
        normalized = float((value - min_equity) / denominator)
        return float(pad_top + plot_height - (normalized * plot_height))

    long_points = " ".join(f"{_x(index):.1f},{_y(value):.1f}" for index, value in enumerate(long_equities))
    short_points = " ".join(f"{_x(index):.1f},{_y(value):.1f}" for index, value in enumerate(short_equities))
    total_points = " ".join(f"{_x(index):.1f},{_y(value):.1f}" for index, value in enumerate(total_equities))

    ticks: list[str] = []
    for step in range(5):
        y = pad_top + plot_height * step / 4
        value = max_equity - (denominator * Decimal(step) / Decimal("4"))
        ticks.append(
            f'<line x1="{pad_left}" y1="{y:.1f}" x2="{pad_left + plot_width}" y2="{y:.1f}" class="chart-grid" />'
            f'<text x="{pad_left - 8}" y="{y + 4:.1f}" text-anchor="end" class="chart-axis-label">{_html_text(_fmt_fixed(value))}</text>'
        )

    x_labels: list[str] = []
    max_labels = 8
    label_step = max(1, len(labels) // max_labels)
    for index, label in enumerate(labels):
        if index % label_step != 0 and index != len(labels) - 1:
            continue
        x = _x(index)
        x_labels.append(
            f'<text x="{x:.1f}" y="{height - 14}" text-anchor="middle" class="chart-axis-label">{_html_text(label)}</text>'
        )

    long_dots = "".join(
        f'<circle cx="{_x(index):.1f}" cy="{_y(value):.1f}" r="3.2" class="chart-dot chart-dot-long">'
        f'<title>{_html_text(f"{labels[index]} 做多期末权益 {_fmt_fixed(value)}")}</title>'
        "</circle>"
        for index, value in enumerate(long_equities)
    )
    short_dots = "".join(
        f'<circle cx="{_x(index):.1f}" cy="{_y(value):.1f}" r="3.2" class="chart-dot chart-dot-short">'
        f'<title>{_html_text(f"{labels[index]} 做空期末权益 {_fmt_fixed(value)}")}</title>'
        "</circle>"
        for index, value in enumerate(short_equities)
    )
    total_dots = "".join(
        f'<circle cx="{_x(index):.1f}" cy="{_y(value):.1f}" r="3.2" class="chart-dot chart-dot-total">'
        f'<title>{_html_text(f"{labels[index]} 合计期末权益 {_fmt_fixed(value)}")}</title>'
        "</circle>"
        for index, value in enumerate(total_equities)
    )

    long_latest_value = _fmt_fixed(long_equities[-1])
    short_latest_value = _fmt_fixed(short_equities[-1])
    total_latest_value = _fmt_fixed(total_equities[-1])
    return (
        '<div class="chart-card">'
        f'<div class="chart-title">{_html_text(title)}</div>'
        f'<div class="chart-subtitle">{_html_text(f"月度累计权益三线：做多 {long_latest_value} / 做空 {short_latest_value} / 合计 {total_latest_value}")}</div>'
        '<div class="chart-legend">'
        f'<span class="chart-legend-item"><span class="chart-legend-swatch chart-legend-swatch-long"></span>{_html_text("做多")}</span>'
        f'<span class="chart-legend-item"><span class="chart-legend-swatch chart-legend-swatch-short"></span>{_html_text("做空")}</span>'
        f'<span class="chart-legend-item"><span class="chart-legend-swatch chart-legend-swatch-total"></span>{_html_text("合计")}</span>'
        "</div>"
        f'<svg class="equity-chart" viewBox="0 0 {width} {height}" role="img" aria-label="{_html_text(title)}">'
        + "".join(ticks)
        + f'<polyline points="{total_points}" fill="none" stroke="#1d4ed8" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" />'
        + f'<polyline points="{long_points}" fill="none" stroke="#0f766e" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" />'
        + f'<polyline points="{short_points}" fill="none" stroke="#c2410c" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" />'
        + total_dots
        + long_dots
        + short_dots
        + "".join(x_labels)
        + "</svg>"
        "</div>"
    )


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


def _parse_overall_report_ts(text: str) -> int:
    normalized = text.strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(normalized, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    raise ValueError(f"unsupported overall report timestamp: {text}")


def _placeholder_instrument(symbol: str) -> Instrument:
    return Instrument(
        inst_id=symbol,
        inst_type="SWAP",
        tick_size=Decimal("0.0001"),
        lot_size=Decimal("1"),
        min_size=Decimal("1"),
        state="live",
    )


def _report_row_value(row: dict[str, str], *keys: str, default: str = "") -> str:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return str(value)
    return default


def _trade_from_report_row(row: dict[str, str]) -> BacktestTrade:
    direction = _report_row_value(row, "方向")
    signal = "long" if direction == "多头" else "short"
    pnl = _parse_decimal(_report_row_value(row, "盈亏", default="0"))
    r_multiple = _parse_decimal(_report_row_value(row, "R倍数", default="0"))
    return BacktestTrade(
        signal=signal,
        entry_index=0,
        exit_index=0,
        entry_ts=_parse_report_ts(_report_row_value(row, "开仓时间")),
        exit_ts=_parse_report_ts(_report_row_value(row, "平仓时间")),
        entry_price=_parse_decimal(_report_row_value(row, "开仓价", default="0")),
        exit_price=_parse_decimal(_report_row_value(row, "平仓价", default="0")),
        stop_loss=Decimal("0"),
        take_profit=Decimal("0"),
        size=_parse_decimal(_report_row_value(row, "成交数量", default="0")),
        gross_pnl=pnl,
        pnl=pnl,
        risk_value=_parse_decimal(_report_row_value(row, "风险金额", default="0")),
        r_multiple=r_multiple,
        exit_reason=_report_row_value(row, "平仓原因"),
    )


def _load_bundle_runs_standard_legacy(specs: tuple[BundleSpec, ...]) -> tuple[BundleRun, ...]:
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


def _load_bundle_runs_from_standard_report(specs: tuple[BundleSpec, ...]) -> tuple[BundleRun, ...]:
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


def _trade_from_overall_report_row(row: dict[str, str]) -> BacktestTrade:
    signal = str(row.get("signal", "")).strip().lower()
    if signal not in {"long", "short"}:
        side = str(row.get("side", "")).strip()
        signal = "long" if side == "做多" else "short"
    pnl = _parse_decimal(row.get("pnl_u", "0"))
    r_multiple = _parse_decimal(row.get("r_multiple", "0"))
    return BacktestTrade(
        signal=signal,
        entry_index=0,
        exit_index=0,
        entry_ts=_parse_overall_report_ts(str(row.get("entry_time", ""))),
        exit_ts=_parse_overall_report_ts(str(row.get("exit_time", ""))),
        entry_price=_parse_decimal(row.get("entry_price", "0")),
        exit_price=_parse_decimal(row.get("exit_price", "0")),
        stop_loss=Decimal("0"),
        take_profit=Decimal("0"),
        size=Decimal("0"),
        gross_pnl=pnl,
        pnl=pnl,
        risk_value=Decimal("100"),
        r_multiple=r_multiple,
        exit_reason=str(row.get("exit_reason", "") or ""),
    )


def _latest_overall_trades_csv() -> Path | None:
    candidates = list(ANALYSIS_REPORT_DIR.glob(OVERALL_TRADES_GLOB))
    if CLEANUP_BACKUPS_DIR.exists():
        for backup_dir in CLEANUP_BACKUPS_DIR.glob("cleanup_*"):
            analysis_root_files = backup_dir / "analysis_root_files"
            if analysis_root_files.exists():
                candidates.extend(analysis_root_files.glob(OVERALL_TRADES_GLOB))
    candidates = sorted(
        candidates,
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _load_bundle_runs(specs: tuple[BundleSpec, ...]) -> tuple[BundleRun, ...]:
    standard_runs = _load_bundle_runs_from_standard_report(specs)
    if standard_runs:
        return standard_runs

    overall_csv = _latest_overall_trades_csv()
    if overall_csv is not None:
        grouped: dict[tuple[str, str], list[BacktestTrade]] = {}
        with overall_csv.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                symbol = str(row.get("symbol", "")).strip().upper()
                side = str(row.get("side", "")).strip()
                if not symbol or side not in {"做多", "做空"}:
                    continue
                grouped.setdefault((symbol, side), []).append(_trade_from_overall_report_row(row))

        runs: list[BundleRun] = []
        for spec in specs:
            trades = sorted(
                grouped.get((spec.symbol, spec.side), []),
                key=lambda item: (item.exit_ts, item.entry_ts, item.signal),
            )
            report = _build_report(trades, initial_capital=BUNDLE_INITIAL_CAPITAL)
            result = BacktestResult(
                candles=[],
                trades=trades,
                report=report,
                instrument=_placeholder_instrument(spec.symbol),
            )
            runs.append(BundleRun(spec=spec, result=result, data_source_note=str(overall_csv)))
        return tuple(runs)

    return _load_bundle_runs_from_standard_report(specs)


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
            f'<p>{_html_text(f"未找到可追溯的 overall CSV 或 standard trades CSV：{OVERALL_TRADES_GLOB} / {STANDARD_TRADES_CSV}。请先恢复或重建回测明细，再重建本说明以带出年度/月度统计。")}</p>'
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
        direction_monthly_rows = _direction_period_equity_rows(long_trades, short_trades, by="month")
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
            + _monthly_equity_chart_html(
                f"{coin} 月度多空权益折线图",
                monthly_rows,
                direction_rows=direction_monthly_rows,
            )
            + f'<details open><summary>{_html_text("年度统计")}</summary><div class="table-scroll">{_simple_table_html(headers, yearly_rows or (_empty_period_row("无数据"),))}</div></details>'
            + f'<details><summary>{_html_text("月度统计")}</summary><div class="table-scroll">{_simple_table_html(headers, monthly_rows or (_empty_period_row("无数据"),))}</div></details>'
            + "</div>"
        )

    long_all = tuple(trade for run in runs if run.spec.side == "做多" for trade in run.result.trades)
    short_all = tuple(trade for run in runs if run.spec.side == "做空" for trade in run.result.trades)
    overall_yearly_rows = _combined_period_rows(long_all, short_all, by="year")
    overall_monthly_rows = _combined_period_rows(long_all, short_all, by="month")
    overall_direction_monthly_rows = _direction_period_equity_rows(long_all, short_all, by="month")
    overall_report = _combined_period_report(long_all, short_all)
    source_notes = sorted({run.data_source_note for run in runs if run.data_source_note})
    source_note_text = (
        "K\u7ebf\u6765\u6e90\uff1a" + "\uff1b".join(source_notes)
        if source_notes
        else f"\u672a\u627e\u5230\u53ef\u8ffd\u6eaf\u7684 overall CSV \u6216 standard trades CSV\uff1a{OVERALL_TRADES_GLOB} / {STANDARD_TRADES_CSV}"
    )
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
        + _monthly_equity_chart_html(
            "全组合月度多空权益折线图",
            overall_monthly_rows,
            direction_rows=overall_direction_monthly_rows,
        )
        + f'<p>{_html_text("口径：固定风险 100U、初始资金 10000U、非复利；多/空列展示各方向交易数与盈亏，合计列按该币或全组合的合并资金曲线统计收益率、回撤与期末权益。")}</p>'
        + f'<details open><summary>{_html_text("年度统计")}</summary><div class="table-scroll">{_simple_table_html(headers, overall_yearly_rows or (_empty_period_row("无数据"),))}</div></details>'
        + f'<details><summary>{_html_text("月度统计")}</summary><div class="table-scroll">{_simple_table_html(headers, overall_monthly_rows or (_empty_period_row("无数据"),))}</div></details>'
        + f'<p><small>{_html_text(source_note_text)}</small></p>'
        + "</div>"
    )
    return "".join(cards)


def _slope_filter_compare_section_html(specs: tuple[BundleSpec, ...]) -> str:
    if not SLOPE_FILTER_COMPARE_CSV.exists():
        return ""

    grouped: dict[str, dict[bool, dict[str, str]]] = {}
    with SLOPE_FILTER_COMPARE_CSV.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            symbol = str(row.get("symbol", "")).strip().upper()
            enabled_text = str(row.get("slope_filter_enabled", "")).strip().lower()
            if not symbol or enabled_text not in {"true", "false"}:
                continue
            grouped.setdefault(symbol, {})[enabled_text == "true"] = row

    default_map = {
        spec.symbol: bool(spec.config.trend_ema_slope_filter_enabled)
        for spec in specs
        if spec.strategy_id == STRATEGY_DYNAMIC_LONG_ID
    }
    cards: list[str] = []
    for symbol in ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "DOGE-USDT-SWAP"):
        pair = grouped.get(symbol)
        if not pair or True not in pair or False not in pair:
            continue
        enabled_row = pair[True]
        disabled_row = pair[False]
        coin = symbol.replace("-USDT-SWAP", "")
        default_enabled = default_map.get(symbol, True)
        stance_text = "当前结论：保留开启" if default_enabled else "当前结论：默认关闭"
        pnl_delta = Decimal(str(disabled_row.get("total_pnl", "0"))) - Decimal(str(enabled_row.get("total_pnl", "0")))
        dd_delta = Decimal(str(disabled_row.get("max_drawdown_pct", "0"))) - Decimal(
            str(enabled_row.get("max_drawdown_pct", "0"))
        )
        range_text = (
            f"回测区间：{enabled_row.get('start_local', '')} -> {enabled_row.get('end_local', '')} | "
            f"样本 {enabled_row.get('candles', '0')} 根 1H K 线"
        )
        enabled_text = (
            f"PnL {_fmt_fixed(enabled_row.get('total_pnl', '0'))} / "
            f"DD {_fmt_fixed(enabled_row.get('max_drawdown_pct', '0'), '0.01')}% / "
            f"Trades {enabled_row.get('trades', '0')}"
        )
        disabled_text = (
            f"PnL {_fmt_fixed(disabled_row.get('total_pnl', '0'))} / "
            f"DD {_fmt_fixed(disabled_row.get('max_drawdown_pct', '0'), '0.01')}% / "
            f"Trades {disabled_row.get('trades', '0')}"
        )
        delta_text = f"PnL {_fmt_fixed(pnl_delta)} / DD {_fmt_fixed(dd_delta, '0.01')}%"
        cards.append(
            '<div class="compare-card">'
            f'<div class="compare-card-head"><h3>{_html_text(coin + " 斜率过滤开 / 关结论")}</h3>'
            f'<span class="compare-badge">{_html_text(stance_text)}</span></div>'
            f'<p class="compare-range">{_html_text(range_text)}</p>'
            '<div class="compare-metrics">'
            f'<div class="compare-metric"><span class="metric-label">{_html_text("开启")}</span><strong class="metric-value">{_html_text(enabled_text)}</strong></div>'
            f'<div class="compare-metric"><span class="metric-label">{_html_text("关闭")}</span><strong class="metric-value">{_html_text(disabled_text)}</strong></div>'
            f'<div class="compare-metric"><span class="metric-label">{_html_text("关闭 - 开启")}</span><strong class="metric-value">{_html_text(delta_text)}</strong></div>'
            "</div>"
            "</div>"
        )

    if not cards:
        return ""

    return (
        '<section class="panel">'
        f'<h2>{_html_text("斜率过滤开 / 关结论")}</h2>'
        f'<p>{_html_text("下面单独汇总 BTC / ETH / SOL / DOGE 动态做多策略的趋势线斜率过滤开关对比，口径为最佳参数时间段、1H 全缓存、固定风险 100U、初始资金 10000U、非复利。")}</p>'
        f'<div class="compare-grid">{"".join(cards)}</div>'
        "</section>"
    )


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
    live_capital_plan_html = _live_capital_plan_html()
    slope_filter_compare_html = _slope_filter_compare_section_html(specs)

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
    .compare-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 14px;
    }}
    .compare-card {{
      border: 1px solid rgba(15, 118, 110, 0.14);
      border-radius: 16px;
      background: rgba(246, 251, 249, 0.96);
      padding: 16px;
    }}
    .compare-card-head {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-bottom: 8px;
    }}
    .compare-card-head h3 {{
      margin: 0;
      font-size: 18px;
    }}
    .compare-badge {{
      display: inline-flex;
      align-items: center;
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(15, 118, 110, 0.1);
      color: #0b5d58;
      font-weight: 700;
      font-size: 12px;
      white-space: nowrap;
    }}
    .compare-range {{
      color: #5b6470;
      font-size: 13px;
      margin-bottom: 10px;
    }}
    .compare-metrics {{
      display: grid;
      gap: 8px;
    }}
    .compare-metric {{
      border: 1px solid rgba(15, 118, 110, 0.12);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.84);
      padding: 10px 12px;
    }}
    .chart-card {{
      margin: 14px 0 10px 0;
      padding: 14px 14px 10px 14px;
      border: 1px solid rgba(15, 118, 110, 0.12);
      border-radius: 14px;
      background: linear-gradient(180deg, rgba(246, 251, 249, 0.96), rgba(255, 255, 255, 0.86));
    }}
    .chart-title {{
      font-size: 15px;
      font-weight: 700;
      color: #0b5d58;
    }}
    .chart-subtitle {{
      margin-top: 4px;
      font-size: 12px;
      color: #6b7280;
    }}
    .chart-legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 14px;
      margin-top: 8px;
      font-size: 12px;
      color: #475569;
    }}
    .chart-legend-item {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }}
    .chart-legend-swatch {{
      width: 20px;
      height: 3px;
      border-radius: 999px;
      display: inline-block;
    }}
    .chart-legend-swatch-long {{
      background: #0f766e;
    }}
    .chart-legend-swatch-short {{
      background: #c2410c;
    }}
    .chart-legend-swatch-total {{
      background: #1d4ed8;
    }}
    .equity-chart {{
      width: 100%;
      height: auto;
      display: block;
      margin-top: 10px;
    }}
    .chart-grid {{
      stroke: rgba(15, 118, 110, 0.12);
      stroke-width: 1;
    }}
    .chart-axis-label {{
      fill: #6b7280;
      font-size: 11px;
    }}
    .chart-dot {{
      stroke: rgba(255, 255, 255, 0.92);
      stroke-width: 1.5;
    }}
    .chart-dot-long {{
      fill: #0f766e;
    }}
    .chart-dot-short {{
      fill: #c2410c;
    }}
    .chart-dot-total {{
      fill: #1d4ed8;
    }}
    .chart-empty {{
      margin: 14px 0 10px 0;
      padding: 12px 14px;
      border: 1px dashed rgba(15, 118, 110, 0.18);
      border-radius: 12px;
      color: #6b7280;
      background: rgba(246, 251, 249, 0.75);
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
    .live-plan {{
      margin-top: 16px;
      border: 1px solid rgba(15, 118, 110, 0.14);
      border-radius: 16px;
      padding: 16px 18px;
      background: linear-gradient(180deg, rgba(246, 251, 249, 0.95), rgba(255, 255, 255, 0.86));
    }}
    .live-plan-table {{
      margin-top: 10px;
    }}
    .live-plan-summary {{
      margin-top: 12px;
      color: #0b5d58;
      font-weight: 700;
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
    {live_capital_plan_html}
  </section>
  {slope_filter_compare_html}
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


def write_outputs() -> tuple[Path, Path]:
    specs = build_specs()
    bundle = build_bundle(specs)
    write_strategy_bundle(bundle, JSON_PATH)
    html_text = build_html(specs)
    HTML_PATH.write_text(html_text, encoding="utf-8-sig")
    if LEGACY_HTML_PATH.exists():
        LEGACY_HTML_PATH.unlink()
    return JSON_PATH, HTML_PATH


def main() -> None:
    json_path, html_path = write_outputs()
    print(json_path)
    print(html_path)


if __name__ == "__main__":
    main()
