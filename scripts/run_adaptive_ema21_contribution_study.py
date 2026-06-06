from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from okx_quant.backtest import BacktestTrade, _run_backtest_with_loaded_data
from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal_fixed
from okx_quant.strategy_catalog import STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID


SYMBOL = "BTC-USDT-SWAP"
BAR = "4H"
INITIAL_CAPITAL = Decimal("10000")
RISK_AMOUNT = Decimal("10")
MAKER_FEE = Decimal("0.00015")
TAKER_FEE = Decimal("0.00036")


@dataclass(frozen=True)
class Window:
    key: str
    label: str
    start_ts: int


@dataclass(frozen=True)
class PoolVariant:
    key: str
    label: str
    candidate_periods: tuple[int, ...]


@dataclass(frozen=True)
class TradeContributionRow:
    scope: str
    period_label: str
    trades: int
    win_rate: str
    total_pnl: str
    total_return_pct: str
    avg_r: str


@dataclass(frozen=True)
class WindowSummary:
    window_key: str
    window_label: str
    core_return_pct: str
    mid_return_pct: str
    return_delta_pct: str
    core_trades: int
    mid_trades: int
    core_pf: str
    mid_pf: str
    core_max_drawdown_pct: str
    mid_max_drawdown_pct: str


WINDOWS: tuple[Window, ...] = (
    Window(key="full", label="Full History", start_ts=0),
    Window(
        key="since_2024",
        label="Since 2024-01-01",
        start_ts=int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000),
    ),
    Window(
        key="since_2025",
        label="Since 2025-01-01",
        start_ts=int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1000),
    ),
)


CORE = PoolVariant(
    key="core",
    label="Adaptive Balanced Core 21/34/55/89",
    candidate_periods=(21, 34, 55, 89),
)
MID = PoolVariant(
    key="mid",
    label="Adaptive Balanced Mid 34/55/89",
    candidate_periods=(34, 55, 89),
)


def _fmt(value: Decimal | None, digits: int = 4) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(value, digits)


def _build_config(candidate_periods: tuple[int, ...]) -> StrategyConfig:
    return StrategyConfig(
        inst_id=SYMBOL,
        bar=BAR,
        ema_period=21,
        trend_ema_period=55,
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=Decimal("1.5"),
        atr_take_multiplier=Decimal("4"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID,
        risk_amount=RISK_AMOUNT,
        entry_reference_ema_period=55,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=0,
        hold_close_exit_bars=0,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        backtest_compounding=False,
        backtest_entry_slippage_rate=Decimal("0"),
        backtest_exit_slippage_rate=Decimal("0"),
        backtest_slippage_rate=Decimal("0"),
        backtest_funding_rate=Decimal("0"),
        rail_break_atr_ratio=Decimal("1.5"),
        rail_reclaim_bars=2,
        rail_switch_min_score_delta=Decimal("12"),
        rail_candidate_ema_periods=candidate_periods,
    )


def _collect_trade_row(scope: str, period_label: str, trades: list[BacktestTrade]) -> TradeContributionRow:
    wins = [trade for trade in trades if trade.pnl > 0]
    total_pnl = sum((trade.pnl for trade in trades), Decimal("0"))
    avg_r = Decimal("0") if not trades else sum((trade.r_multiple for trade in trades), Decimal("0")) / Decimal(len(trades))
    win_rate = Decimal("0") if not trades else (Decimal(len(wins)) / Decimal(len(trades))) * Decimal("100")
    return TradeContributionRow(
        scope=scope,
        period_label=period_label,
        trades=len(trades),
        win_rate=_fmt(win_rate, 2),
        total_pnl=_fmt(total_pnl, 4),
        total_return_pct=_fmt((total_pnl / INITIAL_CAPITAL) * Decimal("100"), 2),
        avg_r=_fmt(avg_r, 4),
    )


def _run_variant(instrument, all_candles: list, window: Window, variant: PoolVariant):
    candles = [candle for candle in all_candles if candle.ts >= window.start_ts]
    return _run_backtest_with_loaded_data(
        candles,
        instrument,
        _build_config(variant.candidate_periods),
        data_source_note=f"local candle_cache full history | {SYMBOL} {BAR} | candles={len(candles)}",
        maker_fee_rate=MAKER_FEE,
        taker_fee_rate=TAKER_FEE,
    )


def _build_markdown(
    summary_rows: list[WindowSummary],
    contribution_rows_by_window: dict[str, list[TradeContributionRow]],
) -> str:
    lines = [
        "# Adaptive EMA21 Contribution Study",
        "",
        f"- Symbol: `{SYMBOL}`",
        f"- Bar: `{BAR}`",
        "- Both variants use the same Balanced 4H risk and exit settings",
        "- Only difference: candidate pool includes `EMA21` or excludes it",
        "",
        "## Window Summary",
        "",
        "| Window | Core Return | Mid Return | Delta | Core Trades | Mid Trades | Core PF | Mid PF | Core Max DD | Mid Max DD |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summary_rows:
        lines.append(
            f"| {row.window_label} | {row.core_return_pct}% | {row.mid_return_pct}% | {row.return_delta_pct}% | "
            f"{row.core_trades} | {row.mid_trades} | {row.core_pf} | {row.mid_pf} | {row.core_max_drawdown_pct}% | {row.mid_max_drawdown_pct}% |"
        )

    for window_key, rows in contribution_rows_by_window.items():
        title = next((item.window_label for item in summary_rows if item.window_key == window_key), window_key)
        lines.extend(
            [
                "",
                f"## {title}",
                "",
                "| Scope | Period | Trades | Win Rate | Total PnL | Return Contribution | Avg R |",
                "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in rows:
            lines.append(
                f"| {row.scope} | {row.period_label} | {row.trades} | {row.win_rate}% | "
                f"{row.total_pnl} | {row.total_return_pct}% | {row.avg_r} |"
            )

    lines.extend(
        [
            "",
            "## Reading Guide",
            "",
            "1. `Core / EMA21 only` shows the direct contribution from trades that entered on EMA21.",
            "2. `Core / Non-EMA21` vs `Mid / All Trades` shows the indirect effect.",
            "If Core's non-EMA21 subset still looks better, EMA21 is improving regime selection and switching rhythm, not just adding extra trades.",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = analysis_report_dir_path()
    out_dir.mkdir(parents=True, exist_ok=True)
    md_path = out_dir / f"adaptive_ema21_contribution_study_{stamp}.md"
    json_path = out_dir / f"adaptive_ema21_contribution_study_{stamp}.json"

    client = OkxRestClient()
    instrument = client.get_instrument(SYMBOL)
    all_candles = [candle for candle in load_candle_cache(SYMBOL, BAR, limit=None) if candle.confirmed]

    summary_rows: list[WindowSummary] = []
    contribution_rows_by_window: dict[str, list[TradeContributionRow]] = {}
    json_rows: list[dict[str, object]] = []

    for window in WINDOWS:
        print(f"run {window.key}", flush=True)
        core_result = _run_variant(instrument, all_candles, window, CORE)
        mid_result = _run_variant(instrument, all_candles, window, MID)
        core_report = core_result.report
        mid_report = mid_result.report

        core_trades = core_result.trades
        ema21_trades = [trade for trade in core_trades if trade.adaptive_rail_period == 21]
        non_ema21_trades = [trade for trade in core_trades if trade.adaptive_rail_period != 21]
        mid_trades = mid_result.trades

        contribution_rows = [
            _collect_trade_row("Core", "EMA21 only", ema21_trades),
            _collect_trade_row("Core", "Non-EMA21", non_ema21_trades),
            _collect_trade_row("Core", "All Trades", core_trades),
            _collect_trade_row("Mid", "All Trades", mid_trades),
        ]

        periods = sorted({trade.adaptive_rail_period for trade in core_trades if trade.adaptive_rail_period is not None})
        for period in periods:
            trades = [trade for trade in core_trades if trade.adaptive_rail_period == period]
            contribution_rows.append(_collect_trade_row("Core Detail", f"EMA{period}", trades))

        contribution_rows_by_window[window.key] = contribution_rows
        summary_rows.append(
            WindowSummary(
                window_key=window.key,
                window_label=window.label,
                core_return_pct=_fmt(core_report.total_return_pct, 2),
                mid_return_pct=_fmt(mid_report.total_return_pct, 2),
                return_delta_pct=_fmt(core_report.total_return_pct - mid_report.total_return_pct, 2),
                core_trades=core_report.total_trades,
                mid_trades=mid_report.total_trades,
                core_pf=_fmt(core_report.profit_factor, 4),
                mid_pf=_fmt(mid_report.profit_factor, 4),
                core_max_drawdown_pct=_fmt(core_report.max_drawdown_pct, 2),
                mid_max_drawdown_pct=_fmt(mid_report.max_drawdown_pct, 2),
            )
        )

        json_rows.append(
            {
                "window": asdict(window),
                "core_report": {
                    "total_return_pct": _fmt(core_report.total_return_pct, 2),
                    "max_drawdown_pct": _fmt(core_report.max_drawdown_pct, 2),
                    "profit_factor": _fmt(core_report.profit_factor, 4),
                    "total_trades": core_report.total_trades,
                },
                "mid_report": {
                    "total_return_pct": _fmt(mid_report.total_return_pct, 2),
                    "max_drawdown_pct": _fmt(mid_report.max_drawdown_pct, 2),
                    "profit_factor": _fmt(mid_report.profit_factor, 4),
                    "total_trades": mid_report.total_trades,
                },
                "contribution_rows": [asdict(row) for row in contribution_rows],
            }
        )

    md_path.write_text(_build_markdown(summary_rows, contribution_rows_by_window), encoding="utf-8")
    json_path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                "symbol": SYMBOL,
                "bar": BAR,
                "rows": json_rows,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(md_path)
    print(json_path)


if __name__ == "__main__":
    main()
