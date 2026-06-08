from __future__ import annotations

import html
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.models import StrategyConfig
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.strategy_catalog import (
    STRATEGY_BODY_RETEST_SHORT_ID,
    STRATEGY_DYNAMIC_LONG_ID,
    STRATEGY_EMA55_SLOPE_SHORT_ID,
)
from okx_quant.strategy_profiles import (
    STRATEGY_PROFILE_SCHEMA_VERSION,
    StrategyBundle,
    build_strategy_profile_from_config,
    write_strategy_bundle,
)


PACKAGE_DIR = analysis_report_dir_path() / "packages"
PACKAGE_DIR.mkdir(parents=True, exist_ok=True)
TARGET_PATH = PACKAGE_DIR / "最佳参数组合包.json"
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)
HTML_PATH = PROJECT_REPORT_DIR / "最佳参数组合包说明.html"


@dataclass(frozen=True)
class LongSpec:
    symbol: str
    profile_id: str
    profile_name: str
    ema_period: int
    ema_type: str
    trend_ema_period: int
    trend_ema_type: str
    entry_reference_ema_period: int
    entry_reference_ema_type: str
    atr_stop_multiplier: Decimal
    atr_take_multiplier: Decimal
    notes: str


@dataclass(frozen=True)
class ShortSpec:
    symbol: str
    profile_id: str
    profile_name: str
    strategy_id: str
    ema_period: int
    ema_type: str
    trend_ema_period: int
    trend_ema_type: str
    daily_filter_mode: str
    daily_filter_ma_type: str
    daily_filter_period: int
    notes: str


LONG_SPECS = (
    LongSpec(
        symbol="BTC-USDT-SWAP",
        profile_id="dynamic_long_best_btc",
        profile_name="BTC 动态委托做多 最佳参数",
        ema_period=21,
        ema_type="ema",
        trend_ema_period=50,
        trend_ema_type="ma",
        entry_reference_ema_period=50,
        entry_reference_ema_type="ma",
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("2"),
        notes="固定研究口径：EMA21 / MA50 / 挂单 MA50 / SL2 / 动态止盈 / 10U 风险 / 不加日线过滤。",
    ),
    LongSpec(
        symbol="ETH-USDT-SWAP",
        profile_id="dynamic_long_best_eth",
        profile_name="ETH 动态委托做多 最佳参数",
        ema_period=21,
        ema_type="ma",
        trend_ema_period=55,
        trend_ema_type="ema",
        entry_reference_ema_period=55,
        entry_reference_ema_type="ma",
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("2"),
        notes="固定研究口径：MA21 / EMA55 / 挂单 MA55 / SL2 / 动态止盈 / 10U 风险 / 不加日线过滤。",
    ),
    LongSpec(
        symbol="SOL-USDT-SWAP",
        profile_id="dynamic_long_best_sol",
        profile_name="SOL 动态委托做多 最佳参数",
        ema_period=21,
        ema_type="ma",
        trend_ema_period=55,
        trend_ema_type="ma",
        entry_reference_ema_period=55,
        entry_reference_ema_type="ma",
        atr_stop_multiplier=Decimal("1"),
        atr_take_multiplier=Decimal("1"),
        notes="固定研究口径：MA21 / MA55 / 挂单 MA55 / SL1 / 动态止盈 / 10U 风险 / 不加日线过滤。",
    ),
    LongSpec(
        symbol="BNB-USDT-SWAP",
        profile_id="dynamic_long_best_bnb",
        profile_name="BNB 动态委托做多 最佳参数",
        ema_period=21,
        ema_type="ma",
        trend_ema_period=55,
        trend_ema_type="ma",
        entry_reference_ema_period=55,
        entry_reference_ema_type="ma",
        atr_stop_multiplier=Decimal("1.5"),
        atr_take_multiplier=Decimal("6"),
        notes="固定研究口径：MA21 / MA55 / 挂单 MA55 / SL1.5 / 动态止盈 / 10U 风险 / 不加日线过滤。",
    ),
)


SHORT_SPECS = (
    ShortSpec(
        symbol="BTC-USDT-SWAP",
        profile_id="slope_short_best_btc",
        profile_name="BTC 斜率做空 最佳参数",
        strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
        ema_period=55,
        ema_type="ema",
        trend_ema_period=55,
        trend_ema_type="ema",
        daily_filter_mode="disabled",
        daily_filter_ma_type="ema",
        daily_filter_period=21,
        notes="100U 深度研究定稿：EMA55 斜率做空，不加日线过滤，ATR14 止损2，ATR分位<=0.5，动态止盈，2R 保本。",
    ),
    ShortSpec(
        symbol="ETH-USDT-SWAP",
        profile_id="slope_short_best_eth",
        profile_name="ETH 斜率做空 最佳参数",
        strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
        ema_period=34,
        ema_type="ma",
        trend_ema_period=34,
        trend_ema_type="ma",
        daily_filter_mode="disabled",
        daily_filter_ma_type="ema",
        daily_filter_period=21,
        notes="100U 深度研究定稿：MA34 斜率做空，不加日线过滤，ATR14 止损2，ATR分位<=0.5，动态止盈，2R 保本。",
    ),
    ShortSpec(
        symbol="SOL-USDT-SWAP",
        profile_id="slope_short_best_sol",
        profile_name="SOL 斜率做空 最佳参数",
        strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
        ema_period=20,
        ema_type="ma",
        trend_ema_period=20,
        trend_ema_type="ma",
        daily_filter_mode="disabled",
        daily_filter_ma_type="ema",
        daily_filter_period=21,
        notes="100U 深度研究定稿：MA20 斜率做空，不加日线过滤，ATR14 止损2，ATR分位<=0.5，动态止盈，2R 保本。",
    ),
    ShortSpec(
        symbol="BNB-USDT-SWAP",
        profile_id="body_retest_short_best_bnb",
        profile_name="BNB 回抽做空 最佳参数",
        strategy_id=STRATEGY_BODY_RETEST_SHORT_ID,
        ema_period=20,
        ema_type="ma",
        trend_ema_period=20,
        trend_ema_type="ma",
        daily_filter_mode="disabled",
        daily_filter_ma_type="ema",
        daily_filter_period=21,
        notes="100U 深度研究定稿：Body/ATR 回抽做空，不加日线过滤，ATR14 止损2，ATR分位<=0.5，动态止盈，2R 保本。",
    ),
    ShortSpec(
        symbol="DOGE-USDT-SWAP",
        profile_id="slope_short_best_doge",
        profile_name="DOGE 斜率做空 最佳参数",
        strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
        ema_period=55,
        ema_type="ma",
        trend_ema_period=55,
        trend_ema_type="ma",
        daily_filter_mode="disabled",
        daily_filter_ma_type="ma",
        daily_filter_period=20,
        notes="100U 深度研究定稿：MA55 斜率做空，不加日线过滤，ATR14 止损2，ATR分位<=0.5，动态止盈，2R 保本。",
    ),
)


def build_dynamic_long_config(spec: LongSpec) -> StrategyConfig:
    return StrategyConfig(
        inst_id=spec.symbol,
        bar="1H",
        ema_period=spec.ema_period,
        ema_type=spec.ema_type,
        trend_ema_period=spec.trend_ema_period,
        trend_ema_type=spec.trend_ema_type,
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=spec.atr_stop_multiplier,
        atr_take_multiplier=spec.atr_take_multiplier,
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment="live",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        risk_amount=Decimal("10"),
        backtest_initial_capital=Decimal("10000"),
        backtest_sizing_mode="fixed_risk",
        entry_reference_ema_period=spec.entry_reference_ema_period,
        entry_reference_ema_type=spec.entry_reference_ema_type,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=0,
    )


def build_slope_short_config(spec: ShortSpec) -> StrategyConfig:
    daily_enabled = str(spec.daily_filter_mode).strip().lower() != "disabled"
    return StrategyConfig(
        inst_id=spec.symbol,
        bar="1H",
        ema_period=spec.ema_period,
        ema_type=spec.ema_type,
        trend_ema_period=spec.trend_ema_period,
        trend_ema_type=spec.trend_ema_type,
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
        strategy_id=spec.strategy_id,
        risk_amount=Decimal("10"),
        backtest_initial_capital=Decimal("10000"),
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
        daily_filter_bar="1D" if daily_enabled else None,
        daily_filter_boundary="bjt_08",
        daily_filter_mode=spec.daily_filter_mode,
        daily_filter_scope="short_only",
        daily_filter_ma_type=spec.daily_filter_ma_type,
        daily_filter_period=spec.daily_filter_period,
    )


def build_body_retest_short_config(spec: ShortSpec) -> StrategyConfig:
    daily_enabled = str(spec.daily_filter_mode).strip().lower() != "disabled"
    return StrategyConfig(
        inst_id=spec.symbol,
        bar="1H",
        ema_period=spec.ema_period,
        ema_type=spec.ema_type,
        trend_ema_period=spec.trend_ema_period,
        trend_ema_type=spec.trend_ema_type,
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
        strategy_id=spec.strategy_id,
        risk_amount=Decimal("10"),
        backtest_initial_capital=Decimal("10000"),
        backtest_sizing_mode="fixed_risk",
        take_profit_mode="dynamic",
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        atr_percentile_filter_max=Decimal("0.5"),
        trend_ema_slope_filter_min_ratio=Decimal("-0.0005"),
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=10,
        daily_filter_enabled=daily_enabled,
        daily_filter_bar="1D" if daily_enabled else None,
        daily_filter_boundary="bjt_08",
        daily_filter_mode=spec.daily_filter_mode,
        daily_filter_scope="short_only",
        daily_filter_ma_type=spec.daily_filter_ma_type,
        daily_filter_period=spec.daily_filter_period,
        body_retest_breakdown_atr_multiplier=Decimal("0.2"),
        body_retest_retest_atr_multiplier=Decimal("0.3"),
        body_retest_stop_buffer_atr_multiplier=Decimal("0.3"),
        body_retest_body_atr_limit=Decimal("1.0"),
        body_retest_watch_bars=6,
    )


def cleanup_old_package_files(target_path: Path) -> list[Path]:
    removed: list[Path] = []
    for path in PACKAGE_DIR.iterdir():
        if path.resolve() == target_path.resolve():
            continue
        if path.is_file():
            path.unlink()
            removed.append(path)
    return removed


def _fmt_decimal(value: Decimal) -> str:
    normalized = format(value, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized


def _short_filter_label(spec: ShortSpec) -> str:
    if str(spec.daily_filter_mode).strip().lower() == "disabled":
        return "不加日线过滤"
    return f"北京时间8点日线 {spec.daily_filter_ma_type.upper()}{spec.daily_filter_period}"


def _long_row_html(spec: LongSpec) -> str:
    return (
        "<tr>"
        f"<td>做多</td>"
        f"<td>{html.escape(spec.symbol.replace('-USDT-SWAP', ''))}</td>"
        f"<td>EMA 动态委托做多</td>"
        f"<td>{html.escape(spec.ema_type.upper())}{spec.ema_period}</td>"
        f"<td>{html.escape(spec.trend_ema_type.upper())}{spec.trend_ema_period}</td>"
        f"<td>{html.escape(spec.entry_reference_ema_type.upper())}{spec.entry_reference_ema_period}</td>"
        f"<td>ATR10 / SL {_fmt_decimal(spec.atr_stop_multiplier)} / TP {_fmt_decimal(spec.atr_take_multiplier)}</td>"
        f"<td>不加日线过滤</td>"
        f"<td>{html.escape(spec.notes)}</td>"
        "</tr>"
    )


def _short_row_html(spec: ShortSpec) -> str:
    line_label = f"{spec.ema_type.upper()}{spec.ema_period}"
    strategy_label = "Body/ATR 回抽做空" if spec.strategy_id == STRATEGY_BODY_RETEST_SHORT_ID else "EMA 斜率做空"
    return (
        "<tr>"
        f"<td>做空</td>"
        f"<td>{html.escape(spec.symbol.replace('-USDT-SWAP', ''))}</td>"
        f"<td>{html.escape(strategy_label)}</td>"
        f"<td>{html.escape(line_label)}</td>"
        f"<td>{html.escape(spec.trend_ema_type.upper())}{spec.trend_ema_period}</td>"
        f"<td>-</td>"
        f"<td>ATR14 / SL 2 / TP 4</td>"
        f"<td>{html.escape(_short_filter_label(spec))}</td>"
        f"<td>{html.escape(spec.notes)}</td>"
        "</tr>"
    )


def write_bundle_html() -> Path:
    rows = [_long_row_html(spec) for spec in LONG_SPECS] + [_short_row_html(spec) for spec in SHORT_SPECS]
    html_text = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>最佳参数组合包说明</title>
  <style>
    body {{
      font-family: "Microsoft YaHei UI", "Microsoft YaHei", sans-serif;
      margin: 24px;
      color: #1f2328;
      background: #f7f4ee;
      line-height: 1.6;
    }}
    h1, h2 {{ margin: 0 0 12px; }}
    .card {{
      background: #fffdf8;
      border: 1px solid #e6dccb;
      border-radius: 14px;
      padding: 18px 20px;
      margin-bottom: 18px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: #fff;
    }}
    th, td {{
      border: 1px solid #e8dfd2;
      padding: 10px 12px;
      text-align: left;
      vertical-align: top;
    }}
    th {{
      background: #f1e7d7;
    }}
    code {{
      background: #f3eee4;
      padding: 1px 5px;
      border-radius: 6px;
    }}
  </style>
</head>
<body>
  <div class="card">
    <h1>最佳参数组合包说明</h1>
    <p>对应文件：<code>{html.escape(str(TARGET_PATH))}</code></p>
    <p>这份组合包当前包含 <strong>9 条</strong> 策略：<strong>4 条 EMA 动态委托做多</strong> + <strong>5 条做空定稿版</strong>。导入时支持部分勾选、全选、改当前 API、指定 API、逐条指定 API，并默认勾选自动启动。</p>
  </div>
  <div class="card">
    <h2>做空命名说明</h2>
    <p><strong>EMA55 斜率做空</strong> 在界面里已经统一改名为 <strong>EMA 斜率做空</strong>。但不是所有币都以斜率版本定稿：按 100U 深度研究结果分别是 <code>BTC EMA55</code>、<code>ETH MA34</code>、<code>SOL MA20</code>、<code>BNB Body/ATR 回抽</code>、<code>DOGE MA55</code>。</p>
  </div>
  <div class="card">
    <h2>条目明细</h2>
    <table>
      <thead>
        <tr>
          <th>方向</th>
          <th>币种</th>
          <th>策略</th>
          <th>快线</th>
          <th>趋势线</th>
          <th>挂单参考</th>
          <th>风控</th>
          <th>日线过滤</th>
          <th>说明</th>
        </tr>
      </thead>
      <tbody>
        {''.join(rows)}
      </tbody>
    </table>
  </div>
</body>
</html>
"""
    HTML_PATH.write_text(html_text, encoding="utf-8")
    return HTML_PATH


def build_bundle() -> StrategyBundle:
    long_profiles = tuple(
        build_strategy_profile_from_config(
            profile_id=spec.profile_id,
            profile_name=spec.profile_name,
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            symbol=spec.symbol,
            config=build_dynamic_long_config(spec),
            api_name="",
            direction_label="只做多",
            run_mode_label="交易并下单",
            tags=("最佳参数", "EMA动态委托做多", "固定研究"),
            notes=spec.notes,
            source_report="D:/qqokx/reports/ema_dynamic_long_fixed_research_log_4coins_10u.html",
        )
        for spec in LONG_SPECS
    )
    short_profiles = tuple(
        build_strategy_profile_from_config(
            profile_id=spec.profile_id,
            profile_name=spec.profile_name,
            strategy_id=spec.strategy_id,
            symbol=spec.symbol,
            config=(
                build_body_retest_short_config(spec)
                if spec.strategy_id == STRATEGY_BODY_RETEST_SHORT_ID
                else build_slope_short_config(spec)
            ),
            api_name="",
            direction_label="只做空",
            run_mode_label="交易并下单",
            tags=("最佳参数", "100U研究", "做空定稿"),
            notes=spec.notes,
            source_report="D:/qqokx/reports/multi_coin_short_slope_deep_research_100u.html",
        )
        for spec in SHORT_SPECS
    )
    return StrategyBundle(
        bundle_version=STRATEGY_PROFILE_SCHEMA_VERSION,
        bundle_name="最佳参数组合包",
        profiles=long_profiles + short_profiles,
        source_report="D:/qqokx/reports/multi_coin_short_slope_deep_research_100u.html",
        auto_start_on_import=True,
    )


def main() -> None:
    removed = cleanup_old_package_files(TARGET_PATH)
    bundle = build_bundle()
    write_strategy_bundle(bundle, TARGET_PATH)
    html_path = write_bundle_html()
    print(f"written: {TARGET_PATH}")
    print(f"html: {html_path}")
    print(f"removed: {len(removed)}")
    for path in removed:
        print(f" - {path.name}")


if __name__ == "__main__":
    main()
