from __future__ import annotations

import html
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
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


REPORT_DIR = analysis_report_dir_path()
PACKAGE_DIR = REPORT_DIR / "packages"
PACKAGE_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
SOURCE_REPORT = "leadership_daily_boundary_compare_report_20260606_103152.html"


@dataclass(frozen=True)
class ReadyStrategySpec:
    profile_id: str
    profile_name: str
    strategy_id: str
    symbol: str
    direction_label: str
    run_mode_label: str
    family: str
    hour_summary: str
    daily_summary: str
    notes: str
    config: StrategyConfig


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def build_dynamic_long_config(
    *,
    symbol: str,
    ema_period: int,
    trend_period: int,
    atr_stop: str,
    entry_reference_period: int,
    daily_ma_type: str,
    daily_period: int,
    environment: str,
) -> StrategyConfig:
    return StrategyConfig(
        inst_id=symbol,
        bar="1H",
        ema_period=ema_period,
        ema_type="ema",
        trend_ema_period=trend_period,
        trend_ema_type="ema",
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=Decimal(atr_stop),
        atr_take_multiplier=Decimal(atr_stop) * Decimal("2"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment=environment,
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        risk_amount=Decimal("10"),
        backtest_initial_capital=Decimal("10000"),
        backtest_sizing_mode="fixed_risk",
        entry_reference_ema_period=entry_reference_period,
        entry_reference_ema_type="ema",
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        daily_filter_enabled=True,
        daily_filter_bar="1D",
        daily_filter_boundary="bjt_08",
        daily_filter_mode="close_vs_ma",
        daily_filter_scope="long_only",
        daily_filter_ma_type=daily_ma_type,
        daily_filter_period=daily_period,
    )


def build_slope_short_config(
    *,
    symbol: str,
    ma_type: str,
    period: int,
    daily_mode: str,
    daily_ma_type: str,
    daily_period: int,
    environment: str,
) -> StrategyConfig:
    return StrategyConfig(
        inst_id=symbol,
        bar="1H",
        ema_period=period,
        ema_type=ma_type,
        trend_ema_period=period,
        trend_ema_type=ma_type,
        big_ema_period=233,
        atr_period=14,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("4"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="short_only",
        position_mode="net",
        environment=environment,
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
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
        daily_filter_enabled=True,
        daily_filter_bar="1D",
        daily_filter_boundary="bjt_08",
        daily_filter_mode=daily_mode,
        daily_filter_scope="short_only",
        daily_filter_ma_type=daily_ma_type,
        daily_filter_period=daily_period,
    )


def build_body_retest_short_config(*, symbol: str, environment: str) -> StrategyConfig:
    return StrategyConfig(
        inst_id=symbol,
        bar="1H",
        ema_period=20,
        ema_type="ma",
        trend_ema_period=20,
        trend_ema_type="ma",
        big_ema_period=233,
        atr_period=14,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("4"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="short_only",
        position_mode="net",
        environment=environment,
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_BODY_RETEST_SHORT_ID,
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
        daily_filter_enabled=True,
        daily_filter_bar="1D",
        daily_filter_boundary="bjt_08",
        daily_filter_mode="weak_day",
        daily_filter_scope="short_only",
        body_retest_breakdown_atr_multiplier=Decimal("0.2"),
        body_retest_retest_atr_multiplier=Decimal("0.3"),
        body_retest_stop_buffer_atr_multiplier=Decimal("0.3"),
        body_retest_body_atr_limit=Decimal("1.0"),
        body_retest_watch_bars=6,
    )


def build_ready_specs(environment: str) -> tuple[ReadyStrategySpec, ...]:
    return (
        ReadyStrategySpec(
            profile_id="btc-long-ema5-bjt08",
            profile_name="BTC 做多 EMA5 日线闸门",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            symbol="BTC-USDT-SWAP",
            direction_label="long_only",
            run_mode_label="trade",
            family="dynamic_long",
            hour_summary="EMA5 / EMA13，挂单参考 EMA5，ATR10 止损 1 倍",
            daily_summary="北京时间 8 点日线，收盘价在 EMA5 上方，只过滤做多",
            notes="动态止盈，2R 保本，手续费偏移开启，每趋势最多 1 次开仓",
            config=build_dynamic_long_config(
                symbol="BTC-USDT-SWAP",
                ema_period=5,
                trend_period=13,
                atr_stop="1",
                entry_reference_period=0,
                daily_ma_type="ema",
                daily_period=5,
                environment=environment,
            ),
        ),
        ReadyStrategySpec(
            profile_id="eth-long-ma5-bjt08",
            profile_name="ETH 做多 MA5 日线闸门",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            symbol="ETH-USDT-SWAP",
            direction_label="long_only",
            run_mode_label="trade",
            family="dynamic_long",
            hour_summary="EMA21 / EMA55，挂单参考 EMA34，ATR10 止损 1.5 倍",
            daily_summary="北京时间 8 点日线，收盘价在 MA5 上方，只过滤做多",
            notes="动态止盈，2R 保本，手续费偏移开启，每趋势最多 1 次开仓",
            config=build_dynamic_long_config(
                symbol="ETH-USDT-SWAP",
                ema_period=21,
                trend_period=55,
                atr_stop="1.5",
                entry_reference_period=34,
                daily_ma_type="ma",
                daily_period=5,
                environment=environment,
            ),
        ),
        ReadyStrategySpec(
            profile_id="sol-long-ema5-bjt08",
            profile_name="SOL 做多 EMA5 日线闸门",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            symbol="SOL-USDT-SWAP",
            direction_label="long_only",
            run_mode_label="trade",
            family="dynamic_long",
            hour_summary="EMA21 / EMA55，挂单参考 EMA13，ATR10 止损 1 倍",
            daily_summary="北京时间 8 点日线，收盘价在 EMA5 上方，只过滤做多",
            notes="动态止盈，2R 保本，手续费偏移开启，每趋势最多 1 次开仓",
            config=build_dynamic_long_config(
                symbol="SOL-USDT-SWAP",
                ema_period=21,
                trend_period=55,
                atr_stop="1",
                entry_reference_period=13,
                daily_ma_type="ema",
                daily_period=5,
                environment=environment,
            ),
        ),
        ReadyStrategySpec(
            profile_id="bnb-long-ema5-bjt08",
            profile_name="BNB 做多 EMA5 日线闸门",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            symbol="BNB-USDT-SWAP",
            direction_label="long_only",
            run_mode_label="trade",
            family="dynamic_long",
            hour_summary="EMA8 / EMA21，挂单参考 EMA13，ATR10 止损 1 倍",
            daily_summary="北京时间 8 点日线，收盘价在 EMA5 上方，只过滤做多",
            notes="动态止盈，2R 保本，手续费偏移开启，每趋势最多 1 次开仓",
            config=build_dynamic_long_config(
                symbol="BNB-USDT-SWAP",
                ema_period=8,
                trend_period=21,
                atr_stop="1",
                entry_reference_period=13,
                daily_ma_type="ema",
                daily_period=5,
                environment=environment,
            ),
        ),
        ReadyStrategySpec(
            profile_id="doge-long-ma13-bjt08",
            profile_name="DOGE 做多 MA13 日线闸门",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            symbol="DOGE-USDT-SWAP",
            direction_label="long_only",
            run_mode_label="trade",
            family="dynamic_long",
            hour_summary="EMA5 / EMA13，挂单参考 EMA5，ATR10 止损 1.5 倍",
            daily_summary="北京时间 8 点日线，收盘价在 MA13 上方，只过滤做多",
            notes="动态止盈，2R 保本，手续费偏移开启，每趋势最多 1 次开仓",
            config=build_dynamic_long_config(
                symbol="DOGE-USDT-SWAP",
                ema_period=5,
                trend_period=13,
                atr_stop="1.5",
                entry_reference_period=0,
                daily_ma_type="ma",
                daily_period=13,
                environment=environment,
            ),
        ),
        ReadyStrategySpec(
            profile_id="btc-short-ema34-ema21-bjt08",
            profile_name="BTC 做空 EMA34 + 日线 EMA21",
            strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
            symbol="BTC-USDT-SWAP",
            direction_label="short_only",
            run_mode_label="trade",
            family="slope_short",
            hour_summary="EMA34 斜率 <= -0.0005，ATR14 止损 2 倍，ATR 分位 <= 0.5",
            daily_summary="北京时间 8 点日线，收盘价在 EMA21 下方，只过滤做空",
            notes="动态止盈，2R 保本，关闭斜率转正平仓",
            config=build_slope_short_config(
                symbol="BTC-USDT-SWAP",
                ma_type="ema",
                period=34,
                daily_mode="close_vs_ma",
                daily_ma_type="ema",
                daily_period=21,
                environment=environment,
            ),
        ),
        ReadyStrategySpec(
            profile_id="eth-short-ema55-ema55-bjt08",
            profile_name="ETH 做空 EMA55 + 日线 EMA55",
            strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
            symbol="ETH-USDT-SWAP",
            direction_label="short_only",
            run_mode_label="trade",
            family="slope_short",
            hour_summary="EMA55 斜率 <= -0.0005，ATR14 止损 2 倍，ATR 分位 <= 0.5",
            daily_summary="北京时间 8 点日线，收盘价在 EMA55 下方，只过滤做空",
            notes="动态止盈，2R 保本，关闭斜率转正平仓",
            config=build_slope_short_config(
                symbol="ETH-USDT-SWAP",
                ma_type="ema",
                period=55,
                daily_mode="close_vs_ma",
                daily_ma_type="ema",
                daily_period=55,
                environment=environment,
            ),
        ),
        ReadyStrategySpec(
            profile_id="sol-short-ma20-ema21-bjt08",
            profile_name="SOL 做空 MA20 + 日线 EMA21",
            strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
            symbol="SOL-USDT-SWAP",
            direction_label="short_only",
            run_mode_label="trade",
            family="slope_short",
            hour_summary="MA20 斜率 <= -0.0005，ATR14 止损 2 倍，ATR 分位 <= 0.5",
            daily_summary="北京时间 8 点日线，收盘价在 EMA21 下方，只过滤做空",
            notes="动态止盈，2R 保本，关闭斜率转正平仓",
            config=build_slope_short_config(
                symbol="SOL-USDT-SWAP",
                ma_type="ma",
                period=20,
                daily_mode="close_vs_ma",
                daily_ma_type="ema",
                daily_period=21,
                environment=environment,
            ),
        ),
        ReadyStrategySpec(
            profile_id="bnb-short-body-retest-bjt08",
            profile_name="BNB 做空 Body/ATR 回抽 + 弱日",
            strategy_id=STRATEGY_BODY_RETEST_SHORT_ID,
            symbol="BNB-USDT-SWAP",
            direction_label="short_only",
            run_mode_label="trade",
            family="body_retest_short",
            hour_summary="MA20 破位 0.2 ATR，回抽 0.3 ATR，止损缓冲 0.3 ATR",
            daily_summary="北京时间 8 点日线，Weak Day，只过滤做空",
            notes="body/ATR <= 1.0，ATR14，ATR 分位 <= 0.5，观察 6 根 K，动态止盈，2R 保本",
            config=build_body_retest_short_config(symbol="BNB-USDT-SWAP", environment=environment),
        ),
        ReadyStrategySpec(
            profile_id="doge-short-ma55-ma20-bjt08",
            profile_name="DOGE 做空 MA55 + 日线 MA20",
            strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
            symbol="DOGE-USDT-SWAP",
            direction_label="short_only",
            run_mode_label="trade",
            family="slope_short",
            hour_summary="MA55 斜率 <= -0.0005，ATR14 止损 2 倍，ATR 分位 <= 0.5",
            daily_summary="北京时间 8 点日线，收盘价在 MA20 下方，只过滤做空",
            notes="动态止盈，2R 保本，关闭斜率转正平仓",
            config=build_slope_short_config(
                symbol="DOGE-USDT-SWAP",
                ma_type="ma",
                period=55,
                daily_mode="close_vs_ma",
                daily_ma_type="ma",
                daily_period=20,
                environment=environment,
            ),
        ),
    )


def build_bundle(bundle_name: str, specs: tuple[ReadyStrategySpec, ...]) -> StrategyBundle:
    profiles = tuple(
        build_strategy_profile_from_config(
            profile_id=spec.profile_id,
            profile_name=spec.profile_name,
            strategy_id=spec.strategy_id,
            symbol=spec.symbol,
            config=spec.config,
            api_name="",
            direction_label=spec.direction_label,
            run_mode_label=spec.run_mode_label,
            tags=("five-coin", "daily-filter", "bjt-08", spec.family),
            notes=spec.notes,
            source_report=SOURCE_REPORT,
        )
        for spec in specs
    )
    return StrategyBundle(
        bundle_version=STRATEGY_PROFILE_SCHEMA_VERSION,
        bundle_name=bundle_name,
        profiles=profiles,
        created_at=utc_now_text(),
        source_report=SOURCE_REPORT,
        auto_start_on_import=False,
    )


def render_rows(specs: tuple[ReadyStrategySpec, ...]) -> str:
    direction_map = {
        "long_only": "只做多",
        "short_only": "只做空",
    }
    family_map = {
        "dynamic_long": "动态委托做多",
        "slope_short": "斜率做空",
        "body_retest_short": "Body/ATR 回抽做空",
    }
    rows: list[str] = []
    for spec in specs:
        rows.append(
            "<tr>"
            f"<td>{html.escape(spec.symbol)}</td>"
            f"<td>{html.escape(direction_map.get(spec.direction_label, spec.direction_label))}</td>"
            f"<td>{html.escape(family_map.get(spec.family, spec.family))}</td>"
            f"<td>{html.escape(spec.hour_summary)}</td>"
            f"<td>{html.escape(spec.daily_summary)}</td>"
            f"<td>{html.escape(spec.notes)}</td>"
            "</tr>"
        )
    return "\n".join(rows)


def build_html_manual(
    *,
    demo_bundle_path: Path,
    live_bundle_path: Path,
    ready_specs: tuple[ReadyStrategySpec, ...],
) -> str:
    generated_at = utc_now_text()
    rows = render_rows(ready_specs)
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>五币日线过滤策略操作手册</title>
  <style>
    body {{
      margin: 0;
      font-family: "Segoe UI", "Microsoft YaHei UI", sans-serif;
      background: linear-gradient(180deg, #f6f2e9 0%, #f1f5f9 100%);
      color: #1f2937;
    }}
    .wrap {{
      max-width: 1180px;
      margin: 0 auto;
      padding: 28px 20px 44px;
    }}
    .card {{
      background: #fffdf8;
      border: 1px solid #d6d0c4;
      border-radius: 18px;
      box-shadow: 0 10px 30px rgba(15, 23, 42, 0.06);
      padding: 22px;
      margin-bottom: 18px;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 14px;
      margin-top: 16px;
    }}
    .kpi {{
      padding: 16px;
      background: #eef6f5;
      border: 1px solid #d6ebe8;
      border-radius: 14px;
    }}
    .label {{ color: #5b6472; font-size: 13px; margin-bottom: 8px; }}
    .value {{ font-size: 24px; font-weight: 700; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 12px; font-size: 14px; }}
    th, td {{ border-bottom: 1px solid #d6d0c4; text-align: left; vertical-align: top; padding: 10px 8px; }}
    th {{ background: #f7fafc; }}
    code {{ background: #f2f4f8; padding: 1px 6px; border-radius: 8px; font-family: Consolas, monospace; }}
    .warn {{
      border-left: 4px solid #92400e;
      background: #fff7ed;
      padding: 12px 14px;
      border-radius: 12px;
      margin-top: 14px;
    }}
    p, li {{ line-height: 1.75; }}
    ul {{ margin: 8px 0 0 20px; padding: 0; }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="card">
      <h1>五币日线过滤策略操作手册</h1>
      <p>生成时间：{html.escape(generated_at)}</p>
      <p>统一标准：<code>只使用当时已经收盘的上一根日线</code>。默认边界：<code>bjt_08</code>，也就是北京时间 08:00 切日线。默认风险口径：<code>每笔 10U</code>。</p>
      <div class="grid">
        <div class="kpi"><div class="label">正式可导入策略</div><div class="value">10 条</div></div>
        <div class="kpi"><div class="label">默认日线边界</div><div class="value">BJT 08:00</div></div>
        <div class="kpi"><div class="label">导入方式</div><div class="value">手动导入</div></div>
      </div>
      <div class="warn">
        说明：BNB 的专用 <code>body/ATR retest short</code> 已完成正式策略化，这一版 bundle 已正式纳入。
      </div>
    </section>

    <section class="card">
      <h2>交付文件</h2>
      <table>
        <thead><tr><th>文件</th><th>路径</th><th>说明</th></tr></thead>
        <tbody>
          <tr><td>Demo Bundle</td><td>{html.escape(str(demo_bundle_path))}</td><td>环境为 demo，默认不自动启动。</td></tr>
          <tr><td>Live Bundle</td><td>{html.escape(str(live_bundle_path))}</td><td>环境为 live，默认不自动启动，建议先在模拟盘或 dry-run 验证。</td></tr>
        </tbody>
      </table>
    </section>

    <section class="card">
      <h2>正式策略清单</h2>
      <table>
        <thead>
          <tr>
            <th>标的</th>
            <th>方向</th>
            <th>策略家族</th>
            <th>1H 核心参数</th>
            <th>日线过滤</th>
            <th>补充说明</th>
          </tr>
        </thead>
        <tbody>
          {rows}
        </tbody>
      </table>
    </section>

    <section class="card">
      <h2>使用说明</h2>
      <table>
        <thead>
          <tr>
            <th>步骤</th>
            <th>说明</th>
          </tr>
        </thead>
        <tbody>
          <tr><td>1. 导入 Bundle</td><td>优先先导入 Demo Bundle，确认参数回填、日线过滤摘要、策略列表都正常，再考虑导入 Live Bundle。</td></tr>
          <tr><td>2. 选择 API</td><td>导入时可以保留文件内 API、统一改成当前 API，或者逐条映射到不同 API。</td></tr>
          <tr><td>3. 部分导入</td><td>如果只想先启用一部分策略，可以取消勾选不需要的条目，Bundle 支持部分导入。</td></tr>
          <tr><td>4. 回测核对</td><td>回测时统一使用 1H，日线过滤保持“上一根已收盘日线”，默认边界为 BJT 08:00。</td></tr>
          <tr><td>5. 实盘建议</td><td>Live Bundle 建议先做 dry-run 或模拟盘验证，确认日志里的日线过滤摘要与预期一致后再正式启动。</td></tr>
        </tbody>
      </table>
    </section>

    <section class="card">
      <h2>审计口径</h2>
      <p>这批策略在回测、回测 UI、实盘 runtime、Bundle 导入这几层已经统一口径：<code>日线过滤只能看到当时上一根已经收盘的日线</code>，不会读取当天尚未收盘的日线收盘价。</p>
      <p>其中 <code>bjt_08</code> 是从 1H 已收盘 K 线重采样得到的北京时间 08:00 日线，而不是直接混用交易所原生 1D。</p>
    </section>
  </div>
</body>
</html>
"""


def build_metadata_payload(
    *,
    demo_bundle_path: Path,
    live_bundle_path: Path,
    ready_specs: tuple[ReadyStrategySpec, ...],
) -> dict[str, object]:
    return {
        "generated_at": utc_now_text(),
        "source_report": SOURCE_REPORT,
        "default_boundary": "bjt_08",
        "risk_per_trade_u": "10",
        "ready_bundle_paths": {
            "demo": str(demo_bundle_path),
            "live": str(live_bundle_path),
        },
        "ready_profiles": [
            {
                "profile_id": spec.profile_id,
                "profile_name": spec.profile_name,
                "symbol": spec.symbol,
                "strategy_id": spec.strategy_id,
                "direction_label": spec.direction_label,
                "family": spec.family,
                "hour_summary": spec.hour_summary,
                "daily_summary": spec.daily_summary,
                "notes": spec.notes,
            }
            for spec in ready_specs
        ],
    }


def main() -> None:
    demo_specs = build_ready_specs("demo")
    live_specs = build_ready_specs("live")

    demo_bundle = build_bundle("five_coin_daily_filter_ready10_bjt08_demo", demo_specs)
    live_bundle = build_bundle("five_coin_daily_filter_ready10_bjt08_live", live_specs)

    demo_bundle_path = PACKAGE_DIR / f"{demo_bundle.bundle_name}_{STAMP}.json"
    live_bundle_path = PACKAGE_DIR / f"{live_bundle.bundle_name}_{STAMP}.json"
    manual_path = PACKAGE_DIR / f"five_coin_daily_filter_ops_manual_{STAMP}.html"
    metadata_path = PACKAGE_DIR / f"five_coin_daily_filter_ops_pack_{STAMP}.json"

    write_strategy_bundle(demo_bundle, demo_bundle_path)
    write_strategy_bundle(live_bundle, live_bundle_path)
    manual_path.write_text(
        build_html_manual(
            demo_bundle_path=demo_bundle_path,
            live_bundle_path=live_bundle_path,
            ready_specs=demo_specs,
        ),
        encoding="utf-8",
    )
    metadata_path.write_text(
        json.dumps(
            build_metadata_payload(
                demo_bundle_path=demo_bundle_path,
                live_bundle_path=live_bundle_path,
                ready_specs=demo_specs,
            ),
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(manual_path)
    print(demo_bundle_path)
    print(live_bundle_path)
    print(metadata_path)


if __name__ == "__main__":
    main()
