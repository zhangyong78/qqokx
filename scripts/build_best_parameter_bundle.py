from __future__ import annotations

import html
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
    STRATEGY_BTC_EMA55_SLOPE_SHORT_ID,
    STRATEGY_DYNAMIC_LONG_ID,
    STRATEGY_EMA55_SLOPE_SHORT_ID,
)
from okx_quant.strategy_profiles import (
    STRATEGY_PROFILE_SCHEMA_VERSION,
    StrategyBundle,
    build_strategy_profile_from_config,
    write_strategy_bundle,
)


BUNDLE_NAME = "\u6700\u4f73\u53c2\u6570\u7ec4\u5408\u5305"
HTML_NAME = "\u6700\u4f73\u53c2\u6570\u7ec4\u5408\u5305\u8bf4\u660e.html"
PACKAGE_DIR = analysis_report_dir_path() / "packages"
PACKAGE_DIR.mkdir(parents=True, exist_ok=True)
JSON_PATH = PACKAGE_DIR / f"{BUNDLE_NAME}.json"
HTML_PATH = PACKAGE_DIR / HTML_NAME
REPORTS_DIR = ROOT / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
LEGACY_HTML_PATH = REPORTS_DIR / HTML_NAME


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


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _fmt_decimal(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _html_text(text: str) -> str:
    escaped = html.escape(text, quote=True)
    return "".join(f"&#x{ord(ch):X};" if ord(ch) > 127 else ch for ch in escaped)


def build_dynamic_long_config(
    *,
    symbol: str,
    ema_period: int,
    trend_ema_period: int,
    entry_reference_ema_period: int,
    atr_stop_multiplier: Decimal,
    trigger_r: int,
) -> StrategyConfig:
    return StrategyConfig(
        inst_id=symbol,
        bar="1H",
        ema_period=ema_period,
        ema_type="ema",
        trend_ema_period=trend_ema_period,
        trend_ema_type="ema",
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=atr_stop_multiplier,
        atr_take_multiplier=atr_stop_multiplier * Decimal("2"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment="live",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        risk_amount=Decimal("100"),
        backtest_initial_capital=Decimal("10000"),
        backtest_sizing_mode="fixed_risk",
        entry_reference_ema_period=entry_reference_ema_period,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        ema55_slope_lock_profit_trigger_r=trigger_r,
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=0,
        startup_chase_window_seconds=0,
        tp_sl_mode="exchange",
        run_mode="trade",
        entry_side_mode="follow_signal",
    )


def build_slope_short_config(
    *,
    symbol: str,
    ema_period: int,
    ema_type: str,
    trend_ema_period: int,
    trend_ema_type: str,
) -> StrategyConfig:
    return StrategyConfig(
        inst_id=symbol,
        bar="1H",
        ema_period=ema_period,
        ema_type=ema_type,
        trend_ema_period=trend_ema_period,
        trend_ema_type=trend_ema_type,
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
        strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
        risk_amount=Decimal("100"),
        backtest_initial_capital=Decimal("10000"),
        backtest_sizing_mode="fixed_risk",
        take_profit_mode="dynamic",
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        ema55_slope_exit_enabled=True,
        ema55_slope_lock_profit_trigger_r=5,
        atr_percentile_filter_max=Decimal("0.5"),
        trend_ema_slope_filter_min_ratio=Decimal("-0.0005"),
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=10,
        tp_sl_mode="exchange",
        run_mode="trade",
        entry_side_mode="follow_signal",
        daily_filter_enabled=False,
        daily_filter_boundary="bjt_08",
        daily_filter_mode="disabled",
        daily_filter_scope="short_only",
        daily_filter_ma_type="ema",
        daily_filter_period=21,
    )


def build_btc_slope_short_config() -> StrategyConfig:
    return StrategyConfig(
        inst_id="BTC-USDT-SWAP",
        bar="1H",
        ema_period=55,
        ema_type="ema",
        trend_ema_period=55,
        trend_ema_type="ema",
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
        strategy_id=STRATEGY_BTC_EMA55_SLOPE_SHORT_ID,
        risk_amount=Decimal("100"),
        backtest_initial_capital=Decimal("10000"),
        backtest_sizing_mode="fixed_risk",
        take_profit_mode="dynamic",
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        ema55_slope_exit_enabled=True,
        ema55_slope_lock_profit_enabled=True,
        ema55_slope_lock_profit_trigger_r=5,
        ema55_slope_negative_entry_bars=1,
        trend_ema_slope_filter_min_ratio=Decimal("-0.0005"),
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=10,
        tp_sl_mode="exchange",
        run_mode="trade",
        entry_side_mode="follow_signal",
    )


def build_specs() -> tuple[BundleSpec, ...]:
    return (
        BundleSpec(
            side="\u505a\u591a",
            symbol="BTC-USDT-SWAP",
            profile_id="dynamic_long_best_btc_v2",
            profile_name="BTC \u52a8\u6001\u59d4\u6258\u505a\u591a \u6700\u4f73\u53c2\u6570",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            strategy_label="EMA \u52a8\u6001\u59d4\u6258\u505a\u591a",
            core_label="EMA5 / EMA13 / \u5165\u573a\u8ddf\u968f EMA5",
            protection_label="ATR10 / SL1 / 6R \u5f00\u542f nR \u4fdd\u672c / \u53cc\u5411\u624b\u7eed\u8d39",
            note="BTC \u591a\u5934\u56de\u6d4b\u4e2d 6R \u6536\u76ca\u6700\u597d\uff0c\u4fdd\u7559\u66f4\u591a\u8d8b\u52bf\u7a7a\u95f4\u3002",
            config=build_dynamic_long_config(
                symbol="BTC-USDT-SWAP",
                ema_period=5,
                trend_ema_period=13,
                entry_reference_ema_period=0,
                atr_stop_multiplier=Decimal("1"),
                trigger_r=6,
            ),
        ),
        BundleSpec(
            side="\u505a\u591a",
            symbol="ETH-USDT-SWAP",
            profile_id="dynamic_long_best_eth_v2",
            profile_name="ETH \u52a8\u6001\u59d4\u6258\u505a\u591a \u6700\u4f73\u53c2\u6570",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            strategy_label="EMA \u52a8\u6001\u59d4\u6258\u505a\u591a",
            core_label="EMA21 / EMA55 / \u5165\u573a EMA34",
            protection_label="ATR10 / SL1.5 / 3R \u5f00\u542f nR \u4fdd\u672c / \u53cc\u5411\u624b\u7eed\u8d39",
            note="ETH \u591a\u5934 3R \u6536\u76ca\u4e0e PF \u6700\u597d\uff0c\u4e0d\u8ffd\u9ad8 R\u3002",
            config=build_dynamic_long_config(
                symbol="ETH-USDT-SWAP",
                ema_period=21,
                trend_ema_period=55,
                entry_reference_ema_period=34,
                atr_stop_multiplier=Decimal("1.5"),
                trigger_r=3,
            ),
        ),
        BundleSpec(
            side="\u505a\u591a",
            symbol="SOL-USDT-SWAP",
            profile_id="dynamic_long_best_sol_v2",
            profile_name="SOL \u52a8\u6001\u59d4\u6258\u505a\u591a \u6700\u4f73\u53c2\u6570",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            strategy_label="EMA \u52a8\u6001\u59d4\u6258\u505a\u591a",
            core_label="EMA21 / EMA55 / \u5165\u573a EMA13",
            protection_label="ATR10 / SL1 / 3R \u5f00\u542f nR \u4fdd\u672c / \u53cc\u5411\u624b\u7eed\u8d39",
            note="SOL \u591a\u5934 3R \u6536\u76ca\u6700\u597d\uff0c\u4e14\u6bd4\u9ad8 R \u56de\u64a4\u66f4\u53ef\u63a7\u3002",
            config=build_dynamic_long_config(
                symbol="SOL-USDT-SWAP",
                ema_period=21,
                trend_ema_period=55,
                entry_reference_ema_period=13,
                atr_stop_multiplier=Decimal("1"),
                trigger_r=3,
            ),
        ),
        BundleSpec(
            side="\u505a\u591a",
            symbol="DOGE-USDT-SWAP",
            profile_id="dynamic_long_best_doge_v2",
            profile_name="DOGE \u52a8\u6001\u59d4\u6258\u505a\u591a \u6700\u4f73\u53c2\u6570",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            strategy_label="EMA \u52a8\u6001\u59d4\u6258\u505a\u591a",
            core_label="EMA5 / EMA13 / \u5165\u573a\u8ddf\u968f EMA5",
            protection_label="ATR10 / SL1.5 / 6R \u5f00\u542f nR \u4fdd\u672c / \u53cc\u5411\u624b\u7eed\u8d39",
            note="DOGE \u591a\u5934 6R \u6536\u76ca\u6700\u597d\uff0c8R PF \u66f4\u9ad8\u4f46\u66f4\u504f\u8fdb\u653b\u3002",
            config=build_dynamic_long_config(
                symbol="DOGE-USDT-SWAP",
                ema_period=5,
                trend_ema_period=13,
                entry_reference_ema_period=0,
                atr_stop_multiplier=Decimal("1.5"),
                trigger_r=6,
            ),
        ),
        BundleSpec(
            side="\u505a\u7a7a",
            symbol="BTC-USDT-SWAP",
            profile_id="slope_short_best_btc_v2",
            profile_name="BTC \u5747\u7ebf\u659c\u7387\u505a\u7a7a \u6700\u4f73\u53c2\u6570",
            strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
            strategy_label="\u5747\u7ebf\u659c\u7387\u505a\u7a7a",
            core_label="EMA55 / EMA55",
            protection_label="ATR14 / SL2 / 5R \u5f00\u542f nR \u4fdd\u672c / \u53cc\u5411\u624b\u7eed\u8d39 / \u659c\u7387\u8f6c\u6b63\u5e73\u4ed3",
            note="BTC \u7a7a\u5934\u6539\u4e3a\u901a\u7528\u5747\u7ebf\u659c\u7387\u505a\u7a7a\uff1bBTC EMA55 \u4e13\u7528\u7b56\u7565\u4ec5\u4fdd\u7559\u4e3a\u7814\u7a76\u7528\u3002",
            config=build_slope_short_config(
                symbol="BTC-USDT-SWAP",
                ema_period=55,
                ema_type="ema",
                trend_ema_period=55,
                trend_ema_type="ema",
            ),
        ),
        BundleSpec(
            side="\u505a\u7a7a",
            symbol="ETH-USDT-SWAP",
            profile_id="slope_short_best_eth_v2",
            profile_name="ETH \u5747\u7ebf\u659c\u7387\u505a\u7a7a \u6700\u4f73\u53c2\u6570",
            strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
            strategy_label="\u5747\u7ebf\u659c\u7387\u505a\u7a7a",
            core_label="MA60 / MA60",
            protection_label="ATR14 / SL2 / 5R \u5f00\u542f nR \u4fdd\u672c / \u53cc\u5411\u624b\u7eed\u8d39 / \u659c\u7387\u8f6c\u6b63\u5e73\u4ed3",
            note="ETH \u7a7a\u5934\u5747\u7ebf\u66ff\u6362\u5b9e\u9a8c\u4e2d MA60 \u6536\u76ca\u548c PF \u6700\u597d\u3002",
            config=build_slope_short_config(
                symbol="ETH-USDT-SWAP",
                ema_period=60,
                ema_type="ma",
                trend_ema_period=60,
                trend_ema_type="ma",
            ),
        ),
        BundleSpec(
            side="\u505a\u7a7a",
            symbol="SOL-USDT-SWAP",
            profile_id="slope_short_best_sol_v2",
            profile_name="SOL \u5747\u7ebf\u659c\u7387\u505a\u7a7a \u6700\u4f73\u53c2\u6570",
            strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
            strategy_label="\u5747\u7ebf\u659c\u7387\u505a\u7a7a",
            core_label="MA20 / MA20",
            protection_label="ATR14 / SL2 / 5R \u5f00\u542f nR \u4fdd\u672c / \u53cc\u5411\u624b\u7eed\u8d39 / \u659c\u7387\u8f6c\u6b63\u5e73\u4ed3",
            note="\u4f7f\u7528\u5f53\u524d SOL \u7a7a\u5934\u5b9a\u7a3f\u53c2\u6570\u3002",
            config=build_slope_short_config(
                symbol="SOL-USDT-SWAP",
                ema_period=20,
                ema_type="ma",
                trend_ema_period=20,
                trend_ema_type="ma",
            ),
        ),
        BundleSpec(
            side="\u505a\u7a7a",
            symbol="DOGE-USDT-SWAP",
            profile_id="slope_short_best_doge_v2",
            profile_name="DOGE \u5747\u7ebf\u659c\u7387\u505a\u7a7a \u6700\u4f73\u53c2\u6570",
            strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
            strategy_label="\u5747\u7ebf\u659c\u7387\u505a\u7a7a",
            core_label="MA21 / MA21",
            protection_label="ATR14 / SL2 / 5R \u5f00\u542f nR \u4fdd\u672c / \u53cc\u5411\u624b\u7eed\u8d39 / \u659c\u7387\u8f6c\u6b63\u5e73\u4ed3",
            note="DOGE \u7a7a\u5934\u6539\u7528 MA21\uff1a\u6536\u76ca\u66f4\u9ad8\uff0cPF \u7565\u4f4e\u4e8e\u539f MA55\u3002",
            config=build_slope_short_config(
                symbol="DOGE-USDT-SWAP",
                ema_period=21,
                ema_type="ma",
                trend_ema_period=21,
                trend_ema_type="ma",
            ),
        ),
    )


def build_bundle(specs: tuple[BundleSpec, ...]) -> StrategyBundle:
    profiles = []
    for spec in specs:
        profiles.append(
            build_strategy_profile_from_config(
                profile_id=spec.profile_id,
                profile_name=spec.profile_name,
                strategy_id=spec.strategy_id,
                symbol=spec.symbol,
                config=spec.config,
                direction_label=spec.side,
                run_mode_label="\u4ea4\u6613\u5e76\u4e0b\u5355",
                enabled=True,
                tags=(
                    "best-parameter-bundle",
                    "2026-06-11",
                    "long" if spec.side == "\u505a\u591a" else "short",
                ),
                notes=spec.note,
                source_report=str(ROOT / "scripts" / "build_best_parameter_bundle.py"),
            )
        )
    return StrategyBundle(
        bundle_version=STRATEGY_PROFILE_SCHEMA_VERSION,
        bundle_name=BUNDLE_NAME,
        profiles=tuple(profiles),
        created_at=_utc_now(),
        source_report=str(ROOT / "scripts" / "build_best_parameter_bundle.py"),
        auto_start_on_import=True,
    )


def build_html(specs: tuple[BundleSpec, ...]) -> str:
    long_count = sum(1 for spec in specs if spec.side == "\u505a\u591a")
    short_count = len(specs) - long_count
    rows = []
    for spec in specs:
        symbol_label = spec.symbol.replace("-USDT-SWAP", "")
        row = (
            "<tr>"
            f"<td>{_html_text(spec.side)}</td>"
            f"<td>{_html_text(symbol_label)}</td>"
            f"<td>{_html_text(spec.strategy_label)}</td>"
            f"<td>{_html_text(spec.core_label)}</td>"
            f"<td>{_html_text(spec.protection_label)}</td>"
            f"<td>{_html_text(spec.note)}</td>"
            "</tr>"
        )
        rows.append(row)

    bullets = [
        "\u7ec4\u5408\u5df2\u6539\u4e3a 4 \u4e2a\u505a\u591a + 4 \u4e2a\u505a\u7a7a\uff0c\u5df2\u79fb\u9664 BNB\u3002",
        "\u505a\u591a\u9996\u6863 R\uff1aBTC=6R\uff0cETH=3R\uff0cSOL=3R\uff0cDOGE=6R\uff0c\u5168\u90e8\u4fdd\u7559 nR \u4fdd\u672c+\u53cc\u5411\u624b\u7eed\u8d39\u3002",
        "\u505a\u7a7a\u9ed8\u8ba4\u4e3a\uff1aATR \u6b62\u635f=2\uff0c\u659c\u7387\u8f6c\u6b63\u5e73\u4ed3=\u5f00\uff0c5R \u5f00\u542f nR \u4fdd\u672c+\u53cc\u5411\u624b\u7eed\u8d39\u3002",
        "BTC \u7a7a\u5934\u4f7f\u7528\u901a\u7528\u5747\u7ebf\u659c\u7387\u505a\u7a7a EMA55\uff0cETH \u6539 MA60\uff0cSOL \u4fdd\u7559 MA20\uff0cDOGE \u6539 MA21\u3002",
    ]

    bullet_html = "".join(f"<li>{_html_text(item)}</li>" for item in bullets)
    title = _html_text("\u6700\u4f73\u53c2\u6570\u7ec4\u5408\u5305\u8bf4\u660e")
    generated_at = _html_text(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    json_path = _html_text(str(JSON_PATH))
    summary_text = _html_text(f"\u672c\u6b21\u5b9a\u7a3f\uff1a{long_count} \u4e2a\u505a\u591a + {short_count} \u4e2a\u505a\u7a7a")
    intro_text = _html_text("\u6b64\u6587\u4ef6\u5bf9\u5e94\u7684 JSON \u7ec4\u5408\u5305\u53ef\u76f4\u63a5\u5bfc\u5165\u3002")
    generated_label = _html_text("\u751f\u6210\u65f6\u95f4")
    json_label = _html_text("JSON \u8def\u5f84")
    core_change_label = _html_text("\u6838\u5fc3\u6539\u52a8")
    core_change_text = _html_text(
        "\u79fb\u9664 BNB\uff0cDOGE \u8865\u5165\u505a\u591a\u7ec4\u5408\uff1b\u505a\u591a\u5206\u5e01\u79cd\u8bbe\u7f6e 6R/3R/3R/6R\uff0c\u505a\u7a7a\u5747\u4f7f\u7528\u901a\u7528\u5747\u7ebf\u659c\u7387\u505a\u7a7a\uff1aBTC EMA55 / ETH MA60 / SOL MA20 / DOGE MA21\u3002"
    )
    overview_label = _html_text("\u7ec4\u5408\u6982\u8981")
    detail_label = _html_text("\u7b56\u7565\u660e\u7ec6")
    direction_label = _html_text("\u65b9\u5411")
    symbol_label = _html_text("\u5e01\u79cd")
    strategy_label = _html_text("\u7b56\u7565")
    core_label = _html_text("\u6838\u5fc3\u53c2\u6570")
    protection_label = _html_text("\u4fdd\u62a4\u903b\u8f91")
    note_label = _html_text("\u5907\u6ce8")
    rows_html = "".join(rows)
    body = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>{title}</title>
  <style>
    :root {{
      --bg: #f7f2e8;
      --panel: #fffaf0;
      --line: #dccfb7;
      --ink: #1f2a30;
      --accent: #0f766e;
      --accent-soft: #e6f4ef;
      --warn: #9a3412;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      padding: 28px;
      background:
        radial-gradient(circle at top right, rgba(15,118,110,0.12), transparent 28%),
        radial-gradient(circle at top left, rgba(217,119,6,0.10), transparent 24%),
        var(--bg);
      color: var(--ink);
      font-family: "Microsoft YaHei UI", "Microsoft YaHei", sans-serif;
      line-height: 1.6;
    }}
    .hero, .panel {{
      max-width: 1360px;
      margin: 0 auto 18px auto;
      background: rgba(255, 250, 240, 0.92);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 22px 24px;
      box-shadow: 0 18px 50px rgba(44, 38, 26, 0.08);
      backdrop-filter: blur(6px);
    }}
    h1, h2 {{ margin: 0 0 12px 0; }}
    h1 {{ font-size: 40px; color: #0b5d58; }}
    h2 {{ font-size: 22px; color: #7c2d12; }}
    p {{ margin: 0 0 10px 0; }}
    ul {{ margin: 0; padding-left: 20px; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 15px;
    }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 12px 10px;
      text-align: left;
      vertical-align: top;
    }}
    th {{
      background: rgba(230, 244, 239, 0.85);
      color: #0b5d58;
      position: sticky;
      top: 0;
    }}
    .meta {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 12px;
      margin-top: 14px;
    }}
    .chip {{
      background: var(--accent-soft);
      border: 1px solid rgba(15,118,110,0.18);
      border-radius: 14px;
      padding: 12px 14px;
    }}
    .summary {{
      color: var(--warn);
      font-weight: 700;
    }}
  </style>
</head>
<body>
  <section class="hero">
    <h1>{title}</h1>
    <p class="summary">{summary_text}</p>
    <p>{intro_text}</p>
    <div class="meta">
      <div class="chip"><strong>{generated_label}</strong><br>{generated_at}</div>
      <div class="chip"><strong>{json_label}</strong><br>{json_path}</div>
      <div class="chip"><strong>{core_change_label}</strong><br>{core_change_text}</div>
    </div>
  </section>
  <section class="panel">
    <h2>{overview_label}</h2>
    <ul>{bullet_html}</ul>
  </section>
  <section class="panel">
    <h2>{detail_label}</h2>
    <table>
      <thead>
        <tr>
          <th>{direction_label}</th>
          <th>{symbol_label}</th>
          <th>{strategy_label}</th>
          <th>{core_label}</th>
          <th>{protection_label}</th>
          <th>{note_label}</th>
        </tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
  </section>
</body>
</html>
"""
    return body


def write_outputs() -> tuple[Path, Path, Path]:
    specs = build_specs()
    bundle = build_bundle(specs)
    write_strategy_bundle(bundle, JSON_PATH)
    JSON_PATH.write_text(JSON_PATH.read_text(encoding="utf-8"), encoding="utf-8-sig")
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
