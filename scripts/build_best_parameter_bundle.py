from __future__ import annotations

import html
import json
import sys
from dataclasses import asdict, dataclass
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


UPDATE_LOGS: tuple[tuple[str, str], ...] = (
    (
        "2026-06-13",
        "补充“动态保护 R 口径说明”章节，明确手续费偏移开启后，保本触发R / 移动止盈触发R / 锁盈档位默认按双向手续费缓冲口径理解。",
    ),
    (
        "2026-06-13",
        "补充“策略设计思路”“参数字段说明”“完整参数 JSON”“文档末尾更新日志”，后续继续沿本文件末尾累计。",
    ),
    (
        "2026-06-12",
        "最佳参数组合包定稿为 4 多 + 4 空，并同步输出可直接导入的 JSON 组合包说明。",
    ),
)


FIELD_DESCRIPTIONS: dict[str, str] = {
    "inst_id": "交易标的，决定策略实际运行的合约。",
    "bar": "主执行周期，本包统一为 1H。",
    "ema_period": "快线周期；做多时用于识别结构启动，做空时用于锁定主导下行均线。",
    "ema_type": "快线类型，ema=指数均线，ma=简单均线。",
    "trend_ema_period": "趋势线周期，用来判定主趋势方向。",
    "trend_ema_type": "趋势线类型，决定趋势线是 EMA 还是 MA。",
    "big_ema_period": "更大级别参考线，本包保留统一框架口径。",
    "atr_period": "ATR 波动周期，用来统一止损和动态盈亏比的波动尺度。",
    "atr_stop_multiplier": "初始止损倍数，止损距离 = ATR x 该倍数。",
    "atr_take_multiplier": "基础止盈参考倍数；本包以动态止盈为主，仍保留统一配置值。",
    "order_size": "固定下单数量；为 0 表示由风险仓位逻辑接管。",
    "trade_mode": "保证金模式，本包统一为 cross。",
    "signal_mode": "信号方向，long_only=只做多，short_only=只做空。",
    "position_mode": "持仓模式，本包统一为 net。",
    "environment": "运行环境标识，本包写入 live。",
    "tp_sl_trigger_type": "止盈止损触发价类型，本包统一使用 mark。",
    "strategy_id": "策略实现 ID，决定走哪套信号与保护逻辑。",
    "risk_amount": "单笔固定风险金额，配合 fixed_risk 仓位模式使用。",
    "backtest_initial_capital": "回测初始资金口径。",
    "backtest_sizing_mode": "回测仓位模式；fixed_risk 表示按固定风险额下单。",
    "take_profit_mode": "止盈模式；dynamic 表示不写死终点，而是跟随走势动态处理。",
    "max_entries_per_trend": "单波趋势最多允许重复进场次数；做多 BTC 设为 3，其余默认 1。",
    "entry_reference_ema_period": "挂单/回踩参考线周期；做多会找这根线去挂单。",
    "entry_reference_ema_type": "挂单参考线类型；0 周期时默认跟随快线类型。",
    "dynamic_two_r_break_even": "是否启用达到首档 R 后的动态保本。",
    "dynamic_break_even_trigger_r": "动态保本触发 R，默认 2R。",
    "dynamic_fee_offset_enabled": "保本位是否补足双向手续费。",
    "ema55_slope_exit_enabled": "做空时是否启用“斜率转正/走平”失效平仓。",
    "ema55_slope_lock_profit_enabled": "做空时是否显式启用锁盈状态。",
    "ema55_slope_lock_profit_trigger_r": "动态锁盈开始触发的 R 值。",
    "dynamic_first_lock_r": "第一档锁盈起点；本包沿用统一默认值。",
    "dynamic_trailing_step_r": "动态锁盈递进步长。",
    "ema55_slope_negative_entry_bars": "做空要求连续多少根均线斜率满足阈值后才允许开仓。",
    "ema55_slope_same_bar_reentry_block": "是否禁止同一根 K 线重复开空。",
    "ema55_slope_dynamic_exit_requires_bear_reentry": "动态离场是否要求出现再次转弱确认。",
    "ema55_slope_dynamic_exit_bear_reentry_break_prev_low": "再次转弱时是否要求跌破前低。",
    "ema55_slope_dynamic_exit_requires_ema_reclaim": "动态离场前是否要求先回收均线。",
    "ema55_slope_locked_reentry_requires_ema21_near": "锁盈后再开空是否要求价格重新贴近 EMA21。",
    "ema55_slope_locked_reentry_min_r": "锁盈后再次开空的最小 R 门槛。",
    "ema55_slope_locked_reentry_max_r": "锁盈后再次开空的最大 R 门槛。",
    "ema55_slope_dynamic_exit_bull_bar_requires_bear_reentry": "阳线扰动后是否必须等再次转弱再离场。",
    "ema55_slope_dynamic_exit_bull_bar_reentry_min_r": "阳线扰动后二次确认的最小 R。",
    "ema55_slope_dynamic_exit_bull_bar_reentry_max_r": "阳线扰动后二次确认的最大 R。",
    "atr_percentile_filter_max": "ATR 分位过滤上限，用于限制极端波动环境。",
    "trend_ema_slope_filter_enabled": "是否启用趋势线回归斜率过滤。",
    "trend_ema_slope_filter_lookback_bars": "趋势线斜率回看窗口。",
    "trend_ema_slope_filter_min_ratio": "趋势线最小斜率阈值；做多要求不弱于它，做空要求不高于它。",
    "body_retest_breakdown_atr_multiplier": "实体回踩类做空策略的跌破确认阈值；本包保留框架默认值。",
    "body_retest_retest_atr_multiplier": "实体回踩类做空策略的回抽距离阈值；本包保留框架默认值。",
    "body_retest_stop_buffer_atr_multiplier": "实体回踩类做空策略的止损缓冲；本包保留框架默认值。",
    "body_retest_body_atr_limit": "实体回踩类做空策略的大实体过滤；本包保留框架默认值。",
    "body_retest_watch_bars": "实体回踩类做空策略的观察窗口；本包保留框架默认值。",
    "startup_chase_current_signal": "程序启动时是否追当前信号。",
    "startup_chase_window_seconds": "启动追单窗口；0 表示禁用追价。",
    "time_stop_break_even_enabled": "是否启用时间保本。",
    "time_stop_break_even_bars": "时间保本触发条数。",
    "hold_close_exit_bars": "持仓固定收盘离场条数；0 表示不用该退出。",
    "trader_virtual_stop_loss": "是否使用交易员虚拟止损。",
    "backtest_profile_id": "回测画像 ID，占位字段，导入时可覆写。",
    "backtest_profile_name": "回测画像名称，占位字段。",
    "backtest_profile_summary": "回测画像摘要，占位字段。",
    "cross_higher_tf_inst_id": "多周期突破过滤的更高周期标的。",
    "cross_higher_tf_bar": "多周期突破过滤的更高周期。",
    "cross_higher_tf_ref_ema_period": "多周期突破过滤参考线周期。",
    "mtf_filter_inst_id": "多周期过滤标的。",
    "mtf_filter_bar": "多周期过滤周期。",
    "mtf_filter_fast_ema_period": "多周期过滤快线周期。",
    "mtf_filter_slow_ema_period": "多周期过滤慢线周期。",
    "mtf_reversal_mode": "多周期反转处理模式。",
    "daily_filter_enabled": "是否启用日线过滤；当前包统一关闭。",
    "daily_filter_inst_id": "日线过滤标的，未指定时默认当前标的。",
    "daily_filter_bar": "日线过滤周期，未指定时默认 1D。",
    "daily_filter_boundary": "日线边界定义，例如北京 08 点切日。",
    "daily_filter_mode": "日线过滤模式；disabled 表示不启用。",
    "daily_filter_scope": "日线过滤作用范围。",
    "daily_filter_ma_type": "日线过滤使用的均线类型。",
    "daily_filter_period": "日线过滤均线周期。",
    "rail_candidate_ema_periods": "自适应轨道策略候选均线集合；本包保留框架默认值。",
    "rail_touch_atr_ratio": "自适应轨道触碰阈值；本包保留框架默认值。",
    "rail_bounce_atr_ratio": "自适应轨道反弹阈值；本包保留框架默认值。",
    "rail_bounce_confirm_bars": "自适应轨道反弹确认条数；本包保留框架默认值。",
    "rail_break_atr_ratio": "自适应轨道失效阈值；本包保留框架默认值。",
    "rail_reclaim_bars": "自适应轨道回收确认条数；本包保留框架默认值。",
    "rail_score_lookback_bars": "自适应轨道评分回看条数；本包保留框架默认值。",
    "rail_switch_min_score_delta": "自适应轨道切换最低分差；本包保留框架默认值。",
    "rail_min_touches": "自适应轨道最少触碰次数；本包保留框架默认值。",
    "rail_min_bounces": "自适应轨道最少反弹次数；本包保留框架默认值。",
    "rail_fast_gate_enabled": "自适应轨道快筛开关；本包保留框架默认值。",
    "rail_fast_gate_period": "自适应轨道快筛周期；本包保留框架默认值。",
    "rail_fast_min_gap_ema200_atr": "自适应轨道与 EMA200 最小间距；本包保留框架默认值。",
    "rail_fast_min_spread_trend_atr": "自适应轨道最小趋势展开；本包保留框架默认值。",
    "rail_fast_max_recent_range_atr": "自适应轨道近期最大震荡范围；本包保留框架默认值。",
    "rail_fast_recent_range_bars": "自适应轨道近期震荡统计条数；本包保留框架默认值。",
}


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


def _format_bool(value: bool) -> str:
    return "开" if value else "关"


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


def _config_dict(config: StrategyConfig) -> dict[str, object]:
    return {key: _json_ready(value) for key, value in asdict(config).items()}


def _config_json(config: StrategyConfig) -> str:
    return json.dumps(_config_dict(config), ensure_ascii=False, indent=2)


def _render_value(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return _format_bool(value)
    if isinstance(value, list):
        return "[" + ", ".join(str(item) for item in value) + "]"
    return str(value)


def _spec_thesis(spec: BundleSpec) -> tuple[str, tuple[str, ...]]:
    config = spec.config
    reference_line = config.entry_reference_line_label()
    trend_line = config.trend_ema_label()
    fast_line = config.ema_label()
    if spec.side == "\u505a\u591a":
        return (
            "\u5148\u7528\u53cc\u5747\u7ebf\u786e\u8ba4\u591a\u5934\u7ed3\u6784\uff0c\u518d\u53bb\u627e\u9002\u5408\u6302\u5355\u7684\u90a3\u6839\u7ebf\u7b49\u56de\u8c03\u3002\u4e0d\u8ffd\u4ef7\uff0c\u4e0d\u5728\u7ed3\u6784\u6ca1\u7ad9\u7a33\u65f6\u786c\u4e0a\uff0c\u800c\u662f\u7528\u8f83\u5c0f\u7684\u521d\u59cb\u98ce\u9669\u53bb\u6362\u53d6\u66f4\u5927\u7684\u8d8b\u52bf\u6bb5\u3002",
            (
                f"\u7ed3\u6784\u786e\u8ba4\uff1a{fast_line} \u5728 {trend_line} \u4e0a\u65b9\uff0c\u4e14\u6536\u76d8\u7ad9\u4e0a {trend_line}\u3002",
                f"\u6302\u5355\u65b9\u5f0f\uff1a\u53cc\u5747\u7ebf\u4ea4\u53c9\u540e\uff0c\u4e0d\u76f4\u63a5\u8ffd\u5165\uff0c\u800c\u662f\u56f4\u7ed5 {reference_line} \u53bb\u7b49\u56de\u8c03\u59d4\u6258\u3002",
                f"\u76c8\u4e8f\u6bd4\u7ba1\u7406\uff1a\u5148\u5728 R={config.dynamic_break_even_trigger_r} \u505a\u4fdd\u672c\uff0c\u518d\u5728 R={config.ema55_slope_lock_profit_trigger_r} \u8fdb\u5165 nR \u9010\u7ea7\u9501\u76c8 + \u624b\u7eed\u8d39\u8865\u507f\u3002",
                f"\u4e0d\u540c\u5e01\u79cd\u7684\u9996\u6863 R \u72ec\u7acb\u4f18\u5316\uff0c\u5355\u6ce2\u6700\u591a\u5141\u8bb8 {config.max_entries_per_trend} \u6b21\u8fdb\u573a\uff0c\u7528\u6765\u5403\u4e3b\u5347\u6bb5\u3002",
                "\u591a\u5934\u7684\u4f18\u52bf\u662f\uff1a\u56de\u8c03\u5165\u573a\u80fd\u538b\u7f29\u521d\u59cb\u6b62\u635f\uff0c\u800c\u8d8b\u52bf\u5ef6\u7eed\u65f6\u53ef\u4ee5\u8ba9\u76c8\u5229\u6bb5\u81ea\u7136\u6253\u5f00\uff0c\u6574\u4f53\u76c8\u4e8f\u6bd4\u66f4\u5927\u3002",
            ),
        )
    return (
        "\u5148\u9501\u5b9a\u4e00\u6761\u6b63\u5728\u5411\u4e0b\u4e3b\u5bfc\u7684\u5747\u7ebf\uff0c\u5f53\u659c\u7387\u8fbe\u5230\u8bbe\u5b9a\u9608\u503c\u540e\u76f4\u63a5\u8ddf\u968f\u7a7a\u5934\uff0c\u4e0d\u5f3a\u6c42\u989d\u5916\u56de\u62bd\u786e\u8ba4\u3002\u5982\u679c\u659c\u7387\u7ee7\u7eed\u53d8\u9661\uff0c\u5c31\u7ed9\u4e88\u4ed3\u4f4d\u66f4\u5927\u7684\u6ce2\u52a8\u7a7a\u95f4\uff0c\u8ba9\u8d8b\u52bf\u81ea\u5df1\u8dd1\u3002",
        (
            f"\u4e3b\u5bfc\u7ebf\u9501\u5b9a\uff1a\u672c\u5305\u5bf9 {spec.symbol.replace('-USDT-SWAP', '')} \u5df2\u7ecf\u5b9a\u7a3f\u4e3b\u5bfc\u7ebf = {fast_line}\uff08\u8d8b\u52bf\u53c2\u8003\u540c\u6b65\u4f7f\u7528 {trend_line}\uff09\u3002",
            f"\u5f00\u4ed3\u6761\u4ef6\uff1a\u5747\u7ebf\u659c\u7387\u6bd4\u4f8b <= {config.trend_ema_slope_filter_min_ratio}\uff0c\u6ee1\u8db3\u65f6\u76f4\u63a5\u505a\u7a7a\u3002",
            f"\u8fde\u7eed\u786e\u8ba4\uff1a\u901a\u7528\u659c\u7387\u505a\u7a7a\u5f53\u524d\u9ed8\u8ba4 {config.ema55_slope_negative_entry_bars} \u6839\u6ee1\u8db3\u9608\u503c\u5373\u53ef\u5f00\u4ed3\u3002",
            f"\u52a8\u6001\u76c8\u4e8f\u6bd4\uff1a{config.ema55_slope_lock_profit_trigger_r}R \u540e\u5f00\u59cb\u9501\u76c8/\u4fdd\u672c\uff0c\u540c\u65f6\u6253\u5f00\u53cc\u5411\u624b\u7eed\u8d39\u8865\u507f\u3002",
            "\u659c\u7387\u53d8\u5f97\u66f4\u9641\u65f6\uff0c\u7b56\u7565\u4e0d\u6025\u4e8e\u7528\u6b7b\u76ee\u6807\u6b62\u76c8\u53bb\u63a7\u6b7b\uff0c\u800c\u662f\u7ed9\u7a7a\u5934\u6ce2\u6bb5\u66f4\u5927\u7684\u8dd1\u52a8\u7a7a\u95f4\u3002",
            "\u5f53\u5747\u7ebf\u659c\u7387\u8f6c\u6b63\u6216\u7ed3\u6784\u5931\u6548\u65f6\u518d\u9000\u573a\uff0c\u8fd9\u6837\u80fd\u8ba9\u5927\u8d8b\u52bf\u9636\u6bb5\u7684 R \u8fdb\u4e00\u6b65\u6269\u5f20\u3002",
        ),
    )


def _key_parameter_rows(spec: BundleSpec) -> tuple[tuple[str, str, str], ...]:
    config = spec.config
    common_rows = [
        ("inst_id", _render_value(config.inst_id), FIELD_DESCRIPTIONS["inst_id"]),
        ("bar", _render_value(config.bar), FIELD_DESCRIPTIONS["bar"]),
        ("strategy_id", _render_value(config.strategy_id), FIELD_DESCRIPTIONS["strategy_id"]),
        ("signal_mode", _render_value(config.signal_mode), FIELD_DESCRIPTIONS["signal_mode"]),
        ("ema_period", _render_value(config.ema_period), FIELD_DESCRIPTIONS["ema_period"]),
        ("ema_type", _render_value(config.ema_type), FIELD_DESCRIPTIONS["ema_type"]),
        ("trend_ema_period", _render_value(config.trend_ema_period), FIELD_DESCRIPTIONS["trend_ema_period"]),
        ("trend_ema_type", _render_value(config.trend_ema_type), FIELD_DESCRIPTIONS["trend_ema_type"]),
        ("atr_period", _render_value(config.atr_period), FIELD_DESCRIPTIONS["atr_period"]),
        ("atr_stop_multiplier", _render_value(config.atr_stop_multiplier), FIELD_DESCRIPTIONS["atr_stop_multiplier"]),
        ("take_profit_mode", _render_value(config.take_profit_mode), FIELD_DESCRIPTIONS["take_profit_mode"]),
        ("risk_amount", _render_value(config.risk_amount), FIELD_DESCRIPTIONS["risk_amount"]),
        ("daily_filter_enabled", _render_value(config.daily_filter_enabled), FIELD_DESCRIPTIONS["daily_filter_enabled"]),
    ]
    if spec.side == "\u505a\u591a":
        common_rows.extend(
            [
                (
                    "entry_reference_ema_period",
                    _render_value(config.resolved_entry_reference_ema_period()),
                    FIELD_DESCRIPTIONS["entry_reference_ema_period"],
                ),
                ("max_entries_per_trend", _render_value(config.max_entries_per_trend), FIELD_DESCRIPTIONS["max_entries_per_trend"]),
                (
                    "dynamic_break_even_trigger_r",
                    _render_value(config.dynamic_break_even_trigger_r),
                    FIELD_DESCRIPTIONS["dynamic_break_even_trigger_r"],
                ),
                (
                    "ema55_slope_lock_profit_trigger_r",
                    _render_value(config.ema55_slope_lock_profit_trigger_r),
                    FIELD_DESCRIPTIONS["ema55_slope_lock_profit_trigger_r"],
                ),
                (
                    "dynamic_fee_offset_enabled",
                    _render_value(config.dynamic_fee_offset_enabled),
                    FIELD_DESCRIPTIONS["dynamic_fee_offset_enabled"],
                ),
                (
                    "startup_chase_window_seconds",
                    _render_value(config.startup_chase_window_seconds),
                    FIELD_DESCRIPTIONS["startup_chase_window_seconds"],
                ),
            ]
        )
    else:
        common_rows.extend(
            [
                (
                    "trend_ema_slope_filter_min_ratio",
                    _render_value(config.trend_ema_slope_filter_min_ratio),
                    FIELD_DESCRIPTIONS["trend_ema_slope_filter_min_ratio"],
                ),
                (
                    "ema55_slope_negative_entry_bars",
                    _render_value(config.ema55_slope_negative_entry_bars),
                    FIELD_DESCRIPTIONS["ema55_slope_negative_entry_bars"],
                ),
                (
                    "ema55_slope_exit_enabled",
                    _render_value(config.ema55_slope_exit_enabled),
                    FIELD_DESCRIPTIONS["ema55_slope_exit_enabled"],
                ),
                (
                    "ema55_slope_lock_profit_trigger_r",
                    _render_value(config.ema55_slope_lock_profit_trigger_r),
                    FIELD_DESCRIPTIONS["ema55_slope_lock_profit_trigger_r"],
                ),
                (
                    "atr_percentile_filter_max",
                    _render_value(config.atr_percentile_filter_max),
                    FIELD_DESCRIPTIONS["atr_percentile_filter_max"],
                ),
            ]
        )
    return tuple(common_rows)


def _field_reference_rows(specs: tuple[BundleSpec, ...]) -> str:
    field_names: list[str] = []
    seen: set[str] = set()
    for spec in specs:
        for field_name in _config_dict(spec.config):
            if field_name in seen:
                continue
            seen.add(field_name)
            field_names.append(field_name)
    rows = []
    for field_name in field_names:
        description = FIELD_DESCRIPTIONS.get(field_name, "\u4fdd\u7559\u4e3a\u7edf\u4e00\u7b56\u7565\u914d\u7f6e\u5b57\u6bb5\uff0c\u672c\u5305\u76f4\u63a5\u6309 JSON \u539f\u503c\u6267\u884c\u3002")
        rows.append(
            "<tr>"
            f"<td><code>{_html_text(field_name)}</code></td>"
            f"<td>{_html_text(description)}</td>"
            "</tr>"
        )
    return "".join(rows)


def _strategy_cards_html(specs: tuple[BundleSpec, ...]) -> str:
    cards: list[str] = []
    for spec in specs:
        symbol_label = spec.symbol.replace("-USDT-SWAP", "")
        thesis_intro, thesis_points = _spec_thesis(spec)
        thesis_html = "".join(f"<li>{_html_text(item)}</li>" for item in thesis_points)
        param_rows = "".join(
            "<tr>"
            f"<td><code>{_html_text(name)}</code></td>"
            f"<td>{_html_text(value)}</td>"
            f"<td>{_html_text(desc)}</td>"
            "</tr>"
            for name, value, desc in _key_parameter_rows(spec)
        )
        cards.append(
            "<section class=\"strategy-card\">"
            f"<h3>{_html_text(spec.side)} / {_html_text(symbol_label)} / {_html_text(spec.strategy_label)}</h3>"
            f"<p>{_html_text(thesis_intro)}</p>"
            f"<p><strong>\u7b56\u7565\u7ed3\u6784</strong><br>{_html_text(spec.core_label)}</p>"
            f"<p><strong>\u4fdd\u62a4\u53e3\u5f84</strong><br>{_html_text(spec.protection_label)}</p>"
            f"<ul>{thesis_html}</ul>"
            "<table class=\"subtable\">"
            "<thead><tr><th>\u53c2\u6570\u540d</th><th>\u5f53\u524d\u503c</th><th>\u8bf4\u660e</th></tr></thead>"
            f"<tbody>{param_rows}</tbody>"
            "</table>"
            "<p><strong>\u5b8c\u6574\u53c2\u6570 JSON</strong></p>"
            f"<pre>{_html_text(_config_json(spec.config))}</pre>"
            "</section>"
        )
    return "".join(cards)


def _dynamic_r_explanation_html() -> str:
    items = (
        "前提：只有在“手续费偏移”开启时，下面这套口径才生效；关闭后就按裸 R 处理。",
        "保本触发R 不是价格刚到裸 2R / 3R 就立即触发，而是先满足对应 R，再额外覆盖双向手续费缓冲。",
        "例如保本触发R=2 且手续费偏移开启时，可以理解为：价格先到 2R + 双向手续费缓冲，然后止损抬到开仓价 + 双向手续费。",
        "移动止盈触发R 也是同一口径。比如移动止盈触发R=3 且手续费偏移开启时，可以理解为：价格先到 3R + 双向手续费缓冲，然后才进入首档锁盈。",
        "首档锁盈位和后续递进锁盈位，在手续费偏移开启时，也都是“锁盈R + 双向手续费缓冲”的止损落点。",
        "如果首档锁盈R=0（自动），则自动规则 = 移动止盈触发R - 移动步长R；例如触发R=3、步长R=1，则首档自动锁 2R，再叠加双向手续费缓冲。",
    )
    list_html = "".join(f"<li>{_html_text(item)}</li>" for item in items)
    return f"<div class=\"subcard\"><ul>{list_html}</ul></div>"


def build_dynamic_long_config(
    *,
    symbol: str,
    ema_period: int,
    trend_ema_period: int,
    entry_reference_ema_period: int,
    atr_stop_multiplier: Decimal,
    atr_take_multiplier: Decimal | None = None,
    trigger_r: int,
    max_entries_per_trend: int = 1,
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
        atr_take_multiplier=atr_take_multiplier if atr_take_multiplier is not None else atr_stop_multiplier * Decimal("2"),
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
        max_entries_per_trend=max_entries_per_trend,
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
            core_label="EMA21 / EMA55 / \u5165\u573a EMA55",
            protection_label="ATR10 / SL2 / 2R \u5f00\u542f nR \u4fdd\u672c / \u53cc\u5411\u624b\u7eed\u8d39 / \u6bcf\u6ce2 3 \u6b21",
            note="BTC \u8fd9\u4e00\u8f6e\u6539\u9009 R008\uff1a\u6536\u76ca\u4ec5\u7565\u4f4e\u4e8e R007\uff0c\u4f46\u56de\u64a4\u66f4\u4f4e\uff0c\u66f4\u9002\u5408\u5b9e\u76d8\u3002",
            config=build_dynamic_long_config(
                symbol="BTC-USDT-SWAP",
                ema_period=21,
                trend_ema_period=55,
                entry_reference_ema_period=55,
                atr_stop_multiplier=Decimal("2"),
                atr_take_multiplier=Decimal("2"),
                trigger_r=2,
                max_entries_per_trend=3,
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

    long_intro = _html_text(
        "\u53cc\u5747\u7ebf\u4ea4\u53c9\u53ea\u662f\u786e\u8ba4\u7ed3\u6784\u5f00\u59cb\uff0c\u771f\u6b63\u7684\u5165\u573a\u4e0d\u662f\u53bb\u8ffd\u5f53\u4e0b\u8fd9\u6839 K \u7ebf\uff0c\u800c\u662f\u56de\u5230\u6302\u5355\u53c2\u8003\u7ebf\u9644\u8fd1\u518d\u62ff\u66f4\u597d\u7684\u6210\u672c\u8fdb\u573a\u3002"
    )
    long_points = "".join(
        f"<li>{_html_text(item)}</li>"
        for item in (
            "\u6838\u5fc3\u524d\u63d0\u662f\u5feb\u7ebf > \u8d8b\u52bf\u7ebf\uff0c\u4e14\u6536\u76d8\u8981\u7ad9\u4e0a\u8d8b\u52bf\u7ebf\uff0c\u8fd9\u6837\u624d\u8bf4\u660e\u7ed3\u6784\u4e0d\u662f\u865a\u5047\u91d1\u53c9\u3002",
            "\u6302\u5355\u7ebf\u4e0d\u56fa\u5b9a\u4e00\u6839\uff0c\u800c\u662f\u6309\u5e01\u79cd\u9009\u62e9\u66f4\u9002\u5408\u56de\u8c03\u63a5\u529b\u7684 EMA \u7ebf\uff0c\u6bd4\u5982 BTC \u6302 EMA55\uff0cETH \u6302 EMA34\uff0cSOL \u6302 EMA13\u3002",
            "\u6b62\u76c8\u4e0d\u662f\u4e00\u53e3\u6c14\u5199\u6b7b\uff0c\u800c\u662f\u5148\u5230\u9996\u6863 R \u518d\u8fdb\u5165\u52a8\u6001\u4fdd\u672c\u4e0e\u9501\u76c8\uff0c\u5f3a\u52bf\u8d70\u52bf\u5c31\u8ba9\u5b83\u7ee7\u7eed\u8dd1\u3002",
            "\u8fd9\u5957\u591a\u5934\u7684\u4f18\u52bf\u662f\uff1a\u5165\u573a\u5c3d\u91cf\u9760\u56de\u8c03\uff0c\u521d\u59cb\u98ce\u9669\u53ef\u63a7\uff0c\u4f46\u76c8\u5229\u6bb5\u53ef\u4ee5\u4f9d\u9760\u8d8b\u52bf\u6269\u5f20\uff0c\u6574\u4f53\u76c8\u4e8f\u6bd4\u66f4\u5927\u3002",
        )
    )
    short_intro = _html_text(
        "\u7a7a\u5934\u4e0d\u518d\u4e3b\u6253\u56de\u62bd\u6302\u5355\uff0c\u800c\u662f\u5148\u9501\u5b9a\u5f53\u524d\u6700\u80fd\u4ee3\u8868\u4e0b\u8dcc\u8f68\u9053\u7684\u5747\u7ebf\uff0c\u53ea\u8981\u659c\u7387\u771f\u6b63\u5411\u4e0b\u8fbe\u6807\uff0c\u5c31\u76f4\u63a5\u8ddf\u7a7a\u3002"
    )
    short_points = "".join(
        f"<li>{_html_text(item)}</li>"
        for item in (
            "\u6bcf\u4e2a\u5e01\u79cd\u4e0d\u5f3a\u6c42\u7528\u540c\u4e00\u6761\u5747\u7ebf\uff0c\u800c\u662f\u9009\u5b9a\u66f4\u80fd\u53cd\u6620\u5176\u7a7a\u5934\u8282\u594f\u7684\u4e3b\u5bfc\u7ebf\uff0c\u4f8b\u5982 BTC EMA55\u3001ETH MA60\u3001SOL MA20\u3001DOGE MA21\u3002",
            "\u53ea\u8981\u4e3b\u5bfc\u7ebf\u659c\u7387\u6bd4\u4f8b\u5c0f\u4e8e\u7b49\u4e8e\u8bbe\u5b9a\u9608\u503c\uff0c\u5c31\u8ba4\u4e3a\u4e0b\u8dcc\u63a8\u8fdb\u8fd8\u5728\uff0c\u53ef\u4ee5\u76f4\u63a5\u505a\u5355\u3002",
            "\u6b62\u76c8\u4e5f\u4e0d\u662f\u56fa\u5b9a\u6b7b\u503c\uff0c\u800c\u662f\u5148\u7528 5R \u542f\u52a8\u52a8\u6001\u4fdd\u672c/\u9501\u76c8\uff0c\u7136\u540e\u7ee7\u7eed\u8ba9\u8d8b\u52bf\u81ea\u5df1\u6269\u5c55\u3002",
            "\u5f53\u659c\u7387\u53d8\u5f97\u66f4\u9641\u65f6\uff0c\u7b56\u7565\u5c31\u7ed9\u4e88\u66f4\u5927\u7684\u6ce2\u52a8\u7a7a\u95f4\uff1b\u53cd\u8fc7\u6765\uff0c\u659c\u7387\u8f6c\u6b63\u6216\u7ed3\u6784\u5931\u6548\u65f6\u518d\u9000\u573a\u3002",
        )
    )
    dynamic_r_explanation = _dynamic_r_explanation_html()
    log_rows = "".join(
        f"<li><strong>{_html_text(date_text)}</strong> { _html_text(message) }</li>"
        for date_text, message in UPDATE_LOGS
    )
    title = _html_text("\u6700\u4f73\u53c2\u6570\u7ec4\u5408\u5305\u8bf4\u660e")
    generated_at = _html_text(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    json_path = _html_text(str(JSON_PATH))
    summary_text = _html_text(f"\u672c\u6b21\u5b9a\u7a3f\uff1a{long_count} \u4e2a\u505a\u591a + {short_count} \u4e2a\u505a\u7a7a")
    intro_text = _html_text("\u6b64\u6587\u4ef6\u5bf9\u5e94\u7684 JSON \u7ec4\u5408\u5305\u53ef\u76f4\u63a5\u5bfc\u5165\u3002")
    generated_label = _html_text("\u751f\u6210\u65f6\u95f4")
    json_label = _html_text("JSON \u8def\u5f84")
    detail_label = _html_text("\u7b56\u7565\u660e\u7ec6")
    direction_label = _html_text("\u65b9\u5411")
    symbol_label = _html_text("\u5e01\u79cd")
    strategy_label = _html_text("\u7b56\u7565")
    core_label = _html_text("\u6838\u5fc3\u53c2\u6570")
    protection_label = _html_text("\u4fdd\u62a4\u903b\u8f91")
    note_label = _html_text("\u5907\u6ce8")
    idea_label = _html_text("\u7b56\u7565\u8bbe\u8ba1\u601d\u8def")
    dynamic_r_label = _html_text("\u52a8\u6001\u4fdd\u62a4 R \u53e3\u5f84")
    long_label = _html_text("EMA \u52a8\u6001\u59d4\u6258\u505a\u591a")
    short_label = _html_text("\u5747\u7ebf\u659c\u7387\u505a\u7a7a")
    log_label = _html_text("\u66f4\u65b0\u65e5\u5fd7")
    dynamic_r_intro = _html_text(
        "\u8fd9\u4e00\u8282\u4e13\u95e8\u89e3\u91ca\u52a8\u6001\u4fdd\u62a4\u91cc\u7684 R \u662f\u600e\u4e48\u7b97\u7684\uff0c\u5c24\u5176\u662f\u624b\u7eed\u8d39\u504f\u79fb\u5f00\u542f\u540e\uff0c\u89e6\u53d1 R \u548c\u9501\u76c8 R \u90fd\u4e0d\u662f\u88f8\u4ef7\u53e3\u5f84\u3002"
    )
    log_intro = _html_text(
        "\u65e5\u5fd7\u56fa\u5b9a\u7559\u5728\u6587\u6863\u6700\u672b\u5c3e\uff0c\u540e\u9762\u6bcf\u6b21\u6539\u5305\u3001\u8865\u8bf4\u660e\u3001\u5f00\u5173\u53d8\u66f4\u90fd\u4ece\u8fd9\u91cc\u8ffd\u8bb0\u3002"
    )
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
    .duo {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 16px;
    }}
    .subcard, .strategy-card {{
      background: rgba(255, 255, 255, 0.72);
      border: 1px solid rgba(220, 207, 183, 0.9);
      border-radius: 16px;
      padding: 18px 18px 16px;
      margin-top: 14px;
    }}
    h3 {{
      margin: 0 0 10px 0;
      color: #0b5d58;
      font-size: 18px;
    }}
    .subtable {{
      margin-top: 12px;
      font-size: 14px;
    }}
    code {{
      font-family: "Consolas", "Courier New", monospace;
      font-size: 13px;
      background: rgba(230, 244, 239, 0.7);
      padding: 1px 4px;
      border-radius: 4px;
    }}
    pre {{
      margin: 10px 0 0 0;
      padding: 14px;
      border-radius: 14px;
      border: 1px solid rgba(15,118,110,0.15);
      background: #f6fbf9;
      overflow-x: auto;
      font-size: 12px;
      line-height: 1.55;
      white-space: pre-wrap;
      word-break: break-word;
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
    </div>
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
  <section class="panel">
    <h2>{idea_label}</h2>
    <div class="duo">
      <div class="subcard">
        <h3>{long_label}</h3>
        <p>{long_intro}</p>
        <ul>{long_points}</ul>
      </div>
      <div class="subcard">
        <h3>{short_label}</h3>
        <p>{short_intro}</p>
        <ul>{short_points}</ul>
      </div>
    </div>
  </section>
  <section class="panel">
    <h2>{dynamic_r_label}</h2>
    <p>{dynamic_r_intro}</p>
    {dynamic_r_explanation}
  </section>
  <section class="panel">
    <h2>{log_label}</h2>
    <p>{log_intro}</p>
    <ul>{log_rows}</ul>
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
