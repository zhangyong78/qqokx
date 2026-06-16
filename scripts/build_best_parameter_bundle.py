from __future__ import annotations

import csv
import html
import json
import sys
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import okx_quant.backtest as backtest_module
from okx_quant.backtest import BacktestPeriodStat, BacktestResult, BacktestTrade, _build_period_stats, _run_backtest_with_loaded_data
from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path, backtest_history_file_path
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
from scripts.run_btc_daily_ma_direction_filter_research import (
    LONG_MAKER_FEE_RATE,
    LONG_TAKER_FEE_RATE,
    SHORT_TAKER_FEE_RATE,
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
BTC_LONG_CANDIDATE_SNAPSHOT_IDS = ("S484", "S485", "S486")
BTC_LONG_SELECTED_SNAPSHOT_ID = "S486"
BTC_LONG_RECENT_MONTH_START = "2025-01"
BUNDLE_INITIAL_CAPITAL = Decimal("10000")
LONG_SIDE = "做多"
SHORT_SIDE = "做空"


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


UPDATE_LOGS: tuple[tuple[str, str], ...] = (
    (
        "2026-06-15",
        "ETH / SOL / DOGE 动态委托做多已按本轮复核同步到参数包：ETH 改为 EMA21 / EMA55 / 挂单 EMA55 + SL1.5 / 1R保本 / 4R锁1R / 11R锁10R / 每波3次；SOL 改为 EMA21 / EMA55 / 挂单 EMA13 + SL1 / 3R保本 / 7R锁1R / 11R锁10R / 每波2次；DOGE 改为 EMA5 / EMA13 / 跟随 EMA5 + SL1 / 3R保本 / 6R锁1R / 11R锁10R / 每波2次。",
    ),
    (
        "2026-06-15",
        "DOGE 均线斜率做空定稿为 MA21 / MA21 + ATR13，保护口径切到 2R 保本、6R 锁 5R，之后每 1R 再上移 1R；该组全样本总盈亏 13222.6145、样本外(2022-01-01 之后)总盈亏 10141.4575，未见明显过拟合信号，并已同步到回测、实盘默认值与最佳参数组合包说明。",
    ),
    (
        "2026-06-15",
        "SOL 均线斜率做空沿用 MA20 主导线，但把默认保护从 ATR14 / 5R 锁 4R 调整为 ATR15 / 2R 保本 / 6R 锁 5R；该组全样本总盈亏 8956.6457、样本外(2022-01-01 之后)总盈亏 10540.1642，未见明显过拟合信号，并已同步到回测、实盘默认值与最佳参数组合包说明。",
    ),
    (
        "2026-06-15",
        "ETH 均线斜率做空定稿切换为 MA61 / MA61 + ATR11，保护口径定为 3R 保本、6R 锁 5R，之后每 1R 再上移 1R；该组在全样本与 2022 年以后独立样本外里都保持第一，并同步到回测、实盘默认值与最佳参数组合包说明。",
    ),
    (
        "2026-06-15",
        "BTC 均线斜率做空默认档位切换为归档 S573：EMA55 / EMA55，动态保护改为 9R 锁 8R + 双向手续费，之后每 1R 再上移 1R；并同步到回测、实盘默认值与最佳参数组合包。",
    ),
    (
        "2026-06-14",
        "BTC EMA 动态委托做多里的“达到 5R 后，若后续收盘跌破趋势 EMA55 则按收盘价平仓”已改为默认关闭；回测、实盘 UI、最佳参数包统一按关闭导出，需手动勾选才启用。",
    ),
    (
        "2026-06-14",
        "BTC EMA 动态委托做多最佳参数包补充独立特殊离场：达到 5R 后，若后续收盘跌破趋势 EMA55，则按收盘价平仓；该触发R与移动止盈触发R分开配置。",
    ),
    (
        "2026-06-14",
        "BTC EMA 动态委托做多在 S484 / S485 / S486 中选择归档 S486：每波最多 1 次，1R 保本 + 双向手续费，4R 锁 1R，11R 锁 10R；501 笔、51.90% 胜率、15576.7125 总盈亏、1503.3383 最大回撤。",
    ),
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
    "max_entries_per_trend": "单波趋势最多允许重复进场次数；本次 BTC 做多最佳参数 S486 设为 1，其余按各币种策略配置。",
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
    "trend_ema_close_exit_after_trigger_r_enabled": "做多特殊离场开关：达到指定 nR 后，若后续收盘跌破趋势 EMA，则按收盘价平仓。",
    "trend_ema_close_exit_after_trigger_r": "上面这个“跌破趋势 EMA 收盘平仓”特殊离场的触发 R；这是独立参数，不与“移动止盈触发R”共用。",
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


def _decimal_or_zero(value: object) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def _fmt_fixed(value: object, places: str = "0.0001") -> str:
    return str(_decimal_or_zero(value).quantize(Decimal(places)))


def _fmt_percent_value(value: object) -> str:
    return f"{_decimal_or_zero(value).quantize(Decimal('0.01'))}%"


def _fmt_bucket_win_rate(bucket: dict[str, object]) -> str:
    trades = int(bucket.get("trades", 0))
    if trades <= 0:
        return "0.00%"
    wins = int(bucket.get("wins", 0))
    rate = Decimal(wins) * Decimal("100") / Decimal(trades)
    return f"{rate.quantize(Decimal('0.01'))}%"


def _snapshot_records(snapshot_ids: tuple[str, ...]) -> dict[str, dict[str, object]]:
    path = backtest_history_file_path()
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    records = data.get("records", []) if isinstance(data, dict) else data
    return {
        str(record.get("snapshot_id")): record
        for record in records
        if isinstance(record, dict) and str(record.get("snapshot_id")) in snapshot_ids
    }


def _snapshot_operation_buckets(record: dict[str, object]) -> tuple[dict[str, dict[str, object]], dict[str, dict[str, object]]]:
    export_path = record.get("export_path")
    if not export_path:
        return {}, {}
    operations_path = Path(str(export_path)).with_suffix(".operations.csv")
    if not operations_path.exists():
        return {}, {}

    yearly: dict[str, dict[str, object]] = defaultdict(lambda: {"trades": 0, "wins": 0, "pnl": Decimal("0")})
    monthly: dict[str, dict[str, object]] = defaultdict(lambda: {"trades": 0, "wins": 0, "pnl": Decimal("0")})
    with operations_path.open("r", encoding="utf-8-sig", newline="") as file:
        for row in csv.DictReader(file):
            if row.get("action") != "exit":
                continue
            datetime_text = row.get("datetime", "")
            if len(datetime_text) < 7:
                continue
            pnl = _decimal_or_zero(row.get("pnl"))
            for bucket, key in ((yearly, datetime_text[:4]), (monthly, datetime_text[:7])):
                bucket[key]["trades"] = int(bucket[key]["trades"]) + 1
                bucket[key]["wins"] = int(bucket[key]["wins"]) + int(pnl > 0)
                bucket[key]["pnl"] = _decimal_or_zero(bucket[key]["pnl"]) + pnl
    return dict(yearly), dict(monthly)


def _bucket_sum(buckets: dict[str, dict[str, object]], start_key: str) -> dict[str, object]:
    total = {"trades": 0, "wins": 0, "pnl": Decimal("0")}
    for key, bucket in buckets.items():
        if key < start_key:
            continue
        total["trades"] = int(total["trades"]) + int(bucket.get("trades", 0))
        total["wins"] = int(total["wins"]) + int(bucket.get("wins", 0))
        total["pnl"] = _decimal_or_zero(total["pnl"]) + _decimal_or_zero(bucket.get("pnl"))
    return total


def _simple_table_html(headers: tuple[str, ...], rows: tuple[tuple[object, ...], ...]) -> str:
    head = "".join(f"<th>{_html_text(header)}</th>" for header in headers)
    body = "".join(
        "<tr>" + "".join(f"<td>{_html_text(str(cell))}</td>" for cell in row) + "</tr>"
        for row in rows
    )
    return f"<table class=\"subtable\"><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


@contextmanager
def _patched_dynamic_trigger_r(config: StrategyConfig):
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


def _coin_order(specs: tuple[BundleSpec, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for spec in specs:
        coin = spec.symbol.replace("-USDT-SWAP", "")
        if coin in seen:
            continue
        seen.add(coin)
        ordered.append(coin)
    return tuple(ordered)


def _run_bundle_backtests(specs: tuple[BundleSpec, ...]) -> tuple[BundleRun, ...]:
    client = OkxRestClient()
    candles_by_symbol: dict[str, list[object]] = {}
    instruments_by_symbol: dict[str, object] = {}
    runs: list[BundleRun] = []
    for spec in specs:
        config = replace(spec.config, environment="demo")
        candles = candles_by_symbol.get(spec.symbol)
        if candles is None:
            candles = [candle for candle in load_candle_cache(spec.symbol, config.bar, limit=None) if candle.confirmed]
            if not candles:
                raise RuntimeError(f"missing candles for {spec.symbol} {config.bar}")
            candles_by_symbol[spec.symbol] = candles
        instrument = instruments_by_symbol.get(spec.symbol)
        if instrument is None:
            instrument = client.get_instrument(spec.symbol)
            instruments_by_symbol[spec.symbol] = instrument
        fee_kwargs = (
            {
                "maker_fee_rate": LONG_MAKER_FEE_RATE,
                "taker_fee_rate": LONG_TAKER_FEE_RATE,
            }
            if spec.side == LONG_SIDE
            else {
                "taker_fee_rate": SHORT_TAKER_FEE_RATE,
            }
        )
        data_source_note = f"local candle_cache full history | {spec.symbol} | best parameter bundle"
        with _patched_dynamic_trigger_r(config):
            result = _run_backtest_with_loaded_data(
                candles,
                instrument,
                config,
                data_source_note=data_source_note,
                **fee_kwargs,
            )
        runs.append(BundleRun(spec=spec, result=result, data_source_note=data_source_note))
    return tuple(runs)


def _period_stat_map(trades: tuple[BacktestTrade, ...], *, by: str) -> dict[str, BacktestPeriodStat]:
    stats = _build_period_stats(list(trades), initial_capital=BUNDLE_INITIAL_CAPITAL, by=by)
    return {item.period_label: item for item in stats}


def _empty_period_row(period_label: str) -> tuple[object, ...]:
    return (
        period_label,
        0,
        "0.0000",
        0,
        "0.0000",
        0,
        "0.00%",
        "0.0000",
        "0.00%",
        "0.0000",
        "0.00%",
        "10000.0000",
    )


def _combined_period_rows(
    long_trades: tuple[BacktestTrade, ...],
    short_trades: tuple[BacktestTrade, ...],
    *,
    by: str,
) -> tuple[tuple[object, ...], ...]:
    total_trades = tuple(sorted((*long_trades, *short_trades), key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal)))
    total_map = _period_stat_map(total_trades, by=by)
    if not total_map:
        return ()
    long_map = _period_stat_map(long_trades, by=by)
    short_map = _period_stat_map(short_trades, by=by)
    rows: list[tuple[object, ...]] = []
    for period_label in sorted(total_map):
        total_stat = total_map[period_label]
        long_stat = long_map.get(period_label)
        short_stat = short_map.get(period_label)
        rows.append(
            (
                period_label,
                long_stat.trades if long_stat else 0,
                _fmt_fixed(long_stat.total_pnl if long_stat else Decimal("0")),
                short_stat.trades if short_stat else 0,
                _fmt_fixed(short_stat.total_pnl if short_stat else Decimal("0")),
                total_stat.trades,
                _fmt_percent_value(total_stat.win_rate),
                _fmt_fixed(total_stat.total_pnl),
                _fmt_percent_value(total_stat.return_pct),
                _fmt_fixed(total_stat.max_drawdown),
                _fmt_percent_value(total_stat.max_drawdown_pct),
                _fmt_fixed(total_stat.end_equity),
            )
        )
    return tuple(rows)


def _coin_period_tables_html(specs: tuple[BundleSpec, ...]) -> str:
    headers = (
        "周期",
        "多交易数",
        "多盈亏",
        "空交易数",
        "空盈亏",
        "合计交易数",
        "合计胜率",
        "合计盈亏",
        "收益率",
        "最大回撤",
        "回撤比例",
        "期末权益",
    )
    runs = _run_bundle_backtests(specs)
    run_map = {(run.coin, run.spec.side): run for run in runs}
    cards: list[str] = []
    for coin in _coin_order(specs):
        long_run = run_map[(coin, LONG_SIDE)]
        short_run = run_map[(coin, SHORT_SIDE)]
        yearly_rows = _combined_period_rows(tuple(long_run.result.trades), tuple(short_run.result.trades), by="year")
        monthly_rows = _combined_period_rows(tuple(long_run.result.trades), tuple(short_run.result.trades), by="month")
        cards.append(
            "<div class=\"strategy-card\">"
            f"<h3>{_html_text(f'{coin} 年度 / 月度统计')}</h3>"
            f"<details open><summary>{_html_text('年度统计')}</summary>{_simple_table_html(headers, yearly_rows or (_empty_period_row('无数据'),))}</details>"
            f"<details><summary>{_html_text('月度统计')}</summary>{_simple_table_html(headers, monthly_rows or (_empty_period_row('无数据'),))}</details>"
            "</div>"
        )

    long_all = tuple(trade for run in runs if run.spec.side == LONG_SIDE for trade in run.result.trades)
    short_all = tuple(trade for run in runs if run.spec.side == SHORT_SIDE for trade in run.result.trades)
    overall_source = runs[0].data_source_note if runs else "local candle_cache full history"
    cards.append(
        "<div class=\"strategy-card\">"
        f"<h3>{_html_text('全组合总表')}</h3>"
        f"<p>{_html_text('口径：固定风险 100U、初始资金 10000U、非复利；多/空列展示各方向交易数与盈亏，合计列按该币或全组合的合并资金曲线统计收益率、回撤与期末权益。')}</p>"
        f"<details open><summary>{_html_text('年度统计')}</summary>{_simple_table_html(headers, _combined_period_rows(long_all, short_all, by='year') or (_empty_period_row('无数据'),))}</details>"
        f"<details><summary>{_html_text('月度统计')}</summary>{_simple_table_html(headers, _combined_period_rows(long_all, short_all, by='month') or (_empty_period_row('无数据'),))}</details>"
        f"<p><small>{_html_text(f'数据来源：{overall_source}；手续费口径与最佳参数包回测默认值一致。')}</small></p>"
        "</div>"
    )
    return "".join(cards)


def _btc_long_performance_html() -> str:
    records = _snapshot_records(BTC_LONG_CANDIDATE_SNAPSHOT_IDS)
    compare_rows: list[tuple[object, ...]] = []
    verdicts = {
        "S484": "不采用：每波 3 次更激进，收益/回撤不如后两组。",
        "S485": "备选：较 S484 更好，但最大回撤没有下降。",
        "S486": "采用：总盈亏、PF、平均R最高，最大回撤最低，近两年合计亏损最小。",
    }
    for snapshot_id in BTC_LONG_CANDIDATE_SNAPSHOT_IDS:
        record = records.get(snapshot_id)
        if not record:
            compare_rows.append((snapshot_id, "未找到", "-", "-", "-", "-", "-", "-", "-", "未纳入比较"))
            continue
        config = record.get("config", {}) if isinstance(record.get("config"), dict) else {}
        report = record.get("report", {}) if isinstance(record.get("report"), dict) else {}
        _, monthly = _snapshot_operation_buckets(record)
        recent = _bucket_sum(monthly, BTC_LONG_RECENT_MONTH_START)
        compare_rows.append(
            (
                snapshot_id,
                config.get("max_entries_per_trend", "-"),
                report.get("total_trades", "-"),
                _fmt_percent_value(report.get("win_rate")),
                _fmt_fixed(report.get("total_pnl")),
                _fmt_fixed(report.get("max_drawdown")),
                _fmt_fixed(report.get("profit_factor")),
                _fmt_fixed(report.get("average_r_multiple")),
                _fmt_fixed(recent["pnl"]),
                verdicts[snapshot_id],
            )
        )

    selected = records.get(BTC_LONG_SELECTED_SNAPSHOT_ID)
    if not selected:
        return _simple_table_html(
            ("归档", "每波开仓", "交易数", "胜率", "总盈亏", "最大回撤", "PF", "平均R", "2025-2026 盈亏", "结论"),
            tuple(compare_rows),
        )

    yearly, monthly = _snapshot_operation_buckets(selected)
    yearly_rows = tuple(
        (
            year,
            bucket["trades"],
            _fmt_bucket_win_rate(bucket),
            _fmt_fixed(bucket["pnl"]),
        )
        for year, bucket in sorted(yearly.items())
    )
    monthly_rows = tuple(
        (
            month,
            bucket["trades"],
            _fmt_bucket_win_rate(bucket),
            _fmt_fixed(bucket["pnl"]),
        )
        for month, bucket in sorted(monthly.items())
        if month >= BTC_LONG_RECENT_MONTH_START
    )
    source_text = (
        "数据来源：backtest_history.json + "
        f"{Path(str(selected.get('export_path', ''))).with_suffix('.operations.csv')}"
    )
    return (
        f"<h3>{_html_text('S484 / S485 / S486 对比')}</h3>"
        + _simple_table_html(
            ("归档", "每波开仓", "交易数", "胜率", "总盈亏", "最大回撤", "PF", "平均R", "2025-2026 盈亏", "结论"),
            tuple(compare_rows),
        )
        + f"<h3>{_html_text('S486 年度表现')}</h3>"
        + _simple_table_html(("年份", "交易数", "胜率", "盈亏"), yearly_rows)
        + f"<h3>{_html_text('S486 2025-2026 月度表现')}</h3>"
        + _simple_table_html(("月份", "交易数", "胜率", "盈亏"), monthly_rows)
        + f"<p><small>{_html_text(source_text)}</small></p>"
    )


def _btc_short_usage_html() -> str:
    intro = (
        "BTC 均线斜率做空默认采用归档 S573。这一版的定位很明确："
        "不去等价格回抽挂单，而是直接基于 EMA55 斜率延续性做空，用 9R 锁 8R 的方式拉长下跌段的盈利。"
    )
    points = (
        "归档快照 S573；报告时间 2026-06-14 22:42:24；交易对 BTC-USDT-SWAP，1 小时，只做空。",
        "回测区间：2019-12-16 14:00 -> 2026-06-14 19:00；样本：56,934 根（全量）。",
        "主导线 EMA55；指标 EMA55 / EMA55 / ATR14；止损 2 ATR；止盈为动态保护。",
        "默认保护规则：9R 锁 8R + 双向手续费，之后每 1R 再上移 1R；同时保留“斜率转正平仓”这条失效退场规则。",
        "回测摘要：交易数 619；胜率 27.63%；总盈亏 8848.8247；最大回撤 2032.1350；PF 1.2672；平均R 0.1424。",
        "实盘建议：把它当作 BTC 空头的主基线就好，如果是作为多头补充模块，可以直接配到 S509 做多里一起用。",
    )
    yearly_rows = (
        ("2019", "1", "0.00%", "-62.9551"),
        ("2020", "79", "24.05%", "645.5076"),
        ("2021", "138", "29.71%", "2468.4209"),
        ("2022", "114", "28.07%", "2350.8274"),
        ("2023", "68", "16.18%", "-1165.3692"),
        ("2024", "92", "28.26%", "935.5094"),
        ("2025", "87", "32.18%", "833.8276"),
        ("2026", "40", "35.00%", "2843.0561"),
    )
    monthly_rows = (
        ("2024-07", "9", "11.11%", "351.5676"),
        ("2024-08", "13", "23.08%", "272.2349"),
        ("2024-09", "5", "40.00%", "63.7382"),
        ("2024-10", "4", "50.00%", "344.4523"),
        ("2024-11", "6", "33.33%", "-221.1978"),
        ("2024-12", "11", "18.18%", "-359.7235"),
        ("2025-01", "7", "28.57%", "-133.9187"),
        ("2025-02", "12", "16.67%", "110.6120"),
        ("2025-03", "11", "36.36%", "-65.4938"),
        ("2025-04", "9", "22.22%", "267.1845"),
        ("2025-05", "7", "0.00%", "-652.6154"),
        ("2025-06", "6", "50.00%", "-53.0300"),
        ("2025-07", "3", "0.00%", "-244.1050"),
        ("2025-08", "7", "57.14%", "191.6883"),
        ("2025-09", "2", "100.00%", "167.0381"),
        ("2025-10", "8", "37.50%", "940.0465"),
        ("2025-11", "7", "42.86%", "640.1648"),
        ("2025-12", "8", "37.50%", "-333.7437"),
        ("2026-01", "5", "40.00%", "567.6249"),
        ("2026-02", "15", "26.67%", "1099.9200"),
        ("2026-03", "7", "57.14%", "247.9236"),
        ("2026-04", "5", "0.00%", "-230.1740"),
        ("2026-05", "5", "40.00%", "45.0266"),
        ("2026-06", "3", "66.67%", "1112.7350"),
    )
    source_text = (
        "数据来源：backtest_history.json + "
        r"D:\qqokx_data\reports\backtest_exports\single_20260614_224216_ema55_slope_short_BTC-USDT-SWAP_1H_short_only.operations.csv"
    )
    points_html = "".join(f"<li>{_html_text(item)}</li>" for item in points)
    return (
        f"<p>{_html_text(intro)}</p>"
        f"<ul>{points_html}</ul>"
        f"<h3>{_html_text('S573 年度表现')}</h3>"
        + _simple_table_html(("年份", "交易数", "胜率", "盈亏"), yearly_rows)
        + f"<h3>{_html_text('S573 最近24个月度表现')}</h3>"
        + _simple_table_html(("月份", "交易数", "胜率", "盈亏"), monthly_rows)
        + f"<p><small>{_html_text(source_text)}</small></p>"
    )


def _btc_short_research_html() -> str:
    intro = (
        "本节整理 2026-06-15 针对 BTC `均线斜率做空` 的连续研究结论，全部以归档 S573 的同一回测口径复现："
        "`BTC-USDT-SWAP / 1H / 2019-12-16 14:00 -> 2026-06-14 19:00 / 全量K线 / 固定风险金 / 非复利 / maker 0.015% / taker 0.036%`。"
    )
    points = (
        "S573 原版：`ATR14 / SL2 / 9R锁8R后每1R移1R / 斜率转正平仓 / negative_entry_bars=1 / max_entries_per_trend=0 / 无日线过滤`。",
        "D 推演版：在 S573 基础上仅改 `negative_entry_bars=2`、`max_entries_per_trend=1`，其余全部保持一致。",
        "E 推演版：在 S573 基础上仅改 `max_entries_per_trend=1 + 日线过滤`；本次分别测试 `close_vs_ma` 与 `weak_day` 两种现有过滤模式。",
    )
    compare_rows = (
        ("S573", "基线", "619", "27.63%", "8848.8247", "1.2672", "0.1424", "10.75%", "当前 BTC 空头主基线。"),
        ("D", "`negBars=2` + `maxEntries=1`", "619", "27.63%", "8848.8247", "1.2672", "0.1424", "10.75%", "与 S573 完全一致。实测说明：这次样本里 `negative_entry_bars=2` 没有筛掉额外交易，且现有 `ema55_slope_short` 回测路径未实际用 `max_entries_per_trend` 限制开仓。"),
        ("E-close_vs_ma", "`maxEntries=1` + 日线 `EMA5 close_vs_ma` (`short_only`)", "483", "28.99%", "6243.0639", "1.2367", "0.1308", "14.53%", "单看空头监视更弱，但后面放回组合层会出现新结论。"),
        ("E-weak_day", "`maxEntries=1` + 日线 `weak_day` (`short_only`)", "497", "28.37%", "3987.4075", "1.1436", "0.0813", "18.45%", "较 close_vs_ma 更弱，可作为日线弱日规则不适合本策略的反例。"),
    )
    combo_rows = (
        ("S509 + S573", "0.70", "21770.8898", "1 / 68", "-89.3241", "在不换空头模块的前提下，这是 S573 最稳的权重点。"),
        ("S509 + D", "0.70", "21770.8898", "1 / 68", "-89.3241", "D 当前与 S573 完全等价，所以组合层结论也完全一致。"),
        ("S509 + E-close_vs_ma", "0.65", "19634.7040", "0 / 68", "378.2796", "最稳档：单看空头更弱，但放回 S509 组合后，12个月滚动窗口可以做到全部为正。"),
        ("S509 + E-close_vs_ma", "0.90", "21195.4700", "0 / 68", "55.7079", "更进攻的稳定档：总盈亏更高，但安全垫变薄。"),
        ("S509 + E-weak_day", "0.80", "18766.6384", "1 / 68", "-118.5737", "能比单多好很多，但仍弱于 S573 组合和 E-close_vs_ma 组合。"),
    )
    ratio_rows = (
        ("`Long 1.0 + S573 0.7`", "21770.8898", "634.7438", "-1169.7395", "-89.3241", "更均衡，是现在 S573 组合下的稳定点。"),
        ("`Long 1.0 + S573 1.0`", "24425.5372", "721.7301", "-1327.7216", "-450.1324", "总盈亏多 `+2654.6474`，但波动更大，12个月坏窗口明显加深。"),
    )
    takeaways = (
        "如果你要的是“空头单策略默认档位”，继续用 S573。",
        "如果你要的是“S509 做多 + BTC 空头”的最稳组合，优先看 `Long 1.0 + E-close_vs_ma 0.65`。",
        "如果你只想用最简单的补充方式，`S509 + S573 1:1` 可以直接用，但要接受“总盈亏更高、坏窗口也更深”这个后果。",
    )
    source_text = "数据来源：backtest_history.json + S509 / S573 operations.csv + 2026-06-15 本地复现的 D / E 回测交易明细。"
    points_html = "".join(f"<li>{_html_text(item)}</li>" for item in points)
    takeaways_html = "".join(f"<li>{_html_text(item)}</li>" for item in takeaways)
    return (
        f"<p>{_html_text(intro)}</p>"
        f"<ul>{points_html}</ul>"
        f"<h3>{_html_text('S573 / D / E 同口径回测结果')}</h3>"
        + _simple_table_html(("版本", "参数差异", "交易数", "胜率", "总盈亏", "PF", "平均R", "最大回撤", "结论"), compare_rows)
        + f"<p>{_html_text('本轮结论要分成两层看：如果只看 BTC 空头单策略，S573 仍是默认主基线；如果把空头放回 S509 做多模块里组合，最稳的结论会发生变化。')}</p>"
        + f"<h3>{_html_text('S509 + 空头模块横向比较')}</h3>"
        + _simple_table_html(("组合方案", "推荐权重", "总盈亏", "12个月亏损窗口", "最差12个月", "组合解读"), combo_rows)
        + f"<h3>{_html_text('S573 0.7 与 1:1 的进攻代价')}</h3>"
        + _simple_table_html(("配比", "总盈亏", "月度波动", "最差单月", "最差12个月", "后果"), ratio_rows)
        + f"<ul>{takeaways_html}</ul>"
        + f"<p><small>{_html_text(source_text)}</small></p>"
    )


def _eth_short_research_html() -> str:
    intro = (
        "本节整理 2026-06-15 针对 ETH `均线斜率做空` 的定稿研究。全部测试统一采用本地全量 K 线，"
        "固定风险金、手续费、初始资金完全沿用归档 S582 的口径，只比较参数组合本身的差异。"
    )
    points = (
        "研究起点没有脱离交易直觉，先围绕你更偏好的 `MA60 / ATR10` 展开，再把归档 S582-S594 的思路拆成三部分复核：主导均线、ATR 周期、保本/锁盈节奏。",
        "最终定稿为 `MA61 / MA61 / ATR11 / SL2 / 3R保本 / 6R锁5R / 之后每1R再上移1R / 斜率转正平仓`，并已同步到回测、UI 默认值和实盘默认值。",
        "这里的“斜率转正平仓”看的不是趋势均线，而是信号均线，也就是左侧那条快线参数；当前 ETH 定稿里两条都设为 `MA61`，所以体感上看不出差别。",
        "对“冷门参数是否过拟合”的结论是：这次不是去追很远的怪值，而是在 `MA60 / ATR10` 这个直觉锚点旁边只做了 `+1 / +1` 的微调；之所以最终选 `MA61 / ATR11`，是因为它在全样本和 2022 年后的独立样本外都同时排第一，而不是只在某一小段里最好。",
        "如果只是想快速验实盘链路，可以先上模拟盘小周期参数；更合适的快检组合是 `15m / MA34 / MA34 / ATR7 / 1R保本 / 3R锁2R`，它用于验证开仓、保本、锁盈、斜率离场是否按预期触发，不用于判断长期收益能力。",
    )
    result_rows = (
        ("全样本", "总盈亏 12096.8311 / 最大回撤 2613.5908", "在候选组合里保持第一。"),
        ("2022年后独立样本外", "总盈亏 12510.9335", "样本外仍保持第一。"),
        ("最终默认档", "MA61 / ATR11 / 3R保本 / 6R锁5R", "作为 ETH 空头默认参数，统一用于回测、UI 和实盘。"),
    )
    source_text = "研究口径：S582 同风险、同手续费、同初始资金，样本来源为本地全量 K 线；本节只总结定稿结论，详细逐组回测记录仍以归档研究日志为准。"
    points_html = "".join(f"<li>{_html_text(item)}</li>" for item in points)
    return (
        f"<p>{_html_text(intro)}</p>"
        f"<ul>{points_html}</ul>"
        f"<h3>{_html_text('本轮定稿结果')}</h3>"
        + _simple_table_html(("维度", "结果", "结论"), result_rows)
        + f"<p><small>{_html_text(source_text)}</small></p>"
    )


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
                (
                    f"\u7279\u6b8a\u79bb\u573a\uff1a\u5229\u6da6\u81f3\u5c11\u8d70\u5230 {config.trend_ema_close_exit_after_trigger_r}R \u540e\uff0c"
                    f"\u82e5\u540e\u7eed\u6536\u76d8\u8dcc\u7834 {trend_line}\uff0c\u5219\u76f4\u63a5\u6309\u6536\u76d8\u79bb\u573a\u3002"
                    if bool(config.trend_ema_close_exit_after_trigger_r_enabled)
                    else f"\u7279\u6b8a\u79bb\u573a\uff1a\u672c\u7ec4\u5408\u672a\u542f\u7528\u201c\u8dcc\u7834 {trend_line} \u6536\u76d8\u79bb\u573a\u201d\u3002"
                ),
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
                    "trend_ema_close_exit_after_trigger_r_enabled",
                    _render_value(config.trend_ema_close_exit_after_trigger_r_enabled),
                    FIELD_DESCRIPTIONS["trend_ema_close_exit_after_trigger_r_enabled"],
                ),
                (
                    "trend_ema_close_exit_after_trigger_r",
                    _render_value(config.trend_ema_close_exit_after_trigger_r),
                    FIELD_DESCRIPTIONS["trend_ema_close_exit_after_trigger_r"],
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
    dynamic_break_even_trigger_r: int = 2,
    dynamic_first_lock_r: int = 0,
    dynamic_trailing_step_r: int = 1,
    dynamic_protection_rules: tuple[dict[str, object], ...] | None = None,
    trend_ema_close_exit_after_trigger_r_enabled: bool = False,
    trend_ema_close_exit_after_trigger_r: int = 5,
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
        dynamic_break_even_trigger_r=dynamic_break_even_trigger_r,
        dynamic_fee_offset_enabled=True,
        dynamic_protection_rules=tuple(dynamic_protection_rules or ()),
        ema55_slope_lock_profit_trigger_r=trigger_r,
        dynamic_first_lock_r=dynamic_first_lock_r,
        dynamic_trailing_step_r=dynamic_trailing_step_r,
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=0,
        trend_ema_close_exit_after_trigger_r_enabled=trend_ema_close_exit_after_trigger_r_enabled,
        trend_ema_close_exit_after_trigger_r=trend_ema_close_exit_after_trigger_r,
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
    atr_period: int = 14,
    dynamic_break_even_trigger_r: int = 2,
    dynamic_first_lock_r: int = 0,
    dynamic_trailing_step_r: int = 1,
    dynamic_protection_rules: tuple[dict[str, object], ...] | None = None,
    ema55_slope_lock_profit_trigger_r: int = 5,
    time_stop_break_even_bars: int = 10,
    daily_filter_boundary: str = "bjt_08",
    daily_filter_scope: str = "short_only",
    daily_filter_period: int = 21,
) -> StrategyConfig:
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
        environment="live",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
        risk_amount=Decimal("100"),
        backtest_initial_capital=Decimal("10000"),
        backtest_sizing_mode="fixed_risk",
        take_profit_mode="dynamic",
        dynamic_two_r_break_even=True,
        dynamic_break_even_trigger_r=dynamic_break_even_trigger_r,
        dynamic_fee_offset_enabled=True,
        dynamic_protection_rules=tuple(dynamic_protection_rules or ()),
        ema55_slope_exit_enabled=True,
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
        tp_sl_mode="exchange",
        run_mode="trade",
        entry_side_mode="follow_signal",
        daily_filter_enabled=False,
        daily_filter_boundary=daily_filter_boundary,
        daily_filter_mode="disabled",
        daily_filter_scope=daily_filter_scope,
        daily_filter_ma_type="ema",
        daily_filter_period=daily_filter_period,
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
            protection_label="ATR10 / SL2 / 1R \u4fdd\u672c + \u53cc\u5411\u624b\u7eed\u8d39 / 4R \u9501 1R + \u53cc\u5411\u624b\u7eed\u8d39\uff0c\u4e4b\u540e\u6bcf 1R \u4e0a\u79fb 1R / 11R \u9501 10R + \u53cc\u5411\u624b\u7eed\u8d39\uff0c\u4e4b\u540e\u6bcf 1R \u518d\u4e0a\u79fb 1R / \u6bcf\u6ce2 1 \u6b21",
            note="BTC \u591a\u5934\u6539\u4e3a S486\uff1a501 \u7b14\u3001\u80dc\u7387 51.90%\u3001\u603b\u76c8\u4e8f 15576.7125\u3001\u6700\u5927\u56de\u64a4 1503.3383\uff1b\u56de\u6d4b\u533a\u95f4 2019-12-16 14:00 \u81f3 2026-06-14 11:00\uff1b\u76f8\u6bd4 S484/S485\uff0c\u603b\u76c8\u4e8f\u66f4\u9ad8\u3001\u56de\u64a4\u66f4\u4f4e\uff0c\u4f46\u4ecd\u53ea\u5efa\u8bae\u5728 BTC \u8d8b\u52bf\u73af\u5883\u4e2d\u542f\u7528\uff0c\u9700\u914d\u5408\u964d\u4ed3/\u6682\u505c\u7eaa\u5f8b\u3002",
            config=build_dynamic_long_config(
                symbol="BTC-USDT-SWAP",
                ema_period=21,
                trend_ema_period=55,
                entry_reference_ema_period=55,
                atr_stop_multiplier=Decimal("2"),
                atr_take_multiplier=Decimal("2"),
                trigger_r=4,
                max_entries_per_trend=1,
                dynamic_break_even_trigger_r=1,
                dynamic_first_lock_r=1,
                dynamic_trailing_step_r=1,
                trend_ema_close_exit_after_trigger_r_enabled=False,
                trend_ema_close_exit_after_trigger_r=5,
                dynamic_protection_rules=(
                    {
                        "trigger_r": 1,
                        "action": "break_even",
                        "lock_r": None,
                        "trail_mode": "none",
                        "trail_every_r": None,
                        "trail_add_r": None,
                    },
                    {
                        "trigger_r": 4,
                        "action": "lock_profit",
                        "lock_r": 1,
                        "trail_mode": "step",
                        "trail_every_r": 1,
                        "trail_add_r": 1,
                    },
                    {
                        "trigger_r": 11,
                        "action": "lock_profit",
                        "lock_r": 10,
                        "trail_mode": "step",
                        "trail_every_r": 1,
                        "trail_add_r": 1,
                    },
                ),
            ),
        ),
        BundleSpec(
            side="\u505a\u591a",
            symbol="ETH-USDT-SWAP",
            profile_id="dynamic_long_best_eth_v2",
            profile_name="ETH \u52a8\u6001\u59d4\u6258\u505a\u591a \u6700\u4f73\u53c2\u6570",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            strategy_label="EMA \u52a8\u6001\u59d4\u6258\u505a\u591a",
            core_label="EMA21 / EMA55 / \u5165\u573a EMA55",
            protection_label="ATR10 / SL1.5 / 1R \u4fdd\u672c + \u53cc\u5411\u624b\u7eed\u8d39 / 4R \u9501 1R + \u53cc\u5411\u624b\u7eed\u8d39\uff0c\u4e4b\u540e\u6bcf 1R \u4e0a\u79fb 1R / 11R \u9501 10R + \u53cc\u5411\u624b\u7eed\u8d39\uff0c\u4e4b\u540e\u6bcf 1R \u518d\u4e0a\u79fb 1R / \u6bcf\u6ce2 3 \u6b21",
            note="ETH \u591a\u5934\u5b9a\u7a3f\u4e3a EMA21 / EMA55 / EMA55 + SL1.5 1R\u4fdd\u672c 4R\u95011R 11R\u950110R\uff1a\u5168\u6837\u672c\u603b\u76c8\u4e8f 14458.4975\u3001\u6700\u5927\u56de\u64a4 4935.7173\uff0c2022 \u5e74\u4ee5\u540e\u72ec\u7acb\u6837\u672c\u5916\u603b\u76c8\u4e8f 6758.1374\uff1b\u5bf9\u6bd4 EMA34 \u6302\u5355\u7ebf\uff0cEMA55 \u5728\u76c8\u4e8f\u4e0e PF \u4e0a\u66f4\u4f18\u3002",
            config=build_dynamic_long_config(
                symbol="ETH-USDT-SWAP",
                ema_period=21,
                trend_ema_period=55,
                entry_reference_ema_period=55,
                atr_stop_multiplier=Decimal("1.5"),
                atr_take_multiplier=Decimal("1.5"),
                trigger_r=4,
                max_entries_per_trend=3,
                dynamic_break_even_trigger_r=1,
                dynamic_first_lock_r=1,
                dynamic_trailing_step_r=1,
                dynamic_protection_rules=(
                    {
                        "trigger_r": 1,
                        "action": "break_even",
                        "lock_r": None,
                        "trail_mode": "none",
                        "trail_every_r": None,
                        "trail_add_r": None,
                    },
                    {
                        "trigger_r": 4,
                        "action": "lock_profit",
                        "lock_r": 1,
                        "trail_mode": "step",
                        "trail_every_r": 1,
                        "trail_add_r": 1,
                    },
                    {
                        "trigger_r": 11,
                        "action": "lock_profit",
                        "lock_r": 10,
                        "trail_mode": "step",
                        "trail_every_r": 1,
                        "trail_add_r": 1,
                    },
                ),
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
            protection_label="ATR10 / SL1 / 3R \u4fdd\u672c + \u53cc\u5411\u624b\u7eed\u8d39 / 7R \u9501 1R + \u53cc\u5411\u624b\u7eed\u8d39\uff0c\u4e4b\u540e\u6bcf 1R \u4e0a\u79fb 1R / 11R \u9501 10R + \u53cc\u5411\u624b\u7eed\u8d39\uff0c\u4e4b\u540e\u6bcf 1R \u518d\u4e0a\u79fb 1R / \u6bcf\u6ce2 2 \u6b21",
            note="SOL \u591a\u5934\u5b9a\u7a3f\u4e3a EMA21 / EMA55 / EMA13 + SL1 3R\u4fdd\u672c 7R\u95011R 11R\u950110R\uff1a\u5168\u6837\u672c\u603b\u76c8\u4e8f 27565.8921\u3001\u6700\u5927\u56de\u64a4 5786.5470\uff0c2022 \u5e74\u4ee5\u540e\u72ec\u7acb\u6837\u672c\u5916\u603b\u76c8\u4e8f 19200.8290\uff1b\u6536\u76ca\u5f39\u6027\u663e\u8457\u5f3a\u4e8e\u6302 EMA55\uff0c\u4f46\u98ce\u9669\u66f4\u6fc0\u8fdb\u3002",
            config=build_dynamic_long_config(
                symbol="SOL-USDT-SWAP",
                ema_period=21,
                trend_ema_period=55,
                entry_reference_ema_period=13,
                atr_stop_multiplier=Decimal("1"),
                atr_take_multiplier=Decimal("1"),
                trigger_r=7,
                max_entries_per_trend=2,
                dynamic_break_even_trigger_r=3,
                dynamic_first_lock_r=1,
                dynamic_trailing_step_r=1,
                dynamic_protection_rules=(
                    {
                        "trigger_r": 3,
                        "action": "break_even",
                        "lock_r": None,
                        "trail_mode": "none",
                        "trail_every_r": None,
                        "trail_add_r": None,
                    },
                    {
                        "trigger_r": 7,
                        "action": "lock_profit",
                        "lock_r": 1,
                        "trail_mode": "step",
                        "trail_every_r": 1,
                        "trail_add_r": 1,
                    },
                    {
                        "trigger_r": 11,
                        "action": "lock_profit",
                        "lock_r": 10,
                        "trail_mode": "step",
                        "trail_every_r": 1,
                        "trail_add_r": 1,
                    },
                ),
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
            protection_label="ATR10 / SL1 / 3R \u4fdd\u672c + \u53cc\u5411\u624b\u7eed\u8d39 / 6R \u9501 1R + \u53cc\u5411\u624b\u7eed\u8d39\uff0c\u4e4b\u540e\u6bcf 1R \u4e0a\u79fb 1R / 11R \u9501 10R + \u53cc\u5411\u624b\u7eed\u8d39\uff0c\u4e4b\u540e\u6bcf 1R \u518d\u4e0a\u79fb 1R / \u6bcf\u6ce2 2 \u6b21",
            note="DOGE \u591a\u5934\u5b9a\u7a3f\u4e3a EMA5 / EMA13 / \u8ddf\u968f EMA5 + SL1 3R\u4fdd\u672c 6R\u95011R 11R\u950110R\uff1a\u5168\u6837\u672c\u603b\u76c8\u4e8f 26014.5449\u3001\u6700\u5927\u56de\u64a4 8798.2647\uff0c2022 \u5e74\u4ee5\u540e\u72ec\u7acb\u6837\u672c\u5916\u603b\u76c8\u4e8f 16415.2575\uff1b\u76f8\u6bd4\u6302 EMA55 \u83b7\u5f97\u66f4\u5f3a\u6536\u76ca\u5f39\u6027\uff0c\u4f46\u4ea4\u6613\u6b21\u6570\u4e0e\u56de\u64a4\u4e5f\u66f4\u9ad8\u3002",
            config=build_dynamic_long_config(
                symbol="DOGE-USDT-SWAP",
                ema_period=5,
                trend_ema_period=13,
                entry_reference_ema_period=0,
                atr_stop_multiplier=Decimal("1"),
                atr_take_multiplier=Decimal("1"),
                trigger_r=6,
                max_entries_per_trend=2,
                dynamic_break_even_trigger_r=3,
                dynamic_first_lock_r=1,
                dynamic_trailing_step_r=1,
                dynamic_protection_rules=(
                    {
                        "trigger_r": 3,
                        "action": "break_even",
                        "lock_r": None,
                        "trail_mode": "none",
                        "trail_every_r": None,
                        "trail_add_r": None,
                    },
                    {
                        "trigger_r": 6,
                        "action": "lock_profit",
                        "lock_r": 1,
                        "trail_mode": "step",
                        "trail_every_r": 1,
                        "trail_add_r": 1,
                    },
                    {
                        "trigger_r": 11,
                        "action": "lock_profit",
                        "lock_r": 10,
                        "trail_mode": "step",
                        "trail_every_r": 1,
                        "trail_add_r": 1,
                    },
                ),
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
            protection_label="ATR14 / SL2 / 9R \u9501 8R + \u53cc\u5411\u624b\u7eed\u8d39\uff0c\u4e4b\u540e\u6bcf 1R \u518d\u4e0a\u79fb 1R / \u659c\u7387\u8f6c\u6b63\u5e73\u4ed3",
            note="BTC \u7a7a\u5934\u6539\u4e3a S573\uff1a619 \u7b14\u3001\u80dc\u7387 27.63%\u3001\u603b\u76c8\u4e8f 8848.8247\u3001\u6700\u5927\u56de\u64a4 2032.1350\uff1b\u56de\u6d4b\u533a\u95f4 2019-12-16 14:00 \u81f3 2026-06-14 19:00\u3002",
            config=build_slope_short_config(
                symbol="BTC-USDT-SWAP",
                ema_period=55,
                ema_type="ema",
                trend_ema_period=55,
                trend_ema_type="ema",
                dynamic_break_even_trigger_r=9,
                dynamic_first_lock_r=8,
                dynamic_trailing_step_r=1,
                dynamic_protection_rules=(
                    {
                        "trigger_r": 9,
                        "action": "lock_profit",
                        "lock_r": 8,
                        "trail_mode": "step",
                        "trail_every_r": 1,
                        "trail_add_r": 1,
                    },
                ),
                ema55_slope_lock_profit_trigger_r=9,
                time_stop_break_even_bars=0,
                daily_filter_boundary="exchange",
                daily_filter_scope="both",
                daily_filter_period=5,
            ),
        ),
        BundleSpec(
            side="\u505a\u7a7a",
            symbol="ETH-USDT-SWAP",
            profile_id="slope_short_best_eth_v2",
            profile_name="ETH \u5747\u7ebf\u659c\u7387\u505a\u7a7a \u6700\u4f73\u53c2\u6570",
            strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
            strategy_label="\u5747\u7ebf\u659c\u7387\u505a\u7a7a",
            core_label="MA61 / MA61",
            protection_label="ATR11 / SL2 / 3R \u4fdd\u672c + \u53cc\u5411\u624b\u7eed\u8d39 / 6R \u9501 5R + \u53cc\u5411\u624b\u7eed\u8d39\uff0c\u4e4b\u540e\u6bcf 1R \u518d\u4e0a\u79fb 1R / \u659c\u7387\u8f6c\u6b63\u5e73\u4ed3",
            note="ETH \u7a7a\u5934\u5b9a\u7a3f\u4e3a MA61 ATR11 3R\u4fdd\u672c 6R\u95015R\uff1a\u5168\u6837\u672c\u603b\u76c8\u4e8f 12096.8311\uff0c\u6700\u5927\u56de\u64a4 2613.5908\uff0c2022 \u5e74\u4ee5\u540e\u72ec\u7acb\u6837\u672c\u5916\u603b\u76c8\u4e8f 12510.9335\uff1b\u76f8\u6bd4 MA60 ATR10 \u66f4\u9002\u5408\u4f5c\u4e3a\u589e\u5f3a\u7248\u9ed8\u8ba4\u53c2\u6570\u3002",
            config=build_slope_short_config(
                symbol="ETH-USDT-SWAP",
                ema_period=61,
                ema_type="ma",
                trend_ema_period=61,
                trend_ema_type="ma",
                atr_period=11,
                dynamic_break_even_trigger_r=3,
                dynamic_first_lock_r=5,
                dynamic_protection_rules=(
                    {
                        "trigger_r": 3,
                        "action": "break_even",
                        "lock_r": None,
                        "trail_mode": "none",
                        "trail_every_r": None,
                        "trail_add_r": None,
                    },
                    {
                        "trigger_r": 6,
                        "action": "lock_profit",
                        "lock_r": 5,
                        "trail_mode": "step",
                        "trail_every_r": 1,
                        "trail_add_r": 1,
                    },
                ),
                ema55_slope_lock_profit_trigger_r=6,
                time_stop_break_even_bars=0,
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
            protection_label="ATR15 / SL2 / 2R \u4fdd\u672c + \u53cc\u5411\u624b\u7eed\u8d39 / 6R \u9501 5R + \u53cc\u5411\u624b\u7eed\u8d39\uff0c\u4e4b\u540e\u6bcf 1R \u518d\u4e0a\u79fb 1R / \u659c\u7387\u8f6c\u6b63\u5e73\u4ed3",
            note="SOL \u7a7a\u5934\u5b9a\u7a3f\u4e3a MA20 ATR15 2R\u4fdd\u672c 6R\u95015R\uff1a\u5168\u6837\u672c\u603b\u76c8\u4e8f 8956.6457\uff0c\u6700\u5927\u56de\u64a4 3570.6972\uff0c2022 \u5e74\u4ee5\u540e\u72ec\u7acb\u6837\u672c\u5916\u603b\u76c8\u4e8f 10540.1642\uff1bMA20 \u76f8\u5bf9 EMA55 \u4ecd\u6709\u66f4\u9ad8\u7684\u603b\u76c8\u4e8f\uff0cATR15 \u6bd4 ATR14 \u66f4\u987a\u6ed1\u3002",
            config=build_slope_short_config(
                symbol="SOL-USDT-SWAP",
                ema_period=20,
                ema_type="ma",
                trend_ema_period=20,
                trend_ema_type="ma",
                atr_period=15,
                dynamic_break_even_trigger_r=2,
                dynamic_first_lock_r=5,
                dynamic_protection_rules=(
                    {
                        "trigger_r": 2,
                        "action": "break_even",
                        "lock_r": None,
                        "trail_mode": "none",
                        "trail_every_r": None,
                        "trail_add_r": None,
                    },
                    {
                        "trigger_r": 6,
                        "action": "lock_profit",
                        "lock_r": 5,
                        "trail_mode": "step",
                        "trail_every_r": 1,
                        "trail_add_r": 1,
                    },
                ),
                ema55_slope_lock_profit_trigger_r=6,
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
            protection_label="ATR13 / SL2 / 2R \u4fdd\u672c + \u53cc\u5411\u624b\u7eed\u8d39 / 6R \u9501 5R + \u53cc\u5411\u624b\u7eed\u8d39\uff0c\u4e4b\u540e\u6bcf 1R \u518d\u4e0a\u79fb 1R / \u659c\u7387\u8f6c\u6b63\u5e73\u4ed3",
            note="DOGE \u7a7a\u5934\u5b9a\u7a3f\u4e3a MA21 ATR13 2R\u4fdd\u672c 6R\u9501 5R\uff1a\u5168\u6837\u672c\u603b\u76c8\u4e8f 13222.6145\uff0c\u6700\u5927\u56de\u64a4 2752.9087\uff0c2022 \u5e74\u4ee5\u540e\u72ec\u7acb\u6837\u672c\u5916\u603b\u76c8\u4e8f 10141.4575\uff1b\u76f8\u6bd4 MA55\uff0cMA21 \u603b\u76c8\u4e8f\u66f4\u9ad8\uff0cATR13 \u6bd4 ATR14 \u66f4\u4f18\u3002",
            config=build_slope_short_config(
                symbol="DOGE-USDT-SWAP",
                ema_period=21,
                ema_type="ma",
                trend_ema_period=21,
                trend_ema_type="ma",
                atr_period=13,
                dynamic_break_even_trigger_r=2,
                dynamic_first_lock_r=5,
                dynamic_protection_rules=(
                    {
                        "trigger_r": 2,
                        "action": "break_even",
                        "lock_r": None,
                        "trail_mode": "none",
                        "trail_every_r": None,
                        "trail_add_r": None,
                    },
                    {
                        "trigger_r": 6,
                        "action": "lock_profit",
                        "lock_r": 5,
                        "trail_mode": "step",
                        "trail_every_r": 1,
                        "trail_add_r": 1,
                    },
                ),
                ema55_slope_lock_profit_trigger_r=6,
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
            "\u6302\u5355\u7ebf\u4e0d\u56fa\u5b9a\u4e00\u6839\uff0c\u800c\u662f\u6309\u5e01\u79cd\u9009\u62e9\u66f4\u9002\u5408\u56de\u8c03\u63a5\u529b\u7684 EMA \u7ebf\uff0c\u6bd4\u5982 BTC \u6302 EMA55\uff0cETH \u6302 EMA55\uff0cSOL \u6302 EMA13\uff0cDOGE \u8ddf\u968f EMA5\u3002",
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
            "\u6bcf\u4e2a\u5e01\u79cd\u4e0d\u5f3a\u6c42\u7528\u540c\u4e00\u6761\u5747\u7ebf\uff0c\u800c\u662f\u9009\u5b9a\u66f4\u80fd\u53cd\u6620\u5176\u7a7a\u5934\u8282\u594f\u7684\u4e3b\u5bfc\u7ebf\uff0c\u4f8b\u5982 BTC EMA55\u3001ETH MA61\u3001SOL MA20\u3001DOGE MA21\u3002",
            "\u53ea\u8981\u4e3b\u5bfc\u7ebf\u659c\u7387\u6bd4\u4f8b\u5c0f\u4e8e\u7b49\u4e8e\u8bbe\u5b9a\u9608\u503c\uff0c\u5c31\u8ba4\u4e3a\u4e0b\u8dcc\u63a8\u8fdb\u8fd8\u5728\uff0c\u53ef\u4ee5\u76f4\u63a5\u505a\u5355\u3002",
            "\u6b62\u76c8\u4e5f\u4e0d\u662f\u56fa\u5b9a\u6b7b\u503c\uff0c\u800c\u662f\u5148\u7528\u5404\u81ea\u7684\u9996\u6863 R \u8fdb\u5165\u52a8\u6001\u4fdd\u62a4\uff08\u4f8b\u5982 BTC \u4e3a 9R \u9501 8R\uff09\uff0c\u7136\u540e\u7ee7\u7eed\u8ba9\u8d8b\u52bf\u81ea\u5df1\u6269\u5c55\u3002",
            "\u5f53\u659c\u7387\u53d8\u5f97\u66f4\u9641\u65f6\uff0c\u7b56\u7565\u5c31\u7ed9\u4e88\u66f4\u5927\u7684\u6ce2\u52a8\u7a7a\u95f4\uff1b\u53cd\u8fc7\u6765\uff0c\u659c\u7387\u8f6c\u6b63\u6216\u7ed3\u6784\u5931\u6548\u65f6\u518d\u9000\u573a\u3002",
        )
    )
    dynamic_r_explanation = _dynamic_r_explanation_html()
    coin_period_tables = _coin_period_tables_html(specs)
    btc_long_performance = _btc_long_performance_html()
    btc_short_usage = _btc_short_usage_html()
    btc_short_research = _btc_short_research_html()
    eth_short_research = _eth_short_research_html()
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
    period_tables_label = _html_text("\u5206\u5e01\u5e74\u5ea6 / \u6708\u5ea6\u7edf\u8ba1")
    dynamic_r_label = _html_text("\u52a8\u6001\u4fdd\u62a4 R \u53e3\u5f84")
    long_label = _html_text("EMA \u52a8\u6001\u59d4\u6258\u505a\u591a")
    short_label = _html_text("\u5747\u7ebf\u659c\u7387\u505a\u7a7a")
    btc_long_label = _html_text("BTC EMA 做多使用口径")
    btc_short_usage_label = _html_text("BTC 空头使用口径")
    btc_short_research_label = _html_text("BTC 空头研究与多空组合推演")
    eth_short_research_label = _html_text("ETH 空头定稿口径")
    log_label = _html_text("\u66f4\u65b0\u65e5\u5fd7")
    dynamic_r_intro = _html_text(
        "\u8fd9\u4e00\u8282\u4e13\u95e8\u89e3\u91ca\u52a8\u6001\u4fdd\u62a4\u91cc\u7684 R \u662f\u600e\u4e48\u7b97\u7684\uff0c\u5c24\u5176\u662f\u624b\u7eed\u8d39\u504f\u79fb\u5f00\u542f\u540e\uff0c\u89e6\u53d1 R \u548c\u9501\u76c8 R \u90fd\u4e0d\u662f\u88f8\u4ef7\u53e3\u5f84\u3002"
    )
    period_tables_intro = _html_text(
        "\u4e0b\u9762\u628a\u6700\u4f73\u53c2\u6570\u5305\u91cc\u7684 4 \u4e2a\u5e01\u6309\u5e74\u5ea6\u548c\u6708\u5ea6\u5206\u522b\u62c6\u5f00\u7edf\u8ba1\u3002\u6bcf\u5f20\u8868\u90fd\u540c\u65f6\u5c55\u793a\u505a\u591a\u3001\u505a\u7a7a\u548c\u5408\u8ba1\uff0c\u5176\u4e2d\u6536\u76ca\u7387 / \u6700\u5927\u56de\u64a4 / \u56de\u64a4\u6bd4\u4f8b / \u671f\u672b\u6743\u76ca\u6309\u8be5\u5e01\u591a\u7a7a\u5408\u5e76\u540e\u7684\u8d44\u91d1\u66f2\u7ebf\u53e3\u5f84\u8ba1\u7b97\u3002"
    )
    btc_long_intro = _html_text(
        "BTC EMA 动态委托做多默认采用归档 S486。它的定位仍然是顺大趋势吃延伸，不是任何行情都稳定盈利的全天候参数；本次选择 S486，是因为它在 S484 / S485 / S486 三组里总盈亏最高、最大回撤最低、近两年合计亏损最小。"
    )
    btc_long_points = "".join(
        f"<li>{_html_text(item)}</li>"
        for item in (
            "归档快照 S486；报告时间 2026-06-14 15:47:55；交易对 BTC-USDT-SWAP，1 小时，只做多。",
            "回测区间：2019-12-16 14:00 -> 2026-06-14 11:00；样本：56,926 根（全量）。",
            "挂单参考线 EMA55；指标 EMA21 / EMA55 / ATR10；止损 2 ATR；止盈为动态止盈。",
            "默认保护规则：1R 先保本 + 双向手续费；4R 锁 1R + 双向手续费，之后每 1R 再上移 1R；11R 锁 10R + 双向手续费，之后每 1R 再上移 1R。",
            "回测摘要：每波开仓 1；交易数 501；胜率 51.90%；总盈亏 15576.7125；最大回撤 1503.3383；PF 1.6422；平均R 0.3172。",
            "实盘建议：BTC 日线/周线仍在上行趋势时正常启用；趋势走平、最近 3 个月持续亏损或回撤接近历史压力区时降仓；最近 6 个月 PF 接近 1 或连续失效时暂停新开，等趋势恢复再开。",
        )
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
    details {{
      margin-top: 12px;
    }}
    summary {{
      cursor: pointer;
      font-weight: 700;
      color: #7c2d12;
      margin-bottom: 8px;
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
    <h2>{period_tables_label}</h2>
    <p>{period_tables_intro}</p>
    {coin_period_tables}
  </section>
  <section class="panel">
    <h2>{btc_long_label}</h2>
    <div class="subcard">
      <p>{btc_long_intro}</p>
      <ul>{btc_long_points}</ul>
      {btc_long_performance}
    </div>
  </section>
  <section class="panel">
    <h2>{btc_short_usage_label}</h2>
    <div class="subcard">
      {btc_short_usage}
    </div>
  </section>
  <section class="panel">
    <h2>{btc_short_research_label}</h2>
    <div class="subcard">
      {btc_short_research}
    </div>
  </section>
  <section class="panel">
    <h2>{eth_short_research_label}</h2>
    <div class="subcard">
      {eth_short_research}
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
