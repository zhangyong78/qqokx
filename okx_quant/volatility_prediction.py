"""Read-only local BTC realized-volatility inference; no fitting or network calls.

Run: python -m okx_quant.volatility_prediction
All output volatilities are annualized percentage points (35 means 35%).
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from .candle_store import candle_store_db_path
from .persistence import deribit_volatility_cache_file_path
from .volatility_model_weights import MODELS, MODEL_VERSION, TRAINING_LABEL_END, TEST_LABEL_END

SHANGHAI = timezone(timedelta(hours=8))
EPS = 1e-8
HOUR_MS = 3_600_000


def _hourly_frame(rows: list[dict], today: str, label: str) -> pd.DataFrame:
    if not rows:
        raise ValueError(f"本地 {label} 小时数据为空。")
    f = pd.DataFrame(rows)
    required = ["ts", "open", "high", "low", "close"]
    if any(c not in f for c in required):
        raise ValueError(f"{label} 缺少 OHLC/时间戳字段。")
    f[required] = f[required].apply(pd.to_numeric, errors="raise")
    if not np.isfinite(f[required].to_numpy(dtype=float)).all():
        raise ValueError(f"{label} 存在非有限数值。")
    # DVOL history starts in 2021; never allow epoch placeholders into indicators.
    f = f[f.ts >= 1_609_459_200_000].copy().sort_values("ts")
    f["date"] = pd.to_datetime(f.ts, unit="ms", utc=True).dt.tz_convert(SHANGHAI).dt.strftime("%Y-%m-%d")
    f = f[f.date < today].copy()
    if f.empty:
        raise ValueError(f"{label} 没有已完成日数据。")
    valid = (f.low > 0) & (f.low <= f[["open", "close"]].min(axis=1)) & (f.high >= f[["open", "close"]].max(axis=1))
    if not f.ts.is_unique or not (f.ts % HOUR_MS == 0).all() or not valid.all():
        raise ValueError(f"{label} 存在重复时间戳、不整点或不合法 OHLC，停止预测。")
    return f


def aggregate_daily(volatility_rows: list[dict], spot_rows: list[dict], today: str) -> pd.DataFrame:
    """Aggregate complete Beijing days using 24 confirmed, consecutive hourly returns."""
    v = _hourly_frame(volatility_rows, today, "DVOL")
    iv = v.groupby("date", sort=True).agg(
        iv=("close", "last"), hours=("ts", "count"), first=("ts", "min"), last=("ts", "max"),
    )
    iv = iv[(iv.hours == 24) & (iv["last"] - iv["first"] == 23 * HOUR_MS)]
    h = _hourly_frame(spot_rows, today, "BTC 现货")
    h["log_return"] = np.log(h.close / h.close.shift())
    h.loc[h.ts.diff() != HOUR_MS, "log_return"] = np.nan
    h["squared_return"] = h.log_return ** 2
    spot = h.groupby("date", sort=True).agg(
        open=("open", "first"), high=("high", "max"), low=("low", "min"), close=("close", "last"),
        hours=("ts", "count"), return_hours=("log_return", "count"), rvvar=("squared_return", "sum"),
    )
    spot = spot[(spot.hours == 24) & (spot.return_hours == 24)].copy()
    spot["rvvar"] *= 365
    return iv[["iv"]].join(spot[["open", "high", "low", "close", "rvvar"]], how="inner").reset_index()


def build_features(daily: pd.DataFrame) -> pd.DataFrame:
    """Same causal definitions and initialization as the locked validation run."""
    f = daily.copy().reset_index(drop=True)
    if len(f) < 61:
        raise ValueError("至少需要 61 个连续完整日的 BTC 小时线与 DVOL 数据。")
    dates = pd.to_datetime(f.date, errors="raise")
    if not (dates.diff().dropna() == pd.Timedelta(days=1)).all():
        raise ValueError("完整日历史不连续，请补齐本地 BTC 小时线和 DVOL 数据后再预测。")
    if not np.isfinite(f[["open", "high", "low", "close", "iv", "rvvar"]].to_numpy()).all():
        raise ValueError("日数据包含非有限数值。")
    if (f[["open", "high", "low", "close", "iv"]] <= 0).any().any() or (f.rvvar < 0).any():
        raise ValueError("日数据包含非法价格/波动率。")
    v = f.rvvar.clip(lower=EPS)
    for days in (1, 7, 30):
        f[f"log_var{days}"] = np.log(v.rolling(days).mean().clip(lower=EPS))
    f["ewma_var"] = v.ewm(alpha=0.06, adjust=False).mean()
    f["log_iv_var"] = np.log((f.iv / 100) ** 2)
    f["iv_change1"] = f.iv.diff()
    for days in (1, 7):
        f[f"btc_ret{days}"] = np.log(f.close / f.close.shift(days))
    tr = pd.concat([f.high - f.low, (f.high - f.close.shift()).abs(), (f.low - f.close.shift()).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / 14, adjust=False).mean()
    f["log_atr"] = np.log(atr / f.close)
    f["range_norm"] = (f.high - f.low) / atr
    f["weekend_fraction1"] = ((dates + pd.Timedelta(days=1)).dt.dayofweek >= 5).astype(int)
    f = f.iloc[60:].reset_index(drop=True)
    if not np.isfinite(f[MODELS[1]["features"]].to_numpy()).all():
        raise ValueError("波动指标无法计算（例如价格完全不动），停止预测。")
    return f


def predict_variance(features: pd.DataFrame, horizon: int = 1) -> np.ndarray:
    if horizon not in MODELS:
        raise ValueError("当前模型只支持 1 日和 7 日。")
    model = MODELS[horizon]
    x = features[model["features"]].to_numpy(dtype=float)
    if not np.isfinite(x).all():
        raise ValueError("预测特征不完整或存在非有限值。")
    normalized = (x - np.asarray(model["mean"])) / np.asarray(model["std"])
    estimate = np.column_stack([np.ones(len(x)), normalized]) @ np.asarray(model["coef"])
    return np.maximum(np.exp(np.clip(estimate, -20, 10)) * model["smearing"], EPS)


def forecast_daily(daily: pd.DataFrame, *, now: datetime | None = None) -> dict:
    now = now or datetime.now(SHANGHAI)
    if now.tzinfo is None:
        raise ValueError("now 必须带时区。")
    today = now.astimezone(SHANGHAI).date()
    # Public callers also cannot inject incomplete/future days.
    daily = daily[daily.date < today.isoformat()].copy()
    features = build_features(daily)
    origin = pd.Timestamp(features.date.iloc[-1]).date()
    lag = (today - timedelta(days=1) - origin).days
    forecasts = []
    for horizon, model in MODELS.items():
        var = float(predict_variance(features.iloc[-1:], horizon)[0])
        forecasts.append({
            "horizon_days": horizon, "model": model["name"], "role": model["evaluation"]["role"],
            "target_start": (origin + timedelta(days=1)).isoformat(),
            "target_end": (origin + timedelta(days=horizon)).isoformat(),
            "annualized_variance": var, "annualized_rv_percent": 100 * np.sqrt(var),
            "horizon_move_scale_percent": 100 * np.sqrt(var * horizon / 365),
            "evaluation": dict(model["evaluation"]),
        })
    historical = features[features.date >= "2025-01-01"].copy()
    predictions = predict_variance(historical, 1)
    lookup = dict(zip(daily.date, daily.rvvar))
    history = []
    for (_, row), var in zip(historical.tail(60).iterrows(), predictions[-60:]):
        target = (pd.Timestamp(row.date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        actual = lookup.get(target)
        history.append({
            "origin": row.date, "target": target, "predicted_rv_percent": 100 * np.sqrt(float(var)),
            "actual_rv_percent": None if actual is None else 100 * np.sqrt(float(actual)),
            "baseline_rv_percent": 100 * np.sqrt(float(row.ewma_var)),
        })
    return {
        "version": MODEL_VERSION, "generated_at": now.astimezone(SHANGHAI).isoformat(),
        "timezone": "Asia/Shanghai", "data_start": daily.date.iloc[0], "data_end": origin.isoformat(),
        "complete_daily_samples": len(daily), "stale_days": lag, "is_current": lag == 0,
        "status": "可用于本次预测" if lag == 0 else f"数据落后 {lag} 日，仅显示历史截止日预测，请先同步本地数据",
        "training_label_end": TRAINING_LABEL_END, "test_label_end": TEST_LABEL_END,
        "current_dvol_30d_percent": float(features.iv.iloc[-1]),
        "latest_realized_1d_percent": 100 * np.sqrt(float(features.rvvar.iloc[-1])),
        "forecasts": forecasts, "history": history,
        "limitations": "预测实际波动而非涨跌或隐含波动率；波动尺度不是置信区间。1/7日 RV 不能直接与30日 DVOL比较后交易期权。历史成绩不是盈利保证。",
    }


def predict_local(*, now: datetime | None = None, volatility_path: Path | None = None, candle_path: Path | None = None) -> dict:
    now = now or datetime.now(SHANGHAI)
    if now.tzinfo is None:
        raise ValueError("now 必须带时区。")
    volatility_path = Path(volatility_path or deribit_volatility_cache_file_path())
    candle_path = Path(candle_path or candle_store_db_path()).resolve()
    payload = json.loads(volatility_path.read_text(encoding="utf-8"))
    rows = payload.get("BTC|hourly_base", {}).get("volatility_hourly", [])
    # Read-only: never create/migrate the application's database.
    with closing(sqlite3.connect(candle_path.as_uri() + "?mode=ro", uri=True)) as db:
        spot = db.execute("SELECT ts,open,high,low,close FROM candles WHERE inst_id=? AND bar=? AND confirmed=1 ORDER BY ts", ("BTC-USDT", "1H")).fetchall()
    spot_rows = [dict(zip(("ts", "open", "high", "low", "close"), row)) for row in spot]
    daily = aggregate_daily(rows, spot_rows, now.astimezone(SHANGHAI).date().isoformat())
    result = forecast_daily(daily, now=now)
    result["sources"] = {"volatility": str(volatility_path), "spot_hourly": str(candle_path)}
    return result


def main() -> None:
    try:
        print(json.dumps(predict_local(), ensure_ascii=False, allow_nan=False, indent=2))
    except (ValueError, OSError, KeyError, sqlite3.Error) as exc:
        raise SystemExit(f"本地波动率预测失败：{exc}") from exc


if __name__ == "__main__":
    main()
