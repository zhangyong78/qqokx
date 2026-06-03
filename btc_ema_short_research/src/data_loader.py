from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PROJECT_ROOT.parent

if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from okx_quant.app_paths import configure_data_root, data_root
from okx_quant.candle_cache import load_candle_cache


def load_config(path: Path | None = None) -> dict[str, Any]:
    config_path = path or (PROJECT_ROOT / "config.yaml")
    return yaml.safe_load(config_path.read_text(encoding="utf-8"))


def load_market_data(config: dict[str, Any]) -> tuple[pd.DataFrame, dict[str, str]]:
    timeframe = str(config["timeframe"]).strip()
    frame = load_market_data_for_timeframe(config, timeframe)
    metadata = build_metadata(config, [timeframe])
    return frame, metadata


def load_market_data_for_timeframe(config: dict[str, Any], timeframe: str) -> pd.DataFrame:
    data_dir = config.get("data_dir")
    configure_data_root(Path(data_dir).expanduser().resolve() if data_dir else None)

    symbol = str(config["symbol"]).strip().upper()
    candles = load_candle_cache(symbol, timeframe, limit=None)
    if not candles:
        raise RuntimeError(f"no local OKX candles found for {symbol} {timeframe}")

    rows = [
        {
            "timestamp": pd.to_datetime(int(candle.ts), unit="ms", utc=True),
            "open": float(candle.open),
            "high": float(candle.high),
            "low": float(candle.low),
            "close": float(candle.close),
            "volume": float(candle.volume),
            "confirmed": bool(candle.confirmed),
        }
        for candle in candles
    ]
    frame = pd.DataFrame(rows).sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)
    return filter_market_data(frame, config)


def load_multi_timeframe_data(config: dict[str, Any], timeframes: list[str]) -> tuple[dict[str, pd.DataFrame], dict[str, str]]:
    unique_timeframes = [item.strip() for item in timeframes if item and str(item).strip()]
    if not unique_timeframes:
        raise ValueError("timeframes must not be empty")

    data_dir = config.get("data_dir")
    configure_data_root(Path(data_dir).expanduser().resolve() if data_dir else None)

    frames = {timeframe: load_market_data_for_timeframe(config, timeframe) for timeframe in unique_timeframes}
    metadata = build_metadata(config, unique_timeframes)
    return frames, metadata


def filter_market_data(frame: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    start = pd.Timestamp(str(config["backtest_start"]), tz="UTC")
    end_value = str(config.get("backtest_end", "latest_available")).strip().lower()
    if end_value == "latest_available":
        filtered = frame[frame["timestamp"] >= start].copy()
    else:
        end = pd.Timestamp(end_value, tz="UTC")
        filtered = frame[(frame["timestamp"] >= start) & (frame["timestamp"] <= end)].copy()
    filtered.reset_index(drop=True, inplace=True)
    return filtered


def build_metadata(config: dict[str, Any], timeframes: list[str]) -> dict[str, str]:
    symbol = str(config["symbol"]).strip().upper()
    metadata = {
        "data_source": str(config.get("data_source", "local_okx_candle_cache")),
        "data_root": str(data_root()),
        "symbol": symbol,
        "timeframe": ", ".join(timeframes),
        "timezone": str(config.get("timezone", "UTC")),
    }
    return metadata


def save_raw_snapshot(frame: pd.DataFrame, config: dict[str, Any]) -> Path:
    path = PROJECT_ROOT / str(config["raw_output_path"])
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, encoding="utf-8-sig")
    return path


def save_processed_snapshot(frame: pd.DataFrame, config: dict[str, Any]) -> Path:
    path = PROJECT_ROOT / str(config["processed_output_path"])
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, encoding="utf-8-sig")
    return path


def results_dir(config: dict[str, Any]) -> Path:
    path = PROJECT_ROOT / str(config.get("results_dir", "results"))
    path.mkdir(parents=True, exist_ok=True)
    (path / "trade_charts").mkdir(parents=True, exist_ok=True)
    return path
