from __future__ import annotations

from html import escape
from pathlib import Path

import pandas as pd

from backtester import backtest_strategy, default_exit_rule, settings_from_config
from data_loader import load_config, load_multi_timeframe_data, results_dir
from indicators import add_features
from plots import save_trade_chart
from v4 import align_daily_environment_to_entry_frame, build_daily_environment_state
from v5 import build_v5_signals
from v7 import simulate_dynamic_risk_trades, tag_strong_regime_trades
from v8 import apply_cost_scenario, v8_cost_scenarios, v8_risk_schedules


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    config = load_config()
    frames, metadata = load_multi_timeframe_data(
        config,
        [str(config["v2_daily_timeframe"]), str(config["v2_entry_timeframe"])],
    )
    daily_raw = frames[str(config["v2_daily_timeframe"])]
    entry_raw = frames[str(config["v2_entry_timeframe"])]

    daily_features = add_features(daily_raw)
    entry_features = add_features(entry_raw)
    environment_state = build_daily_environment_state(daily_features, config)
    aligned_entry = align_daily_environment_to_entry_frame(entry_features, environment_state)
    signals = build_v5_signals(aligned_entry, config)

    v8_config = dict(config)
    v8_config["results_dir"] = str(config["v8_results_dir"])
    results_path = results_dir(v8_config)

    outputs = build_v8_d_outputs(aligned_entry, signals, config)

    for scenario_name, trades in outputs.items():
        csv_name = f"v8_d_{scenario_name}_trades.csv"
        (results_path / csv_name).write_text(trades.to_csv(index=False), encoding="utf-8-sig")

    chart_root = results_path / "trade_charts" / "v8_d_stress_cost_2_0x"
    build_trade_charts(aligned_entry, outputs["stress_cost_2_0x"], chart_root)
    write_html_report(results_path / "v8_d_trade_report.html", metadata, results_path, outputs, chart_root)
    print(f"V8 D trade report complete: {results_path / 'v8_d_trade_report.html'}")


def build_v8_d_outputs(
    frame: pd.DataFrame,
    signals: dict[str, pd.Series],
    config: dict[str, object],
) -> dict[str, pd.DataFrame]:
    baseline_name = str(config["v6_baseline_strategy"])
    strong_name = str(config["v6_candidate_strategy"])
    strong_signal_times = pd.to_datetime(frame.loc[signals[strong_name].fillna(False), "timestamp"], utc=True)
    settings = settings_from_config(config)
    schedule = {item.name: item for item in v8_risk_schedules()}["v8_d_strong_1_5_weak_0_5"]
    scenarios = {item.name: item for item in v8_cost_scenarios()}

    outputs: dict[str, pd.DataFrame] = {}
    for scenario_name in ("base_cost", "stress_cost_2_0x"):
        scenario_settings = apply_cost_scenario(settings, scenarios[scenario_name])
        base_trades = backtest_strategy(
            frame,
            baseline_name,
            signals[baseline_name],
            scenario_settings,
            exit_rule=default_exit_rule(),
        )
        tagged = tag_strong_regime_trades(base_trades, strong_signal_times)
        simulated = simulate_dynamic_risk_trades(
            tagged,
            initial_capital=scenario_settings.initial_capital,
            base_risk_per_trade=float(config["risk_per_trade"]),
            schedule=schedule,
        ).copy()
        simulated["cost_scenario_name"] = scenario_name
        simulated["is_strong_regime"] = simulated["is_strong_regime"].astype(bool)
        outputs[scenario_name] = simulated
    return outputs


def build_trade_charts(frame: pd.DataFrame, trades: pd.DataFrame, chart_root: Path) -> None:
    chart_root.mkdir(parents=True, exist_ok=True)
    for idx, trade in trades.reset_index(drop=True).iterrows():
        file_name = (
            f"{idx + 1:02d}_{pd.to_datetime(trade['entry_time']).strftime('%Y%m%d_%H%M')}"
            f"_{pd.to_datetime(trade['exit_time']).strftime('%Y%m%d_%H%M')}"
            f"_{float(trade['R_multiple']):+.2f}R.png"
        ).replace(":", "_")
        save_trade_chart(frame, trade, chart_root / file_name)


def write_html_report(
    output_path: Path,
    metadata: dict[str, str],
    results_path: Path,
    outputs: dict[str, pd.DataFrame],
    chart_root: Path,
) -> None:
    base_trades = outputs["base_cost"]
    stress_trades = outputs["stress_cost_2_0x"]
    base_summary = summarize_trade_frame(base_trades)
    stress_summary = summarize_trade_frame(stress_trades)
    gallery_items = build_gallery_items(chart_root, stress_trades)

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>V8 D Backtest Trade Report</title>
  <style>
    body {{ font-family: 'Segoe UI', sans-serif; margin: 32px auto; max-width: 1240px; color: #1f2937; background: #f6f3ee; line-height: 1.6; }}
    h1, h2, h3 {{ color: #111827; }}
    .card {{ background: #fffdf8; border: 1px solid #e7dccd; border-radius: 14px; padding: 20px 24px; margin: 18px 0; box-shadow: 0 10px 28px rgba(17,24,39,0.06); }}
    .hero {{ background: linear-gradient(135deg, #f4efe4 0%, #fffdf8 55%, #eef5f1 100%); }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 12px; }}
    .metric {{ background: #fff; border: 1px solid #eadfce; border-radius: 12px; padding: 14px; }}
    .label {{ font-size: 12px; color: #6b7280; text-transform: uppercase; letter-spacing: 0.04em; }}
    .value {{ font-size: 24px; font-weight: 700; margin-top: 6px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ border: 1px solid #e7dccd; padding: 8px 10px; text-align: left; }}
    th {{ background: #f3ecdf; }}
    a {{ color: #0f766e; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .gallery {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 16px; }}
    .thumb {{ background: #fff; border: 1px solid #eadfce; border-radius: 12px; padding: 12px; }}
    .thumb img {{ width: 100%; height: auto; border-radius: 8px; display: block; }}
    .muted {{ color: #6b7280; font-size: 13px; }}
    code {{ background: #f3ecdf; padding: 2px 6px; border-radius: 6px; }}
  </style>
</head>
<body>
  <section class="card hero">
    <h1>V8 D 回测数据与交易 K 线图</h1>
    <p>这页汇总的是 <code>v8_d_strong_1_5_weak_0_5</code> 的完整回测数据。交易图使用 <code>stress_cost_2_0x</code> 口径生成，因为这是更贴近实盘压力测试的版本；但同时也给出 <code>base_cost</code> 的完整明细 CSV。</p>
    <div class="grid">
      <div class="metric"><div class="label">数据源</div><div class="value">{escape(metadata['data_source'])}</div></div>
      <div class="metric"><div class="label">标的</div><div class="value">{escape(metadata['symbol'])}</div></div>
      <div class="metric"><div class="label">周期</div><div class="value">{escape(metadata['timeframe'])}</div></div>
      <div class="metric"><div class="label">交易笔数</div><div class="value">{len(stress_trades)}</div></div>
    </div>
  </section>

  <section class="card">
    <h2>文件下载</h2>
    <ul>
      <li><a href="{escape((results_path / 'v8_d_base_cost_trades.csv').name)}">v8_d_base_cost_trades.csv</a></li>
      <li><a href="{escape((results_path / 'v8_d_stress_cost_2_0x_trades.csv').name)}">v8_d_stress_cost_2_0x_trades.csv</a></li>
    </ul>
  </section>

  <section class="card">
    <h2>回测摘要</h2>
    <h3>base_cost</h3>
    {base_summary.to_html(index=False, border=0)}
    <h3>stress_cost_2_0x</h3>
    {stress_summary.to_html(index=False, border=0)}
  </section>

  <section class="card">
    <h2>stress_cost_2_0x 交易明细</h2>
    <p class="muted">下面表格保留关键字段，完整字段请直接打开 CSV。</p>
    {select_trade_columns(stress_trades).to_html(index=False, border=0)}
  </section>

  <section class="card">
    <h2>交易 K 线图</h2>
    <p class="muted">每张图展示入场前后窗口、EMA21、EMA55、入场价、止损价、出场点。点击图片可打开原图。</p>
    <div class="gallery">
      {gallery_items}
    </div>
  </section>
</body>
</html>"""
    output_path.write_text(html, encoding="utf-8")


def summarize_trade_frame(trades: pd.DataFrame) -> pd.DataFrame:
    gross_profit = float(trades.loc[trades["pnl_usdt"] > 0, "pnl_usdt"].sum())
    gross_loss = float(-trades.loc[trades["pnl_usdt"] <= 0, "pnl_usdt"].sum())
    return pd.DataFrame(
        [
            {
                "trade_count": int(len(trades)),
                "strong_trades": int(trades["is_strong_regime"].sum()),
                "weak_trades": int((~trades["is_strong_regime"]).sum()),
                "total_pnl_usdt": float(trades["pnl_usdt"].sum()),
                "final_equity": float(trades["equity_after"].iloc[-1]) if not trades.empty else 0.0,
                "profit_factor": gross_profit / gross_loss if gross_loss > 0 else 0.0,
                "win_rate": float((trades["pnl_usdt"] > 0).mean()) if not trades.empty else 0.0,
                "avg_R": float(trades["R_multiple"].mean()) if not trades.empty else 0.0,
            }
        ]
    )


def select_trade_columns(trades: pd.DataFrame) -> pd.DataFrame:
    out = trades.copy()
    out["entry_time"] = pd.to_datetime(out["entry_time"], utc=True)
    out["exit_time"] = pd.to_datetime(out["exit_time"], utc=True)
    out["regime"] = out["is_strong_regime"].map(lambda x: "strong" if bool(x) else "weak")
    out["risk_amount"] = out["risk_amount"].round(2)
    out["pnl_usdt"] = out["pnl_usdt"].round(2)
    out["R_multiple"] = out["R_multiple"].round(3)
    out["entry_price"] = out["entry_price"].round(2)
    out["exit_price"] = out["exit_price"].round(2)
    out["equity_after"] = out["equity_after"].round(2)
    return out[
        [
            "entry_time",
            "exit_time",
            "regime",
            "risk_multiplier",
            "risk_amount",
            "entry_price",
            "stop_loss_price",
            "exit_price",
            "R_multiple",
            "pnl_usdt",
            "exit_reason",
            "equity_after",
        ]
    ]


def build_gallery_items(chart_root: Path, trades: pd.DataFrame) -> str:
    items: list[str] = []
    for idx, trade in trades.reset_index(drop=True).iterrows():
        file_name = (
            f"{idx + 1:02d}_{pd.to_datetime(trade['entry_time']).strftime('%Y%m%d_%H%M')}"
            f"_{pd.to_datetime(trade['exit_time']).strftime('%Y%m%d_%H%M')}"
            f"_{float(trade['R_multiple']):+.2f}R.png"
        ).replace(":", "_")
        rel_path = f"trade_charts/v8_d_stress_cost_2_0x/{file_name}"
        items.append(
            "<div class='thumb'>"
            f"<a href='{escape(rel_path)}'><img src='{escape(rel_path)}' alt='{escape(file_name)}'></a>"
            f"<div><strong>#{idx + 1}</strong> {escape(pd.to_datetime(trade['entry_time']).strftime('%Y-%m-%d %H:%M'))}"
            f" | R={float(trade['R_multiple']):.2f}"
            f" | {escape('strong' if bool(trade['is_strong_regime']) else 'weak')}</div>"
            f"<div class='muted'>exit={escape(str(trade['exit_reason']))} | pnl={float(trade['pnl_usdt']):.2f} USDT</div>"
            "</div>"
        )
    return "\n".join(items)


if __name__ == "__main__":
    main()
