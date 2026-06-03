# BTC EMA Short Research

This project builds a reproducible daily EMA short research workflow for BTC using the local QQOKX candle cache.

## Data Source

- Default source: local OKX candle cache
- Default symbol: `BTC-USDT-SWAP`
- Default timeframe: `1D`
- Default timezone: `UTC`

This differs from the original brief's Binance spot default because the working instruction for this task was to use the local OKX market data.

## Run

Run these commands from this project directory:

```bash
pip install -r requirements.txt
python src/main.py
python src/main_v2.py
python src/main_v3.py
python src/main_v4.py
python src/main_v5.py
python src/main_v6.py
python src/main_v7.py
python src/main_v8.py
python src/main_v9.py
python src/main_v10.py
python src/main_v11.py
```

If your local QQOKX data root is not discoverable via `QQOKX_DATA_DIR`, edit `config.yaml` and set `data_dir`.

## Outputs

The main outputs are written to `results/`:

- `strategy_comparison.csv`
- `yearly_performance.csv`
- `trades.csv`
- `equity_curve.html`
- `drawdown_curve.html`
- `research_report.md`
- `trade_charts/`

Raw and processed data snapshots are also written to:

- `data/raw/btcusdt_1d.csv`
- `data/processed/btcusdt_1d_features.csv`

## V2: Daily Direction + 4H Entry

The follow-up V2 study keeps the original daily short thesis as the higher-timeframe direction filter and tests several 4H entry timing variants.

Its outputs are written to `results_v2_daily_4h/`:

- `strategy_comparison.csv`
- `yearly_performance.csv`
- `trades.csv`
- `equity_curve.html`
- `drawdown_curve.html`
- `research_report.md`
- `trade_charts/`

## V3: Exit Rule Study

The V3 study fixes the best V2 entry framework and compares several exit rules to see whether the main bottleneck is in exits rather than entries.

Its outputs are written to `results_v3_exit_study/`:

- `strategy_comparison.csv`
- `yearly_performance.csv`
- `trades.csv`
- `equity_curve.html`
- `drawdown_curve.html`
- `research_report.md`
- `trade_charts/`

## V4: Daily Regime Filter Study

The V4 study fixes the best V2 entry and the best V3 baseline exit, then compares several daily regime filters to find which market environments are worth trading and which ones should be avoided.

Its outputs are written to `results_v4_regime_study/`:

- `strategy_comparison.csv`
- `yearly_performance.csv`
- `trades.csv`
- `equity_curve.html`
- `drawdown_curve.html`
- `research_report.md`
- `trade_charts/`

## V5: Combo Filter Study

The V5 study formalizes the strongest V4 single filters and compares their combinations to see whether combined daily context improves quality without over-filtering the sample.

Its outputs are written to `results_v5_combo_study/`:

- `strategy_comparison.csv`
- `yearly_performance.csv`
- `trades.csv`
- `equity_curve.html`
- `drawdown_curve.html`
- `research_report.md`
- `trade_charts/`

## V6: Robustness Study

The V6 study compares the baseline daily gate and the current best combo gate across chronological anchored walk-forward splits to check whether the edge survives out of sample.

Its outputs are written to `results_v6_robustness/`:

- `strategy_comparison.csv`
- `walkforward_splits.csv`
- `robustness_summary.csv`
- `research_report.md`

## V7: Dynamic Risk Study

The V7 study keeps the same baseline trade sequence but changes per-trade risk depending on whether the trade also belongs to the strongest combo regime.

Its outputs are written to `results_v7_dynamic_risk/`:

- `strategy_comparison.csv`
- `walkforward_splits.csv`
- `robustness_summary.csv`
- `research_report.md`

## V8: Stress Grid Study

The V8 study combines multiple dynamic-risk schedules with tougher fee/slippage assumptions to see which sizing rule survives execution pressure best.

Its outputs are written to `results_v8_stress_grid/`:

- `strategy_comparison.csv`
- `walkforward_splits.csv`
- `robustness_summary.csv`
- `research_report.md`

## V9: Live Readiness Study

The V9 study narrows the V8 shortlist and checks practical deployment pressure through monthly and quarterly performance distributions plus consecutive-loss stress.

Its outputs are written to `results_v9_live_readiness/`:

- `strategy_comparison.csv`
- `monthly_periods.csv`
- `monthly_summary.csv`
- `quarterly_periods.csv`
- `quarterly_summary.csv`
- `streak_pressure.csv`
- `research_report.md`

## V10: Monthly Guardrail Study

The V10 study keeps the harsh-cost execution assumptions and tests whether monthly stop-and-stand-down rules improve the live pain profile enough to justify skipping valid trades.

Its outputs are written to `results_v10_monthly_guardrail/`:

- `strategy_comparison.csv`
- `monthly_periods.csv`
- `monthly_summary.csv`
- `guardrail_months.csv`
- `guardrail_summary.csv`
- `research_report.md`

## V11: Trade-Sequence Guardrail Study

The V11 study keeps the same harsh-cost execution assumptions but replaces calendar-month protection with a shorter-cycle rule that reduces risk after consecutive losses.

Its outputs are written to `results_v11_trade_guardrail/`:

- `strategy_comparison.csv`
- `throttle_usage.csv`
- `streak_pressure.csv`
- `research_report.md`
- `research_report.html`
