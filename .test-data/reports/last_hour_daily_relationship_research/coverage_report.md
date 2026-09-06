# Coverage Report

This report checks the phase1 coverage against the phase2 request. Phase2 keeps the phase1 data model and adds missing modules as additional outputs.

| Module | Status | Notes |
| --- | --- | --- |
| 8:00 session daily aggregation | covered in phase1 | Reused; daily session is 8:00 to next 8:00. |
| 7:00-8:00 signal bar | covered in phase1 | Reused; signal bar is the last hour of the 8:00 session. |
| Entry after 8:00 close | covered in phase1 | Reused. |
| Long/short stop from signal low/high | covered in phase1 | Reused. |
| 1R/2R/stop/final close R | covered in phase1 | Extended with first-touch path flags. |
| Temporary daily state before 7:00 | added in phase2 | Adds temp_day_*_7h and last_hour_change_type. |
| Last hour changes daily color | added in phase2 | Adds changed_bear_to_bull and changed_bull_to_bear. |
| Six-part wick structure | added in phase2 | Adds last_hour_wick_type. |
| Last hour close area in daily range | added in phase2 | Adds close_area and daily_close_location. |
| Four-way volume bucket | added in phase2 | Adds volume_bucket based on prior 24h average. |
| Tail breakout / failed breakout | added in phase2 | Adds break_prev_23h_high/low and failed_*_breakout. |
| Trend regime | covered and renamed in phase2 | Uses current completed 8:00 daily EMA20/EMA50 regime. |
| Recent 2-3 day structure | added in phase2 | Adds consecutive bull/bear, big day, doji, long wick counts. |
| Path backtest first touch | added in phase2 | Adds hit_stop_first, hit_1R_first, hit_2R_first per side. |
| Exit method comparison | added in phase2 | Exports exit_method_comparison.csv. |
| Year/quarter stability | added in phase2 | Exports condition_time_stability.csv. |
| Train/test stability | added in phase2 | Exports train_test_summary.csv. |
| Threshold sensitivity | added in phase2 | Exports threshold_sensitivity.csv. |
| Net expectancy after cost | added in phase2 | Uses net_expectancy_R with configurable cost_r. |
| Top/no-trade model lists | added in phase2 | Exports top_long/top_short/no_trade setup files. |
