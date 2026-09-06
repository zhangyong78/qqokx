# Phase2 Research Report: 7:00-8:00 Last Hour Daily Relationship

## Settings
- symbol: `BTC-USDT-SWAP`
- source: `local candle cache: BTC-USDT-SWAP/1H`
- local timezone offset: `UTC+8`
- daily session: `08:00` to next `08:00`
- transaction cost assumption: `0.050R` per trade
- risk filter: both long and short signal stop distance must be at least `0.100%` for risk-filtered summaries
- conclusion threshold: sample count >= `30`

## Sample Range
- samples: `2373`
- start: `2019-12-17`
- end: `2026-06-15`

## Overall Baseline
| side | sample_count | net_expectancy_R | gross_expectancy_R | hit_stop_first_rate | hit_1R_rate | hit_2R_rate | avg_final_close_R |
| --- | --- | --- | --- | --- | --- | --- | --- |
| long | 2373 | 0.273 | 0.323 | 0.658 | 0.890 | 0.776 | -14.739 |
| short | 2373 | -0.062 | -0.012 | 0.616 | 0.892 | 0.787 | 4.335 |

## Best Exit Methods Overall
| side | exit_method | sample_count | net_expectancy_R | median_net_R | positive_net_rate |
| --- | --- | --- | --- | --- | --- |
| short | next_close | 2373 | 4.285 | -0.195 | 0.481 |
| long | stop_or_close | 2373 | 0.273 | -1.050 | 0.108 |
| long | breakeven_after_1r | 2373 | 0.263 | -1.050 | 0.066 |
| long | half_1r_hold | 2373 | -0.048 | -1.050 | 0.110 |
| short | stop_or_close | 2373 | -0.062 | -1.050 | 0.106 |
| short | breakeven_after_1r | 2373 | -0.063 | -1.050 | 0.066 |
| short | fixed_2r | 2373 | -0.173 | -1.050 | 0.292 |
| short | half_1r_hold | 2373 | -0.173 | -1.050 | 0.109 |
| short | fixed_1r | 2373 | -0.284 | -1.050 | 0.380 |
| long | fixed_2r | 2373 | -0.294 | -1.050 | 0.255 |

## Top Long Setups
| condition_group | condition_value | sample_count | net_expectancy_R | hit_stop_first_rate | hit_1R_rate | hit_2R_rate | test_net_expectancy_R | model_reason |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| phase2_simple_setup | bear__long_upper_bear__middle_area__shrinking_volume | 58 | 0.992 | 0.741 | 0.983 | 0.862 | 0.696 | net 0.99R; samples 58; stop-first 74.1%; train/test both positive |
| doji_day | True | 250 | 0.340 | 0.636 | 0.848 | 0.720 | 0.706 | net 0.34R; samples 250; stop-first 63.6%; train/test both positive |
| prev_3_days_bullish | True | 159 | 0.322 | 0.610 | 0.818 | 0.667 | 0.118 | net 0.32R; samples 159; stop-first 61.0%; train/test both positive |
| recent_3day_bias | three_bullish | 159 | 0.322 | 0.610 | 0.818 | 0.667 | 0.118 | net 0.32R; samples 159; stop-first 61.0%; train/test both positive |
| close_area | middle_area | 503 | 0.293 | 0.636 | 0.881 | 0.757 | 0.398 | net 0.29R; samples 503; stop-first 63.6%; train/test both positive |

## Top Short Setups
| condition_group | condition_value | sample_count | net_expectancy_R | hit_stop_first_rate | hit_1R_rate | hit_2R_rate | test_net_expectancy_R | model_reason |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| doji_day | True | 250 | 0.513 | 0.580 | 0.896 | 0.760 | 0.812 | net 0.51R; samples 250; stop-first 58.0%; train/test both positive |
| volume_bucket | huge_volume | 90 | 0.140 | 0.478 | 0.722 | 0.567 | 0.179 | net 0.14R; samples 90; stop-first 47.8%; train/test both positive |
| last_hour_wick_type | long_upper_bear | 195 | 0.352 | 0.513 | 0.856 | 0.708 | -0.173 | net 0.35R; samples 195; stop-first 51.3% |
| recent_3day_bias | two_bearish | 182 | 0.323 | 0.566 | 0.868 | 0.736 | 1.845 | net 0.32R; samples 182; stop-first 56.6% |
| phase2_simple_setup | bear__long_lower_bear__middle_area__shrinking_volume | 79 | 0.258 | 0.595 | 0.911 | 0.861 | 1.651 | net 0.26R; samples 79; stop-first 59.5% |

## No Trade Setups
| condition_group | condition_value | side | sample_count | net_expectancy_R | hit_stop_first_rate | avoid_reason |
| --- | --- | --- | --- | --- | --- | --- |
| phase2_core_setup | downtrend__kept_bull__long_upper_bull__high_area__no_breakout | long | 37 | -1.050 | 0.622 | negative net expectancy -1.05R |
| phase2_simple_setup | bull__normal_bull__high_area__shrinking_volume | short | 35 | -0.867 | 0.686 | negative net expectancy -0.87R |
| phase2_core_setup | downtrend__kept_bull__long_upper_bull__high_area__no_breakout | short | 37 | -0.839 | 0.514 | negative net expectancy -0.84R |
| phase2_simple_setup | bear__long_lower_bear__low_area__shrinking_volume | short | 56 | -0.796 | 0.732 | negative net expectancy -0.80R |
| phase2_core_setup | downtrend__kept_bear__long_lower_bear__low_area__no_breakout | short | 31 | -0.748 | 0.710 | negative net expectancy -0.75R |
| last_hour_wick_type | normal_bear | long | 140 | -0.713 | 0.743 | negative net expectancy -0.71R |
| big_bear_day | True | long | 114 | -0.628 | 0.640 | negative net expectancy -0.63R |
| last_hour_wick_type | long_lower_bull | short | 113 | -0.625 | 0.717 | negative net expectancy -0.63R |
| phase2_simple_setup | bull__long_upper_bull__low_area__shrinking_volume | long | 50 | -0.593 | 0.620 | negative net expectancy -0.59R |
| phase2_simple_setup | bear__long_upper_bear__middle_area__shrinking_volume | short | 58 | -0.559 | 0.603 | negative net expectancy -0.56R |
| changed_bull_to_bear | True | long | 36 | -0.499 | 0.694 | negative net expectancy -0.50R |
| last_hour_change_type | changed_bull_to_bear | long | 36 | -0.499 | 0.694 | negative net expectancy -0.50R |
| phase2_core_setup | uptrend__kept_bull__long_upper_bull__high_area__no_breakout | short | 46 | -0.496 | 0.630 | negative net expectancy -0.50R |
| changed_bear_to_bull | True | short | 36 | -0.447 | 0.694 | negative net expectancy -0.45R |
| last_hour_change_type | changed_bear_to_bull | short | 36 | -0.447 | 0.694 | negative net expectancy -0.45R |
| phase2_simple_setup | bull__long_upper_bull__high_area__shrinking_volume | long | 102 | -0.445 | 0.657 | negative net expectancy -0.44R |
| phase2_core_setup | uptrend__kept_bull__long_lower_bear__middle_area__no_breakout | short | 37 | -0.431 | 0.568 | negative net expectancy -0.43R |
| phase2_simple_setup | bear__long_lower_bear__low_area__shrinking_volume | long | 56 | -0.426 | 0.589 | negative net expectancy -0.43R |
| phase2_simple_setup | bear__long_lower_bear__high_area__shrinking_volume | short | 59 | -0.414 | 0.475 | negative net expectancy -0.41R |
| phase2_simple_setup | bear__long_lower_bear__middle_area__normal_volume | long | 33 | -0.376 | 0.576 | negative net expectancy -0.38R |

## Stability Notes
- train/test rows exported: `124`
- threshold sensitivity rows exported: `48`
- Treat rows below 30 samples as observation only; they are included in `condition_summary_all_samples.csv` for exploration.
- Phase2 uses only data available at the 8:00 entry decision: the completed 7:00-8:00 bar, the completed 8:00 session day, and prior sessions. The next session is used only for outcomes.
