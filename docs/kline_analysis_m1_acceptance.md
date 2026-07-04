# K-line Analysis M1 Acceptance

This milestone is the first internal-use release of the local K-line workstation.

## Scope

The release is accepted only when all of the following are true:

1. Chart stability
   The `kline-analysis` module can load local candles and render a usable chart on the target workstation even when `QtWebEngine` GPU setup is unstable.

2. Local-first data loading
   The page loads candles from local cache first, falls back to API only when local history is short or stale, and reports the final source in the status bar.

3. Session workspace persistence
   Every `symbol + period` pair keeps its own saved workspace in local state, including:
   - alert lines
   - alert switches
   - recent event log

4. Alert rules in production shape
   The page supports and persists these alert types:
   - EMA 15 crossing SMA 50
   - manual horizontal line alerts
   - manual trend line alerts
   - automatic box breakout alert

5. Non-spam behavior
   The same rule does not emit duplicate alerts for the same candle during auto-refresh.

6. Chart-visible alert geometry
   Saved horizontal lines and trend lines are visible on the native chart and survive app restart.

7. Structure context
   The page shows a lightweight structure summary derived from local candles so the team can see box/trendline context before adding more advanced pattern rules.

8. Verification
   Targeted compile and unit tests pass for:
   - K-line helper logic
   - alert evaluation logic
   - launcher/window helpers affected by this milestone

## Explicitly Out Of Scope

The following are intentionally not required for M1:

- automated trade execution
- interactive Web chart drawing parity with native mode
- full shape-alert catalog beyond box breakout
- multi-user permissions or generalized deployment packaging

## Done Means

M1 is done when a teammate can open `kline-analysis`, load a symbol, add a horizontal line or trend line, restart the app, and still see:

- the chart
- the saved line
- the alert switches
- the alert event history
- no duplicate spam for the same candle on refresh
