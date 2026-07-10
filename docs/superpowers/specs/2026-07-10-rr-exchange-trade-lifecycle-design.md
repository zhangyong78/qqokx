# RR Exchange Trade Lifecycle Design

## Goal

Turn a confirmed RR drawing into an explicitly enabled perpetual-swap trade in
the selected API profile. A drawing remains analysis-only until the user
selects it and presses `Enable trading`.

## Scope

The first delivery supports only `SWAP` instruments and the `moni` (OKX demo)
environment for automated validation. Live trading remains an explicit UI
action and is never enabled by saving or drawing an RR item.

## Order Model

- Entry mode can be market, fixed limit, or best-quote chase.
- A best-quote chase entry is an ordinary passive limit order: buy at bid 1,
  sell at ask 1. It is not an OKX trigger algo, because trigger algos cannot
  remain continuously queued at the best quote.
- Stop loss and take profit are OKX conditional algos using market execution.
- The entry and each protection order get distinct deterministic client IDs.
- A ledger entry owns every order identifier and records every state transition.

## Lifecycle

`draft -> entry_working -> entry_partially_filled | entry_filled -> protected`

`entry_working -> cancelled` is allowed only when no fill exists. If a fill is
present, cancelling stops further entry execution but keeps the filled exposure
protected. The UI asks for confirmation before this action.

The reconciler always reads the exchange state before a cancel-and-replace
operation. A filled or partially-filled entry is never replaced. This prevents
two live entry orders from being created by an ambiguous cancel response.

## Protection

Immediately after a fill is detected, the service creates/reconciles a market
stop-loss algo and a market take-profit algo for the filled size. It resolves
`posSide` through the existing account-mode-aware client helpers, so net and
long/short accounts are both supported. Local monitoring may amend the
stop-loss algo only after the selected management rule triggers; it never
creates a new opening position.

## Cancellation

For an unfilled entry, cancellation removes the entry order and marks the
ledger record cancelled. For a partial or full fill, the dialog explains that a
position exists; confirmation cancels only the remaining entry quantity,
retains protection, and marks the record `protected_cancelled_remainder`.

## Validation

Unit tests cover plan validation, client-ID stability, best-quote selection,
chase cancel/replacement guards, partial-fill cancellation, and protection
creation. A moni smoke test is allowed only after code tests pass and submits
the smallest valid demo order, verifies its identifier/state, and cancels it
before handoff. No live order is submitted during automated validation.
