# Kraken Trading Bot — Context Document (HEAVILY OUTDATED BY SWEEP)

**⚠️ CRITICAL: THIS DOCUMENT DESCRIBES THE RETIRED regime_v3 / GridRunner SYSTEM.**

The runway grid placement bug (constant cancel/rebuild with lag>0.60%) and
strong-uptrend clustering bugs lived in the code described in large parts of
this file. That entire logic (`grid_runner.py` + `strategy/regime_v3.py`)
has been **moved to `obsolete/` and excised from all live paths**.

**Current reality (post-sweep, 2026-05):**
- `strategy_version` MUST be `"hybrid_grid"`
- Live runner is exclusively `HybridGridRunner` + `strategy/hybrid_grid.py`
  (5-state classifier per `STRATEGY.md`: includes explicit Strong Downtrend,
   anchored spacing, ChaseSequencer hard floor, proximity refills)
- No regime_v3 code executes. No GridRunnerConfig exists at runtime.
- See the banner + updated sections in `docs/architecture.md` for what still applies.

Everything below this banner that claims "Active strategy is regime_v3",
describes the three-rule dispatch in grid_runner, or details the old ladder
REBUILD behavior is **historical only** and must not be used as implementation
guidance.

---

This file is for getting a new session up to speed quickly (with heavy caveats above).

## What this bot does

Spot-only DCA-grid trading bot on Kraken. The traded symbol list is
defined in `config.json` `symbols` — read it there for the current
set. Symbols are fundamentals-selected.

**Active strategy is hybrid_grid (5-state per STRATEGY.md)** — unified
ladder/grid with direction-aware behavior:

- Strong Uptrend → ChaseSequencer only (hard floor, no ladder)
- Other states → LadderManager with anchored spacing in down regimes
- Hysteresis on state transitions (90s)
- Exact-price rung refills on TP fills

The old regime_v3 three-rule (chase / pullback / ladder) description that follows is retired.
  config), active in UPTREND / SIDEWAYS / DOWNTREND. As rungs fill,
  new rungs are added at the bottom (ratchet down). As cycles complete
  (TP fills), rungs are refilled at the top.
- **pullback** — currently disabled. Was a single VAL-anchored maker
  bid; superseded by the ladder for volume.

**No stop-losses.** Exits depend on the TP firing. The only ceiling
is `limits.max_capital_pct` — when reached, the bot stops opening
new positions and waits for TPs to free capital. There is no
drawdown floor and no automated capital-preservation; that is the
accepted design (see `feedback_dca_no_floor` memory).

Live mode runs as systemd service `kraken_rules_trading_bot.service`.

## Repo layout

```
kraken_trading_bot/
  main.py               Actual entry point / bootstrap script for the live bot
  config.json           runtime configuration (live)
  credentials/          encrypted Kraken keys + key_loader
  data_manager.py       coordinates kline/book/trade subscriptions + loggers
  exchange/
    connection.py       ccxt-backed REST connection
    private_ws.py       Kraken v2 private WS — order placement + executions
    public_ws.py        Kraken v2 public WS — book / trade / ticker
    order_operations.py place_limit_buy / place_market_buy / fetch_* / cancel_order
    fee_cache.py        per-symbol maker/taker fee dict — fail-loud, no fallback
  execution/
    lifecycle.py        open_position_market / place_limit_buy / cancel_resting_buy
    reconciliation.py   StartupReconciler — boot-time rebuild from exchange
  indicators/           ATR, SMA, BB, CVD, VWAP, multi-window volume profile,
                        rolling_indicators (slope, width), state_features
  live_trading/
    trade_state.json    persistent tracker — every trade goes through here
    trade_history.json  closed-trade archive
  logs/                 trading_bot.log (current) + .1 .. .5 (rotated)
  market_data/          containerized market-data accessors
  obsolete/             retired modules + docs — do not import
  orchestrator/
    trading_orchestrator.py   startup, balance/fee fetch, per-symbol task spawn (the core trading loop)
    runtime.py          RuntimeState + GridRunnerConfig dataclasses
    shared_loop.py      dispatches private WS events (fills, cancels)
    exchange_sync.py    periodic exchange-state sync (5-min default)
    grid_runner.py        regime_v3 dispatcher (chase / ladder / pullback)
    kline_helpers.py    kline-fetch helpers
    shutdown.py         graceful shutdown coordinator
  paper_trading/        paper-mode order simulator
  persistence/
    trade_tracker.py    CRUD on trade records; status state machine
    telemetry/raw_ws_logger.py   dumps every raw WS message as JSONL
  price_calculator.py   calculate_tp_price (fee-aware), calculate_portfolio_value
  risk/
    sizing.py           position-sizing helpers
  scripts/
    gzip_raw_ws.py      nightly raw_ws compression (cron 03:15 UTC)
  state_machine/        long-term state classifier (anchored VWAP, regime_classifier, monitor)
  strategy/
    regime_v3.py        classify_state + chase/ladder/pullback rule helpers
    common.py           shared strategy utilities
  tests/                pytest suite (sparse)
  tools/                operator one-shots (e.g. reconcile_stuck_trades.py)
```

## Strategy fundamentals (regime_v3 — the only strategy that fires today)

State classifier ([strategy/regime_v3.py](strategy/regime_v3.py)
`classify_state`) runs per tick per symbol, reading 15m-derived
indicators (`sma_1h`, `sma_4h`, `slope_1h_pct`, `width_pct`) plus the
multi-window VP outputs. Priority order:

- **SIDEWAYS** wins first when `width_pct < sideways_width_threshold_pct`.
- Otherwise: `sma_1h > sma_4h` AND
  `slope_1h_pct >= strong_uptrend_slope_threshold_pct` → **STRONG_UPTREND**.
- `sma_1h > sma_4h` but slope below threshold → **UPTREND**.
- `sma_1h < sma_4h` → **DOWNTREND**.

Three rules dispatch from the classified state in
[orchestrator/grid_runner.py](orchestrator/grid_runner.py)
`_evaluate_regime_v3`:

1. **chase** (`apply_in_states=[STRONG_UPTREND]`) — taker market BUY.
   The state IS the gate; no extra indicator filter. `spacing_pct` is
   tuned to align the next buy to the prior TP (staircase pattern:
   round-trip fees + `profit_pct` = gross TP = spacing).
   `position_size_pct` of portfolio per order.
2. **ladder** (`apply_in_states=[UPTREND, SIDEWAYS, DOWNTREND]`) —
   `rung_count` maker bids at `-spacing_pct`, `-2·spacing_pct` …
   below mid. When the top rung fills, a new rung is added at the
   bottom (ratchet). When a cycle TPs,
   `shared_loop._refill_ladder_slot` places a new bid at the original
   buy price (slot refill at the top).
3. **pullback** — disabled (`enabled=false`, `apply_in_states=[]`).
   Kept as a structural placeholder.

**Take Profit** is computed at buy-fill time in
`shared_loop._handle_buy_filled` as
`entry × (1 + (buy_fee + sell_fee + intended_profit_pct) / 100)`. Fees
come from `FeeCache` per-symbol; the trade record carries
`intended_profit_pct` set at buy time (no global shadow).

**No automated halts.** Weak uptrend, downtrend, and sideways are all
valid ladder states; there is no trend-halt and no automated capital
preservation. The user is the discretionary circuit breaker for
thesis-level exits (see `feedback_remove_symbol_wind_down_first`).

## Trade state machine (persistence/trade_tracker.py)

A trade record in `live_trading/trade_state.json` moves through these states:

```
buy_pending → buy_filled → sell_pending → sell_filled
       ↘ canceled (terminal)
```

`tracker.cancel_buy(oid)` behavior:
- If the trade has **no fills** yet → status = canceled, archived. Clean.
- If the trade has a **partial fill** → status STAYS as `buy_pending`,
  NOT archived. The expectation is that `shared_loop._handle_buy_canceled`
  (triggered by the private WS `canceled` event) picks it up and
  transitions it to `sell_pending` by placing a TP sell for the partial
  qty.

If that WS event is missed, the runtime `ExchangeSyncTask` catches the
divergence within ~5 min and reconciles. The historical "extra tokens
in the Kraken account that the bot doesn't know about" problem is
mitigated for symbols currently in `config.symbols`. See the
"Order flow architecture" section for the full reconciliation path,
including the one remaining divergence path (symbols removed from
config while still holding open orders).

## Order flow architecture (buying, selling, tracking, reconciling)

Order *mechanics* described here apply regardless of which strategy
is firing.

### Buying

Strategy runners ([orchestrator/grid_runner.py](orchestrator/grid_runner.py)
`_evaluate_regime_v3`) decide when to place an order and dispatch
through [execution/lifecycle.py](execution/lifecycle.py), which calls
[exchange/order_operations.py](exchange/order_operations.py) —
`place_limit_buy` for maker entries (ladder, pullback) or
`place_market_buy` for taker entries (chase). Both send `add_order`
messages over the **private WebSocket** via
`data_manager.send_order(...)`. **No REST fallback** — if the private
WS is disconnected, placement fails loud (`"Cannot place ... private
WS not connected"`).

### Selling (TP placement)

The private WS executions channel pushes the buy fill to
[orchestrator/shared_loop.py](orchestrator/shared_loop.py)
`_handle_buy_filled`. The handler reads `intended_profit_pct` off the
trade record (set at buy time per-rule, no global shadow), computes
the TP price via `calculate_tp_price()` using the per-symbol maker fee
from `FeeCache`, then calls `place_limit_sell()` — also over the
private WS. The TP sell's order_id is stored on the trade record as
`sell_order_id`.

### Fill notifications

Order events arrive over the private WS executions channel.
[exchange/private_ws.py](exchange/private_ws.py) `_dispatch` queues
each event; [orchestrator/shared_loop.py](orchestrator/shared_loop.py)
`_dispatch_event` routes by `order_status`:

- `filled` (trade was `buy_pending`) → `_handle_buy_filled` → place TP sell
- `filled` (trade was `sell_pending`) → `_handle_sell_filled` → mark
  closed, refresh balance, slot-refill ladder if applicable
- `canceled` (trade was `buy_pending`) → `_handle_buy_canceled`
- `partially_filled` → record cumulative qty / avg price / partial fee
  via `tracker.update_partial_fill` (both buy- and sell-side handled)

WS pushes happen in real time; the tracker reflects fills within
milliseconds whenever the WS is healthy.

### Tracking

[persistence/trade_tracker.py](persistence/trade_tracker.py) owns trade
records, the status state machine, and cumulative stats. State
transitions:

```
buy_pending → buy_filled → sell_pending → sell_filled
       ↘ canceled (terminal sidetrack)
```

The tracker is the bot's working store — trading decisions read from
it. Each trade record carries `strategy`, `rule_name`,
`intended_profit_pct`, fill prices / qtys / fees, and both buy and
sell `order_id`s.

### Reconciling — startup

[execution/reconciliation.py](execution/reconciliation.py)
`StartupReconciler.reconcile()` runs once before the strategy loops
start:

1. Cancels stale open buys for configured symbols (`_cancel_open_buys`).
2. Fetches open + closed orders from Kraken.
3. Rebuilds `sell_pending` entries for buys that filled but have no
   sell yet (`_rebuild_sell_pending_entries`).
4. Marks orphans (`_mark_orphan_filled_buys`).

### Reconciling — runtime

[orchestrator/exchange_sync.py](orchestrator/exchange_sync.py)
`ExchangeSyncTask` runs every `exchange_sync_interval_seconds` (300s
default) for the life of the process. Each tick:

1. Compute `known_bot_ids` — union of `tracker.trades` +
   `trade_history.json` (every order the bot has ever placed).
2. `fetch_open_orders` per symbol; `fetch_closed_orders` account-wide
   (30-min lookback).
3. Filter to `known_bot_ids`, then reconcile:
   - Tracker says open but order not on exchange → apply the closed
     state (filled or canceled).
   - `sell_pending` but exchange shows the sell closed → record P&L,
     archive.
   - Exchange has a `known_bot_id` the tracker doesn't track →
     **adopt back into the tracker** (NEVER cancel — exchange wins on
     existence).
4. Log a one-line summary; WARN if any divergence was reconciled, INFO
   otherwise.

**Policy**: this task NEVER cancels orders. Cancellation is owned by
`StartupReconciler` (boot) and the strategy paths (aging, halt,
manual). The sync is read-and-reconcile only.

### Where REST is used

REST is the backstop for everything WS-push can't cover:
`fetch_open_orders`, `fetch_closed_orders`, `fetch_all_balances`,
`fetch_trading_fees` (FeeCache populate), `fetch_all_usd_prices`
(portfolio valuation), `cancel_all_orders`, and
`place_trailing_stop_sell` (Kraken doesn't support trailing stops
over WS). Real-time order events and placements all go over the
private WS.

### Source-of-truth model

Exchange is functionally the source of truth: placements flow into
it, fill/cancel events flow out of it in real time, and divergence
is always reconciled toward the exchange (never the other way). The
tracker is the bot's working cache — kept aligned, not rebuilt.
Worst-case drift window is one sync interval (~5 min) plus WS
reconnect latency.

**One real divergence path the reconciler does NOT cover**: removed
symbols. The reconciler iterates only `config.symbols`. If a symbol
is removed from the config while it still has open orders on Kraken,
those orders fall outside the sync scope and become invisible to the
bot. To remove a symbol cleanly: wind down its resting orders first
(cancel buys; let sells TP out), confirm zero open on Kraken, THEN
remove from config.

## Time zones — read carefully

- **Bot log files** (`logs/trading_bot.log*`) are written in **local
  Eastern time** (EDT in summer, EST in winter). The user lives in ET.
- **Raw WS log files** (`/mnt/externalssd/raw_ws/`) — every record carries
  a `ts` field in **UTC ISO format with `+00:00` offset**. Filenames are
  named with **UTC dates** (`xlm_usd_trade_20260421.jsonl`).
- **The user thinks in ET.** When they say "06:30 this morning" they mean
  06:30 ET. Always convert from UTC for display, label timestamps as ET,
  and never assume the user's chart references UTC.
- A region that begins at, say, 23:55 ET on day X and crosses midnight ET
  rolls into day X+1. But its UTC timestamp may put it in a different UTC
  date file. Both file boundaries (UTC) and ET dates need handling when
  loading data for a "day" the user references.

## Logging architecture

Two distinct logs:

1. **Bot log** at `logs/trading_bot.log` — rotated by Python's
   `RotatingFileHandler`. Two useful per-symbol summary line shapes:
   - `[LiveTradingLoop] ONDO/USD [regime_v3] mid=$0.393740 state=UPTREND width=7.72% slope_1h=-0.430% pullback_bid=0.374425`
   - `[RegimeMonitor] [SM] ETH/USD state=UPTREND sma_1h=2030.27 sma_4h=2027.61 slope_1h=-0.0975 width=3.29% avwap=2029.07`

2. **Raw WS** at
   `/mnt/externalssd/raw_ws/{book,trade,ticker}/<sym>_<chan>_<YYYYMMDD>.jsonl`.
   One line per WS message, zero processing. Volume scales with the
   subscribed symbol count; gzip cron at 03:15 UTC compresses each
   day's output. Disk has years of capacity. Both
   `.jsonl` and `.jsonl.gz` should be supported by any tool that
   reads these.

## Always-on trade-blocking rules

The current set of rules that can prevent or cancel a buy. Tunable
values live in `config.json` — these bullets describe what each rule
DOES; check the config for current settings.

- **Warmup** — `trading.warmup_seconds` after startup. Strategies
  return early until tick-level features settle.
- **State classifier dispatch** — each regime_v3 rule has its own
  `apply_in_states` in config. A rule only fires when the classified
  state is in its list. A rule with `enabled=false` or empty
  `apply_in_states` never fires.
- **Per-symbol cap** — `limits.max_per_symbol`. Counts only ACTIVE
  statuses (`buy_pending`, `buy_filled`, `sell_pending`); orphans
  (`manual_reconcile_needed`) are excluded.
- **Global cap** — `limits.max_global`. Total ACTIVE trades across
  all symbols.
- **Capital cap** — `limits.max_capital_pct` of `cached_balance`.
  Same active-status filter on `tracker.total_funds_deployed()`.
  Typically the binding ceiling — once reached, no new entries fire
  until TPs free capital.
- **Capital remaining** — the prospective order size must fit under
  what's left after the capital cap.
- **Spacing** — new orders must be at least the rule's `spacing_pct`
  from existing positions of OTHER rules. Ladder excludes its own
  rungs (spaced by construction). Chase only checks buys; ladder and
  pullback check both buys and sells.
- **Stale-rung cleanup** — when a ladder bid is canceled or
  ratcheted, `_cancel_stale_rungs` removes the displaced rung.

Ladder explicitly intends many resting bids per symbol — there is no
"one resting buy per symbol" rule, no order aging, and no VWAP-reprice
cancellation.

## Known issues

1. **Removed symbols become orphan orders.** The reconciler iterates
   only `config.symbols`. If a symbol is removed while it still has
   open orders on Kraken, those orders fall outside the sync's scope
   and stay live but invisible to the bot. Witnessed with DOGE/ADA on
   2026-05-21 — ~54 ADA + ~64 DOGE buy orders left live; user
   cancelled them manually on Kraken. Wind down before removing — see
   `feedback_remove_symbol_wind_down_first` memory for the sequence.

2. **Shutdown-cancel logs "canceled" even when the cancel didn't
   send.** During shutdown the private WS goes down before the cancel
   loop completes; cancel attempts fail with `"Cannot cancel order ...
   private WS not connected"` but the tracker can be updated as if
   they succeeded. Compounds issue #1 — the bot believes orders are
   gone when they are not. Not visibly fixed in code; verify before
   relying on clean-shutdown behavior.

## Strong user preferences (saved as memories)

These are **strict** — violating them has caused arguments that derailed
sessions:

1. **No default / fallback values.** Ever. Fail loud, raise, halt entries —
   anything but a silent default that masks a real failure. The user has
   stated this repeatedly. When you suggest code, audit it for `or X`,
   `.get(k, default)`, or try/except-with-default and remove them.

2. **Futures live on Coinbase, not Kraken.** This is a Kraken **spot**
   bot. Don't suggest futures shorts here. The Coinbase US bot owns
   futures legs (which are restricted to US exchange hours).

3. **Use the user's logs, not external APIs.** The raw_ws tape captures
   tick-level data for all symbols. When analyzing past behavior, read
   those files, not Kraken's REST or third-party data. The user has been
   particularly burned by my reaching for external APIs when the local
   data was sufficient.

## Analysis tooling

Three Dash-based interactive tools, each with its own venv and `start.sh`:

- `analysis/hmm_viewer/` (port 8051) — loads a per-bar features
  parquet (built by `analysis/build_state_features.py`), fits
  Gaussian HMMs across a chosen N-state range, overlays per-state
  bands on the price line, auto-saves each fit, and lists / loads /
  deletes saved fits. Includes a `rule_builder` for composing custom
  halt rules from feature conditions; saved rules live in
  `hmm_viewer/rulesets/`.
- `analysis/backtest/` (port 8052) — standalone backtest panel.
  `backtester.py` is a bar-level simulator that originally mirrored
  v2 live-bot semantics (one resting buy per symbol, per-symbol cap,
  spacing, TP at `entry × (1 + tp_pct)`, age-out, maker-only guard,
  optional user-defined halt rules). **Predates regime_v3** — the
  simulator was not ported to chase / ladder mechanics. User
  distrusts backtests as a class (see `feedback_distrust_backtests`
  memory); treat results as suggestive, not predictive.
- `analysis/market_viewer/` — interactive viewer for marking market
  regions and comparing them to classifier output. **Most failures
  came from auto-saving on plotly's `relayoutData` events** —
  re-rendered shapes leak through and get saved as new "user"
  labels, creating phantom regions that compound on every Load.
  Don't auto-save user-input data; require an explicit Save. The
  `labels.bak.<timestamp>` files in the folder are previous label
  snapshots.

Reference docs in `analysis/market_state_and_trading_patterns/`
(strategy notes per regime, including `master_strategy_v1.md`) were
written for the VP / vp_v2 era and pre-date the regime_v3 cleanup —
treat as historical context, not current behavior. Cross-check
against `strategy/regime_v3.py` and `orchestrator/grid_runner.py`
before assuming a documented rule still fires.

## What NOT to do

These are the patterns that keep wasting sessions:

- **Don't use external APIs** when local raw_ws files contain the answer.
- **Don't use default values in functions or data access** (this is a major anti-pattern). 
  Function signatures with defaults (`def foo(bar=10)`), `.get(k, default)`, `or` fallbacks, 
  and try/except-with-default all hide problems. They turn missing configuration, bad data, 
  or incorrect assumptions into silent wrong behavior instead of loud, immediate failures. 
  The user wants to discover bugs quickly through explicit errors, not through mysterious 
  runtime behavior days later. Audit code for these patterns and remove them in favor of 
  strict checks that raise with clear messages.
- **Don't display chart times in UTC** when the user thinks in ET.
- **Don't auto-save** user inputs from `relayoutData` (plotly leaks layout
  shapes back through that event, creating phantom labels).
- **Don't substitute** the classifier's vocabulary for the user's
  vocabulary in their saved labels.
- **Don't keep adding features** when one feature is broken; fix the
  broken one first or admit you can't.
- **Don't claim a fix worked** before the user has restarted the relevant
  process and confirmed visually.
- **Don't write one-shot reconciliation scripts** to paper over
  tracker divergence. The proper periodic reconciler
  (`ExchangeSyncTask`) and startup rebuilder (`StartupReconciler`)
  already exist — use them, and fix them if they miss something. The
  rule isn't "no reconciliation"; it's "no ad-hoc cleanup tools that
  hide the underlying behavior."

## Where to look for current state

- Recent activity: `logs/trading_bot.log` (last few hundred lines)
- Bot uptime: `systemctl status kraken_rules_trading_bot`
- Open positions: `live_trading/trade_state.json` (filter by status)
- Disk usage: `du -sh /mnt/externalssd/raw_ws/`
