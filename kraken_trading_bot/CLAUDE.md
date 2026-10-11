# Kraken Long/Short Bot (v3 copy) — Context Document

This repo is a **copy** of the original Kraken spot bot, made on 2026-10-09
from `crypto_trading_bots@2df2582`. All long/short development happens here.

## Ground rules (strict)

- **Never modify the original bot** (`~/crypto_trading_bots/kraken_trading_bot`)
  or push to its GitHub repo. This copy's only remote is
  `git@github.com:mjjohnson0417/crypto_trading_bots_long_short.git`.
- **Describe changes before making them**; the user wants to be notified.
  **Commit and push only when the user says so.**
- **No default / fallback values.** No `def f(x=10)`, `.get(k, default)`,
  `or X`, or try/except-with-default in new code. Fail loud with a clear
  error. (Older inherited code still has some of these, e.g. in
  `trading_runner._process_order_event` and the reconcilers. Don't add more.)
- **Use local data, not external APIs**, for analysis of past behavior. The
  raw_ws tape on the SSD has tick-level spot and perp data. Research lives in
  `~/strategy_lab` (separate repo); don't put analysis code in this repo.
- **Times for the user are ET.** Bot logs are local ET; raw_ws records and
  filenames are UTC.
- **The user distrusts backtests.** Treat lab results as suggestive.
- Don't write one-shot reconciliation scripts. Fix `StartupReconciler` /
  `ExchangeSyncTask` instead.
- Don't keep adding features while one is broken; fix it first.
- Don't claim a fix works until the user has restarted and confirmed.

## What the bot does now

Spot DCA ladder on Kraken (`config.json` `symbols`, 10 tokens; CC removed 2026-10-10) with a daily
trend gate. Strategy: `strategy/hybrid_grid.py`, runner:
`orchestrator/trading_runner.py`.

- **5-state classifier** (`state_machine/regime_classifier.py`): sma_1h vs
  sma_4h, slope ±0.3%, 4h width < 1% ⇒ SIDEWAYS; 90s hysteresis. Ladder runs
  in every state except STRONG_UPTREND (chase is disabled in config).
  Down states use anchored spacing. The classifier flips about 16×/token/day
  and has no directional edge (strategy_lab finding); it mainly throttles.
- **Ladder:** 5 rungs, `spacing_pct` 1.5, `proximity_pct` 1.0, each rung
  `position_size_pct` 0.25% of portfolio. Ratchets with price.
- **Exit:** one aggregate market sell per symbol when mid ≥ weighted avg
  cost × (1 + buy fee% + **taker** fee% + target%). No stop-losses. The target
  comes from `market_data/swing_target.py` (SwingTargets, config `aggregate_target`,
  added 2026-10-11 from strategy_lab `analysis/swing_targets_down.py`):
  - previous daily close below the 50-day SMA → **+2%** (get back to cash on small rebounds)
  - otherwise **clip(1.0 × median of the last 6 completed 2% rallies, 1%, 8%)**, from a
    causal zigzag on the last 720 completed hourly candles (Kraken REST, refreshed hourly;
    the zigzag is the lab's `causal_dc`)
  - `warmup_pct` (3%) when fewer than 6 rallies exist, or when the hourly data is more
    than 3h stale (logged as an error). Startup fails if hourly candles can't load.
  - `ladder_defaults.profit_pct` (3) is now only encoded in ladder buy cl_ord_ids.
  - Backtest (2022 – mid-2026, $50K, classic fee tiers): 267% vs 176% for fixed +3%,
    worst drop −15.3% vs −17.6%; worse worst single-year drop (−22% vs −18%).
- **200-day buy gate** (`market_data/daily_trend.py`, DailyTrendGate):
  - Rule: buys are paused while the previous completed UTC day's close is below its 200-day SMA.
  - While paused: resting buys are canceled (re-swept every 60s); exits still run.
  - Startup fails if daily candles can't load. Buys pause if the daily data is more than 26h stale.
  - SMA50 is computed and logged for the future short gate.
- **Fees** (`exchange/fee_cache.py`): account volume-tier rates from Kraken, refreshed hourly. As of 2026-10-10: maker 0.20%, taker 0.35%. Fill fees are read from Kraken WS v2 `fees` (per execution) and `last_price`; there is **no `fee` field**. The original bot reads `fee`, so it records every fill fee as $0.

## Paper short grid (dry run only)

Trailing short grid on **Kraken Futures perps**, a copy of strategy_lab's
`lab/short_grid_core.py` mode 3 (settings from `analysis/export_rules_chart.py`).
Code: `strategy/short_grid.py` (rules) + `exchange/paper_futures.py` (paper
account); config `short_grid`; runner task `short_grid`.

- **Gate** (`short_grid.gate`): `below_short_sma` = previous daily close < 50-day SMA
  (the backtested choice); `below_either_sma` = below the 50d or the 200d. Stale
  daily data (> 26h) blocks new shorts. Shorts are NOT closed when the gate turns off.
- **Entry:** once per minute (lab used 1-minute closes) on the perp book mid:
  level = floor(ln(mid)/ln(1.01)); a cross down into a level with no open short
  opens one. Size 0.5% of total equity; skipped if short notional would exceed
  30% of total equity. Total equity = spot paper equity + short equity (one pot).
- **Exit,** each short on its own, every tick: 3% hard stop until ahead by
  5% + round-trip fees (2 × 0.05% taker); then the stop is dropped and it covers
  when mid rises 8% off its trough, never below 0.5% + fees profit.
- **Fills** walk the live perp book (taker). Qty rounds down to the contract
  precision. **Funding** is credited hourly: qty × mark × `relative_funding_rate`.
- **Not modelled:** margin, liquidation (initial margin is only reported).
- **Files** in `paper_trading/`: `paper_shorts.json` (ledger + open shorts),
  `paper_shorts_history.jsonl` (each covered short), `paper_shorts_equity.jsonl`
  (one line per minute). Log line: `PAPER shorts ... TOTAL=$`.
- **Live mode** refuses to start with `short_grid.enabled`: no live futures order
  code exists. That needs separate Futures API keys (perp wallet unfunded); the
  user will add keys once paper results show the strategy works.

## Dry run = paper trading

`trading.dry_run: true` (current setting) routes every placement, cancel,
order query and balance query to `exchange/paper_exchange.py`:
- **Events:** emits Kraken WS v2 executions events, so the normal runner path tracks paper orders.
- **Limit fills:** a resting limit fills when a live public trade prints *strictly through* its price, at the limit price with the maker fee.
- **Market orders:** walk the live book and pay the taker fee.
- **Account rules:** funds are reserved for resting orders, and Kraken minimums are enforced.
- **State:** persists in `paper_trading/` (gitignored):
  - `paper_account.json`
  - `order_cycles.json` and `order_cycles_history.json` (tracker)
  - `paper_equity.jsonl` (one mark-to-market line per minute)
- **Starting cash:** `paper_trading.starting_cash_usd` (10000).

Live mode uses `live_trading/` for the tracker. Paper and live state never mix.

## Data feeds

| Source | What | Code |
|---|---|---|
| `wss://ws.kraken.com/v2` | spot book (depth 100), trades, ticker | `exchange/public_ws.py` → `market_data/{order_book,trades,ticker}.py` |
| `wss://ws-auth.kraken.com/v2` | order placement + executions | `exchange/private_ws.py` |
| `wss://futures.kraken.com/ws/v1` | perp ticker (mark, index, funding, OI), trades, full book | `exchange/futures_public_ws.py` → `market_data/perp_data.py` (`dm.perps`) |
| Kraken spot REST (ccxt) | 15m klines seed, 1d candles, 1h candles (swing targets), fees, balances, prices, order queries | various |
| Futures REST (public) | instrument list, validated at startup | `fetch_futures_instruments` |

- **Perp mapping:** explicit in `perps.products`. CC/USD has no perp.
- **Book gaps:** a perp book sequence gap triggers a reconnect and resnapshot.
- **Watchdogs:** each WS has one that exits the process (systemd restarts it) after `ws_unhealthy_exit_seconds` of silence while the network is up.
- **Task deaths:** `main.py` also exits 1 if the trading or sync task dies.
- **`[PerpStatus]`** logs basis, funding and book health every 5 min.

## Trade journal (the record for analysis)

`persistence/journal.py` appends one JSON line per event to
`<state_dir>/journal.jsonl` (`paper_trading/` in dry run, `live_trading/` live).
Never rotated — the text log keeps only ~30 MB (5 MB × 6). Use the journal, not
the text log, to reconstruct trades.

- **Decisions:** `ladder_action` per placed/canceled order with `reason`
  (`build`, `refill`, `ratchet_up`, `excess_rung`, `enter_strong_uptrend`,
  `enter_downtrend`, `exit_downtrend`, `buy_gate_paused`, `aggregate_target`, chase
  reasons), `mid`, `regime`, `buys_allowed`, dispatch `error`. The aggregate sell
  also has `avg_cost`, `target_price`, `cycles`, `buy_fees`, `taker_fee_pct`,
  `profit_pct`, `target_rule` (`swing` / `below_short_sma` / `warmup` / `stale`), `target_reason`.
- **State:** `regime_change`, `buy_gate`, `short_gate` (flips only), `swing_target`
  (when a token's target rule or value changes), `bot_start` (settings).
- **Results:** `fill` (every fill, both sides), `ladder_closed` (realized P&L of each
  aggregate sell), `short_open` / `short_activated` / `short_cover` (with trigger mid,
  fill price, slippage, net P&L), `short_funding` (hourly).
- The text log gets one `<symbol> decisions mid=… regime=… buys=…: N× action:reason`
  line per tick that acts, and `regime A → B` lines.
- `order_cycles_history.json` still collects zero-fill canceled rungs (archived on
  cancel; reconcilers read it for known order ids), so it is noisy; the journal is clean.

## Order tracking and reconciliation

- **Tracker** (`persistence/trade_tracker.py`): `order_cycles.json`, keyed by
  buy order_id, with `{symbol, buy, sell}`; each side has status and fills
  `[{price, qty, fee, timestamp}]`. Zero-fill canceled buys are pruned.
- **cl_ord_id prefixes:** `l-<bp>-<rand>` ladder buy, `c-` chase buy, `s-` chase TP sell, `a-` aggregate ladder sell.
- **StartupReconciler** (`execution/reconciliation.py`): runs once before trading. It cancels stale open buys, rebuilds sells for filled buys, and adopts orphans.
- **ExchangeSyncTask** (`persistence/exchange_sync.py`): runs on every private-WS (re)connect (not a timer). It reconciles tracker vs exchange and checks the tracker against balances. It never cancels.
- **Known gap:** symbols removed from config fall outside the reconcilers. Wind down their orders before removing a symbol.

## Operations

- **Service:** `kraken_long_short_bot.service` (installed in
  `/etc/systemd/system/`, enabled). Restart=on-failure, RestartSec=30, no
  start-rate limit, waits for network + `/mnt/externalssd`. Logs:
  `journalctl -u kraken_long_short_bot` and `logs/trading_bot.log`.
- **Keys:** `/home/jason/api/kraken/kraken_api_keys.json.gpg`, decrypted via age.
  Env: `TRADING_PASSPHRASE_AGE_FILE=/etc/kraken/trading_passphrase.age`,
  `AGE_IDENTITY_FILE=/home/jason/.age/key.txt`.
- **venv:** `./venv` built from `requirements-bot.txt` (ccxt, pandas, numpy,
  websockets, pytest). `requirements.txt` is the old, analysis-heavy list.
- **Raw tape:** `/mnt/externalssd/raw_ws/{book,trade,ticker,perp_book,perp_trade,perp_ticker}/<sym>_<chan>_<YYYYMMDD UTC>.jsonl`.
  - User crontab gzips finished days at 21:15 ET (`telemetry/gzip_raw_ws.py`).
  - The script deletes the oldest files if the disk passes 90%.
  - The perp book is about 9 GB/day before gzip.
- **Manual run** (dry run is safe; it only reads the real account):
  `TRADING_PASSPHRASE_AGE_FILE=... AGE_IDENTITY_FILE=... venv/bin/python main.py`.
  Don't run it while the service is running.

## Where to look

- Recent activity: `logs/trading_bot.log` (ET timestamps)
- Paper performance: `paper_trading/paper_equity.jsonl`, `order_cycles_history.json`
- Strategy research and the rules chart: `~/strategy_lab` (`claude_notes.txt` is the user's file)
- `docs/` contains notes inherited from the original; cross-check against code.
