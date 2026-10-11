"""TradeJournal — append-only JSONL record of every trading decision and fill.

One line per event in `<state_dir>/journal.jsonl` (paper_trading/ in dry run,
live_trading/ live). Never rotated, unlike the text log, so it is the source
for reconstructing what the bot did, why, and what it earned.

Every line has `ts` (UTC ISO) and `event`. Events:
  bot_start        dry_run, symbols, ladder / short-grid / aggregate-target settings
  regime_change    symbol, from, to
  buy_gate         symbol, allowed, reason              (200-day gate flips)
  short_gate       symbol, allowed, reason              (50-day gate flips)
  swing_target     symbol, rule (swing / below_short_sma / warmup / stale), target_pct,
                   reason                               (when a token's target changes)
  ladder_action    symbol, action (place_limit_buy / cancel_order / place_market_sell),
                   reason, mid, regime, buys_allowed, plus the action's own fields
                   (qty, price, order_id, cl_ord_id, and for the aggregate sell:
                   avg_cost, target_price, total_qty, cycles, buy_fees, taker_fee_pct,
                   profit_pct, target_rule, target_reason)
  fill             symbol, side, order_id, cl_ord_id, price, qty, fee
  ladder_closed    symbol, sell order_id, cycles, qty, buy_cost, buy_fees,
                   sell_proceeds, sell_fees, net_pnl, return_pct
  short_open       symbol, product, id, level, qty, trigger_mid, fill_price,
                   slippage_pct, fee, total_equity, notional_before
  short_activated  symbol, id, entry, mid
  short_cover      symbol, id, reason (stop / trail), trigger_mid, fill_price,
                   slippage_pct, gross_pnl, fees, funding, net_pnl, return_pct
  short_funding    hour, total, per-position amounts
"""
import json
from datetime import datetime, timezone
from pathlib import Path


class TradeJournal:

    def __init__(self, state_dir):
        self.path = Path(state_dir) / 'journal.jsonl'
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, event: str, **fields) -> None:
        row = {'ts': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
               'event': event}
        row.update(fields)
        with open(self.path, 'a') as f:
            f.write(json.dumps(row) + '\n')
