"""Paper positions report: open long baskets, open shorts, and the P&L of
each side. Read-only — reads the state files in paper_trading/ and fetches
public prices from Kraken (spot ticker, futures tickers). Safe to run while
the bot service is running.

    venv/bin/python tools/positions.py

Longs are marked the way the bot's exit is checked: spot mid.
Shorts are shown per token basket (average entry, cover target, basket stop). Net P&L is
after the buy fees and the estimated taker fee of the market sell (rate
from the bot's last FeeCache log line). Shorts are marked at the perp mid;
net is after both taker fees (exit estimated) plus funding received. The short
cover target uses the bot's last journaled drop target.
"""
import json
import re
import sys
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import ccxt

ROOT = Path(__file__).resolve().parent.parent
STATE = ROOT / 'paper_trading'
LOG = ROOT / 'logs' / 'trading_bot.log'
ET = ZoneInfo('America/New_York')
FUTURES_TICKERS = 'https://futures.kraken.com/derivatives/api/v3/tickers'

GREEN, RED, DIM, BOLD, RESET = '\033[32m', '\033[31m', '\033[2m', '\033[1m', '\033[0m'
if not sys.stdout.isatty():
    GREEN = RED = DIM = BOLD = RESET = ''


def parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace('Z', '+00:00'))


def et(s: str) -> str:
    return parse_ts(s).astimezone(ET).strftime('%m-%d %H:%M')


def money(x: float) -> str:
    color = GREEN if x > 0 else RED if x < 0 else ''
    return f"{color}{'+' if x >= 0 else '-'}${abs(x):,.2f}{RESET}"


def pct(x: float) -> str:
    color = GREEN if x > 0 else RED if x < 0 else ''
    return f"{color}{x:+.2f}%{RESET}"


def load_json(name: str):
    with open(STATE / name) as f:
        return json.load(f)


def taker_fees() -> dict:
    """{symbol: taker %} from the bot's latest FeeCache startup lines."""
    pat = re.compile(r'\[FeeCache\] (\S+) maker=[\d.]+% taker=([\d.]+)%')
    fees = {}
    with open(LOG) as f:
        for line in f:
            m = pat.search(line)
            if m:
                fees[m.group(1)] = float(m.group(2))
    return fees


def swing_targets() -> dict:
    """{symbol: latest swing_target journal event}."""
    out = {}
    with open(STATE / 'journal.jsonl') as f:
        for line in f:
            if '"swing_target"' not in line:
                continue
            ev = json.loads(line)
            if ev['event'] == 'swing_target':
                out[ev['symbol']] = ev
    return out


def drop_targets() -> dict:
    """{symbol: latest drop_target journal event}."""
    out = {}
    with open(STATE / 'journal.jsonl') as f:
        for line in f:
            if '"drop_target"' not in line:
                continue
            ev = json.loads(line)
            if ev['event'] == 'drop_target':
                out[ev['symbol']] = ev
    return out


def spot_mids(symbols) -> dict:
    tickers = ccxt.kraken().fetch_tickers(sorted(symbols))
    return {s: (t['bid'] + t['ask']) / 2 for s, t in tickers.items()}


def perp_mids(products) -> dict:
    with urllib.request.urlopen(FUTURES_TICKERS, timeout=15) as r:
        data = json.load(r)
    if data['result'] != 'success':
        raise RuntimeError(f"futures tickers: {data}")
    by = {t['symbol']: t for t in data['tickers']}
    out = {}
    for p in products:
        t = by[p]
        out[p] = (float(t['bid']) + float(t['ask'])) / 2
    return out


def main() -> None:
    cfg = json.load(open(ROOT / 'config.json'))
    sg = cfg['short_grid']
    starting_cash = float(cfg['paper_trading']['starting_cash_usd'])

    account = load_json('paper_account.json')
    cycles = load_json('order_cycles.json')
    shorts = load_json('paper_shorts.json')
    shorts_hist_path = STATE / 'paper_shorts_history.jsonl'
    shorts_hist = ([json.loads(l) for l in open(shorts_hist_path) if l.strip()]
                   if shorts_hist_path.exists() else [])

    # Filled ladder buys with no sell, and resting buy rungs, per symbol.
    baskets = defaultdict(list)
    resting = defaultdict(list)
    for cyc in cycles.values():
        buy = cyc['buy']
        if cyc['sell'] is not None:
            continue
        if buy['status'] == 'filled':
            baskets[cyc['symbol']].append(buy)
        elif buy['status'] in ('new', 'open', 'partially_filled'):
            resting[cyc['symbol']].append(buy)

    held = {f"{b}/USD" for b, q in account['holdings'].items() if q > 0}
    spot = spot_mids(set(baskets) | set(resting) | held)
    perp = perp_mids({p['product'] for p in shorts['open']})
    taker = taker_fees() if baskets else {}
    targets = swing_targets()

    now = datetime.now(timezone.utc)
    print(f"{BOLD}Paper positions — {now.astimezone(ET).strftime('%Y-%m-%d %H:%M:%S ET')}{RESET}\n")

    # ── Longs ───────────────────────────────────────────────────────────
    print(f"{BOLD}LONGS — open baskets (filled ladder buys awaiting the basket exit){RESET}")
    long_unreal = 0.0
    tracked_qty = defaultdict(float)
    if not baskets:
        print("  none")
    for sym in sorted(baskets):
        buys = baskets[sym]
        fills = [f for b in buys for f in b['fills']]
        qty = sum(f['qty'] for f in fills)
        cost = sum(f['qty'] * f['price'] for f in fills)
        buy_fee = sum(f['fee'] for f in fills)
        avg = cost / qty
        mid = spot[sym]
        if sym not in taker:
            raise RuntimeError(f"no FeeCache taker line for {sym} in {LOG}")
        sell_fee = qty * mid * taker[sym] / 100
        net = qty * mid - cost - buy_fee - sell_fee
        long_unreal += net
        tracked_qty[sym.split('/')[0]] += qty
        print(f"  {BOLD}{sym}{RESET}  {len(buys)} buy(s)  qty {qty:.8g}  avg {avg:.8g}  "
              f"mid {mid:.8g}  ({pct((mid / avg - 1) * 100)})")
        for b in sorted(buys, key=lambda b: b['fills'][0]['timestamp']):
            fq = sum(f['qty'] for f in b['fills'])
            fp = sum(f['qty'] * f['price'] for f in b['fills']) / fq
            print(f"      {DIM}filled {et(b['fills'][-1]['timestamp'])} ET  {fq:.8g} @ {fp:.8g}  "
                  f"({(mid / fp - 1) * 100:+.2f}%){RESET}")
        line = f"      net if sold now {money(net)} ({pct(net / (cost + buy_fee) * 100)})"
        tgt = targets.get(sym)
        if tgt is not None:
            # Same formula as hybrid_grid._ladder_exits / calculate_tp_price.
            tp = avg * (1 + (buy_fee / cost * 100 + taker[sym] + tgt['target_pct']) / 100)
            line += (f"   target {tp:.8g} (+{tgt['target_pct']:.2f}% {tgt['rule']}), "
                     f"{(tp / mid - 1) * 100:.2f}% away")
        print(line)

    untracked = {b: q - tracked_qty[b] for b, q in account['holdings'].items()
                 if q - tracked_qty[b] > 1e-9}
    if untracked:
        print(f"\n  {DIM}Held but not in a basket (no exit managed by the bot):{RESET}")
        for b, q in sorted(untracked.items()):
            print(f"  {DIM}  {b}  qty {q:.8g}  worth ${q * spot[b + '/USD']:,.2f}{RESET}")

    print(f"\n  {DIM}Resting buy rungs:{RESET}")
    if not resting:
        print(f"  {DIM}  none{RESET}")
    for sym in sorted(resting):
        prices = sorted((b['limit_price'] for b in resting[sym]), reverse=True)
        usd = sum(b['order_qty'] * b['limit_price'] for b in resting[sym])
        print(f"  {DIM}  {sym:<9} {len(prices)} rungs ${usd:,.0f}  "
              f"highest {prices[0]:.6g} ({(prices[0] / spot[sym] - 1) * 100:+.2f}% from mid)  "
              f"lowest {prices[-1]:.6g}{RESET}")

    spot_equity = account['cash'] + sum(
        q * spot[f"{b}/USD"] for b, q in account['holdings'].items() if q > 0)
    long_total = spot_equity - starting_cash

    # ── Shorts ──────────────────────────────────────────────────────────
    # Each token's open shorts are one basket (strategy/short_grid.py): covered at
    # avg entry × (1 − taker − drop target × multiplier), or stopped at avg entry × (1 + stop).
    taker = float(sg['taker_fee_pct'])
    print(f"\n{BOLD}SHORTS — open perp short baskets{RESET}")
    short_unreal_net = 0.0
    short_unreal_gross = 0.0
    if not shorts['open']:
        print("  none")
    by_sym = defaultdict(list)
    for p in shorts['open']:
        by_sym[p['symbol']].append(p)
    drops = drop_targets()
    for sym in sorted(by_sym):
        ps = by_sym[sym]
        mid = perp[ps[0]['product']]
        qty = sum(p['qty'] for p in ps)
        avg = sum(p['qty'] * p['entry_price'] for p in ps) / qty
        gross = sum(p['qty'] * (p['entry_price'] - mid) for p in ps)
        fees_in = sum(p['entry_fee'] for p in ps)
        funding = sum(p['funding'] for p in ps)
        net = gross - fees_in - qty * mid * taker / 100 + funding
        short_unreal_gross += gross
        short_unreal_net += net
        stop = avg * (1 + float(sg['stop_pct']) / 100)
        line = (f"stop {stop:.6g} ({(stop / mid - 1) * 100:+.2f}%)")
        d = drops.get(sym)
        if d is not None:
            tgt_pct = d['target_pct'] * float(sg['target_multiplier'])
            cover = avg * (1 - taker / 100 - tgt_pct / 100)
            line = (f"covers at {cover:.6g} ({(cover / mid - 1) * 100:+.2f}%, "
                    f"{tgt_pct:.2f}% below avg)  " + line)
        print(f"  {BOLD}{sym:<9}{RESET} {len(ps)} shorts  qty {qty:<10.6g} avg entry {avg:<10.6g} "
              f"mid {mid:<10.6g} {pct((1 - mid / avg) * 100)}  net {money(net)}")
        print(f"      {DIM}{line}  funding {funding:+.4f}  first {et(min(p['opened_at'] for p in ps))} ET{RESET}")

    realized = sum(h['net_pnl'] for h in shorts_hist)
    short_equity = shorts['cash'] + short_unreal_gross   # same as the bot's equity()
    notional = sum(p['qty'] * perp[p['product']] for p in shorts['open'])

    # ── Summary ─────────────────────────────────────────────────────────
    total = spot_equity + short_equity
    print(f"\n{BOLD}PORTFOLIO P&L{RESET}")
    print(f"  Long   open baskets {money(long_unreal)} if sold now  |  "
          f"total since start {money(long_total)}  (spot equity ${spot_equity:,.2f})")
    print(f"  Short  {len(shorts['open'])} open, notional ${notional:,.2f}, "
          f"{money(short_unreal_net)} if covered now  |  "
          f"closed {len(shorts_hist)}: {money(realized)}  |  "
          f"total since start {money(short_equity)}")
    print(f"  {DIM}         short totals: gross realized {shorts['gross_realized']:+.2f}, "
          f"fees {-shorts['fees_paid']:+.2f}, funding {shorts['funding_received']:+.4f}{RESET}")
    print(f"  {BOLD}Total  ${total:,.2f}  {money(total - starting_cash)} "
          f"({pct((total / starting_cash - 1) * 100)}){RESET}")
    print(f"  {DIM}'total since start' marks open positions at mid before exit fees; "
          f"'if sold/covered now' includes them.{RESET}")


if __name__ == '__main__':
    main()
