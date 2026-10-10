"""PaperExchange — simulated Kraken account used when `trading.dry_run` is on.

OrderOperations delegates every placement, cancel, open/closed-order query
and balance query to this class in dry run. Prices stay real: fills are
decided from the live public trade tape and order book.

Order events are pushed onto `data_manager.executions_queue` in Kraken WS v2
executions format (pending_new → new → trade/filled, or canceled), so the
TradingRunner's normal execution handling updates the tracker exactly as it
does for live orders. Query methods return ccxt-shaped dicts, so
StartupReconciler, ExchangeSyncTask and ShutdownManager run unchanged
against the paper account.

Fill model:
  - Limit buy rests until a public trade prints STRICTLY BELOW its limit
    (limit sell: strictly above). It then fills in full at the limit price
    with the maker fee. A limit that crosses the book at placement fills
    immediately as a taker at the touch.
  - Market orders walk the live order book and pay the taker fee.
  - Funds are reserved for resting orders; an order the paper account
    cannot cover is rejected the way Kraken rejects it.
  - Orders below Kraken's minimum size / cost are rejected.

State persists in `<state_dir>/paper_account.json` (cash, holdings, orders),
so a restart continues the same paper account. Closed orders are kept for
`closed_order_retention_hours` so the reconcilers' lookback can see them.
"""
import asyncio
import json
import math
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from logging_config import get_logger


QUOTE = 'USD'


class PaperOrderError(Exception):
    """A paper order Kraken would reject. Message mirrors Kraken's error."""


class PaperExchange:

    def __init__(self, *, symbols, data_manager, fee_cache, markets_source,
                 config, state_dir):
        self.logger = get_logger('PaperExchange')
        self.symbols = [s.upper() for s in symbols]
        self.dm = data_manager
        self.fee_cache = fee_cache
        self.markets_source = markets_source  # ccxt client; .markets loaded at boot

        self.starting_cash_usd = float(config['starting_cash_usd'])
        self.fill_check_interval_seconds = float(config['fill_check_interval_seconds'])
        self.closed_order_retention_hours = float(config['closed_order_retention_hours'])
        if self.starting_cash_usd <= 0:
            raise ValueError("paper_trading.starting_cash_usd must be > 0")

        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.state_dir / 'paper_account.json'
        self.equity_file = self.state_dir / 'paper_equity.jsonl'
        self._load()

        # Highest public trade_id already checked against resting orders.
        self._last_trade_id = {s: None for s in self.symbols}

    # ── Persistence ─────────────────────────────────────────────────────

    def _load(self) -> None:
        if self.state_file.exists():
            with open(self.state_file) as f:
                st = json.load(f)
            self.cash = float(st['cash'])
            self.holdings = {k: float(v) for k, v in st['holdings'].items()}
            self.orders = st['orders']
            self.created_at = st['created_at']
            self.logger.warning(
                f"PAPER account loaded: cash=${self.cash:.2f} "
                f"holdings={self._fmt_holdings()} "
                f"open_orders={sum(1 for o in self.orders.values() if o['status'] == 'open')}"
            )
        else:
            self.cash = self.starting_cash_usd
            self.holdings = {}
            self.orders = {}
            self.created_at = self._now_iso()
            self._save()
            self.logger.warning(
                f"PAPER account created with ${self.cash:.2f} cash "
                f"({self.state_file})"
            )

    def _save(self) -> None:
        tmp = self.state_file.with_suffix('.json.tmp')
        with open(tmp, 'w') as f:
            json.dump({
                'created_at': self.created_at,
                'cash': self.cash,
                'holdings': self.holdings,
                'orders': self.orders,
            }, f, indent=2)
        tmp.replace(self.state_file)

    def _fmt_holdings(self) -> str:
        return '{' + ', '.join(
            f"{k}: {v:.8g}" for k, v in sorted(self.holdings.items()) if v > 0
        ) + '}'

    # ── Helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

    @staticmethod
    def _new_order_id() -> str:
        h = uuid.uuid4().hex.upper()
        return f"P{h[:5]}-{h[5:10]}-{h[10:16]}"

    def _market(self, symbol: str) -> dict:
        markets = self.markets_source.markets
        if not markets:
            raise RuntimeError("ccxt markets not loaded")
        if symbol not in markets:
            raise KeyError(f"symbol {symbol!r} not in ccxt markets")
        return markets[symbol]

    def _check_minimums(self, symbol: str, qty: float, price: float) -> None:
        limits = self._market(symbol)['limits']
        min_qty = limits['amount']['min']
        if min_qty is None:
            raise RuntimeError(f"{symbol}: ccxt market has no amount minimum")
        if qty < min_qty:
            raise PaperOrderError(
                f"EOrder:Order minimum not met (volume minimum not met): "
                f"{symbol} qty={qty} < min {min_qty}"
            )
        min_cost = limits['cost']['min']
        if min_cost is not None and qty * price < min_cost:
            raise PaperOrderError(
                f"EOrder:Order minimum not met (cost minimum not met): "
                f"{symbol} cost={qty * price:.4f} < min {min_cost}"
            )

    def _book_side(self, symbol: str, side: str):
        """Return [(price, qty), ...] best-first for 'bid' or 'ask'."""
        book = self.dm.book.get(symbol)
        levels = book[book['side'] == side]
        if levels.empty:
            raise RuntimeError(f"{symbol}: paper fill needs the order book but {side}s are empty")
        levels = levels.sort_index(ascending=(side == 'ask'))
        return list(zip(levels.index.astype(float), levels['qty'].astype(float)))

    def _walk_book(self, symbol: str, side: str, qty: float):
        """Fill `qty` against the book. Returns avg price. If the visible
        book is thinner than qty, the remainder fills at the last level
        and a warning is logged."""
        remaining = qty
        cost = 0.0
        last_price = None
        for price, level_qty in self._book_side(symbol, side):
            take = min(remaining, level_qty)
            cost += take * price
            remaining -= take
            last_price = price
            if remaining <= 1e-15:
                break
        if remaining > 1e-15:
            self.logger.warning(
                f"{symbol} paper market order {qty} deeper than visible book; "
                f"{remaining} filled at last level {last_price}"
            )
            cost += remaining * last_price
        return cost / qty

    def _reserved_usd(self) -> float:
        return sum(o['reserved_usd'] for o in self.orders.values()
                   if o['status'] == 'open' and o['side'] == 'buy')

    def _reserved_base(self, base: str) -> float:
        return sum(o['amount'] for o in self.orders.values()
                   if o['status'] == 'open' and o['side'] == 'sell'
                   and o['symbol'].split('/')[0] == base)

    def free_usd(self) -> float:
        return self.cash - self._reserved_usd()

    def free_base(self, base: str) -> float:
        held = self.holdings[base] if base in self.holdings else 0.0
        return held - self._reserved_base(base)

    # ── Event emission (Kraken WS v2 executions shape) ──────────────────

    def _emit(self, order: dict, exec_type: str, status: str, trade: dict = None) -> None:
        ev = {
            'exec_type': exec_type,
            'order_status': status,
            'order_id': order['id'],
            'cl_ord_id': order['cl_ord_id'],
            'symbol': order['symbol'],
            'side': order['side'],
            'order_type': order['type'],
            'order_qty': order['amount'],
            'cum_qty': order['filled'],
            'cum_cost': order['cost'],
            'avg_price': order['average'],
            'timestamp': self._now_iso(),
        }
        if order['type'] == 'limit':
            ev['limit_price'] = order['price']
        if trade is not None:
            ev.update({
                'exec_id': uuid.uuid4().hex[:18].upper(),
                'last_qty': trade['qty'],
                'last_price': trade['price'],
                'cost': trade['qty'] * trade['price'],
                'fees': [{'asset': QUOTE, 'qty': trade['fee']}],
                'fee_usd_equiv': trade['fee'],
                'liquidity_ind': trade['liquidity'],
            })
        if status == 'canceled':
            ev['reason'] = 'User requested'
        self.dm.executions_queue.put_nowait(
            {'channel': 'executions', 'type': 'update', 'data': [ev]}
        )

    # ── Placement ───────────────────────────────────────────────────────

    def _new_order(self, symbol, side, ord_type, qty, price, cl_ord_id) -> dict:
        trades = self.dm.trades.get(symbol)
        after_id = int(trades['trade_id'].iloc[-1]) if not trades.empty else None
        return {
            'id': self._new_order_id(),
            'cl_ord_id': cl_ord_id,
            'symbol': symbol,
            'side': side,
            'type': ord_type,
            'amount': qty,
            'price': price,
            'status': 'open',
            'filled': 0.0,
            'cost': 0.0,
            'average': 0.0,
            'fee': 0.0,
            'reserved_usd': 0.0,
            'after_trade_id': after_id,
            'created': self._now_iso(),
            'closed': None,
        }

    def place(self, symbol, side, ord_type, qty, price, cl_ord_id) -> str:
        """Place a paper order. Returns order_id; raises PaperOrderError
        with a Kraken-style message if Kraken would reject it."""
        symbol = symbol.upper()
        if symbol not in self.symbols:
            raise PaperOrderError(f"EQuery:Unknown asset pair {symbol}")
        base = symbol.split('/')[0]

        if ord_type == 'market':
            ref_price = self._book_side(symbol, 'ask' if side == 'buy' else 'bid')[0][0]
        else:
            ref_price = price
        self._check_minimums(symbol, qty, ref_price)

        order = self._new_order(symbol, side, ord_type, qty, price, cl_ord_id)

        if side == 'buy':
            fee_pct = self.fee_cache.taker(symbol) if ord_type == 'market' else self.fee_cache.maker(symbol)
            need = qty * ref_price * (1 + fee_pct / 100.0)
            if need > self.free_usd() + 1e-9:
                raise PaperOrderError(
                    f"EOrder:Insufficient funds: {symbol} buy needs ${need:.2f}, "
                    f"free ${self.free_usd():.2f}"
                )
            if ord_type == 'limit':
                order['reserved_usd'] = need
        else:
            if qty > self.free_base(base) + 1e-12:
                raise PaperOrderError(
                    f"EOrder:Insufficient funds: {symbol} sell {qty} > free "
                    f"{base} {self.free_base(base)}"
                )

        self.orders[order['id']] = order
        self._save()
        self._emit(order, 'pending_new', 'pending_new')
        self._emit(order, 'new', 'new')

        if ord_type == 'market':
            px = self._walk_book(symbol, 'ask' if side == 'buy' else 'bid', qty)
            self._fill(order, px, 'taker')
        else:
            # A limit that crosses the book executes immediately as taker.
            if side == 'buy':
                best_ask = self._book_side(symbol, 'ask')[0][0]
                if price >= best_ask:
                    self._fill(order, best_ask, 'taker')
            else:
                best_bid = self._book_side(symbol, 'bid')[0][0]
                if price <= best_bid:
                    self._fill(order, best_bid, 'taker')
        return order['id']

    def _fill(self, order: dict, price: float, liquidity: str) -> None:
        symbol = order['symbol']
        base = symbol.split('/')[0]
        qty = order['amount']
        fee_pct = self.fee_cache.taker(symbol) if liquidity == 'taker' else self.fee_cache.maker(symbol)
        cost = qty * price
        fee = cost * fee_pct / 100.0
        held = self.holdings[base] if base in self.holdings else 0.0
        if order['side'] == 'buy':
            self.cash -= cost + fee
            self.holdings[base] = held + qty
        else:
            self.cash += cost - fee
            self.holdings[base] = held - qty
        order.update({
            'status': 'closed', 'filled': qty, 'cost': cost, 'average': price,
            'fee': fee, 'reserved_usd': 0.0, 'closed': self._now_iso(),
        })
        self._save()
        self.logger.info(
            f"PAPER fill {order['side']} {symbol} qty={qty:.8g} @ {price:.8g} "
            f"({liquidity}, fee ${fee:.4f}) cash=${self.cash:.2f}"
        )
        self._emit(order, 'trade', 'filled',
                   trade={'qty': qty, 'price': price, 'fee': fee,
                          'liquidity': 'm' if liquidity == 'maker' else 't'})
        self._emit(order, 'filled', 'filled')

    def cancel(self, order_id: str) -> str:
        """'success' if a resting order was canceled, 'unknown' otherwise
        (same contract as OrderOperations.cancel_order)."""
        order = self.orders[order_id] if order_id in self.orders else None
        if order is None or order['status'] != 'open':
            return 'unknown'
        order.update({'status': 'canceled', 'reserved_usd': 0.0,
                      'closed': self._now_iso()})
        self._save()
        self._emit(order, 'canceled', 'canceled')
        return 'success'

    # ── Resting-order fills from the public trade tape ──────────────────

    async def run(self, shutdown_event: asyncio.Event) -> None:
        self.logger.info("PaperExchange fill loop started")
        while not shutdown_event.is_set():
            try:
                self._check_fills()
                self._prune_closed()
            except Exception as e:
                self.logger.error(f"PaperExchange fill check failed: {e}", exc_info=True)
            await asyncio.sleep(self.fill_check_interval_seconds)

    def _check_fills(self) -> None:
        for symbol in self.symbols:
            trades = self.dm.trades.get(symbol)
            if trades.empty:
                continue
            last_seen = self._last_trade_id[symbol]
            new = trades if last_seen is None else trades[trades['trade_id'] > last_seen]
            if new.empty:
                continue
            self._last_trade_id[symbol] = int(new['trade_id'].iloc[-1])
            for order in list(self.orders.values()):
                if order['status'] != 'open' or order['symbol'] != symbol:
                    continue
                after = order['after_trade_id']
                eligible = new if after is None else new[new['trade_id'] > after]
                if eligible.empty:
                    continue
                if order['side'] == 'buy':
                    hit = bool((eligible['price'] < order['price']).any())
                else:
                    hit = bool((eligible['price'] > order['price']).any())
                if hit:
                    self._fill(order, order['price'], 'maker')

    def _prune_closed(self) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self.closed_order_retention_hours)
        stale = [
            oid for oid, o in self.orders.items()
            if o['status'] != 'open'
            and datetime.fromisoformat(o['closed'].replace('Z', '+00:00')) < cutoff
        ]
        if stale:
            for oid in stale:
                del self.orders[oid]
            self._save()

    # ── ccxt-shaped queries ─────────────────────────────────────────────

    @staticmethod
    def _to_ccxt(o: dict) -> dict:
        ts = o['created']
        return {
            'id': o['id'],
            'clientOrderId': o['cl_ord_id'],
            'symbol': o['symbol'],
            'side': o['side'],
            'type': o['type'],
            'status': o['status'],
            'amount': o['amount'],
            'filled': o['filled'],
            'remaining': o['amount'] - o['filled'],
            'price': o['price'],
            'average': o['average'],
            'cost': o['cost'],
            'datetime': ts,
            'timestamp': int(datetime.fromisoformat(ts.replace('Z', '+00:00')).timestamp() * 1000),
            'fee': {'cost': o['fee'], 'currency': QUOTE},
        }

    def fetch_open_orders(self, symbol) -> list:
        return [self._to_ccxt(o) for o in self.orders.values()
                if o['status'] == 'open' and (symbol is None or o['symbol'] == symbol)]

    def fetch_closed_orders(self, since_ms) -> list:
        out = []
        for o in self.orders.values():
            if o['status'] == 'open':
                continue
            closed_ms = datetime.fromisoformat(o['closed'].replace('Z', '+00:00')).timestamp() * 1000
            if since_ms is None or closed_ms >= since_ms:
                out.append(self._to_ccxt(o))
        return out

    def balances(self) -> dict:
        """ccxt fetch_balance shape: {'free': {...}, 'used': {...}, 'total': {...}}."""
        total = {QUOTE: self.cash}
        total.update({k: v for k, v in self.holdings.items() if v > 0})
        free = {QUOTE: self.free_usd()}
        free.update({k: self.free_base(k) for k in total if k != QUOTE})
        used = {k: total[k] - free[k] for k in total}
        return {'free': free, 'used': used, 'total': total}

    # ── Equity record ───────────────────────────────────────────────────

    def record_equity(self, usd_prices: dict) -> float:
        """Mark the paper account to market, append a line to
        paper_equity.jsonl, and return equity in USD."""
        positions = {}
        for base, qty in self.holdings.items():
            if qty <= 0:
                continue
            if base not in usd_prices:
                raise RuntimeError(f"PAPER equity: no USD price for {base}")
            positions[base] = qty * usd_prices[base]
        equity = self.cash + sum(positions.values())
        row = {
            'ts': self._now_iso(),
            'equity': equity,
            'cash': self.cash,
            'positions_usd': positions,
            'return_pct': (equity / self.starting_cash_usd - 1) * 100,
        }
        with open(self.equity_file, 'a') as f:
            f.write(json.dumps(row) + '\n')
        return equity
