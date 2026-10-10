"""Kraken Futures perpetual market data — per-product ticker, trades, book.

Fed by `exchange/futures_public_ws.py` (wss://futures.kraken.com/ws/v1).
Products are mapped explicitly in config `perps.products`
({spot_symbol: product_id}, e.g. "ETH/USD": "PF_ETHUSD"); a spot symbol
with no perp (CC/USD) is simply absent from the map.

Per product:
  ticker  latest ticker message fields (mark, index, bid/ask, last,
          funding_rate, relative_funding_rate, next_funding_rate_time,
          openInterest, ...) plus `received_at`.
  trades  deque of {time (UTC datetime), price, qty, side, seq, uid}.
  book    {price: qty} per side, rebuilt from book_snapshot and updated
          by book deltas (qty is the new absolute size; 0 removes the
          level). A sequence gap marks the book invalid; the WS layer then
          reconnects to get a fresh snapshot.

Kraken Futures funding on PF_ perps accrues hourly; `relative_funding_rate`
is the fraction of notional per hour paid by longs to shorts when positive.
"""
from collections import deque
from datetime import datetime, timezone

from logging_config import get_logger


def _ms_to_dt(ms) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, timezone.utc)


class _Book:
    def __init__(self):
        self.bids: dict = {}
        self.asks: dict = {}
        self.seq = None
        self.valid = False
        self.updated_at = None


class PerpMarketData:

    def __init__(self, config):
        self.logger = get_logger('PerpMarketData')
        products = config['products']
        self.spot_to_product = {s.upper(): p.upper() for s, p in products.items()}
        self.product_to_spot = {p: s for s, p in self.spot_to_product.items()}
        if len(self.product_to_spot) != len(self.spot_to_product):
            raise ValueError("perps.products maps two spot symbols to one product")
        self.products = list(self.product_to_spot)
        self.trade_buffer_maxlen = int(config['trade_buffer_maxlen'])

        self.ticker = {p: None for p in self.products}
        self.trades = {p: deque(maxlen=self.trade_buffer_maxlen) for p in self.products}
        self.books = {p: _Book() for p in self.products}
        # {product_id: instrument} from Kraken Futures, set by
        # validate_instruments() at startup (contract precision, margin tiers).
        self.instruments = None
        # Set by apply() when a book needs a fresh snapshot; the WS layer
        # reconnects and clears it.
        self.resync_needed = False

    # ── Validation against the live instrument list ─────────────────────

    def validate_instruments(self, instruments: dict) -> None:
        """`instruments` is {product_id: instrument} from Kraken Futures
        /derivatives/api/v3/instruments. Raises if any configured product
        is missing, not tradeable, or not a perpetual."""
        for spot, product in self.spot_to_product.items():
            if product not in instruments:
                raise RuntimeError(f"perps: {product} ({spot}) not listed on Kraken Futures")
            ins = instruments[product]
            if not ins['tradeable']:
                raise RuntimeError(f"perps: {product} ({spot}) is not tradeable")
            if ins['type'] != 'flexible_futures' or not product.startswith('PF_'):
                raise RuntimeError(
                    f"perps: {product} ({spot}) is not a perpetual "
                    f"(type={ins['type']!r})"
                )
        self.instruments = {p: instruments[p] for p in self.products}
        self.logger.info(f"Perp products validated: {self.spot_to_product}")

    # ── Message intake ──────────────────────────────────────────────────

    def apply(self, msg: dict) -> None:
        feed = msg['feed']
        product = msg['product_id']
        if product not in self.ticker:
            return
        if feed == 'ticker':
            row = dict(msg)
            row['received_at'] = datetime.now(timezone.utc)
            self.ticker[product] = row
        elif feed == 'trade_snapshot':
            buf = self.trades[product]
            buf.clear()
            for t in sorted(msg['trades'], key=lambda t: t['seq']):
                buf.append(self._trade_row(t))
        elif feed == 'trade':
            self.trades[product].append(self._trade_row(msg))
        elif feed == 'book_snapshot':
            book = self.books[product]
            book.bids = {float(l['price']): float(l['qty']) for l in msg['bids'] if l['qty'] > 0}
            book.asks = {float(l['price']): float(l['qty']) for l in msg['asks'] if l['qty'] > 0}
            book.seq = msg['seq']
            book.valid = True
            book.updated_at = _ms_to_dt(msg['timestamp'])
        elif feed == 'book':
            book = self.books[product]
            if not book.valid:
                return
            if msg['seq'] != book.seq + 1:
                self.logger.warning(
                    f"{product} book sequence gap: have {book.seq}, got "
                    f"{msg['seq']} — book invalid until resnapshot"
                )
                book.valid = False
                self.resync_needed = True
                return
            side = book.bids if msg['side'] == 'buy' else book.asks
            price = float(msg['price'])
            qty = float(msg['qty'])
            if qty == 0:
                side.pop(price, None)
            else:
                side[price] = qty
            book.seq = msg['seq']
            book.updated_at = _ms_to_dt(msg['timestamp'])
        else:
            raise ValueError(f"unknown futures feed {feed!r}")

    @staticmethod
    def _trade_row(t: dict) -> dict:
        return {
            'time': _ms_to_dt(t['time']),
            'price': float(t['price']),
            'qty': float(t['qty']),
            'side': t['side'],
            'type': t['type'],
            'seq': int(t['seq']),
            'uid': t['uid'],
        }

    def invalidate_all_books(self) -> None:
        for book in self.books.values():
            book.valid = False
        self.resync_needed = False

    # ── Accessors ───────────────────────────────────────────────────────

    def product_for(self, spot_symbol: str):
        """Perp product_id for a spot symbol, or None if it has no perp."""
        s = spot_symbol.upper()
        return self.spot_to_product[s] if s in self.spot_to_product else None

    def best_bid_ask(self, product: str):
        """(bid, ask) from the live book. Raises if the book is not valid."""
        book = self.books[product]
        if not book.valid or not book.bids or not book.asks:
            raise RuntimeError(f"{product}: perp book not available")
        return max(book.bids), min(book.asks)

    def book_levels(self, product: str, side: str):
        """[(price, qty), ...] best-first; side is 'bid' or 'ask'."""
        book = self.books[product]
        if not book.valid:
            raise RuntimeError(f"{product}: perp book not available")
        if side == 'bid':
            return sorted(book.bids.items(), reverse=True)
        if side == 'ask':
            return sorted(book.asks.items())
        raise ValueError(f"side must be 'bid' or 'ask', got {side!r}")

    def latest_ticker(self, product: str) -> dict:
        t = self.ticker[product]
        if t is None:
            raise RuntimeError(f"{product}: no perp ticker received yet")
        return t
