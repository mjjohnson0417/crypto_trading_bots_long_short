"""Per-symbol order book, fed from the public WS book channel.

Per symbol: two dicts, bids and asks, {price: qty}, plus the timestamp of
the last applied message. A WS message only touches dict
entries — no pandas work on the message path (a pandas row edit per level
cost ~0.7 ms and could not keep up with busy markets).

Snapshot messages replace both dicts for the symbol.
Update messages upsert per-price entries; qty == 0 drops the entry.

Readers:
    levels(symbol, side)  [(price, qty), ...] best-first ('bid' | 'ask')
    last_update(symbol)   datetime of the last applied message, or None
"""
import asyncio
from datetime import datetime

from logging_config import get_logger


def parse_ts(s: str) -> datetime:
    """Kraken v2 ISO timestamp ('...Z') → aware UTC datetime."""
    return datetime.fromisoformat(s)


class OrderBookBuffer:
    """Per-symbol bid/ask dicts of the live book."""

    def __init__(self, symbols):
        self.logger = get_logger('OrderBookBuffer')
        self.symbols = [s.upper() for s in symbols]
        self.bids = {s: {} for s in self.symbols}
        self.asks = {s: {} for s in self.symbols}
        self.updated = {s: None for s in self.symbols}

    def levels(self, symbol: str, side: str) -> list:
        symbol = symbol.upper()
        if side == 'bid':
            return sorted(self.bids[symbol].items(), reverse=True)
        if side == 'ask':
            return sorted(self.asks[symbol].items())
        raise ValueError(f"unknown book side {side!r}")

    def last_update(self, symbol: str):
        return self.updated[symbol.upper()]

    async def consume(self, queue: asyncio.Queue) -> None:
        """Pull book messages off the queue and apply them. Long-lived task."""
        while True:
            msg = await queue.get()
            try:
                self._apply(msg)
            except Exception as e:
                self.logger.error(f"order_book apply failed: {e}", exc_info=True)

    def _apply(self, msg: dict) -> None:
        msg_type = msg['type']
        for entry in msg['data']:
            symbol = entry['symbol']
            if symbol not in self.bids:
                continue
            ts = parse_ts(entry['timestamp'])
            if msg_type == 'snapshot':
                self._apply_snapshot(symbol, entry)
            elif msg_type == 'update':
                self._apply_update(symbol, entry)
            else:
                raise ValueError(f"unknown book msg type {msg_type!r}")
            self.updated[symbol] = ts

    def _apply_snapshot(self, symbol: str, entry: dict) -> None:
        self.bids[symbol] = {float(b['price']): float(b['qty'])
                             for b in entry['bids'] if float(b['qty']) > 0}
        self.asks[symbol] = {float(a['price']): float(a['qty'])
                             for a in entry['asks'] if float(a['qty']) > 0}

    def _apply_update(self, symbol: str, entry: dict) -> None:
        for levels, book in ((entry['bids'], self.bids[symbol]), (entry['asks'], self.asks[symbol])):
            for lvl in levels:
                price = float(lvl['price'])
                qty = float(lvl['qty'])
                if qty == 0:
                    book.pop(price, None)
                else:
                    book[price] = qty
