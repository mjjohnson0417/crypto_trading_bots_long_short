"""Per-symbol order book DataFrame, fed from the public WS book channel.

Per symbol: one pandas DataFrame indexed by price, columns:
    qty       (float)
    side      ('bid' | 'ask')
    timestamp (pandas Timestamp, when this level was last written)

Snapshot messages replace the whole DataFrame for the symbol.
Update messages upsert per-price rows; qty == 0 drops the row.
"""
import asyncio

import pandas as pd

from logging_config import get_logger


class OrderBookBuffer:
    """Per-symbol price-indexed DataFrames of the live book."""

    def __init__(self, symbols):
        self.logger = get_logger('OrderBookBuffer')
        self.symbols = [s.upper() for s in symbols]
        self.books = {s: self._empty() for s in self.symbols}

    @staticmethod
    def _empty() -> pd.DataFrame:
        df = pd.DataFrame({
            'qty': pd.Series(dtype='float64'),
            'side': pd.Series(dtype='object'),
            'timestamp': pd.Series(dtype='datetime64[ns, UTC]'),
        })
        df.index = pd.Index([], dtype='float64', name='price')
        return df

    def get(self, symbol: str) -> pd.DataFrame:
        return self.books[symbol.upper()]

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
            if symbol not in self.books:
                continue
            ts = pd.to_datetime(entry['timestamp'], utc=True)
            if msg_type == 'snapshot':
                self._apply_snapshot(symbol, entry, ts)
            elif msg_type == 'update':
                self._apply_update(symbol, entry, ts)
            else:
                raise ValueError(f"unknown book msg type {msg_type!r}")

    def _apply_snapshot(self, symbol: str, entry: dict, ts) -> None:
        rows = []
        index = []
        for b in entry['bids']:
            price = float(b['price'])
            qty = float(b['qty'])
            if qty <= 0:
                continue
            index.append(price)
            rows.append({'qty': qty, 'side': 'bid', 'timestamp': ts})
        for a in entry['asks']:
            price = float(a['price'])
            qty = float(a['qty'])
            if qty <= 0:
                continue
            index.append(price)
            rows.append({'qty': qty, 'side': 'ask', 'timestamp': ts})
        df = pd.DataFrame(rows, index=pd.Index(index, name='price', dtype='float64'))
        self.books[symbol] = df

    def _apply_update(self, symbol: str, entry: dict, ts) -> None:
        book = self.books[symbol]
        for b in entry['bids']:
            price = float(b['price'])
            qty = float(b['qty'])
            if qty == 0:
                if price in book.index:
                    book = book.drop(price)
            else:
                book.loc[price] = {'qty': qty, 'side': 'bid', 'timestamp': ts}
        for a in entry['asks']:
            price = float(a['price'])
            qty = float(a['qty'])
            if qty == 0:
                if price in book.index:
                    book = book.drop(price)
            else:
                book.loc[price] = {'qty': qty, 'side': 'ask', 'timestamp': ts}
        self.books[symbol] = book
