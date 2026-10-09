"""Per-symbol ticker DataFrame, fed from the public WS ticker channel.

Per symbol: a rolling time-indexed DataFrame. Each ticker message appends
one row. The latest row is the current value.

Columns mirror Kraken v2 ticker payload keys (timestamp becomes the
index):
    bid, bid_qty, ask, ask_qty, last, volume, vwap, low, high,
    change, change_pct
"""
import asyncio

import pandas as pd

from logging_config import get_logger


_COLUMNS = [
    'bid', 'bid_qty', 'ask', 'ask_qty', 'last',
    'volume', 'vwap', 'low', 'high', 'change', 'change_pct',
]


class TickerBuffer:
    """Per-symbol time-indexed rolling DataFrame of ticker messages."""

    def __init__(self, symbols, maxlen: int):
        self.logger = get_logger('TickerBuffer')
        self.symbols = [s.upper() for s in symbols]
        self.maxlen = maxlen
        self.tickers = {s: self._empty() for s in self.symbols}

    @staticmethod
    def _empty() -> pd.DataFrame:
        df = pd.DataFrame({c: pd.Series(dtype='float64') for c in _COLUMNS})
        df.index = pd.DatetimeIndex([], name='timestamp', tz='UTC')
        return df

    def get(self, symbol: str) -> pd.DataFrame:
        return self.tickers[symbol.upper()]

    async def consume(self, queue: asyncio.Queue) -> None:
        while True:
            msg = await queue.get()
            try:
                self._apply(msg)
            except Exception as e:
                self.logger.error(f"ticker apply failed: {e}", exc_info=True)

    def _apply(self, msg: dict) -> None:
        for entry in msg['data']:
            symbol = entry['symbol']
            if symbol not in self.tickers:
                continue
            ts = pd.to_datetime(entry['timestamp'], utc=True)
            row = {c: float(entry[c]) for c in _COLUMNS if c in entry and entry[c] is not None}
            new_row = pd.DataFrame([row], index=pd.DatetimeIndex([ts], name='timestamp'))
            merged = pd.concat([self.tickers[symbol], new_row])
            self.tickers[symbol] = merged.iloc[-self.maxlen:]
