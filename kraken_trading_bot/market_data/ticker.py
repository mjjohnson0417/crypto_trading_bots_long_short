"""Per-symbol latest ticker, fed from the public WS ticker channel.

Per symbol: the newest ticker message as a plain dict (floats), plus
timestamp. The strategy reads only the latest bid/ask, so no history is
kept and nothing but a dict assignment happens per WS message.

Keys mirror the Kraken v2 ticker payload:
    timestamp, bid, bid_qty, ask, ask_qty, last, volume, vwap, low, high,
    change, change_pct
"""
import asyncio

from logging_config import get_logger
from market_data.order_book import parse_ts


_COLUMNS = [
    'bid', 'bid_qty', 'ask', 'ask_qty', 'last',
    'volume', 'vwap', 'low', 'high', 'change', 'change_pct',
]


class TickerBuffer:
    """Per-symbol latest ticker message."""

    def __init__(self, symbols):
        self.logger = get_logger('TickerBuffer')
        self.symbols = [s.upper() for s in symbols]
        self.tickers = {s: None for s in self.symbols}

    def latest(self, symbol: str):
        """Newest ticker row, or None before the first message."""
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
            row = {c: float(entry[c]) for c in _COLUMNS if c in entry and entry[c] is not None}
            row['timestamp'] = parse_ts(entry['timestamp'])
            self.tickers[symbol] = row
