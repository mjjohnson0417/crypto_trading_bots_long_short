"""Per-symbol trade tape, fed from the public WS trade channel.

Per symbol: a deque (maxlen rows) of plain dicts, oldest first. A WS
message only appends to the deque — no pandas work on the message path.

Row keys mirror the Kraken v2 trade payload:
    timestamp (aware UTC datetime), side, price, qty, ord_type, trade_id

Readers:
    last_trade_id(symbol)        newest trade_id, or None
    since_id(symbol, trade_id)   rows with trade_id > trade_id, oldest first
    since_time(symbol, ts)       rows with timestamp > ts, oldest first
"""
import asyncio
from collections import deque

from logging_config import get_logger
from market_data.order_book import parse_ts


class TradeBuffer:
    """Per-symbol rolling deque of trades."""

    def __init__(self, symbols, maxlen: int):
        self.logger = get_logger('TradeBuffer')
        self.symbols = [s.upper() for s in symbols]
        self.maxlen = maxlen
        self.buffers = {s: deque(maxlen=maxlen) for s in self.symbols}

    def last_trade_id(self, symbol: str):
        buf = self.buffers[symbol.upper()]
        return buf[-1]['trade_id'] if buf else None

    def since_id(self, symbol: str, trade_id) -> list:
        buf = self.buffers[symbol.upper()]
        if trade_id is None:
            return list(buf)
        out = []
        for row in reversed(buf):
            if row['trade_id'] <= trade_id:
                break
            out.append(row)
        out.reverse()
        return out

    def since_time(self, symbol: str, ts) -> list:
        buf = self.buffers[symbol.upper()]
        if ts is None:
            return list(buf)
        out = []
        for row in reversed(buf):
            if row['timestamp'] <= ts:
                break
            out.append(row)
        out.reverse()
        return out

    async def consume(self, queue: asyncio.Queue) -> None:
        while True:
            msg = await queue.get()
            try:
                self._apply(msg)
            except Exception as e:
                self.logger.error(f"trade apply failed: {e}", exc_info=True)

    def _apply(self, msg: dict) -> None:
        for trade in msg['data']:
            symbol = trade['symbol']
            if symbol not in self.buffers:
                continue
            self.buffers[symbol].append({
                'timestamp': parse_ts(trade['timestamp']),
                'side': trade['side'],
                'price': float(trade['price']),
                'qty': float(trade['qty']),
                'ord_type': trade['ord_type'],
                'trade_id': int(trade['trade_id']),
            })
