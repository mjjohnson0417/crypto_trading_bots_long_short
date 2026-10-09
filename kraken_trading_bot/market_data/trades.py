"""Per-symbol trades DataFrame, fed from the public WS trade channel.

Per symbol: a rolling time-indexed DataFrame. Each trade in a message
appends one row. Bounded by `maxlen` rows via tail-trim.

Columns mirror Kraken v2 trade payload keys (timestamp becomes the
index):
    side, price, qty, ord_type, trade_id
"""
import asyncio

import pandas as pd

from logging_config import get_logger


class TradeBuffer:
    """Per-symbol time-indexed rolling DataFrame of trades."""

    def __init__(self, symbols, maxlen: int):
        self.logger = get_logger('TradeBuffer')
        self.symbols = [s.upper() for s in symbols]
        self.maxlen = maxlen
        self.buffers = {s: self._empty() for s in self.symbols}

    @staticmethod
    def _empty() -> pd.DataFrame:
        df = pd.DataFrame({
            'side': pd.Series(dtype='object'),
            'price': pd.Series(dtype='float64'),
            'qty': pd.Series(dtype='float64'),
            'ord_type': pd.Series(dtype='object'),
            'trade_id': pd.Series(dtype='int64'),
        })
        df.index = pd.DatetimeIndex([], name='timestamp', tz='UTC')
        return df

    def get(self, symbol: str) -> pd.DataFrame:
        return self.buffers[symbol.upper()]

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
            ts = pd.to_datetime(trade['timestamp'], utc=True)
            row = pd.DataFrame(
                [{
                    'side': trade['side'],
                    'price': float(trade['price']),
                    'qty': float(trade['qty']),
                    'ord_type': trade['ord_type'],
                    'trade_id': int(trade['trade_id']),
                }],
                index=pd.DatetimeIndex([ts], name='timestamp'),
            )
            merged = pd.concat([self.buffers[symbol], row])
            self.buffers[symbol] = merged.iloc[-self.maxlen:]
