"""Per-symbol klines DataFrame, populated by REST at boot.

Per symbol: a time-indexed DataFrame of OHLCV bars.

Columns:
    open, high, low, close, volume

Seed-only. There is no WS feed and no runtime update path. The buffer
exists so the indicator seeder can warm up rolling SMAs immediately
instead of waiting hours for trade-tape history.
"""
import pandas as pd

from logging_config import get_logger


class KlineBuffer:
    """Per-symbol time-indexed DataFrame of OHLCV bars. Seed-only."""

    def __init__(self, symbols):
        self.logger = get_logger('KlineBuffer')
        self.symbols = [s.upper() for s in symbols]
        self.klines = {s: self._empty() for s in self.symbols}

    @staticmethod
    def _empty() -> pd.DataFrame:
        df = pd.DataFrame({
            'open': pd.Series(dtype='float64'),
            'high': pd.Series(dtype='float64'),
            'low': pd.Series(dtype='float64'),
            'close': pd.Series(dtype='float64'),
            'volume': pd.Series(dtype='float64'),
        })
        df.index = pd.DatetimeIndex([], name='timestamp', tz='UTC')
        return df

    def get(self, symbol: str) -> pd.DataFrame:
        return self.klines[symbol.upper()]

    def seed_from_rest(self, symbol: str, rows: list) -> None:
        """Replace this symbol's DataFrame with the REST result.

        `rows` is the ccxt `fetch_ohlcv` return: a list of
        `[ms_timestamp, open, high, low, close, volume]`.
        """
        symbol = symbol.upper()
        if symbol not in self.klines:
            raise KeyError(f"KlineBuffer has no symbol {symbol!r}")
        df = pd.DataFrame(
            rows,
            columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'],
        )
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df = df.set_index('timestamp')
        self.klines[symbol] = df
