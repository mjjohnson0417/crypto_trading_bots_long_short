"""Tick-driven indicator calculator.

`IndicatorCalculator` keeps a per-symbol rolling window of trade prices
over the trailing 4 hours (two deques: epoch seconds, price).
`calculate(symbol, new_trades, ticker_data, now)` appends the trades newer
than the last one ingested, trims to the 4h window, and returns:

    best_bid, best_ask, mid_price        — from ticker_data
    sma_1h, sma_4h                       — rolling means over the trade tape
    slope_1h_pct                         — sma_1h now vs. 15-min-shifted-back
    width_pct_4h                         — (max - min)/mid × 100 over the last 4h

These are exactly the values the regime classifier reads.

The window math runs once per tick on numpy arrays built from the deques;
nothing is done per WS message.

Bootstrap via `seed_from_klines(symbol, kline_df, now)` so the window holds
4 hours of price points immediately at boot instead of after hours of
live trades.
"""
from collections import deque
from datetime import timedelta

import numpy as np

from logging_config import get_logger


class IndicatorCalculator:
    """Per-symbol indicators from the trade tape + live ticker."""

    _MAX_WINDOW_HOURS = 4
    _SLOPE_LOOKBACK_MIN = 15

    def __init__(self, symbols: list):
        self.logger = get_logger('IndicatorCalculator')
        self.symbols = symbols
        self._times = {s: deque() for s in symbols}    # epoch seconds
        self._prices = {s: deque() for s in symbols}
        self._last_ts = {s: None for s in symbols}     # newest ingested (datetime)
        self.logger.info(f"IndicatorCalculator initialized for {symbols}")

    def seed_from_klines(self, symbol: str, kline_df, now) -> None:
        """Bootstrap the window from historical kline closes (time index).

        Re-seeding is idempotent — the existing window is replaced.
        """
        times, prices = deque(), deque()
        last = None
        if kline_df is not None and not kline_df.empty:
            cutoff = now - timedelta(hours=self._MAX_WINDOW_HOURS)
            for ts, close in zip(kline_df.index, kline_df['close']):
                if ts >= cutoff and close > 0:
                    times.append(ts.timestamp())
                    prices.append(float(close))
                    last = ts.to_pydatetime() if last is None or ts > last else last
        self._times[symbol], self._prices[symbol] = times, prices
        self._last_ts[symbol] = last
        self.logger.info(
            f"Seeded {symbol}: {len(prices)} price points "
            f"over {self._MAX_WINDOW_HOURS}h"
        )

    def last_ingested(self, symbol: str):
        """Timestamp of the newest trade ingested; newer trades are new."""
        return self._last_ts[symbol]

    def calculate(self, symbol: str, new_trades: list, ticker_data: dict, now) -> dict:
        """Ingest `new_trades` (trade-buffer rows newer than
        last_ingested(symbol), oldest first) and emit the indicators dict."""
        indicators = {
            'timestamp': now.isoformat(),
            'symbol': symbol,
        }

        # best_bid / best_ask / mid_price come only from the ticker channel.
        if ticker_data:
            tb = ticker_data.get('best_bid')
            ta = ticker_data.get('best_ask')
            if tb and ta and tb > 0 and ta > 0:
                indicators['best_bid'] = tb
                indicators['best_ask'] = ta
                indicators['mid_price'] = (tb + ta) / 2.0

        times, prices = self._times[symbol], self._prices[symbol]
        for t in new_trades:
            if t['price'] > 0:
                times.append(t['timestamp'].timestamp())
                prices.append(t['price'])
                if self._last_ts[symbol] is None or t['timestamp'] > self._last_ts[symbol]:
                    self._last_ts[symbol] = t['timestamp']

        # Trim to the 4h rolling window (rows arrive in time order).
        now_s = now.timestamp()
        cutoff = now_s - self._MAX_WINDOW_HOURS * 3600
        while times and times[0] < cutoff:
            times.popleft()
            prices.popleft()

        # mid_price comes from the ticker block above; absent means we
        # haven't received a ticker yet and can't compute % indicators.
        if not prices or 'mid_price' not in indicators or indicators['mid_price'] <= 0:
            indicators['sma_1h'] = None
            indicators['sma_4h'] = None
            indicators['width_pct_4h'] = None
            indicators['slope_1h_pct'] = None
            self.logger.info(indicators)
            return indicators
        mid_price = indicators['mid_price']

        t = np.fromiter(times, dtype='float64', count=len(times))
        p = np.fromiter(prices, dtype='float64', count=len(prices))

        # Windowed slices.
        last_1h = p[t >= now_s - 3600]
        last_4h = p[t >= now_s - 4 * 3600]

        sma_1h = float(last_1h.mean()) if last_1h.size else None
        sma_4h = float(last_4h.mean()) if last_4h.size else None

        # sma_1h_past = 1h window shifted back _SLOPE_LOOKBACK_MIN.
        past_end = now_s - self._SLOPE_LOOKBACK_MIN * 60
        past_window = p[(t >= past_end - 3600) & (t < past_end)]
        sma_1h_past = float(past_window.mean()) if past_window.size else None

        slope_1h_pct = None
        if sma_1h is not None and sma_1h_past is not None and mid_price > 0:
            slope_1h_pct = (sma_1h - sma_1h_past) / mid_price * 100.0

        # 4h width — range over the last 4h of trades.
        width_pct_4h = ((float(last_4h.max()) - float(last_4h.min())) / mid_price * 100.0
                        if last_4h.size else None)

        indicators['sma_1h'] = sma_1h
        indicators['sma_4h'] = sma_4h
        indicators['width_pct_4h'] = width_pct_4h
        indicators['slope_1h_pct'] = slope_1h_pct
        self.logger.info(indicators)
        return indicators
