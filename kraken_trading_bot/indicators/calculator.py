"""Tick-driven indicator calculator.

`IndicatorCalculator` keeps a per-symbol DataFrame of trade prices over
the trailing 8-hour window. `calculate(symbol, trade_df, ticker_data)`
ingests new rows from the public `trades` DataFrame, trims to the 8h
window, and returns:

    best_bid, best_ask, mid_price        — from ticker_data
    sma_1h, sma_4h                       — rolling means over the trade tape
    slope_1h_pct                         — sma_1h now vs. 15-min-shifted-back
    width_pct_4h, width_pct_8h           — (max - min)/mid × 100 over those windows
    donch_hi, donch_lo                   — 8h price max / min (Donchian)

Bootstrap via `seed_from_klines(symbol, kline_df)` so the rolling DataFrame
holds 8 hours of price points immediately at boot instead of after hours
of live trades.
"""
from typing import Optional

import pandas as pd

from logging_config import get_logger


class IndicatorCalculator:
    """Per-symbol indicators from the trade tape + live ticker."""

    _MAX_WINDOW_HOURS = 8
    _SLOPE_LOOKBACK_MIN = 15

    def __init__(self, symbols: list):
        self.logger = get_logger('IndicatorCalculator')
        self.symbols = symbols
        self._prices = {s: self._empty_df() for s in symbols}
        self._last_ts = {s: None for s in symbols}
        self.logger.info(f"IndicatorCalculator initialized for {symbols}")

    @staticmethod
    def _empty_df() -> pd.DataFrame:
        df = pd.DataFrame({'price': pd.Series(dtype='float64')})
        df.index = pd.DatetimeIndex([], name='timestamp', tz='UTC')
        return df

    def seed_from_klines(self, symbol: str, kline_df) -> None:
        """Bootstrap the rolling DataFrame from historical kline closes.

        Re-seeding is idempotent — the existing buffer is replaced.
        """
        try:
            if kline_df is None or kline_df.empty:
                return

            df = kline_df.copy()
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
                df = df.set_index('timestamp')
            elif not isinstance(df.index, pd.DatetimeIndex):
                return

            now = pd.Timestamp.now(tz='UTC')
            window_start = now - pd.Timedelta(hours=self._MAX_WINDOW_HOURS)
            recent = df[df.index >= window_start]
            recent = recent[recent['close'] > 0][['close']].rename(
                columns={'close': 'price'}
            )

            self._prices[symbol] = recent.copy()
            self._last_ts[symbol] = recent.index.max() if not recent.empty else None

            self.logger.info(
                f"Seeded {symbol}: {len(recent)} price points "
                f"over {self._MAX_WINDOW_HOURS}h"
            )
        except Exception as e:
            self.logger.error(f"Failed to seed indicators for {symbol}: {e}")

    def calculate(self, symbol: str, trade_df: pd.DataFrame,
                  ticker_data: dict) -> dict:
        """Ingest new trades and emit the indicators dict."""
        now = pd.Timestamp.now(tz='UTC')
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

        # Ingest new trades from the passed-in trade DataFrame.
        if trade_df is not None and not trade_df.empty:
            last_ts = self._last_ts[symbol]
            new_trades = (trade_df if last_ts is None
                          else trade_df[trade_df.index > last_ts])
            new_trades = new_trades[new_trades['price'] > 0]
            if not new_trades.empty:
                self._prices[symbol] = pd.concat(
                    [self._prices[symbol], new_trades[['price']]]
                )
                self._last_ts[symbol] = self._prices[symbol].index.max()

        # Trim to the 8h rolling window.
        cutoff = now - pd.Timedelta(hours=self._MAX_WINDOW_HOURS)
        df = self._prices[symbol]
        df = df[df.index >= cutoff]
        self._prices[symbol] = df

        # mid_price comes from the ticker block above; absent means we
        # haven't received a ticker yet and can't compute % indicators.
        if df.empty or 'mid_price' not in indicators or indicators['mid_price'] <= 0:
            indicators['sma_1h'] = None
            indicators['sma_4h'] = None
            indicators['width_pct_4h'] = None
            indicators['width_pct_8h'] = None
            indicators['donch_hi'] = None
            indicators['donch_lo'] = None
            indicators['slope_1h_pct'] = None
            self.logger.info(indicators)
            return indicators
        mid_price = indicators['mid_price']

        # Windowed slices.
        last_1h = df[df.index >= now - pd.Timedelta(hours=1)]
        last_4h = df[df.index >= now - pd.Timedelta(hours=4)]

        sma_1h = last_1h['price'].mean() if not last_1h.empty else None
        sma_4h = last_4h['price'].mean() if not last_4h.empty else None

        # sma_1h_past = 1h window shifted back _SLOPE_LOOKBACK_MIN.
        past_end = now - pd.Timedelta(minutes=self._SLOPE_LOOKBACK_MIN)
        past_start = past_end - pd.Timedelta(hours=1)
        past_window = df[(df.index >= past_start) & (df.index < past_end)]
        sma_1h_past = past_window['price'].mean() if not past_window.empty else None

        slope_1h_pct = None
        if (sma_1h is not None and not pd.isna(sma_1h)
                and sma_1h_past is not None and not pd.isna(sma_1h_past)
                and mid_price > 0):
            slope_1h_pct = (sma_1h - sma_1h_past) / mid_price * 100.0

        # 4h width — range over the last 4h of trades.
        if not last_4h.empty:
            width_pct_4h = (last_4h['price'].max() - last_4h['price'].min()) / mid_price * 100.0
        else:
            width_pct_4h = None

        # 8h Donchian + width — full rolling window (which is 8h).
        donch_hi = df['price'].max()
        donch_lo = df['price'].min()
        width_pct_8h = (donch_hi - donch_lo) / mid_price * 100.0

        def _maybe(v):
            return v if (v is not None and not pd.isna(v)) else None

        indicators['sma_1h'] = _maybe(sma_1h)
        indicators['sma_4h'] = _maybe(sma_4h)
        indicators['width_pct_4h'] = _maybe(width_pct_4h)
        indicators['width_pct_8h'] = _maybe(width_pct_8h)
        indicators['donch_hi'] = _maybe(donch_hi)
        indicators['donch_lo'] = _maybe(donch_lo)
        indicators['slope_1h_pct'] = slope_1h_pct
        self.logger.info(indicators)
        return indicators
