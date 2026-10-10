"""Daily-candle trend gate — the 200-day buy gate.

Per symbol, fetches Kraken daily candles over REST, keeps only COMPLETED
UTC days, and computes simple moving averages of the daily close. The
buy gate is:

    buys allowed  ⇔  previous completed day's close ≥ its N-day SMA
                     (the SMA window ends on, and includes, that close)

This matches strategy_lab's `daily_pause_flags` (pause when the previous
day's close is below its SMA).

Fail-loud policy:
  - `initialize()` raises if any symbol cannot be fetched or has fewer
    than the required completed days. The bot must not start without it.
  - A runtime refresh failure keeps the previous data and logs an error.
    Once the newest completed day is older than `max_staleness_hours`,
    `buy_gate()` blocks buys for that symbol until a refresh succeeds.
"""
from datetime import datetime, timedelta, timezone

from logging_config import get_logger


_ONE_DAY = timedelta(days=1)


class DailyTrendGate:

    def __init__(self, symbols, rest_exchange, config):
        self.logger = get_logger('DailyTrendGate')
        self.symbols = [s.upper() for s in symbols]
        self.rest_exchange = rest_exchange

        self.buy_gate_sma_days = int(config['buy_gate_sma_days'])
        self.short_gate_sma_days = int(config['short_gate_sma_days'])
        self.fetch_limit = int(config['fetch_limit'])
        self.refresh_interval_seconds = float(config['refresh_interval_seconds'])
        self.max_staleness_hours = float(config['max_staleness_hours'])

        required = max(self.buy_gate_sma_days, self.short_gate_sma_days)
        if self.fetch_limit <= required:
            raise ValueError(
                f"daily_trend.fetch_limit={self.fetch_limit} must exceed the "
                f"longest SMA ({required}) to leave room for the in-progress day"
            )

        # symbol → dict(day, close, sma_buy, sma_short, buys_allowed, fetched_at)
        self.status = {}

    # ── Refresh ─────────────────────────────────────────────────────────

    async def initialize(self) -> None:
        """First fetch for every symbol. Raises on any failure."""
        now = datetime.now(timezone.utc)
        for symbol in self.symbols:
            self.status[symbol] = await self._compute(symbol, now)
            self._log_status(symbol)

    async def refresh(self) -> None:
        """Periodic refresh. Per-symbol failures keep the old data; the
        staleness check in buy_gate() then blocks buys if it ages out."""
        now = datetime.now(timezone.utc)
        for symbol in self.symbols:
            try:
                new = await self._compute(symbol, now)
            except Exception as e:
                self.logger.error(f"{symbol} daily trend refresh failed: {e}")
                continue
            old = self.status[symbol]
            self.status[symbol] = new
            if new['day'] != old['day'] or new['buys_allowed'] != old['buys_allowed']:
                self._log_status(symbol)

    async def _compute(self, symbol: str, now: datetime) -> dict:
        rows = await self.rest_exchange.fetch_ohlcv(
            symbol, '1d', limit=self.fetch_limit,
        )
        # Keep only days that have closed: bar open + 1 day ≤ now.
        completed = [
            r for r in rows
            if datetime.fromtimestamp(r[0] / 1000, timezone.utc) + _ONE_DAY <= now
        ]
        required = max(self.buy_gate_sma_days, self.short_gate_sma_days)
        if len(completed) < required:
            raise RuntimeError(
                f"{symbol}: only {len(completed)} completed daily candles; "
                f"need {required} for the SMA gates"
            )
        closes = [float(r[4]) for r in completed]
        close = closes[-1]
        sma_buy = sum(closes[-self.buy_gate_sma_days:]) / self.buy_gate_sma_days
        sma_short = sum(closes[-self.short_gate_sma_days:]) / self.short_gate_sma_days
        return {
            'day': datetime.fromtimestamp(completed[-1][0] / 1000, timezone.utc),
            'close': close,
            'sma_buy': sma_buy,
            'sma_short': sma_short,
            'buys_allowed': not (close < sma_buy),
            'fetched_at': now,
        }

    def _log_status(self, symbol: str) -> None:
        s = self.status[symbol]
        self.logger.info(
            f"{symbol} daily {s['day']:%Y-%m-%d} close={s['close']:.6g} "
            f"sma{self.short_gate_sma_days}={s['sma_short']:.6g} "
            f"sma{self.buy_gate_sma_days}={s['sma_buy']:.6g} "
            f"buys={'ALLOWED' if s['buys_allowed'] else 'PAUSED'} "
            f"below_sma{self.short_gate_sma_days}={s['close'] < s['sma_short']}"
        )

    # ── Gate ────────────────────────────────────────────────────────────

    def buy_gate(self, symbol: str, now: datetime):
        """Return (allowed: bool, reason: str)."""
        s = self.status[symbol]
        age_hours = (now - (s['day'] + _ONE_DAY)).total_seconds() / 3600.0
        if age_hours > self.max_staleness_hours:
            return False, (
                f"daily data stale: last completed day {s['day']:%Y-%m-%d} "
                f"is {age_hours:.1f}h old (> {self.max_staleness_hours}h)"
            )
        if not s['buys_allowed']:
            return False, (
                f"close {s['close']:.6g} on {s['day']:%Y-%m-%d} below "
                f"sma{self.buy_gate_sma_days} {s['sma_buy']:.6g}"
            )
        return True, (
            f"close {s['close']:.6g} on {s['day']:%Y-%m-%d} at/above "
            f"sma{self.buy_gate_sma_days} {s['sma_buy']:.6g}"
        )

    SHORT_GATE_MODES = ('below_short_sma', 'below_either_sma')

    def short_gate(self, symbol: str, now: datetime, mode: str):
        """Return (allowed: bool, reason: str) for opening new shorts.

        mode 'below_short_sma':  previous close < its short-gate SMA (50d).
        mode 'below_either_sma': previous close < the 50d OR the 200d SMA.
        Stale daily data blocks new shorts, the same as buy_gate()."""
        if mode not in self.SHORT_GATE_MODES:
            raise ValueError(f"short gate mode {mode!r} not in {self.SHORT_GATE_MODES}")
        s = self.status[symbol]
        age_hours = (now - (s['day'] + _ONE_DAY)).total_seconds() / 3600.0
        if age_hours > self.max_staleness_hours:
            return False, (
                f"daily data stale: last completed day {s['day']:%Y-%m-%d} "
                f"is {age_hours:.1f}h old (> {self.max_staleness_hours}h)"
            )
        below_short = s['close'] < s['sma_short']
        below_buy = s['close'] < s['sma_buy']
        allowed = below_short or (mode == 'below_either_sma' and below_buy)
        return allowed, (
            f"close {s['close']:.6g} on {s['day']:%Y-%m-%d}, "
            f"sma{self.short_gate_sma_days} {s['sma_short']:.6g} "
            f"({'below' if below_short else 'at/above'}), "
            f"sma{self.buy_gate_sma_days} {s['sma_buy']:.6g} "
            f"({'below' if below_buy else 'at/above'}), gate={mode}"
        )
