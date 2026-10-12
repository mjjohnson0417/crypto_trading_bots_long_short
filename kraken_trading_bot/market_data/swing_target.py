"""Aggregate-exit target per token: small in a falling market, sized to recent rallies otherwise.

From strategy_lab (analysis/swing_targets_down.py, "swing θ2 K6 α1, fixed2 below"):

  token's previous daily close below its 50-day SMA   → below_short_sma_pct (+2 %)
  otherwise → clip(swing_alpha × median of the last swing_rallies completed
                   rallies, min_pct, max_pct)

Rallies come from a causal zigzag on COMPLETED hourly closes (Kraken REST, the
last hourly_fetch_limit hours): a swing is confirmed once price reverses
swing_theta_pct % from its running extreme; a completed rally is the move from
the swing low to the swing high, in %. The zigzag is lab/turning_points.causal_dc.

Short-basket cover target (drop_target): the mirror image — clip(swing_alpha ×
median of the last swing_rallies completed DROPS, min_pct, max_pct), a drop being
the move from the swing high to the swing low, in %. No 50-day rule (shorts only
run below the 50-day).

Explicit rules when the swing figure cannot be used (both logged and journaled):
  warmup  fewer than swing_rallies completed rallies in the fetched hours → warmup_pct
  stale   the newest completed hour is older than max_staleness_hours (refreshes
          failing) → warmup_pct
The 50-day state comes from DailyTrendGate (the shorts' gate data).

Fail-loud policy as DailyTrendGate: initialize() raises if any symbol cannot be
fetched; a runtime refresh failure keeps the previous data and logs an error.
"""
from datetime import datetime, timedelta, timezone
from statistics import median

from logging_config import get_logger


_ONE_HOUR = timedelta(hours=1)


def completed_rallies(closes, theta):
    """Sizes (%) of completed rallies, oldest first: lab causal_dc on `closes`,
    a rally completes when the up-leg flips to a down-leg."""
    return completed_swings(closes, theta)[0]


def completed_swings(closes, theta):
    """(rallies, drops): sizes (%) of completed up-legs and down-legs, oldest
    first. A rally completes when the up-leg flips to a down-leg, a drop when
    the down-leg flips to an up-leg."""
    n = len(closes)
    rallies = []
    drops = []
    hi_i = lo_i = 0
    mode, start, ext = 0, -1, -1
    for i in range(n):
        x = closes[i]
        prev_mode, prev_start, prev_ext = mode, start, ext
        if mode == 0:
            if x > closes[hi_i]:
                hi_i = i
            if x < closes[lo_i]:
                lo_i = i
            if x >= closes[lo_i] * (1 + theta):
                mode, start, ext = 1, lo_i, i
            elif x <= closes[hi_i] * (1 - theta):
                mode, start, ext = -1, hi_i, i
        elif mode == 1:
            if x > closes[ext]:
                ext = i
            elif x <= closes[ext] * (1 - theta):
                mode, start, ext = -1, ext, i
        else:
            if x < closes[ext]:
                ext = i
            elif x >= closes[ext] * (1 + theta):
                mode, start, ext = 1, ext, i
        if prev_mode == 1 and mode == -1:
            rallies.append(100.0 * (closes[prev_ext] / closes[prev_start] - 1))
        elif prev_mode == -1 and mode == 1:
            drops.append(100.0 * (1 - closes[prev_ext] / closes[prev_start]))
    return rallies, drops


class SwingTargets:

    def __init__(self, symbols, rest_exchange, config, daily_trend, journal):
        self.logger = get_logger('SwingTargets')
        self.symbols = [s.upper() for s in symbols]
        self.rest_exchange = rest_exchange
        self.daily_trend = daily_trend
        self.journal = journal

        self.theta_pct = float(config['swing_theta_pct'])
        self.rallies = int(config['swing_rallies'])
        self.alpha = float(config['swing_alpha'])
        self.min_pct = float(config['min_pct'])
        self.max_pct = float(config['max_pct'])
        self.below_short_sma_pct = float(config['below_short_sma_pct'])
        self.warmup_pct = float(config['warmup_pct'])
        self.fetch_limit = int(config['hourly_fetch_limit'])
        self.refresh_interval_seconds = float(config['refresh_interval_seconds'])
        self.max_staleness_hours = float(config['max_staleness_hours'])
        for name in ('theta_pct', 'rallies', 'alpha', 'min_pct', 'max_pct', 'below_short_sma_pct',
                     'warmup_pct', 'fetch_limit', 'refresh_interval_seconds', 'max_staleness_hours'):
            if getattr(self, name) <= 0:
                raise ValueError(f"aggregate_target.{name} must be > 0")
        if self.min_pct > self.max_pct:
            raise ValueError("aggregate_target.min_pct must not exceed max_pct")

        # symbol → dict(hour, rallies, swing_pct or None, fetched_at)
        self.status = {}
        # symbol → (rule, target rounded) last journaled
        self._journaled = {}
        self._journaled_drop = {}

    # ── Refresh ─────────────────────────────────────────────────────────

    async def initialize(self) -> None:
        """First fetch for every symbol. Raises on any failure."""
        now = datetime.now(timezone.utc)
        for symbol in self.symbols:
            self.status[symbol] = await self._compute(symbol, now)
            self._log_status(symbol)

    async def refresh(self) -> None:
        now = datetime.now(timezone.utc)
        for symbol in self.symbols:
            try:
                new = await self._compute(symbol, now)
            except Exception as e:
                self.logger.error(f"{symbol} swing target refresh failed: {e}")
                continue
            old = self.status[symbol]
            self.status[symbol] = new
            if (new['swing_pct'] != old['swing_pct'] or new['n_rallies'] != old['n_rallies']
                    or new['drop_pct'] != old['drop_pct']):
                self._log_status(symbol)

    async def _compute(self, symbol: str, now: datetime) -> dict:
        rows = await self.rest_exchange.fetch_ohlcv(symbol, '1h', limit=self.fetch_limit)
        completed = [
            r for r in rows
            if datetime.fromtimestamp(r[0] / 1000, timezone.utc) + _ONE_HOUR <= now
        ]
        if not completed:
            raise RuntimeError(f"{symbol}: no completed hourly candles returned")
        closes = [float(r[4]) for r in completed]
        rallies, drops = completed_swings(closes, self.theta_pct / 100.0)
        med, swing_pct = self._clip_median(rallies)
        d_med, drop_pct = self._clip_median(drops)
        return {
            'hour': datetime.fromtimestamp(completed[-1][0] / 1000, timezone.utc),
            'n_hours': len(completed),
            'n_rallies': len(rallies),
            'median_pct': med,
            'swing_pct': swing_pct,
            'n_drops': len(drops),
            'drop_median_pct': d_med,
            'drop_pct': drop_pct,
            'fetched_at': now,
        }

    def _clip_median(self, sizes):
        """(median, clipped target) of the last `swing_rallies` sizes, or (None, None)."""
        last = sizes[-self.rallies:]
        if len(last) < self.rallies:
            return None, None
        med = median(last)
        return med, min(self.max_pct, max(self.min_pct, self.alpha * med))

    def _log_status(self, symbol: str) -> None:
        s = self.status[symbol]
        if s['swing_pct'] is None:
            self.logger.info(
                f"{symbol} swing target: {s['n_rallies']} completed {self.theta_pct:g}% rallies in "
                f"{s['n_hours']}h (< {self.rallies}) → warmup {self.warmup_pct:g}%"
            )
        else:
            self.logger.info(
                f"{symbol} swing target {s['swing_pct']:.2f}% = clip({self.alpha:g} × median "
                f"{s['median_pct']:.2f}% of the last {self.rallies} of {s['n_rallies']} rallies, "
                f"{self.min_pct:g}–{self.max_pct:g}%), last hour {s['hour']:%Y-%m-%d %H:%M}"
            )
        if s['drop_pct'] is None:
            self.logger.info(f"{symbol} drop target: {s['n_drops']} completed drops (< {self.rallies}) "
                             f"→ warmup {self.warmup_pct:g}%")
        else:
            self.logger.info(f"{symbol} drop target {s['drop_pct']:.2f}% = clip({self.alpha:g} × median "
                             f"{s['drop_median_pct']:.2f}% of the last {self.rallies} of {s['n_drops']} drops)")

    # ── Target ──────────────────────────────────────────────────────────

    def target(self, symbol: str, now: datetime):
        """Return (profit_pct, rule, reason) for the aggregate exit."""
        d = self.daily_trend.status[symbol]
        if d['close'] < d['sma_short']:
            pct, rule = self.below_short_sma_pct, 'below_short_sma'
            reason = (f"close {d['close']:.6g} on {d['day']:%Y-%m-%d} below "
                      f"sma{self.daily_trend.short_gate_sma_days} {d['sma_short']:.6g}")
        else:
            s = self.status[symbol]
            age_hours = (now - (s['hour'] + _ONE_HOUR)).total_seconds() / 3600.0
            if age_hours > self.max_staleness_hours:
                pct, rule = self.warmup_pct, 'stale'
                reason = (f"hourly data stale: last completed hour {s['hour']:%Y-%m-%d %H:%M} "
                          f"is {age_hours:.1f}h old (> {self.max_staleness_hours}h)")
                self.logger.error(f"{symbol} swing target {reason} — using warmup {pct:g}%")
            elif s['swing_pct'] is None:
                pct, rule = self.warmup_pct, 'warmup'
                reason = f"{s['n_rallies']} completed rallies < {self.rallies}"
            else:
                pct, rule = s['swing_pct'], 'swing'
                reason = (f"{self.alpha:g} × median {s['median_pct']:.2f}% of the last "
                          f"{self.rallies} {self.theta_pct:g}% rallies")
        key = (rule, round(pct, 2))
        if self._journaled.get(symbol) != key:
            self._journaled[symbol] = key
            self.journal.write('swing_target', symbol=symbol, rule=rule, target_pct=pct, reason=reason)
        return pct, rule, reason

    def drop_target(self, symbol: str, now: datetime):
        """Return (drop_pct, rule, reason) for the short-basket cover."""
        s = self.status[symbol]
        age_hours = (now - (s['hour'] + _ONE_HOUR)).total_seconds() / 3600.0
        if age_hours > self.max_staleness_hours:
            pct, rule = self.warmup_pct, 'stale'
            reason = (f"hourly data stale: last completed hour {s['hour']:%Y-%m-%d %H:%M} "
                      f"is {age_hours:.1f}h old (> {self.max_staleness_hours}h)")
            self.logger.error(f"{symbol} drop target {reason} — using warmup {pct:g}%")
        elif s['drop_pct'] is None:
            pct, rule = self.warmup_pct, 'warmup'
            reason = f"{s['n_drops']} completed drops < {self.rallies}"
        else:
            pct, rule = s['drop_pct'], 'swing'
            reason = (f"{self.alpha:g} × median {s['drop_median_pct']:.2f}% of the last "
                      f"{self.rallies} {self.theta_pct:g}% drops")
        key = (rule, round(pct, 2))
        if self._journaled_drop.get(symbol) != key:
            self._journaled_drop[symbol] = key
            self.journal.write('drop_target', symbol=symbol, rule=rule, target_pct=pct, reason=reason)
        return pct, rule, reason
