"""ShortGridStrategy — trailing short grid on Kraken Futures perps.

A copy of strategy_lab's short grid in trailing mode (lab/short_grid_core.py,
mode 3, settings from analysis/export_rules_chart.py), run on live perp
prices instead of spot minute candles. Paper only for now: positions go to
`exchange/paper_futures.py`.

Per token with a perp, using the perp book mid:

  ENTRY  once per `entry_interval_seconds` (the lab used 1-minute closes):
         level = floor(ln(mid) / ln(1 + spacing)). When the level is lower
         than the previous sample's, the short gate allows it, and no open
         short holds that level, open a short of size_pct % of total equity
         — unless total short notional would exceed cap_pct % of total
         equity. The first sample after a restart only sets the level.
  EXIT   every tick, each short on its own (fees = 2 × taker, round trip):
         - before activation: cover at market if mid ≥ entry × (1 + stop).
         - activation: mid ≤ entry × (1 − (fees + trail_activate)%). The
           stop is dropped and the trough (lowest mid since) is tracked.
         - after activation: cover when mid ≥ min(trough × (1 + trail),
           entry × (1 − (fees + trail_floor)%)).
         Shorts are NOT closed when the gate turns off (as in the lab).

`total equity` = spot paper equity + the short account's equity, one pot
as in the lab.
"""
import math
from datetime import datetime

from logging_config import get_logger


class ShortGridStrategy:

    def __init__(self, *, symbols, config, perps, daily_trend, account, journal):
        self.logger = get_logger('ShortGrid')
        self.journal = journal
        self.perps = perps
        self.daily_trend = daily_trend
        self.account = account

        self.gate_mode = config['gate']
        if self.gate_mode not in daily_trend.SHORT_GATE_MODES:
            raise ValueError(
                f"short_grid.gate {self.gate_mode!r} not in {daily_trend.SHORT_GATE_MODES}"
            )
        self.spacing_pct = float(config['spacing_pct'])
        self.size_pct = float(config['size_pct'])
        self.cap_pct = float(config['cap_pct'])
        self.stop_pct = float(config['stop_pct'])
        self.trail_activate_pct = float(config['trail_activate_pct'])
        self.trail_pct = float(config['trail_pct'])
        self.trail_floor_pct = float(config['trail_floor_pct'])
        self.entry_interval_seconds = float(config['entry_interval_seconds'])
        self.max_perp_book_age_seconds = float(config['max_perp_book_age_seconds'])
        for name in ('spacing_pct', 'size_pct', 'cap_pct', 'stop_pct', 'trail_pct',
                     'entry_interval_seconds', 'max_perp_book_age_seconds'):
            if getattr(self, name) <= 0:
                raise ValueError(f"short_grid.{name} must be > 0")
        self.round_trip_fee_pct = 2 * account.taker_fee_pct
        self._log_g = math.log(1 + self.spacing_pct / 100.0)

        # Only symbols with a perp are shorted.
        self.symbols = []
        for s in symbols:
            product = perps.product_for(s)
            if product is None:
                self.logger.warning(f"{s} has no perp in perps.products — never shorted")
            else:
                self.symbols.append(s)
        self.product = {s: perps.product_for(s) for s in self.symbols}

        self.prev_level = {s: None for s in self.symbols}
        self.last_entry_bucket = {s: None for s in self.symbols}
        self.gate_allowed = {s: None for s in self.symbols}
        self.book_ok = {s: None for s in self.symbols}

        self.logger.info(
            f"Short grid: gate={self.gate_mode} spacing={self.spacing_pct}% "
            f"size={self.size_pct}% cap={self.cap_pct}% stop={self.stop_pct}% "
            f"activate={self.trail_activate_pct}%+fees trail={self.trail_pct}% "
            f"floor={self.trail_floor_pct}%+fees taker={account.taker_fee_pct}% "
            f"symbols={self.symbols}"
        )

    # ── Entry point (called every tick by the runner) ───────────────────

    def step(self, now: datetime, spot_equity: float) -> None:
        self.account.apply_funding(now)
        for symbol in self.symbols:
            try:
                self._step_symbol(symbol, now, spot_equity)
            except Exception as e:
                self.logger.error(f"{symbol} short grid error: {e}", exc_info=True)

    def _perp_mid(self, symbol: str, now: datetime):
        """Book mid, or None (with a log on the change) if the perp book is
        invalid or older than max_perp_book_age_seconds."""
        product = self.product[symbol]
        book = self.perps.books[product]
        problem = None
        if not book.valid or not book.bids or not book.asks:
            problem = "perp book not valid"
        elif (now - book.updated_at).total_seconds() > self.max_perp_book_age_seconds:
            problem = (f"perp book {(now - book.updated_at).total_seconds():.0f}s old "
                       f"(> {self.max_perp_book_age_seconds}s)")
        if problem is not None:
            if self.book_ok[symbol] is not False:
                self.logger.warning(f"{symbol} short grid paused: {problem}")
            self.book_ok[symbol] = False
            return None
        if self.book_ok[symbol] is False:
            self.logger.info(f"{symbol} short grid resumed: perp book OK")
        self.book_ok[symbol] = True
        bid, ask = self.perps.best_bid_ask(product)
        return (bid + ask) / 2.0

    def _step_symbol(self, symbol: str, now: datetime, spot_equity: float) -> None:
        mid = self._perp_mid(symbol, now)
        if mid is None:
            return
        for pos in [p for p in self.account.open if p['symbol'] == symbol]:
            self._manage_exit(pos, mid, now)

        bucket = int(now.timestamp() // self.entry_interval_seconds)
        if bucket == self.last_entry_bucket[symbol]:
            return
        self.last_entry_bucket[symbol] = bucket
        level = math.floor(math.log(mid) / self._log_g)
        prev = self.prev_level[symbol]
        self.prev_level[symbol] = level

        allowed, reason = self.daily_trend.short_gate(symbol, now, self.gate_mode)
        if allowed != self.gate_allowed[symbol]:
            self.logger.info(
                f"{symbol} shorts {'ENABLED' if allowed else 'DISABLED'} — {reason}"
            )
            self.journal.write('short_gate', symbol=symbol, allowed=allowed, reason=reason)
            self.gate_allowed[symbol] = allowed

        if prev is None or level >= prev or not allowed:
            return
        if any(p['symbol'] == symbol and p['level'] == level for p in self.account.open):
            return
        total = spot_equity + self.account.equity()
        if total <= 0:
            raise RuntimeError(f"total equity ${total:.2f} is not positive")
        usd = total * self.size_pct / 100.0
        notional = self.account.notional()
        if notional + usd > total * self.cap_pct / 100.0:
            self.logger.info(
                f"{symbol} short at level {level} skipped: cap — notional "
                f"${notional:.2f} + ${usd:.2f} > {self.cap_pct}% of ${total:.2f}"
            )
            return
        self.account.open_short(
            symbol=symbol, product=self.product[symbol], level=level,
            usd=usd, ref_price=mid, now=now, total_equity=total,
            notional_before=notional,
        )

    def _manage_exit(self, pos: dict, mid: float, now: datetime) -> None:
        entry = pos['entry_price']
        if pos['trough'] is None:
            activate_px = entry * (1 - (self.round_trip_fee_pct + self.trail_activate_pct) / 100.0)
            if mid <= activate_px:
                pos['trough'] = mid
                pos['activated_at'] = now.isoformat().replace('+00:00', 'Z')
                self.account.save()
                self.journal.write('short_activated', symbol=pos['symbol'], id=pos['id'],
                                   entry=entry, mid=mid)
                self.logger.info(
                    f"PAPER SHORT trail activated {pos['symbol']} entry={entry:.8g} "
                    f"mid={mid:.8g} ({(1 - mid / entry) * 100:.2f}% ahead)"
                )
            elif mid >= entry * (1 + self.stop_pct / 100.0):
                self.account.cover(pos, 'stop', now, mid)
            return
        floor_px = entry * (1 - (self.round_trip_fee_pct + self.trail_floor_pct) / 100.0)
        cover_px = min(pos['trough'] * (1 + self.trail_pct / 100.0), floor_px)
        if mid >= cover_px:
            self.account.cover(pos, 'trail', now, mid)
        elif mid < pos['trough']:
            pos['trough'] = mid
            self.account.save()
