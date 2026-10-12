"""ShortGridStrategy — short baskets on Kraken Futures perps while a token is below its 50-day.

The short side of strategy_lab's minute_basket study ("longs all the way down, shorts only
below the 50-day", analysis/minute_basket.py, 2× size and 2× target), run on live perp
prices. Paper only for now: positions go to `exchange/paper_futures.py`.

Per token with a perp, using the perp book mid, every tick:

  ENTRY  a fixed grid of levels `spacing_pct` apart, level k at (1 + spacing)^k — the same
         grid as the long ladder. While the short gate allows it (previous daily close below
         the 50-day SMA), each level the mid falls through since the previous tick opens one
         short of size_pct % of total equity at market — unless a short of that level is
         already open, or total short notional would exceed cap_pct % of total equity. The
         first sample after a restart only sets the level.
  EXIT   all open shorts of the token form ONE basket (average entry = qty-weighted entry
         price). It is covered entirely at market when
           target: mid ≤ average entry × (1 − taker − drop target × target_multiplier)
           stop:   mid ≥ average entry × (1 + stop_pct)
         drop target = market_data/swing_target.drop_target (median of the token's last
         completed 2 % hourly drops, clipped). Exits run whether or not the gate is open.

`total equity` = spot portfolio value + the short account's equity, one pot as in the lab.
"""
import math
from datetime import datetime

from logging_config import get_logger


class ShortGridStrategy:

    def __init__(self, *, symbols, config, perps, daily_trend, swing_targets, account, journal):
        self.logger = get_logger('ShortGrid')
        self.journal = journal
        self.perps = perps
        self.daily_trend = daily_trend
        self.swing_targets = swing_targets
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
        self.target_multiplier = float(config['target_multiplier'])
        self.max_perp_book_age_seconds = float(config['max_perp_book_age_seconds'])
        for name in ('spacing_pct', 'size_pct', 'cap_pct', 'stop_pct', 'target_multiplier',
                     'max_perp_book_age_seconds'):
            if getattr(self, name) <= 0:
                raise ValueError(f"short_grid.{name} must be > 0")
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
        self.gate_allowed = {s: None for s in self.symbols}
        self.book_ok = {s: None for s in self.symbols}

        self.logger.info(
            f"Short baskets: gate={self.gate_mode} spacing={self.spacing_pct}% "
            f"size={self.size_pct}% cap={self.cap_pct}% stop={self.stop_pct}% "
            f"target=drop target × {self.target_multiplier:g} taker={account.taker_fee_pct}% "
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
        self._manage_basket(symbol, mid, now)

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
        open_levels = {p['level'] for p in self.account.open if p['symbol'] == symbol}
        # every level fallen through since the last tick: prev, prev − 1, …, level + 1
        for k in range(prev, level, -1):
            if k in open_levels:
                continue
            total = spot_equity + self.account.equity()
            if total <= 0:
                raise RuntimeError(f"total equity ${total:.2f} is not positive")
            usd = total * self.size_pct / 100.0
            notional = self.account.notional()
            if notional + usd > total * self.cap_pct / 100.0:
                self.logger.info(
                    f"{symbol} short at level {k} skipped: cap — notional "
                    f"${notional:.2f} + ${usd:.2f} > {self.cap_pct}% of ${total:.2f}"
                )
                return
            self.account.open_short(
                symbol=symbol, product=self.product[symbol], level=k,
                usd=usd, ref_price=mid, now=now, total_equity=total,
                notional_before=notional,
            )

    def _manage_basket(self, symbol: str, mid: float, now: datetime) -> None:
        basket = [p for p in self.account.open if p['symbol'] == symbol]
        if not basket:
            return
        qty = sum(p['qty'] for p in basket)
        avg = sum(p['qty'] * p['entry_price'] for p in basket) / qty
        drop_pct, rule, _reason = self.swing_targets.drop_target(symbol, now)
        target_pct = drop_pct * self.target_multiplier
        cover_px = avg * (1 - self.account.taker_fee_pct / 100.0 - target_pct / 100.0)
        stop_px = avg * (1 + self.stop_pct / 100.0)
        if mid <= cover_px:
            reason = 'basket_target'
        elif mid >= stop_px:
            reason = 'basket_stop'
        else:
            return
        self.logger.info(
            f"PAPER SHORT basket {reason} {symbol}: {len(basket)} shorts qty={qty:.8g} "
            f"avg entry={avg:.8g} mid={mid:.8g} target={cover_px:.8g} "
            f"({target_pct:.2f}% = {drop_pct:.2f}% {rule} × {self.target_multiplier:g}) stop={stop_px:.8g}"
        )
        self.journal.write('short_basket_close', symbol=symbol, reason=reason, shorts=len(basket),
                           qty=qty, avg_entry=avg, mid=mid, cover_px=cover_px, stop_px=stop_px,
                           target_pct=target_pct, drop_pct=drop_pct, drop_rule=rule)
        for pos in basket:
            self.account.cover(pos, reason, now, mid)
