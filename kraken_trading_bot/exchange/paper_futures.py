"""PaperFuturesAccount — simulated Kraken Futures short positions (dry run).

Holds the paper shorts opened by `strategy/short_grid.py`. Prices are
real: every open and cover walks the live perp order book in
`dm.perps` and pays the configured Kraken Futures taker fee.

Ledger (mirrors strategy_lab's short grid, which shares one pot with the
spot ladder):
  cash      running USD result of the short side: gross P&L of covered
            shorts − every fee paid + funding received. Starts at 0.
  equity    cash + unrealized P&L of open shorts at the perp mark price.
The runner adds `equity` to the spot paper equity for sizing and reports.

Funding: Kraken charges PF_ perps hourly on the hour. At each hour
boundary every open short receives qty × mark × relative_funding_rate
(positive rate = longs pay shorts, so the short receives it). If the bot
was down across several boundaries, the missed hours are charged at the
current rate and a warning is logged.

Margin and liquidation are not simulated. `margin_required()` reports the
initial margin Kraken would ask for (first margin tier), for reference.

State: `<state_dir>/paper_shorts.json` (ledger + open shorts),
`paper_shorts_history.jsonl` (one line per covered short) and
`paper_shorts_equity.jsonl` (one mark-to-market line per record_equity).
"""
import json
import math
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from logging_config import get_logger


_ONE_HOUR = timedelta(hours=1)


def _iso(dt: datetime) -> str:
    return dt.isoformat().replace('+00:00', 'Z')


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace('Z', '+00:00'))


def _floor_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


class PaperFuturesAccount:

    def __init__(self, *, perps, taker_fee_pct, state_dir, journal):
        self.logger = get_logger('PaperFutures')
        self.perps = perps
        self.journal = journal
        self.taker_fee_pct = float(taker_fee_pct)
        if self.taker_fee_pct < 0:
            raise ValueError("short_grid.taker_fee_pct must be >= 0")

        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.state_dir / 'paper_shorts.json'
        self.history_file = self.state_dir / 'paper_shorts_history.jsonl'
        self.equity_file = self.state_dir / 'paper_shorts_equity.jsonl'
        self._load()

    # ── Persistence ─────────────────────────────────────────────────────

    def _load(self) -> None:
        if self.state_file.exists():
            with open(self.state_file) as f:
                st = json.load(f)
            self.created_at = st['created_at']
            self.cash = float(st['cash'])
            self.gross_realized = float(st['gross_realized'])
            self.fees_paid = float(st['fees_paid'])
            self.funding_received = float(st['funding_received'])
            self.closed_count = int(st['closed_count'])
            self.last_funding_hour = (
                None if st['last_funding_hour'] is None else _parse(st['last_funding_hour'])
            )
            self.open = st['open']
            self.logger.warning(
                f"PAPER shorts loaded: open={len(self.open)} cash=${self.cash:.2f} "
                f"closed={self.closed_count}"
            )
        else:
            self.created_at = _iso(datetime.now(timezone.utc))
            self.cash = 0.0
            self.gross_realized = 0.0
            self.fees_paid = 0.0
            self.funding_received = 0.0
            self.closed_count = 0
            self.last_funding_hour = None
            self.open = []
            self.save()
            self.logger.warning(f"PAPER shorts account created ({self.state_file})")

    def save(self) -> None:
        tmp = self.state_file.with_suffix('.json.tmp')
        with open(tmp, 'w') as f:
            json.dump({
                'created_at': self.created_at,
                'cash': self.cash,
                'gross_realized': self.gross_realized,
                'fees_paid': self.fees_paid,
                'funding_received': self.funding_received,
                'closed_count': self.closed_count,
                'last_funding_hour': (
                    None if self.last_funding_hour is None else _iso(self.last_funding_hour)
                ),
                'open': self.open,
            }, f, indent=2)
        tmp.replace(self.state_file)

    # ── Market helpers ──────────────────────────────────────────────────

    def _walk(self, product: str, side: str, qty: float) -> float:
        """Average fill price for `qty` against the live perp book.
        side 'bid' for a sell (open short), 'ask' for a buy (cover). If the
        visible book is thinner than qty, the rest fills at the last level
        and a warning is logged."""
        remaining = qty
        cost = 0.0
        last_price = None
        for price, level_qty in self.perps.book_levels(product, side):
            take = min(remaining, level_qty)
            cost += take * price
            remaining -= take
            last_price = price
            if remaining <= 1e-15:
                break
        if last_price is None:
            raise RuntimeError(f"{product}: perp book has no {side}s")
        if remaining > 1e-15:
            self.logger.warning(
                f"{product} paper order {qty} deeper than visible book; "
                f"{remaining} filled at last level {last_price}"
            )
            cost += remaining * last_price
        return cost / qty

    def _qty_for_usd(self, product: str, usd: float, price: float) -> float:
        """Contracts for `usd` notional, rounded DOWN to the product's
        contract precision. 0.0 if smaller than one step."""
        precision = int(self.perps.instruments[product]['contractValueTradePrecision'])
        step = 10.0 ** -precision
        qty = math.floor(usd / price / step) * step
        return round(qty, max(precision, 0))

    def _mark(self, product: str) -> float:
        return float(self.perps.latest_ticker(product)['markPrice'])

    # ── Open / cover ────────────────────────────────────────────────────

    def open_short(self, *, symbol: str, product: str, level: int, usd: float,
                   ref_price: float, now: datetime, total_equity: float,
                   notional_before: float):
        """Sell `usd` worth of `product` at market. `ref_price` is the perp
        mid that triggered the entry. Returns the position dict, or None if
        usd rounds below one contract step."""
        qty = self._qty_for_usd(product, usd, ref_price)
        if qty <= 0:
            self.logger.warning(
                f"{symbol} paper short skipped: ${usd:.2f} is below one "
                f"{product} contract step at {ref_price:.6g}"
            )
            return None
        price = self._walk(product, 'bid', qty)
        fee = qty * price * self.taker_fee_pct / 100.0
        self.cash -= fee
        self.fees_paid += fee
        pos = {
            'id': uuid.uuid4().hex[:12],
            'symbol': symbol,
            'product': product,
            'level': int(level),
            'qty': qty,
            'entry_price': price,
            'opened_at': _iso(now),
            'entry_fee': fee,
            'funding': 0.0,
        }
        self.open.append(pos)
        self.save()
        slippage = (ref_price - price) / ref_price * 100   # + = filled worse than the mid
        self.journal.write(
            'short_open', symbol=symbol, product=product, id=pos['id'], level=int(level),
            qty=qty, trigger_mid=ref_price, fill_price=price, slippage_pct=slippage,
            fee=fee, usd_target=usd, total_equity=total_equity,
            notional_before=notional_before,
        )
        self.logger.info(
            f"PAPER SHORT open {symbol} ({product}) qty={qty:.8g} @ {price:.8g} "
            f"(trigger mid {ref_price:.8g}, slippage {slippage:+.3f}%) "
            f"notional=${qty * price:.2f} fee=${fee:.4f} level={level} "
            f"open_shorts={len(self.open)}"
        )
        return pos

    def cover(self, pos: dict, reason: str, now: datetime, trigger_mid: float) -> None:
        """Buy back `pos` at market and record it in the history file.
        `trigger_mid` is the perp mid that triggered the stop / trail."""
        price = self._walk(pos['product'], 'ask', pos['qty'])
        slippage = (price - trigger_mid) / trigger_mid * 100   # + = filled worse than the mid
        fee = pos['qty'] * price * self.taker_fee_pct / 100.0
        gross = pos['qty'] * (pos['entry_price'] - price)
        self.cash += gross - fee
        self.gross_realized += gross
        self.fees_paid += fee
        self.closed_count += 1
        self.open = [p for p in self.open if p['id'] != pos['id']]
        net = gross - pos['entry_fee'] - fee + pos['funding']
        row = dict(pos)
        row.update({
            'closed_at': _iso(now),
            'reason': reason,
            'trigger_mid': trigger_mid,
            'exit_slippage_pct': slippage,
            'exit_price': price,
            'exit_fee': fee,
            'gross_pnl': gross,
            'net_pnl': net,
            'return_pct': net / (pos['qty'] * pos['entry_price']) * 100,
        })
        with open(self.history_file, 'a') as f:
            f.write(json.dumps(row) + '\n')
        self.save()
        self.journal.write(
            'short_cover', symbol=pos['symbol'], id=pos['id'], reason=reason,
            trigger_mid=trigger_mid, fill_price=price, slippage_pct=slippage,
            entry_price=pos['entry_price'], qty=pos['qty'], gross_pnl=gross,
            fees=pos['entry_fee'] + fee, funding=pos['funding'], net_pnl=net,
            return_pct=row['return_pct'], opened_at=pos['opened_at'],
        )
        self.logger.info(
            f"PAPER SHORT cover ({reason}) {pos['symbol']} qty={pos['qty']:.8g} "
            f"entry={pos['entry_price']:.8g} exit={price:.8g} "
            f"net=${net:+.2f} ({row['return_pct']:+.2f}%) open_shorts={len(self.open)}"
        )

    # ── Funding ─────────────────────────────────────────────────────────

    def apply_funding(self, now: datetime) -> None:
        hour = _floor_hour(now)
        if self.last_funding_hour is None:
            self.last_funding_hour = hour
            self.save()
            return
        hours = int(round((hour - self.last_funding_hour) / _ONE_HOUR))
        if hours <= 0:
            return
        if hours > 1 and self.open:
            self.logger.warning(
                f"PAPER shorts: {hours} funding hours since {_iso(self.last_funding_hour)} "
                f"(bot was down); charging the missed hours at the current rates"
            )
        total = 0.0
        per_position = []
        for pos in self.open:
            ticker = self.perps.latest_ticker(pos['product'])
            rate = float(ticker['relative_funding_rate'])
            amount = pos['qty'] * float(ticker['markPrice']) * rate * hours
            pos['funding'] += amount
            total += amount
            per_position.append({'id': pos['id'], 'symbol': pos['symbol'],
                                 'rate': rate, 'hours': hours, 'amount': amount})
        self.cash += total
        self.funding_received += total
        self.last_funding_hour = hour
        self.save()
        if self.open:
            self.journal.write('short_funding', hour=_iso(hour), total=total,
                               positions=per_position)
            self.logger.info(
                f"PAPER shorts funding {_iso(hour)}: ${total:+.4f} on "
                f"{len(self.open)} shorts (total ${self.funding_received:+.4f})"
            )

    # ── Valuation ───────────────────────────────────────────────────────

    def notional(self) -> float:
        return sum(p['qty'] * self._mark(p['product']) for p in self.open)

    def unrealized(self) -> float:
        return sum(p['qty'] * (p['entry_price'] - self._mark(p['product'])) for p in self.open)

    def equity(self) -> float:
        return self.cash + self.unrealized()

    def margin_required(self) -> float:
        """Initial margin at each product's first Kraken margin tier."""
        return sum(
            p['qty'] * self._mark(p['product'])
            * float(self.perps.instruments[p['product']]['marginLevels'][0]['initialMargin'])
            for p in self.open
        )

    def record_equity(self, spot_equity: float) -> dict:
        """Mark open shorts to market, append a line to
        paper_shorts_equity.jsonl, and return the summary."""
        notional = self.notional()
        unreal = self.unrealized()
        short_equity = self.cash + unreal
        total = spot_equity + short_equity
        row = {
            'ts': _iso(datetime.now(timezone.utc)),
            'open_shorts': len(self.open),
            'notional': notional,
            'notional_pct_of_total': notional / total * 100 if total > 0 else None,
            'margin_required': self.margin_required(),
            'unrealized': unreal,
            'cash': self.cash,
            'gross_realized': self.gross_realized,
            'fees_paid': self.fees_paid,
            'funding_received': self.funding_received,
            'closed_count': self.closed_count,
            'short_equity': short_equity,
            'spot_equity': spot_equity,
            'total_equity': total,
        }
        with open(self.equity_file, 'a') as f:
            f.write(json.dumps(row) + '\n')
        return row
