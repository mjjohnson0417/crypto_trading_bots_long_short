"""HybridGridStrategy — single-class strategy module.

The runner constructs once with `symbols`, `config`, `fee_cache`,
`data_manager`, `journal`, `swing_targets`. Then calls `evaluate(symbol, indicators, mid_price,
orders, cached_balance, now, buys_allowed, buy_gate_reason)` per tick.
`buys_allowed` comes from the daily-trend gate (200-day SMA, or always True
when daily_trend.buy_gate_enabled is false): while it is False, no new buys
are placed, resting buys are canceled, and exits still run. Returns a list of
plain-dict actions for the runner to dispatch against `order_ops`.

Ladder = a FIXED grid of buy prices per token, level k at (1 + spacing)^k.
`rung_count` resting limit buys sit at the highest free levels below the
mid; a level holding a filled, unsold ladder buy is not bought again until
the basket sells (so the bot never stacks buys at one price). The grid does
not move with the regime: buys continue all the way down. All filled ladder
buys of a token form one basket, sold at market at the aggregate target.

Action dicts (every action also carries "reason" and "regime" for the
trade journal; the runner ignores keys it does not dispatch on):
    {"action": "place_market_buy", "symbol", "qty", "cl_ord_id"}
    {"action": "place_limit_buy",  "symbol", "qty", "price", "cl_ord_id"}
    {"action": "place_limit_sell", "symbol", "qty", "price", "cl_ord_id"}
    {"action": "cancel_order",     "symbol", "order_id"}

cl_ord_id convention (capped at Kraken's 18-char free-text limit):
    Buy:               "<r>-<bp>-<rand8>"  — r='c'(chase) or 'l'(ladder).
    Chase TP sell:     "s-<rand8>"         — parent linked at place time.
    Aggregate sell:    "a-<rand8>"         — covered cycles bound by symbol on first WS event.
"""
from __future__ import annotations

import math
import uuid
from typing import List, Optional

from logging_config import get_logger
from price_calculator import calculate_tp_price, get_tick_size
from state_machine.regime_classifier import RegimeClassifier, STATE_STRONG_UPTREND


_ACTIVE_STATUSES = ('pending_new', 'new', 'partially_filled')


class _ChaseState:
    def __init__(self):
        self.strong_uptrend_start_price: Optional[float] = None
        self.last_confirmed_state: Optional[str] = None


class _BuyGateState:
    def __init__(self):
        self.allowed: Optional[bool] = None
        self.last_cancel_sweep = None


class HybridGridStrategy:

    def __init__(self, symbols, config, fee_cache, data_manager, journal, swing_targets):
        self.logger = get_logger('HybridGridStrategy')
        # Aggregate-exit target per token (market_data/swing_target.py):
        # +2 % below the 50-day, else sized to the token's recent rallies.
        self.swing_targets = swing_targets
        self.symbols = [s.upper() for s in symbols]
        self.fee_cache = fee_cache
        self.data_manager = data_manager
        self.journal = journal

        hcfg = config['hybrid_grid']
        h_ladder = hcfg['ladder']
        h_chase = hcfg['chase']
        h_ladder_defaults = hcfg['ladder_defaults']

        self.hysteresis_seconds = int(hcfg['hysteresis_seconds'])
        self.ladder_rung_count = int(h_ladder['rung_count'])
        self.ladder_spacing_pct = float(h_ladder['spacing_pct'])
        if self.ladder_rung_count <= 0 or self.ladder_spacing_pct <= 0:
            raise ValueError("hybrid_grid.ladder rung_count and spacing_pct must be > 0")
        self._log_g = math.log(1.0 + self.ladder_spacing_pct / 100.0)
        self.chase_enabled = bool(h_chase['enabled'])
        self.chase_position_size_pct = float(h_chase['position_size_pct'])
        self.chase_profit_pct = float(h_chase['profit_pct'])
        self.chase_spacing_pct = float(h_chase['spacing_pct'])
        self.ladder_position_size_pct = float(h_ladder_defaults['position_size_pct'])
        self.ladder_profit_pct = float(h_ladder_defaults['profit_pct'])
        self.buy_gate_cancel_retry_seconds = float(
            config['daily_trend']['buy_gate_cancel_retry_seconds']
        )

        if not self.chase_enabled:
            self.logger.info(
                "Chase strategy is disabled in config — no new chase (c-) entries or market buys will be created. "
                "Legacy chase cycles (if any remain in tracker) will still generate TP exits via _chase_exits when their target is reached."
            )

        self.classifiers = {
            s: RegimeClassifier(hysteresis_seconds=self.hysteresis_seconds)
            for s in self.symbols
        }
        self.chase = {s: _ChaseState() for s in self.symbols}
        self.buy_gate = {s: _BuyGateState() for s in self.symbols}
        # Last confirmed regime per symbol, for transition logging.
        self.regime = {s: None for s in self.symbols}

    # ── Public entry point ───────────────────────────────────────────────

    def evaluate(self, symbol, indicators, mid_price, orders, cached_balance, now,
                 buys_allowed, buy_gate_reason) -> List[dict]:
        actions: List[dict] = []
        state = self.classifiers[symbol].update(indicators, now)
        prev_state = self.regime[symbol]
        if state != prev_state:
            self.logger.info(f"{symbol} regime {prev_state} → {state}")
            self.journal.write('regime_change', symbol=symbol, **{'from': prev_state, 'to': state})
            self.regime[symbol] = state

        actions.extend(self._handle_state_transitions(symbol, state, mid_price, orders))

        if not buys_allowed:
            # Daily-trend gate: no new buys; cancel resting ones (skip any
            # the state transition above already canceled this tick).
            already = {a['order_id'] for a in actions if a['action'] == 'cancel_order'}
            for a in self._buy_gate_paused(symbol, buy_gate_reason, orders, now):
                if a['order_id'] not in already:
                    actions.append(a)
        else:
            self._buy_gate_allowed(symbol, buy_gate_reason)
            if state == STATE_STRONG_UPTREND and self.chase_enabled:
                actions.extend(self._chase_decisions(symbol, mid_price, orders, cached_balance))
            actions.extend(self._ladder_decisions(symbol, mid_price, orders, cached_balance))

        actions.extend(self._exit_decisions(symbol, mid_price, orders, now))
        for a in actions:
            a['regime'] = state
        return actions

    # ── cl_ord_id encode / decode (strategy-private) ─────────────────────

    # Kraken WS v2 caps free-text cl_ord_id at 18 chars.
    # Format: "<r>-<bp>-<rand8>"  →  e.g. "l-0200-233fcd69" (15 chars)
    # where <r> is 'c' (chase) or 'l' (ladder), bp is profit_pct × 100.
    _RULE_TO_CODE = {'chase': 'c', 'ladder': 'l'}
    _CODE_TO_RULE = {v: k for k, v in _RULE_TO_CODE.items()}

    @classmethod
    def _encode_buy_cl_ord_id(cls, rule: str, profit_pct: float) -> str:
        code = cls._RULE_TO_CODE[rule]
        bp = round(float(profit_pct) * 100)
        return f"{code}-{bp:04d}-{uuid.uuid4().hex[:8]}"

    @classmethod
    def _decode_buy_cl_ord_id(cls, cl_ord_id: str):
        parts = cl_ord_id.split('-')
        if len(parts) != 3:
            return None
        rule = cls._CODE_TO_RULE.get(parts[0])
        if rule is None:
            return None
        try:
            profit_pct = int(parts[1]) / 100.0
        except ValueError:
            return None
        return rule, profit_pct

    # Sell cl_ord_ids: 10 chars, prefix indicates kind.
    #   's-<rand8>'  — chase TP limit sell (parent linked at place time via action dict)
    #   'a-<rand8>'  — aggregate ladder market sell (covered cycles bound by symbol on first WS event)
    @staticmethod
    def _encode_chase_sell_cl_ord_id() -> str:
        return f"s-{uuid.uuid4().hex[:8]}"

    # ── State-transition handling (chase only; the ladder ignores the regime) ──

    def _handle_state_transitions(self, symbol, state, mid_price, orders) -> List[dict]:
        actions: List[dict] = []
        chase = self.chase[symbol]

        # Chase floor capture/release.
        if state == STATE_STRONG_UPTREND and chase.last_confirmed_state != STATE_STRONG_UPTREND:
            if mid_price > 0:
                chase.strong_uptrend_start_price = float(mid_price)
        elif chase.last_confirmed_state == STATE_STRONG_UPTREND and state != STATE_STRONG_UPTREND:
            chase.strong_uptrend_start_price = None
            # Cancel any open chase buys on exit from STRONG_UPTREND.
            for buy_oid, _cycle in self._active_buys_by_rule(symbol, orders, 'chase'):
                actions.append({"action": "cancel_order", "symbol": symbol,
                                "order_id": buy_oid, "reason": "exit_strong_uptrend"})

        chase.last_confirmed_state = state
        return actions

    # ── Daily-trend buy gate ────────────────────────────────────────────

    def _buy_gate_paused(self, symbol, reason, orders, now) -> List[dict]:
        """Buys are paused: cancel every resting buy for the symbol on
        the flip, and re-sweep every buy_gate_cancel_retry_seconds in
        case a cancel did not take."""
        gate = self.buy_gate[symbol]
        flipped = gate.allowed is not False
        if flipped:
            self.logger.warning(f"{symbol} buys PAUSED — {reason}")
            self.journal.write('buy_gate', symbol=symbol, allowed=False, reason=reason)
        gate.allowed = False

        if (not flipped and gate.last_cancel_sweep is not None
                and (now - gate.last_cancel_sweep).total_seconds()
                < self.buy_gate_cancel_retry_seconds):
            return []
        gate.last_cancel_sweep = now

        actions: List[dict] = []
        for rule in ('ladder', 'chase'):
            for buy_oid, _cycle in self._active_buys_by_rule(symbol, orders, rule):
                actions.append({"action": "cancel_order", "symbol": symbol,
                                "order_id": buy_oid, "reason": "buy_gate_paused"})
        if actions:
            self.logger.info(
                f"{symbol} buy gate canceling {len(actions)} resting buy(s)"
            )
        return actions

    def _buy_gate_allowed(self, symbol, reason) -> None:
        gate = self.buy_gate[symbol]
        if gate.allowed is not True:
            self.logger.info(f"{symbol} buys ALLOWED — {reason}")
            self.journal.write('buy_gate', symbol=symbol, allowed=True, reason=reason)
        gate.allowed = True
        gate.last_cancel_sweep = None

    # ── Chase decisions ─────────────────────────────────────────────────

    def _chase_decisions(self, symbol, mid_price, orders, cached_balance) -> List[dict]:
        chase = self.chase[symbol]
        floor = chase.strong_uptrend_start_price
        if floor is None or mid_price < floor:
            return []

        # Already an active chase buy in flight?
        for _buy_oid, _cycle in self._active_buys_by_rule(symbol, orders, 'chase'):
            return []

        # Spacing against any active cycle for this symbol.
        if self._violates_spacing(symbol, mid_price, orders, self.chase_spacing_pct):
            return []

        size_usd = cached_balance * self.chase_position_size_pct / 100.0
        if size_usd <= 0:
            return []
        qty = size_usd / mid_price
        cl_ord_id = self._encode_buy_cl_ord_id('chase', self.chase_profit_pct)
        return [{"action": "place_market_buy", "symbol": symbol,
                 "qty": qty, "cl_ord_id": cl_ord_id, "reason": "chase_entry"}]

    # ── Ladder decisions ────────────────────────────────────────────────

    def _ladder_decisions(self, symbol, mid_price, orders, cached_balance) -> List[dict]:
        """Keep a resting buy at each of the `rung_count` highest free grid
        levels below the mid. Free = no filled, unsold ladder buy at that
        level. Resting buys at any other level are canceled."""
        if mid_price <= 0:
            return []
        resting = {}          # level → buy order_id
        held = set()          # levels with a filled, unsold ladder buy
        for buy_oid, cycle in orders.items():
            if cycle['symbol'] != symbol or cycle['sell'] is not None:
                continue
            buy = cycle['buy']
            meta = self._decode_buy_cl_ord_id(buy['cl_ord_id'])
            if meta is None or meta[0] != 'ladder':
                continue
            if buy['status'] in _ACTIVE_STATUSES:
                resting[self._grid_level(buy['limit_price'])] = buy_oid
            elif buy['status'] == 'filled':
                held.add(self._grid_level(buy['limit_price']))

        k = math.floor(math.log(mid_price) / self._log_g)
        if math.exp(k * self._log_g) >= mid_price:
            k -= 1
        desired = []
        while len(desired) < self.ladder_rung_count:
            if k not in held:
                desired.append(k)
            k -= 1

        actions: List[dict] = []
        for level, oid in resting.items():
            if level not in desired:
                actions.append({"action": "cancel_order", "symbol": symbol,
                                "order_id": oid, "reason": "grid_move"})
        size_usd = cached_balance * self.ladder_position_size_pct / 100.0
        if size_usd <= 0:
            return actions
        for level in desired:
            if level in resting:
                continue
            price = math.exp(level * self._log_g)
            actions.append({"action": "place_limit_buy", "symbol": symbol,
                            "qty": size_usd / price, "price": price,
                            "cl_ord_id": self._encode_buy_cl_ord_id('ladder', self.ladder_profit_pct),
                            "reason": "grid_rung", "level": level})
        return actions

    def _grid_level(self, price: float) -> int:
        """Grid level of a buy's limit price (orders are rounded down to the
        price tick, far finer than the grid spacing)."""
        return round(math.log(price) / self._log_g)

    # ── Exit decisions ──────────────────────────────────────────────────

    def _exit_decisions(self, symbol, mid_price, orders, now) -> List[dict]:
        """Two paths:
           - Chase: each filled chase buy with no sell → its own limit TP.
           - Ladder: aggregate avg-cost across all filled ladder buys
             with no sell. If mid_price ≥ target, ONE market sell for
             the total qty. The profit part of the target comes from
             swing_targets.target() (not ladder_defaults.profit_pct).
        """
        actions: List[dict] = []
        actions.extend(self._chase_exits(symbol, mid_price, orders))
        actions.extend(self._ladder_exits(symbol, mid_price, orders, now))
        return actions

    def _chase_exits(self, symbol, mid_price, orders) -> List[dict]:
        actions: List[dict] = []
        for buy_oid, cycle in orders.items():
            if cycle['symbol'] != symbol:
                continue
            buy = cycle['buy']
            if buy['status'] != 'filled':
                continue
            if cycle['sell'] is not None:
                continue
            meta = self._decode_buy_cl_ord_id(buy['cl_ord_id'])
            if meta is None or meta[0] != 'chase':
                continue
            _rule, profit_pct = meta
            fills = buy['fills']
            total_qty = sum(f['qty'] for f in fills)
            if total_qty <= 0:
                continue
            total_value = sum(f['price'] * f['qty'] for f in fills)
            avg_fill_price = total_value / total_qty
            total_buy_fee = sum(f['fee'] for f in fills)

            maker_fee = self.fee_cache.maker(symbol)
            tick = get_tick_size(mid_price)
            tp_price = calculate_tp_price(
                avg_fill_price, total_buy_fee, total_qty,
                maker_fee, profit_pct, tick,
            )
            cl_ord_id = self._encode_chase_sell_cl_ord_id()
            actions.append({"action": "place_limit_sell", "symbol": symbol,
                            "qty": total_qty, "price": tp_price,
                            "cl_ord_id": cl_ord_id,
                            "parent_buy_order_id": buy_oid, "reason": "chase_tp"})
        return actions

    def _ladder_exits(self, symbol, mid_price, orders, now) -> List[dict]:
        # Aggregate across all filled ladder buys for the symbol that
        # haven't been sold yet. Compute weighted avg cost. If
        # mid_price ≥ avg * (1 + buy fee + taker fee + target), emit one market sell
        # for the total qty. target = swing_targets.target(): +2 % below the
        # 50-day SMA, else sized to the token's recent rallies.
        total_qty = 0.0
        total_value = 0.0
        total_buy_fee = 0.0
        n_cycles = 0
        for _buy_oid, cycle in orders.items():
            if cycle['symbol'] != symbol:
                continue
            buy = cycle['buy']
            if buy['status'] != 'filled':
                continue
            if cycle['sell'] is not None:
                continue
            meta = self._decode_buy_cl_ord_id(buy['cl_ord_id'])
            if meta is None or meta[0] != 'ladder':
                continue
            n_cycles += 1
            for f in buy['fills']:
                total_qty += f['qty']
                total_value += f['price'] * f['qty']
                total_buy_fee += f['fee']

        if total_qty <= 0:
            return []
        avg_cost = total_value / total_qty
        # The aggregate exit is a market sell, so the sell side pays the
        # taker rate (current volume tier from FeeCache).
        taker_fee = self.fee_cache.taker(symbol)
        profit_pct, target_rule, target_reason = self.swing_targets.target(symbol, now)
        # Target: cover round-trip fees + intended profit, against the
        # weighted average buy cost.
        target_price = calculate_tp_price(
            avg_cost, total_buy_fee, total_qty,
            taker_fee, profit_pct, 0,
        )
        if mid_price < target_price:
            return []
        cl_ord_id = self._encode_agg_sell_cl_ord_id()
        return [{"action": "place_market_sell", "symbol": symbol,
                 "qty": total_qty, "cl_ord_id": cl_ord_id,
                 "reason": "aggregate_target", "avg_cost": avg_cost,
                 "target_price": target_price, "cycles": n_cycles,
                 "buy_fees": total_buy_fee, "taker_fee_pct": taker_fee,
                 "profit_pct": profit_pct, "target_rule": target_rule,
                 "target_reason": target_reason}]

    @staticmethod
    def _encode_agg_sell_cl_ord_id() -> str:
        return f"a-{uuid.uuid4().hex[:8]}"

    # ── Helpers ─────────────────────────────────────────────────────────

    def _active_buys_by_rule(self, symbol, orders, rule):
        for buy_oid, cycle in orders.items():
            if cycle['symbol'] != symbol:
                continue
            buy = cycle['buy']
            if buy['status'] not in _ACTIVE_STATUSES:
                continue
            meta = self._decode_buy_cl_ord_id(buy['cl_ord_id'])
            if meta is None or meta[0] != rule:
                continue
            yield buy_oid, cycle

    def _violates_spacing(self, symbol, price, orders, spacing_pct) -> bool:
        if spacing_pct <= 0:
            return False
        threshold = spacing_pct / 100.0 - 1e-9
        for _buy_oid, cycle in orders.items():
            if cycle['symbol'] != symbol:
                continue
            buy = cycle['buy']
            # Compare against buy limit/avg fill and sell limit when present.
            # b_price == 0.0 means a market order (no limit); skip that comparison.
            b_price = buy['limit_price']
            if b_price and abs(price - b_price) / b_price < threshold:
                return True
            fills = buy['fills']
            if fills:
                qty = sum(f['qty'] for f in fills)
                if qty > 0:
                    avg = sum(f['price'] * f['qty'] for f in fills) / qty
                    if avg and abs(price - avg) / avg < threshold:
                        return True
            sell = cycle['sell']
            if sell is not None:
                s_price = sell['limit_price']
                if s_price and abs(price - s_price) / s_price < threshold:
                    return True
        return False
