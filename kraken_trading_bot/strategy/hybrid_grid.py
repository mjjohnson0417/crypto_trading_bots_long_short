"""HybridGridStrategy — single-class strategy module.

The runner constructs once with `symbols`, `config`, `fee_cache`,
`data_manager`. Then calls `evaluate(symbol, indicators, mid_price,
orders, cached_balance, now)` per tick. Returns a list of plain-dict
actions for the runner to dispatch against `order_ops`.

Action dicts:
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

import uuid
from typing import List, Optional

from logging_config import get_logger
from price_calculator import calculate_tp_price, get_tick_size
from state_machine.regime_classifier import (
    RegimeClassifier,
    STATE_STRONG_UPTREND,
    STATE_STRONG_DOWNTREND,
    STATE_UPTREND,
    STATE_DOWNTREND,
)


_ACTIVE_STATUSES = ('pending_new', 'new', 'partially_filled')


class _ChaseState:
    def __init__(self):
        self.strong_uptrend_start_price: Optional[float] = None
        self.last_confirmed_state: Optional[str] = None


class _LadderState:
    def __init__(self):
        self.anchor_price: Optional[float] = None
        self.spacing_distance: Optional[float] = None
        self.is_anchored: bool = False
        self.last_adjust_price: Optional[float] = None
        self.last_confirmed_state: Optional[str] = None


class HybridGridStrategy:

    def __init__(self, symbols, config, fee_cache, data_manager):
        self.logger = get_logger('HybridGridStrategy')
        self.symbols = [s.upper() for s in symbols]
        self.fee_cache = fee_cache
        self.data_manager = data_manager

        hcfg = config['hybrid_grid']
        h_ladder = hcfg['ladder']
        h_chase = hcfg['chase']
        h_ladder_defaults = hcfg['ladder_defaults']

        self.hysteresis_seconds = int(hcfg['hysteresis_seconds'])
        self.ladder_rung_count = int(h_ladder['rung_count'])
        self.ladder_spacing_pct = float(h_ladder['spacing_pct'])
        self.ladder_proximity_pct = float(h_ladder['proximity_pct'])
        self.chase_enabled = bool(h_chase['enabled'])
        self.chase_position_size_pct = float(h_chase['position_size_pct'])
        self.chase_profit_pct = float(h_chase['profit_pct'])
        self.chase_spacing_pct = float(h_chase['spacing_pct'])
        self.ladder_position_size_pct = float(h_ladder_defaults['position_size_pct'])
        self.ladder_profit_pct = float(h_ladder_defaults['profit_pct'])

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
        self.ladder = {s: _LadderState() for s in self.symbols}

    # ── Public entry point ───────────────────────────────────────────────

    def evaluate(self, symbol, indicators, mid_price, orders, cached_balance, now) -> List[dict]:
        actions: List[dict] = []
        state = self.classifiers[symbol].update(indicators, now)

        actions.extend(self._handle_state_transitions(symbol, state, mid_price, orders))

        if state == STATE_STRONG_UPTREND:
            if self.chase_enabled:
                actions.extend(self._chase_decisions(symbol, mid_price, orders, cached_balance))
        else:
            actions.extend(
                self._ladder_decisions(symbol, mid_price, orders, cached_balance)
            )

        actions.extend(self._exit_decisions(symbol, mid_price, orders))
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

    # ── State-transition handling ───────────────────────────────────────

    def _handle_state_transitions(self, symbol, state, mid_price, orders) -> List[dict]:
        actions: List[dict] = []
        chase = self.chase[symbol]
        ladder = self.ladder[symbol]

        # Chase floor capture/release.
        if state == STATE_STRONG_UPTREND and chase.last_confirmed_state != STATE_STRONG_UPTREND:
            if mid_price > 0:
                chase.strong_uptrend_start_price = float(mid_price)
        elif chase.last_confirmed_state == STATE_STRONG_UPTREND and state != STATE_STRONG_UPTREND:
            chase.strong_uptrend_start_price = None
            # Cancel any open chase buys on exit from STRONG_UPTREND.
            for buy_oid, _cycle in self._active_buys_by_rule(symbol, orders, 'chase'):
                actions.append({"action": "cancel_order",
                                "symbol": symbol, "order_id": buy_oid})

        chase.last_confirmed_state = state

        # Ladder transition rules: cancel-all on entering STRONG_UPTREND,
        # entering Down regime (then rebuild anchored), exiting Down regime.
        prev = ladder.last_confirmed_state
        if state == STATE_STRONG_UPTREND:
            for buy_oid, _cycle in self._active_buys_by_rule(symbol, orders, 'ladder'):
                actions.append({"action": "cancel_order",
                                "symbol": symbol, "order_id": buy_oid})
            ladder.anchor_price = None
            ladder.spacing_distance = None
            ladder.is_anchored = False
            ladder.last_adjust_price = None
        elif (state in (STATE_DOWNTREND, STATE_STRONG_DOWNTREND)
              and prev not in (STATE_DOWNTREND, STATE_STRONG_DOWNTREND)):
            for buy_oid, _cycle in self._active_buys_by_rule(symbol, orders, 'ladder'):
                actions.append({"action": "cancel_order",
                                "symbol": symbol, "order_id": buy_oid})
            ladder.anchor_price = float(mid_price)
            ladder.is_anchored = True
            ladder.spacing_distance = float(mid_price) * (self.ladder_spacing_pct / 100.0)
            ladder.last_adjust_price = None
        elif (prev in (STATE_DOWNTREND, STATE_STRONG_DOWNTREND)
              and state not in (STATE_DOWNTREND, STATE_STRONG_DOWNTREND)):
            for buy_oid, _cycle in self._active_buys_by_rule(symbol, orders, 'ladder'):
                actions.append({"action": "cancel_order",
                                "symbol": symbol, "order_id": buy_oid})
            ladder.anchor_price = None
            ladder.spacing_distance = None
            ladder.is_anchored = False
            ladder.last_adjust_price = None

        ladder.last_confirmed_state = state
        return actions

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
                 "qty": qty, "cl_ord_id": cl_ord_id}]

    # ── Ladder decisions ────────────────────────────────────────────────

    def _ladder_decisions(self, symbol, mid_price, orders, cached_balance) -> List[dict]:
        actions: List[dict] = []
        ladder = self.ladder[symbol]

        # Active ladder buy prices below mid.
        active_ladder_buys = [
            (buy_oid, cycle)
            for buy_oid, cycle in self._active_buys_by_rule(symbol, orders, 'ladder')
        ]
        existing_buys = sorted(
            (cycle['buy']['limit_price'] for _, cycle in active_ladder_buys),
        )

        # Proximity gate.
        if (ladder.last_adjust_price is not None
                and ladder.last_adjust_price > 0
                and len(existing_buys) >= self.ladder_rung_count):
            move = abs(mid_price - ladder.last_adjust_price) / ladder.last_adjust_price * 100.0
            if move < self.ladder_proximity_pct:
                return []

        # Desired rung target prices.
        desired = self._desired_rung_prices(symbol, mid_price)
        below = [p for p in existing_buys if p < mid_price]
        forbidden = set(below)
        threshold = self.ladder_spacing_pct / 100.0 - 1e-9
        candidates = [
            p for p in desired
            if p < mid_price
            and not any(abs(p - f) / max(f, 1e-9) < threshold for f in forbidden)
        ]
        candidates.sort(reverse=True)  # top-down

        # Direction (for ratcheting).
        last = ladder.last_adjust_price
        if last is None or last <= 0:
            direction = "flat"
        elif mid_price > last:
            direction = "up"
        elif mid_price < last:
            direction = "down"
        else:
            direction = "flat"

        target = self.ladder_rung_count
        active = len(below)

        # Excess: cancel lowest first.
        cancels_prices: List[float] = []
        if active > target:
            cancels_prices.extend(below[:active - target])
            active -= len(cancels_prices)

        missing = max(0, target - active)
        places_prices: List[float] = []

        if direction == "up":
            places_prices.extend(candidates[:missing])
            if missing == 0 and candidates and below:
                new_top = candidates[0]
                if new_top > max(below):
                    places_prices.append(new_top)
                    cancels_prices.append(below[0])
        else:
            bottom_up = sorted(candidates)
            places_prices.extend(bottom_up[:missing])

        places_prices = [p for p in places_prices if p < mid_price]
        places_prices = sorted(set(places_prices), reverse=True)
        cancels_prices = sorted(set(cancels_prices))

        ladder.last_adjust_price = float(mid_price)

        # Resolve cancel prices to order_ids.
        price_to_oid = {cycle['buy']['limit_price']: buy_oid
                        for buy_oid, cycle in active_ladder_buys}
        for price in cancels_prices:
            oid = price_to_oid.get(price)
            if oid:
                actions.append({"action": "cancel_order",
                                "symbol": symbol, "order_id": oid})

        # Emit places. Sizing per rung.
        for price in places_prices:
            size_usd = cached_balance * self.ladder_position_size_pct / 100.0
            if size_usd <= 0:
                break
            qty = size_usd / price
            cl_ord_id = self._encode_buy_cl_ord_id('ladder', self.ladder_profit_pct)
            actions.append({"action": "place_limit_buy", "symbol": symbol,
                            "qty": qty, "price": price, "cl_ord_id": cl_ord_id})

        return actions

    def _desired_rung_prices(self, symbol, mid_price) -> List[float]:
        ladder = self.ladder[symbol]
        if mid_price <= 0:
            return []
        out: List[float] = []
        if ladder.is_anchored and ladder.spacing_distance:
            dist = ladder.spacing_distance
            for i in range(1, self.ladder_rung_count + 1):
                p = mid_price - i * dist
                if p < mid_price:
                    out.append(p)
        else:
            spacing = self.ladder_spacing_pct / 100.0
            for i in range(1, self.ladder_rung_count + 1):
                p = mid_price * (1 - i * spacing)
                if p < mid_price:
                    out.append(p)
        return out

    # ── Exit decisions ──────────────────────────────────────────────────

    def _exit_decisions(self, symbol, mid_price, orders) -> List[dict]:
        """Two paths:
           - Chase: each filled chase buy with no sell → its own limit TP.
           - Ladder: aggregate avg-cost across all filled ladder buys
             with no sell. If mid_price ≥ target, ONE market sell for
             the total qty.
        """
        actions: List[dict] = []
        actions.extend(self._chase_exits(symbol, mid_price, orders))
        actions.extend(self._ladder_exits(symbol, mid_price, orders))
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
                            "parent_buy_order_id": buy_oid})
        return actions

    def _ladder_exits(self, symbol, mid_price, orders) -> List[dict]:
        # Aggregate across all filled ladder buys for the symbol that
        # haven't been sold yet. Compute weighted avg cost. If
        # mid_price ≥ avg * (1 + fees + profit_pct), emit one market sell
        # for the total qty.
        total_qty = 0.0
        total_value = 0.0
        total_buy_fee = 0.0
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
            for f in buy['fills']:
                total_qty += f['qty']
                total_value += f['price'] * f['qty']
                total_buy_fee += f['fee']

        if total_qty <= 0:
            return []
        avg_cost = total_value / total_qty
        maker_fee = self.fee_cache.maker(symbol)
        # Target: cover round-trip fees + intended profit, against the
        # weighted average buy cost.
        target_price = calculate_tp_price(
            avg_cost, total_buy_fee, total_qty,
            maker_fee, self.ladder_profit_pct, 0,
        )
        if mid_price < target_price:
            return []
        cl_ord_id = self._encode_agg_sell_cl_ord_id()
        return [{"action": "place_market_sell", "symbol": symbol,
                 "qty": total_qty, "cl_ord_id": cl_ord_id}]

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
