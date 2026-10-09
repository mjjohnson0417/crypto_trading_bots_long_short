"""Trading runner — the central spine.

Imports the strategy and the tracker, uses them with the data layer to
run trading. Constructs OrderOperations, FeeCache, IndicatorCalculator,
TradeTracker, and HybridGridStrategy itself. Spawns two long-lived
background tasks:

  - executions consumer: drains data_manager.executions_queue, writes
    tracker updates.
  - tick loop: per symbol, computes indicators, asks strategy for
    actions, dispatches them to OrderOperations.

Plus a periodic balance refresh.

Constructor args are the things that exist outside the trading stack:
data_manager, exchange_connection, config, shutdown_event. The runner
builds the rest internally.
"""

import asyncio
from datetime import datetime, timezone

from exchange.fee_cache import FeeCache
from exchange.order_operations import OrderOperations
from indicators import IndicatorCalculator
from logging_config import get_logger
from persistence.trade_tracker import TradeTracker
from price_calculator import calculate_portfolio_value
from strategy.hybrid_grid import HybridGridStrategy


class TradingRunner:

    def __init__(self, *, data_manager, exchange_connection, config,
                 shutdown_event):
        self.logger = get_logger('TradingRunner')
        self.data_manager = data_manager
        self.exchange_connection = exchange_connection
        self.config = config
        self.shutdown_event = shutdown_event

        self.symbols = [s.upper() for s in config['symbols']]

        trading_cfg = config['trading']
        self.dry_run = bool(trading_cfg['dry_run'])
        self.warmup_seconds = float(trading_cfg['warmup_seconds'])
        self.max_book_age_seconds = float(trading_cfg['max_book_age_seconds'])
        self.tick_interval_seconds = float(trading_cfg['tick_interval_seconds'])
        self.balance_refresh_interval_seconds = float(
            trading_cfg['balance_refresh_interval_seconds']
        )
        self.order_send_timeout_seconds = float(
            trading_cfg['order_send_timeout_seconds']
        )

        # Built objects.
        self.order_ops = OrderOperations(
            exchange_connection, data_manager,
            dry_run=self.dry_run,
            send_timeout_seconds=self.order_send_timeout_seconds,
        )
        self.fee_cache = FeeCache(exchange_connection)
        self.tracker = TradeTracker()
        self.indicator_calculator = IndicatorCalculator(self.symbols)
        self.strategy = HybridGridStrategy(
            symbols=self.symbols,
            config=config,
            fee_cache=self.fee_cache,
            data_manager=data_manager,
        )

        # Runtime state.
        self.cached_balance = 0.0
        self.placement_paused = False
        self.start_time = datetime.now(timezone.utc)

        # Per-(order_id, side) cumulative-qty tracking so we can derive
        # individual fill rows from Kraken's running totals. Rebuilt
        # from persisted tracker fills at boot so a restart doesn't
        # re-apply already-recorded fills as fresh deltas.
        self._cum_qty_seen: dict = self._rebuild_cum_qty_seen()

    def _rebuild_cum_qty_seen(self) -> dict:
        """Sum the persisted fill rows for every buy and sell currently
        in the tracker to recover the cum_qty we last saw from Kraken.
        Aggregate sells (one sell.order_id covering many cycles) are
        summed across all covering cycles."""
        cum: dict = {}
        for _buy_oid, cycle in self.tracker.cycles.items():
            buy = cycle['buy']
            if buy['fills']:
                cum[(buy['order_id'], 'buy')] = sum(f['qty'] for f in buy['fills'])
            sell = cycle['sell']
            if sell is not None and sell['fills']:
                key = (sell['order_id'], 'sell')
                if key not in cum:
                    cum[key] = 0.0
                cum[key] += sum(f['qty'] for f in sell['fills'])
        return cum

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Bootstrap then run forever (until shutdown_event)."""
        self.logger.info("TradingRunner starting up")

        # Fee cache populate.
        await self.fee_cache.populate(self.symbols)

        # Seed indicators from REST klines (already in data_manager).
        for symbol in self.symbols:
            try:
                kline_df = self.data_manager.klines.get(symbol)
                self.indicator_calculator.seed_from_klines(symbol, kline_df)
            except Exception as e:
                self.logger.error(
                    f"Failed to seed indicators for {symbol}: {e}"
                )

        # Initial balance fetch.
        await self._refresh_balance()
        self.logger.info(
            f"TradingRunner ready — {len(self.symbols)} symbols, "
            f"balance=${self.cached_balance:.2f}"
        )

        # Spawn background tasks.
        tasks = [
            asyncio.create_task(self._executions_consumer(), name='executions_consumer'),
            asyncio.create_task(self._tick_loop(), name='tick_loop'),
            asyncio.create_task(self._balance_refresh_loop(), name='balance_refresh'),
        ]
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            self.logger.info("TradingRunner stopped")

    # ── Executions consumer ─────────────────────────────────────────────

    async def _executions_consumer(self) -> None:
        """Drain the executions queue and update the tracker."""
        queue = self.data_manager.executions_queue
        while not self.shutdown_event.is_set():
            try:
                msg = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                for order in msg['data']:
                    self._process_order_event(order)
            except Exception as e:
                self.logger.error(
                    f"Executions process error: {e}", exc_info=True,
                )

    def _process_order_event(self, order: dict) -> None:
        order_id = order['order_id']
        cl_ord_id = order['cl_ord_id']  # Kraken always echoes this for our orders.
        status = order['order_status']
        timestamp = order['timestamp']

        is_agg_sell = cl_ord_id.startswith('a-')
        is_chase_tp_sell = cl_ord_id.startswith('s-')

        if is_agg_sell:
            self._process_agg_sell_event(
                order, order_id, cl_ord_id, status, timestamp,
            )
            return

        if is_chase_tp_sell:
            # Chase is currently disabled in config (no new c- buys or s- TPs are created).
            # However, legacy chase cycles and their s- TP sells can still arrive via
            # adoption of orphan open sells or historical data. The reconciler defers
            # binding for s- sells; we must handle first-event binding here (like ladder agg).
            cycle_key = self._find_cycle_by_sell_order_id(order_id)
            if cycle_key is None:
                # Best-effort heuristic bind for legacy/orphan chase TP sells (s- prefix).
                # Match by symbol + filled qty (tolerance ~1%, same as StartupReconciler).
                symbol = order.get('symbol') or 'UNKNOWN'
                sell_amt = float(order.get('order_qty') or order.get('amount') or order.get('cum_qty') or 0.0)
                best = None
                best_delta = float('inf')
                best_buy_filled = 0.0
                for buy_oid, cycle in self.tracker.cycles.items():
                    if cycle.get('symbol') != symbol:
                        continue
                    buy = cycle.get('buy') or {}
                    cl = buy.get('cl_ord_id') or ''
                    if not cl.startswith('c-'):
                        continue
                    if cycle.get('sell') is not None:
                        continue
                    buy_filled = sum(float(f.get('qty', 0)) for f in (buy.get('fills') or []))
                    if buy_filled <= 0:
                        continue
                    delta = abs(sell_amt - buy_filled)
                    if delta < best_delta:
                        best_delta = delta
                        best = buy_oid
                        best_buy_filled = buy_filled
                if best is not None and (best_delta / max(sell_amt or best_buy_filled or 1, 1e-9) < 0.01 or best_delta < 1e-6):
                    cycle_key = best
                    try:
                        limit_price = float(order.get('limit_price') or order.get('price') or order.get('average') or 0.0)
                        self.tracker.add_sell(
                            parent_buy_order_id=cycle_key,
                            sell_order_id=order_id,
                            cl_ord_id=cl_ord_id,
                            ord_type='limit',
                            order_qty=sell_amt or best_buy_filled,
                            limit_price=limit_price,
                            timestamp=timestamp,
                            status=status,
                        )
                        self.logger.info(f"Bound legacy chase TP sell {order_id} to buy cycle {cycle_key} via qty heuristic (chase disabled)")
                    except Exception as link_e:
                        self.logger.error(f"Failed heuristic bind for chase sell {order_id}: {link_e}")
                else:
                    self.logger.warning(
                        f"Chase TP sell event {order_id} (symbol={symbol}) has no matching parent chase buy cycle in tracker. "
                        "Chase strategy is disabled; this is a legacy orphan sell. Event processed best-effort; fills may be lost if no cycle."
                    )
                    # Do not raise. Skip further processing for this event so the executions consumer stays healthy.
                    return
            side = 'sell'
        else:
            cycle_key = order_id
            side = 'buy'

        cycle = self.tracker.cycles.get(cycle_key)

        # First time seeing a buy → add it. Chase TP sells are stamped at
        # place time, so cycle.sell is already populated when their first
        # event arrives.
        if cycle is None and side == 'buy':
            # Some executions payloads (e.g. certain status updates like 'canceled',
            # or sparse updates) omit ord_type / limit_price / order_qty.
            # Use safe defaults; cl_ord_id prefix already told us it's one of our buys.
            ord_type = order.get('ord_type') or order.get('order_type') or 'limit'
            limit_price = float(order.get('limit_price') or 0.0)
            order_qty = float(order.get('order_qty') or 0.0)
            symbol = order.get('symbol') or 'UNKNOWN'
            self.tracker.add_buy(
                buy_order_id=order_id,
                symbol=symbol,
                cl_ord_id=cl_ord_id,
                ord_type=ord_type,
                order_qty=order_qty,
                limit_price=limit_price,
                timestamp=timestamp,
                status=status,
            )
        elif cycle is not None and cycle[side] is not None:
            if cycle[side]['status'] != status:
                self.tracker.update_status(cycle_key, side, status)

        # Immediate prune for zero-fill canceled buys (very common for stale
        # ladder rungs that get ratcheted or canceled by state/proximity logic).
        # Without this, canceled orders would accumulate in the active tracker
        # (and bloat known_bot_ids + order_cycles.json) until the next boot
        # or WS reconnect. We only prune pure zero-fill ones here; any with
        # fills + no sell are real positions (or were manually liquidated) and
        # are handled by the balance reconciliation pass.
        if side == 'buy' and status == 'canceled' and cycle_key:
            c = self.tracker.cycles.get(cycle_key)
            if c and c.get('sell') is None:
                buy = c.get('buy') or {}
                filled_qty = sum(float(f.get('qty', 0)) for f in (buy.get('fills') or []))
                if filled_qty < 1e-9:
                    c['_pruned_stale_zero_fill_cancel'] = True
                    c['_pruned_at'] = datetime.now(timezone.utc).isoformat()
                    self.tracker.archive(cycle_key)
                    self.logger.debug(f"Pruned zero-fill canceled buy {cycle_key} on cancel event")

        # Fill delta — only read fill-related fields if there's actually a fill.
        # Use .get() because some status-only update events may omit fill fields.
        cum_qty = float(order.get('cum_qty') or 0.0)
        key = (order_id, side)
        prev_cum = self._cum_qty_seen[key] if key in self._cum_qty_seen else 0.0
        if cum_qty > prev_cum:
            delta_qty = cum_qty - prev_cum
            self.tracker.append_fill(
                buy_order_id=cycle_key,
                side=side,
                price=float(order.get('avg_price') or 0.0),
                qty=delta_qty,
                fee=float(order.get('fee') or 0.0),
                timestamp=timestamp,
            )
            self._cum_qty_seen[key] = cum_qty

        # Archive on sell filled.
        if is_chase_tp_sell and status == 'filled':
            self.tracker.archive(cycle_key)

    def _process_agg_sell_event(self, order: dict, order_id: str,
                                cl_ord_id: str, status: str,
                                timestamp: str) -> None:
        """Aggregate ladder sell — covers many open ladder cycles for a
        symbol. On first event, attach the sell to every open ladder
        cycle for that symbol. On fill deltas, pro-rata distribute the
        delta to each covered cycle by buy-side filled qty. Archive
        each covered cycle when the aggregate sell reaches 'filled'."""
        symbol = order.get('symbol') or 'UNKNOWN'

        # Find covered cycles. After first event, they have sell.order_id == this order_id.
        covered = [
            (buy_oid, cycle)
            for buy_oid, cycle in self.tracker.cycles.items()
            if cycle['sell'] is not None
            and cycle['sell']['order_id'] == order_id
        ]
        # First event: bind to all open ladder cycles for the symbol.
        if not covered:
            covered = self._find_open_ladder_cycles(symbol)
            ord_type = order.get('ord_type') or order.get('order_type') or 'limit'
            limit_price = float(order.get('limit_price') or 0.0)
            for buy_oid, cycle in covered:
                buy_filled_qty = sum(
                    float(f['qty']) for f in cycle['buy']['fills']
                )
                self.tracker.add_sell(
                    parent_buy_order_id=buy_oid,
                    sell_order_id=order_id,
                    cl_ord_id=cl_ord_id,
                    ord_type=ord_type,
                    order_qty=buy_filled_qty,
                    limit_price=limit_price,
                    timestamp=timestamp,
                    status=status,
                )

        # Status updates on each covered cycle.
        for buy_oid, cycle in covered:
            if cycle['sell'] is not None and cycle['sell']['status'] != status:
                self.tracker.update_status(buy_oid, 'sell', status)

        # Pro-rata fill delta by each cycle's buy-side filled qty.
        cum_qty = float(order.get('cum_qty') or 0.0)
        key = (order_id, 'sell')
        prev_cum = self._cum_qty_seen[key] if key in self._cum_qty_seen else 0.0
        if cum_qty > prev_cum:
            delta_qty = cum_qty - prev_cum
            avg_price = float(order.get('avg_price') or 0.0)
            fee = float(order.get('fee') or 0.0)
            buy_qtys = [
                sum(float(f['qty']) for f in cycle['buy']['fills'])
                for _buy_oid, cycle in covered
            ]
            total = sum(buy_qtys)
            if total > 0:
                for (buy_oid, _cycle), bqty in zip(covered, buy_qtys):
                    share = bqty / total
                    self.tracker.append_fill(
                        buy_order_id=buy_oid,
                        side='sell',
                        price=avg_price,
                        qty=delta_qty * share,
                        fee=fee * share,
                        timestamp=timestamp,
                    )
            self._cum_qty_seen[key] = cum_qty

        # Archive each covered cycle when the aggregate sell is filled.
        if status == 'filled':
            for buy_oid, _cycle in covered:
                self.tracker.archive(buy_oid)

    def _find_open_ladder_cycles(self, symbol: str) -> list:
        """Return [(buy_order_id, cycle), ...] for cycles where the buy is
        a filled ladder buy with no sell attached yet."""
        out = []
        for buy_oid, cycle in self.tracker.cycles.items():
            if cycle['symbol'] != symbol:
                continue
            buy = cycle['buy']
            if buy['status'] != 'filled':
                continue
            if cycle['sell'] is not None:
                continue
            if not buy['cl_ord_id'].startswith('l-'):
                continue
            out.append((buy_oid, cycle))
        return out

    def _find_cycle_by_sell_order_id(self, sell_order_id: str):
        """Return the buy_order_id (cycle key) whose attached sell matches
        the given sell order_id, or None if no cycle is linked."""
        for buy_oid, cycle in self.tracker.cycles.items():
            sell = cycle.get('sell')
            if sell and sell.get('order_id') == sell_order_id:
                return buy_oid
        return None

    # ── Tick loop ───────────────────────────────────────────────────────

    async def _tick_loop(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                now = datetime.now(timezone.utc)
                if not self.placement_paused:
                    if (now - self.start_time).total_seconds() >= self.warmup_seconds:
                        for symbol in self.symbols:
                            try:
                                await self._evaluate_symbol(symbol, now)
                            except Exception as e:
                                self.logger.error(
                                    f"{symbol} evaluate error: {e}",
                                    exc_info=True,
                                )
            except Exception as e:
                self.logger.error(f"Tick loop error: {e}", exc_info=True)
            await asyncio.sleep(self.tick_interval_seconds)

    async def _evaluate_symbol(self, symbol: str, now: datetime) -> None:
        ticker_df = self.data_manager.ticker.get(symbol)
        if ticker_df.empty:
            return
        latest = ticker_df.iloc[-1]
        bid = float(latest['bid'])
        ask = float(latest['ask'])
        if bid <= 0 or ask <= 0:
            return
        mid_price = (bid + ask) / 2.0

        # Book staleness gate.
        book_df = self.data_manager.book.get(symbol)
        if not book_df.empty:
            last_book_ts = book_df['timestamp'].max()
            book_age = (now - last_book_ts.to_pydatetime()).total_seconds()
            if book_age > self.max_book_age_seconds:
                return

        trades_df = self.data_manager.trades.get(symbol)
        ticker_data = {'best_bid': bid, 'best_ask': ask}
        indicators = self.indicator_calculator.calculate(
            symbol, trades_df, ticker_data,
        )

        actions = self.strategy.evaluate(
            symbol=symbol,
            indicators=indicators,
            mid_price=mid_price,
            orders=self.tracker.cycles,
            cached_balance=self.cached_balance,
            now=now,
        )
        for action in actions:
            try:
                await self._dispatch_action(action)
            except Exception as e:
                self.logger.error(
                    f"Action dispatch error {action}: {e}", exc_info=True,
                )
                # On sell insufficient, immediately force a balance-based position reconciliation.
                # This corrects the tracker (archives phantom cycles whose tokens are gone per exchange)
                # so subsequent ticks do not keep re-emitting the same failing sell.
                kind = action.get('action')
                if kind in ('place_market_sell', 'place_limit_sell'):
                    err = str(e).lower()
                    if 'insufficient' in err or 'funds' in err:
                        sym = action.get('symbol')
                        self.logger.warning(f"{sym} sell hit insufficient funds — forcing tracker <-> exchange balance reconciliation")
                        try:
                            from execution.reconciliation import reconcile_open_positions_to_exchange_balances
                            await reconcile_open_positions_to_exchange_balances(
                                self.tracker, self.order_ops, self.symbols, self.logger
                            )
                        except Exception as recon_e:
                            self.logger.error(f"Forced position/balance recon after sell error failed: {recon_e}")

    async def _dispatch_action(self, action: dict) -> None:
        kind = action.get('action')
        symbol = action.get('symbol')
        if kind == 'place_market_buy':
            await self.order_ops.place_market_buy(
                symbol, action['qty'], cl_ord_id=action['cl_ord_id'],
            )
        elif kind == 'place_limit_buy':
            await self.order_ops.place_limit_buy(
                symbol, action['qty'], action['price'],
                cl_ord_id=action['cl_ord_id'],
            )
        elif kind == 'place_limit_sell':
            # Last-second defense: clip sell qty to actual free base balance.
            # Tracker should already reflect exchange, but this prevents bad orders
            # even if a divergence appears between reconcile points.
            try:
                base = symbol.split('/')[0].upper()
                free = await self.order_ops.get_free_balance(base)
                orig = float(action.get('qty', 0))
                if orig > free + 1e-12:
                    action = dict(action)
                    action['qty'] = max(0.0, free)
                    self.logger.warning(
                        f"{symbol} sell preflight: clipping qty from {orig:.8f} to {action['qty']:.8f} "
                        f"(exchange free {base}={free:.8f})"
                    )
                if action['qty'] <= 1e-12:
                    self.logger.info(f"Skipping place_limit_sell for {symbol}: no free {base} on exchange")
                    return
            except Exception as bal_e:
                self.logger.error(f"Free balance preflight check failed for {symbol} sell: {bal_e}")

            sell_order_id = await self.order_ops.place_limit_sell(
                symbol, action['qty'], action['price'],
                cl_ord_id=action['cl_ord_id'],
            )
            # Chase TP sells: link sell→parent buy in the tracker the
            # moment Kraken returns the sell order_id. Required because
            # the cl_ord_id is just 's-<rand>' — the parent linkage no
            # longer lives in the id. In dry_run, place_* returns None,
            # so skip the stamp.
            parent_buy_oid = action.get('parent_buy_order_id')
            if sell_order_id and parent_buy_oid:
                self.tracker.add_sell(
                    parent_buy_order_id=parent_buy_oid,
                    sell_order_id=sell_order_id,
                    cl_ord_id=action['cl_ord_id'],
                    ord_type='limit',
                    order_qty=action['qty'],
                    limit_price=action['price'],
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    status='pending_new',
                )
        elif kind == 'place_market_sell':
            # Last-second defense: clip sell qty to actual free base balance.
            try:
                base = symbol.split('/')[0].upper()
                free = await self.order_ops.get_free_balance(base)
                orig = float(action.get('qty', 0))
                if orig > free + 1e-12:
                    action = dict(action)
                    action['qty'] = max(0.0, free)
                    self.logger.warning(
                        f"{symbol} sell preflight: clipping qty from {orig:.8f} to {action['qty']:.8f} "
                        f"(exchange free {base}={free:.8f})"
                    )
                if action['qty'] <= 1e-12:
                    self.logger.info(f"Skipping place_market_sell for {symbol}: no free {base} on exchange")
                    return
            except Exception as bal_e:
                self.logger.error(f"Free balance preflight check failed for {symbol} sell: {bal_e}")

            await self.order_ops.place_market_sell(
                symbol, action['qty'], cl_ord_id=action['cl_ord_id'],
            )
        elif kind == 'cancel_order':
            await self.order_ops.cancel_order(symbol, action['order_id'])
        else:
            self.logger.warning(f"Unknown action kind: {kind!r} in {action}")

    # ── Balance refresh ─────────────────────────────────────────────────

    async def _balance_refresh_loop(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                await asyncio.sleep(self.balance_refresh_interval_seconds)
            except asyncio.CancelledError:
                return
            if self.shutdown_event.is_set():
                return
            await self._refresh_balance()

    async def _refresh_balance(self) -> None:
        try:
            balances = await self.order_ops.fetch_all_balances()
            usd_prices = await self.order_ops.fetch_all_usd_prices()
            self.cached_balance = calculate_portfolio_value(balances, usd_prices)
        except Exception as e:
            self.logger.error(f"Balance refresh failed: {e}")
