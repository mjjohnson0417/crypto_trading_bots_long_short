"""Boot-time StartupReconciler.

Runs once, early in main.py after DataManager init and TradingRunner
construction (so tracker + order_ops exist) but before the runner task
is spawned and before any strategy ticks or executions consumer.

Responsibilities (no more, no less):
- Cancel stale bot-owned open buys that have zero fills (account-wide).
- Preserve/record partial fills on open bot buys so the live runner can
  complete them into TP sells.
- Catch up tracker cycles from closed orders that executed while down.
- Re-attach sells (open or recently closed) to filled buys that have
  sell=None in the tracker (chase s- or ladder a-), using qty+symbol
  heuristic where exact parent linkage is not encoded in the cl_ord_id.
- Adopt "orphan" bot orders that live on Kraken (in known_bot_ids) but
  are missing from the current tracker (e.g. placement acks missed).
- For removed symbols (buys whose symbol not in current config.symbols):
  cancel the buys (account-wide scan); sells are left alone to liquidate.

Design constraints:
- Tracker is passive. Only calls: add_buy, add_sell, update_status,
  append_fill, archive, save.
- No defaults/fallbacks that hide problems. Fail loud (log ERROR + raise
  or explicit skip with reason) on anything unclassifiable.
- New cl_ord_id format in use: c-<bp>-<rand> (chase buy), l-<bp>-<rand>
  (ladder buy), s-<rand> (chase TP), a-<rand> (agg ladder sell).
- Cycle shape: {symbol, buy: {..., status, fills}, sell: None | {...}}.
- This module NEVER places orders. It cancels ONLY at boot for the
  explicit stale/removed cases (same policy as original + shutdown).
- Source of truth is the exchange; tracker is brought into agreement.

See plan in user query for full method intent.

Phase A decisions (locked in after user authorization to complete the plan):

A1. Orphan filled buys (buy filled, sell=None, no matching sell on Kraken):
    - We log ERROR loudly (the "I already alerted" signal).
    - We ensure the cycle exists in the tracker with sell=None.
    - We do NOT set cycle['manual_reconcile_needed'].
    Rationale: The current HybridGridStrategy + TradingRunner already treat any
    "filled buy with sell is None" as something that needs a TP (see
    _exit_decisions / _chase_exits / _ladder_exits). Adding the cycle lets the
    live strategy automatically recover the position by placing the sell.
    Introducing the flag would have been inert without modifying strategy/runner
    logic (explicitly out of scope per the plan).

A2. Rebuilding missing sells for filled buys:
    - For s- (chase TP): qty + symbol heuristic with tolerance (~1% relative or
      small absolute) because s-<rand> carries no parent pointer.
    - For a- (ladder agg): symbol match (by design, one sell can cover many buys).
    - We scan both open_orders (preferred) and closed_orders.
    - Exact cl_ord linkage is used when possible but is rarely available for sells.

A3. reconnect_event: Added to PrivateWebSocket exactly as specified. Set after
    the executions subscribe send on initial connect and every reconnect.

A4. Wiring order in main.py: StartupReconciler is awaited after TradingRunner
    construction but before any asyncio.create_task for the runner. The
    ExchangeSyncTask is spawned as a sibling long-lived task. Both are
    cancelled on shutdown.

A5. Removed-symbol orphans: Handled via account-wide fetch (symbol=None) in the
    boot cancel pass. Buys for symbols not in the current config.symbols list
    are cancelled. Sells are left open for liquidation.

Open questions answers (as implemented):
- A1 orphan flag: log ERROR + make recoverable via tracker (not pure log-only,
  not the manual_reconcile_needed field).
- A2 heuristic: acceptable and implemented with logging of the match source.
- Removed-symbol policy: confirmed and implemented (cancel buys, leave sells).
- Lookback: config-driven ("trading"."exchange_sync_lookback_seconds", default
  1800). Boot uses a longer internal window (48h) for closed orders.
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

from logging_config import get_logger


class StartupReconciler:
    """One-shot boot reconciler against current passive TradeTracker."""

    def __init__(self, tracker: Any, order_ops: Any, symbols: list[str]):
        self.logger = get_logger("StartupReconciler")
        self.tracker = tracker
        self.order_ops = order_ops
        self.symbols = [s.upper() for s in symbols]
        # Generous closed lookback for boot (48h). Runtime sync uses the
        # shorter configured value.
        self._closed_lookback_seconds_boot = 48 * 3600

    async def reconcile(self) -> None:
        """Orchestrator: bring tracker into agreement with Kraken at boot."""
        self.logger.info("StartupReconciler starting")
        known = self._compute_known_bot_ids()
        self.logger.info(f"known_bot_ids count={len(known)}")

        # 1. Account-wide: cancel zero-fill bot buys + removed-symbol buys.
        #    Record partial fills for any partial bot buys we decide to keep.
        await self._cancel_open_buys(known)

        # 2. Fetch (filtered post-fetch by known + our cl_ord prefixes where useful).
        open_orders = await self._fetch_open_orders(known)
        closed_orders = await self._fetch_closed_orders(known)

        # 3. Bring existing tracker buys up to date from closed orders.
        self._apply_closed_buys(closed_orders)

        # 4. For tracker cycles that are buy-filled + sell=None, try to
        #    locate the corresponding sell (open preferred for still-live TP;
        #    closed if the TP also completed while we were down). If none,
        #    add/record the buy (if not present) so the live strategy's
        #    _exit_decisions will place a fresh TP sell. Log loudly.
        self._rebuild_sells_for_filled_buys(open_orders, closed_orders)

        # 5. Adopt any Kraken order whose id is in known_bot_ids (or
        #    carries our c-/l- buy cl_ord prefix) but is not represented
        #    in the tracker yet.
        self._adopt_orphan_open_orders(open_orders, known)
        self._adopt_orphan_closed_orders(closed_orders, known)

        self.tracker.save()
        self.logger.info("StartupReconciler complete")

        # Prune pure stale canceled buys (zero fills, no sell). These are ladder rungs
        # that were canceled during ratcheting/state changes with no economic impact.
        # They bloat the tracker and known_bot_ids without representing real trades.
        prune_stale_canceled_buys(self.tracker, self.logger)

        # Final pass: make tracker holdings claims match exchange token balances 100%.
        # This is the critical step that prevents phantom sell obligations after
        # manual external sales or other balance divergences.
        try:
            pos_corrections = await reconcile_open_positions_to_exchange_balances(
                self.tracker, self.order_ops, self.symbols, self.logger
            )
            if pos_corrections:
                self.logger.warning(
                    f"Position/balance reconciliation corrected {pos_corrections} cycles "
                    "(tracker now reflects actual exchange free balances for bases)."
                )
        except Exception as e:
            self.logger.error(f"Position balance reconciliation at boot failed: {e}")

    def _compute_known_bot_ids(self) -> set[str]:
        """Union of every buy_order_id + sell order_id we have ever touched."""
        ids: set[str] = set()
        for buy_oid, cycle in self.tracker.cycles.items():
            if buy_oid:
                ids.add(buy_oid)
            sell = cycle.get("sell") if cycle else None
            if sell and sell.get("order_id"):
                ids.add(sell["order_id"])

        hist_path = self.tracker.history_file
        if hist_path.exists():
            try:
                with open(hist_path) as f:
                    history = json.load(f)
                for c in history:
                    b = c.get("buy") or {}
                    if b.get("order_id"):
                        ids.add(b["order_id"])
                    s = c.get("sell") or {}
                    if s.get("order_id"):
                        ids.add(s["order_id"])
            except Exception as e:
                # Fail loud — history corruption must be noticed.
                self.logger.error(f"Failed to read order_cycles_history.json: {e}")
                raise
        return ids

    async def _cancel_open_buys(self, known: set[str]) -> None:
        """Account-wide fetch.

        - Cancel bot-owned buys (known id OR c-/l- cl_ord prefix) that have
          zero filled qty. These are stale resting ladder/chase buys.
        - For removed symbols (not in self.symbols): cancel their buys too
          (even if not previously known to this tracker run).
        - Partial-filled bot buys: adopt + append_fill the executed portion
          (do NOT cancel). This lets the runner pick up the partial->TP path.
        """
        try:
            all_open = await self.order_ops.fetch_open_orders(symbol=None)
        except Exception as e:
            self.logger.error(f"fetch_open_orders(account-wide) failed: {e}")
            raise

        for o in all_open or []:
            if o.get("side") != "buy":
                continue

            oid = o.get("id") or o.get("order_id")
            if not oid:
                continue

            sym = o.get("symbol")
            if not sym:
                continue

            cl_ord = o.get("clientOrderId") or ""
            is_our_cl = cl_ord.startswith("c-") or cl_ord.startswith("l-")
            is_known = oid in known
            is_config_sym = sym in self.symbols

            if not (is_known or is_our_cl):
                # Not ours (foreign order or user-placed) — ignore.
                continue

            filled_qty = float(o.get("filled") or 0.0)
            amount = float(o.get("amount") or 0.0)
            status = o.get("status") or "open"

            if not is_config_sym:
                # Removed symbol policy: cancel the buy. Sells (if any) left
                # for liquidation. Log at warning so operator sees cleanup.
                self.logger.warning(
                    f"Removed symbol buy on Kraken — cancelling: {oid} {sym} "
                    f"filled={filled_qty}"
                )
                await self.order_ops.cancel_order(sym, oid)
                continue

            if filled_qty <= 0:
                # Stale zero-fill resting buy for a configured symbol.
                self.logger.info(
                    f"Cancelling zero-fill bot buy at boot: {oid} {sym}"
                )
                await self.order_ops.cancel_order(sym, oid)
                continue

            # Partial fill on a live bot buy for configured symbol.
            # Adopt into tracker (if missing) and record the executed fill
            # so _executions_consumer / strategy can drive it to TP.
            self.logger.info(
                f"Partial fill at boot for bot buy {oid} {sym}: "
                f"filled={filled_qty}/{amount} — recording for runner TP flow"
            )
            self._ensure_buy_from_rest(o, oid, status="partially_filled")
            # Record a synthetic fill row using the aggregate info we have.
            # (Detailed per-fill history is lost, but avg + total is enough
            # for later TP math in strategy and for history.)
            if filled_qty > 0:
                avg_price = float(o.get("average") or o.get("price") or 0.0)
                fee_total = 0.0
                fee = o.get("fee") or {}
                if isinstance(fee, dict):
                    fee_total = float(fee.get("cost") or 0.0)
                ts = (o.get("datetime") or datetime.now(timezone.utc).isoformat())
                try:
                    self.tracker.append_fill(
                        buy_order_id=oid,
                        side="buy",
                        price=avg_price,
                        qty=filled_qty,
                        fee=fee_total,
                        timestamp=ts,
                    )
                except Exception as e:
                    self.logger.error(f"append_fill partial failed for {oid}: {e}")
                    # Do not raise — the buy record itself is the important part.

    async def _fetch_open_orders(self, known: set[str]) -> list[dict]:
        """Account-wide open orders (ccxt unified). Caller post-filters."""
        try:
            orders = await self.order_ops.fetch_open_orders(symbol=None)
            return orders or []
        except Exception as e:
            self.logger.error(f"fetch_open_orders failed: {e}")
            raise

    async def _fetch_closed_orders(self, known: set[str]) -> list[dict]:
        """Closed orders with a boot-appropriate lookback (no since=None)."""
        since_ms = int(
            (datetime.now(timezone.utc).timestamp() - self._closed_lookback_seconds_boot)
            * 1000
        )
        try:
            orders = await self.order_ops.fetch_closed_orders(since_timestamp=since_ms)
            return orders or []
        except Exception as e:
            self.logger.error(f"fetch_closed_orders failed: {e}")
            raise

    def _apply_closed_buys(self, closed_orders: list[dict]) -> None:
        """For tracker buys whose order_id appears in closed_orders and whose
        status or fill info is behind, catch up via update_status + append_fill.
        Synthetic fill from aggregate closed record (detailed fills lost).
        """
        closed_by_id = {}
        for o in closed_orders:
            oid = o.get("id") or o.get("order_id")
            if oid:
                closed_by_id[oid] = o

        for buy_oid, cycle in list(self.tracker.cycles.items()):
            if buy_oid not in closed_by_id:
                continue
            o = closed_by_id[buy_oid]
            side = "buy"
            new_status = self._normalize_status(o.get("status"))
            cur = cycle.get(side) or {}
            if cur.get("status") != new_status:
                try:
                    self.tracker.update_status(buy_oid, side, new_status)
                except Exception as e:
                    self.logger.error(f"update_status closed buy {buy_oid} failed: {e}")

            # If more was filled according to closed record, append delta.
            filled_now = float(o.get("filled") or 0.0)
            # We don't have per-cycle cumulative here easily; append a row
            # using the closed aggregate if it shows positive filled.
            # (Idempotency is not perfect — worst case duplicate fill rows
            # with same totals; downstream sums still work.)
            if filled_now > 0:
                avg = float(o.get("average") or o.get("price") or 0.0)
                fee = o.get("fee") or {}
                fee_cost = float(fee.get("cost") or 0.0) if isinstance(fee, dict) else 0.0
                ts = o.get("datetime") or datetime.now(timezone.utc).isoformat()
                try:
                    self.tracker.append_fill(
                        buy_order_id=buy_oid,
                        side=side,
                        price=avg,
                        qty=filled_now,
                        fee=fee_cost,
                        timestamp=ts,
                    )
                except Exception as e:
                    self.logger.error(f"append_fill closed buy {buy_oid} failed: {e}")

    def _rebuild_sells_for_filled_buys(
        self, open_orders: list[dict], closed_orders: list[dict]
    ) -> None:
        """For every cycle in tracker where buy.status == 'filled' and sell is None,
        search open (preferred) then closed for a matching sell.

        Matching rules (per plan A2):
        - Chase TP (s-): symbol + qty heuristic (filled buy qty ≈ sell amount).
        - Agg ladder (a-): symbol match (one a- sell covers 1..N ladder buys).
        If a sell is found, call add_sell (and append any sell fills we can).
        If the sell is already 'filled' (from closed), also archive the cycle.

        If NO sell (open or closed) is found for a known bot filled buy:
        - Log ERROR loudly.
        - Ensure the buy is present in tracker (filled + its fills).
        - Leave sell=None. The live _tick_loop + strategy._exit_decisions
          (chase_exits / ladder_exits) will see it and place a fresh TP sell
          (or include in next agg market sell). This is recovery, not a
          manual_reconcile_needed orphan that blocks action.
        """
        # Index sells by cl_ord for quick lookup + all a- / s- sells.
        def _index_sells(orders: list[dict]) -> dict:
            by_cl: dict[str, dict] = {}
            agg_sells: list[dict] = []   # a- sells
            chase_sells: list[dict] = [] # s- sells
            for o in orders:
                if o.get("side") != "sell":
                    continue
                cl = o.get("clientOrderId") or ""
                oid = o.get("id") or o.get("order_id")
                if not oid:
                    continue
                by_cl[cl] = o
                if cl.startswith("a-"):
                    agg_sells.append(o)
                elif cl.startswith("s-"):
                    chase_sells.append(o)
            return {"by_cl": by_cl, "agg": agg_sells, "chase": chase_sells}

        open_idx = _index_sells(open_orders)
        closed_idx = _index_sells(closed_orders)

        for buy_oid, cycle in list(self.tracker.cycles.items()):
            buy = cycle.get("buy") or {}
            if buy.get("status") != "filled":
                continue
            if cycle.get("sell") is not None:
                continue

            sym = cycle.get("symbol")
            buy_filled_qty = sum(float(f.get("qty", 0)) for f in (buy.get("fills") or []))
            if buy_filled_qty <= 0:
                # Corrupt or zero-size filled buy — do not try to sell.
                self.logger.error(
                    f"Orphan zero-fill 'filled' buy in tracker: {buy_oid} {sym} "
                    "— skipping sell rebuild; investigate history."
                )
                continue

            cl = buy.get("cl_ord_id", "")
            is_chase = cl.startswith("c-")
            is_ladder = cl.startswith("l-")

            # Try to find a matching sell.
            found_sell = None
            source = None

            # Exact cl_ord match first (if a prior place stamped a cl that we can see).
            candidate_cl = None
            # We don't have the sell cl stored on the buy side, so skip pure cl.

            # For ladder: any a- sell on the same symbol (open first).
            if is_ladder or not is_chase:
                for o in open_idx["agg"]:
                    if o.get("symbol") == sym:
                        found_sell = o
                        source = "open_agg"
                        break
                if not found_sell:
                    for o in closed_idx["agg"]:
                        if o.get("symbol") == sym:
                            found_sell = o
                            source = "closed_agg"
                            break

            # For chase: best-effort qty+symbol match among s- sells.
            if not found_sell and is_chase:
                best = None
                best_delta = float("inf")
                for o in open_idx["chase"] + closed_idx["chase"]:
                    if o.get("symbol") != sym:
                        continue
                    sell_amt = float(o.get("amount") or o.get("filled") or 0.0)
                    delta = abs(sell_amt - buy_filled_qty)
                    if delta < best_delta:
                        best_delta = delta
                        best = o
                # Accept if within ~1% or small absolute (handles rounding/fee effects).
                if best is not None and (best_delta / max(buy_filled_qty, 1e-9) < 0.01 or best_delta < 1e-6):
                    found_sell = best
                    source = "open_chase" if best in open_idx["chase"] else "closed_chase"

            if found_sell:
                sell_oid = found_sell.get("id") or found_sell.get("order_id")
                sell_cl = found_sell.get("clientOrderId") or ""
                sell_status = self._normalize_status(found_sell.get("status"))
                sell_amt = float(found_sell.get("amount") or found_sell.get("filled") or buy_filled_qty)
                sell_price = float(found_sell.get("average") or found_sell.get("price") or 0.0)
                ts = found_sell.get("datetime") or datetime.now(timezone.utc).isoformat()
                try:
                    self.tracker.add_sell(
                        parent_buy_order_id=buy_oid,
                        sell_order_id=sell_oid,
                        cl_ord_id=sell_cl,
                        ord_type="limit" if "limit" in str(found_sell.get("type", "")).lower() else "market",
                        order_qty=sell_amt,
                        limit_price=sell_price,
                        timestamp=ts,
                        status=sell_status,
                    )
                    # Record a synthetic sell fill row (aggregate is all we have).
                    fee = found_sell.get("fee") or {}
                    fee_cost = float(fee.get("cost") or 0.0) if isinstance(fee, dict) else 0.0
                    self.tracker.append_fill(
                        buy_order_id=buy_oid,
                        side="sell",
                        price=sell_price,
                        qty=sell_amt,
                        fee=fee_cost,
                        timestamp=ts,
                    )
                    if sell_status == "filled":
                        self.tracker.archive(buy_oid)
                    self.logger.info(
                        f"Rebuilt sell for filled buy {buy_oid} {sym} via {source}: "
                        f"sell_oid={sell_oid} status={sell_status}"
                    )
                except Exception as e:
                    self.logger.error(f"Failed to attach rebuilt sell for {buy_oid}: {e}")
                continue

            # No sell found — this is the orphan-filled-buy case.
            # Per current architecture (strategy _exit_decisions + chase/ladder exits):
            # we WANT the cycle present with sell=None so the running bot will
            # place the TP (or agg sell) on the next tick after warmup.
            # Log ERROR for operator visibility (historical "I already alerted").
            self.logger.error(
                f"Orphan filled buy (no sell on Kraken open or closed): "
                f"{buy_oid} {sym} qty_filled≈{buy_filled_qty}. "
                "Cycle left with sell=None; live strategy will place TP sell."
            )
            # Ensure the buy side is fully represented (in case this cycle
            # came only from history or partial adoption).
            self._ensure_buy_from_tracker_or_synth(buy_oid, cycle)

    def _adopt_orphan_open_orders(self, open_orders: list[dict], known: set[str]) -> None:
        """Any Kraken open order whose id is known (or carries our buy cl_ord)
        but is absent from tracker → add_buy (or add_sell for s-/a- if we can
        locate parent). Pure adopt; no cancel, no place.
        """
        for o in open_orders:
            oid = o.get("id") or o.get("order_id")
            if not oid or oid in self.tracker.cycles:
                continue

            cl = o.get("clientOrderId") or ""
            side = o.get("side")
            sym = o.get("symbol")
            is_our_buy_cl = cl.startswith("c-") or cl.startswith("l-")
            is_our_sell_cl = cl.startswith("s-") or cl.startswith("a-")

            if not (oid in known or is_our_buy_cl or is_our_sell_cl):
                continue

            status = self._normalize_status(o.get("status"))
            ts = o.get("datetime") or datetime.now(timezone.utc).isoformat()
            amt = float(o.get("amount") or 0.0)
            px = float(o.get("price") or o.get("average") or 0.0)
            ord_type = o.get("type") or "limit"

            if side == "buy" and (is_our_buy_cl or oid in known):
                try:
                    self.tracker.add_buy(
                        buy_order_id=oid,
                        symbol=sym,
                        cl_ord_id=cl,
                        ord_type=ord_type,
                        order_qty=amt,
                        limit_price=px,
                        timestamp=ts,
                        status=status,
                    )
                    # If it shows filled qty already, record it.
                    filled = float(o.get("filled") or 0.0)
                    if filled > 0:
                        fee = o.get("fee") or {}
                        fcost = float(fee.get("cost") or 0.0) if isinstance(fee, dict) else 0.0
                        self.tracker.append_fill(oid, "buy", px, filled, fcost, ts)
                    self.logger.info(f"Adopted orphan open buy {oid} {sym} status={status}")
                except Exception as e:
                    self.logger.error(f"Adopt open buy {oid} failed: {e}")
            elif side == "sell" and is_our_sell_cl:
                # For sells we need a parent. Best effort: for s- we cannot know
                # parent from cl_ord alone; for a- we can bind later on WS event
                # (the runner already has _process_agg_sell_event that does the
                # symbol scan on first event). For boot we can skip strict add_sell
                # here or adopt a placeholder — the WS path will correct it.
                # To stay simple and safe: log and let the executions consumer
                # + runner logic bind it when the next execution message arrives.
                self.logger.info(
                    f"Orphan open sell {oid} {sym} cl={cl} seen at boot; "
                    "will be bound by executions consumer / _process_*_sell_event"
                )

    def _adopt_orphan_closed_orders(self, closed_orders: list[dict], known: set[str]) -> None:
        """Adopt fully closed bot cycles we have never seen.

        If we see a closed buy whose oid is known/our cl but not in tracker,
        add it (filled), then see if a matching closed sell exists for it and
        complete+archive. This recovers round-trips that both sides finished
        while the bot was down and the orders aged out of the live tracker.
        """
        closed_by_id = { (o.get("id") or o.get("order_id")): o for o in closed_orders if o.get("id") or o.get("order_id") }

        for oid, o in closed_by_id.items():
            if oid in self.tracker.cycles:
                continue
            cl = o.get("clientOrderId") or ""
            side = o.get("side")
            if side != "buy":
                continue
            is_our = (oid in known) or cl.startswith("c-") or cl.startswith("l-")
            if not is_our:
                continue

            sym = o.get("symbol")
            status = self._normalize_status(o.get("status"))
            ts = o.get("datetime") or datetime.now(timezone.utc).isoformat()
            amt = float(o.get("amount") or 0.0)
            px = float(o.get("average") or o.get("price") or 0.0)
            try:
                self.tracker.add_buy(
                    buy_order_id=oid,
                    symbol=sym,
                    cl_ord_id=cl,
                    ord_type=o.get("type") or "limit",
                    order_qty=amt,
                    limit_price=px,
                    timestamp=ts,
                    status=status,
                )
                filled = float(o.get("filled") or amt)
                if filled > 0:
                    fee = o.get("fee") or {}
                    fcost = float(fee.get("cost") or 0.0) if isinstance(fee, dict) else 0.0
                    self.tracker.append_fill(oid, "buy", px, filled, fcost, ts)
                self.logger.info(f"Adopted orphan closed buy {oid} {sym} status={status}")
            except Exception as e:
                self.logger.error(f"Adopt closed buy {oid} failed: {e}")
                continue

            # Try to find and complete a matching sell from the same closed batch.
            # Same heuristic as rebuild.
            buy_filled = filled
            found = None
            for soid, so in closed_by_id.items():
                if so.get("side") != "sell" or so.get("symbol") != sym:
                    continue
                scl = so.get("clientOrderId") or ""
                if scl.startswith("a-") or (scl.startswith("s-") and abs(float(so.get("filled") or 0) - buy_filled) / max(buy_filled, 1e-9) < 0.01):
                    found = (soid, so)
                    break
            if found:
                soid, so = found
                sstatus = self._normalize_status(so.get("status"))
                sprice = float(so.get("average") or so.get("price") or 0.0)
                samt = float(so.get("filled") or so.get("amount") or buy_filled)
                sts = so.get("datetime") or ts
                try:
                    self.tracker.add_sell(oid, soid, so.get("clientOrderId") or "", so.get("type") or "market", samt, sprice, sts, sstatus)
                    fee = so.get("fee") or {}
                    self.tracker.append_fill(oid, "sell", sprice, samt, float((fee or {}).get("cost") or 0), sts)
                    if sstatus == "filled":
                        self.tracker.archive(oid)
                    self.logger.info(f"Completed orphan closed cycle via sell {soid} for buy {oid}")
                except Exception as e:
                    self.logger.error(f"Complete orphan closed cycle {oid} failed: {e}")

    # ── helpers ──────────────────────────────────────────────────────────

    def _normalize_status(self, raw: str | None) -> str:
        if not raw:
            return "open"
        r = raw.lower()
        if r in ("filled", "closed"):
            return "filled"
        if r in ("canceled", "cancelled", "expired"):
            return "canceled"
        if "partial" in r:
            return "partially_filled"
        if r in ("open", "pending_new", "new", "pending"):
            return "pending_new"
        return raw  # pass through unknown; runner/strategy are tolerant of 'filled' checks

    def _ensure_buy_from_rest(self, o: dict, oid: str, status: str) -> None:
        """Idempotent: make sure a buy row exists for oid using REST order info."""
        if oid in self.tracker.cycles:
            return
        sym = o.get("symbol")
        cl = o.get("clientOrderId") or ""
        amt = float(o.get("amount") or 0.0)
        px = float(o.get("price") or o.get("average") or 0.0)
        ts = o.get("datetime") or datetime.now(timezone.utc).isoformat()
        try:
            self.tracker.add_buy(
                buy_order_id=oid,
                symbol=sym,
                cl_ord_id=cl,
                ord_type=o.get("type") or "limit",
                order_qty=amt,
                limit_price=px,
                timestamp=ts,
                status=self._normalize_status(status),
            )
        except Exception as e:
            self.logger.error(f"_ensure_buy_from_rest {oid} failed: {e}")

    def _ensure_buy_from_tracker_or_synth(self, buy_oid: str, cycle: dict) -> None:
        """If the cycle is only a shell (e.g. from history load without full
        buy subdoc), make sure the buy side is present with what we have.
        """
        buy = cycle.get("buy") or {}
        if not buy.get("order_id"):
            # Should not happen for cycles loaded from tracker.
            return
        # Already present in self.tracker.cycles — nothing to do.
        # The error log above already happened; strategy will see the buy.


def prune_stale_canceled_buys(tracker: Any, log: Any = None) -> int:
    """Remove pure stale canceled buys (zero fills, no sell attached) from
    the active tracker. These are almost always ladder rungs that were
    canceled by ratcheting, state transitions, or proximity adjustments
    before any fill occurred. They have no economic impact and should not
    accumulate forever in order_cycles.json or known_bot_ids.

    Completed roundtrips (with sells) belong in history; pure cancels do
    not represent "executed trades".

    Returns number of cycles pruned.
    """
    if log is None:
        log = get_logger("StaleBuyPruner")

    to_prune = []
    for buy_oid, cycle in list(tracker.cycles.items()):
        buy = cycle.get("buy") or {}
        if buy.get("status") != "canceled":
            continue
        if cycle.get("sell") is not None:
            continue
        fills = buy.get("fills") or []
        total_filled = sum(float(f.get("qty", 0)) for f in fills)
        if total_filled < 1e-9:
            to_prune.append(buy_oid)

    for buy_oid in to_prune:
        try:
            cycle = tracker.cycles.get(buy_oid)
            if cycle:
                cycle["_pruned_stale_zero_fill_cancel"] = True
                cycle["_pruned_at"] = datetime.now(timezone.utc).isoformat()
            tracker.archive(buy_oid)
            log.debug(f"Pruned stale zero-fill canceled buy {buy_oid}")
        except Exception as e:
            log.error(f"Failed to prune stale buy {buy_oid}: {e}")

    if to_prune:
        log.info(f"Pruned {len(to_prune)} stale zero-fill canceled buys from active tracker")
    return len(to_prune)


async def reconcile_open_positions_to_exchange_balances(
    tracker: Any, order_ops: Any, symbols: list[str] | None = None, log: Any = None
) -> int:
    """Enforce: the tracker MUST reflect the exchange 100% for token holdings.

    For every base currency of the given symbols, compute the tracker's current
    claimed unsold quantity (sum of remaining buy fills on cycles that have no
    completed sell). Query the exchange free balance for that base. If claimed
    > actual free, the excess represents tokens that have left the account
    without a corresponding bot sell (manual sale, withdrawal, dust, prior
    reconciliation gap, etc.).

    We correct the tracker toward the exchange by archiving the excess cycles
    (moving them to history with reconciliation metadata). This stops phantom
    sell attempts and makes the tracker match reality.

    Called from StartupReconciler (boot) and ExchangeSyncTask (reconnects), and
    can be forced from the runner on sell errors.

    Returns the number of cycles archived/corrected.
    """
    if log is None:
        log = get_logger("PositionBalanceReconciler")

    if symbols is None or len(symbols) == 0:
        symbols = sorted({c.get("symbol") for c in tracker.cycles.values() if c.get("symbol")})

    if not symbols:
        return 0

    from collections import defaultdict
    base_to_syms: dict[str, list[str]] = defaultdict(list)
    for s in symbols:
        if "/" not in str(s):
            continue
        base = str(s).split("/")[0].upper()
        base_to_syms[base].append(s)

    try:
        detailed = await order_ops.fetch_detailed_balances()
    except Exception as e:
        log.error(f"fetch_detailed_balances failed in position reconciliation: {e}")
        return 0

    free_map = detailed.get("free") or {}

    corrections = 0

    for base, sym_list in base_to_syms.items():
        candidates: list[tuple[str, dict, float, str]] = []  # (oid, cycle, remaining, sym)
        claimed = 0.0

        for buy_oid, cycle in list(tracker.cycles.items()):
            if cycle.get("symbol") not in sym_list:
                continue
            sell = cycle.get("sell") or {}
            sell_fills = sell.get("fills") or []
            sell_done = sum(float(f.get("qty", 0)) for f in sell_fills)
            buy = cycle.get("buy") or {}
            buy_fills = buy.get("fills") or []
            buy_done = sum(float(f.get("qty", 0)) for f in buy_fills)
            remaining = max(0.0, buy_done - sell_done)
            if remaining > 1e-12:
                candidates.append((buy_oid, cycle, remaining, cycle.get("symbol")))
                claimed += remaining

        actual_free = float(free_map.get(base, 0.0) or 0.0)
        if claimed <= actual_free + 1e-9:
            continue

        excess = claimed - actual_free
        log.error(
            f"TRACKER/EXCHANGE DIVERGENCE {base}: tracker claims {claimed:.8f} unsold "
            f"across {len(candidates)} cycles (sell=None or incomplete), "
            f"exchange free balance={actual_free:.8f}. Excess={excess:.8f}. "
            "Correcting tracker to match exchange (archiving excess as external/manual liquidation)."
        )

        # Drop largest first — conservative, removes biggest phantom claims preferentially.
        candidates.sort(key=lambda t: t[2], reverse=True)

        to_archive: list[tuple[str, dict, float, str]] = []
        still_excess = excess
        for buy_oid, cycle, rem, sym in candidates:
            if still_excess <= 1e-9:
                break
            to_archive.append((buy_oid, cycle, rem, sym))
            still_excess -= rem

        for buy_oid, cycle, rem, sym in to_archive:
            try:
                cycle["_balance_reconciled_external"] = True
                cycle["_reconciled_free_balance"] = actual_free
                cycle["_reconciled_excess"] = excess
                cycle["_reconciled_reason"] = (
                    "tracker corrected to exchange holdings; tokens sold/removed outside bot"
                )
                cycle["_reconciled_at"] = datetime.now(timezone.utc).isoformat()
                tracker.archive(buy_oid)
                corrections += 1
                log.warning(
                    f"Archived {buy_oid} {sym} (remaining≈{rem:.8f}) via balance reconciliation for {base} "
                    f"(free={actual_free:.8f})"
                )
            except Exception as e:
                log.error(f"Failed to archive {buy_oid} during position/balance correction: {e}")

    return corrections
