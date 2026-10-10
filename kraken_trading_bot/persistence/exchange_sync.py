"""ExchangeSyncTask — runtime reconciler that runs on private WS reconnects.

Catches anything missed during an outage (fills, status changes, placements
whose acks were lost, sells that completed while the executions consumer
was not draining, etc.).

Trigger: data_manager.private_ws.reconnect_event (set by PrivateWebSocket
after every successful executions subscribe, including the initial one).

It is deliberately *not* on a timer; it is outage-driven. An initial audit
shortly after boot is harmless (and provides belt-and-suspenders coverage
with the explicit StartupReconciler).

Constraints (identical to StartupReconciler):
- Only tracker passive writes (add_*, update_status, append_fill, archive, save).
- No defaults that hide bugs.
- Never cancels. Never places.
- Uses known_bot_ids (current cycles + history) + cl_ord prefix detection.
- Closed orders limited to the configured lookback (default 1800s).

This module was implemented as part of restoring the full
StartupReconciler + ExchangeSyncTask system per the provided plan.
It only performs read + tracker write reconciliation. Never cancels, never places.
See execution/reconciliation.py for Phase A design decision notes.
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

from logging_config import get_logger


class ExchangeSyncTask:
    """Long-lived task. Awaits reconnect_event, audits, repeats."""

    def __init__(
        self,
        tracker: Any,
        order_ops: Any,
        data_manager: Any,
        shutdown_event: asyncio.Event,
        closed_orders_lookback_seconds: int,
    ):
        self.logger = get_logger("ExchangeSync")
        self.tracker = tracker
        self.order_ops = order_ops
        self.data_manager = data_manager
        self.shutdown_event = shutdown_event
        self.lookback_s = int(closed_orders_lookback_seconds)

    async def run(self) -> None:
        """Main loop: wait for (re)connect signal, audit, repeat."""
        self.logger.info("ExchangeSyncTask starting (waiting for reconnect_event)")
        while not self.shutdown_event.is_set():
            try:
                await self.data_manager.private_ws.reconnect_event.wait()
                self.data_manager.private_ws.reconnect_event.clear()
                if self.shutdown_event.is_set():
                    break
                await self._audit()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"ExchangeSyncTask error in run: {e}", exc_info=True)
                # Back off a little so we don't spin on persistent bug.
                await asyncio.sleep(5)

    async def _audit(self) -> None:
        """One audit pass: diff tracker vs exchange (open + recent closed),
        write only to tracker to bring it into agreement.
        """
        t0 = datetime.now(timezone.utc)
        known = self._compute_known_bot_ids()
        self.logger.debug(f"audit start known={len(known)}")

        # Parallel fetch (open is account-wide; closed is lookback-limited).
        open_coro = self.order_ops.fetch_open_orders(symbol=None)
        since_ms = int((datetime.now(timezone.utc).timestamp() - self.lookback_s) * 1000)
        closed_coro = self.order_ops.fetch_closed_orders(since_timestamp=since_ms)

        try:
            open_orders, closed_orders = await asyncio.gather(
                open_coro, closed_coro, return_exceptions=True
            )
            if isinstance(open_orders, Exception):
                self.logger.error(f"fetch_open_orders failed in audit: {open_orders}")
                open_orders = []
            if isinstance(closed_orders, Exception):
                self.logger.error(f"fetch_closed_orders failed in audit: {closed_orders}")
                closed_orders = []
        except Exception as e:
            self.logger.error(f"gather fetch failed in audit: {e}")
            open_orders, closed_orders = [], []

        open_by_id = {}
        for o in open_orders or []:
            oid = o.get("id") or o.get("order_id")
            if oid:
                open_by_id[oid] = o

        closed_by_id = {}
        for o in closed_orders or []:
            oid = o.get("id") or o.get("order_id")
            if oid:
                closed_by_id[oid] = o

        divergences = 0

        # 1. Walk every tracker cycle and catch up anything the exchange
        #    says is further along (closed while we were offline).
        for buy_oid, cycle in list(self.tracker.cycles.items()):
            buy = cycle.get("buy") or {}
            sell = cycle.get("sell")

            # Buy side
            if buy_oid in closed_by_id:
                o = closed_by_id[buy_oid]
                new_status = self._normalize_status(o.get("status"))
                if buy.get("status") != new_status:
                    try:
                        self.tracker.update_status(buy_oid, "buy", new_status)
                        divergences += 1
                    except Exception as e:
                        self.logger.error(f"audit update buy status {buy_oid}: {e}")
                # Record aggregate fill info if we see more filled.
                filled = float(o.get("filled") or 0.0)
                if filled > 0:
                    avg = float(o.get("average") or o.get("price") or 0.0)
                    fee_cost = self._fee_cost(o)
                    ts = o.get("datetime") or t0.isoformat()
                    try:
                        self.tracker.append_fill(buy_oid, "buy", avg, filled, fee_cost, ts)
                    except Exception as e:
                        self.logger.error(f"audit append buy fill {buy_oid}: {e}")

            # Sell side (if attached)
            if sell:
                soid = sell.get("order_id")
                if soid and soid in closed_by_id:
                    o = closed_by_id[soid]
                    new_status = self._normalize_status(o.get("status"))
                    if sell.get("status") != new_status:
                        try:
                            self.tracker.update_status(buy_oid, "sell", new_status)
                            divergences += 1
                        except Exception as e:
                            self.logger.error(f"audit update sell status {soid}: {e}")
                    filled = float(o.get("filled") or 0.0)
                    if filled > 0:
                        avg = float(o.get("average") or o.get("price") or 0.0)
                        fee_cost = self._fee_cost(o)
                        ts = o.get("datetime") or t0.isoformat()
                        try:
                            self.tracker.append_fill(buy_oid, "sell", avg, filled, fee_cost, ts)
                        except Exception as e:
                            self.logger.error(f"audit append sell fill {soid}: {e}")
                    if new_status == "filled":
                        try:
                            self.tracker.archive(buy_oid)
                            divergences += 1
                        except Exception as e:
                            self.logger.error(f"audit archive {buy_oid}: {e}")

            # Tracker thinks open but exchange has no record in open and
            # the lookback closed also doesn't have it → treat as gone.
            # (Rare; usually caught by the closed path above.)
            if buy.get("status") not in ("filled", "canceled"):
                if buy_oid not in open_by_id and buy_oid not in closed_by_id:
                    # We don't have definitive "canceled" vs "filled" info.
                    # Conservative: mark canceled (the exchange sync policy
                    # is "exchange wins"). The next startup reconciler or
                    # manual inspection of history + raw_ws will clarify.
                    try:
                        self.tracker.update_status(buy_oid, "buy", "canceled")
                        divergences += 1
                        self.logger.warning(f"tracker buy {buy_oid} missing from exchange open+closed lookback → marked canceled")
                    except Exception as e:
                        self.logger.error(f"audit force-cancel missing buy {buy_oid}: {e}")

        # 2. Adopt anything on the exchange that belongs to us (known or our
        #    cl_ord prefix) but is not in the tracker. This is the "to_adopt"
        #    path for orders placed just before a crash/restart, or whose
        #    placement response was lost.
        for oid, o in open_by_id.items():
            if oid in self.tracker.cycles:
                continue
            if not self._is_bot_owned(o, oid, known):
                continue
            side = o.get("side")
            cl = o.get("clientOrderId") or ""
            sym = o.get("symbol")
            status = self._normalize_status(o.get("status"))
            amt = float(o.get("amount") or 0.0)
            px = float(o.get("price") or o.get("average") or 0.0)
            ts = o.get("datetime") or t0.isoformat()
            ordt = o.get("type") or "limit"
            try:
                if side == "buy":
                    self.tracker.add_buy(oid, sym, cl, ordt, amt, px, ts, status)
                    filled = float(o.get("filled") or 0.0)
                    if filled > 0:
                        self.tracker.append_fill(oid, "buy", px, filled, self._fee_cost(o), ts)
                    divergences += 1
                    self.logger.info(f"adopted open buy from exchange {oid} {sym}")
                elif side == "sell" and (cl.startswith("s-") or cl.startswith("a-")):
                    # We cannot reliably know the parent buy for an s- sell here.
                    # a- sells are bound by symbol scan in the runner on first event.
                    # Record a note; the executions consumer will finish wiring
                    # when the next WS message arrives for it.
                    self.logger.info(
                        f"adopted open sell {oid} {sym} cl={cl} (binding deferred to executions path)"
                    )
            except Exception as e:
                self.logger.error(f"adopt open {oid} failed: {e}")

        # Also adopt from recent closed if they are known to us but absent.
        for oid, o in closed_by_id.items():
            if oid in self.tracker.cycles:
                continue
            if not self._is_bot_owned(o, oid, known):
                continue
            # Only adopt buys here (sells without parents are less useful).
            if o.get("side") != "buy":
                continue
            cl = o.get("clientOrderId") or ""
            sym = o.get("symbol")
            # A closed buy that executed nothing is a canceled rung, not a
            # position: adopting it only to prune it again was pure churn.
            if o["filled"] is None:
                self.logger.error(f"closed buy {oid} {sym} has no 'filled' value; not adopted")
                continue
            if float(o["filled"]) <= 0:
                continue
            status = self._normalize_status(o.get("status"))
            amt = float(o.get("amount") or 0.0)
            px = float(o.get("average") or o.get("price") or 0.0)
            ts = o.get("datetime") or t0.isoformat()
            try:
                self.tracker.add_buy(oid, sym, cl, o.get("type") or "limit", amt, px, ts, status)
                filled = float(o.get("filled") or 0.0)
                if filled > 0:
                    self.tracker.append_fill(oid, "buy", px, filled, self._fee_cost(o), ts)
                divergences += 1
                self.logger.info(f"adopted closed buy from exchange {oid} {sym} status={status}")
                # If this closed buy has a matching closed sell in the same batch, complete it.
                # (Light heuristic; full pairing is best-effort.)
                for soid, so in closed_by_id.items():
                    if so.get("side") != "sell" or so.get("symbol") != sym:
                        continue
                    # A sell that executed nothing can't complete a cycle.
                    if so["filled"] is None or float(so["filled"]) <= 0:
                        continue
                    scl = so.get("clientOrderId") or ""
                    if scl.startswith("a-") or (scl.startswith("s-") and abs(float(so.get("filled") or 0) - filled) / max(filled or 1, 1e-9) < 0.01):
                        sstatus = self._normalize_status(so.get("status"))
                        sprice = float(so.get("average") or so.get("price") or 0.0)
                        samt = float(so["filled"])
                        sts = so.get("datetime") or ts
                        try:
                            self.tracker.add_sell(oid, soid, scl, so.get("type") or "market", samt, sprice, sts, sstatus)
                            self.tracker.append_fill(oid, "sell", sprice, samt, self._fee_cost(so), sts)
                            if sstatus == "filled":
                                self.tracker.archive(oid)
                            divergences += 1
                        except Exception as e:
                            self.logger.error(f"audit complete adopted closed cycle {oid}: {e}")
                        break
            except Exception as e:
                self.logger.error(f"adopt closed {oid} failed: {e}")

        dt = (datetime.now(timezone.utc) - t0).total_seconds()
        level = self.logger.warning if divergences else self.logger.info
        level(
            f"ExchangeSync audit done in {dt:.2f}s "
            f"(known={len(known)}, open={len(open_by_id)}, closed={len(closed_by_id)}, "
            f"divergences_fixed={divergences})"
        )
        self.tracker.save()

        # Enforce tracker == exchange token balances (in addition to order reconciliation).
        # If tracker claims unsold qty for a base but free balance is lower (manual sell,
        # external move, etc.), correct by archiving excess cycles.
        try:
            from execution.reconciliation import reconcile_open_positions_to_exchange_balances
            # Derive symbols from current cycles so we cover whatever the tracker knows about.
            active_symbols = sorted({c.get("symbol") for c in self.tracker.cycles.values() if c.get("symbol")})
            pos_corrections = await reconcile_open_positions_to_exchange_balances(
                self.tracker, self.order_ops, active_symbols or None, self.logger
            )
            if pos_corrections:
                divergences += pos_corrections
                self.logger.warning(
                    f"ExchangeSync position/balance reconciliation corrected {pos_corrections} cycles "
                    "(tracker holdings now match exchange free balances)."
                )
        except Exception as e:
            self.logger.error(f"Position balance reconciliation in ExchangeSync failed: {e}")

        # Prune any new stale zero-fill canceled buys discovered during this audit.
        # This prevents the active tracker (and known_bot_ids) from growing
        # indefinitely with canceled ladder rungs that never filled.
        # Pure cancels do not represent executed trades.
        try:
            from execution.reconciliation import prune_stale_canceled_buys
            pruned = prune_stale_canceled_buys(self.tracker, self.logger)
            if pruned:
                divergences += pruned
                self.logger.info(f"ExchangeSync pruned {pruned} stale zero-fill canceled buys")
        except Exception as e:
            self.logger.error(f"Stale buy prune in ExchangeSync failed: {e}")

    def _compute_known_bot_ids(self) -> set[str]:
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
                    for c in json.load(f):
                        b = (c or {}).get("buy") or {}
                        if b.get("order_id"):
                            ids.add(b["order_id"])
                        s = (c or {}).get("sell") or {}
                        if s.get("order_id"):
                            ids.add(s["order_id"])
            except Exception as e:
                self.logger.error(f"history read failed in audit: {e}")
        return ids

    def _is_bot_owned(self, o: dict, oid: str, known: set[str]) -> bool:
        if oid in known:
            return True
        cl = o.get("clientOrderId") or ""
        return cl.startswith("c-") or cl.startswith("l-") or cl.startswith("s-") or cl.startswith("a-")

    def _normalize_status(self, raw: str | None) -> str:
        if not raw:
            return "pending_new"
        r = raw.lower()
        if r in ("filled", "closed"):
            return "filled"
        if r in ("canceled", "cancelled", "expired"):
            return "canceled"
        if "partial" in r:
            return "partially_filled"
        if r in ("open", "pending_new", "new", "pending"):
            return "pending_new"
        return raw

    def _fee_cost(self, o: dict) -> float:
        fee = o.get("fee") or {}
        if isinstance(fee, dict):
            return float(fee.get("cost") or 0.0)
        return 0.0
