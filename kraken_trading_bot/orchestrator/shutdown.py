"""Shutdown manager — single source of truth for the shutdown lifecycle.

Owns:
- The shared asyncio.Event signaling tasks to exit.
- Signal handler installation (SIGINT, SIGTERM) via
  `loop.add_signal_handler` so the handler can schedule async work
  directly on the event loop instead of just flipping a flag.
- The cancel-and-shutdown coroutine that runs in response to a signal.
- Verified buy cancellation against Kraken (no exit until Kraken
  confirms all bot-owned buys are gone, or N retry attempts fail and
  we log every leftover order_id).
- Teardown of DataManager and exchange connection.

Design rules:
- Signal handler ONLY schedules `cancel_and_shutdown`. It does NOT
  flip `data_manager.shutdown=True` or any other flag that would
  poison the WebSocket. The WS must stay alive through the cancel
  phase so cancels can actually be sent.
- `data_manager.shutdown=True` is set later inside `close()`, AFTER
  cancels are verified.
- Idempotent: a second signal is ignored if shutdown is already in
  progress.
"""
import asyncio
import signal
import traceback

from logging_config import get_logger


class ShutdownManager:
    """Single source of truth for the shutdown lifecycle."""

    CANCEL_VERIFY_ATTEMPTS = 3
    CANCEL_VERIFY_DELAY_SECONDS = 1.0

    def __init__(self, shutdown_event, order_ops, data_manager, exchange_connection):
        """
        Args:
            shutdown_event: asyncio.Event signaling tasks to stop.
            order_ops: OrderOperations instance (REST + WS).
            data_manager: DataManager instance.
            exchange_connection: ExchangeConnection (ccxt close).

        `runner` is wired in later by main via `set_runner()` — it does
        not exist at ShutdownManager construction time.
        """
        self.logger = get_logger('ShutdownManager')
        self.shutdown_event = shutdown_event
        self.order_ops = order_ops
        self.data_manager = data_manager
        self.exchange_connection = exchange_connection
        self.runner = None
        self._shutdown_started = False

    def set_runner(self, runner) -> None:
        """Wire in the TradingRunner once main has constructed it. Used
        by `cancel_and_shutdown` to set `runner.placement_paused = True`,
        which makes the strategy loop skip placement decisions while the
        shutdown sequence is in progress."""
        self.runner = runner

    def install_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
        """Wire SIGINT and SIGTERM to schedule `cancel_and_shutdown`
        on the event loop. Replaces the legacy `signal.signal()` form
        with the asyncio-aware variant so the handler can run async
        work, not just flip a flag.
        """
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._on_signal)
        self.logger.info("Signal handlers installed (SIGINT, SIGTERM)")

    def _on_signal(self) -> None:
        """Runs in the event-loop context when a shutdown signal arrives.
        Idempotent: a second signal is ignored."""
        if self._shutdown_started:
            self.logger.info(
                "Shutdown signal received (shutdown already in progress, ignoring)"
            )
            return
        self._shutdown_started = True
        self.logger.info(
            "Shutdown signal received — scheduling cancel_and_shutdown"
        )
        asyncio.create_task(self.cancel_and_shutdown())

    async def cancel_and_shutdown(self) -> None:
        """The full shutdown sequence:

        1. Pause new placements (`state.placement_paused = True`) so
           strategies stop creating new orders immediately.
        2. Cancel all open buys on Kraken with verification (loop fetch
           + cancel until zero remain or retry cap exhausted).
        3. Set `shutdown_event` → trading tasks exit cleanly.
        4. `close()` → tear down DataManager and exchange connection.
           This is where `data_manager.shutdown=True` finally gets set.
        """
        self.logger.info("Initiating cancel-and-shutdown sequence")
        # Mark started in case this was invoked directly (e.g. backstop
        # path in cli's finally) rather than via _on_signal.
        self._shutdown_started = True

        # 1. Pause new placements
        if self.runner is not None:
            self.runner.placement_paused = True
            self.logger.info("Placements paused — strategy will skip new orders")

        # 2. Cancel all open buys + verify
        try:
            await self._cancel_all_buys_verified()
        except Exception as e:
            self.logger.error(
                f"Cancel-and-verify raised: {e}\n{traceback.format_exc()}"
            )

        # 3. Signal trading tasks to exit
        self.shutdown_event.set()
        self.logger.info("shutdown_event set — trading tasks will exit")

        # Give tasks time to wind down their current iteration before we
        # close their connections out from under them. The strategy
        # runner's outer tick has a 1s sleep — anything less than that
        # risks closing the WS while a place/cancel is in flight, which
        # holds aiohttp connector references past shutdown and triggers
        # the "Unclosed connector" warning.
        await asyncio.sleep(1.5)

        # 4. Teardown
        await self.close()

    async def _cancel_all_buys_verified(self) -> None:
        """Cancel every open buy on Kraken, verify, retry up to N times.

        Each attempt:
          - Fetch open orders (REST, account-wide).
          - Filter to buys.
          - If zero → done, log and return.
          - Otherwise cancel each, sleep briefly, loop.

        After the retry cap: log ERROR listing every remaining order_id,
        then return anyway (don't block shutdown indefinitely).
        """
        for attempt in range(1, self.CANCEL_VERIFY_ATTEMPTS + 1):
            try:
                orders = await self.order_ops.fetch_open_orders(symbol=None)
            except Exception as e:
                self.logger.warning(
                    f"Cancel-verify attempt {attempt}: "
                    f"fetch_open_orders failed: {e}"
                )
                await asyncio.sleep(self.CANCEL_VERIFY_DELAY_SECONDS)
                continue

            buys = [o for o in orders if o.get('side') == 'buy']
            if not buys:
                self.logger.info(
                    f"Cancel-verify attempt {attempt}: no open buys "
                    f"remain on Kraken"
                )
                return

            self.logger.info(
                f"Cancel-verify attempt {attempt}: {len(buys)} open "
                f"buy(s) to cancel (parallel)"
            )
            # Fire all cancels concurrently — sequential awaits blew the
            # systemd TimeoutStopSec window when many orders were open.
            results = await asyncio.gather(
                *[
                    self.order_ops.cancel_order(o.get('symbol'), o.get('id'))
                    for o in buys
                ],
                return_exceptions=True,
            )
            for order, result in zip(buys, results):
                oid = order.get('id')
                sym = order.get('symbol')
                if isinstance(result, Exception):
                    self.logger.warning(f"  cancel {oid} {sym} raised: {result}")
                else:
                    self.logger.info(f"  cancel {oid} {sym}: {result}")
            # Let Kraken process the cancellations before re-fetching.
            await asyncio.sleep(self.CANCEL_VERIFY_DELAY_SECONDS)

        # Retry cap exhausted — final fetch to log what's still alive.
        try:
            orders = await self.order_ops.fetch_open_orders(symbol=None)
            leftovers = [o for o in orders if o.get('side') == 'buy']
            if leftovers:
                self.logger.error(
                    f"SHUTDOWN: {len(leftovers)} buy order(s) STILL OPEN on "
                    f"Kraken after {self.CANCEL_VERIFY_ATTEMPTS} cancel "
                    f"attempts. Order IDs: "
                    f"{[(o.get('id'), o.get('symbol')) for o in leftovers]}"
                )
            else:
                self.logger.info(
                    f"Cancel-verify final fetch: no open buys remain"
                )
        except Exception as e:
            self.logger.error(
                f"SHUTDOWN: could not verify leftover buys after "
                f"{self.CANCEL_VERIFY_ATTEMPTS} attempts: {e}"
            )

    async def close(self) -> None:
        """Tear down DataManager and exchange connection. This is where
        `data_manager.shutdown=True` gets set — NOT in the signal handler,
        because that would poison the WS before cancels could be sent."""
        self.logger.info("Closing data manager and exchange connection...")
        try:
            if self.data_manager is not None:
                try:
                    await asyncio.wait_for(
                        self.data_manager.close(), timeout=5.0
                    )
                    self.logger.info("DataManager closed")
                except asyncio.TimeoutError:
                    self.logger.warning("DataManager close timed out")
                except Exception as e:
                    self.logger.warning(f"DataManager close error: {e}")

            if self.exchange_connection is not None:
                try:
                    await asyncio.wait_for(
                        self.exchange_connection.close(), timeout=5.0
                    )
                    self.logger.info("ExchangeConnection closed")
                except asyncio.TimeoutError:
                    self.logger.warning("ExchangeConnection close timed out")
                except Exception as e:
                    self.logger.warning(f"ExchangeConnection close error: {e}")
        except Exception as e:
            self.logger.error(
                f"Error during shutdown close: {e}\n{traceback.format_exc()}"
            )
        finally:
            self.logger.info("Shutdown complete")
