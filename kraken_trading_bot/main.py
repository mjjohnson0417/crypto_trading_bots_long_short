"""Kraken trading bot CLI entry point.

Boot sequence:
  1. Load config + decrypt API keys.
  2. Connect to Kraken (REST).
  3. Start the DataManager: opens public + private WS, runs sorters,
     buffers consume into DataFrames.
  4. Construct TradingRunner (builds OrderOperations, FeeCache, TradeTracker,
     IndicatorCalculator, HybridGridStrategy).
  5. StartupReconciler runs (awaited) — brings tracker in sync with Kraken
     using only passive writes; cancels stale/removed-symbol buys.
  6. Trading task + ExchangeSyncTask (reconnect-driven auditor) are spawned.
  7. ShutdownManager installs handlers and owns the verified cancel sequence.

`trading.dry_run` in config gates all order placements and cancels at
the OrderOperations layer — when true, the full trading stack runs end
to end but no orders hit the exchange.
"""
import asyncio
import logging
import sys
import traceback
from pathlib import Path

from config import load_config
from exchange.connection import ExchangeConnection
from credentials import KeyLoader
from logging_config import setup_centralized_logging, get_logger
from market_data import DataManager
from telemetry import RawWsLogger
from execution import StartupReconciler
from orchestrator import ShutdownManager, TradingRunner
from persistence.exchange_sync import ExchangeSyncTask


exchange_connection = None
data_manager = None
raw_logger = None
shutdown_event = asyncio.Event()

logger = get_logger('RulesTradingBot')


async def main():
    """Entry point — bring up data layer, trading runner, shutdown manager."""
    global exchange_connection, data_manager, raw_logger

    logger.info("Starting bot.")

    try:
        config_path = Path(__file__).parent / 'config.json'
        config = load_config(config_path)

        setup_centralized_logging(config)
        logger.info(f"Loaded config (dry_run={config['trading']['dry_run']})")

        api_key, secret = KeyLoader(config).load()

        exchange_connection = ExchangeConnection(api_key, secret)
        await exchange_connection.check_rest_connection()
        logger.info("REST API connection verified")

        symbols = config['symbols']

        raw_logger = RawWsLogger(config['logging']['raw_ws_logging'])

        data_manager = DataManager(
            symbols=symbols,
            exchange_connection=exchange_connection,
            config=config,
            raw_logger=raw_logger,
        )
        await data_manager.initialize()
        logger.info(f"DataManager initialized for {symbols}")

        runner = TradingRunner(
            data_manager=data_manager,
            exchange_connection=exchange_connection,
            config=config,
            shutdown_event=shutdown_event,
        )

        # Daily candles + SMAs for the 200-day buy gate. Raises (and the
        # bot exits) if any symbol can't be fetched or lacks history.
        await runner.daily_trend.initialize()

        # Boot-time reconciliation MUST run before the runner task (and thus
        # before _tick_loop or executions_consumer can act on tracker state).
        reconciler = StartupReconciler(
            tracker=runner.tracker,
            order_ops=runner.order_ops,
            symbols=symbols,
        )
        await reconciler.reconcile()

        shutdown_mgr = ShutdownManager(
            shutdown_event=shutdown_event,
            order_ops=runner.order_ops,
            data_manager=data_manager,
            exchange_connection=exchange_connection,
        )
        shutdown_mgr.set_runner(runner)
        shutdown_mgr.install_signal_handlers(asyncio.get_running_loop())

        trading_task = None
        sync_task = None

        trading_task = asyncio.create_task(runner.start(), name='trading_runner')

        sync = ExchangeSyncTask(
            tracker=runner.tracker,
            order_ops=runner.order_ops,
            data_manager=data_manager,
            shutdown_event=shutdown_event,
            closed_orders_lookback_seconds=int(
                config['trading']['exchange_sync_lookback_seconds']
            ),
        )
        sync_task = asyncio.create_task(sync.run(), name='exchange_sync')

        logger.info("Trading stack up — waiting for shutdown signal.")
        # Wait for a shutdown signal OR for a long-lived task to end. Either
        # task ending on its own is a failure: run the normal cancel-and-
        # shutdown sequence, then exit non-zero so systemd restarts the bot.
        shutdown_wait = asyncio.create_task(shutdown_event.wait(), name='shutdown_wait')
        done, _ = await asyncio.wait(
            {shutdown_wait, trading_task, sync_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if shutdown_wait not in done:
            for t in done:
                exc = None if t.cancelled() else t.exception()
                logger.error(
                    f"Task {t.get_name()} stopped unexpectedly "
                    f"(exception={exc!r}) — shutting down for restart"
                )
            await shutdown_mgr.cancel_and_shutdown()
            raise RuntimeError("long-lived task stopped unexpectedly")
        logger.info("Shutdown signal received.")

        # Cancel long-lived tasks (sync first is fine; both are independent).
        for t in (sync_task, trading_task):
            if t is not None and not t.done():
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass

    except Exception as e:
        logger.error(f"Error in startup: {e}\n{traceback.format_exc()}")
        raise SystemExit(1)
    finally:
        if data_manager is not None:
            try:
                await data_manager.close()
            except Exception as e:
                logger.error(f"DataManager close error: {e}", exc_info=True)
        if raw_logger is not None:
            try:
                raw_logger.close()
            except Exception as e:
                logger.error(f"RawWsLogger close error: {e}", exc_info=True)
        if exchange_connection is not None:
            try:
                await exchange_connection.close()
            except Exception as e:
                logger.error(f"ExchangeConnection close error: {e}", exc_info=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt")
    except Exception as e:
        logger.error(f"Fatal error: {e}\n{traceback.format_exc()}")
        sys.exit(1)
