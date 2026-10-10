"""DataManager — the market-data subsystem coordinator.

Owns:
  - The four DataFrames per symbol (book, trades, ticker, klines).
  - The public + private WebSocket wrappers and their tasks.
  - The Kraken Futures public WebSocket and the perp market data
    (`dm.perps`: ticker / trades / book per PF_ product).
  - The two sorters (public / private).
  - The queues between sorters and containers.
  - The REST kline seed at boot.
  - The DataFrame periodic-snapshot logger (for verification).

Other modules consume `dm.book`, `dm.trades`, `dm.ticker`, `dm.klines`
directly — those are the public interface.
"""
import asyncio
from datetime import datetime, timezone

from exchange import ExchangeConnection, PrivateWebSocket, PublicWebSocket
from exchange.futures_public_ws import FuturesPublicWebSocket, fetch_futures_instruments
from logging_config import get_logger
from market_data.klines import KlineBuffer
from market_data.order_book import OrderBookBuffer
from market_data.perp_data import PerpMarketData
from market_data.private_sorter import PrivateMessageSorter
from market_data.public_sorter import PublicMessageSorter
from market_data.ticker import TickerBuffer
from market_data.trades import TradeBuffer


class DataManager:

    def __init__(self, symbols: list, exchange_connection: ExchangeConnection,
                 config: dict, raw_logger=None):
        """
        Args:
            symbols: trading pairs to subscribe to.
            exchange_connection: ExchangeConnection — provides the REST client.
            config: full config dict.
            raw_logger: optional RawWsLogger reference. If provided, the
                public sorter writes each message entry to it.
        """
        self.logger = get_logger('DataManager')
        self.symbols = [s.upper() for s in symbols]
        self.exchange_connection = exchange_connection
        self.rest_exchange = exchange_connection.rest_exchange
        if self.rest_exchange is None:
            raise ValueError("ExchangeConnection has no valid REST exchange")

        self.config = config
        md_cfg = config['market_data']
        self.kline_seed_interval = md_cfg['kline_seed_interval']
        self.kline_seed_lookback = int(md_cfg['kline_seed_lookback'])
        self.trade_buffer_maxlen = int(md_cfg['trade_buffer_maxlen'])
        self.ticker_buffer_maxlen = int(md_cfg['ticker_buffer_maxlen'])
        self.public_ping_health_max_age_seconds = float(
            md_cfg['public_ping_health_max_age_seconds']
        )
        self.private_ping_health_max_age_seconds = float(
            md_cfg['private_ping_health_max_age_seconds']
        )
        self.private_ping_interval_seconds = float(
            md_cfg['private_ping_interval_seconds']
        )
        self.ws_health_check_interval_seconds = float(
            md_cfg['ws_health_check_interval_seconds']
        )
        self.ws_unhealthy_exit_seconds = float(
            md_cfg['ws_unhealthy_exit_seconds']
        )
        self.dataframe_log_interval_seconds = float(
            md_cfg['dataframe_log_interval_seconds']
        )

        self.shutdown = False
        self.tasks: list = []

        # Containers (the DataFrames).
        self.book = OrderBookBuffer(self.symbols)
        self.trades = TradeBuffer(self.symbols, maxlen=self.trade_buffer_maxlen)
        self.ticker = TickerBuffer(self.symbols, maxlen=self.ticker_buffer_maxlen)
        self.klines = KlineBuffer(self.symbols)

        # Queues — one per public channel, two for private.
        self.book_queue: asyncio.Queue = asyncio.Queue()
        self.trade_queue: asyncio.Queue = asyncio.Queue()
        self.ticker_queue: asyncio.Queue = asyncio.Queue()
        self.executions_queue: asyncio.Queue = asyncio.Queue()
        self.command_response_queue: asyncio.Queue = asyncio.Queue()

        # Sorters.
        self.public_sorter = PublicMessageSorter(
            self.symbols,
            book_queue=self.book_queue,
            trade_queue=self.trade_queue,
            ticker_queue=self.ticker_queue,
            raw_logger=raw_logger,
        )
        self.private_sorter = PrivateMessageSorter(
            executions_queue=self.executions_queue,
            command_response_queue=self.command_response_queue,
            on_pong=None,
        )

        # WS wrappers (own their own connections + reconnect loops +
        # health watchdog).
        self.public_ws = PublicWebSocket(
            self.symbols,
            sorter=self.public_sorter,
            is_shutdown=lambda: self.shutdown,
            health_check_interval_seconds=self.ws_health_check_interval_seconds,
            unhealthy_exit_seconds=self.ws_unhealthy_exit_seconds,
            health_max_age_seconds=self.public_ping_health_max_age_seconds,
        )
        # Kraken Futures perps (public data only).
        perps_cfg = config['perps']
        self.perps = PerpMarketData(perps_cfg)
        self.perp_status_log_interval_seconds = float(
            perps_cfg['status_log_interval_seconds']
        )
        self.perp_instruments_timeout_seconds = float(
            perps_cfg['instruments_timeout_seconds']
        )
        if raw_logger is None:
            raise ValueError("DataManager requires the RawWsLogger for perp logging")
        raw_logger.add_symbols(self.perps.products)
        self.futures_ws = FuturesPublicWebSocket(
            self.perps,
            raw_logger=raw_logger,
            raw_log_channels=perps_cfg['raw_log_channels'],
            is_shutdown=lambda: self.shutdown,
            health_check_interval_seconds=self.ws_health_check_interval_seconds,
            unhealthy_exit_seconds=self.ws_unhealthy_exit_seconds,
            health_max_age_seconds=float(perps_cfg['health_max_age_seconds']),
        )
        self.private_ws = PrivateWebSocket(
            self.rest_exchange,
            sorter=self.private_sorter,
            command_response_queue=self.command_response_queue,
            is_shutdown=lambda: self.shutdown,
            health_check_interval_seconds=self.ws_health_check_interval_seconds,
            unhealthy_exit_seconds=self.ws_unhealthy_exit_seconds,
            health_max_age_seconds=self.private_ping_health_max_age_seconds,
        )

    # ── Startup ────────────────────────────────────────────────────────

    async def initialize(self) -> None:
        """REST-seed klines, obtain the private WS token, spawn all tasks."""
        self.logger.info("Initializing DataManager")

        await self._seed_klines_via_rest()
        await self.private_ws.obtain_token()
        # Fail loud if any configured perp is missing / not a perpetual.
        instruments = await fetch_futures_instruments(self.perp_instruments_timeout_seconds)
        self.perps.validate_instruments(instruments)

        spec = [
            (self.public_ws.run(), 'public_ws'),
            (self.public_ws.health_watchdog(), 'public_ws_health'),
            (self.private_ws.run(), 'private_ws'),
            (self.private_ws.run_pinger(self.private_ping_interval_seconds),
             'private_ws_pinger'),
            (self.private_ws.health_watchdog(), 'private_ws_health'),
            (self.book.consume(self.book_queue), 'book_consumer'),
            (self.trades.consume(self.trade_queue), 'trade_consumer'),
            (self.ticker.consume(self.ticker_queue), 'ticker_consumer'),
            (self._dataframe_logger(), 'dataframe_logger'),
            (self.futures_ws.run(), 'futures_ws'),
            (self.futures_ws.health_watchdog(), 'futures_ws_health'),
            (self._perp_status_logger(), 'perp_status_logger'),
        ]
        for coro, name in spec:
            t = asyncio.create_task(coro, name=name)
            t.add_done_callback(self._task_done_callback)
            self.tasks.append(t)

        self.logger.info(f"Started {len(self.tasks)} background tasks")

    def _task_done_callback(self, task: asyncio.Task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            self.logger.error(
                f"Background task {task.get_name()} crashed: {exc}",
                exc_info=exc,
            )

    async def _seed_klines_via_rest(self) -> None:
        """One-shot REST seed for each symbol."""
        interval = self.kline_seed_interval
        lookback = self.kline_seed_lookback
        self.logger.info(
            f"Seeding klines via REST: interval={interval} lookback={lookback}"
        )
        for symbol in self.symbols:
            try:
                rows = await self.rest_exchange.fetch_ohlcv(
                    symbol, interval, limit=lookback,
                )
                self.klines.seed_from_rest(symbol, rows)
                self.logger.info(f"Seeded {symbol} klines: {len(rows)} rows")
            except Exception as e:
                self.logger.error(
                    f"Failed to seed klines for {symbol}: {e}", exc_info=True,
                )

    async def _dataframe_logger(self) -> None:
        """Periodic snapshot — emits each symbol's four DataFrames via the
        DataManager logger. Cadence is `dataframe_log_interval_seconds`.
        Set `DataManager` to `"OFF"` in config.logging.module_levels to
        silence."""
        while not self.shutdown:
            await asyncio.sleep(self.dataframe_log_interval_seconds)
            for symbol in self.symbols:
                self.logger.info(f"{symbol} book:\n{self.book.get(symbol)}")
                self.logger.info(f"{symbol} trades:\n{self.trades.get(symbol)}")
                self.logger.info(f"{symbol} ticker:\n{self.ticker.get(symbol)}")
                self.logger.info(f"{symbol} klines:\n{self.klines.get(symbol)}")

    async def _perp_status_logger(self) -> None:
        """One line per perp every `perps.status_log_interval_seconds`:
        mark vs spot mid (basis), funding, book depth, last trade age."""
        log = get_logger('PerpStatus')
        while not self.shutdown:
            await asyncio.sleep(self.perp_status_log_interval_seconds)
            now = datetime.now(timezone.utc)
            for product in self.perps.products:
                spot = self.perps.product_to_spot[product]
                try:
                    t = self.perps.latest_ticker(product)
                    book = self.perps.books[product]
                    trades = self.perps.trades[product]
                    spot_ticker = self.ticker.get(spot)
                    if spot_ticker.empty:
                        raise RuntimeError(f"no spot ticker for {spot}")
                    last = spot_ticker.iloc[-1]
                    spot_mid = (float(last['bid']) + float(last['ask'])) / 2.0
                    basis = (t['markPrice'] / spot_mid - 1) * 100
                    # relative_funding_rate: fraction of notional per hour.
                    fund_hr = t['relative_funding_rate'] * 100
                    fund_ann = fund_hr * 24 * 365
                    trade_age = (now - trades[-1]['time']).total_seconds() if trades else None
                    log.info(
                        f"[PERP] {product} ({spot}) mark={t['markPrice']:.6g} "
                        f"spot_mid={spot_mid:.6g} basis={basis:+.3f}% "
                        f"funding={fund_hr:+.5f}%/h ({fund_ann:+.1f}%/yr) "
                        f"bid/ask={t['bid']}/{t['ask']} "
                        f"book={'ok' if book.valid else 'INVALID'} "
                        f"({len(book.bids)}b/{len(book.asks)}a) "
                        f"oi={t['openInterest']} "
                        f"last_trade_age={'none' if trade_age is None else f'{trade_age:.0f}s'}"
                    )
                except Exception as e:
                    log.warning(f"[PERP] {product} ({spot}) status unavailable: {e}")

    # ── Shutdown ───────────────────────────────────────────────────────

    async def close(self) -> None:
        self.shutdown = True
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.logger.info("DataManager closed")
