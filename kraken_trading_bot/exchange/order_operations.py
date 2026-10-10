"""Order operations — places, cancels, and queries orders on Kraken.

WebSocket for placements and cancels (fast, no nonce issues).
REST for balances, fees, and order queries.

No state tracking. No price calculations. No reconciliation.
"""
import math
from typing import Optional

from exchange.paper_exchange import PaperOrderError
from logging_config import get_logger

logger = get_logger('OrderOperations')


class InsufficientFundsError(Exception):
    """Raised when Kraken reports insufficient funds — the bot's view of
    the available balance disagrees with the exchange's."""


class OrderRejectedError(Exception):
    """Raised when Kraken rejects an order for a permanent reason (bad
    arguments, volume below the pair minimum, etc.). Retrying the
    identical order can never succeed; the caller must take a different
    path rather than loop."""


class OrderOperations:
    """Order management via Kraken WebSocket and REST API."""

    # Kraken error substrings that mean "this exact order will never work".
    _PERMANENT_REJECT_MARKERS = (
        'volume minimum not met',
        'Invalid arguments',
        'Invalid price',
        'Invalid volume',
        'Order minimum not met',
    )

    def __init__(self, exchange_connection, data_manager, dry_run, send_timeout_seconds,
                 paper_exchange):
        """
        Args:
            exchange_connection: ExchangeConnection instance (provides rest_exchange).
            data_manager: Provides `private_ws` (PrivateWebSocket) for
                order placement via `send_order` / `is_connected`.
            dry_run: When True, place_* and cancel_order log the intended
                action and return without touching the exchange. REST
                read methods are unaffected.
            send_timeout_seconds: How long to wait for Kraken's WS ack on
                each place/cancel. Required — no default.
            paper_exchange: PaperExchange when dry_run is True (required),
                None when live (required). In dry run every placement,
                cancel, order query and balance query goes to the paper
                account; market prices still come from Kraken.
        """
        self.exchange = exchange_connection
        self.dm = data_manager
        self.dry_run = bool(dry_run)
        self.send_timeout = float(send_timeout_seconds)
        if self.dry_run and paper_exchange is None:
            raise ValueError("dry_run requires a PaperExchange")
        if not self.dry_run and paper_exchange is not None:
            raise ValueError("PaperExchange given but dry_run is off")
        self.paper = paper_exchange
        if self.dry_run:
            logger.warning("OrderOperations DRY_RUN active — orders go to the PAPER account, not Kraken")

    def _paper_place(self, symbol, side, ord_type, quantity, price, cl_ord_id):
        """Dry-run placement with the same rounding and error contract as
        the live WS path: buys log and return None on rejection; sells
        raise InsufficientFundsError / OrderRejectedError."""
        qty = self._format_qty(quantity, symbol)
        px = self._format_price(price, symbol) if ord_type == 'limit' else None
        try:
            order_id = self.paper.place(symbol, side, ord_type, qty, px, cl_ord_id)
        except PaperOrderError as e:
            error = str(e)
            logger.error(f"[PAPER] {ord_type} {side} failed {symbol}: {error}")
            if side == 'sell':
                if 'Insufficient funds' in error:
                    raise InsufficientFundsError(f"{symbol}: {error}")
                if self._is_permanent_rejection(error):
                    raise OrderRejectedError(f"{symbol}: {error}")
            return None
        where = f" @ ${px:.6f}" if px is not None else ""
        logger.info(f"[PAPER] {ord_type} {side} placed: {order_id} {symbol} qty={qty:.6f}{where} cl_ord_id={cl_ord_id}")
        return order_id

    def _ws_available(self):
        return self.dm is not None and self.dm.private_ws.is_connected()

    def _get_tick(self, symbol: str) -> float:
        """Get tick size for a symbol from exchange market data."""
        markets = self.exchange.rest_exchange.markets
        if not markets:
            raise RuntimeError("ccxt markets not loaded")
        if symbol not in markets:
            raise KeyError(f"symbol {symbol!r} not in ccxt markets")
        return markets[symbol]['precision']['price']

    def _format_price(self, price: float, symbol: str) -> float:
        """Round quantity to exchange-accepted precision for a symbol."""
        tick = self._get_tick(symbol)
        if tick <= 0:
            raise ValueError(f"{symbol}: invalid price tick {tick!r}")
        tick_str = f"{tick:.10f}".rstrip('0')
        decimals = len(tick_str.split('.')[-1]) if '.' in tick_str else 0
        return round(math.floor(price / tick) * tick, decimals)

    def _format_qty(self, qty: float, symbol: str) -> float:
        """Round quantity to exchange-accepted precision for a symbol."""
        markets = self.exchange.rest_exchange.markets
        if not markets:
            raise RuntimeError("ccxt markets not loaded")
        if symbol not in markets:
            raise KeyError(f"symbol {symbol!r} not in ccxt markets")
        lot = markets[symbol]['precision']['amount']
        if lot <= 0:
            raise ValueError(f"{symbol}: invalid amount lot {lot!r}")
        lot_str = f"{lot:.10f}".rstrip('0')
        decimals = len(lot_str.split('.')[-1]) if '.' in lot_str else 0
        return round(math.floor(qty / lot) * lot, decimals)

    def _is_permanent_rejection(self, error) -> bool:
        s = str(error)
        return any(m in s for m in self._PERMANENT_REJECT_MARKERS)

    async def place_limit_buy(self, symbol: str, quantity: float, price: float, cl_ord_id: Optional[str] = None):
        """Place a limit buy via WebSocket. Returns order_id or None."""
        if self.dry_run:
            return self._paper_place(symbol, 'buy', 'limit', quantity, price, cl_ord_id)
        if self._ws_available():
            params = {
                "order_type": "limit",
                "side": "buy",
                "symbol": symbol,
                "limit_price": self._format_price(price, symbol),
                "order_qty": self._format_qty(quantity, symbol),
            }
            if cl_ord_id:
                params["cl_ord_id"] = cl_ord_id
            response = await self.dm.private_ws.send_order({
                "method": "add_order",
                "params": params
            }, self.send_timeout)
            if response and response.get('success'):
                order_id = response['result']['order_id']
                logger.info(f"WS limit buy placed: {order_id} {symbol} qty={quantity:.6f} @ ${price:.6f}")
                return order_id
            error = response.get('error', 'unknown') if response else 'no response'
            logger.error(f"WS limit buy failed {symbol}: {error}")
            return None

        logger.error(f"Cannot place limit buy {symbol}: private WS not connected")
        return None

    async def place_limit_sell(self, symbol: str, quantity: float, price: float, cl_ord_id: Optional[str] = None):
        """Place a limit sell via WebSocket. Returns order_id or None.
        Raises InsufficientFundsError or OrderRejectedError on permanent failures.
        """
        if self.dry_run:
            return self._paper_place(symbol, 'sell', 'limit', quantity, price, cl_ord_id)
        if self._ws_available():
            params = {
                "order_type": "limit",
                "side": "sell",
                "symbol": symbol,
                "limit_price": self._format_price(price, symbol),
                "order_qty": self._format_qty(quantity, symbol),
            }
            if cl_ord_id:
                params["cl_ord_id"] = cl_ord_id
            response = await self.dm.private_ws.send_order({
                "method": "add_order",
                "params": params
            }, self.send_timeout)
            if response and response.get('success'):
                order_id = response['result']['order_id']
                logger.info(f"WS limit sell placed: {order_id} {symbol} qty={quantity:.6f} @ ${price:.6f}")
                return order_id
            error = response.get('error', 'unknown') if response else 'no response'
            logger.error(f"WS limit sell failed {symbol}: {error}")
            if 'Insufficient funds' in str(error):
                raise InsufficientFundsError(f"{symbol}: {error}")
            if self._is_permanent_rejection(error):
                raise OrderRejectedError(f"{symbol}: {error}")
            return None

        logger.error(f"Cannot place limit sell {symbol}: private WS not connected")
        return None

    async def place_market_buy(self, symbol: str, quantity: float, cl_ord_id: Optional[str] = None):
        """Place a market buy via WebSocket. Returns order_id or None."""
        if self.dry_run:
            return self._paper_place(symbol, 'buy', 'market', quantity, None, cl_ord_id)
        if self._ws_available():
            params = {
                "order_type": "market",
                "side": "buy",
                "symbol": symbol,
                "order_qty": self._format_qty(quantity, symbol),
            }
            if cl_ord_id:
                params["cl_ord_id"] = cl_ord_id
            response = await self.dm.private_ws.send_order({
                "method": "add_order",
                "params": params
            }, self.send_timeout)
            if response and response.get('success'):
                order_id = response['result']['order_id']
                logger.info(f"WS market buy placed: {order_id} {symbol} qty={quantity:.6f}")
                return order_id
            error = response.get('error', 'unknown') if response else 'no response'
            logger.error(f"WS market buy failed {symbol}: {error}")
            return None

        logger.error(f"Cannot place market buy {symbol}: private WS not connected")
        return None

    async def place_market_sell(self, symbol: str, quantity: float, cl_ord_id: Optional[str] = None):
        """Place a market sell via WebSocket. Returns order_id or None.
        Raises InsufficientFundsError or OrderRejectedError on permanent failures
        (consistent with place_limit_sell).
        """
        if self.dry_run:
            return self._paper_place(symbol, 'sell', 'market', quantity, None, cl_ord_id)
        if self._ws_available():
            params = {
                "order_type": "market",
                "side": "sell",
                "symbol": symbol,
                "order_qty": self._format_qty(quantity, symbol),
            }
            if cl_ord_id:
                params["cl_ord_id"] = cl_ord_id
            response = await self.dm.private_ws.send_order({
                "method": "add_order",
                "params": params
            }, self.send_timeout)
            if response and response.get('success'):
                order_id = response['result']['order_id']
                logger.info(f"WS market sell placed: {order_id} {symbol} qty={quantity:.6f}")
                return order_id
            error = response.get('error', 'unknown') if response else 'no response'
            logger.error(f"WS market sell failed {symbol}: {error}")
            if 'Insufficient funds' in str(error):
                raise InsufficientFundsError(f"{symbol}: {error}")
            if self._is_permanent_rejection(error):
                raise OrderRejectedError(f"{symbol}: {error}")
            return None

        logger.error(f"Cannot place market sell {symbol}: private WS not connected")
        return None

    async def cancel_order(self, symbol: str, order_id: str) -> str:
        """Cancel an order via WebSocket.

        Returns:
            'success' — Kraken accepted the cancel. A WS cancel event will follow.
            'unknown' — Kraken returned "Unknown order"; already gone (filled or
                previously cancelled). No WS cancel event will arrive — callers
                relying on the event to update tracker state must do it themselves.
            'error'   — any other failure.
        """
        if self.dry_run:
            result = self.paper.cancel(order_id)
            logger.info(f"[PAPER] cancel {order_id} {symbol}: {result}")
            return result
        if self._ws_available():
            response = await self.dm.private_ws.send_order({
                "method": "cancel_order",
                "params": {
                    "order_id": [order_id],
                }
            }, self.send_timeout)
            if response and response.get('success'):
                logger.info(f"WS order canceled: {order_id} {symbol}")
                return 'success'
            error_str = str(response.get('error', 'unknown')) if response else 'no response'
            if 'Unknown order' in error_str:
                logger.info(f"WS cancel: order already gone {order_id} {symbol}")
                return 'unknown'
            logger.error(f"WS cancel failed {order_id} {symbol}: {error_str}")
            return 'error'

        logger.error(f"Cannot cancel order {order_id} {symbol}: private WS not connected")
        return 'error'

    # ── REST-only methods (no WebSocket equivalent) ──

    async def fetch_open_orders(self, symbol: str = None):
        """Fetch open orders (REST). symbol=None → all open orders account-wide."""
        if self.dry_run:
            return self.paper.fetch_open_orders(symbol)
        return await self.exchange.rest_exchange.fetch_open_orders(symbol)

    async def fetch_closed_orders(self, since_timestamp: float = None):
        """Fetch closed orders (REST). since_timestamp in ms."""
        if self.dry_run:
            return self.paper.fetch_closed_orders(since_timestamp)
        return await self.exchange.rest_exchange.fetch_closed_orders(
            since=int(since_timestamp) if since_timestamp else None)

    async def fetch_all_balances(self) -> dict:
        """Fetch all currency balances in one REST call. Returns {currency: total}."""
        if self.dry_run:
            balance = self.paper.balances()
        else:
            balance = await self.exchange.rest_exchange.fetch_balance()
        totals = balance['total']
        return {k: float(v) for k, v in totals.items() if v and float(v) > 0}

    async def fetch_all_usd_prices(self) -> dict:
        """Fetch USD prices for every tradable pair. Returns {base: usd_price}.

        One REST call (bulk fetch_tickers). Used by portfolio-value math so
        untraded holdings can be priced without subscribing them to the WS.
        """
        tickers = await self.exchange.rest_exchange.fetch_tickers()
        prices = {}
        for symbol, ticker in tickers.items():
            if not symbol.endswith('/USD'):
                continue
            base = symbol.split('/')[0]
            last = ticker.get('last') or ticker.get('close')
            if last and last > 0:
                prices[base] = float(last)
        return prices

    async def fetch_detailed_balances(self) -> dict:
        """Full ccxt balance response (includes 'free', 'used', 'total' per currency at top level).
        Exchange is the source of truth for holdings.
        """
        if self.dry_run:
            return self.paper.balances()
        balance = await self.exchange.rest_exchange.fetch_balance()
        return balance or {}

    async def get_free_balance(self, currency: str) -> float:
        """Return the free/available (sellable) balance for a currency (e.g. 'SOL').
        Falls back to 0 on any issue. Uses 'free' from the standard ccxt structure.
        """
        try:
            bal = await self.fetch_detailed_balances()
            free = bal.get('free') or {}
            val = free.get(currency)
            if val is None:
                # Some exchanges nest under the currency key
                cur_info = bal.get(currency) or {}
                if isinstance(cur_info, dict):
                    val = cur_info.get('free')
            return float(val or 0.0)
        except Exception:
            return 0.0
