"""Kraken REST connection via ccxt.

One responsibility: construct and close the ccxt REST client. The WebSocket
work is handled directly by `exchange/private_ws.py` and `exchange/public_ws.py`
using the `websockets` library — ccxt's WS client is not used.
"""
import asyncio

import ccxt.async_support as ccxt

from logging_config import get_logger

logger = get_logger('ExchangeConnection')


class ExchangeConnection:
    """Owns the ccxt REST client for Kraken."""

    def __init__(self, api_key: str, secret: str):
        if not api_key or not secret:
            raise ValueError("API key and secret required.")

        try:
            self.rest_exchange = ccxt.kraken({
                'apiKey': api_key,
                'secret': secret,
                'enableRateLimit': True,
                'options': {'defaultType': 'spot'},
            })
        except Exception as e:
            raise RuntimeError(f"Kraken REST client construction failed: {e}") from e

        logger.info("Kraken connection established.")

    async def check_rest_connection(self) -> None:
        """Verify REST works by fetching markets. Raises on failure."""
        try:
            async with asyncio.timeout(10):
                markets = await self.rest_exchange.fetch_markets()
        except asyncio.TimeoutError:
            raise RuntimeError("REST connection check timed out after 10 seconds")
        except Exception as e:
            raise RuntimeError(f"REST connection check failed: {e}") from e

        logger.debug("REST connection verified: fetched %d markets.", len(markets))

    async def close(self) -> None:
        try:
            await self.rest_exchange.close()
            logger.info("REST connection closed.")
            # aiohttp grace period after ClientSession.close(): the underlying
            # SSL transport needs time to flush its FIN before the connector's
            # __del__ runs cleanly. 0.25s was documented as enough but in
            # practice leaves "Unclosed connector" warnings; 1.0s is reliable.
            await asyncio.sleep(1.0)
        except Exception as e:
            logger.error("Error closing connection: %s", e, exc_info=True)
