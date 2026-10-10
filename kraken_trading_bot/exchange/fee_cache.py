"""Per-symbol maker/taker fee cache.

Populated at startup by fetching `fetch_trading_fee` for every symbol the
bot trades. Raises loudly on any fetch failure — the bot does not start
if the exchange refuses to disclose fees.

The rates are the account's current Kraken volume tier, which changes
as 30-day volume changes, so the runner calls `refresh()` periodically.
A refresh failure keeps the last known rates and logs an error.
"""
from logging_config import get_logger

logger = get_logger('FeeCache')


class FeeCache:
    """`{symbol: {'maker': pct, 'taker': pct}}` populated on startup."""

    def __init__(self, exchange_connection):
        self.rest = exchange_connection.rest_exchange
        self._fees = {}

    async def _fetch(self, symbol: str) -> dict:
        """Fetch one symbol's maker/taker in percent. Raises on failure."""
        try:
            fee = await self.rest.fetch_trading_fee(symbol)
        except Exception as e:
            raise RuntimeError(
                f"FeeCache: fetch_trading_fee failed for {symbol}: {e}"
            ) from e
        maker = fee.get('maker')
        taker = fee.get('taker')
        if maker is None or taker is None:
            raise RuntimeError(
                f"FeeCache: exchange returned incomplete fees for "
                f"{symbol}: {fee!r}"
            )
        return {'maker': float(maker) * 100.0, 'taker': float(taker) * 100.0}

    async def populate(self, symbols: list) -> None:
        """Fetch maker/taker for every symbol. Raises on any failure."""
        for symbol in symbols:
            self._fees[symbol] = await self._fetch(symbol)
            logger.info(
                f"{symbol} maker={self._fees[symbol]['maker']:.4f}% "
                f"taker={self._fees[symbol]['taker']:.4f}%"
            )

    async def refresh(self) -> None:
        """Re-fetch every cached symbol. Logs rate changes (tier moves).
        A failed symbol keeps its last known rates."""
        for symbol in list(self._fees):
            try:
                new = await self._fetch(symbol)
            except Exception as e:
                logger.error(f"{e} — keeping last known rates")
                continue
            old = self._fees[symbol]
            if new != old:
                logger.info(
                    f"{symbol} fee tier changed: maker {old['maker']:.4f}% → "
                    f"{new['maker']:.4f}%, taker {old['taker']:.4f}% → "
                    f"{new['taker']:.4f}%"
                )
            self._fees[symbol] = new

    def maker(self, symbol: str) -> float:
        if symbol not in self._fees:
            raise RuntimeError(
                f"FeeCache: no entry for {symbol} (cache not populated?)"
            )
        return self._fees[symbol]['maker']

    def taker(self, symbol: str) -> float:
        if symbol not in self._fees:
            raise RuntimeError(
                f"FeeCache: no entry for {symbol} (cache not populated?)"
            )
        return self._fees[symbol]['taker']
