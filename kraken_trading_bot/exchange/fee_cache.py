"""Per-symbol maker/taker fee cache.

Populated at startup by fetching `fetch_trading_fee` for every symbol the
bot trades. Raises loudly on any fetch failure — the bot does not start
if the exchange refuses to disclose fees.
"""
from logging_config import get_logger

logger = get_logger('FeeCache')


class FeeCache:
    """`{symbol: {'maker': pct, 'taker': pct}}` populated on startup."""

    def __init__(self, exchange_connection):
        self.rest = exchange_connection.rest_exchange
        self._fees = {}

    async def populate(self, symbols: list) -> None:
        """Fetch maker/taker for every symbol. Raises on any failure."""
        for symbol in symbols:
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
            self._fees[symbol] = {
                'maker': float(maker) * 100.0,
                'taker': float(taker) * 100.0,
            }
            logger.info(
                f"{symbol} maker={self._fees[symbol]['maker']:.4f}% "
                f"taker={self._fees[symbol]['taker']:.4f}%"
            )

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
