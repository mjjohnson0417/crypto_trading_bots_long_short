"""Public WS message sorter.

Receives the raw decoded WS message and pushes it onto the right queue
based on the `channel` field. Heartbeats, subscribe acks, and pongs are
dropped.

If a `raw_logger` is provided, each per-entry payload inside `data[]` is
written to the raw_ws archive before the message is queued.
"""
from logging_config import get_logger


class PublicMessageSorter:
    """Routes incoming public WS messages by `channel` into queues."""

    def __init__(self, symbols, book_queue, trade_queue, ticker_queue,
                 raw_logger=None):
        self.logger = get_logger('PublicMessageSorter')
        self.symbols = {s.upper() for s in symbols}
        self.book_queue = book_queue
        self.trade_queue = trade_queue
        self.ticker_queue = ticker_queue
        self.raw_logger = raw_logger

    def sort(self, msg: dict) -> None:
        channel = msg.get('channel')
        if channel not in ('book', 'trade', 'ticker'):
            return

        self._log(channel, msg)

        if channel == 'book':
            self.book_queue.put_nowait(msg)
        elif channel == 'trade':
            self.trade_queue.put_nowait(msg)
        elif channel == 'ticker':
            self.ticker_queue.put_nowait(msg)

    def _log(self, channel: str, msg: dict) -> None:
        if self.raw_logger is None:
            return
        msg_type = msg['type']
        for entry in msg['data']:
            symbol = entry['symbol']
            if symbol in self.symbols:
                self.raw_logger.log(
                    channel=channel,
                    symbol=symbol,
                    msg_type=msg_type,
                    payload=entry,
                )
