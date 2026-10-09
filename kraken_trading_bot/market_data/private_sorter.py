"""Private WS message sorter.

Routes incoming private WS messages:
    channel == 'executions'                          → executions_queue
    method in {add_order, cancel_order, cancel_all}  → command_response_queue
    method == 'pong'                                 → on_pong callback (pinger)
    channel == 'heartbeat'                           → dropped
    everything else (subscribe ack, etc.)            → dropped
"""
from typing import Callable, Optional

from logging_config import get_logger


class PrivateMessageSorter:
    """Routes incoming private WS messages by `channel` / `method` into queues."""

    _COMMAND_METHODS = ('add_order', 'cancel_order', 'cancel_all')

    def __init__(self, executions_queue, command_response_queue,
                 on_pong: Optional[Callable[[dict], None]] = None):
        self.logger = get_logger('PrivateMessageSorter')
        self.executions_queue = executions_queue
        self.command_response_queue = command_response_queue
        self.on_pong = on_pong

    def sort(self, msg: dict) -> None:
        # Kraken WS v2 private messages carry EITHER 'channel' OR 'method',
        # not both — so .get() without a default (returns None) is the
        # correct semantic, not a fallback masking missing data.
        if msg.get('channel') == 'executions':
            self.executions_queue.put_nowait(msg)
            return

        method = msg.get('method')
        if method in self._COMMAND_METHODS:
            self.command_response_queue.put_nowait(msg)
            return

        if method == 'pong':
            if self.on_pong is not None:
                self.on_pong(msg)
            return

        # heartbeat, subscribe ack, anything else: drop.
