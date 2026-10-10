"""Kraken Futures public WebSocket (wss://futures.kraken.com/ws/v1).

Connect → subscribe ticker / trade / book / heartbeat for the configured
perp products → hand each message to PerpMarketData (and the raw_ws
logger) → reconnect with exponential backoff on close.

A book sequence gap sets `perp_data.resync_needed`; the read loop then
closes the socket so the reconnect delivers fresh snapshots.

Health: same policy as the spot PublicWebSocket — if no message arrives
for `unhealthy_exit_seconds` while the local network is up, the process
exits for systemd to restart.
"""
import asyncio
import json
import sys
import urllib.request
from datetime import datetime, timezone

import websockets

from exchange.public_ws import _local_network_up
from logging_config import get_logger


INSTRUMENTS_URL = "https://futures.kraken.com/derivatives/api/v3/instruments"


async def fetch_futures_instruments(timeout_seconds: float) -> dict:
    """{product_id: instrument} from the public Kraken Futures REST API.
    Raises on any failure."""
    def _get():
        with urllib.request.urlopen(INSTRUMENTS_URL, timeout=timeout_seconds) as r:
            return json.load(r)
    body = await asyncio.to_thread(_get)
    if body['result'] != 'success':
        raise RuntimeError(f"Kraken Futures instruments request failed: {body!r}")
    return {i['symbol']: i for i in body['instruments']}


class FuturesPublicWebSocket:

    URL = "wss://futures.kraken.com/ws/v1"
    FEEDS = ('ticker', 'trade', 'book', 'heartbeat')
    # Raw-log channel name per data feed (snapshots share their feed's channel).
    _RAW_CHANNEL = {
        'ticker': 'perp_ticker',
        'trade': 'perp_trade', 'trade_snapshot': 'perp_trade',
        'book': 'perp_book', 'book_snapshot': 'perp_book',
    }

    def __init__(self, perp_data, raw_logger, raw_log_channels, is_shutdown,
                 health_check_interval_seconds, unhealthy_exit_seconds,
                 health_max_age_seconds):
        self.logger = get_logger('FuturesPublicWebSocket')
        self.perp_data = perp_data
        self.raw_logger = raw_logger
        self.raw_log_channels = set(raw_log_channels)
        unknown = self.raw_log_channels - set(self._RAW_CHANNEL.values())
        if unknown:
            raise ValueError(f"perps.raw_log_channels has unknown entries {sorted(unknown)}")
        self.is_shutdown = is_shutdown
        self.health_check_interval_seconds = float(health_check_interval_seconds)
        self.unhealthy_exit_seconds = float(unhealthy_exit_seconds)
        self.health_max_age_seconds = float(health_max_age_seconds)
        self.last_message_time = datetime.now(timezone.utc)

    async def run(self) -> None:
        products = self.perp_data.products
        consecutive_failures = 0
        while not self.is_shutdown():
            try:
                if consecutive_failures > 0:
                    delay = min(30, 2 ** consecutive_failures)
                    self.logger.warning(
                        f"Futures WS reconnecting in {delay}s (attempt {consecutive_failures})..."
                    )
                    await asyncio.sleep(delay)

                async with websockets.connect(self.URL) as ws:
                    self.logger.info("Kraken Futures public WebSocket connected")
                    consecutive_failures = 0
                    self.perp_data.invalidate_all_books()
                    for feed in self.FEEDS:
                        await ws.send(json.dumps({
                            "event": "subscribe", "feed": feed, "product_ids": products,
                        }))
                    self.last_message_time = datetime.now(timezone.utc)

                    async for message in ws:
                        self.last_message_time = datetime.now(timezone.utc)
                        msg = json.loads(message)
                        if 'event' in msg:
                            self._handle_event(msg)
                            continue
                        feed = msg['feed']
                        if feed == 'heartbeat':
                            continue
                        self._raw_log(feed, msg)
                        try:
                            self.perp_data.apply(msg)
                        except Exception as e:
                            self.logger.error(f"perp apply failed ({feed}): {e}", exc_info=True)
                        if self.perp_data.resync_needed:
                            self.logger.warning("Futures book gap — reconnecting for fresh snapshots")
                            break
                if self.perp_data.resync_needed:
                    consecutive_failures = 1  # short backoff before resubscribing

            except websockets.ConnectionClosed as e:
                consecutive_failures += 1
                self.logger.warning(f"Futures WebSocket closed: {e}")
            except Exception as e:
                consecutive_failures += 1
                self.logger.error(f"Futures WebSocket error: {e}")

    def _handle_event(self, msg: dict) -> None:
        event = msg['event']
        if event == 'subscribed':
            # heartbeat is account-wide: its ack carries no product_ids.
            products = msg['product_ids'] if 'product_ids' in msg else ''
            self.logger.debug(f"Futures subscription confirmed: {msg['feed']} {products}")
        elif event == 'info':
            return
        elif event in ('error', 'alert', 'subscribed_failed'):
            self.logger.error(f"Futures WS {event}: {msg}")
        else:
            self.logger.info(f"Futures WS event: {msg}")

    def _raw_log(self, feed: str, msg: dict) -> None:
        channel = self._RAW_CHANNEL[feed]
        if channel not in self.raw_log_channels:
            return
        self.raw_logger.log(
            channel=channel,
            symbol=msg['product_id'],
            msg_type=feed,
            payload=msg,
        )

    def is_healthy(self, max_age_seconds: float) -> bool:
        return (datetime.now(timezone.utc) - self.last_message_time).total_seconds() < max_age_seconds

    async def health_watchdog(self) -> None:
        unhealthy_since = None
        while not self.is_shutdown():
            await asyncio.sleep(self.health_check_interval_seconds)
            now = datetime.now(timezone.utc)
            if self.is_healthy(self.health_max_age_seconds):
                if unhealthy_since is not None:
                    self.logger.info(
                        f"Futures WS recovered after "
                        f"{(now - unhealthy_since).total_seconds():.0f}s"
                    )
                unhealthy_since = None
                continue
            if unhealthy_since is None:
                unhealthy_since = now
                self.logger.warning("Futures WS unhealthy — exit countdown started")
                continue
            elapsed = (now - unhealthy_since).total_seconds()
            if elapsed < self.unhealthy_exit_seconds:
                continue
            if await _local_network_up():
                self.logger.error(
                    f"Futures WS unhealthy for {elapsed:.0f}s and local network "
                    f"is up. Exiting (systemd will restart)."
                )
                sys.exit(1)
            self.logger.warning(
                f"Futures WS unhealthy for {elapsed:.0f}s but local network "
                f"is down — staying up. Will re-check after another window."
            )
            unhealthy_since = now
