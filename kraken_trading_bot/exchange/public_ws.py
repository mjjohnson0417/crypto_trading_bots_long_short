"""Kraken v2 public WebSocket — owns the socket lifecycle only.

Connect → subscribe → read messages → pass each to the sorter.
Reconnect with exponential backoff on close. No data parsing or
container updates happen here — that's the sorter + container layer.
"""
import asyncio
import json
import socket
import sys
from datetime import datetime, timezone

import websockets

from logging_config import get_logger


class PublicWebSocket:
    """Public ticker/book/trade subscription with auto-reconnect."""

    URL = "wss://ws.kraken.com/v2"

    def __init__(self, symbols: list, sorter, is_shutdown,
                 health_check_interval_seconds: float,
                 unhealthy_exit_seconds: float,
                 health_max_age_seconds: float):
        self.logger = get_logger('PublicWebSocket')
        self.symbols = [s.upper() for s in symbols]
        self.sorter = sorter
        self.is_shutdown = is_shutdown
        self.health_check_interval_seconds = health_check_interval_seconds
        self.unhealthy_exit_seconds = unhealthy_exit_seconds
        self.health_max_age_seconds = health_max_age_seconds

        self.subscription_confirmed = {
            s: {'ticker': False, 'book': False, 'trade': False}
            for s in self.symbols
        }
        # Start with "now" so a fresh instance doesn't immediately appear
        # unhealthy to the watchdog (avoiding the "unhealthy — exit countdown
        # started" warning on every boot). The run() loop will keep it fresh
        # once messages arrive.
        self.last_message_time = datetime.now(timezone.utc)

    async def run(self) -> None:
        """Connect loop: connect → subscribe → read messages → reconnect on close."""
        consecutive_failures = 0
        while not self.is_shutdown():
            try:
                if consecutive_failures > 0:
                    delay = min(30, 2 ** consecutive_failures)
                    self.logger.warning(
                        f"Public WS reconnecting in {delay}s (attempt {consecutive_failures})..."
                    )
                    await asyncio.sleep(delay)

                async with websockets.connect(self.URL) as ws:
                    self.logger.info("Kraken public WebSocket connected")
                    consecutive_failures = 0

                    for symbol in self.symbols:
                        self.subscription_confirmed[symbol] = {
                            'ticker': False, 'book': False, 'trade': False
                        }

                    await self._subscribe(ws)
                    # Mark as recently active on successful connect/subscribe so the
                    # health watchdog doesn't immediately log the "unhealthy — exit
                    # countdown started" warning on every boot.
                    self.last_message_time = datetime.now(timezone.utc)

                    async for message in ws:
                        self.last_message_time = datetime.now(timezone.utc)
                        try:
                            data = json.loads(message)
                        except Exception as e:
                            self.logger.error(f"Public WS JSON decode error: {e}")
                            continue

                        channel = data.get('channel')
                        method = data.get('method')

                        if channel == 'heartbeat' or method == 'pong':
                            continue
                        if method == 'subscribe':
                            self._handle_subscription_ack(data)
                            continue

                        try:
                            self.sorter.sort(data)
                        except Exception as e:
                            self.logger.error(
                                f"Public WS sort error: {e}", exc_info=True,
                            )

            except websockets.ConnectionClosed as e:
                consecutive_failures += 1
                self.logger.warning(f"Public WebSocket closed: {e}")
            except Exception as e:
                consecutive_failures += 1
                self.logger.error(f"Public WebSocket error: {e}")

    async def _subscribe(self, ws) -> None:
        await ws.send(json.dumps({
            "method": "subscribe",
            "params": {"channel": "ticker", "symbol": self.symbols, "snapshot": True}
        }))
        await ws.send(json.dumps({
            "method": "subscribe",
            "params": {
                "channel": "book", "symbol": self.symbols,
                "depth": 100, "snapshot": True
            }
        }))
        await ws.send(json.dumps({
            "method": "subscribe",
            "params": {"channel": "trade", "symbol": self.symbols, "snapshot": True}
        }))
        self.logger.info(f"Subscribed to ticker, book, trade for {self.symbols}")

    def _handle_subscription_ack(self, data: dict) -> None:
        if data.get('success', False):
            result = data['result']
            channel = result['channel']
            symbol = result['symbol']
            if symbol in self.subscription_confirmed:
                self.subscription_confirmed[symbol][channel] = True
            self.logger.info(f"Subscription confirmed: {channel} {symbol}")
        else:
            error = data.get('error', 'unknown error')
            self.logger.error(f"Subscription failed: {error}")

    def is_healthy(self, max_age_seconds: float) -> bool:
        """True if a message has been received within max_age_seconds."""
        if self.last_message_time is None:
            return False
        return (datetime.now(timezone.utc) - self.last_message_time).total_seconds() < max_age_seconds

    async def health_watchdog(self) -> None:
        """Periodic self-check. If the WS has been unhealthy for
        `unhealthy_exit_seconds` AND the local network is up, exit the
        process for systemd to restart. If the local network is down,
        log and keep retrying — restart wouldn't help."""
        unhealthy_since = None
        while not self.is_shutdown():
            await asyncio.sleep(self.health_check_interval_seconds)
            now = datetime.now(timezone.utc)
            if self.is_healthy(self.health_max_age_seconds):
                if unhealthy_since is not None:
                    self.logger.info(
                        f"Public WS recovered after "
                        f"{(now - unhealthy_since).total_seconds():.0f}s"
                    )
                unhealthy_since = None
                continue
            if unhealthy_since is None:
                unhealthy_since = now
                self.logger.warning("Public WS unhealthy — exit countdown started")
                continue
            elapsed = (now - unhealthy_since).total_seconds()
            if elapsed < self.unhealthy_exit_seconds:
                continue
            if await _local_network_up():
                self.logger.error(
                    f"Public WS unhealthy for {elapsed:.0f}s and local network "
                    f"is up. Exiting (systemd will restart)."
                )
                sys.exit(1)
            else:
                self.logger.warning(
                    f"Public WS unhealthy for {elapsed:.0f}s but local network "
                    f"is down — staying up. Will re-check after another window."
                )
                unhealthy_since = now  # reset to start a fresh countdown


async def _local_network_up() -> bool:
    """Quick DNS check against Kraken's API host. Returns True if the
    lookup resolves within 5 seconds, False otherwise."""
    try:
        await asyncio.wait_for(
            asyncio.to_thread(socket.gethostbyname, 'api.kraken.com'),
            timeout=5.0,
        )
        return True
    except Exception:
        return False
