"""Kraken v2 private WebSocket — owns the socket lifecycle and outbound
order sends. Inbound dispatch (executions, command responses) goes
through the private sorter.

Heartbeat / pong handling stays here — they're connection-level
concerns (pong updates `last_pong_ts` for `is_healthy()`), not data.
"""
import asyncio
import json
import socket
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import websockets

from logging_config import get_logger


class PrivateWebSocket:
    """Authenticated Kraken v2 private WS with auto-reconnect and pinger."""

    URL = "wss://ws-auth.kraken.com/v2"

    def __init__(self, rest_exchange, sorter, command_response_queue,
                 is_shutdown,
                 health_check_interval_seconds: float,
                 unhealthy_exit_seconds: float,
                 health_max_age_seconds: float):
        self.logger = get_logger('PrivateWebSocket')
        self.rest_exchange = rest_exchange
        self.sorter = sorter
        self.command_response_queue = command_response_queue
        self.is_shutdown = is_shutdown
        self.health_check_interval_seconds = health_check_interval_seconds
        self.unhealthy_exit_seconds = unhealthy_exit_seconds
        self.health_max_age_seconds = health_max_age_seconds

        self.token: Optional[str] = None
        self._ws = None

        # Start with "now" so a fresh instance doesn't immediately appear
        # ancient to the health watchdog (which would log the scary
        # "unhealthy — exit countdown started" warning on every boot).
        self.last_pong_ts = time.time()
        self._ping_req_id = 1_000_000

        self.reconnect_event = asyncio.Event()

    async def obtain_token(self) -> bool:
        """Fetch the auth token via REST. Returns True on success."""
        try:
            response = await self.rest_exchange.private_post_getwebsocketstoken()
            self.token = response['result']['token']
            if self.token:
                self.logger.info("Private WebSocket token obtained")
                return True
            self.logger.warning("Could not get private WebSocket token")
            return False
        except Exception as e:
            self.logger.warning(f"Failed to get private WebSocket token: {e}")
            return False

    async def run(self) -> None:
        """Connect loop with exponential backoff and token refresh per attempt."""
        if not self.token:
            self.logger.warning("No private WebSocket token — skipping private WS")
            return

        consecutive_failures = 0
        while not self.is_shutdown():
            try:
                if consecutive_failures > 0:
                    delay = min(30, 2 ** consecutive_failures)
                    self.logger.warning(
                        f"Private WS reconnecting in {delay}s "
                        f"(attempt {consecutive_failures})..."
                    )
                    await asyncio.sleep(delay)
                    if not await self.obtain_token():
                        consecutive_failures += 1
                        continue

                async with websockets.connect(self.URL) as ws:
                    self.logger.info("Private WebSocket connected")
                    self._ws = ws
                    consecutive_failures = 0

                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "params": {
                            "channel": "executions",
                            "token": self.token,
                            "snap_orders": True,
                            "snap_trades": False,
                        }
                    }))
                    self.last_pong_ts = time.time()
                    self.reconnect_event.set()

                    async for message in ws:
                        try:
                            data = json.loads(message)
                        except Exception as e:
                            self.logger.error(f"Private WS JSON decode error: {e}")
                            continue

                        # Kraken WS v2 private messages carry EITHER 'channel'
                        # OR 'method', not both — .get() without a default is
                        # the correct semantic here.
                        if data.get('channel') == 'heartbeat':
                            continue
                        if data.get('method') == 'pong':
                            self.last_pong_ts = time.time()
                            continue

                        try:
                            self.sorter.sort(data)
                        except Exception as e:
                            self.logger.error(
                                f"Private WS sort error: {e}", exc_info=True,
                            )

                    self._ws = None

            except websockets.ConnectionClosed as e:
                self._ws = None
                consecutive_failures += 1
                self.logger.warning(f"Private WebSocket closed: {e}")
            except Exception as e:
                self._ws = None
                consecutive_failures += 1
                self.logger.error(f"Private WebSocket error: {e}")

    async def run_pinger(self, interval: float) -> None:
        """Send a `ping` every `interval` seconds. Pongs are routed back
        through the message loop and update `last_pong_ts`."""
        while not self.is_shutdown():
            try:
                if self._ws is not None:
                    self._ping_req_id += 1
                    try:
                        await self._ws.send(json.dumps({
                            "method": "ping",
                            "req_id": self._ping_req_id,
                        }))
                    except Exception as e:
                        self.logger.warning(f"Private WS ping send failed: {e}")
            except Exception as e:
                self.logger.error(f"Private WS pinger error: {e}")
            await asyncio.sleep(interval)

    async def send_order(self, message: dict, timeout: float):
        """Send an order message and wait for the matching response."""
        if self._ws is None:
            self.logger.error("Private WebSocket not connected — cannot send order")
            return None

        if 'params' not in message:
            message['params'] = {}
        message['params']['token'] = self.token

        try:
            while not self.command_response_queue.empty():
                self.command_response_queue.get_nowait()
            await self._ws.send(json.dumps(message))
            return await asyncio.wait_for(
                self.command_response_queue.get(), timeout=timeout,
            )
        except asyncio.TimeoutError:
            self.logger.error("WebSocket order response timed out")
            return None
        except Exception as e:
            self.logger.error(f"WebSocket send_order error: {e}")
            return None

    def is_connected(self) -> bool:
        return self._ws is not None

    def is_healthy(self, max_age_seconds: float) -> bool:
        if self._ws is None:
            return False
        return (time.time() - self.last_pong_ts) < max_age_seconds

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
                        f"Private WS recovered after "
                        f"{(now - unhealthy_since).total_seconds():.0f}s"
                    )
                unhealthy_since = None
                continue
            if unhealthy_since is None:
                unhealthy_since = now
                self.logger.warning("Private WS unhealthy — exit countdown started")
                continue
            elapsed = (now - unhealthy_since).total_seconds()
            if elapsed < self.unhealthy_exit_seconds:
                continue
            if await _local_network_up():
                self.logger.error(
                    f"Private WS unhealthy for {elapsed:.0f}s and local "
                    f"network is up. Exiting (systemd will restart)."
                )
                sys.exit(1)
            else:
                self.logger.warning(
                    f"Private WS unhealthy for {elapsed:.0f}s but local "
                    f"network is down — staying up. Will re-check after another window."
                )
                unhealthy_since = now


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
