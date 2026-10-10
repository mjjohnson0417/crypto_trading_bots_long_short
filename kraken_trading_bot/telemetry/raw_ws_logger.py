"""Raw WebSocket message logger.

Dumps every incoming WS message to a daily JSONL file, per channel, with
zero post-processing. No in-memory state, no sorting, no metric computation
on the hot path. Analysis is done offline by reading the JSONL files.

One file per (channel, symbol, UTC date):
  {output_path}/{channel}/{symbol_underscored}_{channel}_{YYYYMMDD}.jsonl

Each line is:
  {"ts": ISO8601, "channel": "book"|"trade"|"ticker", "type": <msg_type>,
   "symbol": <symbol>, "payload": <original WS payload object>}

Replay is straightforward: `for line in open(f): msg = json.loads(line)` —
feed `payload` back through `OrderBookState.apply_update()` to reconstruct
state at any point in time.
"""
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from logging_config import get_logger


class RawWsLogger:
    """Append-only JSONL logger for raw WebSocket messages."""

    def __init__(self, config: dict):
        self.logger = get_logger('RawWsLogger')
        self.enabled = config['enabled']
        self.log_path = Path(config['output_path'])
        self.log_symbols = {s.upper() for s in config['log_symbols']}

        # (channel, symbol, date) -> open file handle
        self._files: dict = {}

        if self.enabled:
            self.log_path.mkdir(parents=True, exist_ok=True)

    def add_symbols(self, symbols) -> None:
        """Allow additional symbols (e.g. perp product_ids) to be logged."""
        self.log_symbols |= {s.upper() for s in symbols}

    def log(self, channel: str, symbol: str, msg_type: str,
            payload: dict, timestamp: Optional[datetime] = None) -> None:
        """Append one raw-message line.

        No queue, no async — direct write to the buffered file handle. Writes
        are line-buffered so they're durable at sensible intervals without
        syscalls on every message.
        """
        if not self.enabled:
            return
        if symbol not in self.log_symbols:
            return

        ts = timestamp or datetime.now(timezone.utc)
        date_str = ts.strftime('%Y%m%d')
        key = (channel, symbol, date_str)
        fh = self._files.get(key)
        if fh is None or fh.closed:
            fh = self._open(channel, symbol, date_str)
        if fh is None:
            return  # couldn't open, logger will have logged the error

        try:
            line = json.dumps({
                'ts': ts.isoformat(),
                'channel': channel,
                'type': msg_type,
                'symbol': symbol,
                'payload': payload,
            }, default=self._fallback_serializer)
            fh.write(line + '\n')
        except Exception as e:
            self.logger.error(f"raw_ws write failed ({channel} {symbol}): {e}")

    def _open(self, channel: str, symbol: str, date_str: str):
        """Open a (channel, symbol, date) file for append."""
        # Close any previous file for this (channel, symbol) from an older date
        for (c, s, d), fh in list(self._files.items()):
            if c == channel and s == symbol and d != date_str:
                try:
                    fh.close()
                except Exception:
                    pass
                self._files.pop((c, s, d), None)

        ch_dir = self.log_path / channel
        ch_dir.mkdir(parents=True, exist_ok=True)
        sym_part = symbol.replace('/', '_').lower()
        fname = f"{sym_part}_{channel}_{date_str}.jsonl"
        path = ch_dir / fname
        try:
            fh = open(path, 'a', buffering=1)  # line-buffered
            self._files[(channel, symbol, date_str)] = fh
            return fh
        except Exception as e:
            self.logger.error(f"raw_ws open failed {path}: {e}")
            return None

    def _fallback_serializer(self, obj):
        """Serialize anything the default encoder can't handle."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        return str(obj)

    def close(self) -> None:
        for fh in self._files.values():
            try:
                fh.flush()
                fh.close()
            except Exception:
                pass
        self._files.clear()
