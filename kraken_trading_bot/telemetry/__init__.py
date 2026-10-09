"""Telemetry sinks.

RawWsLogger dumps every WS message to daily JSONL files with zero
processing. Analysis is done offline by replaying the JSONL files.
"""
from telemetry.raw_ws_logger import RawWsLogger

__all__ = ['RawWsLogger']
