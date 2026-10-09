"""5-state regime classifier with hysteresis (per STRATEGY.md).

This module is the SOLE owner of:
- The state vocabulary (STATE_* constants).
- The raw classification rules (`_classify_raw`).
- The hysteresis state machine (`RegimeClassifier`).

Consumers (strategy/, orchestrator/) import from here; nothing else
defines or returns these state strings.

State priority order (first match wins in `_classify_raw`):
    1. SIDEWAYS         — width_pct_4h < 1.0
    2. STRONG_UPTREND   — sma_1h > sma_4h AND slope_1h_pct > +0.3
    3. STRONG_DOWNTREND — sma_1h < sma_4h AND slope_1h_pct < -0.3
    4. UPTREND          — sma_1h > sma_4h
    5. DOWNTREND        — sma_1h < sma_4h
    fallback            — SIDEWAYS
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from logging_config import get_logger


# State constants
STATE_SIDEWAYS = "SIDEWAYS"
STATE_STRONG_UPTREND = "STRONG_UPTREND"
STATE_STRONG_DOWNTREND = "STRONG_DOWNTREND"
STATE_UPTREND = "UPTREND"
STATE_DOWNTREND = "DOWNTREND"


class RegimeClassifier:
    """Stateful classifier with hysteresis.

    `update(indicators, now)` returns the confirmed state, applying a
    confirmation window before adopting a candidate transition.
    """

    def __init__(self, hysteresis_seconds: int):
        self.logger = get_logger('RegimeClassifier')
        self.hysteresis = timedelta(seconds=hysteresis_seconds)
        self._current_state: Optional[str] = None
        self._candidate_state: Optional[str] = None
        self._candidate_since: Optional[datetime] = None
        self._confirmed_at: Optional[datetime] = None

    def update(self, indicators: dict, now: datetime) -> str:
        new_state = self._classify_raw(indicators)

        if self._current_state is None:
            self._current_state = new_state
            self._confirmed_at = now
        elif new_state == self._current_state:
            self._candidate_state = None
            self._candidate_since = None
        else:
            if self._candidate_state != new_state:
                self._candidate_state = new_state
                self._candidate_since = now
            if (self._candidate_since
                    and (now - self._candidate_since) >= self.hysteresis):
                self._current_state = new_state
                self._confirmed_at = now
                self._candidate_state = None
                self._candidate_since = None

        self.logger.info(self._current_state)
        return self._current_state

    def _classify_raw(self, indicators: dict) -> str:
        """Pure classification without hysteresis.

        Indicator keys expected:
            - width_pct_4h  (price range over the trailing 4h, as % of mid)
            - sma_1h, sma_4h
            - slope_1h_pct  (change in sma_1h over last 15min, as %)
        """
        width_pct_4h = indicators.get("width_pct_4h")
        if width_pct_4h is not None and width_pct_4h < 1.0:
            return STATE_SIDEWAYS

        sma_1h = indicators.get("sma_1h")
        sma_4h = indicators.get("sma_4h")
        slope_1h_pct = indicators.get("slope_1h_pct")

        if sma_1h is None or sma_4h is None or slope_1h_pct is None:
            return STATE_SIDEWAYS

        if sma_1h > sma_4h and slope_1h_pct > 0.3:
            return STATE_STRONG_UPTREND
        if sma_1h < sma_4h and slope_1h_pct < -0.3:
            return STATE_STRONG_DOWNTREND
        if sma_1h > sma_4h:
            return STATE_UPTREND
        if sma_1h < sma_4h:
            return STATE_DOWNTREND
        return STATE_SIDEWAYS

    def reset(self) -> None:
        self._current_state = None
        self._candidate_state = None
        self._candidate_since = None
        self._confirmed_at = None
