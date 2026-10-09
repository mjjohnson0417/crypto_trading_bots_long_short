"""state_machine — market state classification.

This package owns the state vocabulary and the classifier. It does NOT
compute indicators (those live in `indicators/calculator.py`). The
classifier reads pre-computed indicator values and returns a state.
"""
from state_machine.regime_classifier import (
    RegimeClassifier,
    STATE_SIDEWAYS,
    STATE_STRONG_UPTREND,
    STATE_STRONG_DOWNTREND,
    STATE_UPTREND,
    STATE_DOWNTREND,
)

__all__ = [
    'RegimeClassifier',
    'STATE_SIDEWAYS',
    'STATE_STRONG_UPTREND',
    'STATE_STRONG_DOWNTREND',
    'STATE_UPTREND',
    'STATE_DOWNTREND',
]
