"""Strategy layer — pure entry-signal evaluators.

Each strategy is a pure function: (indicators, book, existing state,
config) → Optional[EntrySignal]. No I/O, no order placement, no tracker
updates.
"""
