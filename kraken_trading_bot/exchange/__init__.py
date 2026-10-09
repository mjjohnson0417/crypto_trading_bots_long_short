"""Exchange layer — adapters for Kraken REST and WebSocket APIs.

Contains external-world adapters only. No business logic, no strategy,
no state beyond connection/session details.
"""
from exchange.connection import ExchangeConnection
from exchange.order_operations import OrderOperations, InsufficientFundsError
from exchange.public_ws import PublicWebSocket
from exchange.private_ws import PrivateWebSocket

__all__ = [
    'ExchangeConnection',
    'OrderOperations',
    'InsufficientFundsError',
    'PublicWebSocket',
    'PrivateWebSocket',
]
