"""
Price Calculator — single source for all price math.

Pure math. No state. All inputs provided by the caller.
"""


import math


def get_tick_size(price: float) -> float:
    """Determine tick size by inspecting the decimal precision of a price.

    Pass a recent bid or ask from the ticker channel. Kraken sends prices
    at the exact precision required for each pair, so the number of
    decimal places in the price gives the tick (10^-N).

    Returns 0 if `price` is missing or non-positive — calculate_tp_price
    treats tick=0 as "no rounding," which is the safe fallback during the
    startup window before ticker data has arrived.

    Edge case: if a ticker price happens to land on a round value (e.g.
    $0.4380 when the actual tick is 0.0001), this returns 0.001 instead
    of 0.0001. The over-rounded TP is still at a valid Kraken increment
    and is accepted; we just get a slightly coarser TP placement.
    """
    if not price or price <= 0:
        return 0.0
    s = f"{price:.10f}".rstrip('0').rstrip('.')
    if '.' not in s:
        return 1.0
    decimals = len(s.split('.')[1])
    return 10 ** -decimals


def calculate_tp_price(entry_price: float, entry_fee_usd: float, quantity: float,
                       maker_fee_pct: float, profit_pct: float,
                       tick_size: float = 0) -> float:
    """Calculate take profit price using actual entry fee from exchange.

    The sell side is a limit order (maker fee). The buy side fee is whatever
    the exchange actually charged (could be maker or taker).

    Args:
        entry_price: Fill price of the buy order
        entry_fee_usd: Actual fee charged by exchange on the buy (in USD)
        quantity: Number of tokens bought
        maker_fee_pct: Current maker fee for the sell side (e.g. 0.20)
        profit_pct: Desired profit margin above fees from config (e.g. 0.10)
        tick_size: Minimum price increment for this symbol (e.g. 0.1 for BTC)

    Returns:
        Limit sell price that nets profit_pct after actual buy fee + maker sell fee.
    """
    # entry_fee as percentage of trade value
    trade_value = entry_price * quantity
    if trade_value <= 0:
        return entry_price * 1.005  # fallback

    entry_fee_pct = (entry_fee_usd / trade_value) * 100
    tp_pct = entry_fee_pct + maker_fee_pct + profit_pct
    raw_tp = entry_price * (1 + tp_pct / 100)

    if tick_size > 0:
        return math.ceil(raw_tp / tick_size) * tick_size + tick_size

    return raw_tp


def calculate_portfolio_value(balances: dict, usd_prices: dict) -> float:
    """Calculate total portfolio value in USD across **all** holdings.

    Args:
        balances: `{currency: amount}` from `fetch_all_balances`.
        usd_prices: `{base_currency: usd_price}` from
            `OrderOperations.fetch_all_usd_prices`. Every non-USD
            balance must have an entry here to be counted; missing
            entries are silently dropped (these are typically tokens
            with no live USD pair on the exchange).

    Returns:
        Total USD value of every priced non-zero balance plus the raw
        USD balance. Untraded holdings (e.g. CC the user holds but the
        bot doesn't trade) are included in the total — sizing math
        elsewhere scales position size against this number.
    """
    # USD may legitimately be absent from `balances` (fetch_all_balances
    # filters to non-zero totals). Absence means zero USD on the books,
    # not a missing field — start from 0.0 in that case.
    total = float(balances['USD']) if 'USD' in balances else 0.0
    for currency, qty in balances.items():
        if currency == 'USD':
            continue
        qty_f = float(qty)
        if qty_f <= 0:
            continue
        price = usd_prices.get(currency)
        if price and price > 0:
            total += qty_f * price
    return total


def calculate_trade_size(usable_capital: float, pct_per_trade: float, capital_remaining: float) -> float:
    """Calculate dollar amount for a single trade.

    Args:
        usable_capital: Total capital available (balance * max_capital_pct / 100)
        pct_per_trade: Percentage of usable capital per trade from config (e.g. 0.5)
        capital_remaining: Usable capital minus currently deployed funds

    Returns:
        Dollar amount for this trade, or 0 if no capital available.
    """
    if capital_remaining <= 0:
        return 0.0
    return min(usable_capital * pct_per_trade / 100, capital_remaining)


def calculate_pnl(entry_price: float, exit_price: float, quantity: float,
                  entry_fee: float, exit_fee: float) -> float:
    """Calculate net PnL using actual fees reported by the exchange.

    Args:
        entry_price: Buy fill price from exchange
        exit_price: Sell fill price from exchange
        quantity: Number of tokens traded
        entry_fee: Actual fee charged by exchange on buy fill (USD)
        exit_fee: Actual fee charged by exchange on sell fill (USD)

    Returns:
        Net profit/loss in USD.
    """
    gross = (exit_price - entry_price) * quantity
    return gross - entry_fee - exit_fee
