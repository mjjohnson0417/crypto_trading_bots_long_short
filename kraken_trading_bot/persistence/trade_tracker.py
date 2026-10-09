"""Order-cycle dictionary.

The tracker holds active cycles in memory, mirrors them to
`order_cycles.json`, and writes completed cycles to
`order_cycles_history.json` when the runner calls `archive()`.

Cycle structure:
    cycles[buy_order_id] = {
        "symbol": str,
        "buy":  {
            "cl_ord_id":    str,
            "order_id":     str,
            "ord_type":     str,
            "order_qty":    float,
            "limit_price":  float,
            "timestamp":    str,
            "status":       str,
            "fills": [{"price", "qty", "fee", "timestamp"}, ...],
        },
        "sell": same shape as "buy", or None until the sell is placed.
    }
"""
import json
from pathlib import Path


class TradeTracker:

    def __init__(self, state_dir: str = 'live_trading'):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.state_dir / 'order_cycles.json'
        self.history_file = self.state_dir / 'order_cycles_history.json'
        self.cycles: dict = {}
        self.load()

    # ── Read ─────────────────────────────────────────────────────────────

    def load(self) -> None:
        if self.state_file.exists():
            with open(self.state_file) as f:
                self.cycles = json.load(f)
        else:
            self.cycles = {}

    def save(self) -> None:
        with open(self.state_file, 'w') as f:
            json.dump(self.cycles, f, indent=2)

    # ── Write — runner calls these as it processes executions events ─────

    def add_buy(self, buy_order_id: str, symbol: str, cl_ord_id: str,
                ord_type: str, order_qty: float, limit_price: float,
                timestamp: str, status: str) -> None:
        self.cycles[buy_order_id] = {
            "symbol": symbol,
            "buy": {
                "cl_ord_id": cl_ord_id,
                "order_id": buy_order_id,
                "ord_type": ord_type,
                "order_qty": float(order_qty),
                "limit_price": float(limit_price),
                "timestamp": timestamp,
                "status": status,
                "fills": [],
            },
            "sell": None,
        }
        self.save()

    def add_sell(self, parent_buy_order_id: str, sell_order_id: str,
                 cl_ord_id: str, ord_type: str, order_qty: float,
                 limit_price: float, timestamp: str, status: str) -> None:
        cycle = self.cycles.get(parent_buy_order_id)
        if cycle is None:
            return
        cycle['sell'] = {
            "cl_ord_id": cl_ord_id,
            "order_id": sell_order_id,
            "ord_type": ord_type,
            "order_qty": float(order_qty),
            "limit_price": float(limit_price),
            "timestamp": timestamp,
            "status": status,
            "fills": [],
        }
        self.save()

    def update_status(self, buy_order_id: str, side: str, status: str) -> None:
        cycle = self.cycles.get(buy_order_id)
        if cycle is None or cycle.get(side) is None:
            return
        cycle[side]['status'] = status
        self.save()

    def append_fill(self, buy_order_id: str, side: str,
                    price: float, qty: float, fee: float,
                    timestamp: str) -> None:
        cycle = self.cycles.get(buy_order_id)
        if cycle is None or cycle.get(side) is None:
            return
        cycle[side]['fills'].append({
            "price": float(price),
            "qty": float(qty),
            "fee": float(fee),
            "timestamp": timestamp,
        })
        self.save()

    def archive(self, buy_order_id: str) -> None:
        cycle = self.cycles.pop(buy_order_id, None)
        if cycle is None:
            return
        history = []
        if self.history_file.exists():
            with open(self.history_file) as f:
                history = json.load(f)
        history.append(cycle)
        with open(self.history_file, 'w') as f:
            json.dump(history, f, indent=2)
        self.save()
