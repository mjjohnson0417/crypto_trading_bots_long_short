# docs/ — module map

One file per directory. Use this to decide what to keep and what to remove.

| File | Directory | What lives there |
|------|-----------|------------------|
| [top_level.md](top_level.md) | `./` | main.py, data_manager facade, logging, price math, config.json, run/watchdog scripts |
| [config.md](config.md) | `config/` | `config.json` loader |
| [credentials.md](credentials.md) | `credentials/` | GPG-encrypted API key loader |
| [detection.md](detection.md) | `detection/` | empty package (retired) |
| [exchange.md](exchange.md) | `exchange/` | REST + WS clients, order ops, fee cache |
| [execution.md](execution.md) | `execution/` | PositionLifecycle, StartupReconciler |
| [indicators.md](indicators.md) | `indicators/` | per-tick indicator calculators |
| [live_trading.md](live_trading.md) | `live_trading/` | runtime state files (trade_state.json, etc.) |
| [market_data.md](market_data.md) | `market_data/` | pure state containers — klines, book, ticker, trades |
| [orchestrator.md](orchestrator.md) | `orchestrator/` | trading_orchestrator, hybrid_grid_runner, shared_loop, exchange_sync, shutdown |
| [persistence.md](persistence.md) | `persistence/` | TradeTracker + raw-WS telemetry |
| [risk.md](risk.md) | `risk/` | halt gate, position sizing (both placeholders) |
| [scripts.md](scripts.md) | `scripts/` | cron jobs (raw-WS gzip) |
| [state_machine.md](state_machine.md) | `state_machine/` | bar-driven regime classifier + anchored VWAP (shadow only) |
| [strategy.md](strategy.md) | `strategy/` | hybrid_grid pure logic |
| [tests.md](tests.md) | `tests/` | pytest suite |
| [tools.md](tools.md) | `tools/` | operator one-shots |

The pre-existing [architecture.md](architecture.md) and [STRATEGY.md](STRATEGY.md) are unchanged.
