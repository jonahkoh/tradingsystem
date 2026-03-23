# tradingsystem

Lightweight crypto trading pipeline using Roostoo Mock Exchange, Redis Pub/Sub, and Python microservices. The system trades five assets (BTC, ETH, SOL, XRP, FET) with independent strategy engines per coin, a shared execution manager, and a real-time web dashboard.

## Architecture

Services run in one Docker network and communicate through Redis channels.

- Process A: Feed Handler (multi-pair ticker polling)
- Process B: Strategy Engines (one per asset, details below)
- Process C: Execution Manager (Roostoo signed orders)
- Process D: Ledger Service (durable CSV audit log)
- Process E: API Gateway (control + reporting + websocket stream)
- Process F: Sentiment Oracle (RSS + VADER scoring)

## Redis Channels

| Channel | Publisher | Subscriber(s) | Purpose |
|---|---|---|---|
| market_data | Feed Handler | Strategy Engines, API Gateway | Mid-price snapshots |
| sentiment_signals | Sentiment Oracle | Strategy Engines, API Gateway | NLP compound sentiment score |
| trade_signals | Strategy Engines | Execution Manager, API Gateway | BUY/SELL instructions |
| order_results | Execution Manager | Ledger, API Gateway | Execution outcomes |
| execution_confirmations | Execution Manager | Strategy Engines, API Gateway | Fill confirmations for state tracking |
| position_updates | Strategy Engines | API Gateway | Live position state for dashboard |
| control | API Gateway | Execution Manager | HALT/START control |

## Service Files

| Process | Main File | Asset |
|---|---|---|
| A | services/feed-handler/market_data.py | All (multi-pair) |
| B1 | services/strategy-engine/btc_eth_strategy_engine.py | BTC, ETH |
| B2 | services/strategy-engine/sol_strategy_engine.py | SOL |
| B3 | services/strategy-engine/xrp_strategy_engine.py | XRP |
| B4 | services/strategy-engine/fet_strategy_engine.py | FET |
| C | services/execution-manager/execution_service.py | All |
| D | services/ledger/ledger_service.py | All |
| E | services/api-gateway/api_gateway.py | All |
| F | services/sentiment-oracle/sentiment_service.py | All |

## Trading Strategies

Each asset runs a dedicated strategy engine tailored to its market microstructure. All strategies incorporate sentiment from the Sentiment Oracle and publish signals to the shared execution manager.

### BTC/ETH — Sentiment-Adjusted Bollinger Band Mean Reversion

Targets range-bound price action around a statistical mean.

- **Entry**: Price falls below the lower Bollinger Band (SMA120 - k*σ) while a 480-period trend filter confirms an uptrend (price > trend SMA). The Bollinger multiplier k adjusts dynamically by sentiment: 1.45σ in hype (aggressive entry), 2.95σ in severe fear (conservative).
- **Exit**: Partial sell at SMA (50%), remainder at upper band. ATR-based stop-loss with 1-hour cooldown after trigger.
- **Position sizing**: Volatility-scaled notional (2% of portfolio per trade) multiplied by a sentiment scalar (0.5x in fear, 1.15x in hype) and inverse volatility scalar.
- **Risk rationale**: Mean reversion captures the bid-ask spread in ranging markets. The trend filter avoids catching falling knives. ATR stop-loss caps per-trade drawdown for Calmar ratio preservation. Sentiment gating reduces exposure during high-fear regimes, improving Sortino ratio by cutting left-tail trades.

### SOL — Adaptive Multi-Regime Strategy

SOL exhibits distinct market regimes (trending, ranging, declining). A single fixed strategy underperforms across all conditions, so this engine detects the current regime and applies the optimal approach.

- **Regime detection**: 2-of-3 majority vote from ATR% (ATR/price), Bollinger Band width, and price deviation from SMA50. A 120-tick dwell filter prevents whipsawing between regimes.
- **RANGING**: Bollinger Band mean reversion (buy at lower band, sell at SMA20).
- **TRENDING_UP**: ATR breakout momentum (buy when single-tick delta > 2x ATR, exit on EMA7/EMA99 death cross).
- **TRENDING_DOWN / UNCLEAR**: DCA tranches — up to 5 buys at 1.5% dip intervals, exit all at 2% above average cost.
- **Position sizing**: Volatility-targeted ($2,000 risk per ATR unit) with a rolling 10-trade Sharpe scalar (0.3x to 1.5x). Winning streaks scale up, losing streaks scale down. Sentiment scalar further adjusts (0.5x in severe fear to 1.2x in hype).
- **Risk rationale**: Multi-regime adaptation keeps the strategy active across all market conditions. The Sharpe scalar dynamically adjusts position size based on recent risk-adjusted returns. DCA tranching in downtrends reduces average cost for recovery, improving the Calmar ratio by lowering peak-to-trough drawdown relative to eventual recovery returns.

### XRP — ATR Breakout with Regime Gating

Designed for event-driven, high-beta price action.

- **Entry**: Single-tick price spike exceeds 2x ATR while regime is TRENDING_UP (20/60 SMA crossover with 30-tick dwell confirmation) and sentiment score > -0.10.
- **Exit**: 8% take-profit, ATR-based stop-loss (1.5x ATR below entry), or sentiment reversal exit (profitable + sentiment < -0.25).
- **Position sizing**: Fixed 15% portfolio allocation ($150k notional), integer quantity.
- **Risk rationale**: The triple gate (regime + ATR breakout + sentiment) ensures entries only in confirmed momentum environments, which is a high Sharpe approach — fewer trades but higher win rate. Tight ATR stop-loss limits per-trade loss for Calmar. The sentiment reversal exit protects profits during sudden headline-driven reversals.

### FET — ATR Volatility Breakout

Similar structure to XRP, tuned for FET's lower liquidity and higher volatility.

- **Entry**: Single-tick upside move exceeds 2x ATR with sentiment > -0.20.
- **Exit**: 1.5% take-profit, 2x ATR stop-loss, or sentiment reversal at -0.25 while profitable.
- **Position sizing**: 10% portfolio allocation ($100k notional), 1 decimal precision.
- **Risk rationale**: Tighter take-profit (1.5% vs 8% for XRP) reflects FET's tendency for shorter momentum bursts. The lower sentiment gate (-0.20 vs -0.10) allows more entries while still filtering extreme fear. Fast exits preserve Sortino ratio by cutting losers quickly.

### How Strategies Address the Judging Ratios

| Ratio | What It Measures | How Our Strategies Optimize |
|---|---|---|
| **Sharpe** | Return per unit of total risk | Volatility-scaled position sizing reduces exposure in high-vol environments. Rolling Sharpe scalar (SOL) dynamically shrinks positions during losing streaks and scales up during winning streaks. Sentiment gating filters out low-conviction trades. |
| **Sortino** | Return per unit of downside risk | ATR-based stop-losses cap maximum per-trade loss. Sentiment fear gates prevent entries during high-downside-risk regimes. Partial exits (BTC/ETH) lock in profits early, converting unrealized gains to realized before reversals. |
| **Calmar** | Return relative to max drawdown | DCA tranching (SOL) averages down during drawdowns for faster recovery. Cooldown periods after stop-losses prevent revenge trading that compounds drawdowns. Regime detection avoids counter-trend entries that cause the deepest losses. |

## Branch Strategy and Development Workflow

### Branch Layout

| Branch | Purpose | Exchange |
|---|---|---|
| `main` | Production deployment | Roostoo |
| `dev/binance` | Strategy development and testing | Binance Spot Testnet |
| `staging/roostoo` | Pre-production integration testing | Roostoo |

### Deploying a New Strategy

1. **Develop on `dev/binance`**: Write the strategy engine in `services/strategy-engine/`. The Binance Spot Testnet provides free, unlimited test trades so iteration is zero-cost. Add the strategy as a new service in `docker-compose.yml` using the shared strategy Dockerfile with a `STRATEGY_FILE` env var override.

2. **Validate with live testnet data**: Run `docker compose up -d` on `dev/binance`. Monitor via the dashboard at `localhost:8000` and strategy logs (`docker compose logs -f <service>`). Verify that the strategy generates signals, execution succeeds, and the ledger records trades correctly.

3. **Unit test**: Add tests in `tests/` covering signal generation, position sizing, and edge cases. Run `python -m unittest discover tests/ -v`. CI runs automatically on push.

4. **PR to `staging/roostoo`**: Once validated on Binance testnet, open a PR from `dev/binance` to `staging/roostoo`. Set `EXCHANGE=roostoo` in `.env` and verify the strategy works against the Roostoo mock API (different response format, HMAC auth, pair naming).

5. **PR to `main`**: After staging validation, PR from `staging/roostoo` to `main` for production deployment. CI runs unit tests, smoke tests, and a Docker build check.

### Adding a Strategy File

1. Create `services/strategy-engine/<coin>_strategy_engine.py` following the existing pattern:
   - Subscribe to `market_data` and `execution_confirmations` channels
   - Filter ticks by `TRADE_SYMBOL`
   - Publish signals to `trade_signals` channel
   - Push position state to Redis (`strategy_state:<SYMBOL>`) and `position_updates` channel
2. Add a `COPY` line to `services/strategy-engine/Dockerfile`
3. Add a service block to `docker-compose.yml` with `STRATEGY_FILE` and `TRADE_SYMBOL` env vars
4. Add corresponding tests in `tests/`

## Quick Start

### Prerequisites

- Docker Desktop
- Python 3.11+ (optional for local test runs)

### 1) Configure env

Copy .env.example to .env and fill in credentials:

- API_KEY
- SECRET_KEY
- ROOSTOO_BASE_URL (default https://mock-api.roostoo.com)

### 2) Build and run

```bash
docker compose up -d --build
```

### 3) Basic health checks

```bash
docker compose ps
docker compose logs --tail=80 api-gateway
docker compose logs --tail=80 sentiment-oracle
```

## Data Persistence

Ledger writes a durable CSV audit file to data/logs/execution_log.csv.

## API Gateway

Base URL: http://localhost:8000

### HTTP Endpoints

- GET /health
- POST /api/kill — halt all execution
- POST /api/resume — resume execution
- GET /api/balance — account balances
- GET /api/trades — recent trade log
- GET /api/positions — per-coin position state
- GET /api/pnl — realized PnL summary

### WebSocket

- WS /ws/stream

Streams live events from: market_data, trade_signals, sentiment_signals, order_results, execution_confirmations, position_updates.

## Notes

- Use 127.0.0.1 for local websocket testing if localhost is unstable in your client.
- If port 6379 is already in use, stop the conflicting Redis container before starting this stack.
