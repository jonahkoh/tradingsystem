# tradingsystem

Lightweight crypto trading pipeline using Roostoo Mock Exchange, Redis Pub/Sub, and Python microservices.

## Architecture

Services run in one Docker network and communicate through Redis channels.

- Process A: Feed Handler
- Process B: Strategy Engine (Sentiment-Adjusted Mean Reversion)
- Process C: Execution Manager (Roostoo signed orders)
- Process D: Ledger Service (durable CSV audit log)
- Process E: API Gateway (control + reporting + websocket stream)
- Process F: Sentiment Oracle (RSS + VADER scoring)

## Redis Channels

| Channel | Publisher | Subscriber(s) | Purpose |
|---|---|---|---|
| market_data | Feed Handler | Strategy Engine, API Gateway | Mid-price snapshots |
| sentiment_signals | Sentiment Oracle | Strategy Engine, API Gateway | NLP compound sentiment score |
| trade_signals | Strategy Engine | Execution Manager, API Gateway | BUY/SELL instructions |
| order_results | Execution Manager | Ledger, API Gateway | Execution outcomes |
| control | API Gateway | Execution Manager | HALT/START control |

## Service Files

| Process | Main File |
|---|---|
| A | services/feed-handler/market_data.py |
| B | services/strategy-engine/strategy_engine.py |
| C | services/execution-manager/execution_service.py |
| D | services/ledger/ledger_service.py |
| E | services/api-gateway/api_gateway.py |
| F | services/sentiment-oracle/sentiment_service.py |

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

## API Gateway

Base URL: http://localhost:8000

### HTTP Endpoints

- GET /health
- POST /api/kill
- POST /api/resume
- GET /api/balance
- GET /api/trades

### WebSocket

- WS /ws/stream

Streams live events from:

- market_data
- trade_signals
- sentiment_signals

## Data Persistence

Ledger writes a durable CSV audit file to:

- data/logs/execution_log.csv

The active runtime no longer uses SQLite trades.db.

## Notes

- Use 127.0.0.1 for local websocket testing if localhost is unstable in your client.
- If port 6379 is already in use, stop the conflicting Redis container before starting this stack.
