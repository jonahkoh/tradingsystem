# tradingsystem

Lightweight crypto trading pipeline using Binance Spot Testnet, Redis Pub/Sub, and Python microservices.

## System Design

```
┌─────────────────────────────────────────────────────────────────────┐
│                         External Services                           │
│                                                                     │
│   Binance WebSocket                    Binance Testnet REST         │
│   stream.binance.com                   testnet.binance.vision       │
│   btcusdt@bookTicker                   /api/v3/order                │
│   (real production prices)             (paper trading)              │
└────────────┬───────────────────────────────────▲────────────────────┘
             │ WebSocket stream                   │ POST order
             ▼                                   │
┌─────────────────────────────────────────────────────────────────────┐
│                    Docker Network — tradingsystem_net               │
│                                                                     │
│  ┌─────────────────┐   market_data    ┌──────────────────────────┐ │
│  │  [A] Feed       │ ───────────────► │  [B] Strategy Engine     │ │
│  │  Handler        │                  │                          │ │
│  │                 │                  │  Rolling MA crossover    │ │
│  │  Throttles WS   │   market_data    │  Fast: 7 ticks           │ │
│  │  ticks → 1/s    │ ───────────────► │  Slow: 25 ticks          │ │
│  │  Publishes      │  (unrealized     │                          │ │
│  │  mid-price      │   PnL feed)      │  Emits BUY / SELL only   │ │
│  └─────────────────┘                  └────────────┬─────────────┘ │
│                                                    │ trade_signals  │
│                                                    ▼               │
│  ┌─────────────────┐   control        ┌──────────────────────────┐ │
│  │  [E] API        │ ───────────────► │  [C] Execution Manager   │ │
│  │  Gateway        │                  │                          │ │
│  │                 │                  │  Dynamic sizing (1% risk) │ │
│  │  FastAPI :8000  │                  │  Places MARKET orders    │ ├─┘
│  │                 │                  │  Handles HALT command    │
│  │  GET  /health   │                  └────────────┬─────────────┘
│  │  GET  /status   │                               │ order_results
│  │  GET  /pnl      │                               ▼
│  │  GET  /trades   │                  ┌──────────────────────────┐
│  │  POST /kill     │                  │  [D] Ledger & Risk       │
│  │                 │                  │  Monitor                 │
│  └────────┬────────┘                  │                          │
│           │                           │  Tracks position + PnL   │
│           │  reads                    │  Weighted avg entry      │
│           │◄──────────────────────────┤  Realized + unrealized   │
│           │                           │  Restores state on boot  │
│           │                           └────────────┬─────────────┘
│           │                                        │ writes
│           │                                        ▼
│           │                           ┌──────────────────────────┐
│           └──────────────────────────►│  ./data/  (host volume)  │
│                      reads            │                          │
│                                       │  trades.db  (SQLite)     │
│                                       │  trades.log (flat file)  │
│                                       └──────────────────────────┘
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  Redis (Pub/Sub broker)                                     │   │
│  │                                                             │   │
│  │  market_data   · trade_signals   · order_results · control  │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                              ▲
                              │  curl / Postman / Browser
                          👤 Operator
```

### Redis channels

| Channel          | Publisher          | Subscriber(s)          | Payload                          |
|------------------|--------------------|------------------------|----------------------------------|
| `market_data`    | Feed Handler       | Strategy Engine, Ledger | `{symbol, mid_price, timestamp}` |
| `trade_signals`  | Strategy Engine    | Execution Manager      | `{action, symbol, type}`         |
| `order_results`  | Execution Manager  | Ledger                 | `{order_id, side, price, qty, result}` |
| `control`        | API Gateway        | Execution Manager      | `{command: "HALT"}`              |

### Services

| File                 | Process | Role                                              |
|----------------------|---------|---------------------------------------------------|
| `feed-handler.py`    | A       | Streams live BTC prices from Binance WebSocket    |
| `strategy-engine.py` | B       | MA crossover signals (7 vs 25 window)             |
| `execution.py`       | C       | Places orders on Binance Testnet, enforces kill switch |
| `ledger.py`          | D       | Tracks position, PnL, persists fills to SQLite    |
| `api_gateway.py`     | E       | HTTP control plane — kill switch + PnL endpoints  |

### External dependencies

| Service                       | Used by            | Purpose                         |
|-------------------------------|--------------------|---------------------------------|
| Binance WebSocket (prod)      | Feed Handler       | Real-time BTC order book feed   |
| Binance Testnet REST          | Execution Manager  | Paper trading order execution   |

---

## Quick Start

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Python 3.11+

### 1 — Clone & configure
```bash
git clone https://github.com/jonahkoh/tradingsystem.git
cd tradingsystem
cp .env.example .env
# Edit .env and paste your Binance Testnet API key + secret
# Get keys at: https://testnet.binance.vision/
```

### 2 — Start all services
```bash
docker compose up -d
```

### 3 — Verify the stack is healthy
```bash
python test_setup.py
```
All 5 checks should print `[PASS]`.

---

## API Endpoints

| Method | Endpoint   | Description                              |
|--------|------------|------------------------------------------|
| GET    | `/health`  | Liveness probe                           |
| GET    | `/status`  | Kill switch state this session           |
| GET    | `/pnl`     | Current PnL summary from SQLite          |
| GET    | `/trades`  | Recent fills (`?limit=N`, default 100)   |
| POST   | `/kill`    | Emergency halt — cancels all open orders |

Interactive docs available at `http://localhost:8000/docs`.

---

## Notes
- All trading is on **Binance Spot Testnet** — no real funds are used.
- Never commit your `.env` file. It is listed in `.gitignore`.
- PnL state persists across restarts via `./data/trades.db`.
- To stop all services: `docker compose down`
