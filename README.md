# tradingsystem

Lightweight crypto trading pipeline using Binance Spot Testnet, Redis Pub/Sub, and Python microservices.

## Architecture

```
Binance Testnet (WebSocket)
        │  btcusdt@bookTicker
        ▼
[A] Feed Handler          →  market_data     →  [B] Strategy Engine
                                                       │  trade_signals
                                                       ▼
[E] API Gateway / Kill Switch  →  control  →  [C] Execution Manager
                                                       │  order_results
                                                       ▼
                                               [D] Ledger & Risk Monitor
```

| Channel        | Publisher        | Subscriber(s)              |
|----------------|------------------|----------------------------|
| `market_data`  | Feed Handler     | Strategy Engine            |
| `trade_signals`| Strategy Engine  | Execution Manager          |
| `order_results`| Execution Manager| Ledger & Risk Monitor      |
| `control`      | API Gateway      | Execution Manager          |

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

### 2 — Start Redis
```bash
docker compose up -d
```

### 3 — Install Python dependencies
```bash
pip install -r requirements.txt
```

### 4 — Verify the stack is healthy
```bash
python test_setup.py
```
All 5 checks should print `[PASS]`.

### 5 — Run the Feed Handler
```bash
python feed-handler.py
```

## Services (to be added progressively)

| File                  | Process | Owner  | Status      |
|-----------------------|---------|--------|-------------|
| `feed-handler.py`     | A       | wenbao | ✅ Done     |
| `strategy-engine.py`  | B       | Keene  | 🔲 Pending  |
| `execution-manager.py`| C       | wenbao | 🔲 Pending  |
| `ledger.py`           | D       | Keene  | 🔲 Pending  |
| `api-gateway.py`      | E       | wenbao | 🔲 Pending  |

## Notes
- All trading is on **Binance Spot Testnet** — no real funds are used.
- Never commit your `.env` file. It is listed in `.gitignore`.
- To stop Redis: `docker compose down`
