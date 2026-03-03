"""
Process A — Feed Handler
========================
Connects to Binance WebSocket (btcusdt@bookTicker) and publishes mid-prices.

Data source  : Real Binance production WebSocket by default (FEED_TESTNET=false)
               Switch to testnet stream by setting FEED_TESTNET=true in .env
Order routing: Execution Manager (Process C) — always targets Binance Testnet REST

Redis output channel : market_data
Payload schema       : {"symbol": "BTCUSDT", "mid_price": float,
                        "timestamp": int}
"""

import asyncio
import json
import os
import time

import redis
import websockets
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
REDIS_HOST  = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT  = int(os.getenv("REDIS_PORT", 6379))
SYMBOL      = os.getenv("TRADE_SYMBOL", "BTCUSDT").lower()   # ws stream is lowercase
CHANNEL     = "market_data"

# Binance WebSocket endpoint
# FEED_TESTNET=false (default) → real production stream, real prices, no auth needed
# FEED_TESTNET=true            → testnet stream, simulated prices
# NOTE: this is independent of order execution — orders always go to Binance Testnet REST.
FEED_TESTNET = os.getenv("FEED_TESTNET", "false").lower() == "true"
WS_HOST = "stream.testnet.binance.vision" if FEED_TESTNET else "stream.binance.com:9443"
WS_URL  = f"wss://{WS_HOST}/ws/{SYMBOL}@bookTicker"

# ── Redis client ──────────────────────────────────────────────────────────────
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


# ── Main stream loop ──────────────────────────────────────────────────────────
async def stream_book_ticker() -> None:
    print(f"[Feed Handler] Connecting to {WS_URL}")
    while True:                                         # auto-reconnect outer loop
        try:
            async with websockets.connect(WS_URL, ping_interval=20) as ws:
                print("[Feed Handler] Connected. Publishing to Redis …")
                async for raw in ws:
                    data      = json.loads(raw)
                    best_bid  = float(data["b"])        # best bid price
                    best_ask  = float(data["a"])        # best ask price
                    mid_price = round((best_bid + best_ask) / 2, 2)

                    payload = json.dumps({
                        "symbol":    SYMBOL.upper(),
                        "mid_price": mid_price,
                        "timestamp": int(time.time()),
                    })

                    r.publish(CHANNEL, payload)
                    print(f"[Feed Handler] {payload}")

        except websockets.ConnectionClosed as exc:
            print(f"[Feed Handler] Connection closed ({exc}). Reconnecting in 3 s …")
            await asyncio.sleep(3)
        except Exception as exc:
            print(f"[Feed Handler] Unexpected error: {exc}. Reconnecting in 5 s …")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(stream_book_ticker())
