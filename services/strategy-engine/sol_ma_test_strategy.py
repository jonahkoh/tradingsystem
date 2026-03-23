"""SOL 7/99 MA Crossover Test Strategy (Binance Testnet validation).

Simple moving average crossover strategy for SOL/USD:
  - BUY when MA(7) crosses above MA(99)
  - SELL ALL when MA(7) crosses below MA(99)

No fees, no stop-loss, no position sizing complexity.
This exists purely to validate the Binance testnet pipeline end-to-end.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import deque

import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

MARKET_DATA_CHANNEL = "market_data"
TRADE_SIGNALS_CHANNEL = "trade_signals"
EXECUTION_CONFIRMATIONS_CHANNEL = "execution_confirmations"

TRADE_SYMBOL = "SOL/USD"
ORDER_TYPE = "MARKET"
BUY_QTY = os.getenv("SOL_TEST_QTY", "1")

MA_FAST = 7
MA_SLOW = 99

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SOL-MA-Test] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

POSITION_CHANNEL = "position_updates"
STATE_KEY = f"strategy_state:{TRADE_SYMBOL}"

prices: deque[float] = deque(maxlen=MA_SLOW)
in_position = False
pending_action: str | None = None
open_qty = 0.0
entry_price = 0.0


def ma(n: int) -> float | None:
    if len(prices) < n:
        return None
    return sum(list(prices)[-n:]) / n


async def publish_signal(
    redis_client: aioredis.Redis,
    action: str,
    quantity: str,
    price: float,
    reason: str,
) -> None:
    global pending_action
    payload = {
        "action": action,
        "symbol": TRADE_SYMBOL,
        "type": ORDER_TYPE,
        "quantity": quantity,
        "reason": reason,
        "timestamp": int(time.time()),
        "expected_price": price,
    }
    await redis_client.publish(TRADE_SIGNALS_CHANNEL, json.dumps(payload))
    pending_action = action
    log.info("Signal → %s %s %s reason=%s px=%.4f", action, quantity, TRADE_SYMBOL, reason, price)


async def handle_tick(price: float, timestamp: int, redis_client: aioredis.Redis) -> None:
    global in_position

    prices.append(price)

    fast = ma(MA_FAST)
    slow = ma(MA_SLOW)

    if fast is None or slow is None:
        log.info("Warming up: %d/%d ticks | price=%.4f", len(prices), MA_SLOW, price)
        return

    if pending_action is not None:
        return

    log.info("MA7=%.4f MA99=%.4f price=%.4f in_position=%s", fast, slow, price, in_position)

    if not in_position and fast > slow:
        await publish_signal(redis_client, "BUY", BUY_QTY, price, "MA7_cross_above_MA99")

    elif in_position and fast < slow:
        await publish_signal(redis_client, "SELL", "ALL", price, "MA7_cross_below_MA99")

    if in_position:
        await update_position_state(redis_client, price)


async def update_position_state(redis_client: aioredis.Redis, price: float) -> None:
    state = {
        "in_position": str(in_position),
        "open_position_quantity": str(open_qty),
        "entry_price": str(entry_price),
        "average_cost": str(entry_price),
        "atr_at_entry": "0.0",
        "cooldown_until": "0.0",
    }
    await redis_client.hset(STATE_KEY, mapping=state)

    snapshot = json.dumps({
        "symbol": TRADE_SYMBOL,
        "in_position": in_position,
        "quantity": open_qty,
        "entry_price": entry_price,
        "current_price": price,
    })
    await redis_client.publish(POSITION_CHANNEL, snapshot)


async def handle_execution_confirmation(raw_payload: str, redis_client: aioredis.Redis) -> None:
    global in_position, pending_action, open_qty, entry_price

    try:
        data = json.loads(raw_payload)
    except json.JSONDecodeError:
        return

    symbol = str(data.get("symbol", "")).upper()
    if symbol != TRADE_SYMBOL:
        return

    action = str(data.get("action", "")).upper()
    result = str(data.get("result", "")).upper()
    qty = float(data.get("effective_qty", 0))

    if result == "SUCCESS":
        if action == "BUY":
            in_position = True
            open_qty = qty
            entry_price = float(data.get("expected_price", 0) or prices[-1] if prices else 0)
        elif action == "SELL":
            in_position = False
            open_qty = 0.0
            entry_price = 0.0

    pending_action = None
    log.info("Confirmation: %s %s result=%s qty=%.4f | in_position=%s", action, symbol, result, qty, in_position)

    current_price = prices[-1] if prices else 0.0
    await update_position_state(redis_client, current_price)


async def run() -> None:
    log.info("Starting SOL MA(7/99) crossover test | qty=%s | pair=%s", BUY_QTY, TRADE_SYMBOL)

    while True:
        redis_client = None
        try:
            redis_client = aioredis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True,
                socket_connect_timeout=5,
            )
            await redis_client.ping()
            log.info("Connected to Redis at %s:%s", REDIS_HOST, REDIS_PORT)

            pubsub = redis_client.pubsub(ignore_subscribe_messages=True)
            await pubsub.subscribe(MARKET_DATA_CHANNEL, EXECUTION_CONFIRMATIONS_CHANNEL)
            log.info("Subscribed to %s, %s", MARKET_DATA_CHANNEL, EXECUTION_CONFIRMATIONS_CHANNEL)

            async for message in pubsub.listen():
                if message.get("type") != "message":
                    continue

                channel = message.get("channel")
                raw_data = message.get("data", "")
                payload = (
                    raw_data.decode("utf-8", errors="replace")
                    if isinstance(raw_data, bytes)
                    else str(raw_data)
                )

                if channel == EXECUTION_CONFIRMATIONS_CHANNEL:
                    await handle_execution_confirmation(payload, redis_client)
                    continue

                if channel == MARKET_DATA_CHANNEL:
                    try:
                        tick = json.loads(payload)
                        symbol = str(tick.get("symbol", ""))
                        if symbol.upper() != TRADE_SYMBOL:
                            continue
                        price = float(tick["mid_price"])
                        timestamp = int(tick.get("timestamp", time.time()))
                    except (KeyError, ValueError, TypeError, json.JSONDecodeError):
                        continue
                    await handle_tick(price, timestamp, redis_client)

        except aioredis.RedisError as exc:
            log.error("Redis error: %s. Retrying in 5s.", exc)
            await asyncio.sleep(5)
        finally:
            if redis_client:
                await redis_client.close()


if __name__ == "__main__":
    asyncio.run(run())
