"""
Process B — Strategy Engine
============================
Subscribes to the `market_data` Redis channel published by the Feed Handler.
Maintains a rolling window of mid-prices and emits crossover-only trade
signals to the `trade_signals` channel.

MA windows:
  Fast MA : 7  ticks
  Slow MA : 25 ticks

Crossover rules:
  BUY  — fast MA crosses ABOVE slow MA
  SELL — fast MA crosses BELOW slow MA
  HOLD — no crossover (silent, no publish)
"""

import json
import logging
import os
import signal
import sys
import time
from collections import deque

import redis
from dotenv import load_dotenv

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────
REDIS_HOST   = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT   = int(os.getenv("REDIS_PORT", 6379))
SYMBOL       = os.getenv("TRADE_SYMBOL", "BTCUSDT")

IN_CHANNEL   = "market_data"
OUT_CHANNEL  = "trade_signals"

FAST_WINDOW  = 7
SLOW_WINDOW  = 25

RETRY_DELAY  = 5   # seconds between Redis reconnect attempts

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Strategy] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

# ── State ──────────────────────────────────────────────────────────────────────
prices: deque[float] = deque(maxlen=SLOW_WINDOW)
prev_fast_above: bool | None = None   # None until we have enough data for both MAs


# ── Signal handler ─────────────────────────────────────────────────────────────
def _handle_shutdown(signum, frame):  # noqa: ANN001
    log.info("Shutdown signal received. Exiting strategy engine.")
    sys.exit(0)


signal.signal(signal.SIGINT, _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


# ── MA helpers ─────────────────────────────────────────────────────────────────
def _mean(values: list[float]) -> float:
    return sum(values) / len(values)


def _compute_signal(price: float, timestamp: int) -> dict | None:
    """
    Append *price* to the rolling window and return a signal dict if a
    crossover is detected, otherwise return None.
    """
    global prev_fast_above

    prices.append(price)

    if len(prices) < SLOW_WINDOW:
        # Not enough data yet for the slow MA.
        return None

    fast_ma = _mean(list(prices)[-FAST_WINDOW:])
    slow_ma = _mean(list(prices))

    fast_above = fast_ma > slow_ma

    if prev_fast_above is None:
        # First time we have enough data — initialise state, no signal.
        prev_fast_above = fast_above
        return None

    signal_action: str | None = None

    if not prev_fast_above and fast_above:
        signal_action = "BUY"
    elif prev_fast_above and not fast_above:
        signal_action = "SELL"

    prev_fast_above = fast_above

    if signal_action is None:
        return None   # HOLD — no crossover

    return {
        "action":    signal_action,
        "symbol":    SYMBOL,
        "type":      "MARKET",
        "timestamp": timestamp,
    }


# ── Main loop ──────────────────────────────────────────────────────────────────
def run() -> None:
    log.info("Strategy engine starting. Subscribing to '%s'.", IN_CHANNEL)

    while True:
        try:
            r = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True,
                socket_connect_timeout=5,
            )
            r.ping()
            log.info("Connected to Redis at %s:%s.", REDIS_HOST, REDIS_PORT)

            pubsub = r.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(IN_CHANNEL)
            log.info("Listening on '%s' …", IN_CHANNEL)

            for message in pubsub.listen():
                if message["type"] != "message":
                    continue

                try:
                    data      = json.loads(message["data"])
                    price     = float(data["mid_price"])
                    timestamp = int(data["timestamp"])
                except (KeyError, ValueError, json.JSONDecodeError) as exc:
                    log.warning("Malformed message skipped: %s | error: %s", message["data"], exc)
                    continue

                signal_payload = _compute_signal(price, timestamp)

                if signal_payload is not None:
                    r.publish(OUT_CHANNEL, json.dumps(signal_payload))
                    log.info(
                        "Signal published → action=%s symbol=%s fast_window=%d slow_window=%d",
                        signal_payload["action"],
                        signal_payload["symbol"],
                        FAST_WINDOW,
                        SLOW_WINDOW,
                    )
                else:
                    log.debug(
                        "HOLD | prices_buffered=%d/%d | price=%.2f",
                        len(prices),
                        SLOW_WINDOW,
                        price,
                    )

        except redis.exceptions.ConnectionError as exc:
            log.error("Redis connection lost: %s. Retrying in %ds …", exc, RETRY_DELAY)
            time.sleep(RETRY_DELAY)
        except redis.exceptions.RedisError as exc:
            log.error("Redis error: %s. Retrying in %ds …", exc, RETRY_DELAY)
            time.sleep(RETRY_DELAY)


if __name__ == "__main__":
    run()