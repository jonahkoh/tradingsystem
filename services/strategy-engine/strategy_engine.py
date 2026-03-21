"""
Process B — Strategy Engine (Volatility-Filtered Mean Reversion)
=================================================================
Subscribes to Redis `market_data` and publishes trade signals to `trade_signals`.

Trading logic:
  BUY  when OUT_OF_POSITION and:
      price < lower Bollinger Band
      AND volatility (ATR proxy / price) <= MAX_VOL_THRESHOLD

  SELL (take profit) when IN_POSITION and:
      price >= Bollinger middle band (SMA)

  SELL (emergency stop) when IN_POSITION and:
      price < (entry_price - 1.5 * ATR proxy)

No-HFT behavior:
  - Signals only fire on state transitions (IN/OUT position).
  - Optional cooldown guard between signal publishes.
"""

from __future__ import annotations

import json
import logging
import math
import os
import signal
import sys
import time
from collections import deque

import redis
from dotenv import load_dotenv

load_dotenv()


# ── Config ─────────────────────────────────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

IN_CHANNEL = "market_data"
OUT_CHANNEL = "trade_signals"

DEFAULT_SYMBOL = os.getenv("TRADE_SYMBOL", "BTC/USD")

# Indicator windows
ROLLING_WINDOW = int(os.getenv("ROLLING_WINDOW", "50"))
BB_PERIOD = int(os.getenv("BB_PERIOD", "20"))
BB_STD_MULT = float(os.getenv("BB_STD_MULT", "2.0"))
ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))

# Volatility filter: ATR proxy / price must be <= this ratio to allow BUY.
MAX_VOL_THRESHOLD = float(os.getenv("MAX_VOL_THRESHOLD", "0.01"))  # 1.0%

# Optional anti-spam safety: minimum seconds between signal publishes.
SIGNAL_COOLDOWN_SEC = float(os.getenv("SIGNAL_COOLDOWN_SEC", "2.0"))

RETRY_DELAY = 5


# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Strategy] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


# ── State ──────────────────────────────────────────────────────────────────────
prices: deque[float] = deque(maxlen=ROLLING_WINDOW)

in_position = False
entry_price = 0.0
last_signal_ts = 0.0


def _handle_shutdown(signum, frame):  # noqa: ANN001
    log.info("Shutdown signal received. Exiting strategy engine.")
    sys.exit(0)


signal.signal(signal.SIGINT, _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


def _mean(values: list[float]) -> float:
    return sum(values) / len(values)


def _std(values: list[float], mean_val: float) -> float:
    # Population std is sufficient for rolling strategy diagnostics.
    variance = sum((v - mean_val) ** 2 for v in values) / len(values)
    return math.sqrt(variance)


def _atr_proxy(latest_prices: list[float]) -> float:
    """
    ATR proxy from mid-prices only:
      mean(abs(price_t - price_{t-1})) over ATR_PERIOD deltas.
    """
    deltas = [abs(latest_prices[i] - latest_prices[i - 1]) for i in range(1, len(latest_prices))]
    if not deltas:
        return 0.0
    # Use the most recent ATR_PERIOD deltas.
    tail = deltas[-ATR_PERIOD:]
    return _mean(tail)


def _compute_indicators(latest_prices: list[float]) -> dict[str, float]:
    bb_slice = latest_prices[-BB_PERIOD:]
    sma = _mean(bb_slice)
    std = _std(bb_slice, sma)
    upper = sma + BB_STD_MULT * std
    lower = sma - BB_STD_MULT * std

    atr = _atr_proxy(latest_prices)
    current_price = latest_prices[-1]
    vol_ratio = (atr / current_price) if current_price > 0 else 0.0

    return {
        "sma": sma,
        "upper": upper,
        "lower": lower,
        "atr": atr,
        "vol_ratio": vol_ratio,
    }


def _publish_signal(r: redis.Redis, action: str, symbol: str, reason: str, timestamp: int) -> None:
    global last_signal_ts

    now = time.time()
    if now - last_signal_ts < SIGNAL_COOLDOWN_SEC:
        log.info(
            "Signal cooldown active (%.2fs < %.2fs). %s skipped.",
            now - last_signal_ts,
            SIGNAL_COOLDOWN_SEC,
            action,
        )
        return

    payload = {
        "action": action,
        "symbol": symbol,
        "type": "MARKET",
        "reason": reason,
        "timestamp": timestamp,
    }
    r.publish(OUT_CHANNEL, json.dumps(payload))
    last_signal_ts = now
    log.info("Signal published → %s", payload)


def _evaluate_signal(price: float, symbol: str, timestamp: int, r: redis.Redis) -> None:
    global in_position, entry_price

    prices.append(price)
    if len(prices) < BB_PERIOD:
        log.info(
            "Waiting for window fill: %d/%d prices buffered.",
            len(prices),
            BB_PERIOD,
        )
        return

    ind = _compute_indicators(list(prices))
    sma = ind["sma"]
    lower = ind["lower"]
    upper = ind["upper"]
    atr = ind["atr"]
    vol_ratio = ind["vol_ratio"]

    state = "IN_POSITION" if in_position else "OUT_OF_POSITION"
    log.info(
        (
            "px=%.6f | SMA=%.6f | BB[%.6f, %.6f] | ATR~%.6f | vol=%.4f%% "
            "| state=%s | entry=%.6f"
        ),
        price,
        sma,
        lower,
        upper,
        atr,
        vol_ratio * 100,
        state,
        entry_price,
    )

    # BUY rule (mean reversion + volatility filter)
    if not in_position:
        if price < lower and vol_ratio <= MAX_VOL_THRESHOLD:
            in_position = True
            entry_price = price
            _publish_signal(r, "BUY", symbol, "BB_Lower_Cross", timestamp)
        return

    # SELL rule 1: emergency stop-loss
    stop_price = entry_price - (1.5 * atr)
    if price < stop_price:
        in_position = False
        _publish_signal(r, "SELL", symbol, "ATR_Stop_Loss", timestamp)
        entry_price = 0.0
        return

    # SELL rule 2: take-profit at mean reversion back to middle band.
    if price >= sma:
        in_position = False
        _publish_signal(r, "SELL", symbol, "Mean_Reversion_TP", timestamp)
        entry_price = 0.0


def run() -> None:
    log.info(
        (
            "Strategy engine starting | BB_PERIOD=%d STD=%.2f ATR_PERIOD=%d "
            "MAX_VOL_THRESHOLD=%.4f"
        ),
        BB_PERIOD,
        BB_STD_MULT,
        ATR_PERIOD,
        MAX_VOL_THRESHOLD,
    )

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
                    data = json.loads(message["data"])
                    price = float(data["mid_price"])
                    timestamp = int(data["timestamp"])
                    symbol = str(data.get("symbol", DEFAULT_SYMBOL))
                except (KeyError, ValueError, TypeError, json.JSONDecodeError) as exc:
                    log.warning("Malformed market_data message skipped: %s | error: %s", message["data"], exc)
                    continue

                _evaluate_signal(price, symbol, timestamp, r)

        except redis.exceptions.ConnectionError as exc:
            log.error("Redis connection lost: %s. Retrying in %ds …", exc, RETRY_DELAY)
            time.sleep(RETRY_DELAY)
        except redis.exceptions.RedisError as exc:
            log.error("Redis error: %s. Retrying in %ds …", exc, RETRY_DELAY)
            time.sleep(RETRY_DELAY)


if __name__ == "__main__":
    run()
