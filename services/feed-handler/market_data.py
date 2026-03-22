"""
Process A — Market Data (Roostoo)
=================================
Polls Roostoo REST ticker endpoint and publishes mid-price snapshots to Redis.

Input (Roostoo):
  GET /v3/ticker?pair=BTC/USD&timestamp=<13-digit-ms>

Output (Redis channel: market_data):
  {"symbol": "BTC/USD", "mid_price": 60500.50, "timestamp": 1709450000}

No-HFT guardrails:
  - Poll interval default is 5.0 seconds (configurable)
  - Optional RUN_ONCE=true mode for local smoke testing
"""

from __future__ import annotations

import json
import os
import time
from typing import Any

import httpx
import redis
from dotenv import load_dotenv

load_dotenv()


# ── Config ────────────────────────────────────────────────────────────────────
ROOSTOO_BASE_URL = os.getenv("ROOSTOO_BASE_URL", "https://mock-api.roostoo.com").rstrip("/")
PAIR = os.getenv("TRADE_SYMBOL", "BTC/USD")
PAIRS_ENV = os.getenv("TRADE_SYMBOLS", "")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
MARKET_CHANNEL = "market_data"

# Hackathon no-HFT requirement: do not poll faster than this.
POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "5.0"))
REQUEST_TIMEOUT_SEC = float(os.getenv("REQUEST_TIMEOUT_SEC", "10.0"))

# Optional one-shot mode for local/dev validation.
RUN_ONCE = os.getenv("RUN_ONCE", "false").lower() == "true"


def _normalize_pair(raw_pair: str) -> str:
    """Best-effort conversion for legacy symbols (e.g., BTCUSDT -> BTC/USD)."""
    pair = raw_pair.strip().upper()
    if "/" in pair:
        return pair
    if pair.endswith("USDT") and len(pair) > 4:
        return f"{pair[:-4]}/USD"
    return pair


def _pairs_to_poll() -> list[str]:
    if PAIRS_ENV.strip():
        parsed = [_normalize_pair(p) for p in PAIRS_ENV.split(",") if p.strip()]
        if parsed:
            return parsed
    return [_normalize_pair(PAIR)]


def _sorted_params(params: dict[str, Any]) -> str:
    """Build alphabetically-sorted query string: key=value&key=value."""
    return "&".join(f"{k}={params[k]}" for k in sorted(params.keys()))


def _current_ms() -> int:
    return int(time.time() * 1000)


def _fetch_top_of_book(client: httpx.Client, base_url: str, pair: str) -> tuple[float, float, int]:
    """
    Fetch top-of-book for pair and return (max_bid, min_ask, server_time_ms).

    Roostoo contract (provided):
      {
        "Success": true,
        "Data": {
          "BTC/USD": {"MaxBid": ..., "MinAsk": ...}
        }
      }
    """
    params = {
        "pair": pair,
        "timestamp": _current_ms(),
    }
    query = _sorted_params(params)
    url = f"{base_url}/v3/ticker?{query}"

    resp = client.get(url)
    resp.raise_for_status()
    data = resp.json()

    if not data.get("Success", False):
        raise ValueError(f"Roostoo ticker returned Success=false: {data.get('ErrMsg', 'unknown error')}")

    pair_block = data.get("Data", {}).get(pair)
    if not pair_block:
        raise KeyError(f"Pair {pair!r} missing in Roostoo response Data block")

    max_bid = float(pair_block["MaxBid"])
    min_ask = float(pair_block["MinAsk"])
    server_time_ms = int(data.get("ServerTime", _current_ms()))
    return max_bid, min_ask, server_time_ms


def run() -> None:
    pairs = _pairs_to_poll()
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    print(
        f"[Market Data] Starting Roostoo poller | pairs={pairs} "
        f"| base={ROOSTOO_BASE_URL} | poll={POLL_INTERVAL_SEC}s"
    )

    with httpx.Client(timeout=REQUEST_TIMEOUT_SEC) as client:
        while True:
            cycle_start = time.time()
            for pair in pairs:
                try:
                    max_bid, min_ask, server_time_ms = _fetch_top_of_book(client, ROOSTOO_BASE_URL, pair)
                    mid_price = round((max_bid + min_ask) / 2.0, 6)

                    payload = {
                        "symbol": pair,
                        "mid_price": mid_price,
                        # Keep integer seconds to preserve current strategy contract.
                        "timestamp": int(server_time_ms / 1000),
                    }

                    r.publish(MARKET_CHANNEL, json.dumps(payload))
                    print(f"[Market Data] Published: {payload}")

                except Exception as exc:
                    print(f"[Market Data] Poll failed for {pair}: {exc}")

            if RUN_ONCE:
                print("[Market Data] RUN_ONCE=true, exiting after one polling cycle.")
                return

            elapsed = time.time() - cycle_start
            sleep_for = max(POLL_INTERVAL_SEC - elapsed, 0.0)
            time.sleep(sleep_for)


if __name__ == "__main__":
    run()
