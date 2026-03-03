"""
Process E — API Gateway & Kill Switch
=======================================
A lightweight FastAPI HTTP server that acts as the human control plane
for the trading pipeline.

Endpoints
  GET  /health   — liveness probe (no Redis required)
  GET  /status   — reports whether the kill switch has been fired this session
  POST /kill     — publishes {"command": "HALT"} to the Redis `control` channel,
                   which Process C (Execution Manager) receives and acts on instantly

How the kill switch works end-to-end
  1. Operator hits POST /kill (curl, browser, Postman, etc.)
  2. This server publishes {"command": "HALT"} to the Redis `control` channel.
  3. Process C is already subscribed to `control`. Redis delivers the message
     in <1 ms (it's on the same local network / same machine).
  4. Process C's handle_control() fires: sets _halted, cancels open orders,
     calls sys.exit(0).
  5. No further orders can be placed even if trade_signals keep arriving.

Redis is injected via FastAPI's Depends() mechanism so tests can substitute
a mock without monkey-patching globals.
"""

import logging
import os
import time

import redis
import redis.exceptions
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.responses import JSONResponse

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────
REDIS_HOST      = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT      = int(os.getenv("REDIS_PORT", 6379))
CONTROL_CHANNEL = "control"

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [API Gateway] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

# ── App state ──────────────────────────────────────────────────────────────────
# Tracks whether /kill has been called this session.
# Simple bool is fine here — the gateway is single-process, single-threaded
# (uvicorn handles concurrency at the ASGI level, not via threads).
_kill_fired: bool = False
_kill_timestamp: float | None = None

# ── FastAPI app ────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Trading System — API Gateway",
    description="Control plane for the crypto trading pipeline.",
    version="1.0.0",
)


# ── Redis dependency ───────────────────────────────────────────────────────────
def get_redis() -> redis.Redis:
    """
    FastAPI dependency that yields a Redis client.

    WHY Depends(): tests can override this with app.dependency_overrides so
    every endpoint gets a mock Redis — no real connection needed in tests.
    """
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        socket_connect_timeout=3,
    )


# ── Core publish helper ────────────────────────────────────────────────────────
def publish_halt(r: redis.Redis) -> int:
    """
    Publish the HALT command to the control channel.

    Returns the number of subscribers that received the message (from Redis).
    Raises redis.exceptions.RedisError on connectivity issues.
    """
    import json
    payload = json.dumps({"command": "HALT"})
    subscribers = r.publish(CONTROL_CHANNEL, payload)
    log.warning("HALT published to '%s'. Subscribers notified: %d", CONTROL_CHANNEL, subscribers)
    return subscribers


# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.get("/health", tags=["ops"])
def health() -> JSONResponse:
    """
    Liveness probe — always returns 200 if the process is running.
    Does NOT check Redis so container orchestrators get a fast response.
    """
    return JSONResponse({"status": "ok", "service": "api-gateway"})


@app.get("/status", tags=["ops"])
def status_endpoint() -> JSONResponse:
    """
    Reports whether the kill switch has been activated this session.
    Useful for dashboards and monitoring.
    """
    return JSONResponse({
        "kill_switch_fired": _kill_fired,
        "kill_timestamp":    _kill_timestamp,
    })


@app.post("/kill", tags=["control"], status_code=status.HTTP_200_OK)
def kill_switch(r: redis.Redis = Depends(get_redis)) -> JSONResponse:
    """
    Trigger the emergency kill switch.

    Publishes {"command": "HALT"} to the Redis `control` channel.
    Process C (Execution Manager) receives this instantly and:
      1. Sets its internal _halted flag — no more orders accepted.
      2. Calls cancel_open_orders() on Binance.
      3. Exits cleanly.

    Returns 200 with how many pipeline processes were notified.
    Returns 503 if Redis is unreachable (kill switch could not be delivered).
    """
    global _kill_fired, _kill_timestamp

    try:
        subscribers = publish_halt(r)
    except redis.exceptions.RedisError as exc:
        log.error("Failed to publish HALT: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Redis unreachable — HALT not delivered: {exc}",
        )

    _kill_fired     = True
    _kill_timestamp = time.time()

    log.warning("Kill switch activated. Pipeline halted.")

    return JSONResponse({
        "command":     "HALT",
        "channel":     CONTROL_CHANNEL,
        "subscribers": subscribers,
        "result":      "HALT published successfully",
    })


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api_gateway:app", host="0.0.0.0", port=8000, reload=False)
