"""Process E - API Gateway (FastAPI control plane and reporting)."""

from __future__ import annotations

import csv
import hashlib
import hmac
import inspect
import json
import logging
import os
import time
from collections import deque
from contextlib import suppress
from pathlib import Path
from typing import Any

import httpx
import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()


# Configuration
API_KEY = os.getenv("API_KEY", "").strip()
SECRET_KEY = os.getenv("SECRET_KEY", "").strip()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

CONTROL_CHANNEL = "control"
MARKET_DATA_CHANNEL = "market_data"
TRADE_SIGNALS_CHANNEL = "trade_signals"
SENTIMENT_SIGNALS_CHANNEL = "sentiment_signals"

BALANCE_URL = "https://mock-api.roostoo.com/v3/balance"
CSV_FILE = Path(os.getenv("LOG_FILE", "logs/execution_log.csv"))
TRADES_LIMIT = 50

EXPECTED_TRADE_HEADERS = [
    "Timestamp",
    "Action",
    "Pair",
    "Price",
    "Quantity",
    "Strategy_Signal",
    "API_Status",
    "Latency_ms",
    "Raw_Error",
]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [API Gateway] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


app = FastAPI(title="Trading System API Gateway", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def on_startup() -> None:
    app.state.redis = aioredis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        socket_connect_timeout=5,
    )
    log.info("API Gateway online. Redis target %s:%s", REDIS_HOST, REDIS_PORT)


@app.on_event("shutdown")
async def on_shutdown() -> None:
    redis_client: aioredis.Redis | None = getattr(app.state, "redis", None)
    if redis_client is not None:
        close_method = getattr(redis_client, "aclose", None)
        if callable(close_method):
            maybe_awaitable = close_method()
            if inspect.isawaitable(maybe_awaitable):
                await maybe_awaitable
        log.info("Redis client closed.")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _build_balance_signature(secret_key: str, timestamp_ms: int) -> str:
    message = f"timestamp={timestamp_ms}"
    digest = hmac.new(secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256)
    return digest.hexdigest()


async def _publish_control(command: str) -> None:
    redis_client: aioredis.Redis = app.state.redis
    payload = json.dumps({"command": command})
    await redis_client.publish(CONTROL_CHANNEL, payload)


def _normalize_trade_row(row: dict[str, str]) -> dict[str, str]:
    normalized = {header: row.get(header, "") for header in EXPECTED_TRADE_HEADERS}
    # Backward compatibility with the current ledger header naming.
    if not normalized["Latency_ms"] and row.get("latency_ms"):
        normalized["Latency_ms"] = row["latency_ms"]
    if not normalized["Raw_Error"] and row.get("raw_error"):
        normalized["Raw_Error"] = row["raw_error"]
    return normalized


def _read_last_trades(csv_file: Path, limit: int = TRADES_LIMIT) -> list[dict[str, str]]:
    if not csv_file.exists():
        return []

    try:
        with csv_file.open("r", newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames is None:
                return []
            rows = deque(reader, maxlen=limit)
    except FileNotFoundError:
        return []
    except OSError as exc:
        log.error("Failed reading trades CSV: %s", exc)
        raise HTTPException(status_code=500, detail="Unable to read trades log") from exc

    return [_normalize_trade_row(dict(row)) for row in reversed(rows)]


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "online", "service": "api-gateway"}


@app.post("/api/kill")
async def kill_pipeline() -> dict[str, str]:
    try:
        await _publish_control("HALT")
    except aioredis.RedisError as exc:
        log.error("Failed to publish HALT: %s", exc)
        raise HTTPException(status_code=503, detail="Failed to publish HALT command") from exc

    return {"status": "ok", "message": "Pipeline halting", "command": "HALT"}


@app.post("/api/resume")
async def resume_pipeline() -> dict[str, str]:
    try:
        await _publish_control("START")
    except aioredis.RedisError as exc:
        log.error("Failed to publish START: %s", exc)
        raise HTTPException(status_code=503, detail="Failed to publish START command") from exc

    return {"status": "ok", "message": "Pipeline resuming", "command": "START"}


@app.get("/api/balance")
async def get_balance() -> Any:
    if not API_KEY or not SECRET_KEY:
        raise HTTPException(status_code=500, detail="API_KEY/SECRET_KEY not configured")

    timestamp_ms = _now_ms()
    signature = _build_balance_signature(SECRET_KEY, timestamp_ms)

    headers = {
        "RST-API-KEY": API_KEY,
        "MSG-SIGNATURE": signature,
    }

    params = {"timestamp": str(timestamp_ms)}

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(BALANCE_URL, headers=headers, params=params)
        except httpx.HTTPError as exc:
            log.error("Balance request failed: %s", exc)
            raise HTTPException(status_code=502, detail="Balance request failed") from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise HTTPException(status_code=502, detail="Invalid JSON response from exchange") from exc

    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=payload)

    return payload


@app.get("/api/trades")
async def get_trades() -> list[dict[str, str]]:
    return _read_last_trades(CSV_FILE, limit=TRADES_LIMIT)


@app.websocket("/ws/stream")
async def websocket_stream(websocket: WebSocket) -> None:
    await websocket.accept()

    redis_client: aioredis.Redis = app.state.redis
    pubsub = redis_client.pubsub(ignore_subscribe_messages=True)

    try:
        await pubsub.subscribe(
            MARKET_DATA_CHANNEL,
            TRADE_SIGNALS_CHANNEL,
            SENTIMENT_SIGNALS_CHANNEL,
        )

        while True:
            message = await pubsub.get_message(timeout=1.0)
            if message is None:
                continue

            if message.get("type") != "message":
                continue

            channel = str(message.get("channel", ""))
            raw_data = message.get("data", "")

            if isinstance(raw_data, bytes):
                raw_text = raw_data.decode("utf-8", errors="replace")
            else:
                raw_text = str(raw_data)

            try:
                data = json.loads(raw_text)
            except json.JSONDecodeError:
                data = {"raw": raw_text}

            await websocket.send_json({"channel": channel, "data": data})

    except WebSocketDisconnect:
        log.info("WebSocket client disconnected cleanly.")
    except aioredis.RedisError as exc:
        log.error("Redis pubsub stream error: %s", exc)
        with suppress(Exception):
            await websocket.send_json({"error": "Redis stream unavailable"})
    finally:
        with suppress(Exception):
            await pubsub.unsubscribe(
                MARKET_DATA_CHANNEL,
                TRADE_SIGNALS_CHANNEL,
                SENTIMENT_SIGNALS_CHANNEL,
            )
        with suppress(Exception):
            await pubsub.aclose()
        with suppress(Exception):
            await websocket.close()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api_gateway:app", host="0.0.0.0", port=8000)
