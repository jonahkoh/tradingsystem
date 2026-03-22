"""Process E - API Gateway (FastAPI control plane and reporting)."""

from __future__ import annotations

import asyncio
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
from fastapi.responses import HTMLResponse

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
POSITION_CHANNEL = "position_updates"
POSITION_KEY = "position:current"

BALANCE_URL = "https://mock-api.roostoo.com/v3/balance"
CSV_FILE = Path(os.getenv("LOG_FILE", "logs/execution_log.csv"))
TRADES_LIMIT = 50

TRADING_FEE_RATE = float(os.getenv("TRADING_FEE_RATE", "0.001"))

# Signals written by the startup liquidation routine — excluded from PnL.
_STARTUP_SIGNALS = {
    "SYSTEM_REBOOT_LIQUIDATION",
    "STARTUP_BALANCE_SYNC",
    "STARTUP_BALANCE_SYNC_CLEAR",
}

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


async def _watch_prices() -> None:
    """Background task: subscribe to market_data and cache the latest price per symbol."""
    while True:
        redis_client: aioredis.Redis | None = None
        try:
            redis_client = aioredis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True,
                socket_connect_timeout=5,
            )
            pubsub = redis_client.pubsub(ignore_subscribe_messages=True)
            await pubsub.subscribe(MARKET_DATA_CHANNEL)
            log.info("Price watcher subscribed to %s", MARKET_DATA_CHANNEL)
            while True:
                msg = await pubsub.get_message(timeout=1.0)
                if msg and msg.get("type") == "message":
                    with suppress(Exception):
                        tick = json.loads(msg["data"])
                        sym  = tick.get("symbol")
                        px   = float(tick.get("mid_price", 0))
                        if sym and px > 0:
                            app.state.latest_prices[sym] = px
        except Exception as exc:
            log.warning("Price watcher error: %s — retrying in 5s.", exc)
        finally:
            with suppress(Exception):
                if redis_client is not None:
                    await redis_client.aclose()
        await asyncio.sleep(5)


@app.on_event("startup")
async def on_startup() -> None:
    app.state.redis = aioredis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        socket_connect_timeout=5,
    )
    app.state.latest_prices: dict[str, float] = {}
    asyncio.create_task(_watch_prices())
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


def _compute_pnl(
    csv_file: Path,
    fee_rate: float = TRADING_FEE_RATE,
    latest_prices: dict[str, float] | None = None,
) -> dict:
    """
    Read the full execution log and compute realized PnL using FIFO lot matching.

    For each symbol:
      - BUY rows open a new lot (price, qty).
      - SELL rows consume the oldest lots first (FIFO), realising PnL per lot consumed.
      - Both sides pay `fee_rate` on the notional, so:
          buy_cost     = price × qty × (1 + fee_rate)
          sell_proceed = price × qty × (1 - fee_rate)

    Startup-liquidation rows (SYSTEM_REBOOT_LIQUIDATION etc.) are excluded.
    Only rows whose API_Status contains "SUCCESS" are counted.
    """
    if not csv_file.exists():
        return {
            "realized_pnl_usd": 0.0,
            "winning_trades": 0,
            "losing_trades": 0,
            "breakeven_trades": 0,
            "total_closed_trades": 0,
            "fee_rate": fee_rate,
            "per_symbol": {},
        }

    try:
        with csv_file.open("r", newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
    except OSError as exc:
        log.error("Failed reading trades CSV for PnL: %s", exc)
        raise HTTPException(status_code=500, detail="Unable to read trades log") from exc

    # Per-symbol FIFO lot queues: deque of [price, remaining_qty]
    lots:         dict[str, deque] = {}
    realized:     dict[str, float] = {}
    winning:      dict[str, int]   = {}
    losing:       dict[str, int]   = {}
    breakeven:    dict[str, int]   = {}

    for row in rows:
        if "SUCCESS" not in row.get("API_Status", ""):
            continue

        signal = row.get("Strategy_Signal", "").upper()
        if signal in _STARTUP_SIGNALS:
            continue

        action = row.get("Action", "").upper()
        if action not in {"BUY", "SELL"}:
            continue

        pair = row.get("Pair", "UNKNOWN")

        try:
            price = float(row.get("Price") or 0)
            # Prefer Effective_Qty (actual fill); fall back to Quantity.
            qty   = float(row.get("Effective_Qty") or row.get("Quantity") or 0)
        except (ValueError, TypeError):
            continue

        if price <= 0 or qty <= 0:
            continue

        # Initialise per-symbol accumulators on first encounter.
        if pair not in lots:
            lots[pair]      = deque()
            realized[pair]  = 0.0
            winning[pair]   = 0
            losing[pair]    = 0
            breakeven[pair] = 0

        if action == "BUY":
            lots[pair].append([price, qty])

        elif action == "SELL":
            remaining_qty  = qty
            matched_cost   = 0.0

            while remaining_qty > 1e-8 and lots[pair]:
                buy_price, buy_qty = lots[pair][0]
                if buy_qty <= remaining_qty + 1e-8:
                    # Consume the whole lot.
                    matched_cost  += buy_price * buy_qty * (1 + fee_rate)
                    remaining_qty -= buy_qty
                    lots[pair].popleft()
                else:
                    # Partially consume.
                    matched_cost  += buy_price * remaining_qty * (1 + fee_rate)
                    lots[pair][0][1] -= remaining_qty
                    remaining_qty    = 0.0

            # Only book PnL on the qty that had a matching BUY lot.
            # Unmatched qty (remaining_qty > 0) means we sold a position opened
            # before the CSV history — treating it as $0 cost would inflate PnL.
            matched_qty      = qty - remaining_qty
            if matched_qty <= 1e-8:
                continue   # no matched lots — skip this SELL entirely
            sell_proceeds    = price * matched_qty * (1 - fee_rate)
            trade_pnl        = sell_proceeds - matched_cost
            realized[pair]  += trade_pnl

            if trade_pnl > 0.01:
                winning[pair]   += 1
            elif trade_pnl < -0.01:
                losing[pair]    += 1
            else:
                breakeven[pair] += 1

    prices = latest_prices or {}

    # Aggregate across symbols.
    total_realized   = 0.0
    total_unrealized = 0.0
    total_winning    = 0
    total_losing     = 0
    total_breakeven  = 0
    per_symbol: dict[str, dict] = {}

    for sym in sorted(realized):
        pnl = realized[sym]
        w   = winning[sym]
        l   = losing[sym]
        b   = breakeven[sym]

        total_realized  += pnl
        total_winning   += w
        total_losing    += l
        total_breakeven += b

        open_lots_list = [
            {"price": round(p, 8), "qty": round(q, 8)}
            for p, q in lots.get(sym, [])
        ]
        open_qty  = sum(q for _, q in lots.get(sym, []))
        open_cost = sum(p * q * (1 + fee_rate) for p, q in lots.get(sym, []))

        # Unrealized PnL: what we'd receive selling now minus what we paid.
        current_price = prices.get(sym)
        if current_price and open_qty > 0:
            current_value    = current_price * open_qty * (1 - fee_rate)
            unrealized_pnl   = current_value - open_cost
            total_unrealized += unrealized_pnl
            sym_entry: dict[str, Any] = {
                "realized_pnl_usd":    round(pnl, 2),
                "unrealized_pnl_usd":  round(unrealized_pnl, 2),
                "winning_trades":      w,
                "losing_trades":       l,
                "breakeven_trades":    b,
                "open_lots":           open_lots_list,
                "open_qty":            round(open_qty, 8),
                "open_cost_basis_usd": round(open_cost, 2),
                "current_price":       round(current_price, 8),
            }
        else:
            sym_entry = {
                "realized_pnl_usd":    round(pnl, 2),
                "unrealized_pnl_usd":  None,   # price not yet received
                "winning_trades":      w,
                "losing_trades":       l,
                "breakeven_trades":    b,
                "open_lots":           open_lots_list,
                "open_qty":            round(open_qty, 8),
                "open_cost_basis_usd": round(open_cost, 2),
                "current_price":       None,
            }

        per_symbol[sym] = sym_entry

    return {
        "realized_pnl_usd":    round(total_realized, 2),
        "unrealized_pnl_usd":  round(total_unrealized, 2),
        "total_pnl_usd":       round(total_realized + total_unrealized, 2),
        "winning_trades":      total_winning,
        "losing_trades":       total_losing,
        "breakeven_trades":    total_breakeven,
        "total_closed_trades": total_winning + total_losing + total_breakeven,
        "fee_rate":            fee_rate,
        "per_symbol":          per_symbol,
    }


@app.get("/api/pnl")
async def get_pnl() -> dict:
    """
    Compute realized PnL from the execution log using FIFO lot matching.

    Returns:
      - realized_pnl_usd: total profit/loss across all closed positions
      - winning/losing/breakeven_trades: round-trip trade counts
      - per_symbol: per-asset breakdown with open lot details
    """
    try:
        latest_prices: dict[str, float] = getattr(app.state, "latest_prices", {})
        return _compute_pnl(CSV_FILE, latest_prices=latest_prices)
    except HTTPException:
        raise
    except Exception as exc:
        log.error("PnL computation failed: %s", exc)
        raise HTTPException(status_code=500, detail="PnL computation failed") from exc


@app.get("/api/trades")
async def get_trades() -> list[dict[str, str]]:
    return _read_last_trades(CSV_FILE, limit=TRADES_LIMIT)


@app.get("/api/position")
async def get_position() -> Any:
    redis_client: aioredis.Redis = app.state.redis
    raw = await redis_client.get(POSITION_KEY)
    if raw is None:
        return {
            "in_position": False,
            "quantity": "0.0000",
            "entry_price": 0.0,
            "stop_loss_price": 0.0,
            "symbol": "",
            "pending_action": None,
            "sentiment": "NEUTRAL",
            "timestamp": 0,
        }
    return json.loads(raw)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    html_path = Path(__file__).parent / "dashboard.html"
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


@app.websocket("/ws/stream")
async def websocket_stream(websocket: WebSocket) -> None:
    await websocket.accept()
    log.info("WebSocket client connected to /ws/stream")

    redis_client: aioredis.Redis = app.state.redis
    pubsub = redis_client.pubsub(ignore_subscribe_messages=True)

    try:
        await pubsub.subscribe(
            MARKET_DATA_CHANNEL,
            TRADE_SIGNALS_CHANNEL,
            SENTIMENT_SIGNALS_CHANNEL,
            POSITION_CHANNEL,
        )
        log.info(
            "WebSocket subscribed to channels: %s, %s, %s, %s",
            MARKET_DATA_CHANNEL,
            TRADE_SIGNALS_CHANNEL,
            SENTIMENT_SIGNALS_CHANNEL,
            POSITION_CHANNEL,
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
            log.info("Forwarded websocket event from channel=%s", channel)

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
                POSITION_CHANNEL,
            )
        with suppress(Exception):
            await pubsub.aclose()
        with suppress(Exception):
            await websocket.close()
        log.info("WebSocket /ws/stream connection closed")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api_gateway:app", host="0.0.0.0", port=8000)
