"""
test_setup.py — End-to-end connectivity checks
===============================================
Run this after `docker compose up -d` to confirm the full stack is healthy.

Checks performed:
  1. Redis  — TCP connection + PING
  2. Redis  — Pub/Sub round-trip (publish & receive on a test channel)
  3. Binance — REST API reachability (server time, no auth required)
  4. Binance — Authenticated REST (account info, requires valid API keys)
  5. Binance — WebSocket connection + first bookTicker message
"""

import asyncio
import json
import os
import sys
import time

import redis
import websockets
from binance.client import Client
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
REDIS_HOST       = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT       = int(os.getenv("REDIS_PORT", 6379))
BINANCE_API_KEY  = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
SYMBOL           = os.getenv("TRADE_SYMBOL", "BTCUSDT").lower()
WS_URL           = f"wss://stream.testnet.binance.vision/ws/{SYMBOL}@bookTicker"

PASS = "\033[92m[PASS]\033[0m"
FAIL = "\033[91m[FAIL]\033[0m"
INFO = "\033[94m[INFO]\033[0m"

results: list[tuple[str, bool]] = []


# ── Helpers ───────────────────────────────────────────────────────────────────
def log(name: str, ok: bool, detail: str = "") -> None:
    tag = PASS if ok else FAIL
    print(f"  {tag} {name}" + (f"  →  {detail}" if detail else ""))
    results.append((name, ok))


# ── Test 1: Redis TCP + PING ──────────────────────────────────────────────────
def test_redis_connection() -> None:
    print("\n[1] Redis — connection")
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
                        socket_connect_timeout=3)
        pong = r.ping()
        log("Redis PING", pong, f"{REDIS_HOST}:{REDIS_PORT}")
    except Exception as exc:
        log("Redis PING", False, str(exc))


# ── Test 2: Redis Pub/Sub round-trip ─────────────────────────────────────────
def test_redis_pubsub() -> None:
    print("\n[2] Redis — Pub/Sub round-trip")
    TEST_CHANNEL  = "test_channel"
    TEST_MESSAGE  = "hello_tradingsystem"
    received: list[str] = []

    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        pub = r.pubsub()
        pub.subscribe(TEST_CHANNEL)

        # consume the subscribe confirmation
        pub.get_message(timeout=1)

        r.publish(TEST_CHANNEL, TEST_MESSAGE)
        time.sleep(0.1)                          

        msg = pub.get_message(timeout=1)
        if msg and msg["type"] == "message":
            received.append(msg["data"])

        pub.unsubscribe(TEST_CHANNEL)
        pub.close()

        ok = TEST_MESSAGE in received
        log("Pub/Sub round-trip", ok, f"sent='{TEST_MESSAGE}' received={received}")
    except Exception as exc:
        log("Pub/Sub round-trip", False, str(exc))


# ── Test 3: Binance REST (no auth) ───────────────────────────────────────────
def test_binance_rest_public() -> None:
    print("\n[3] Binance Testnet — public REST (server time)")
    try:
        client = Client("", "", testnet=True)
        server_time_ms = client.get_server_time()["serverTime"]
        local_time_ms  = int(time.time() * 1000)
        drift_ms       = abs(server_time_ms - local_time_ms)
        log("REST /api/v3/time", True, f"server={server_time_ms} drift={drift_ms}ms")
        if drift_ms > 1000:
            print(f"  {INFO} Clock drift {drift_ms}ms — keep below 1000ms for signed requests")
    except Exception as exc:
        log("REST /api/v3/time", False, str(exc))


# ── Test 4: Binance REST (authenticated) ─────────────────────────────────────
def test_binance_rest_auth() -> None:
    print("\n[4] Binance Testnet — authenticated REST (account info)")
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        print(f"  {INFO} Skipped — BINANCE_API_KEY / BINANCE_API_SECRET not set in .env")
        results.append(("Authed REST", None))   # type: ignore[arg-type]
        return
    try:
        client = Client(BINANCE_API_KEY, BINANCE_API_SECRET, testnet=True)
        account = client.get_account()
        balances = [b for b in account["balances"] if float(b["free"]) > 0]
        log("GET /api/v3/account", True,
            f"accountType={account['accountType']}  "
            f"non-zero balances: {[b['asset'] for b in balances]}")
    except Exception as exc:
        log("GET /api/v3/account", False, str(exc))


# ── Test 5: Binance WebSocket (first tick) ───────────────────────────────────
async def _ws_first_tick() -> dict:
    async with websockets.connect(WS_URL, open_timeout=10) as ws:
        raw = await asyncio.wait_for(ws.recv(), timeout=10)
        return json.loads(raw)


def test_binance_websocket() -> None:
    print(f"\n[5] Binance Testnet — WebSocket ({SYMBOL}@bookTicker)")
    try:
        loop = asyncio.new_event_loop()
        data = loop.run_until_complete(_ws_first_tick())
        loop.close()
        bid = float(data["b"])
        ask = float(data["a"])
        mid = round((bid + ask) / 2, 2)
        log(f"bookTicker first tick", True,
            f"bid={bid}  ask={ask}  mid={mid}")
    except Exception as exc:
        log(f"bookTicker first tick", False, str(exc))


# ── Summary ───────────────────────────────────────────────────────────────────
def print_summary() -> None:
    print("\n" + "─" * 50)
    definitive  = [(n, ok) for (n, ok) in results if ok is not None]
    passed      = sum(1 for (_, ok) in definitive if ok)
    failed      = sum(1 for (_, ok) in definitive if not ok)
    print(f"  Results: {passed} passed, {failed} failed out of {len(definitive)} checks")
    if failed:
        print(f"\n  {FAIL} Failing checks:")
        for name, ok in definitive:
            if not ok:
                print(f"       • {name}")
    else:
        print(f"\n  {PASS} All checks passed — stack is healthy.")
    print("─" * 50)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("  tradingsystem — setup test")
    print("=" * 50)

    test_redis_connection()
    test_redis_pubsub()
    test_binance_rest_public()
    test_binance_rest_auth()
    test_binance_websocket()
    print_summary()

    sys.exit(0 if all(ok for (_, ok) in results if ok is not None) else 1)
