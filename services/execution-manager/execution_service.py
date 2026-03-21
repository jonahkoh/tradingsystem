"""
Process C - Execution Service (Roostoo)
=======================================
Listens to Redis `trade_signals` and executes signed POST orders on Roostoo.

Inbound signal example:
  {"action": "BUY", "symbol": "BTC/USD", "type": "MARKET", "quantity": "1.0"}

Outbound result (Redis `order_results`):
  {
    "result": "SUCCESS"|"FAILED",
    "status_code": 200,
    "latency_ms": 42,
    "order_details": {...},
    "raw_error": "..."
  }
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import signal
import threading
import time
from typing import Any

import httpx
import redis
from dotenv import load_dotenv

load_dotenv()


# Configuration
ROOSTOO_BASE_URL = os.getenv("ROOSTOO_BASE_URL", "https://mock-api.roostoo.com").strip().rstrip("/")
API_KEY = os.getenv("API_KEY", "").strip()
SECRET_KEY = os.getenv("SECRET_KEY", "").strip()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

DEFAULT_PAIR = os.getenv("TRADE_SYMBOL", "BTC/USD")
DEFAULT_ORDER_TYPE = os.getenv("TRADE_ORDER_TYPE", "MARKET")
DEFAULT_QUANTITY = os.getenv("TRADE_QUANTITY", "1.0")

EXECUTION_MIN_INTERVAL_SEC = float(os.getenv("EXECUTION_MIN_INTERVAL_SEC", "2.0"))
REQUEST_TIMEOUT_SEC = float(os.getenv("REQUEST_TIMEOUT_SEC", "10.0"))
RETRY_DELAY_SEC = 5
AUTH_DEBUG = os.getenv("AUTH_DEBUG", "false").lower() == "true"

SIGNALS_CHANNEL = "trade_signals"
CONTROL_CHANNEL = "control"
RESULTS_CHANNEL = "order_results"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Execution] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


_halted = threading.Event()


def _current_ms() -> int:
    return int(time.time() * 1000)


def _sorted_params_string(params: dict[str, Any]) -> str:
    """Build alphabetically sorted key=value string joined by '&'."""
    return "&".join(f"{k}={params[k]}" for k in sorted(params.keys()))


def _sign_message(secret_key: str, message: str) -> str:
    digest = hmac.new(secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256)
    return digest.hexdigest()


def _build_headers(api_key: str, signature: str) -> dict[str, str]:
    return {
        "RST-API-KEY": api_key,
        "MSG-SIGNATURE": signature,
        "Content-Type": "application/x-www-form-urlencoded",
    }


def _validate_signal(signal_payload: dict[str, Any]) -> tuple[str, str, str, str]:
    side = str(signal_payload.get("action", "")).upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError(f"Invalid action: {side!r}")

    pair = str(signal_payload.get("symbol", DEFAULT_PAIR)).strip().upper()
    order_type = str(signal_payload.get("type", DEFAULT_ORDER_TYPE)).strip().upper()
    quantity = str(signal_payload.get("quantity", DEFAULT_QUANTITY)).strip()

    if not pair:
        raise ValueError("Missing symbol/pair in signal")
    if order_type not in {"MARKET", "LIMIT"}:
        raise ValueError(f"Unsupported order type: {order_type!r}")

    parsed_qty = float(quantity)
    if parsed_qty <= 0:
        raise ValueError("quantity must be > 0")

    return side, pair, order_type, quantity


class ExecutionThrottle:
    """Ensure a minimum interval between order executions (No-HFT rule)."""

    def __init__(self, min_interval_sec: float) -> None:
        self._min_interval_sec = max(min_interval_sec, 0.0)
        self._last_exec_monotonic = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_exec_monotonic
            if elapsed < self._min_interval_sec:
                time.sleep(self._min_interval_sec - elapsed)
            self._last_exec_monotonic = time.monotonic()


def _extract_order_details(response_json: Any) -> dict[str, Any] | None:
    if isinstance(response_json, dict):
        order_detail = response_json.get("OrderDetail")
        if isinstance(order_detail, dict):
            return order_detail
        data_block = response_json.get("Data")
        if isinstance(data_block, dict):
            return data_block
        return response_json
    return None


def _build_order_result(
    *,
    success: bool,
    status_code: int,
    latency_ms: int,
    side: str,
    pair: str,
    quantity: str,
    response_json: Any = None,
    raw_error: str = "",
) -> dict[str, Any]:
    order_details = _extract_order_details(response_json)
    order_id = None
    if isinstance(order_details, dict):
        order_id = order_details.get("OrderID") or order_details.get("orderId")

    return {
        "timestamp": int(time.time()),
        "result": "SUCCESS" if success else "FAILED",
        "status_code": status_code,
        "latency_ms": latency_ms,
        "order_id": order_id,
        "side": side,
        "symbol": pair,
        "qty": float(quantity),
        "price": 0.0,
        "order_details": order_details,
        "raw_error": raw_error,
    }


def execute_trade_signal(
    signal_payload: dict[str, Any],
    http_client: httpx.Client,
    redis_client: redis.Redis,
    throttle: ExecutionThrottle,
) -> dict[str, Any]:
    """Validate, sign, execute, and publish one trade signal."""
    side, pair, order_type, quantity = _validate_signal(signal_payload)

    params = {
        "pair": pair,
        "quantity": quantity,
        "side": side,
        "timestamp": _current_ms(),
        "type": order_type,
    }
    payload = _sorted_params_string(params)
    signature = _sign_message(SECRET_KEY, payload)
    headers = _build_headers(API_KEY, signature)

    if AUTH_DEBUG:
        log.info(
            "Auth debug | api_key_len=%d secret_len=%d payload=%s signature_prefix=%s",
            len(API_KEY),
            len(SECRET_KEY),
            payload,
            signature[:12],
        )

    throttle.wait()

    url = f"{ROOSTOO_BASE_URL}/v3/place_order"
    start = time.perf_counter()
    response = http_client.post(url, content=payload, headers=headers)
    latency_ms = int((time.perf_counter() - start) * 1000)

    try:
        response_json = response.json()
    except ValueError:
        response_json = None

    success = response.status_code == 200 and bool(
        isinstance(response_json, dict) and response_json.get("Success", False)
    )

    raw_error = ""
    if not success:
        if isinstance(response_json, dict):
            raw_error = str(response_json.get("ErrMsg") or response_json)
        else:
            raw_error = response.text

        if response.status_code == 401:
            log.warning(
                (
                    "401 Unauthorized from Roostoo. This usually indicates key/secret mismatch, "
                    "wrong credential scope for this endpoint, or server-side canonicalization mismatch."
                )
            )

    result = _build_order_result(
        success=success,
        status_code=response.status_code,
        latency_ms=latency_ms,
        side=side,
        pair=pair,
        quantity=quantity,
        response_json=response_json,
        raw_error=raw_error,
    )

    redis_client.publish(RESULTS_CHANNEL, json.dumps(result))
    return result


def _handle_control(raw_data: str) -> None:
    try:
        payload = json.loads(raw_data)
    except json.JSONDecodeError:
        log.warning("Malformed control payload skipped: %r", raw_data)
        return

    command = str(payload.get("command", "")).upper()
    if command == "HALT":
        _halted.set()
        log.warning("HALT received. Execution paused.")
        return
    if command == "START":
        _halted.clear()
        log.info("START received. Execution resumed.")
        return

    log.info("Unknown control command ignored: %r", command)


def _handle_trade_message(
    raw_data: str,
    http_client: httpx.Client,
    redis_client: redis.Redis,
    throttle: ExecutionThrottle,
) -> None:
    if _halted.is_set():
        log.warning("HALTED state active - signal ignored.")
        return

    try:
        signal_payload = json.loads(raw_data)
    except json.JSONDecodeError:
        log.warning("Malformed trade signal skipped: %r", raw_data)
        return

    try:
        result = execute_trade_signal(signal_payload, http_client, redis_client, throttle)
        log.info(
            "Order result published: result=%s status=%s latency_ms=%s",
            result["result"],
            result["status_code"],
            result["latency_ms"],
        )
    except Exception as exc:  # Keep service alive and publish failure telemetry.
        raw_qty = signal_payload.get("quantity", DEFAULT_QUANTITY)
        try:
            safe_qty = float(raw_qty)
        except (TypeError, ValueError):
            safe_qty = 0.0

        fallback = {
            "timestamp": int(time.time()),
            "result": "FAILED",
            "status_code": 0,
            "latency_ms": 0,
            "order_id": None,
            "side": str(signal_payload.get("action", "UNKNOWN")).upper(),
            "symbol": str(signal_payload.get("symbol", DEFAULT_PAIR)),
            "qty": safe_qty,
            "price": 0.0,
            "order_details": None,
            "raw_error": str(exc),
        }
        redis_client.publish(RESULTS_CHANNEL, json.dumps(fallback))
        log.error("Execution failure published to %s: %s", RESULTS_CHANNEL, exc)


def _handle_os_signal(signum, frame):  # noqa: ANN001
    log.info("OS signal %s received. Shutting down.", signum)
    raise SystemExit(0)


signal.signal(signal.SIGINT, _handle_os_signal)
signal.signal(signal.SIGTERM, _handle_os_signal)


def run() -> None:
    if not API_KEY or not SECRET_KEY:
        log.warning("API_KEY or SECRET_KEY missing. Orders will likely fail until set in .env.")

    throttle = ExecutionThrottle(EXECUTION_MIN_INTERVAL_SEC)
    log.info(
        "Execution service starting | base=%s | min_interval=%.2fs",
        ROOSTOO_BASE_URL,
        EXECUTION_MIN_INTERVAL_SEC,
    )

    with httpx.Client(timeout=REQUEST_TIMEOUT_SEC) as http_client:
        while True:
            try:
                r = redis.Redis(
                    host=REDIS_HOST,
                    port=REDIS_PORT,
                    decode_responses=True,
                    socket_connect_timeout=5,
                )
                r.ping()
                log.info("Connected to Redis at %s:%s", REDIS_HOST, REDIS_PORT)

                pubsub = r.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(SIGNALS_CHANNEL, CONTROL_CHANNEL)
                log.info("Listening on channels: %s, %s", SIGNALS_CHANNEL, CONTROL_CHANNEL)

                for message in pubsub.listen():
                    if message.get("type") != "message":
                        continue

                    channel = message.get("channel")
                    raw_data = message.get("data", "")

                    if channel == CONTROL_CHANNEL:
                        _handle_control(raw_data)
                        continue

                    if channel == SIGNALS_CHANNEL:
                        _handle_trade_message(raw_data, http_client, r, throttle)

            except redis.exceptions.ConnectionError as exc:
                log.error("Redis connection error: %s. Retrying in %ds", exc, RETRY_DELAY_SEC)
                time.sleep(RETRY_DELAY_SEC)
            except redis.exceptions.RedisError as exc:
                log.error("Redis error: %s. Retrying in %ds", exc, RETRY_DELAY_SEC)
                time.sleep(RETRY_DELAY_SEC)


if __name__ == "__main__":
    run()