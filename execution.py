"""
Process C — Execution Manager
===============================
Subscribes to `trade_signals` (from Strategy Engine) and `control`
(from API Gateway / Kill Switch).

On a trade signal  : formats the Binance REST payload, POSTs to
                     /api/v3/order (Spot Testnet), then publishes the
                     exchange response to `order_results`.

On a HALT command  : cancels all open orders on the symbol, sets the
                     internal kill-switch flag, and exits cleanly.

Redis channels
  IN  — trade_signals  {"action": "BUY"|"SELL", "symbol": ..., "type": "MARKET"}
  IN  — control        {"command": "HALT"}
  OUT — order_results  {"order_id": int, "symbol": str, "side": str,
                        "price": float, "qty": float, "result": "SUCCESS"|"FAILED",
                        "timestamp": int}
"""

import json
import logging
import math
import os
import signal
import sys
import threading
import time

import redis
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException
from dotenv import load_dotenv

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────
REDIS_HOST    = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT    = int(os.getenv("REDIS_PORT", 6379))
API_KEY       = os.getenv("BINANCE_API_KEY", "")
API_SECRET    = os.getenv("BINANCE_API_SECRET", "")
SYMBOL        = os.getenv("TRADE_SYMBOL", "BTCUSDT")
TRADE_QTY     = float(os.getenv("TRADE_QUANTITY", "0.001"))  # fallback / minimum qty
RISK_PCT      = float(os.getenv("RISK_PCT", "0.01"))         # fraction of free quote balance to risk per trade (1 %)

SIGNALS_CHANNEL = "trade_signals"
CONTROL_CHANNEL = "control"
RESULTS_CHANNEL = "order_results"
RETRY_DELAY     = 5   # seconds between Redis reconnect attempts

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Execution] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

# ── Kill-switch ────────────────────────────────────────────────────────────────
# threading.Event is safe to check from the main loop and to set from the
# control-message handler running in the same thread.
_halted = threading.Event()


# ══════════════════════════════════════════════════════════════════════════════
# Business logic — no Redis or I/O side-effects at this layer.
# These pure/injectable functions are fully unit-testable.
# ══════════════════════════════════════════════════════════════════════════════

def _get_dynamic_qty(client: Client, symbol: str) -> float:
    """
    Calculate order quantity as RISK_PCT of the free quote-asset balance.

    Steps:
      1. Fetch the current mid price via /api/v3/ticker/price.
      2. Fetch free USDT balance from /api/v3/account.
      3. qty = floor((free_usdt * RISK_PCT) / price, 5 decimal places).
      4. Enforce a floor of TRADE_QTY so we never send a sub-minimum order.

    Falls back silently to TRADE_QTY on any error so a transient Binance
    hiccup never blocks order execution.
    """
    try:
        ticker        = client.get_symbol_ticker(symbol=symbol)
        current_price = float(ticker["price"])
        if current_price <= 0:
            raise ValueError(f"Non-positive price returned: {current_price}")

        account       = client.get_account()
        quote_asset   = symbol[len(symbol.rstrip("USDT")):] if symbol.endswith("USDT") else "USDT"
        free_balance  = next(
            (float(b["free"]) for b in account["balances"] if b["asset"] == quote_asset),
            0.0,
        )

        # Truncate (not round) to 5 d.p. — Binance rejects qty with too many decimals
        raw_qty = (free_balance * RISK_PCT) / current_price
        qty     = math.floor(raw_qty * 1e5) / 1e5

        if qty < TRADE_QTY:
            log.warning(
                "Dynamic qty %.5f BTC is below minimum %.5f. Using minimum.",
                qty, TRADE_QTY,
            )
            return TRADE_QTY

        log.info(
            "Dynamic sizing: %.2f %s × %.0f%% ÷ %.2f = %.5f %s",
            free_balance, quote_asset, RISK_PCT * 100, current_price,
            qty, symbol[:3],
        )
        return qty

    except Exception as exc:
        log.warning("Dynamic sizing failed (%s). Falling back to TRADE_QTY=%.5f.", exc, TRADE_QTY)
        return TRADE_QTY


def format_order_payload(signal: dict, quantity: float = TRADE_QTY) -> dict:
    """
    Convert a trade_signals payload into Binance REST order parameters.

    WHY a separate function: the mapping from our internal schema to the
    Binance schema is a distinct concern. Isolating it means we can unit-test
    the translation logic without touching any network code.

    Returns a dict ready to be unpacked into client.create_order(**payload).

    Raises ValueError if the action or order type is unrecognised.
    """
    action = signal.get("action", "").upper()
    order_type = signal.get("type", "MARKET").upper()

    if action not in ("BUY", "SELL"):
        raise ValueError(f"Unknown action: {action!r}. Expected 'BUY' or 'SELL'.")
    if order_type not in ("MARKET", "LIMIT"):
        raise ValueError(f"Unsupported order type: {order_type!r}.")

    payload: dict = {
        "symbol":   signal.get("symbol", SYMBOL),
        "side":     action,
        "type":     order_type,
        "quantity": quantity,
    }

    # LIMIT orders additionally require a price and a timeInForce.
    if order_type == "LIMIT":
        if "price" not in signal:
            raise ValueError("LIMIT order requires a 'price' field in the signal.")
        payload["price"]       = signal["price"]
        payload["timeInForce"] = signal.get("timeInForce", "GTC")

    return payload


def place_order(client: Client, order_params: dict) -> dict:
    """
    Fire the REST POST to Binance and normalise the response.

    WHY always returns, never raises: a FAILED result is still meaningful
    data for the Ledger. If this function raised, the caller would need
    duplicate try/except blocks AND the failure would never reach the Ledger.

    Returns a result dict that is published verbatim to order_results.
    """
    try:
        resp = client.create_order(**order_params)

        # Binance fills may be empty for MARKET orders on testnet; default to 0.
        avg_price = _parse_avg_price(resp)
        qty       = float(resp.get("executedQty") or resp.get("origQty") or 0)

        return {
            "order_id":  resp["orderId"],
            "symbol":    resp["symbol"],
            "side":      resp["side"],
            "price":     avg_price,
            "qty":       qty,
            "result":    "SUCCESS",
            "timestamp": int(resp.get("transactTime", time.time() * 1000) / 1000),
        }

    except BinanceAPIException as exc:
        log.error("Binance API error: code=%s msg=%s", exc.code, exc.message)
        return _failed_result(order_params, reason=exc.message)

    except BinanceRequestException as exc:
        log.error("Binance request error: %s", exc)
        return _failed_result(order_params, reason=str(exc))

    except Exception as exc:
        log.error("Unexpected error placing order: %s", exc)
        return _failed_result(order_params, reason=str(exc))


def handle_signal(
    raw_data: str,
    client: Client,
    redis_client: redis.Redis,
) -> dict | None:
    """
    Full signal lifecycle: parse → validate → place → publish.

    WHY injectable client/redis: allows tests to pass mock objects without
    monkey-patching globals. The caller (run()) wires in the real instances.

    Returns the result dict that was published, or None if the message was
    skipped (e.g. malformed JSON, process halted).
    """
    if _halted.is_set():
        log.warning("Halted — signal ignored.")
        return None

    try:
        signal = json.loads(raw_data)
        qty = _get_dynamic_qty(client, signal.get("symbol", SYMBOL))
        order_params = format_order_payload(signal, quantity=qty)
    except (json.JSONDecodeError, ValueError) as exc:
        log.warning("Bad signal message (%s): %r", exc, raw_data)
        return None

    log.info(
        "Executing %s %s %s qty=%.4f …",
        order_params["side"],
        order_params["symbol"],
        order_params["type"],
        order_params["quantity"],
    )

    result = place_order(client, order_params)
    redis_client.publish(RESULTS_CHANNEL, json.dumps(result))

    log.info(
        "Published to %s → order_id=%s result=%s",
        RESULTS_CHANNEL,
        result.get("order_id"),
        result["result"],
    )
    return result


def handle_control(
    raw_data: str,
    client: Client,
) -> None:
    """
    Process a message from the `control` channel.

    WHY halting is separate from the main loop: the control channel must be
    responsive even when a trade is mid-flight. This handler is called inline
    from the same pubsub loop so there is no thread-safety issue.
    """
    try:
        msg = json.loads(raw_data)
    except json.JSONDecodeError:
        log.warning("Malformed control message: %r", raw_data)
        return

    command = msg.get("command", "").upper()

    if command == "HALT":
        log.warning("HALT command received. Cancelling open orders and shutting down …")
        _halted.set()

        try:
            client.cancel_open_orders(symbol=SYMBOL)
            log.info("All open orders for %s cancelled.", SYMBOL)
        except (BinanceAPIException, BinanceRequestException, Exception) as exc:
            # Log but do NOT abort the shutdown — halting is safety-critical.
            log.error("Could not cancel open orders: %s", exc)

        _eod_shutdown()

    else:
        log.info("Unknown control command ignored: %r", command)


# ── Internal helpers ───────────────────────────────────────────────────────────

def _parse_avg_price(resp: dict) -> float:
    """
    Extract the average fill price from a Binance order response.

    MARKET orders report fills inside resp["fills"]; LIMIT orders report
    price directly. Testnet sometimes returns empty fills — default to 0.0.
    """
    fills = resp.get("fills", [])
    if fills:
        total_qty  = sum(float(f["qty"])   for f in fills)
        total_cost = sum(float(f["price"]) * float(f["qty"]) for f in fills)
        return round(total_cost / total_qty, 2) if total_qty else 0.0
    return float(resp.get("price") or 0.0)


def _failed_result(order_params: dict, reason: str = "") -> dict:
    """Build a normalised FAILED result dict so the Ledger always gets a message."""
    return {
        "order_id":  None,
        "symbol":    order_params.get("symbol", SYMBOL),
        "side":      order_params.get("side", "UNKNOWN"),
        "price":     0.0,
        "qty":       0.0,
        "result":    "FAILED",
        "reason":    reason,
        "timestamp": int(time.time()),
    }


def _eod_shutdown() -> None:
    log.info("Execution Manager halted. Goodbye.")
    sys.exit(0)


# ── Signal handler (OS signals) ───────────────────────────────────────────────
def _handle_os_signal(signum, frame):  # noqa: ANN001
    log.info("OS signal %s received. Shutting down gracefully …", signum)
    _halted.set()
    _eod_shutdown()


signal.signal(signal.SIGINT,  _handle_os_signal)
signal.signal(signal.SIGTERM, _handle_os_signal)


# ── Main loop ──────────────────────────────────────────────────────────────────
def run() -> None:
    log.info(
        "Execution Manager starting. Subscribing to '%s' and '%s'.",
        SIGNALS_CHANNEL,
        CONTROL_CHANNEL,
    )

    binance_client = Client(API_KEY, API_SECRET, testnet=True)

    while True:
        if _halted.is_set():
            break

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
            pubsub.subscribe(SIGNALS_CHANNEL, CONTROL_CHANNEL)
            log.info("Listening …")

            for message in pubsub.listen():
                if _halted.is_set():
                    log.info("Kill switch active — exiting message loop.")
                    return

                if message["type"] != "message":
                    continue

                channel  = message["channel"]
                raw_data = message["data"]

                if channel == SIGNALS_CHANNEL:
                    handle_signal(raw_data, binance_client, r)

                elif channel == CONTROL_CHANNEL:
                    handle_control(raw_data, binance_client)
                    # handle_control calls sys.exit on HALT, but if it returns
                    # for any other (unknown) command we simply continue.

        except redis.exceptions.ConnectionError as exc:
            log.error("Redis connection lost: %s. Retrying in %ds …", exc, RETRY_DELAY)
            time.sleep(RETRY_DELAY)
        except redis.exceptions.RedisError as exc:
            log.error("Redis error: %s. Retrying in %ds …", exc, RETRY_DELAY)
            time.sleep(RETRY_DELAY)


if __name__ == "__main__":
    run()
