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
import math
import os
import signal
import threading
import time
import csv
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
SELL_QTY_EPSILON = float(os.getenv("SELL_QTY_EPSILON", "0.00000001"))
MIN_TRADABLE_QTY = float(os.getenv("MIN_TRADABLE_QTY", "0.000001"))
QUERY_ORDER_LIMIT = int(os.getenv("QUERY_ORDER_LIMIT", "100"))
STARTUP_SYNC_DELAY_SEC = float(os.getenv("STARTUP_SYNC_DELAY_SEC", "3.0"))
SYNC_STATE_ON_STARTUP = os.getenv("SYNC_STATE_ON_STARTUP", "true").lower() == "true"
SYNC_HISTORY_TO_LEDGER = os.getenv("SYNC_HISTORY_TO_LEDGER", "true").lower() == "true"
CLEAR_LEDGER_CSV_ON_STARTUP = os.getenv("CLEAR_LEDGER_CSV_ON_STARTUP", "true").lower() == "true"
MIN_HOLD_NOTIONAL_USD = float(os.getenv("MIN_HOLD_NOTIONAL_USD", "15.0"))
INVENTORY_SYNC_PAIRS = [
    p.strip().upper()
    for p in os.getenv("INVENTORY_SYNC_PAIRS", "BTC/USD,ETH/USD,SOL/USD,XRP/USD,FET/USD").split(",")
    if p.strip()
]
LOG_DIR = os.getenv("LOG_DIR", "logs")
LOG_FILE = os.getenv("LOG_FILE", os.path.join(LOG_DIR, "execution_log.csv"))

CSV_HEADERS = [
    "Timestamp",
    "Action",
    "Pair",
    "Price",
    "Quantity",
    "Requested_Qty",
    "Effective_Qty",
    "Strategy_Signal",
    "API_Status",
]

SIGNALS_CHANNEL = "trade_signals"
CONTROL_CHANNEL = "control"
RESULTS_CHANNEL = "order_results"
EXECUTION_CONFIRMATIONS_CHANNEL = "execution_confirmations"
BALANCE_URL_PATH = "/v3/balance"
CANCEL_ALL_ORDERS_PATH = "/v3/cancel_all_open_orders"
QUERY_ORDER_PATH = "/v3/query_order"
TICKER_URL_PATH = "/v3/ticker"
HISTORY_LAST_SYNC_ORDER_ID_KEY = "execution:history_last_sync_order_id"


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

    if side == "SELL" and quantity.upper() == "ALL":
        return side, pair, order_type, quantity

    parsed_qty = float(quantity)
    if parsed_qty <= 0:
        raise ValueError("quantity must be > 0")

    return side, pair, order_type, quantity


def _floor_to_decimals(value: float, decimals: int) -> float:
    scale = 10 ** decimals
    return math.floor(max(value, 0.0) * scale) / scale


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
    requested_qty: float,
    effective_qty: float,
    strategy_signal: str = "",
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
        "qty": effective_qty,
        "requested_qty": requested_qty,
        "effective_qty": effective_qty,
        "price": 0.0,
        "strategy_signal": strategy_signal,
        "order_details": order_details,
        "raw_error": raw_error,
    }


def _extract_available_balance(response_json: Any, asset: str) -> float:
    if not isinstance(response_json, dict):
        return 0.0

    # Roostoo balance shape:
    # {"Success": true, "Wallet": {"BTC": {"Free": 0.1, "Lock": 0.0}, ...}}
    for wallet_key in ("Wallet", "SpotWallet"):
        wallet = response_json.get(wallet_key)
        if isinstance(wallet, dict):
            asset_block = wallet.get(asset.upper())
            if asset_block is None:
                # Fallback for case-mismatched keys.
                for key, value in wallet.items():
                    if str(key).upper() == asset.upper():
                        asset_block = value
                        break
            if isinstance(asset_block, dict):
                free_val = asset_block.get("Free")
                if free_val is None:
                    free_val = asset_block.get("free")
                if free_val is not None:
                    try:
                        return float(free_val)
                    except (TypeError, ValueError):
                        pass

    candidates = response_json.get("Data")
    if isinstance(candidates, dict):
        candidates = [candidates]

    if not isinstance(candidates, list):
        candidates = [response_json]

    asset_upper = asset.upper()
    for item in candidates:
        if not isinstance(item, dict):
            continue

        symbol = str(item.get("Asset") or item.get("Coin") or item.get("Currency") or "").upper()
        if symbol and symbol != asset_upper:
            continue

        available = item.get("Available")
        if available is None:
            available = item.get("Balance")
        if available is None:
            available = item.get("Free")
        if available is None:
            continue

        try:
            return float(available)
        except (TypeError, ValueError):
            continue

    return 0.0


def _extract_locked_balance(response_json: Any, asset: str) -> float:
    if not isinstance(response_json, dict):
        return 0.0

    for wallet_key in ("Wallet", "SpotWallet"):
        wallet = response_json.get(wallet_key)
        if isinstance(wallet, dict):
            asset_block = wallet.get(asset.upper())
            if asset_block is None:
                for key, value in wallet.items():
                    if str(key).upper() == asset.upper():
                        asset_block = value
                        break
            if isinstance(asset_block, dict):
                lock_val = asset_block.get("Lock")
                if lock_val is None:
                    lock_val = asset_block.get("Locked")
                if lock_val is None:
                    lock_val = asset_block.get("lock")
                if lock_val is not None:
                    try:
                        return float(lock_val)
                    except (TypeError, ValueError):
                        pass

    candidates = response_json.get("Data")
    if isinstance(candidates, dict):
        candidates = [candidates]

    if not isinstance(candidates, list):
        candidates = [response_json]

    asset_upper = asset.upper()
    for item in candidates:
        if not isinstance(item, dict):
            continue

        symbol = str(item.get("Asset") or item.get("Coin") or item.get("Currency") or "").upper()
        if symbol and symbol != asset_upper:
            continue

        locked = item.get("Lock")
        if locked is None:
            locked = item.get("Locked")
        if locked is None:
            continue

        try:
            return float(locked)
        except (TypeError, ValueError):
            continue

    return 0.0


def _get_balance_snapshot(http_client: httpx.Client, pair: str) -> tuple[float, float]:
    base_asset = _base_asset_from_pair(pair)
    timestamp = _current_ms()
    params = {"timestamp": timestamp}
    payload = _sorted_params_string(params)
    signature = _sign_message(SECRET_KEY, payload)
    headers = {
        "RST-API-KEY": API_KEY,
        "MSG-SIGNATURE": signature,
    }

    url = f"{ROOSTOO_BASE_URL}{BALANCE_URL_PATH}"
    response = http_client.get(url, params={"timestamp": str(timestamp)}, headers=headers)

    try:
        response_json = response.json()
    except ValueError:
        response_json = None

    if response.status_code >= 400:
        raise RuntimeError(f"balance lookup failed: {response.status_code} {response.text}")

    free_qty = _extract_available_balance(response_json, base_asset)
    locked_qty = _extract_locked_balance(response_json, base_asset)
    return free_qty, locked_qty


def _base_asset_from_pair(pair: str) -> str:
    pair_upper = pair.strip().upper()
    if "/" in pair_upper:
        return pair_upper.split("/", 1)[0]

    for quote in ("USDT", "USDC", "BUSD", "USD"):
        if pair_upper.endswith(quote) and len(pair_upper) > len(quote):
            return pair_upper[: -len(quote)]

    return pair_upper


def _get_available_base_balance(http_client: httpx.Client, pair: str) -> float:
    base_asset = _base_asset_from_pair(pair)
    timestamp = _current_ms()
    params = {"timestamp": timestamp}
    payload = _sorted_params_string(params)
    signature = _sign_message(SECRET_KEY, payload)
    headers = {
        "RST-API-KEY": API_KEY,
        "MSG-SIGNATURE": signature,
    }

    url = f"{ROOSTOO_BASE_URL}{BALANCE_URL_PATH}"
    response = http_client.get(url, params={"timestamp": str(timestamp)}, headers=headers)

    try:
        response_json = response.json()
    except ValueError:
        response_json = None

    if response.status_code >= 400:
        raise RuntimeError(f"balance lookup failed: {response.status_code} {response.text}")

    return _extract_available_balance(response_json, base_asset)


def _cancel_all_open_orders(http_client: httpx.Client, pair: str) -> None:
    ts = _current_ms()
    params = {"pair": pair, "timestamp": ts}
    payload = _sorted_params_string(params)
    sig = _sign_message(SECRET_KEY, payload)
    headers = {
        "RST-API-KEY": API_KEY,
        "MSG-SIGNATURE": sig,
        "Content-Type": "application/x-www-form-urlencoded",
    }
    url = f"{ROOSTOO_BASE_URL}{CANCEL_ALL_ORDERS_PATH}"
    response = http_client.post(url, content=payload, headers=headers)
    if response.status_code >= 400:
        log.warning("cancel_all_open_orders returned %s: %s", response.status_code, response.text)


def _extract_actual_fill_price(response_json: Any) -> float:
    order_details = _extract_order_details(response_json)
    if isinstance(order_details, dict):
        for key in ("FilledAverPrice", "Price"):
            value = order_details.get(key)
            if value not in (None, ""):
                try:
                    return float(value)
                except (TypeError, ValueError):
                    pass
    return 0.0


def _query_orders(
    http_client: httpx.Client,
    *,
    pair: str | None = None,
    offset: int | None = None,
    limit: int = QUERY_ORDER_LIMIT,
    pending_only: bool | None = None,
) -> list[dict[str, Any]]:
    params: dict[str, Any] = {
        "timestamp": _current_ms(),
        "limit": str(max(limit, 1)),
    }
    # Roostoo query_order rejects offset=0. Omit offset unless caller provides >= 1.
    if offset is not None:
        params["offset"] = str(max(offset, 1))
    if pair:
        params["pair"] = pair.strip().upper()
    if pending_only is not None:
        params["pending_only"] = "TRUE" if pending_only else "FALSE"

    payload = _sorted_params_string(params)
    signature = _sign_message(SECRET_KEY, payload)
    headers = {
        "RST-API-KEY": API_KEY,
        "MSG-SIGNATURE": signature,
        "Content-Type": "application/x-www-form-urlencoded",
    }

    url = f"{ROOSTOO_BASE_URL}{QUERY_ORDER_PATH}"
    response = http_client.post(url, content=payload, headers=headers)

    try:
        response_json = response.json()
    except ValueError as exc:
        raise RuntimeError(f"query_order returned non-JSON: {response.text}") from exc

    if response.status_code >= 400:
        raise RuntimeError(f"query_order failed: {response.status_code} {response.text}")

    if not isinstance(response_json, dict):
        raise RuntimeError(f"query_order unsuccessful: {response_json}")

    if not bool(response_json.get("Success", False)):
        err = str(response_json.get("ErrMsg", ""))
        if "no order matched" in err.lower():
            return []
        raise RuntimeError(f"query_order unsuccessful: {err or response_json}")

    orders = response_json.get("OrderMatched", [])
    if not isinstance(orders, list):
        return []
    return [o for o in orders if isinstance(o, dict)]


def _query_all_orders(http_client: httpx.Client, *, pair: str | None = None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    offset: int | None = None
    page_size = max(QUERY_ORDER_LIMIT, 1)

    while True:
        page = _query_orders(http_client, pair=pair, offset=offset, limit=page_size)
        if not page:
            break

        out.extend(page)
        if len(page) < page_size:
            break
        offset = len(out)

    return out


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _get_mid_price(http_client: httpx.Client, pair: str) -> float:
    params = {
        "pair": pair.strip().upper(),
        "timestamp": str(_current_ms()),
    }
    response = http_client.get(f"{ROOSTOO_BASE_URL}{TICKER_URL_PATH}", params=params)

    try:
        payload = response.json()
    except ValueError:
        return 0.0

    if response.status_code >= 400 or not isinstance(payload, dict) or not bool(payload.get("Success", False)):
        return 0.0

    data = payload.get("Data")
    if not isinstance(data, dict):
        return 0.0

    pair_block = data.get(pair.upper())
    if not isinstance(pair_block, dict):
        return 0.0

    max_bid = _safe_float(pair_block.get("MaxBid"))
    min_ask = _safe_float(pair_block.get("MinAsk"))
    if max_bid <= 0.0 or min_ask <= 0.0:
        return 0.0

    return (max_bid + min_ask) / 2.0


def _pair_step_decimals(pair: str) -> int:
    pair_upper = pair.upper()
    if pair_upper in {"SOL/USD", "XRP/USD"}:
        return 0
    if pair_upper == "FET/USD":
        return 1
    return 4


def _format_qty_for_pair(pair: str, qty: float) -> str:
    decimals = _pair_step_decimals(pair)
    q = _floor_to_decimals(max(qty, 0.0), decimals)
    if decimals == 0:
        return str(int(q))
    return f"{q:.{decimals}f}"


def _strategy_state_from_inventory(
    pair: str,
    held_qty: float,
    entry_price: float,
    reference_price: float,
) -> dict[str, str]:
    qty_text = _format_qty_for_pair(pair, held_qty)
    rounded_qty = _safe_float(qty_text)
    notional_usd = max(rounded_qty, 0.0) * max(reference_price, 0.0)
    in_position = (
        rounded_qty >= MIN_TRADABLE_QTY
        and (
            reference_price <= 0.0
            or notional_usd >= MIN_HOLD_NOTIONAL_USD
        )
    )
    pair_upper = pair.upper()

    if pair_upper in {"SOL/USD", "XRP/USD", "FET/USD"}:
        return {
            "in_position": str(in_position),
            "open_position_quantity": qty_text,
            "entry_price": str(max(entry_price, 0.0)),
            "atr_at_entry": "0.0",
            "cooldown_until": "0.0",
        }

    return {
        "in_position": str(in_position),
        "open_position_quantity": qty_text,
        "entry_price": str(max(entry_price, 0.0)),
        "stop_loss_price": "0.0",
        "half_position_sold": "False",
        "cooldown_until": "0.0",
    }


def _estimate_entry_price_from_orders(orders: list[dict[str, Any]]) -> float:
    avg_cost = 0.0
    qty = 0.0

    sortable = sorted(
        orders,
        key=lambda o: (_safe_int(o.get("CreateTimestamp")), _safe_int(o.get("OrderID"))),
    )
    for order in sortable:
        status = str(order.get("Status", "")).upper()
        if status != "FILLED":
            continue

        side = str(order.get("Side", "")).upper()
        filled_qty = _safe_float(order.get("FilledQuantity") or order.get("Quantity"))
        if filled_qty <= 0:
            continue

        fill_price = _safe_float(order.get("FilledAverPrice") or order.get("Price"))
        if fill_price <= 0:
            continue

        if side == "BUY":
            total_cost = avg_cost * qty + fill_price * filled_qty
            qty += filled_qty
            avg_cost = total_cost / qty if qty > 0 else 0.0
        elif side == "SELL":
            qty = max(qty - filled_qty, 0.0)
            if qty <= 0.0:
                avg_cost = 0.0

    return avg_cost


def _build_history_result_payload(order: dict[str, Any]) -> dict[str, Any]:
    status = str(order.get("Status", "")).upper()
    result = "SUCCESS" if status == "FILLED" else "FAILED"
    ts = _safe_int(order.get("FinishTimestamp")) or _safe_int(order.get("CreateTimestamp"))
    return {
        "timestamp": ts or int(time.time()),
        "result": result,
        "status_code": 200,
        "latency_ms": 0,
        "order_id": order.get("OrderID"),
        "side": str(order.get("Side", "")).upper(),
        "symbol": str(order.get("Pair", "")).upper(),
        "qty": _safe_float(order.get("FilledQuantity") or order.get("Quantity")),
        "requested_qty": _safe_float(order.get("Quantity")),
        "effective_qty": _safe_float(order.get("FilledQuantity") or order.get("Quantity")),
        "price": _safe_float(order.get("FilledAverPrice") or order.get("Price")),
        "order_details": order,
        "strategy_signal": "ROOSTOO_HISTORY_SYNC",
        "raw_error": "" if result == "SUCCESS" else f"historical_status={status or 'UNKNOWN'}",
    }


def _sync_strategy_state_from_roostoo(http_client: httpx.Client, redis_client: redis.Redis) -> None:
    synced = 0
    for pair in INVENTORY_SYNC_PAIRS:
        free_qty, locked_qty = _get_balance_snapshot(http_client, pair)
        held_qty = max(free_qty + locked_qty, 0.0)
        pair_orders = _query_all_orders(http_client, pair=pair)
        entry_price = _estimate_entry_price_from_orders(pair_orders)
        market_price = _get_mid_price(http_client, pair)
        ref_price = market_price if market_price > 0.0 else entry_price

        state_key = f"strategy_state:{pair}"
        mapping = _strategy_state_from_inventory(pair, held_qty, entry_price, ref_price)
        redis_client.hset(state_key, mapping=mapping)
        synced += 1
        log.info(
            "Startup state sync | pair=%s held=%.8f entry=%.8f ref=%.8f in_position=%s key=%s",
            pair,
            held_qty,
            entry_price,
            ref_price,
            mapping.get("in_position"),
            state_key,
        )

    log.info("Startup strategy state sync completed for %d pairs.", synced)


def _sync_order_history_to_ledger(http_client: httpx.Client, redis_client: redis.Redis) -> None:
    all_orders = _query_all_orders(http_client, pair=None)
    if not all_orders:
        log.info("History sync: no orders returned from query_order.")
        return

    all_orders.sort(key=lambda o: _safe_int(o.get("OrderID")))

    last_synced = _safe_int(redis_client.get(HISTORY_LAST_SYNC_ORDER_ID_KEY))
    published = 0
    max_order_id = last_synced

    for order in all_orders:
        order_id = _safe_int(order.get("OrderID"))
        if order_id <= last_synced:
            continue

        result_payload = _build_history_result_payload(order)
        redis_client.publish(RESULTS_CHANNEL, json.dumps(result_payload))
        published += 1
        if order_id > max_order_id:
            max_order_id = order_id

    if max_order_id > last_synced:
        redis_client.set(HISTORY_LAST_SYNC_ORDER_ID_KEY, str(max_order_id))

    log.info(
        "History sync complete | published=%d last_order_id=%d",
        published,
        max_order_id,
    )


def _clear_ledger_csv_file(redis_client: redis.Redis) -> None:
    if not CLEAR_LEDGER_CSV_ON_STARTUP:
        return

    try:
        os.makedirs(os.path.dirname(LOG_FILE) or ".", exist_ok=True)
        with open(LOG_FILE, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_HEADERS)
            writer.writeheader()
        # Force a full history replay after CSV reset.
        redis_client.set(HISTORY_LAST_SYNC_ORDER_ID_KEY, "0")
        log.info("Startup CSV reset complete | file=%s", LOG_FILE)
    except OSError as exc:
        log.warning("Startup CSV reset skipped (unable to write %s): %s", LOG_FILE, exc)


def _publish_execution_confirmation(
    redis_client: redis.Redis,
    *,
    signal_payload: dict[str, Any],
    result_payload: dict[str, Any],
) -> None:
    confirmation = {
        "action": str(signal_payload.get("action", "")).upper(),
        "symbol": str(signal_payload.get("symbol", DEFAULT_PAIR)),
        "reason": str(signal_payload.get("reason", "")),
        "result": result_payload.get("result", "FAILED"),
        "status_code": int(result_payload.get("status_code", 0)),
        "requested_qty": float(result_payload.get("requested_qty", 0.0)),
        "effective_qty": float(result_payload.get("effective_qty", 0.0)),
        "timestamp": int(time.time()),
    }
    redis_client.publish(EXECUTION_CONFIRMATIONS_CHANNEL, json.dumps(confirmation))


def execute_trade_signal(
    signal_payload: dict[str, Any],
    http_client: httpx.Client,
    redis_client: redis.Redis,
    throttle: ExecutionThrottle,
) -> dict[str, Any]:
    """Validate, sign, execute, and publish one trade signal."""
    side, pair, order_type, quantity = _validate_signal(signal_payload)
    strategy_signal = str(signal_payload.get("reason") or "").strip() or "LIVE_SIGNAL"
    requested_qty = float(quantity) if quantity.upper() != "ALL" else -1.0
    effective_qty = requested_qty

    if side == "SELL":
        available_qty, locked_qty = _get_balance_snapshot(http_client, pair)
        if available_qty < MIN_TRADABLE_QTY and locked_qty > MIN_TRADABLE_QTY:
            log.warning(
                "SELL requested but free balance is low while locked balance exists (free=%.8f locked=%.8f). "
                "Cancelling open orders and retrying free balance.",
                available_qty,
                locked_qty,
            )
            _cancel_all_open_orders(http_client, pair)
            time.sleep(0.2)
            available_qty = _get_available_base_balance(http_client, pair)

        if quantity.upper() == "ALL":
            # SELL ALL should use exchange-compatible precision to avoid step-size rejects.
            effective_qty = _floor_to_decimals(available_qty, 4)
            requested_qty = available_qty
        else:
            decimals = _pair_step_decimals(pair)
            if decimals == 0:
                # Integer-quantity pairs: epsilon subtraction causes floor() to drop a whole unit.
                # Skip epsilon; just floor to 0 decimals directly.
                effective_qty = _floor_to_decimals(min(requested_qty, max(available_qty, 0.0)), 0)
            else:
                effective_qty = _floor_to_decimals(
                    min(requested_qty, max(available_qty - SELL_QTY_EPSILON, 0.0)),
                    decimals,
                )

        if effective_qty < MIN_TRADABLE_QTY:
            result = _build_order_result(
                success=False,
                status_code=400,
                latency_ms=0,
                side=side,
                pair=pair,
                quantity=f"{effective_qty:.8f}",
                requested_qty=requested_qty,
                effective_qty=effective_qty,
                strategy_signal=strategy_signal,
                response_json=None,
                raw_error=(
                    "sell skipped: insufficient available balance after clamp "
                    f"(requested={requested_qty:.8f}, available={available_qty:.8f}, locked={locked_qty:.8f})"
                ),
            )
            redis_client.publish(RESULTS_CHANNEL, json.dumps(result))
            return result

    effective_qty = _floor_to_decimals(effective_qty, 8)

    if effective_qty < MIN_TRADABLE_QTY:
        result = _build_order_result(
            success=False,
            status_code=400,
            latency_ms=0,
            side=side,
            pair=pair,
            quantity=f"{effective_qty:.8f}",
            requested_qty=requested_qty,
            effective_qty=effective_qty,
            strategy_signal=strategy_signal,
            response_json=None,
            raw_error=(
                "order skipped: effective quantity below minimum tradable "
                f"(effective={effective_qty:.8f}, min={MIN_TRADABLE_QTY:.8f})"
            ),
        )
        redis_client.publish(RESULTS_CHANNEL, json.dumps(result))
        return result

    quantity = _format_qty_for_pair(pair, effective_qty)

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

    _cancel_all_open_orders(http_client, pair)

    url = f"{ROOSTOO_BASE_URL}/v3/place_order"
    start = time.perf_counter()
    response = http_client.post(url, content=payload, headers=headers)
    if response.status_code == 429:
        log.warning("HTTP 429 detected. Sleeping 0.5s and retrying once.")
        time.sleep(0.5)
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

    expected_price = float(signal_payload.get("expected_price", 0.0) or 0.0)
    actual_fill_price = _extract_actual_fill_price(response_json)
    if expected_price > 0.0 and actual_fill_price > 0.0:
        slippage_pct = ((actual_fill_price - expected_price) / expected_price) * 100.0
        log.info(
            "Slippage | expected=%.8f actual=%.8f slippage_pct=%.5f",
            expected_price,
            actual_fill_price,
            slippage_pct,
        )

    result = _build_order_result(
        success=success,
        status_code=response.status_code,
        latency_ms=latency_ms,
        side=side,
        pair=pair,
        quantity=quantity,
        requested_qty=requested_qty,
        effective_qty=effective_qty,
        strategy_signal=strategy_signal,
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
        _publish_execution_confirmation(
            redis_client,
            signal_payload=signal_payload,
            result_payload=result,
        )
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
            "requested_qty": safe_qty,
            "effective_qty": safe_qty,
            "price": 0.0,
            "strategy_signal": str(signal_payload.get("reason") or "").strip() or "LIVE_SIGNAL",
            "order_details": None,
            "raw_error": str(exc),
        }
        redis_client.publish(RESULTS_CHANNEL, json.dumps(fallback))
        _publish_execution_confirmation(
            redis_client,
            signal_payload=signal_payload,
            result_payload=fallback,
        )
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

                if SYNC_STATE_ON_STARTUP or SYNC_HISTORY_TO_LEDGER:
                    time.sleep(max(STARTUP_SYNC_DELAY_SEC, 0.0))

                if SYNC_HISTORY_TO_LEDGER:
                    _clear_ledger_csv_file(r)
                    _sync_order_history_to_ledger(http_client, r)

                if SYNC_STATE_ON_STARTUP:
                    _sync_strategy_state_from_roostoo(http_client, r)

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