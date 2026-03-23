"""
Binance Testnet Adapter — Execution Service
=============================================
Provides exchange operations (place order, get balance, cancel orders)
against Binance Spot Testnet, returning data normalised to the internal
format the rest of the system expects.

Internal pair format (BTC/USD) is converted to Binance format (BTCUSDT)
at the boundary.  Callers never see Binance-specific symbols.
"""

from __future__ import annotations

import logging
import math
import os
import time
from typing import Any

from binance.client import Client as BinanceClient
from binance.exceptions import BinanceAPIException
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "").strip()

PAIR_MAP: dict[str, str] = {
    "BTC/USD": "BTCUSDT",
    "ETH/USD": "ETHUSDT",
    "SOL/USD": "SOLUSDT",
    "XRP/USD": "XRPUSDT",
    "FET/USD": "FETUSDT",
}
PAIR_MAP_REVERSE: dict[str, str] = {v: k for k, v in PAIR_MAP.items()}

_client: BinanceClient | None = None
_precision_cache: dict[str, int] = {}


def _get_client() -> BinanceClient:
    global _client
    if _client is None:
        _client = BinanceClient(
            BINANCE_API_KEY,
            BINANCE_API_SECRET,
            testnet=True,
        )
        log.info("Binance Testnet client initialised.")
    return _client


def _to_binance_symbol(pair: str) -> str:
    sym = PAIR_MAP.get(pair.upper())
    if sym is None:
        raise ValueError(f"No Binance mapping for pair {pair!r}")
    return sym


def _base_asset(pair: str) -> str:
    return pair.split("/")[0].upper()


# ── Precision ─────────────────────────────────────────────────────────────────

def get_pair_precision(pair: str) -> int:
    """Return quantity decimal places for *pair* from Binance exchangeInfo (cached)."""
    binance_sym = _to_binance_symbol(pair)
    if binance_sym in _precision_cache:
        return _precision_cache[binance_sym]

    client = _get_client()
    info = client.get_symbol_info(binance_sym)
    if info is None:
        log.warning("No exchangeInfo for %s; defaulting to 4 decimals.", binance_sym)
        _precision_cache[binance_sym] = 4
        return 4

    for filt in info.get("filters", []):
        if filt.get("filterType") == "LOT_SIZE":
            step = float(filt["stepSize"])
            if step > 0:
                decimals = max(int(round(-math.log10(step))), 0)
                _precision_cache[binance_sym] = decimals
                return decimals

    _precision_cache[binance_sym] = 4
    return 4


# ── Balance ───────────────────────────────────────────────────────────────────

def get_balance(pair: str) -> tuple[float, float]:
    """Return (free, locked) balance for the base asset of *pair*."""
    client = _get_client()
    asset = _base_asset(pair)
    account = client.get_account()

    for entry in account.get("balances", []):
        if entry["asset"].upper() == asset:
            return float(entry["free"]), float(entry["locked"])

    return 0.0, 0.0


# ── Mid-price ─────────────────────────────────────────────────────────────────

def get_mid_price(pair: str) -> float:
    """Return mid-price for *pair* from Binance order-book ticker."""
    client = _get_client()
    binance_sym = _to_binance_symbol(pair)
    ticker = client.get_orderbook_ticker(symbol=binance_sym)

    bid = float(ticker["bidPrice"])
    ask = float(ticker["askPrice"])
    if bid <= 0 or ask <= 0:
        return 0.0
    return (bid + ask) / 2.0


# ── Cancel orders ─────────────────────────────────────────────────────────────

def cancel_all_orders(pair: str) -> None:
    """Cancel all open orders for *pair* on Binance Testnet."""
    client = _get_client()
    binance_sym = _to_binance_symbol(pair)
    try:
        client.get_open_orders(symbol=binance_sym)
        client.cancel_all_orders(symbol=binance_sym)
    except BinanceAPIException as exc:
        if exc.code == -2011:  # "Unknown order"
            pass
        else:
            log.warning("Binance cancel_all_orders for %s: %s", binance_sym, exc)


# ── Place order ───────────────────────────────────────────────────────────────

def place_order(
    pair: str,
    side: str,
    quantity: str,
    order_type: str,
) -> dict[str, Any]:
    """
    Place an order on Binance Testnet and return an internal-format result dict.

    The result dict has the same shape as what the Roostoo code path builds
    via ``_build_order_result``, so calling code doesn't need to branch.
    """
    client = _get_client()
    binance_sym = _to_binance_symbol(pair)
    start = time.perf_counter()

    try:
        if order_type.upper() == "MARKET":
            resp = client.create_order(
                symbol=binance_sym,
                side=side.upper(),
                type="MARKET",
                quantity=quantity,
            )
        else:
            raise ValueError(f"Unsupported order type for Binance adapter: {order_type}")

        latency_ms = int((time.perf_counter() - start) * 1000)

        filled_qty = float(resp.get("executedQty", 0))
        fills = resp.get("fills", [])
        if fills:
            total_cost = sum(float(f["price"]) * float(f["qty"]) for f in fills)
            total_qty = sum(float(f["qty"]) for f in fills)
            avg_price = total_cost / total_qty if total_qty > 0 else 0.0
        else:
            avg_price = float(resp.get("price", 0))

        order_details = {
            "OrderID": resp.get("orderId"),
            "Status": resp.get("status", "UNKNOWN"),
            "Side": resp.get("side", side.upper()),
            "Pair": pair,
            "FilledQuantity": filled_qty,
            "FilledAverPrice": avg_price,
            "Quantity": float(resp.get("origQty", quantity)),
            "Price": avg_price,
        }

        success = resp.get("status") == "FILLED"

        return {
            "timestamp": int(time.time()),
            "result": "SUCCESS" if success else "FAILED",
            "status_code": 200,
            "latency_ms": latency_ms,
            "order_id": resp.get("orderId"),
            "side": side.upper(),
            "symbol": pair,
            "qty": filled_qty,
            "requested_qty": float(quantity),
            "effective_qty": filled_qty,
            "price": avg_price,
            "order_details": order_details,
            "raw_error": "",
            "Success": success,
            "OrderDetail": order_details,
        }

    except BinanceAPIException as exc:
        latency_ms = int((time.perf_counter() - start) * 1000)
        return {
            "timestamp": int(time.time()),
            "result": "FAILED",
            "status_code": exc.status_code,
            "latency_ms": latency_ms,
            "order_id": None,
            "side": side.upper(),
            "symbol": pair,
            "qty": 0.0,
            "requested_qty": float(quantity),
            "effective_qty": 0.0,
            "price": 0.0,
            "order_details": None,
            "raw_error": str(exc),
            "Success": False,
        }


# ── Query orders ──────────────────────────────────────────────────────────────

def query_orders(pair: str, limit: int = 100) -> list[dict[str, Any]]:
    """Query order history for *pair*, returning Roostoo-normalised dicts."""
    client = _get_client()
    binance_sym = _to_binance_symbol(pair)

    try:
        raw_orders = client.get_all_orders(symbol=binance_sym, limit=limit)
    except BinanceAPIException as exc:
        log.warning("Binance query_orders for %s: %s", binance_sym, exc)
        return []

    normalised = []
    for o in raw_orders:
        status = o.get("status", "UNKNOWN")
        normalised.append({
            "OrderID": o.get("orderId"),
            "Status": "FILLED" if status == "FILLED" else status,
            "Side": o.get("side", ""),
            "Pair": pair,
            "Quantity": float(o.get("origQty", 0)),
            "FilledQuantity": float(o.get("executedQty", 0)),
            "FilledAverPrice": float(o.get("price", 0)),
            "Price": float(o.get("price", 0)),
            "CreateTimestamp": o.get("time", 0),
            "FinishTimestamp": o.get("updateTime", 0),
        })

    return normalised
