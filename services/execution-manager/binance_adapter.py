"""Binance Spot Testnet adapter for the execution manager."""

from __future__ import annotations

import logging
import os
import time
from typing import Any

from binance.client import Client as BinanceClient
from binance.exceptions import BinanceAPIException

log = logging.getLogger(__name__)

PAIR_MAP = {
    "BTC/USD": "BTCUSDT",
    "ETH/USD": "ETHUSDT",
    "SOL/USD": "SOLUSDT",
    "XRP/USD": "XRPUSDT",
    "FET/USD": "FETUSDT",
}
PAIR_MAP_REVERSE = {v: k for k, v in PAIR_MAP.items()}

_client: BinanceClient | None = None
_precision_cache: dict[str, int] = {}


def _get_client() -> BinanceClient:
    global _client
    if _client is None:
        _client = BinanceClient(
            os.environ["BINANCE_API_KEY"],
            os.environ["BINANCE_API_SECRET"],
            testnet=True,
        )
    return _client


def _to_binance_symbol(pair: str) -> str:
    key = pair.strip().upper()
    if key not in PAIR_MAP:
        raise ValueError(f"Unsupported pair for Binance: {pair}")
    return PAIR_MAP[key]


def _to_internal_pair(binance_symbol: str) -> str:
    return PAIR_MAP_REVERSE.get(binance_symbol, binance_symbol)


def get_balance(pair: str) -> tuple[float, float]:
    """Return (free, locked) for the base asset of the given pair."""
    client = _get_client()
    base_asset = pair.strip().upper().split("/")[0]
    account = client.get_account()
    for b in account.get("balances", []):
        if b["asset"] == base_asset:
            return float(b["free"]), float(b["locked"])
    return 0.0, 0.0


def get_mid_price(pair: str) -> float:
    client = _get_client()
    ticker = client.get_orderbook_ticker(symbol=_to_binance_symbol(pair))
    bid = float(ticker["bidPrice"])
    ask = float(ticker["askPrice"])
    return (bid + ask) / 2.0


def get_pair_precision(pair: str) -> int:
    binance_sym = _to_binance_symbol(pair)
    if binance_sym in _precision_cache:
        return _precision_cache[binance_sym]
    client = _get_client()
    info = client.get_symbol_info(binance_sym)
    for f in info.get("filters", []):
        if f["filterType"] == "LOT_SIZE":
            step = f["stepSize"]
            if "." in step:
                decimals = len(step.rstrip("0").split(".")[1])
            else:
                decimals = 0
            _precision_cache[binance_sym] = decimals
            return decimals
    _precision_cache[binance_sym] = 8
    return 8


def cancel_all_orders(pair: str) -> None:
    client = _get_client()
    binance_sym = _to_binance_symbol(pair)
    try:
        client.cancel_all_open_orders(symbol=binance_sym)
    except BinanceAPIException as exc:
        if exc.code != -2011:  # "Unknown order" is fine
            raise


def query_orders(pair: str, limit: int = 100) -> list[dict[str, Any]]:
    client = _get_client()
    binance_sym = _to_binance_symbol(pair)
    internal_pair = pair.strip().upper()
    raw = client.get_all_orders(symbol=binance_sym, limit=limit)
    out: list[dict[str, Any]] = []
    for o in raw:
        filled_qty = float(o.get("executedQty", 0))
        orig_qty = float(o.get("origQty", 0))
        price = float(o.get("price", 0))
        out.append({
            "OrderID": o["orderId"],
            "Status": o.get("status", "UNKNOWN"),
            "Side": o.get("side", ""),
            "Pair": internal_pair,
            "Quantity": orig_qty,
            "FilledQuantity": filled_qty,
            "FilledAverPrice": price,
            "CreateTime": o.get("time", 0),
            "UpdateTime": o.get("updateTime", 0),
        })
    return out


def place_order(pair: str, side: str, quantity: str, order_type: str) -> dict[str, Any]:
    """Place an order and return a result dict matching the internal format."""
    client = _get_client()
    binance_sym = _to_binance_symbol(pair)
    internal_pair = pair.strip().upper()

    try:
        resp = client.create_order(
            symbol=binance_sym,
            side=side.upper(),
            type=order_type.upper(),
            quantity=quantity,
        )

        filled_qty = float(resp.get("executedQty", 0))
        fills = resp.get("fills", [])
        if fills:
            total_cost = sum(float(f["price"]) * float(f["qty"]) for f in fills)
            total_qty = sum(float(f["qty"]) for f in fills)
            avg_price = total_cost / total_qty if total_qty else 0.0
        else:
            avg_price = float(resp.get("price", 0))

        order_detail = {
            "OrderID": resp["orderId"],
            "Status": resp.get("status", ""),
            "Side": side.upper(),
            "Pair": internal_pair,
            "Quantity": float(quantity),
            "FilledQuantity": filled_qty,
            "FilledAverPrice": avg_price,
        }

        return {
            "Success": True,
            "OrderDetail": order_detail,
            "result": "SUCCESS",
            "status_code": 200,
            "latency_ms": 0,
            "order_id": resp["orderId"],
            "side": side.upper(),
            "symbol": internal_pair,
            "qty": filled_qty,
            "requested_qty": float(quantity),
            "effective_qty": filled_qty,
            "price": avg_price,
            "order_details": order_detail,
            "raw_error": "",
        }

    except BinanceAPIException as exc:
        return {
            "Success": False,
            "result": "FAILED",
            "status_code": exc.status_code,
            "latency_ms": 0,
            "order_id": None,
            "side": side.upper(),
            "symbol": internal_pair,
            "qty": 0.0,
            "requested_qty": float(quantity),
            "effective_qty": 0.0,
            "price": 0.0,
            "order_details": None,
            "raw_error": str(exc.message),
        }
