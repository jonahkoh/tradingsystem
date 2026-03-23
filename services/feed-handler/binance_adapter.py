"""Binance Spot Testnet adapter for the feed handler."""

from __future__ import annotations

import os
import time

from binance.client import Client as BinanceClient

PAIR_MAP = {
    "BTC/USD": "BTCUSDT",
    "ETH/USD": "ETHUSDT",
    "SOL/USD": "SOLUSDT",
    "XRP/USD": "XRPUSDT",
    "FET/USD": "FETUSDT",
}

_client: BinanceClient | None = None


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


def fetch_ticker(pair: str) -> tuple[float, float, int]:
    """Return (bid, ask, server_time_ms) for a given internal pair."""
    client = _get_client()
    binance_sym = _to_binance_symbol(pair)
    ticker = client.get_orderbook_ticker(symbol=binance_sym)
    bid = float(ticker["bidPrice"])
    ask = float(ticker["askPrice"])
    server_time_ms = int(time.time() * 1000)
    return bid, ask, server_time_ms
