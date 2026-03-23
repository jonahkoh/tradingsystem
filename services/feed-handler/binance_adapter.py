"""
Binance Testnet Adapter — Feed Handler
=======================================
Fetches top-of-book (bid/ask) from Binance Spot Testnet and returns data
in the same format the rest of the system expects.

Internal pair format (BTC/USD) is converted to Binance format (BTCUSDT)
at the boundary. Callers never see Binance-specific symbols.
"""

from __future__ import annotations

import logging
import os
import time

from binance.client import Client as BinanceClient
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "").strip()

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
            BINANCE_API_KEY,
            BINANCE_API_SECRET,
            testnet=True,
        )
        log.info("Binance Testnet client initialized.")
    return _client


def _to_binance_symbol(pair: str) -> str:
    sym = PAIR_MAP.get(pair.upper())
    if sym is None:
        raise ValueError(f"No Binance mapping for pair {pair!r}")
    return sym


def fetch_ticker(pair: str) -> tuple[float, float, int]:
    """
    Fetch top-of-book for *pair* from Binance Testnet.

    Returns (bid_price, ask_price, server_time_ms) — same contract as
    the Roostoo ``_fetch_top_of_book`` function.
    """
    client = _get_client()
    binance_sym = _to_binance_symbol(pair)

    ticker = client.get_orderbook_ticker(symbol=binance_sym)

    bid = float(ticker["bidPrice"])
    ask = float(ticker["askPrice"])
    server_time_ms = int(time.time() * 1000)

    return bid, ask, server_time_ms
