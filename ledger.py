"""
Process D — Ledger & Risk Monitor
===================================
Subscribes to `order_results` (fills) and `market_data` (price feed) channels.

Responsibilities:
  - Track net BTC position and weighted average entry price
  - Calculate unrealised PNL using the latest market price
  - Append structured fill records to trades.log
  - Print a position summary to console after every successful fill
  - Print an EOD PNL report on SIGINT / SIGTERM (Ctrl+C)

Only SUCCESS fills mutate position state. FAILED orders are silently ignored.
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

import redis
from dotenv import load_dotenv

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────
REDIS_HOST     = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT     = int(os.getenv("REDIS_PORT", 6379))
SYMBOL         = os.getenv("TRADE_SYMBOL", "BTCUSDT")
BASE_ASSET     = SYMBOL[:3]          # e.g. "BTC"

ORDER_CHANNEL  = "order_results"
MARKET_CHANNEL = "market_data"
LOG_FILE       = "trades.log"
RETRY_DELAY    = 5                   # seconds between Redis reconnect attempts

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Ledger] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

# ── Position state (module-level so the SIGINT handler can read it) ────────────
current_qty:          float = 0.0
avg_entry_price:      float = 0.0
last_market_price:    float = 0.0
session_realized_pnl: float = 0.0


# ── PNL helpers ────────────────────────────────────────────────────────────────
def _unrealized_pnl() -> float:
    if current_qty <= 0.0 or avg_entry_price == 0.0:
        return 0.0
    return (last_market_price - avg_entry_price) * current_qty


def _fmt_pnl(pnl: float) -> str:
    sign = "+" if pnl >= 0 else ""
    return f"{sign}${pnl:.2f}"


# ── Position update ────────────────────────────────────────────────────────────
def _apply_fill(side: str, price: float, qty: float) -> None:
    """Mutate position state in-place; realise PNL on SELL fills."""
    global current_qty, avg_entry_price, session_realized_pnl

    if side == "BUY":
        new_qty = current_qty + qty
        # Weighted average: only update if we end up net long
        if new_qty > 0.0:
            avg_entry_price = (
                (current_qty * avg_entry_price) + (qty * price)
            ) / new_qty
        current_qty = new_qty

    elif side == "SELL":
        if avg_entry_price > 0.0:
            session_realized_pnl += (price - avg_entry_price) * qty
        current_qty = max(0.0, current_qty - qty)
        if current_qty == 0.0:
            avg_entry_price = 0.0   # position closed; reset entry


# ── Output helpers ─────────────────────────────────────────────────────────────
def _write_log(side: str, price: float, qty: float, timestamp: int) -> None:
    """Append one fill line to trades.log (matches the specified format)."""
    dt   = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    upnl = _unrealized_pnl()

    line = (
        f"{dt} | {side:<4} | "
        f"price={price:.2f} | "
        f"qty={qty:.6f} | "
        f"pos={current_qty:.6f} {BASE_ASSET} | "
        f"pnl={_fmt_pnl(upnl)}"
    )

    with open(LOG_FILE, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")

    # Echo to console via the logger so it gets the timestamp prefix
    log.info(line)


def _print_position_summary() -> None:
    upnl = _unrealized_pnl()
    log.info(
        "POSITION  qty=%.6f %s | avg_entry=%.2f | last_px=%.2f | "
        "unrealized=%s | realized=%s",
        current_qty,
        BASE_ASSET,
        avg_entry_price,
        last_market_price,
        _fmt_pnl(upnl),
        _fmt_pnl(session_realized_pnl),
    )


def _eod_report() -> None:
    upnl  = _unrealized_pnl()
    total = session_realized_pnl + upnl

    print("\n" + "=" * 56)
    print("  EOD PNL REPORT")
    print("=" * 56)
    print(f"  Symbol           : {SYMBOL}")
    print(f"  Final position   : {current_qty:.6f} {BASE_ASSET}")
    print(f"  Avg entry price  : {avg_entry_price:.2f}")
    print(f"  Last market px   : {last_market_price:.2f}")
    print(f"  Realized PNL     : {_fmt_pnl(session_realized_pnl)}")
    print(f"  Unrealized PNL   : {_fmt_pnl(upnl)}")
    print(f"  Total PNL        : {_fmt_pnl(total)}")
    print("=" * 56)


# ── Signal handler ─────────────────────────────────────────────────────────────
def _handle_shutdown(signum, frame):  # noqa: ANN001
    log.info("Shutdown signal received. Generating EOD report …")
    _eod_report()
    sys.exit(0)


signal.signal(signal.SIGINT,  _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


# ── Main loop ──────────────────────────────────────────────────────────────────
def run() -> None:
    global last_market_price

    log.info(
        "Ledger starting. Subscribing to '%s' and '%s'.",
        ORDER_CHANNEL,
        MARKET_CHANNEL,
    )

    while True:
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
            pubsub.subscribe(ORDER_CHANNEL, MARKET_CHANNEL)
            log.info("Listening …")

            for message in pubsub.listen():
                if message["type"] != "message":
                    continue

                channel = message["channel"]

                try:
                    data = json.loads(message["data"])
                except (json.JSONDecodeError, ValueError) as exc:
                    log.warning("Malformed JSON on '%s': %s", channel, exc)
                    continue

                # ── Keep last known market price (for unrealised PNL) ──────
                if channel == MARKET_CHANNEL:
                    try:
                        last_market_price = float(data["mid_price"])
                    except (KeyError, ValueError):
                        pass
                    continue

                # ── Process order fill ─────────────────────────────────────
                if channel == ORDER_CHANNEL:
                    if data.get("result") != "SUCCESS":
                        log.debug(
                            "Ignored order_id=%s (result=%s).",
                            data.get("order_id"),
                            data.get("result"),
                        )
                        continue

                    try:
                        side  = str(data["side"]).upper()
                        price = float(data["price"])
                        qty   = float(data["qty"])
                        # timestamp not guaranteed in the schema — fall back to now
                        ts    = int(data.get("timestamp", time.time()))
                    except (KeyError, ValueError, TypeError) as exc:
                        log.warning("Invalid fill payload: %s | %s", data, exc)
                        continue

                    _apply_fill(side, price, qty)
                    _write_log(side, price, qty, ts)
                    _print_position_summary()

        except redis.exceptions.ConnectionError as exc:
            log.error("Redis connection lost: %s. Retrying in %ds …", exc, RETRY_DELAY)
            time.sleep(RETRY_DELAY)
        except redis.exceptions.RedisError as exc:
            log.error("Redis error: %s. Retrying in %ds …", exc, RETRY_DELAY)
            time.sleep(RETRY_DELAY)


if __name__ == "__main__":
    run()