"""Process G — XRP ATR Breakout Strategy Engine.

XRP exhibits jump-diffusion price dynamics driven by binary legal/regulatory events
(SEC lawsuit rulings, court decisions, settlement announcements). Unlike BTC/ETH,
XRP does NOT mean-revert reliably — institutional snap-back liquidity is thin and
price can spike 40%+ in minutes on a single headline.

Strategy: ATR Volatility Breakout with XRP Sentiment Gate
──────────────────────────────────────────────────────────
  ENTRY  — price spikes > ATR_BREAKOUT_K × ATR(14) in a single tick
            AND XRP-specific sentiment is not in fear mode
            (sentiment acts as a gate, not a trigger — price leads)

  EXIT 1 — ATR stop-loss: price drops > ATR_STOPLOSS_K × ATR from entry
  EXIT 2 — Take-profit:   price rises > TAKE_PROFIT_MULT × entry
  EXIT 3 — Sentiment reversal: score < SENTIMENT_EXIT_GATE while profitable
            (cut winning position early if bad news detected)

ATR is computed using Wilder's smoothing (EMA with alpha = 1/ATR_PERIOD).
Seeded from the simple average of the first ATR_PERIOD true ranges.

Subscribes to:
    market_data           — mid_price ticks (filtered to TRADE_SYMBOL only)
    sentiment_signals_xrp — XRP-specific NLP compound scores

Publishes to:
    trade_signals         — BUY / SELL signals (shared with execution manager)
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import signal
import sys
import time
from collections import deque
from typing import Any

import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()


# ── Config ─────────────────────────────────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

MARKET_DATA_CHANNEL             = "market_data"
XRP_SENTIMENT_CHANNEL           = "sentiment_signals_xrp"
TRADE_SIGNALS_CHANNEL           = "trade_signals"
EXECUTION_CONFIRMATIONS_CHANNEL = "execution_confirmations"

TRADE_SYMBOL       = os.getenv("TRADE_SYMBOL", "XRP/USD")
DEFAULT_ORDER_TYPE = "MARKET"

# ATR parameters
ATR_PERIOD       = int(os.getenv("XRP_ATR_PERIOD",       "14"))
ATR_BREAKOUT_K   = float(os.getenv("XRP_ATR_BREAKOUT_K", "2.0"))  # entry: spike > 2× ATR
ATR_STOPLOSS_K   = float(os.getenv("XRP_ATR_STOPLOSS_K", "1.5"))  # exit: drop > 1.5× ATR from entry
# Minimum ATR in USD — prevents triggering on micro-noise when price is flat during warmup.
# At XRP ~$1.40, 0.003 ≈ 0.2% move — a meaningful minimum before calling it a "breakout".
ATR_MIN_USD      = float(os.getenv("XRP_ATR_MIN_USD",    "0.003"))

# Take-profit and sentiment gates
TAKE_PROFIT_MULT    = float(os.getenv("XRP_TAKE_PROFIT_MULT",    "1.08"))   # 8% take-profit
SENTIMENT_FEAR_GATE = float(os.getenv("XRP_SENTIMENT_FEAR_GATE", "-0.10"))  # suppress entry if score < gate
SENTIMENT_EXIT_GATE = float(os.getenv("XRP_SENTIMENT_EXIT_GATE", "-0.15"))  # exit on reversal if score < gate

# Portfolio sizing — XRP gets a smaller allocation (15%) due to higher event risk.
PORTFOLIO_USD    = float(os.getenv("PORTFOLIO_USD",    "1000000"))
XRP_ALLOCATION   = float(os.getenv("XRP_ALLOCATION",  "0.15"))
TARGET_ORDER_USD = PORTFOLIO_USD * XRP_ALLOCATION

TRADING_FEE_RATE = float(os.getenv("TRADING_FEE_RATE",           "0.001"))
POLL_TIMEOUT_SEC = float(os.getenv("STRATEGY_POLL_TIMEOUT_SEC",  "1.0"))
RETRY_DELAY_SEC  = int(os.getenv("STRATEGY_RETRY_DELAY_SEC",     "5"))
COOLDOWN_SECONDS = int(os.getenv("STOP_LOSS_COOLDOWN_SEC",        "3600"))
CONFIRMATION_TIMEOUT_SEC = float(os.getenv("CONFIRMATION_TIMEOUT_SEC", "60"))


# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [XRP-Strategy] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


# ── Strategy State ─────────────────────────────────────────────────────────────
# ATR computation state
true_ranges: deque[float] = deque(maxlen=ATR_PERIOD)
atr_value: float       = 0.0
atr_initialized: bool  = False
last_price: float | None = None   # previous tick price for true-range computation

# Sentiment state
xrp_sentiment_score: float = 0.0

# Position state
in_position: bool              = False
open_position_quantity: str    = "0.0000"
entry_price: float             = 0.0
atr_at_entry: float            = 0.0   # ATR captured at entry — used for stop-loss distance

# Execution handshake state
pending_action: str | None     = None
pending_action_since: float    = 0.0
pending_sell_quantity: str     = "0.0000"
cooldown_until: float          = 0.0

# Startup liquidation guards
boot_liquidation_done: bool    = False
boot_liquidation_pending: bool = False

STATE_KEY = f"strategy_state:{TRADE_SYMBOL}"


# ── Signal Handlers ────────────────────────────────────────────────────────────
def _handle_shutdown(signum: int, frame: Any) -> None:
    log.info("Shutdown signal %s received. Exiting XRP strategy engine.", signum)
    sys.exit(0)


signal.signal(signal.SIGINT,  _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


# ── ATR Computation ────────────────────────────────────────────────────────────
def _compute_atr_update(true_range: float) -> float | None:
    """
    Append one true range and return the current ATR, or None if not yet seeded.

    Uses Wilder's smoothing: ATR_t = (ATR_{t-1} × (n-1) + TR_t) / n
    Seeded from the simple average of the first ATR_PERIOD true ranges.
    """
    global atr_value, atr_initialized

    true_ranges.append(true_range)

    if not atr_initialized:
        if len(true_ranges) < ATR_PERIOD:
            return None
        # Seed with simple mean of first ATR_PERIOD observations.
        atr_value = sum(true_ranges) / ATR_PERIOD
        atr_initialized = True
        log.info(
            "ATR(%d) seeded at %.8f from %d initial true ranges.",
            ATR_PERIOD, atr_value, ATR_PERIOD,
        )
        return atr_value

    # Wilder's exponential smoothing with alpha = 1/ATR_PERIOD.
    atr_value = (atr_value * (ATR_PERIOD - 1) + true_range) / ATR_PERIOD
    return atr_value


def _target_quantity(price: float) -> str:
    """Floor-divide target notional by price to get whole-unit order quantity.

    Roostoo requires integer step sizes for XRP — no fractional units.
    """
    if price <= 0:
        return "0"
    qty = int(math.floor(TARGET_ORDER_USD / price))
    return str(qty)


# ── State Persistence ──────────────────────────────────────────────────────────
async def _save_state(redis_client: aioredis.Redis) -> None:
    try:
        await redis_client.hset(STATE_KEY, mapping={
            "in_position":            str(in_position),
            "open_position_quantity": open_position_quantity,
            "entry_price":            str(entry_price),
            "atr_at_entry":           str(atr_at_entry),
            "cooldown_until":         str(cooldown_until),
        })
    except aioredis.RedisError as exc:
        log.warning("Failed to persist state: %s", exc)


async def _load_state(redis_client: aioredis.Redis) -> bool:
    global in_position, open_position_quantity, entry_price, atr_at_entry, cooldown_until
    try:
        state = await redis_client.hgetall(STATE_KEY)
    except aioredis.RedisError as exc:
        log.warning("Failed to load persisted state: %s", exc)
        return False
    if not state or state.get("in_position") is None:
        return False
    in_position            = state.get("in_position", "False") == "True"
    open_position_quantity = state.get("open_position_quantity", "0")
    entry_price            = float(state.get("entry_price",    "0.0"))
    atr_at_entry           = float(state.get("atr_at_entry",  "0.0"))
    cooldown_until         = float(state.get("cooldown_until", "0.0"))
    log.info(
        "Resumed persisted state: in_position=%s qty=%s entry=%.6f atr_entry=%.6f",
        in_position, open_position_quantity, entry_price, atr_at_entry,
    )
    return True


# ── Signal Publishing ──────────────────────────────────────────────────────────
async def _publish_signal(
    redis_client: aioredis.Redis,
    *,
    action: str,
    symbol: str,
    quantity: str,
    reason: str,
    timestamp: int,
    expected_price: float,
) -> None:
    payload: dict[str, Any] = {
        "action":         action,
        "symbol":         symbol,
        "type":           DEFAULT_ORDER_TYPE,
        "quantity":       quantity,
        "reason":         reason,
        "timestamp":      timestamp,
        "expected_price": expected_price,
    }
    try:
        await redis_client.publish(TRADE_SIGNALS_CHANNEL, json.dumps(payload))
        log.info(
            "Signal published → action=%s symbol=%s qty=%s reason=%s px=%.6f",
            action, symbol, quantity, reason, expected_price,
        )
    except aioredis.RedisError as exc:
        log.error("Failed to publish trade signal: %s", exc)
        raise


# ── Market Tick Evaluation ─────────────────────────────────────────────────────
async def evaluate_market_tick(
    price: float,
    symbol: str,
    timestamp: int,
    redis_client: aioredis.Redis,
) -> None:
    global last_price, pending_action, pending_action_since, in_position, open_position_quantity
    global entry_price, atr_at_entry, cooldown_until, pending_sell_quantity
    global boot_liquidation_pending

    # Guard: only process ticks for this strategy's configured symbol.
    if symbol != TRADE_SYMBOL:
        return

    # ── ATR warm-up ─────────────────────────────────────────────────────────
    if last_price is None:
        last_price = price
        log.info("First XRP tick received: px=%.6f. ATR warm-up started.", price)
        return

    prev_price = last_price
    true_range = abs(price - prev_price)
    current_atr = _compute_atr_update(true_range)
    last_price = price   # advance rolling price AFTER computing TR

    if current_atr is None:
        log.info(
            "ATR warm-up: %d/%d true ranges collected. px=%.6f",
            len(true_ranges), ATR_PERIOD, price,
        )
        return

    # ── Tick diagnostics ────────────────────────────────────────────────────
    now_ts   = time.time()
    state    = "IN_POSITION" if in_position else "OUT_OF_POSITION"
    delta    = price - prev_price
    cooldown_remaining = max(cooldown_until - now_ts, 0.0)
    log.info(
        "px=%.6f Δ=%.6f ATR=%.6f (%.2f×ATR) sentiment=%.4f state=%s cooldown=%.0fs",
        price, delta, current_atr, (abs(delta) / current_atr if current_atr > 0 else 0),
        xrp_sentiment_score, state, cooldown_remaining,
    )

    # ── ATR floor: ignore signals until ATR reflects real volatility ────────
    if current_atr < ATR_MIN_USD:
        log.info(
            "ATR too small (%.6f < min=%.6f) — waiting for real volatility.",
            current_atr, ATR_MIN_USD,
        )
        return

    # ── Suppress while awaiting fill or startup liquidation ─────────────────
    if pending_action is not None:
        if time.time() - pending_action_since > CONFIRMATION_TIMEOUT_SEC:
            log.warning(
                "Pending %s confirmation timed out after %.0fs — resetting.",
                pending_action, CONFIRMATION_TIMEOUT_SEC,
            )
            pending_action = None
        else:
            log.info("Signal suppressed: awaiting %s fill confirmation.", pending_action)
            return

    if boot_liquidation_pending:
        log.info("Signal suppressed: awaiting startup liquidation confirmation.")
        return

    # ── Entry: Upside ATR Breakout + Sentiment Gate ──────────────────────────
    if (
        not in_position
        and now_ts > cooldown_until
        and delta > ATR_BREAKOUT_K * current_atr          # price spiked up > 2× ATR in one tick
        and xrp_sentiment_score > SENTIMENT_FEAR_GATE     # not in fear — gate open
    ):
        quantity             = _target_quantity(price)
        pending_action       = "BUY"
        pending_action_since = time.time()
        entry_price          = price
        atr_at_entry         = current_atr
        await _publish_signal(
            redis_client,
            action="BUY",
            symbol=symbol,
            quantity=quantity,
            reason=f"ATR_Breakout_{ATR_BREAKOUT_K}x_senti={xrp_sentiment_score:.3f}",
            timestamp=timestamp,
            expected_price=price,
        )
        return

    # No exits to evaluate if flat.
    if not in_position:
        return

    stop_loss_price  = entry_price - (ATR_STOPLOSS_K * atr_at_entry)
    take_profit_price = entry_price * TAKE_PROFIT_MULT

    # ── Exit 1: ATR Stop-Loss ────────────────────────────────────────────────
    if price <= stop_loss_price:
        sell_qty              = open_position_quantity
        pending_action        = "SELL"
        pending_action_since  = time.time()
        pending_sell_quantity = sell_qty
        cooldown_until        = now_ts + COOLDOWN_SECONDS
        await _publish_signal(
            redis_client,
            action="SELL",
            symbol=symbol,
            quantity=sell_qty,
            reason=f"ATR_StopLoss_{ATR_STOPLOSS_K}x",
            timestamp=timestamp,
            expected_price=price,
        )
        log.warning(
            "ATR stop-loss triggered: px=%.6f entry=%.6f stop=%.6f atr_entry=%.6f",
            price, entry_price, stop_loss_price, atr_at_entry,
        )
        return

    # ── Exit 2: Take-Profit ──────────────────────────────────────────────────
    if price >= take_profit_price:
        sell_qty              = open_position_quantity
        pending_action        = "SELL"
        pending_action_since  = time.time()
        pending_sell_quantity = sell_qty
        await _publish_signal(
            redis_client,
            action="SELL",
            symbol=symbol,
            quantity=sell_qty,
            reason=f"ATR_TakeProfit_{int((TAKE_PROFIT_MULT - 1) * 100)}pct",
            timestamp=timestamp,
            expected_price=price,
        )
        log.info(
            "Take-profit triggered: px=%.6f entry=%.6f target=%.6f gain=%.2f%%",
            price, entry_price, take_profit_price,
            (price / entry_price - 1.0) * 100,
        )
        return

    # ── Exit 3: Sentiment Reversal (bad news while profitable) ───────────────
    # Only exit on sentiment if we're still above entry — avoids doubling down
    # on a losing trade that is already heading for the ATR stop.
    if price > entry_price and xrp_sentiment_score < SENTIMENT_EXIT_GATE:
        sell_qty              = open_position_quantity
        pending_action        = "SELL"
        pending_action_since  = time.time()
        pending_sell_quantity = sell_qty
        await _publish_signal(
            redis_client,
            action="SELL",
            symbol=symbol,
            quantity=sell_qty,
            reason=f"Sentiment_Reversal_senti={xrp_sentiment_score:.3f}",
            timestamp=timestamp,
            expected_price=price,
        )
        log.warning(
            "Sentiment reversal exit: score=%.4f (gate=%.2f) px=%.6f entry=%.6f",
            xrp_sentiment_score, SENTIMENT_EXIT_GATE, price, entry_price,
        )
        return


# ── Execution Confirmation Handler ─────────────────────────────────────────────
async def handle_execution_confirmation(raw_payload: str, redis_client: aioredis.Redis) -> None:
    global in_position, open_position_quantity, pending_action, entry_price
    global atr_at_entry, pending_sell_quantity, boot_liquidation_pending

    try:
        data = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        log.warning("Malformed execution confirmation skipped: %r | error: %s", raw_payload, exc)
        return

    # Filter: ignore confirmations for other symbols.
    confirmed_symbol = str(data.get("symbol", ""))
    if confirmed_symbol and confirmed_symbol != TRADE_SYMBOL:
        return

    side   = str(data.get("action", data.get("side", ""))).upper()
    result = str(data.get("result", "")).upper()
    reason = str(data.get("reason", "")).upper()

    if side not in {"BUY", "SELL"}:
        return

    # ── Startup Liquidation Acknowledgment ───────────────────────────────────
    if boot_liquidation_pending and side == "SELL":
        if reason not in {
            "SYSTEM_REBOOT_LIQUIDATION",
            "STARTUP_BALANCE_SYNC",
            "STARTUP_BALANCE_SYNC_CLEAR",
        }:
            return

        # Clear regardless of result — a failed liquidation means nothing was held (already clean).
        boot_liquidation_pending = False
        pending_action           = None
        pending_sell_quantity    = "0.0000"
        in_position              = False
        open_position_quantity   = "0.0000"
        entry_price              = 0.0
        atr_at_entry             = 0.0
        await _save_state(redis_client)
        if result == "SUCCESS":
            log.info("Startup liquidation completed for %s.", TRADE_SYMBOL)
        else:
            log.info("Startup liquidation skipped for %s (nothing held): result=%s", TRADE_SYMBOL, result)
        return

    # ── Normal fill acknowledgment ───────────────────────────────────────────
    if pending_action is None:
        return  # stale or duplicate confirmation

    if side != pending_action:
        log.info(
            "Ignoring out-of-order fill: side=%s while pending=%s", side, pending_action,
        )
        return

    if result != "SUCCESS":
        log.warning("%s fill failed (result=%s). Reverting pending state.", side, result)
        if side == "BUY":
            in_position            = False
            open_position_quantity = "0.0000"
            entry_price            = 0.0
            atr_at_entry           = 0.0
        elif side == "SELL":
            in_position = True
            if open_position_quantity in {"", "0", "0.0", "0.0000"}:
                open_position_quantity = pending_sell_quantity
        pending_sell_quantity = "0.0000"
        pending_action        = None
        return

    # Extract executed quantity — confirmation payload carries effective_qty.
    try:
        executed_qty = float(data.get("effective_qty", 0.0) or 0.0)
    except (TypeError, ValueError):
        executed_qty = 0.0

    if side == "BUY":
        net_qty                = int(math.floor(executed_qty * (1.0 - TRADING_FEE_RATE)))
        in_position            = net_qty > 0
        open_position_quantity = str(net_qty)
        pending_sell_quantity  = "0"
        pending_action         = None
        await _save_state(redis_client)
        log.info(
            "BUY confirmed: executed=%.8f fee_rate=%.4f net_qty=%s entry=%.6f atr=%.6f",
            executed_qty, TRADING_FEE_RATE, open_position_quantity,
            entry_price, atr_at_entry,
        )

    elif side == "SELL":
        try:
            current_qty = float(open_position_quantity)
        except ValueError:
            current_qty = 0.0

        remaining              = max(current_qty - executed_qty, 0.0)
        in_position            = remaining > 0
        open_position_quantity = f"{remaining:.4f}"
        pending_sell_quantity  = "0.0000"
        if remaining <= 0.0:
            entry_price  = 0.0
            atr_at_entry = 0.0
        pending_action = None
        await _save_state(redis_client)
        log.info(
            "SELL confirmed: executed=%.8f remaining_qty=%s",
            executed_qty, open_position_quantity,
        )


# ── XRP Sentiment Update Handler ───────────────────────────────────────────────
def handle_xrp_sentiment_update(raw_payload: str) -> None:
    global xrp_sentiment_score

    try:
        data  = json.loads(raw_payload)
        score = float(data.get("compound_score", 0.0))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        log.warning(
            "Malformed XRP sentiment payload skipped: %r | error: %s", raw_payload, exc,
        )
        return

    prev_score         = xrp_sentiment_score
    xrp_sentiment_score = score

    log.info(
        "XRP sentiment updated: %.4f → %.4f | entry_gate=%.2f exit_gate=%.2f | headline=%r",
        prev_score, xrp_sentiment_score,
        SENTIMENT_FEAR_GATE, SENTIMENT_EXIT_GATE,
        str(data.get("latest_headline", ""))[:80],
    )


# ── Pub/Sub Consumer ───────────────────────────────────────────────────────────
async def consume_stream(redis_client: aioredis.Redis) -> None:
    pubsub = redis_client.pubsub(ignore_subscribe_messages=True)
    await pubsub.subscribe(
        MARKET_DATA_CHANNEL,
        XRP_SENTIMENT_CHANNEL,
        EXECUTION_CONFIRMATIONS_CHANNEL,
    )
    log.info(
        "XRP strategy subscribed | market=%s | sentiment=%s | confirmations=%s",
        MARKET_DATA_CHANNEL, XRP_SENTIMENT_CHANNEL, EXECUTION_CONFIRMATIONS_CHANNEL,
    )

    try:
        while True:
            try:
                message = await pubsub.get_message(timeout=POLL_TIMEOUT_SEC)
            except aioredis.RedisError as exc:
                log.error("pubsub.get_message error: %s", exc)
                raise   # bubble up to reconnect loop

            if message is None:
                continue
            if message.get("type") != "message":
                continue

            channel  = str(message.get("channel", ""))
            raw_data = message.get("data", "")
            payload  = (
                raw_data.decode("utf-8", errors="replace")
                if isinstance(raw_data, bytes)
                else str(raw_data)
            )

            if channel == XRP_SENTIMENT_CHANNEL:
                handle_xrp_sentiment_update(payload)
                continue

            if channel == EXECUTION_CONFIRMATIONS_CHANNEL:
                await handle_execution_confirmation(payload, redis_client)
                continue

            if channel == MARKET_DATA_CHANNEL:
                try:
                    tick      = json.loads(payload)
                    price     = float(tick["mid_price"])
                    timestamp = int(tick.get("timestamp", time.time()))
                    symbol    = str(tick.get("symbol", TRADE_SYMBOL))
                except (KeyError, ValueError, TypeError, json.JSONDecodeError) as exc:
                    log.warning(
                        "Malformed market_data payload skipped: %r | error: %s", payload, exc,
                    )
                    continue

                await evaluate_market_tick(price, symbol, timestamp, redis_client)

    finally:
        await pubsub.unsubscribe(
            MARKET_DATA_CHANNEL,
            XRP_SENTIMENT_CHANNEL,
            EXECUTION_CONFIRMATIONS_CHANNEL,
        )
        await pubsub.aclose()


# ── Startup Liquidation ────────────────────────────────────────────────────────
async def initialize_clean_slate(redis_client: aioredis.Redis) -> None:
    global in_position, open_position_quantity, pending_action, entry_price
    global atr_at_entry, cooldown_until, pending_sell_quantity, boot_liquidation_pending

    payload = {
        "action":   "SELL",
        "symbol":   TRADE_SYMBOL,
        "type":     DEFAULT_ORDER_TYPE,
        "quantity": "ALL",
        "reason":   "SYSTEM_REBOOT_LIQUIDATION",
    }
    try:
        await redis_client.publish(TRADE_SIGNALS_CHANNEL, json.dumps(payload))
    except aioredis.RedisError as exc:
        log.error("Failed to publish startup liquidation signal: %s", exc)
        raise

    boot_liquidation_pending = True
    in_position              = False
    open_position_quantity   = "0.0000"
    pending_action           = "SELL"
    entry_price              = 0.0
    atr_at_entry             = 0.0
    cooldown_until           = 0.0
    pending_sell_quantity    = "0.0000"
    log.info("Startup clean-slate liquidation signal published for %s.", TRADE_SYMBOL)


# ── Entry Point ────────────────────────────────────────────────────────────────
async def run() -> None:
    global boot_liquidation_done

    log.info(
        "XRP ATR Breakout Strategy starting | symbol=%s | atr_period=%d | "
        "breakout_k=%.2f | stoploss_k=%.2f | tp=%.0f%% | "
        "sentiment_entry_gate=%.2f | sentiment_exit_gate=%.2f | "
        "target_notional=$%.2f (%.0f%% of $%.0f)",
        TRADE_SYMBOL,
        ATR_PERIOD,
        ATR_BREAKOUT_K,
        ATR_STOPLOSS_K,
        (TAKE_PROFIT_MULT - 1) * 100,
        SENTIMENT_FEAR_GATE,
        SENTIMENT_EXIT_GATE,
        TARGET_ORDER_USD,
        XRP_ALLOCATION * 100,
        PORTFOLIO_USD,
    )

    while True:
        redis_client: aioredis.Redis | None = None
        try:
            redis_client = aioredis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True,
                socket_connect_timeout=5,
            )
            await redis_client.ping()
            log.info("Connected to Redis at %s:%s", REDIS_HOST, REDIS_PORT)

            if not boot_liquidation_done:
                await asyncio.sleep(8)
                if await _load_state(redis_client):
                    log.info("Resumed from persisted state — skipping startup liquidation.")
                    boot_liquidation_done = True
                else:
                    await initialize_clean_slate(redis_client)
                    boot_liquidation_done = True

            await consume_stream(redis_client)

        except aioredis.RedisError as exc:
            log.error(
                "Redis connection error: %s. Retrying in %ds.", exc, RETRY_DELAY_SEC,
            )
            await asyncio.sleep(RETRY_DELAY_SEC)
        except Exception as exc:
            log.error(
                "Unexpected error in XRP strategy run loop: %s. Retrying in %ds.",
                exc, RETRY_DELAY_SEC,
            )
            await asyncio.sleep(RETRY_DELAY_SEC)
        finally:
            if redis_client is not None:
                try:
                    await redis_client.aclose()
                except Exception:
                    pass


if __name__ == "__main__":
    asyncio.run(run())
