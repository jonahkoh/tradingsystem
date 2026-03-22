"""Process H — SOL EMA Momentum Strategy Engine.

SOL is a high-beta, retail-driven asset. Unlike BTC/ETH it does NOT mean-revert
reliably — capital floods in during ecosystem hype (DeFi, NFTs, memecoins) and
the price trends for days. Trying to "buy the dip" on SOL during a network outage
will get you liquidated.

Strategy: EMA Crossover Momentum with ATR Stop-Loss
─────────────────────────────────────────────────────
  ENTRY  — fast EMA crosses above slow EMA (golden cross)
            AND global sentiment is not in SEVERE_FEAR
            (only suppress entries in extreme panic — SOL moves in all other regimes)

  EXIT 1 — ATR stop-loss: price drops > ATR_STOPLOSS_K × ATR from entry
            (wide stop to avoid being shaken out by normal momentum pullbacks)
  EXIT 2 — Death cross: fast EMA crosses below slow EMA (trend reversal confirmed)

EMA update uses standard formula: EMA_t = α × price_t + (1 - α) × EMA_{t-1}
  where α = 2 / (period + 1)

ATR uses Wilder's smoothing (same as XRP and BTC/ETH strategies).

Subscribes to:
    market_data       — mid_price ticks (filtered to TRADE_SYMBOL only)
    sentiment_signals — global NLP compound score

Publishes to:
    trade_signals     — BUY / SELL signals (shared execution manager)
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
SENTIMENT_CHANNEL               = "sentiment_signals"
TRADE_SIGNALS_CHANNEL           = "trade_signals"
EXECUTION_CONFIRMATIONS_CHANNEL = "execution_confirmations"

TRADE_SYMBOL       = os.getenv("TRADE_SYMBOL", "SOL/USD")
DEFAULT_ORDER_TYPE = "MARKET"

# EMA periods.
# At 5-second polling: fast=20 ≈ 100s, slow=50 ≈ 250s.
# The golden/death cross provides a trend signal with ~2.5× separation.
EMA_FAST_PERIOD = int(os.getenv("SOL_EMA_FAST_PERIOD", "20"))
EMA_SLOW_PERIOD = int(os.getenv("SOL_EMA_SLOW_PERIOD", "50"))

# ATR stop-loss — wider than mean-reversion (2.0×) to avoid shaking out during momentum pullbacks.
ATR_PERIOD      = int(os.getenv("SOL_ATR_PERIOD",      "14"))
ATR_STOPLOSS_K  = float(os.getenv("SOL_ATR_STOPLOSS_K", "2.0"))
FALLBACK_STOP   = float(os.getenv("SOL_FALLBACK_STOP_MULTIPLIER", "0.95"))  # 5% fixed fallback

# Sentiment gate — only suppress entry in SEVERE_FEAR (-0.30 threshold).
# SOL momentum works in UNEASE/NEUTRAL/HYPE; only extreme panic is dangerous.
SENTIMENT_ENTRY_GATE = float(os.getenv("SOL_SENTIMENT_ENTRY_GATE", "-0.30"))

# Portfolio sizing — SOL gets 20% of portfolio.
PORTFOLIO_USD    = float(os.getenv("PORTFOLIO_USD",   "1000000"))
SOL_ALLOCATION   = float(os.getenv("SOL_ALLOCATION",  "0.20"))
TARGET_ORDER_USD = PORTFOLIO_USD * SOL_ALLOCATION

TRADING_FEE_RATE = float(os.getenv("TRADING_FEE_RATE",          "0.001"))
POLL_TIMEOUT_SEC = float(os.getenv("STRATEGY_POLL_TIMEOUT_SEC", "1.0"))
RETRY_DELAY_SEC  = int(os.getenv("STRATEGY_RETRY_DELAY_SEC",    "5"))
COOLDOWN_SECONDS = int(os.getenv("STOP_LOSS_COOLDOWN_SEC",       "3600"))


# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SOL-Strategy] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


# ── Strategy State ─────────────────────────────────────────────────────────────
# EMA state
fast_ema: float | None = None   # EMA(EMA_FAST_PERIOD)
slow_ema: float | None = None   # EMA(EMA_SLOW_PERIOD)
prev_fast_ema: float | None = None   # previous tick fast EMA (for crossover detection)
prev_slow_ema: float | None = None   # previous tick slow EMA
tick_count: int = 0   # ticks received; crossover logic waits until >= EMA_SLOW_PERIOD

# ATR state for stop-loss
atr_true_ranges: deque[float] = deque(maxlen=ATR_PERIOD)
atr_value: float       = 0.0
atr_initialized: bool  = False
last_atr_price: float | None = None

# Sentiment
global_sentiment_score: float = 0.0

# Position state
in_position: bool           = False
open_position_quantity: str = "0.0000"
entry_price: float          = 0.0
atr_at_entry: float         = 0.0   # ATR snapshot at entry — stop-loss anchor

# Execution handshake
pending_action: str | None  = None
pending_sell_quantity: str  = "0.0000"
cooldown_until: float       = 0.0

# Startup guards
boot_liquidation_done: bool    = False
boot_liquidation_pending: bool = False


# ── Shutdown Handler ───────────────────────────────────────────────────────────
def _handle_shutdown(signum: int, frame: Any) -> None:
    log.info("Shutdown signal %s received. Exiting SOL strategy engine.", signum)
    sys.exit(0)


signal.signal(signal.SIGINT,  _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


# ── EMA Computation ────────────────────────────────────────────────────────────
def _update_ema(prev: float | None, price: float, period: int) -> float:
    """
    Standard EMA update: EMA_t = α × price_t + (1-α) × EMA_{t-1}
    where α = 2 / (period + 1).
    Seeds from the first price when prev is None.
    """
    if prev is None:
        return price
    alpha = 2.0 / (period + 1)
    return alpha * price + (1.0 - alpha) * prev


# ── ATR Computation (Wilder's Smoothing) ───────────────────────────────────────
def _compute_atr(current_price: float) -> float | None:
    """Update ATR state and return current value, or None while still seeding."""
    global atr_value, atr_initialized, last_atr_price

    if last_atr_price is None:
        last_atr_price = current_price
        return None

    tr = abs(current_price - last_atr_price)
    last_atr_price = current_price
    atr_true_ranges.append(tr)

    if not atr_initialized:
        if len(atr_true_ranges) < ATR_PERIOD:
            return None
        atr_value = sum(atr_true_ranges) / ATR_PERIOD
        atr_initialized = True
        log.info("ATR(%d) seeded at %.8f.", ATR_PERIOD, atr_value)
        return atr_value

    atr_value = (atr_value * (ATR_PERIOD - 1) + tr) / ATR_PERIOD
    return atr_value


def _target_quantity(price: float) -> str:
    """Floor-divide target notional by price to whole units (Roostoo integer step size)."""
    if price <= 0:
        return "0"
    qty = int(math.floor(TARGET_ORDER_USD / price))
    return str(qty)


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
    global fast_ema, slow_ema, prev_fast_ema, prev_slow_ema, tick_count
    global pending_action, in_position, open_position_quantity
    global entry_price, atr_at_entry, cooldown_until, pending_sell_quantity
    global boot_liquidation_pending

    # Guard: only process ticks for this strategy's symbol.
    if symbol != TRADE_SYMBOL:
        return

    # ── ATR update ───────────────────────────────────────────────────────────
    current_atr = _compute_atr(price)

    # ── EMA update ───────────────────────────────────────────────────────────
    # Capture previous values BEFORE updating (needed for crossover detection).
    prev_fast_ema = fast_ema
    prev_slow_ema = slow_ema

    fast_ema   = _update_ema(fast_ema, price, EMA_FAST_PERIOD)
    slow_ema   = _update_ema(slow_ema, price, EMA_SLOW_PERIOD)
    tick_count += 1

    # ── Warm-up ──────────────────────────────────────────────────────────────
    if tick_count < EMA_SLOW_PERIOD:
        log.info(
            "EMA warm-up: %d/%d ticks | fast_ema=%.6f slow_ema=%.6f",
            tick_count, EMA_SLOW_PERIOD, fast_ema, slow_ema,
        )
        return

    # ── Crossover detection ──────────────────────────────────────────────────
    fast_above_slow      = fast_ema > slow_ema
    prev_fast_above_slow = (
        prev_fast_ema > prev_slow_ema
        if (prev_fast_ema is not None and prev_slow_ema is not None)
        else fast_above_slow   # default to current state if no previous — no crossover
    )
    golden_cross = fast_above_slow and not prev_fast_above_slow
    death_cross  = not fast_above_slow and prev_fast_above_slow

    # ── Tick log ─────────────────────────────────────────────────────────────
    now_ts = time.time()
    state  = "IN_POSITION" if in_position else "OUT_OF_POSITION"
    spread = fast_ema - slow_ema
    log.info(
        "px=%.6f | fast=%.6f slow=%.6f spread=%.6f | ATR=%.6f | "
        "senti=%.4f | %s%s | state=%s | cooldown=%.0fs",
        price, fast_ema, slow_ema, spread,
        current_atr if current_atr else 0.0,
        global_sentiment_score,
        "GOLDEN_CROSS " if golden_cross else "",
        "DEATH_CROSS "  if death_cross  else "",
        state,
        max(cooldown_until - now_ts, 0.0),
    )

    # ── Suppress while pending ────────────────────────────────────────────────
    if pending_action is not None:
        log.info("Signal suppressed: awaiting %s fill confirmation.", pending_action)
        return

    if boot_liquidation_pending:
        log.info("Signal suppressed: awaiting startup liquidation.")
        return

    # ── Entry: Golden Cross + Sentiment Gate ─────────────────────────────────
    if (
        not in_position
        and now_ts > cooldown_until
        and golden_cross
        and global_sentiment_score > SENTIMENT_ENTRY_GATE
    ):
        quantity       = _target_quantity(price)
        pending_action = "BUY"
        entry_price    = price

        # ATR stop-loss with fixed fallback.
        if current_atr and current_atr > 0:
            atr_at_entry = current_atr
            stop_price   = entry_price - (ATR_STOPLOSS_K * atr_at_entry)
            log.info(
                "Golden cross BUY: entry=%.6f ATR=%.6f stop=%.6f senti=%.4f",
                entry_price, atr_at_entry, stop_price, global_sentiment_score,
            )
        else:
            atr_at_entry = 0.0
            log.info(
                "Golden cross BUY: entry=%.6f (ATR not seeded, fallback stop %.0f%%)",
                entry_price, (1.0 - FALLBACK_STOP) * 100,
            )

        await _publish_signal(
            redis_client,
            action="BUY",
            symbol=symbol,
            quantity=quantity,
            reason=f"Golden_Cross_fast{EMA_FAST_PERIOD}_slow{EMA_SLOW_PERIOD}_senti={global_sentiment_score:.3f}",
            timestamp=timestamp,
            expected_price=price,
        )
        return

    # No exits to evaluate if flat.
    if not in_position:
        return

    # Compute stop price from ATR captured at entry (or fallback).
    if atr_at_entry > 0:
        stop_loss_price = entry_price - (ATR_STOPLOSS_K * atr_at_entry)
    else:
        stop_loss_price = entry_price * FALLBACK_STOP

    # ── Exit 1: ATR Stop-Loss ─────────────────────────────────────────────────
    if price <= stop_loss_price:
        sell_qty              = open_position_quantity
        pending_action        = "SELL"
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

    # ── Exit 2: Death Cross (trend reversal) ──────────────────────────────────
    if death_cross:
        sell_qty              = open_position_quantity
        pending_action        = "SELL"
        pending_sell_quantity = sell_qty
        await _publish_signal(
            redis_client,
            action="SELL",
            symbol=symbol,
            quantity=sell_qty,
            reason=f"Death_Cross_fast{EMA_FAST_PERIOD}_slow{EMA_SLOW_PERIOD}",
            timestamp=timestamp,
            expected_price=price,
        )
        log.info(
            "Death cross exit: px=%.6f entry=%.6f pnl=%.2f%%",
            price, entry_price, (price / entry_price - 1.0) * 100,
        )
        return


# ── Execution Confirmation Handler ─────────────────────────────────────────────
def handle_execution_confirmation(raw_payload: str) -> None:
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
        if result == "SUCCESS":
            log.info("Startup liquidation completed for %s.", TRADE_SYMBOL)
        else:
            log.info("Startup liquidation skipped for %s (nothing held): result=%s", TRADE_SYMBOL, result)
        return

    if pending_action is None:
        return

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
        log.info(
            "BUY confirmed: executed=%.8f fee_rate=%.4f net_qty=%s entry=%.6f",
            executed_qty, TRADING_FEE_RATE, open_position_quantity, entry_price,
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
        log.info(
            "SELL confirmed: executed=%.8f remaining_qty=%s",
            executed_qty, open_position_quantity,
        )


# ── Sentiment Update Handler ───────────────────────────────────────────────────
def handle_sentiment_update(raw_payload: str) -> None:
    global global_sentiment_score

    try:
        data  = json.loads(raw_payload)
        score = float(data.get("compound_score", 0.0))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        log.warning("Malformed sentiment payload skipped: %r | error: %s", raw_payload, exc)
        return

    prev               = global_sentiment_score
    global_sentiment_score = score
    log.info(
        "Global sentiment updated: %.4f → %.4f | entry_gate=%.2f",
        prev, global_sentiment_score, SENTIMENT_ENTRY_GATE,
    )


# ── Pub/Sub Consumer ───────────────────────────────────────────────────────────
async def consume_stream(redis_client: aioredis.Redis) -> None:
    pubsub = redis_client.pubsub(ignore_subscribe_messages=True)
    await pubsub.subscribe(
        MARKET_DATA_CHANNEL,
        SENTIMENT_CHANNEL,
        EXECUTION_CONFIRMATIONS_CHANNEL,
    )
    log.info(
        "SOL strategy subscribed | market=%s | sentiment=%s | confirmations=%s",
        MARKET_DATA_CHANNEL, SENTIMENT_CHANNEL, EXECUTION_CONFIRMATIONS_CHANNEL,
    )

    try:
        while True:
            try:
                message = await pubsub.get_message(timeout=POLL_TIMEOUT_SEC)
            except aioredis.RedisError as exc:
                log.error("pubsub.get_message error: %s", exc)
                raise

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

            if channel == SENTIMENT_CHANNEL:
                handle_sentiment_update(payload)
                continue

            if channel == EXECUTION_CONFIRMATIONS_CHANNEL:
                handle_execution_confirmation(payload)
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
            SENTIMENT_CHANNEL,
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
        "SOL Momentum Strategy starting | symbol=%s | "
        "ema_fast=%d (≈%ds) | ema_slow=%d (≈%ds) | "
        "atr_period=%d | atr_stop_k=%.2f | "
        "sentiment_gate=%.2f | target_notional=$%.2f (%.0f%% of $%.0f)",
        TRADE_SYMBOL,
        EMA_FAST_PERIOD, EMA_FAST_PERIOD * 5,
        EMA_SLOW_PERIOD, EMA_SLOW_PERIOD * 5,
        ATR_PERIOD, ATR_STOPLOSS_K,
        SENTIMENT_ENTRY_GATE,
        TARGET_ORDER_USD, SOL_ALLOCATION * 100, PORTFOLIO_USD,
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
                # Wait for execution manager to subscribe to trade_signals.
                # Both containers start simultaneously — without this delay the
                # liquidation signal is published before anyone is listening.
                await asyncio.sleep(8)
                await initialize_clean_slate(redis_client)
                boot_liquidation_done = True

            await consume_stream(redis_client)

        except aioredis.RedisError as exc:
            log.error("Redis connection error: %s. Retrying in %ds.", exc, RETRY_DELAY_SEC)
            await asyncio.sleep(RETRY_DELAY_SEC)
        except Exception as exc:
            log.error(
                "Unexpected error in SOL strategy run loop: %s. Retrying in %ds.",
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
