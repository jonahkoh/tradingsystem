"""Process J — FET ATR Breakout Strategy Engine.

FET (Fetch.ai) is a high-beta AI token whose price is driven by macro AI narrative,
partnership announcements, and crypto risk-on/risk-off cycles. It exhibits sharp
breakout spikes followed by trend continuation — very similar dynamics to XRP's
event-driven jumps, making ATR breakout the natural strategy choice.

Strategy: ATR Volatility Breakout with Sentiment Gate
───────────────────────────────────────────────────────
  ENTRY  — price spikes > ATR_BREAKOUT_K × ATR(14) in a single tick
            AND global sentiment is not in fear mode

  EXIT 1 — ATR stop-loss: price drops > ATR_STOPLOSS_K × ATR from entry
  EXIT 2 — Take-profit:   price rises > TAKE_PROFIT_MULT × entry
  EXIT 3 — Sentiment reversal: bad news while profitable

ATR uses Wilder's smoothing. Quantities rounded to 1 decimal place (Roostoo amt_prec=1).

Subscribes to:
    market_data       — mid_price ticks (filtered to FET/USD)
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

TRADE_SYMBOL       = "FET/USD"
DEFAULT_ORDER_TYPE = "MARKET"

# ATR parameters
ATR_PERIOD      = int(os.getenv("FET_ATR_PERIOD",      "14"))
ATR_BREAKOUT_K  = float(os.getenv("FET_ATR_BREAKOUT_K", "2.0"))
BREAKOUT_CONFIRM_TICKS = int(os.getenv("FET_BREAKOUT_CONFIRM_TICKS", "2"))
ATR_STOPLOSS_K  = float(os.getenv("FET_ATR_STOPLOSS_K", "2.0"))
FALLBACK_STOP_MULT = float(os.getenv("FET_FALLBACK_STOP_MULTIPLIER", "0.985"))
# Minimum ATR floor — suppresses entries during flat warmup.
# FET ~$0.5-2: 0.003 ≈ 0.3% move minimum before calling it a breakout.
ATR_MIN_USD     = float(os.getenv("FET_ATR_MIN_USD",    "0.003"))

TAKE_PROFIT_MULT    = float(os.getenv("FET_TAKE_PROFIT_MULT",    "1.015"))  # 1.5% take-profit
SENTIMENT_FEAR_GATE = float(os.getenv("FET_SENTIMENT_FEAR_GATE", "-0.20"))
SENTIMENT_EXIT_GATE = float(os.getenv("FET_SENTIMENT_EXIT_GATE", "-0.25"))

PORTFOLIO_USD    = float(os.getenv("PORTFOLIO_USD",   "1000000"))
FET_ALLOCATION   = float(os.getenv("FET_ALLOCATION",  "0.10"))
TARGET_ORDER_USD = PORTFOLIO_USD * FET_ALLOCATION

TRADING_FEE_RATE = float(os.getenv("TRADING_FEE_RATE",           "0.001"))
POLL_TIMEOUT_SEC = float(os.getenv("STRATEGY_POLL_TIMEOUT_SEC",  "1.0"))
RETRY_DELAY_SEC  = int(os.getenv("STRATEGY_RETRY_DELAY_SEC",     "5"))
COOLDOWN_SECONDS = int(os.getenv("FET_STOP_LOSS_COOLDOWN_SEC",   "3600"))
CONFIRMATION_TIMEOUT_SEC = float(os.getenv("CONFIRMATION_TIMEOUT_SEC", "60"))


# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [FET-Strategy] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


# ── Strategy State ─────────────────────────────────────────────────────────────
true_ranges: deque[float] = deque(maxlen=ATR_PERIOD)
atr_value: float          = 0.0
atr_initialized: bool     = False
last_price: float | None  = None

sentiment_score: float     = 0.0

in_position: bool              = False
open_position_quantity: str    = "0.0"
entry_price: float             = 0.0
atr_at_entry: float            = 0.0

pending_action: str | None     = None
pending_action_since: float    = 0.0
pending_sell_quantity: str     = "0.0"
cooldown_until: float          = 0.0
breakout_streak: int           = 0

boot_liquidation_done: bool    = False
boot_liquidation_pending: bool = False

STATE_KEY = f"strategy_state:{TRADE_SYMBOL}"


# ── Shutdown Handler ────────────────────────────────────────────────────────────
def _handle_shutdown(signum: int, frame: Any) -> None:
    log.info("Shutdown signal %s received. Exiting FET strategy engine.", signum)
    sys.exit(0)


signal.signal(signal.SIGINT,  _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


# ── ATR Computation (Wilder's Smoothing) ───────────────────────────────────────
def _compute_atr_update(true_range: float) -> float | None:
    global atr_value, atr_initialized

    true_ranges.append(true_range)

    if not atr_initialized:
        if len(true_ranges) < ATR_PERIOD:
            return None
        atr_value = sum(true_ranges) / ATR_PERIOD
        atr_initialized = True
        log.info("ATR(%d) seeded at %.8f.", ATR_PERIOD, atr_value)
        return atr_value

    atr_value = (atr_value * (ATR_PERIOD - 1) + true_range) / ATR_PERIOD
    return atr_value


def _target_quantity(price: float) -> str:
    """Round to 1 decimal place — Roostoo FET/USD has amt_prec=1."""
    if price <= 0:
        return "0.0"
    qty = math.floor((TARGET_ORDER_USD / price) * 10) / 10
    return f"{qty:.1f}"


def _safe_nonneg_float(value: Any) -> float:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(num):
        return 0.0
    return max(num, 0.0)


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
    open_position_quantity = state.get("open_position_quantity", "0.0")
    entry_price            = _safe_nonneg_float(state.get("entry_price", "0.0"))
    atr_at_entry           = _safe_nonneg_float(state.get("atr_at_entry", "0.0"))
    cooldown_until         = _safe_nonneg_float(state.get("cooldown_until", "0.0"))

    resumed_qty = _safe_nonneg_float(open_position_quantity)

    # Defensive reset for inconsistent resumed state.
    if in_position and (resumed_qty <= 0.0 or entry_price <= 0.0):
        log.warning(
            "Inconsistent resumed state detected; resetting flat state "
            "(in_position=%s qty=%r entry=%.6f).",
            in_position,
            open_position_quantity,
            entry_price,
        )
        in_position = False
        open_position_quantity = "0.0"
        entry_price = 0.0
        atr_at_entry = 0.0

    if not in_position:
        open_position_quantity = "0.0"
        entry_price = 0.0
        atr_at_entry = 0.0

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
    global boot_liquidation_pending, breakout_streak

    if symbol != TRADE_SYMBOL or price <= 0:
        return

    if last_price is None:
        last_price = price
        log.info("First FET tick: px=%.6f. ATR warm-up started.", price)
        return

    prev_price  = last_price
    true_range  = abs(price - prev_price)
    current_atr = _compute_atr_update(true_range)
    last_price  = price

    if current_atr is None:
        log.info(
            "ATR warm-up: %d/%d true ranges. px=%.6f",
            len(true_ranges), ATR_PERIOD, price,
        )
        breakout_streak = 0
        return

    now_ts   = time.time()
    delta    = price - prev_price
    state    = "IN_POSITION" if in_position else "OUT_OF_POSITION"
    log.info(
        "px=%.6f Δ=%.6f ATR=%.6f (%.2f×ATR) senti=%.4f state=%s cooldown=%.0fs",
        price, delta, current_atr,
        (abs(delta) / current_atr if current_atr > 0 else 0),
        sentiment_score, state, max(cooldown_until - now_ts, 0.0),
    )

    # ATR floor: ignore signals until ATR reflects real volatility.
    if current_atr < ATR_MIN_USD:
        log.info(
            "ATR too small (%.6f < min=%.6f) — waiting for real volatility.",
            current_atr, ATR_MIN_USD,
        )
        breakout_streak = 0
        return

    if pending_action is not None:
        if time.time() - pending_action_since > CONFIRMATION_TIMEOUT_SEC:
            log.warning(
                "Pending %s confirmation timed out after %.0fs — resetting.",
                pending_action, CONFIRMATION_TIMEOUT_SEC,
            )
            pending_action = None
        else:
            log.info("Signal suppressed: awaiting %s fill confirmation.", pending_action)
            breakout_streak = 0
            return

    if boot_liquidation_pending:
        log.info("Signal suppressed: awaiting startup liquidation confirmation.")
        breakout_streak = 0
        return

    # ── Entry: Upside ATR Breakout + Sentiment Gate ──────────────────────────
    breakout_hit = delta > ATR_BREAKOUT_K * current_atr
    if not in_position and now_ts > cooldown_until and breakout_hit:
        breakout_streak += 1
    else:
        breakout_streak = 0

    if (
        not in_position
        and now_ts > cooldown_until
        and breakout_streak >= BREAKOUT_CONFIRM_TICKS
        and sentiment_score > SENTIMENT_FEAR_GATE
    ):
        quantity             = _target_quantity(price)
        if _safe_nonneg_float(quantity) <= 0.0:
            log.info("Entry skipped: computed FET quantity is %s at px=%.6f", quantity, price)
            return
        pending_action       = "BUY"
        pending_action_since = time.time()
        entry_price          = price
        atr_at_entry         = current_atr
        breakout_streak      = 0
        await _publish_signal(
            redis_client,
            action="BUY",
            symbol=symbol,
            quantity=quantity,
            reason=f"ATR_Breakout_{ATR_BREAKOUT_K}x_confirm={BREAKOUT_CONFIRM_TICKS}_senti={sentiment_score:.3f}",
            timestamp=timestamp,
            expected_price=price,
        )
        return

    if not in_position:
        return

    if atr_at_entry > 0:
        stop_loss_price = entry_price - (ATR_STOPLOSS_K * atr_at_entry)
    else:
        # Persisted startup state can resume without ATR-at-entry.
        stop_loss_price = entry_price * FALLBACK_STOP_MULT
    take_profit_price = entry_price * TAKE_PROFIT_MULT

    # ── Exit 1: ATR Stop-Loss ─────────────────────────────────────────────────
    if price <= stop_loss_price:
        sell_qty              = open_position_quantity
        if _safe_nonneg_float(sell_qty) <= 0.0:
            log.warning("Stop-loss skipped: in_position=True but sell quantity is %r", sell_qty)
            in_position = False
            return
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

    # ── Exit 2: Take-Profit ───────────────────────────────────────────────────
    if price >= take_profit_price:
        sell_qty              = open_position_quantity
        if _safe_nonneg_float(sell_qty) <= 0.0:
            log.warning("Take-profit skipped: in_position=True but sell quantity is %r", sell_qty)
            in_position = False
            return
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
    if price > entry_price and sentiment_score < SENTIMENT_EXIT_GATE:
        sell_qty              = open_position_quantity
        if _safe_nonneg_float(sell_qty) <= 0.0:
            log.warning("Sentiment exit skipped: in_position=True but sell quantity is %r", sell_qty)
            in_position = False
            return
        pending_action        = "SELL"
        pending_action_since  = time.time()
        pending_sell_quantity = sell_qty
        await _publish_signal(
            redis_client,
            action="SELL",
            symbol=symbol,
            quantity=sell_qty,
            reason=f"Sentiment_Reversal_senti={sentiment_score:.3f}",
            timestamp=timestamp,
            expected_price=price,
        )
        log.warning(
            "Sentiment reversal exit: score=%.4f (gate=%.2f) px=%.6f entry=%.6f",
            sentiment_score, SENTIMENT_EXIT_GATE, price, entry_price,
        )
        return


# ── Execution Confirmation Handler ─────────────────────────────────────────────
async def handle_execution_confirmation(raw_payload: str, redis_client: aioredis.Redis) -> None:
    global in_position, open_position_quantity, pending_action, entry_price
    global atr_at_entry, pending_sell_quantity, boot_liquidation_pending

    try:
        data = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        log.warning("Malformed confirmation skipped: %r | %s", raw_payload, exc)
        return

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
        boot_liquidation_pending = False
        pending_action           = None
        pending_sell_quantity    = "0.0"
        in_position              = False
        open_position_quantity   = "0.0"
        entry_price              = 0.0
        atr_at_entry             = 0.0
        await _save_state(redis_client)
        if result == "SUCCESS":
            log.info("Startup liquidation completed for %s.", TRADE_SYMBOL)
        else:
            log.info("Startup liquidation skipped for %s (nothing held): result=%s", TRADE_SYMBOL, result)
        return

    if pending_action is None:
        return

    if side != pending_action:
        log.info("Ignoring out-of-order fill: side=%s while pending=%s", side, pending_action)
        return

    if result != "SUCCESS":
        log.warning("%s fill failed (result=%s). Reverting pending state.", side, result)
        if side == "BUY":
            in_position            = False
            open_position_quantity = "0.0"
            entry_price            = 0.0
            atr_at_entry           = 0.0
        elif side == "SELL":
            in_position = True
            if open_position_quantity in {"", "0", "0.0"}:
                open_position_quantity = pending_sell_quantity
        pending_sell_quantity = "0.0"
        pending_action        = None
        return

    try:
        executed_qty = float(data.get("effective_qty", 0.0) or 0.0)
    except (TypeError, ValueError):
        executed_qty = 0.0

    if side == "BUY":
        net_qty                = math.floor(executed_qty * (1.0 - TRADING_FEE_RATE) * 10) / 10
        in_position            = net_qty > 0
        open_position_quantity = f"{net_qty:.1f}"
        pending_sell_quantity  = "0.0"
        pending_action         = None
        await _save_state(redis_client)
        log.info(
            "BUY confirmed: executed=%.4f net_qty=%s entry=%.6f atr=%.6f",
            executed_qty, open_position_quantity, entry_price, atr_at_entry,
        )

    elif side == "SELL":
        try:
            current_qty = float(open_position_quantity)
        except ValueError:
            current_qty = 0.0
        remaining              = max(current_qty - executed_qty, 0.0)
        in_position            = remaining > 0
        open_position_quantity = f"{remaining:.1f}"
        pending_sell_quantity  = "0.0"
        if remaining <= 0.0:
            entry_price  = 0.0
            atr_at_entry = 0.0
        pending_action = None
        await _save_state(redis_client)
        log.info(
            "SELL confirmed: executed=%.4f remaining_qty=%s",
            executed_qty, open_position_quantity,
        )


# ── Sentiment Update Handler ───────────────────────────────────────────────────
def handle_sentiment_update(raw_payload: str) -> None:
    global sentiment_score

    try:
        data  = json.loads(raw_payload)
        coin  = str(data.get("coin", "GLOBAL")).upper()
        score = float(data.get("compound_score", 0.0))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        log.warning("Malformed sentiment payload skipped: %r | %s", raw_payload, exc)
        return

    if coin not in {"GLOBAL", "FET"}:
        return

    prev           = sentiment_score
    sentiment_score = score
    log.info(
        "Sentiment updated: %.4f → %.4f | entry_gate=%.2f exit_gate=%.2f",
        prev, sentiment_score, SENTIMENT_FEAR_GATE, SENTIMENT_EXIT_GATE,
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
        "FET strategy subscribed | market=%s | sentiment=%s | confirmations=%s",
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
                await handle_execution_confirmation(payload, redis_client)
                continue

            if channel == MARKET_DATA_CHANNEL:
                try:
                    tick      = json.loads(payload)
                    price     = float(tick["mid_price"])
                    timestamp = int(tick.get("timestamp", time.time()))
                    symbol    = str(tick.get("symbol", TRADE_SYMBOL))
                except (KeyError, ValueError, TypeError, json.JSONDecodeError) as exc:
                    log.warning("Malformed market_data skipped: %r | %s", payload, exc)
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
        log.error("Failed to publish startup liquidation: %s", exc)
        raise

    boot_liquidation_pending = True
    in_position              = False
    open_position_quantity   = "0.0"
    pending_action           = "SELL"
    entry_price              = 0.0
    atr_at_entry             = 0.0
    cooldown_until           = 0.0
    pending_sell_quantity    = "0.0"
    log.info("Startup clean-slate liquidation published for %s.", TRADE_SYMBOL)


# ── Entry Point ────────────────────────────────────────────────────────────────
async def run() -> None:
    global boot_liquidation_done

    log.info(
        "FET ATR Breakout Strategy starting | symbol=%s | atr_period=%d | "
        "breakout_k=%.2f | stoploss_k=%.2f | tp=%.1f%% | "
        "sentiment_entry_gate=%.2f | target_notional=$%.2f (%.0f%% of $%.0f)",
        TRADE_SYMBOL, ATR_PERIOD,
        ATR_BREAKOUT_K, ATR_STOPLOSS_K,
        (TAKE_PROFIT_MULT - 1) * 100,
        SENTIMENT_FEAR_GATE,
        TARGET_ORDER_USD, FET_ALLOCATION * 100, PORTFOLIO_USD,
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
                    log.info("No persisted state found — starting without startup liquidation.")
                    boot_liquidation_done = True

            await consume_stream(redis_client)

        except aioredis.RedisError as exc:
            log.error("Redis error: %s. Retrying in %ds.", exc, RETRY_DELAY_SEC)
            await asyncio.sleep(RETRY_DELAY_SEC)
        except Exception as exc:
            log.error("Unexpected error: %s. Retrying in %ds.", exc, RETRY_DELAY_SEC)
            await asyncio.sleep(RETRY_DELAY_SEC)
        finally:
            if redis_client is not None:
                try:
                    await redis_client.aclose()
                except Exception:
                    pass


if __name__ == "__main__":
    asyncio.run(run())
