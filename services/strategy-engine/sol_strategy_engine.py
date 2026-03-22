"""Process H — SOL DCA (Dollar Cost Averaging) Strategy Engine.

SOL is a high-beta, retail-driven asset that trends hard in both directions.
In bearish/choppy markets, EMA crossover whipsaws repeatedly. DCA adapts by
buying in tranches as price falls, lowering the average cost basis so that a
partial recovery is enough to take profit.

Strategy: DCA Tranche Buying with Sentiment-Scaled Sizing
──────────────────────────────────────────────────────────
  ENTRY 1 — First tranche: price dips INITIAL_DIP_PCT% below SMA(20)
             AND sentiment > SENTIMENT_GATE
  ENTRY N — Each subsequent tranche: price dips TRANCHE_DIP_PCT% below
             the previous buy price, up to MAX_TRANCHES total

  EXIT 1  — Take-profit: price >= average_cost × (1 + PROFIT_MARGIN)
             Sells ALL tranches in one signal
  EXIT 2  — ATR stop-loss: price drops > ATR_STOPLOSS_K × ATR from average_cost
             Safety net against catastrophic drops

Tranche size is sentiment-scaled:
  SEVERE_FEAR → 0.5×  (capital preservation)
  UNEASE      → 0.75×
  NEUTRAL     → 1.0×
  HYPE        → 1.2×  (higher conviction)

ATR uses Wilder's smoothing (same as all other strategies).

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

TRADE_SYMBOL       = "SOL/USD"
DEFAULT_ORDER_TYPE = "MARKET"

# DCA strategy parameters.
PROFIT_MARGIN   = float(os.getenv("SOL_DCA_PROFIT_MARGIN",  "0.02"))   # 2% take-profit above avg cost
INITIAL_DIP_PCT = float(os.getenv("SOL_DCA_INITIAL_DIP",    "0.01"))   # 1% below SMA for first tranche
TRANCHE_DIP_PCT = float(os.getenv("SOL_DCA_TRANCHE_DIP",    "0.015"))  # 1.5% below last buy for next
MAX_TRANCHES    = int(os.getenv("SOL_DCA_MAX_TRANCHES",      "5"))
SMA_PERIOD      = int(os.getenv("SOL_DCA_SMA_PERIOD",        "20"))
SENTIMENT_GATE  = float(os.getenv("SOL_DCA_SENTIMENT_GATE",  "-0.30"))

# Sentiment scalars for tranche sizing — mirrors BTC/ETH strategy regime thresholds.
SENTIMENT_SCALAR_SEVERE_FEAR = float(os.getenv("SOL_DCA_SCALAR_SEVERE_FEAR", "0.5"))
SENTIMENT_SCALAR_UNEASE      = float(os.getenv("SOL_DCA_SCALAR_UNEASE",      "0.75"))
SENTIMENT_SCALAR_NEUTRAL     = float(os.getenv("SOL_DCA_SCALAR_NEUTRAL",     "1.0"))
SENTIMENT_SCALAR_HYPE        = float(os.getenv("SOL_DCA_SCALAR_HYPE",        "1.2"))

# ATR stop-loss — safety net for catastrophic drawdowns.
ATR_PERIOD      = int(os.getenv("SOL_ATR_PERIOD",               "14"))
ATR_STOPLOSS_K  = float(os.getenv("SOL_ATR_STOPLOSS_K",          "2.0"))
FALLBACK_STOP   = float(os.getenv("SOL_FALLBACK_STOP_MULTIPLIER", "0.95"))  # 5% fixed fallback

# Portfolio sizing — SOL gets 20% of portfolio.
PORTFOLIO_USD    = float(os.getenv("PORTFOLIO_USD",  "1000000"))
SOL_ALLOCATION   = float(os.getenv("SOL_ALLOCATION", "0.20"))
TARGET_ORDER_USD = PORTFOLIO_USD * SOL_ALLOCATION

TRADING_FEE_RATE         = float(os.getenv("TRADING_FEE_RATE",            "0.001"))
POLL_TIMEOUT_SEC         = float(os.getenv("STRATEGY_POLL_TIMEOUT_SEC",   "1.0"))
RETRY_DELAY_SEC          = int(os.getenv("STRATEGY_RETRY_DELAY_SEC",      "5"))
COOLDOWN_SECONDS         = int(os.getenv("STOP_LOSS_COOLDOWN_SEC",        "3600"))
CONFIRMATION_TIMEOUT_SEC = float(os.getenv("CONFIRMATION_TIMEOUT_SEC",    "60"))


# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SOL-Strategy] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


# ── Strategy State ─────────────────────────────────────────────────────────────
# SMA window for initial dip detection
sma_prices: deque[float] = deque(maxlen=SMA_PERIOD)

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
average_cost: float         = 0.0   # weighted average cost across all tranches
atr_at_entry: float         = 0.0   # ATR snapshot at first entry — stop-loss anchor

# DCA tranche tracking
tranche_count: int    = 0     # number of tranches bought
last_buy_price: float = 0.0   # price of last confirmed BUY — anchor for next dip

# Execution handshake
pending_action: str | None  = None
pending_action_since: float = 0.0
pending_sell_quantity: str  = "0.0000"
cooldown_until: float       = 0.0

# Startup guards
boot_liquidation_done: bool    = False
boot_liquidation_pending: bool = False

STATE_KEY = f"strategy_state:{TRADE_SYMBOL}"


# ── Shutdown Handler ───────────────────────────────────────────────────────────
def _handle_shutdown(signum: int, frame: Any) -> None:
    log.info("Shutdown signal %s received. Exiting SOL strategy engine.", signum)
    sys.exit(0)


signal.signal(signal.SIGINT,  _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


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
    """Floor-divide full target notional by price to whole units."""
    if price <= 0:
        return "0"
    qty = int(math.floor(TARGET_ORDER_USD / price))
    return str(qty)


def _sentiment_scalar() -> float:
    """Map current sentiment score to a tranche size multiplier."""
    score = global_sentiment_score
    if score < -0.30:
        return SENTIMENT_SCALAR_SEVERE_FEAR
    if score < -0.05:
        return SENTIMENT_SCALAR_UNEASE
    if score <= 0.25:
        return SENTIMENT_SCALAR_NEUTRAL
    return SENTIMENT_SCALAR_HYPE


def _tranche_quantity(price: float) -> str:
    """One tranche = (TARGET_ORDER_USD / MAX_TRANCHES) × sentiment_scalar, in whole units."""
    if price <= 0:
        return "0"
    tranche_usd = (TARGET_ORDER_USD / MAX_TRANCHES) * _sentiment_scalar()
    qty = int(math.floor(tranche_usd / price))
    return str(qty)


# ── State Persistence ──────────────────────────────────────────────────────────
async def _save_state(redis_client: aioredis.Redis) -> None:
    try:
        await redis_client.hset(STATE_KEY, mapping={
            "in_position":            str(in_position),
            "open_position_quantity": open_position_quantity,
            "average_cost":           str(average_cost),
            "atr_at_entry":           str(atr_at_entry),
            "cooldown_until":         str(cooldown_until),
            "tranche_count":          str(tranche_count),
            "last_buy_price":         str(last_buy_price),
        })
    except aioredis.RedisError as exc:
        log.warning("Failed to persist state: %s", exc)


async def _load_state(redis_client: aioredis.Redis) -> bool:
    global in_position, open_position_quantity, average_cost, atr_at_entry
    global cooldown_until, tranche_count, last_buy_price
    try:
        state = await redis_client.hgetall(STATE_KEY)
    except aioredis.RedisError as exc:
        log.warning("Failed to load persisted state: %s", exc)
        return False
    if not state or state.get("in_position") is None:
        return False
    in_position            = state.get("in_position", "False") == "True"
    open_position_quantity = state.get("open_position_quantity", "0")
    average_cost           = float(state.get("average_cost",    "0.0"))
    atr_at_entry           = float(state.get("atr_at_entry",    "0.0"))
    cooldown_until         = float(state.get("cooldown_until",  "0.0"))
    tranche_count          = int(float(state.get("tranche_count",  "0")))
    last_buy_price         = float(state.get("last_buy_price",  "0.0"))

    # Migration shim: old state used entry_price — promote it to average_cost.
    if in_position and average_cost == 0.0:
        legacy = float(state.get("entry_price", "0.0"))
        if legacy > 0:
            average_cost = legacy
            log.warning("Migrated legacy entry_price=%.4f to average_cost.", average_cost)

    # Defensive: if not in position, tranche state must be clean.
    if not in_position:
        tranche_count  = 0
        last_buy_price = 0.0
        average_cost   = 0.0

    log.info(
        "Resumed persisted state: in_position=%s qty=%s avg_cost=%.4f "
        "tranches=%d last_buy=%.4f atr_entry=%.6f",
        in_position, open_position_quantity, average_cost,
        tranche_count, last_buy_price, atr_at_entry,
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
    global average_cost, tranche_count, last_buy_price, atr_at_entry
    global pending_action, pending_action_since, in_position, open_position_quantity
    global cooldown_until, pending_sell_quantity
    global boot_liquidation_pending

    # Guard: only process ticks for this strategy's symbol.
    if symbol != TRADE_SYMBOL:
        return

    # Guard: zero/negative prices cause division errors.
    if price <= 0:
        return

    # ── ATR update ───────────────────────────────────────────────────────────
    current_atr = _compute_atr(price)

    # ── SMA update ───────────────────────────────────────────────────────────
    sma_prices.append(price)

    # ── SMA warm-up ──────────────────────────────────────────────────────────
    if len(sma_prices) < SMA_PERIOD:
        log.info("SMA warm-up: %d/%d prices.", len(sma_prices), SMA_PERIOD)
        return

    sma    = sum(sma_prices) / SMA_PERIOD
    now_ts = time.time()
    state  = "IN_POSITION" if in_position else "OUT_OF_POSITION"
    tp_price = average_cost * (1.0 + PROFIT_MARGIN) if average_cost > 0 else 0.0

    log.info(
        "px=%.6f | SMA=%.6f | avg_cost=%.6f | tranches=%d/%d | ATR=%.6f | "
        "senti=%.4f (%.2f×) | %s | tp=%.6f | cooldown=%.0fs",
        price, sma, average_cost, tranche_count, MAX_TRANCHES,
        current_atr if current_atr else 0.0,
        global_sentiment_score, _sentiment_scalar(),
        state, tp_price,
        max(cooldown_until - now_ts, 0.0),
    )

    # ── Suppress while pending ────────────────────────────────────────────────
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
        log.info("Signal suppressed: awaiting startup liquidation.")
        return

    # ── Entry: First Tranche ──────────────────────────────────────────────────
    if (
        not in_position
        and tranche_count == 0
        and now_ts > cooldown_until
        and price <= sma * (1.0 - INITIAL_DIP_PCT)
        and global_sentiment_score > SENTIMENT_GATE
    ):
        quantity = _tranche_quantity(price)
        if int(quantity) <= 0:
            log.warning("First tranche quantity rounds to 0 at price=%.6f — skipping.", price)
            return
        pending_action       = "BUY"
        pending_action_since = time.time()
        # Provisional average_cost — updated on confirmation with actual fill price.
        average_cost         = price
        last_buy_price       = price
        atr_at_entry         = current_atr if current_atr and current_atr > 0 else 0.0
        await _publish_signal(
            redis_client,
            action="BUY",
            symbol=symbol,
            quantity=quantity,
            reason=(
                f"DCA_Tranche1_dip={INITIAL_DIP_PCT*100:.1f}pct"
                f"_sma={sma:.4f}_senti={global_sentiment_score:.3f}"
                f"_scalar={_sentiment_scalar():.2f}x"
            ),
            timestamp=timestamp,
            expected_price=price,
        )
        return

    # ── Entry: Additional Tranches ────────────────────────────────────────────
    if (
        in_position
        and tranche_count < MAX_TRANCHES
        and last_buy_price > 0
        and price <= last_buy_price * (1.0 - TRANCHE_DIP_PCT)
        and now_ts > cooldown_until
        and global_sentiment_score > SENTIMENT_GATE
    ):
        quantity = _tranche_quantity(price)
        if int(quantity) <= 0:
            log.warning(
                "Tranche %d quantity rounds to 0 at price=%.6f — skipping.",
                tranche_count + 1, price,
            )
            # Fall through to exit checks.
        else:
            pending_action       = "BUY"
            pending_action_since = time.time()
            last_buy_price       = price   # provisional; confirmed in handler
            await _publish_signal(
                redis_client,
                action="BUY",
                symbol=symbol,
                quantity=quantity,
                reason=(
                    f"DCA_Tranche{tranche_count + 1}_dip={TRANCHE_DIP_PCT*100:.1f}pct"
                    f"_last={last_buy_price:.4f}_senti={global_sentiment_score:.3f}"
                    f"_scalar={_sentiment_scalar():.2f}x"
                ),
                timestamp=timestamp,
                expected_price=price,
            )
            return

    # No exits to evaluate if flat.
    if not in_position:
        return

    # Compute stop price from ATR captured at entry (or fallback).
    if atr_at_entry > 0:
        stop_loss_price = average_cost - (ATR_STOPLOSS_K * atr_at_entry)
    else:
        stop_loss_price = average_cost * FALLBACK_STOP

    # ── Exit 1: ATR Stop-Loss ─────────────────────────────────────────────────
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
            "ATR stop-loss triggered: px=%.6f avg_cost=%.6f stop=%.6f atr_entry=%.6f tranches=%d",
            price, average_cost, stop_loss_price, atr_at_entry, tranche_count,
        )
        return

    # ── Exit 2: Take-Profit ───────────────────────────────────────────────────
    take_profit_price = average_cost * (1.0 + PROFIT_MARGIN)
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
            reason=f"DCA_TakeProfit_{int(PROFIT_MARGIN*100)}pct_avg={average_cost:.4f}",
            timestamp=timestamp,
            expected_price=price,
        )
        log.info(
            "DCA take-profit triggered: px=%.6f avg_cost=%.6f target=%.6f "
            "gain=%.2f%% tranches=%d",
            price, average_cost, take_profit_price,
            (price / average_cost - 1.0) * 100, tranche_count,
        )
        return


# ── Execution Confirmation Handler ─────────────────────────────────────────────
async def handle_execution_confirmation(raw_payload: str, redis_client: aioredis.Redis) -> None:
    global in_position, open_position_quantity, pending_action, average_cost
    global atr_at_entry, pending_sell_quantity, boot_liquidation_pending
    global tranche_count, last_buy_price

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

        boot_liquidation_pending = False
        pending_action           = None
        pending_sell_quantity    = "0.0000"
        in_position              = False
        open_position_quantity   = "0.0000"
        average_cost             = 0.0
        atr_at_entry             = 0.0
        tranche_count            = 0
        last_buy_price           = 0.0
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
            if tranche_count == 0:
                # First tranche failed — full revert.
                in_position            = False
                open_position_quantity = "0.0000"
                average_cost           = 0.0
                atr_at_entry           = 0.0
                last_buy_price         = 0.0
            # For nth tranche failures, preserve the existing position — tranche_count
            # is NOT incremented on failure, so state remains consistent.
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
        net_qty = int(math.floor(executed_qty * (1.0 - TRADING_FEE_RATE)))
        if net_qty > 0:
            prev_qty      = int(float(open_position_quantity)) if in_position else 0
            new_total_qty = prev_qty + net_qty
            # Weighted average cost update.
            buy_price    = last_buy_price if last_buy_price > 0 else average_cost
            average_cost = (average_cost * prev_qty + buy_price * net_qty) / new_total_qty
            open_position_quantity = str(new_total_qty)
            in_position   = True
            tranche_count += 1
            last_buy_price = buy_price
        pending_sell_quantity = "0"
        pending_action        = None
        await _save_state(redis_client)
        log.info(
            "BUY confirmed (tranche %d/%d): executed=%.8f net_qty=%d "
            "total_qty=%s avg_cost=%.6f last_buy=%.6f",
            tranche_count, MAX_TRANCHES,
            executed_qty, net_qty, open_position_quantity, average_cost, last_buy_price,
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
            average_cost   = 0.0
            atr_at_entry   = 0.0
            tranche_count  = 0
            last_buy_price = 0.0
        pending_action = None
        await _save_state(redis_client)
        log.info(
            "SELL confirmed: executed=%.8f remaining_qty=%s tranches_reset=%s",
            executed_qty, open_position_quantity, remaining <= 0.0,
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
        "Global sentiment updated: %.4f → %.4f | gate=%.2f | scalar=%.2f×",
        prev, global_sentiment_score, SENTIMENT_GATE, _sentiment_scalar(),
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
            SENTIMENT_CHANNEL,
            EXECUTION_CONFIRMATIONS_CHANNEL,
        )
        await pubsub.aclose()


# ── Startup Liquidation ────────────────────────────────────────────────────────
async def initialize_clean_slate(redis_client: aioredis.Redis) -> None:
    global in_position, open_position_quantity, pending_action, average_cost
    global atr_at_entry, cooldown_until, pending_sell_quantity, boot_liquidation_pending
    global tranche_count, last_buy_price

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
    average_cost             = 0.0
    atr_at_entry             = 0.0
    cooldown_until           = 0.0
    pending_sell_quantity    = "0.0000"
    tranche_count            = 0
    last_buy_price           = 0.0
    log.info("Startup clean-slate liquidation signal published for %s.", TRADE_SYMBOL)


# ── Entry Point ────────────────────────────────────────────────────────────────
async def run() -> None:
    global boot_liquidation_done

    log.info(
        "SOL DCA Strategy starting | symbol=%s | "
        "profit_margin=%.1f%% | initial_dip=%.1f%% | tranche_dip=%.1f%% | "
        "max_tranches=%d | sma_period=%d | sentiment_gate=%.2f | "
        "atr_period=%d | atr_stop_k=%.2f | target_notional=$%.2f (%.0f%% of $%.0f)",
        TRADE_SYMBOL,
        PROFIT_MARGIN * 100, INITIAL_DIP_PCT * 100, TRANCHE_DIP_PCT * 100,
        MAX_TRANCHES, SMA_PERIOD, SENTIMENT_GATE,
        ATR_PERIOD, ATR_STOPLOSS_K,
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
                await asyncio.sleep(8)
                if await _load_state(redis_client):
                    log.info("Resumed from persisted state — skipping startup liquidation.")
                    boot_liquidation_done = True
                else:
                    log.info("No persisted state found — starting without startup liquidation.")
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
