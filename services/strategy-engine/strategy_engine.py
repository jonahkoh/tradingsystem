"""Process B - Strategy Engine (Sentiment-Adjusted Mean Reversion).

Subscribes to:
    market_data           — mid_price ticks (filtered to TRADE_SYMBOL)
    sentiment_signals     — NLP compound score
    execution_confirmations

Publishes trade signals to trade_signals.

Improvements over v1:
  - 200-period trend filter: BUY only when price > long-term SMA (avoids falling knives)
  - Wider Bollinger window (default 120 × 5s ≈ 10 min, was 20 × 5s ≈ 100s)
  - ATR(14) dynamic stop-loss replacing fixed 3% (adapts to market volatility)
  - Dynamic position sizing: scales TARGET_ORDER_USD by sentiment + volatility
  - Partial exits: 50% at SMA, remaining 50% at upper Bollinger Band
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
ORDER_RESULTS_CHANNEL           = "order_results"
EXECUTION_CONFIRMATIONS_CHANNEL = "execution_confirmations"
POSITION_CHANNEL = "position_updates"
POSITION_KEY = "position:current"

DEFAULT_SYMBOL     = os.getenv("TRADE_SYMBOL", "BTC/USD")
DEFAULT_ORDER_TYPE = "MARKET"

# Symbol prefix: allows per-symbol env var overrides.
# ETH instance reads ETH_WINDOW_PERIOD first, falling back to WINDOW_PERIOD.
# BTC instance reads BTC_WINDOW_PERIOD first, falling back to WINDOW_PERIOD.
_SYM = DEFAULT_SYMBOL.split("/")[0].upper()   # "BTC" or "ETH"

def _cfg(key: str, default: str) -> str:
    return os.getenv(f"{_SYM}_{key}", os.getenv(key, default))

# Bollinger Band window.
WINDOW_PERIOD = int(_cfg("WINDOW_PERIOD", "120"))

# Trend filter: only allow BUY when price > this SMA.
TREND_FILTER_PERIOD = int(_cfg("TREND_FILTER_PERIOD", "480"))

# ATR stop-loss.
ATR_PERIOD      = int(_cfg("ATR_PERIOD",      "14"))
ATR_STOPLOSS_K  = float(_cfg("ATR_STOPLOSS_K", "1.5"))
FALLBACK_STOP_LOSS = float(_cfg("FALLBACK_STOP_LOSS_MULTIPLIER", "0.97"))

# Partial exits: first 50% at SMA, second 50% at upper Bollinger Band.
PARTIAL_EXITS_ENABLED = _cfg("PARTIAL_EXITS_ENABLED", "true").lower() == "true"

# Portfolio and base sizing.
PORTFOLIO_USD    = float(os.getenv("PORTFOLIO_USD",    "1000000"))
RISK_PER_TRADE   = float(_cfg("RISK_PER_TRADE",        "0.02"))
TARGET_ORDER_USD = PORTFOLIO_USD * RISK_PER_TRADE

# Dynamic sizing scalars.
VOLATILITY_SENSITIVITY = float(_cfg("VOLATILITY_SENSITIVITY", "20.0"))
MIN_POSITION_SCALAR    = float(_cfg("MIN_POSITION_SCALAR",    "0.4"))

POLL_TIMEOUT_SEC         = float(os.getenv("STRATEGY_POLL_TIMEOUT_SEC",   "1.0"))
RETRY_DELAY_SEC          = int(os.getenv("STRATEGY_RETRY_DELAY_SEC",      "5"))
TRADING_FEE_RATE         = float(os.getenv("TRADING_FEE_RATE",            "0.001"))
MIN_SELL_MULTIPLIER      = float(_cfg("MIN_SELL_MULTIPLIER",               "1.0025"))
COOLDOWN_SECONDS         = int(_cfg("STOP_LOSS_COOLDOWN_SEC",              "3600"))
CONFIRMATION_TIMEOUT_SEC = float(os.getenv("CONFIRMATION_TIMEOUT_SEC",    "60"))


# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Strategy] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


# ── Sentiment Profiles ─────────────────────────────────────────────────────────
SEVERE_FEAR_PROFILE = {"label": "SEVERE_FEAR", "multiplier": 2.95, "reason": "2.95_SD_Panic_Dip"}
UNEASE_PROFILE      = {"label": "UNEASE",      "multiplier": 2.45, "reason": "2.45_SD_Unease_Dip"}
NEUTRAL_PROFILE     = {"label": "NEUTRAL",     "multiplier": 1.95, "reason": "1.95_SD_Neutral_Dip"}
HYPE_PROFILE        = {"label": "HYPE",        "multiplier": 1.45, "reason": "1.45_SD_Hype_Dip"}

# Dynamic sizing: sentiment reduces/increases base notional.
_SENTIMENT_SIZE_SCALARS: dict[str, float] = {
    "SEVERE_FEAR": 0.50,   # panic market — half size
    "UNEASE":      0.75,
    "NEUTRAL":     1.00,
    "HYPE":        1.15,   # confident market — slightly larger
}


# ── Strategy State ─────────────────────────────────────────────────────────────
# Bollinger Band window
prices: deque[float] = deque(maxlen=WINDOW_PERIOD)

# Trend filter window (longer-term SMA)
trend_prices: deque[float] = deque(maxlen=TREND_FILTER_PERIOD)

# ATR state for dynamic stop-loss
atr_true_ranges: deque[float] = deque(maxlen=ATR_PERIOD)
atr_value: float       = 0.0
atr_initialized: bool  = False
last_atr_price: float | None = None   # previous tick price for true-range computation

# Sentiment
latest_sentiment_score: float = 0.0

# Position state
in_position: bool              = False
open_position_quantity: str    = "0.0000"
entry_price: float             = 0.0
stop_loss_price: float         = 0.0
half_position_sold: bool       = False   # True after first partial exit, False after full close

# Execution handshake state
pending_action: str | None     = None
pending_action_since: float    = 0.0
pending_sell_quantity: str     = "0.0000"
cooldown_until: float          = 0.0

# Startup guards
boot_liquidation_done: bool    = False
boot_liquidation_pending: bool = False

# Redis key for persisted position state (survives container restarts).
STATE_KEY = f"strategy_state:{DEFAULT_SYMBOL}"


# ── Shutdown Handler ───────────────────────────────────────────────────────────
def _handle_shutdown(signum: int, frame: Any) -> None:
    log.info("Shutdown signal received. Exiting strategy engine.")
    sys.exit(0)


signal.signal(signal.SIGINT,  _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


# ── Math Helpers ───────────────────────────────────────────────────────────────
def _mean(values: list[float]) -> float:
    return sum(values) / len(values)


def _std(values: list[float], mean_val: float) -> float:
    variance = sum((v - mean_val) ** 2 for v in values) / len(values)
    return math.sqrt(variance)


# ── ATR (Wilder's Smoothing) ───────────────────────────────────────────────────
def _compute_atr(current_price: float) -> float | None:
    """
    Update ATR state and return current value, or None while still seeding.

    Uses Wilder's smoothing: ATR_t = (ATR_{t-1} × (n-1) + TR_t) / n
    Seeded from simple average of first ATR_PERIOD true ranges.
    """
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
        log.info("ATR(%d) seeded at %.6f.", ATR_PERIOD, atr_value)
        return atr_value

    atr_value = (atr_value * (ATR_PERIOD - 1) + tr) / ATR_PERIOD
    return atr_value


# ── Sentiment / Sizing Helpers ─────────────────────────────────────────────────
def resolve_sd_profile(compound_score: float) -> dict[str, float | str]:
    if compound_score < -0.30:
        return SEVERE_FEAR_PROFILE
    if compound_score < -0.05:
        return UNEASE_PROFILE
    if compound_score <= 0.25:
        return NEUTRAL_PROFILE
    return HYPE_PROFILE


def _dynamic_target_notional(sma: float, std: float, profile_label: str) -> float:
    """
    Scale TARGET_ORDER_USD by volatility and sentiment.

    vol_scalar  = 1 / (1 + VOLATILITY_SENSITIVITY × (std/sma))
    senti_scalar = lookup from _SENTIMENT_SIZE_SCALARS
    """
    norm_vol   = (std / sma) if sma > 0 else 0.0
    vol_scalar = max(MIN_POSITION_SCALAR, 1.0 / (1.0 + VOLATILITY_SENSITIVITY * norm_vol))
    senti_scalar = _SENTIMENT_SIZE_SCALARS.get(profile_label, 1.0)
    notional = TARGET_ORDER_USD * vol_scalar * senti_scalar
    return notional


def _quantity_from_notional(price: float, notional: float) -> str:
    if price <= 0 or notional <= 0:
        return "0.0000"
    qty = math.floor((notional / price) * 10_000) / 10_000
    return f"{qty:.4f}"


def compute_bollinger_inputs(latest_prices: list[float]) -> tuple[float, float]:
    sma = _mean(latest_prices)
    std = _std(latest_prices, sma)
    return sma, std


# ── Signal Publishing ──────────────────────────────────────────────────────────
async def publish_signal(
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
        log.info("Signal published → %s", payload)
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
    global pending_action, in_position, open_position_quantity, entry_price
    global stop_loss_price, cooldown_until, pending_sell_quantity
    global boot_liquidation_pending, half_position_sold

    # Guard: only process ticks for this strategy's configured symbol.
    if symbol != DEFAULT_SYMBOL:
        return

    # ── 1. ATR update (uses previous price, must run before appending) ───────
    current_atr = _compute_atr(price)

    # ── 2. Bollinger window ──────────────────────────────────────────────────
    prices.append(price)
    if len(prices) < WINDOW_PERIOD:
        log.info("Warming Bollinger window: %d/%d prices.", len(prices), WINDOW_PERIOD)
        return

    # ── 3. Trend filter window ───────────────────────────────────────────────
    trend_prices.append(price)
    trend_sma: float | None = (
        _mean(list(trend_prices)) if len(trend_prices) >= TREND_FILTER_PERIOD else None
    )

    # ── 4. Bollinger Band computation ────────────────────────────────────────
    sma, std = compute_bollinger_inputs(list(prices))
    profile       = resolve_sd_profile(latest_sentiment_score)
    dynamic_lower = sma - (float(profile["multiplier"]) * std)
    upper_band    = sma + (float(profile["multiplier"]) * std)

    # ── 5. Dynamic sizing ────────────────────────────────────────────────────
    dyn_notional = _dynamic_target_notional(sma, std, str(profile["label"]))
    quantity     = _quantity_from_notional(price, dyn_notional)

    # ── 6. Tick log ──────────────────────────────────────────────────────────
    state = "IN_POSITION" if in_position else "OUT_OF_POSITION"
    trend_tag = f"trend_sma={trend_sma:.2f}" if trend_sma is not None else "trend=seeding"
    log.info(
        "px=%.6f | SMA=%.6f | SD=%.6f | lower=%.6f | upper=%.6f | "
        "ATR=%.6f | senti=%.4f (%s %.2fSD) | notional=$%.0f | %s | %s",
        price, sma, std, dynamic_lower, upper_band,
        current_atr if current_atr else 0.0,
        latest_sentiment_score, profile["label"], float(profile["multiplier"]),
        dyn_notional, trend_tag, state,
    )

    # ── 7. Suppress while pending ────────────────────────────────────────────
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

    now_ts = time.time()

    # ── 8. Cooldown early cancellation ──────────────────────────────────────
    if cooldown_until > now_ts and price > sma:
        cooldown_until = 0.0
        log.info("Market recovered above SMA — cooldown cancelled early.")

    # ── 9. Entry: BUY ────────────────────────────────────────────────────────
    if not in_position and now_ts > cooldown_until and price < dynamic_lower:
        # Trend filter gate: only buy dips in an uptrend.
        if trend_sma is not None and price <= trend_sma:
            log.info(
                "BUY suppressed by trend filter: px=%.6f <= trend_sma=%.6f (downtrend).",
                price, trend_sma,
            )
            return

        pending_action       = "BUY"
        pending_action_since = time.time()
        entry_price          = price

        # ATR-based stop-loss; fall back to fixed % before ATR seeds.
        if current_atr and current_atr > 0:
            stop_loss_price = entry_price - (ATR_STOPLOSS_K * current_atr)
            log.info(
                "Stop-loss set: entry=%.6f ATR=%.6f k=%.2f stop=%.6f",
                entry_price, current_atr, ATR_STOPLOSS_K, stop_loss_price,
            )
        else:
            stop_loss_price = entry_price * FALLBACK_STOP_LOSS
            log.info(
                "ATR not seeded — using fallback stop-loss %.4f → stop=%.6f",
                FALLBACK_STOP_LOSS, stop_loss_price,
            )

        await publish_signal(
            redis_client,
            action="BUY",
            symbol=symbol,
            quantity=quantity,
            reason=str(profile["reason"]),
            timestamp=timestamp,
            expected_price=price,
        )
        return

    # No exits to evaluate if flat.
    if not in_position:
        return

    # ── 10. Stop-Loss (ATR-based) ────────────────────────────────────────────
    if price <= stop_loss_price:
        sell_qty              = open_position_quantity
        pending_action        = "SELL"
        pending_action_since  = time.time()
        pending_sell_quantity = sell_qty
        cooldown_until        = now_ts + COOLDOWN_SECONDS
        await publish_signal(
            redis_client,
            action="SELL",
            symbol=symbol,
            quantity=sell_qty,
            reason="ATR_StopLoss",
            timestamp=timestamp,
            expected_price=price,
        )
        log.warning(
            "ATR stop-loss triggered: px=%.6f stop=%.6f entry=%.6f",
            price, stop_loss_price, entry_price,
        )
        return

    # ── 11. Take-Profit (partial or full) ────────────────────────────────────
    minimum_sell_price = entry_price * MIN_SELL_MULTIPLIER

    if PARTIAL_EXITS_ENABLED:
        # First partial exit: sell 50% when price >= SMA and fee spread met.
        if not half_position_sold and price >= sma and price >= minimum_sell_price:
            total_qty = float(open_position_quantity)
            half_qty  = math.floor(total_qty / 2 * 10_000) / 10_000

            if half_qty > 0:
                pending_action        = "SELL"
                pending_action_since  = time.time()
                pending_sell_quantity = f"{half_qty:.4f}"
                await publish_signal(
                    redis_client,
                    action="SELL",
                    symbol=symbol,
                    quantity=f"{half_qty:.4f}",
                    reason="Partial_Exit_50pct_SMA",
                    timestamp=timestamp,
                    expected_price=price,
                )
                return
            else:
                # Dust position — skip partial, fall through to full exit below.
                log.warning(
                    "Half qty rounds to zero (total=%.8f) — partial exit skipped.", total_qty,
                )

        # Second partial exit: sell remaining 50% when price reaches upper band.
        if half_position_sold and price >= upper_band:
            sell_qty              = open_position_quantity
            pending_action        = "SELL"
            pending_action_since  = time.time()
            pending_sell_quantity = sell_qty
            await publish_signal(
                redis_client,
                action="SELL",
                symbol=symbol,
                quantity=sell_qty,
                reason="Partial_Exit_50pct_Upper_Band",
                timestamp=timestamp,
                expected_price=price,
            )
            return

    else:
        # Original single-exit: sell all at SMA once fee spread met.
        if price >= sma and price >= minimum_sell_price:
            sell_qty              = open_position_quantity
            pending_action        = "SELL"
            pending_action_since  = time.time()
            pending_sell_quantity = sell_qty
            await publish_signal(
                redis_client,
                action="SELL",
                symbol=symbol,
                quantity=sell_qty,
                reason="Mean_Reversion_TP",
                timestamp=timestamp,
                expected_price=price,
            )
            return

        if price >= sma:
            log.info("Holding position: SMA reached but 0.25%% fee spread not met.")


# ── State Persistence ──────────────────────────────────────────────────────────
async def _save_state(redis_client: aioredis.Redis) -> None:
    """Persist position state to Redis so restarts don't trigger liquidation."""
    try:
        await redis_client.hset(STATE_KEY, mapping={
            "in_position":            str(in_position),
            "open_position_quantity": open_position_quantity,
            "entry_price":            str(entry_price),
            "stop_loss_price":        str(stop_loss_price),
            "half_position_sold":     str(half_position_sold),
            "cooldown_until":         str(cooldown_until),
        })
    except aioredis.RedisError as exc:
        log.warning("Failed to persist state: %s", exc)


async def _load_state(redis_client: aioredis.Redis) -> bool:
    """Load persisted state. Returns True if state was found and loaded."""
    global in_position, open_position_quantity, entry_price
    global stop_loss_price, half_position_sold, cooldown_until
    try:
        state = await redis_client.hgetall(STATE_KEY)
    except aioredis.RedisError as exc:
        log.warning("Failed to load persisted state: %s", exc)
        return False
    if not state or state.get("in_position") is None:
        return False
    in_position            = state.get("in_position", "False") == "True"
    open_position_quantity = state.get("open_position_quantity", "0.0000")
    entry_price            = float(state.get("entry_price",     "0.0"))
    stop_loss_price        = float(state.get("stop_loss_price", "0.0"))
    half_position_sold     = state.get("half_position_sold", "False") == "True"
    cooldown_until         = float(state.get("cooldown_until", "0.0"))
    log.info(
        "Resumed persisted state: in_position=%s qty=%s entry=%.4f stop=%.4f half_sold=%s",
        in_position, open_position_quantity, entry_price, stop_loss_price, half_position_sold,
    )
    return True


# ── Execution Confirmation Handler ─────────────────────────────────────────────
def _extract_executed_quantity(order_result: dict[str, Any]) -> float:
    order_details = order_result.get("order_details")
    if isinstance(order_details, dict):
        for key in ("FilledQuantity", "Quantity"):
            value = order_details.get(key)
            if value not in (None, ""):
                try:
                    return float(value)
                except (TypeError, ValueError):
                    pass

    for key in ("effective_qty", "qty"):
        value = order_result.get(key)
        if value not in (None, ""):
            try:
                return float(value)
            except (TypeError, ValueError):
                pass

    return 0.0


def handle_order_result_update(raw_payload: str) -> None:
    _ = raw_payload
    # Position state is driven by execution_confirmations to avoid Pub/Sub blind spots.
    return


async def handle_execution_confirmation(raw_payload: str, redis_client: aioredis.Redis) -> None:
    global in_position, open_position_quantity, pending_action, entry_price
    global stop_loss_price, pending_sell_quantity, boot_liquidation_pending
    global half_position_sold

    try:
        data = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        log.warning("Malformed execution confirmation skipped: %r | error: %s", raw_payload, exc)
        return

    side   = str(data.get("side", "")).upper()
    if side == "":
        side = str(data.get("action", "")).upper()
    result = str(data.get("result", "")).upper()
    reason = str(data.get("reason", "")).upper()

    if side not in {"BUY", "SELL"}:
        return

    # ── Startup Liquidation Acknowledgment ───────────────────────────────────
    if boot_liquidation_pending and side == "SELL":
        startup_reason = reason in {
            "SYSTEM_REBOOT_LIQUIDATION",
            "STARTUP_BALANCE_SYNC",
            "STARTUP_BALANCE_SYNC_CLEAR",
        }
        if not startup_reason:
            return

        # Clear regardless of result — a failed liquidation means nothing was held (already clean).
        boot_liquidation_pending = False
        pending_action           = None
        pending_sell_quantity    = "0.0000"
        in_position              = False
        open_position_quantity   = "0.0000"
        entry_price              = 0.0
        stop_loss_price          = 0.0
        half_position_sold       = False
        await _save_state(redis_client)
        if result == "SUCCESS":
            log.info("Startup liquidation completed.")
        else:
            log.info("Startup liquidation skipped (nothing held): result=%s", result)
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
            stop_loss_price        = 0.0
            half_position_sold     = False
        elif side == "SELL":
            in_position = True
            if open_position_quantity in {"", "0", "0.0", "0.0000"}:
                open_position_quantity = pending_sell_quantity
        pending_sell_quantity = "0.0000"
        pending_action        = None
        return

    executed_qty = _extract_executed_quantity(data)

    if side == "BUY":
        net_qty                = max(executed_qty * (1.0 - TRADING_FEE_RATE), 0.0)
        in_position            = net_qty > 0
        open_position_quantity = f"{net_qty:.4f}"
        pending_sell_quantity  = "0.0000"
        pending_action         = None
        half_position_sold     = False
        await _save_state(redis_client)
        log.info(
            "BUY confirmed: executed=%.8f fee_rate=%.4f net_qty=%s entry=%.6f stop=%.6f",
            executed_qty, TRADING_FEE_RATE, open_position_quantity,
            entry_price, stop_loss_price,
        )
        return

    if side == "SELL":
        try:
            current_qty = float(open_position_quantity)
        except ValueError:
            current_qty = 0.0

        remaining              = max(current_qty - executed_qty, 0.0)
        in_position            = remaining > 0
        open_position_quantity = f"{remaining:.4f}"
        pending_sell_quantity  = "0.0000"

        if remaining <= 0.0:
            entry_price        = 0.0
            stop_loss_price    = 0.0
            half_position_sold = False
        else:
            half_position_sold = True

        pending_action = None
        await _save_state(redis_client)
        log.info(
            "SELL confirmed: executed=%.8f remaining_qty=%s half_sold=%s",
            executed_qty, open_position_quantity, half_position_sold,
        )


def handle_sentiment_update(raw_payload: str) -> None:
    global latest_sentiment_score

    try:
        data  = json.loads(raw_payload)
        score = float(data.get("compound_score", 0.0))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        log.warning("Malformed sentiment payload skipped: %r | error: %s", raw_payload, exc)
        return

    latest_sentiment_score = score
    profile = resolve_sd_profile(latest_sentiment_score)
    log.info(
        "Sentiment updated: score=%.4f → profile=%s (%.2f SD)",
        latest_sentiment_score, profile["label"], float(profile["multiplier"]),
    )


async def _publish_position_state(redis_client: aioredis.Redis) -> None:
    profile = resolve_sd_profile(latest_sentiment_score)
    snapshot = json.dumps({
        "in_position": in_position,
        "quantity": open_position_quantity,
        "entry_price": entry_price,
        "stop_loss_price": stop_loss_price,
        "symbol": DEFAULT_SYMBOL,
        "pending_action": pending_action,
        "sentiment": profile["label"],
        "timestamp": int(time.time()),
    })
    await redis_client.set(POSITION_KEY, snapshot)
    await redis_client.publish(POSITION_CHANNEL, snapshot)


# ── Pub/Sub Consumer ───────────────────────────────────────────────────────────
async def consume_stream(redis_client: aioredis.Redis) -> None:
    pubsub = redis_client.pubsub(ignore_subscribe_messages=True)
    await pubsub.subscribe(
        MARKET_DATA_CHANNEL,
        SENTIMENT_CHANNEL,
        ORDER_RESULTS_CHANNEL,
        EXECUTION_CONFIRMATIONS_CHANNEL,
    )
    log.info(
        "Strategy engine subscribed to: %s, %s, %s, %s",
        MARKET_DATA_CHANNEL, SENTIMENT_CHANNEL,
        ORDER_RESULTS_CHANNEL, EXECUTION_CONFIRMATIONS_CHANNEL,
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

            if channel == ORDER_RESULTS_CHANNEL:
                handle_order_result_update(payload)
                continue

            if channel == EXECUTION_CONFIRMATIONS_CHANNEL:
                await handle_execution_confirmation(payload, redis_client)
                await _publish_position_state(redis_client)
                continue

            if channel != MARKET_DATA_CHANNEL:
                continue

            try:
                tick      = json.loads(payload)
                price     = float(tick["mid_price"])
                timestamp = int(tick.get("timestamp", time.time()))
                symbol    = str(tick.get("symbol", DEFAULT_SYMBOL))
            except (KeyError, ValueError, TypeError, json.JSONDecodeError) as exc:
                log.warning("Malformed market_data payload skipped: %r | error: %s", payload, exc)
                continue

            await evaluate_market_tick(price, symbol, timestamp, redis_client)

    finally:
        await pubsub.unsubscribe(
            MARKET_DATA_CHANNEL,
            SENTIMENT_CHANNEL,
            ORDER_RESULTS_CHANNEL,
            EXECUTION_CONFIRMATIONS_CHANNEL,
        )
        await pubsub.aclose()


# ── Startup Liquidation ────────────────────────────────────────────────────────
async def initialize_clean_slate(redis_client: aioredis.Redis) -> None:
    global in_position, open_position_quantity, pending_action, entry_price
    global stop_loss_price, cooldown_until, pending_sell_quantity
    global boot_liquidation_pending, half_position_sold

    payload = {
        "action":   "SELL",
        "symbol":   DEFAULT_SYMBOL,
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
    stop_loss_price          = 0.0
    cooldown_until           = 0.0
    pending_sell_quantity    = "0.0000"
    half_position_sold       = False
    log.info("Startup clean-slate liquidation signal published for %s.", DEFAULT_SYMBOL)


# ── Entry Point ────────────────────────────────────────────────────────────────
async def run() -> None:
    global boot_liquidation_done

    log.info(
        "Strategy engine starting | symbol=%s | bollinger_window=%d (≈%.0fs) | "
        "trend_filter=%d (≈%.0fs) | atr_period=%d | atr_stop_k=%.2f | "
        "partial_exits=%s | target_notional=$%.2f",
        DEFAULT_SYMBOL,
        WINDOW_PERIOD,   WINDOW_PERIOD   * 5,
        TREND_FILTER_PERIOD, TREND_FILTER_PERIOD * 5,
        ATR_PERIOD, ATR_STOPLOSS_K,
        PARTIAL_EXITS_ENABLED,
        TARGET_ORDER_USD,
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

            # if not boot_liquidation_done:
            #     await asyncio.sleep(8)
            #     if await _load_state(redis_client):
            #         log.info("Resumed from persisted state — skipping startup liquidation.")
            #         boot_liquidation_done = True
            #     else:
            #         await initialize_clean_slate(redis_client)
            #         boot_liquidation_done = True

            await consume_stream(redis_client)

        except aioredis.RedisError as exc:
            log.error("Redis error: %s. Retrying in %ds.", exc, RETRY_DELAY_SEC)
            await asyncio.sleep(RETRY_DELAY_SEC)
        except Exception as exc:
            log.error(
                "Unexpected error in strategy run loop: %s. Retrying in %ds.",
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
