"""Process H — SOL Adaptive Multi-Regime Strategy Engine.

SOL is a high-beta, retail-driven asset that transitions between distinct market
regimes. A single fixed strategy underperforms across all conditions:
  • DCA alone sits idle in flat markets and whipsaws in strong trends.
  • Breakout alone generates false entries in ranges.
  • Mean-reversion alone loses badly in strong trends.

This engine detects the current market regime from three independent indicators
and applies the optimal strategy for each:

  RANGING       → Bollinger Band mean reversion
                  BUY at lower band (SMA20 − 2σ), SELL at SMA20 (or above)

  TRENDING_UP   → ATR Breakout (momentum)
                  BUY when breakout holds for SOL_BREAKOUT_CONFIRM_TICKS
                  with delta > ATR_BREAKOUT_K × ATR
                  SELL at BREAKOUT_TP_MULT above entry

  TRENDING_DOWN → DCA Tranches (cost-averaging into the decline)
  UNCLEAR       → DCA Tranches with 0.5× reduced sizing (conservative)

Regime Detection — majority vote, 2 of 3 indicators must agree:
  ATR%       = ATR / price              trending > 0.5%,   ranging < 0.2%
  BB width   = (upper−lower) / SMA20   trending > 2%,     ranging < 1%
  Price dev  = |price − SMA50| / SMA50  trending > 2%,     ranging < 1%

Regime dwell: a new regime must persist for REGIME_DWELL_TICKS consecutive ticks
before the engine commits to it, preventing whipsawing on noisy signals.

Position Sizing — volatility-targeted:
  qty = RISK_PER_TRADE_USD × scalar / ATR  (capped at allocation / price)
  Sentiment scalar:  SEVERE_FEAR 0.5×, UNEASE 0.75×, NEUTRAL 1.0×, HYPE 1.2×
  Sharpe scalar:     clamp(0.9 + rolling_sharpe × 0.6, 0.3, 1.5)
  Combined, sizing automatically shrinks in high-volatility or losing streaks
  and expands in low-volatility profitable periods — up to the allocation cap.

Exit logic uses the regime at entry (not the current regime) so a position
opened in one mode isn't prematurely closed by a different mode's threshold.
ATR stop-loss is regime-agnostic and always has highest priority.

Subscribes to:
    market_data       — mid_price ticks (filtered to SOL/USD only)
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
import statistics
import sys
import time
from collections import deque
from typing import Any

import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()


# ── Channels & Symbol ──────────────────────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

MARKET_DATA_CHANNEL             = "market_data"
SENTIMENT_CHANNEL               = "sentiment_signals"
TRADE_SIGNALS_CHANNEL           = "trade_signals"
EXECUTION_CONFIRMATIONS_CHANNEL = "execution_confirmations"

TRADE_SYMBOL       = "SOL/USD"
DEFAULT_ORDER_TYPE = "MARKET"

# ── ATR (shared across all modes) ─────────────────────────────────────────────
ATR_PERIOD     = int(os.getenv("SOL_ATR_PERIOD",               "14"))
ATR_STOPLOSS_K = float(os.getenv("SOL_ATR_STOPLOSS_K",         "2.0"))
FALLBACK_STOP  = float(os.getenv("SOL_FALLBACK_STOP_MULTIPLIER", "0.95"))

# ── Regime Detection ──────────────────────────────────────────────────────────
ATR_PCT_TRENDING   = float(os.getenv("SOL_ATR_PCT_TRENDING",   "0.005"))  # 0.5%
ATR_PCT_RANGING    = float(os.getenv("SOL_ATR_PCT_RANGING",    "0.002"))  # 0.2%
BB_WIDTH_TRENDING  = float(os.getenv("SOL_BB_WIDTH_TRENDING",  "0.02"))   # 2%
BB_WIDTH_RANGING   = float(os.getenv("SOL_BB_WIDTH_RANGING",   "0.01"))   # 1%
PRICE_DEV_TRENDING = float(os.getenv("SOL_PRICE_DEV_TRENDING", "0.02"))   # 2%
PRICE_DEV_RANGING  = float(os.getenv("SOL_PRICE_DEV_RANGING",  "0.01"))   # 1%
REGIME_DWELL_TICKS = int(os.getenv("SOL_REGIME_DWELL_TICKS",  "120"))     # ~10 min at 5s/tick

SMA_PERIOD   = int(os.getenv("SOL_SMA_PERIOD", "20"))
SMA50_PERIOD = 50   # fixed; used as trend-direction anchor
BB_STD_MULT  = float(os.getenv("SOL_BB_STD_MULT", "2.0"))

# Minimum ticks before any signal — need SMA50 to be valid.
WARMUP_TICKS = SMA50_PERIOD

# ── RANGING: Bollinger Band Mean Reversion ────────────────────────────────────
BB_PROFIT_MARGIN = float(os.getenv("SOL_BB_PROFIT_MARGIN", "0.005"))  # 0.5% min gain at exit

# ── TRENDING_UP: ATR Breakout + EMA death-cross exit ─────────────────────────
ATR_BREAKOUT_K = float(os.getenv("SOL_ATR_BREAKOUT_K", "2.0"))
BREAKOUT_CONFIRM_TICKS = int(os.getenv("SOL_BREAKOUT_CONFIRM_TICKS", "2"))
EMA_FAST_PERIOD = int(os.getenv("SOL_EMA_FAST_PERIOD", "7"))    # fast EMA for cross exit
EMA_SLOW_PERIOD = int(os.getenv("SOL_EMA_SLOW_PERIOD", "99"))   # slow EMA for cross exit

# ── Stop-loss floor ───────────────────────────────────────────────────────────
MIN_STOP_PCT = float(os.getenv("SOL_MIN_STOP_PCT", "0.005"))     # 0.5% min stop distance

# ── TRENDING_DOWN / UNCLEAR: DCA Tranches ────────────────────────────────────
PROFIT_MARGIN   = float(os.getenv("SOL_DCA_PROFIT_MARGIN", "0.02"))   # 2% TP
INITIAL_DIP_PCT = float(os.getenv("SOL_DCA_INITIAL_DIP",   "0.01"))   # 1% below SMA20
TRANCHE_DIP_PCT = float(os.getenv("SOL_DCA_TRANCHE_DIP",   "0.015"))  # 1.5% below last buy
MAX_TRANCHES    = int(os.getenv("SOL_DCA_MAX_TRANCHES",     "5"))
SENTIMENT_GATE  = float(os.getenv("SOL_DCA_SENTIMENT_GATE", "-0.30"))

# ── Position Sizing ───────────────────────────────────────────────────────────
SENTIMENT_SCALAR_SEVERE_FEAR = float(os.getenv("SOL_DCA_SCALAR_SEVERE_FEAR", "0.5"))
SENTIMENT_SCALAR_UNEASE      = float(os.getenv("SOL_DCA_SCALAR_UNEASE",      "0.75"))
SENTIMENT_SCALAR_NEUTRAL     = float(os.getenv("SOL_DCA_SCALAR_NEUTRAL",     "1.0"))
SENTIMENT_SCALAR_HYPE        = float(os.getenv("SOL_DCA_SCALAR_HYPE",        "1.2"))

# Dollar risk per ATR move: qty = RISK_PER_TRADE_USD × scalar / ATR
RISK_PER_TRADE_USD = float(os.getenv("SOL_RISK_PER_TRADE_USD", "2000.0"))
SHARPE_WINDOW      = int(os.getenv("SOL_SHARPE_WINDOW",        "10"))

# ── Portfolio ─────────────────────────────────────────────────────────────────
PORTFOLIO_USD    = float(os.getenv("PORTFOLIO_USD",  "1000000"))
SOL_ALLOCATION   = float(os.getenv("SOL_ALLOCATION", "0.20"))
TARGET_ORDER_USD = PORTFOLIO_USD * SOL_ALLOCATION

TRADING_FEE_RATE         = float(os.getenv("TRADING_FEE_RATE",            "0.001"))
POLL_TIMEOUT_SEC         = float(os.getenv("STRATEGY_POLL_TIMEOUT_SEC",   "1.0"))
RETRY_DELAY_SEC          = int(os.getenv("STRATEGY_RETRY_DELAY_SEC",      "5"))
COOLDOWN_SECONDS         = int(os.getenv("SOL_STOP_LOSS_COOLDOWN_SEC", os.getenv("STOP_LOSS_COOLDOWN_SEC", "3600")))
CONFIRMATION_TIMEOUT_SEC = float(os.getenv("CONFIRMATION_TIMEOUT_SEC",    "60"))


# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SOL-Strategy] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


# ── ATR State ─────────────────────────────────────────────────────────────────
atr_true_ranges: deque[float] = deque(maxlen=ATR_PERIOD)
atr_value:       float        = 0.0
atr_initialized: bool         = False
last_atr_price:  float | None = None

# ── Price Windows ─────────────────────────────────────────────────────────────
sma_prices:   deque[float] = deque(maxlen=SMA_PERIOD)    # SMA20 / BB window
sma50_prices: deque[float] = deque(maxlen=SMA50_PERIOD)  # SMA50 for regime direction

# ── Regime ────────────────────────────────────────────────────────────────────
current_regime:   str = "UNCLEAR"   # committed regime
candidate_regime: str = "UNCLEAR"   # regime accumulating dwell ticks
candidate_ticks:  int = 0           # how many consecutive ticks candidate has held

# ── Sentiment ─────────────────────────────────────────────────────────────────
global_sentiment_score: float = 0.0

# ── Position ──────────────────────────────────────────────────────────────────
in_position:            bool  = False
open_position_quantity: str   = "0.0000"
average_cost:           float = 0.0
atr_at_entry:           float = 0.0
entry_regime:           str   = ""   # regime under which the position was opened

# ── DCA State ─────────────────────────────────────────────────────────────────
tranche_count: int   = 0
last_buy_price: float = 0.0

# ── Rolling Sharpe ────────────────────────────────────────────────────────────
trade_returns:     deque[float] = deque(maxlen=SHARPE_WINDOW)
pending_sell_price: float       = 0.0   # price at time of SELL signal for return calc

# ── Breakout: previous tick price for delta ───────────────────────────────────
prev_price: float | None = None
breakout_streak: int = 0

# ── EMA(7) / EMA(99) for death-cross exit ────────────────────────────────────
ema_fast: float | None = None   # EMA(7)
ema_slow: float | None = None   # EMA(99)
alpha_fast: float = 2.0 / (EMA_FAST_PERIOD + 1)
alpha_slow: float = 2.0 / (EMA_SLOW_PERIOD + 1)

# ── Execution Handshake ───────────────────────────────────────────────────────
pending_action:       str | None = None
pending_action_since: float      = 0.0
pending_sell_quantity: str       = "0.0000"
cooldown_until:       float      = 0.0

# ── Startup Guards ────────────────────────────────────────────────────────────
boot_liquidation_done:    bool = False
boot_liquidation_pending: bool = False

STATE_KEY = f"strategy_state:{TRADE_SYMBOL}"


# ── Shutdown Handler ───────────────────────────────────────────────────────────
def _handle_shutdown(signum: int, frame: Any) -> None:
    log.info("Shutdown signal %s received. Exiting SOL strategy engine.", signum)
    sys.exit(0)


signal.signal(signal.SIGINT,  _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


# ── ATR (Wilder's Smoothing) ───────────────────────────────────────────────────
def _compute_atr(current_price: float) -> float | None:
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


# ── Sizing Helpers ────────────────────────────────────────────────────────────
def _sentiment_scalar() -> float:
    s = global_sentiment_score
    if s < -0.30: return SENTIMENT_SCALAR_SEVERE_FEAR
    if s < -0.05: return SENTIMENT_SCALAR_UNEASE
    if s <= 0.25: return SENTIMENT_SCALAR_NEUTRAL
    return SENTIMENT_SCALAR_HYPE


def _sharpe_scalar() -> float:
    """Scale by rolling Sharpe of last SHARPE_WINDOW closed trades. Range: [0.3, 1.5]."""
    if len(trade_returns) < 3:
        return 1.0
    try:
        mu  = statistics.mean(trade_returns)
        sig = statistics.stdev(trade_returns)
    except statistics.StatisticsError:
        return 1.0
    if sig < 1e-8:
        return 1.0 if mu >= 0 else 0.3
    return max(0.3, min(1.5, 0.9 + (mu / sig) * 0.6))


def _sized_quantity(price: float, max_alloc_usd: float) -> str:
    """
    Volatility-targeted size capped at max_alloc_usd / price.

    qty = RISK_PER_TRADE_USD × scalar / ATR  (dollars-at-risk per ATR move)
    Capped at allocation cap = max_alloc_usd × scalar / price.
    Falls back to cap-only when ATR is not yet seeded.
    """
    if price <= 0:
        return "0"
    scalar    = _sentiment_scalar() * _sharpe_scalar()
    alloc_qty = int(math.floor(max_alloc_usd * scalar / price))
    if atr_value > 0:
        risk_qty = int(math.floor(RISK_PER_TRADE_USD * scalar / atr_value))
        qty = min(risk_qty, alloc_qty)
    else:
        qty = alloc_qty
    return str(max(qty, 0))


# ── Bollinger Bands ───────────────────────────────────────────────────────────
def _bollinger_bands(sma: float) -> tuple[float, float]:
    """Compute (upper, lower) from the SMA20 window."""
    if len(sma_prices) < SMA_PERIOD:
        return sma, sma
    variance = sum((p - sma) ** 2 for p in sma_prices) / len(sma_prices)
    std = math.sqrt(variance)
    return sma + BB_STD_MULT * std, sma - BB_STD_MULT * std


# ── EMA Update ────────────────────────────────────────────────────────────────
def _update_emas(price: float) -> None:
    global ema_fast, ema_slow
    if ema_fast is None:
        ema_fast = price
        ema_slow = price
    else:
        ema_fast = alpha_fast * price + (1 - alpha_fast) * ema_fast
        ema_slow = alpha_slow * price + (1 - alpha_slow) * ema_slow


def _death_cross() -> bool:
    """Returns True when EMA7 has crossed below EMA99 (trend exhaustion)."""
    return ema_fast is not None and ema_slow is not None and ema_fast < ema_slow


# ── Regime Detection ──────────────────────────────────────────────────────────
def _detect_regime(price: float, sma20: float, sma50: float) -> str:
    """
    Classify current market regime using a 2-of-3 majority vote.

    Returns: TRENDING_UP, TRENDING_DOWN, RANGING, or UNCLEAR.
    """
    if price <= 0 or sma20 <= 0 or sma50 <= 0 or atr_value <= 0:
        return "UNCLEAR"

    atr_pct   = atr_value / price
    bb_u, bb_l = _bollinger_bands(sma20)
    bb_width  = (bb_u - bb_l) / sma20 if sma20 > 0 else 0
    price_dev = abs(price - sma50) / sma50

    t = 0  # trending votes
    r = 0  # ranging votes

    if   atr_pct  > ATR_PCT_TRENDING:    t += 1
    elif atr_pct  < ATR_PCT_RANGING:     r += 1
    if   bb_width > BB_WIDTH_TRENDING:   t += 1
    elif bb_width < BB_WIDTH_RANGING:    r += 1
    if   price_dev > PRICE_DEV_TRENDING: t += 1
    elif price_dev < PRICE_DEV_RANGING:  r += 1

    if t >= 2:
        return "TRENDING_UP" if price >= sma50 else "TRENDING_DOWN"
    if r >= 2:
        return "RANGING"
    return "UNCLEAR"


def _update_regime(detected: str) -> None:
    """
    Apply dwell filter: commit to a new regime only after REGIME_DWELL_TICKS
    consecutive ticks of the same new signal.
    """
    global current_regime, candidate_regime, candidate_ticks

    if detected == current_regime:
        # Already in this regime — reset candidate
        candidate_regime = detected
        candidate_ticks  = 0
        return

    if detected == candidate_regime:
        candidate_ticks += 1
        if candidate_ticks >= REGIME_DWELL_TICKS:
            log.info(
                "Regime committed: %s → %s (held for %d ticks)",
                current_regime, detected, candidate_ticks,
            )
            current_regime   = detected
            candidate_ticks  = 0
    else:
        # New candidate — start fresh dwell count
        candidate_regime = detected
        candidate_ticks  = 1


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
            "entry_regime":           entry_regime,
        })
    except aioredis.RedisError as exc:
        log.warning("Failed to persist state: %s", exc)


async def _load_state(redis_client: aioredis.Redis) -> bool:
    global in_position, open_position_quantity, average_cost, atr_at_entry
    global cooldown_until, tranche_count, last_buy_price, entry_regime

    try:
        state = await redis_client.hgetall(STATE_KEY)
    except aioredis.RedisError as exc:
        log.warning("Failed to load persisted state: %s", exc)
        return False

    if not state or state.get("in_position") is None:
        return False

    in_position            = state.get("in_position", "False") == "True"
    open_position_quantity = state.get("open_position_quantity", "0")
    average_cost           = float(state.get("average_cost",   "0.0"))
    atr_at_entry           = float(state.get("atr_at_entry",   "0.0"))
    cooldown_until         = float(state.get("cooldown_until", "0.0"))
    tranche_count          = int(float(state.get("tranche_count", "0")))
    last_buy_price         = float(state.get("last_buy_price", "0.0"))
    entry_regime           = state.get("entry_regime", "")

    # Migration: old state may have entry_price instead of average_cost
    if in_position and average_cost == 0.0:
        legacy = float(state.get("entry_price", "0.0"))
        if legacy > 0:
            average_cost = legacy
            log.warning("Migrated legacy entry_price=%.4f to average_cost.", average_cost)

    try:
        resumed_qty = float(open_position_quantity)
    except (TypeError, ValueError):
        resumed_qty = 0.0

    if in_position and (resumed_qty <= 0.0 or average_cost <= 0.0):
        log.warning(
            "State mismatch corrected: in_position=True but qty=%.8f avg_cost=%.8f. Resetting to flat.",
            resumed_qty,
            average_cost,
        )
        in_position = False
        open_position_quantity = "0.0000"
        average_cost = 0.0
        atr_at_entry = 0.0
        tranche_count = 0
        last_buy_price = 0.0
        entry_regime = ""

    if not in_position:
        tranche_count  = 0
        last_buy_price = 0.0
        average_cost   = 0.0

    log.info(
        "Resumed state: in_position=%s qty=%s avg_cost=%.4f "
        "tranches=%d last_buy=%.4f atr_entry=%.6f entry_regime=%s",
        in_position, open_position_quantity, average_cost,
        tranche_count, last_buy_price, atr_at_entry, entry_regime,
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
            "Signal → action=%s symbol=%s qty=%s reason=%s px=%.6f",
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
    global average_cost, tranche_count, last_buy_price, atr_at_entry, entry_regime
    global pending_action, pending_action_since, in_position, open_position_quantity
    global cooldown_until, pending_sell_quantity, pending_sell_price
    global boot_liquidation_pending, prev_price, breakout_streak

    if symbol != TRADE_SYMBOL or price <= 0:
        return

    # ATR + price window + EMA updates
    _compute_atr(price)
    _update_emas(price)
    sma_prices.append(price)
    sma50_prices.append(price)

    # Warm-up: SMA50 must be fully seeded before regime detection is valid
    if len(sma50_prices) < WARMUP_TICKS:
        log.info("Warm-up: %d/%d ticks for SMA50.", len(sma50_prices), WARMUP_TICKS)
        prev_price = price
        breakout_streak = 0
        return

    sma20 = sum(sma_prices) / len(sma_prices)
    sma50 = sum(sma50_prices) / SMA50_PERIOD

    # Detect regime and apply dwell filter
    detected = _detect_regime(price, sma20, sma50)
    _update_regime(detected)

    bb_upper, bb_lower = _bollinger_bands(sma20)
    now_ts = time.time()

    log.info(
        "px=%.4f | SMA20=%.4f | SMA50=%.4f | BB[%.4f/%.4f] | "
        "EMA7=%.4f EMA99=%.4f | regime=%s(cand=%s %dt) | avg=%.4f | "
        "tr=%d/%d | ATR=%.6f | senti=%.4f(%.2f×s %.2f×sh) | %s",
        price, sma20, sma50, bb_upper, bb_lower,
        ema_fast or 0.0, ema_slow or 0.0,
        current_regime, candidate_regime, candidate_ticks,
        average_cost, tranche_count, MAX_TRANCHES,
        atr_value,
        global_sentiment_score, _sentiment_scalar(), _sharpe_scalar(),
        "IN" if in_position else "OUT",
    )

    # Pending confirmation timeout
    if pending_action is not None:
        if time.time() - pending_action_since > CONFIRMATION_TIMEOUT_SEC:
            log.warning("Pending %s timed out — resetting.", pending_action)
            pending_action = None
        else:
            prev_price = price
            breakout_streak = 0
            return

    if boot_liquidation_pending:
        prev_price = price
        breakout_streak = 0
        return

    # ── EXITS (highest priority, regime-agnostic) ─────────────────────────────
    if in_position:
        breakout_streak = 0
        # 1. ATR stop-loss with minimum floor to prevent stop-hunting in flat markets
        if atr_at_entry > 0:
            atr_stop   = average_cost - ATR_STOPLOSS_K * atr_at_entry
            floor_stop = average_cost * (1.0 - MIN_STOP_PCT)
            stop_price = min(atr_stop, floor_stop)   # wider of the two
        else:
            stop_price = average_cost * FALLBACK_STOP
        if price <= stop_price:
            sell_qty              = open_position_quantity
            if float(sell_qty or 0.0) <= 0.0:
                log.warning("ATR stop skipped: in_position=True but sell quantity is %r", sell_qty)
                in_position = False
                open_position_quantity = "0.0000"
                average_cost = 0.0
                atr_at_entry = 0.0
                tranche_count = 0
                last_buy_price = 0.0
                entry_regime = ""
                await _save_state(redis_client)
                prev_price = price
                return
            pending_action        = "SELL"
            pending_action_since  = time.time()
            pending_sell_quantity = sell_qty
            pending_sell_price    = price
            cooldown_until        = now_ts + COOLDOWN_SECONDS
            await _publish_signal(
                redis_client, action="SELL", symbol=symbol, quantity=sell_qty,
                reason=f"ATR_StopLoss_{ATR_STOPLOSS_K}x_entry_regime={entry_regime}",
                timestamp=timestamp, expected_price=price,
            )
            log.warning(
                "ATR stop: px=%.4f stop=%.4f avg=%.4f entry_regime=%s tranches=%d",
                price, stop_price, average_cost, entry_regime, tranche_count,
            )
            prev_price = price
            return

        # 2. Exit — threshold depends on the regime under which we entered.
        # If entry_regime is empty (e.g. position synced from exchange history rather than
        # initiated by this strategy), fall back to current_regime so the correct exit rule applies.
        effective_entry_regime = entry_regime if entry_regime else current_regime
        if effective_entry_regime == "RANGING":
            take_profit = max(sma20, average_cost * (1.0 + BB_PROFIT_MARGIN))
            tp_triggered = price >= take_profit
            tp_reason    = f"BB_MeanReversion_exit_sma={sma20:.4f}_avg={average_cost:.4f}"
        elif effective_entry_regime == "TRENDING_UP":
            # Death cross: EMA7 crosses below EMA99 — trend exhaustion exit
            tp_triggered = _death_cross() and price > average_cost * (1.0 + TRADING_FEE_RATE * 2)
            tp_reason    = f"EMA_DeathCross_fast={ema_fast:.4f}_slow={ema_slow:.4f}_avg={average_cost:.4f}"
        else:
            take_profit  = average_cost * (1.0 + PROFIT_MARGIN)
            tp_triggered = price >= take_profit
            tp_reason    = f"DCA_TakeProfit_{int(PROFIT_MARGIN * 100)}pct_avg={average_cost:.4f}_tr={tranche_count}_regime={effective_entry_regime}"

        if tp_triggered:
            sell_qty              = open_position_quantity
            if float(sell_qty or 0.0) <= 0.0:
                log.warning("Exit skipped: in_position=True but sell quantity is %r", sell_qty)
                in_position = False
                open_position_quantity = "0.0000"
                average_cost = 0.0
                atr_at_entry = 0.0
                tranche_count = 0
                last_buy_price = 0.0
                entry_regime = ""
                await _save_state(redis_client)
                prev_price = price
                return
            pending_action        = "SELL"
            pending_action_since  = time.time()
            pending_sell_quantity = sell_qty
            pending_sell_price    = price
            await _publish_signal(
                redis_client, action="SELL", symbol=symbol, quantity=sell_qty,
                reason=tp_reason, timestamp=timestamp, expected_price=price,
            )
            prev_price = price
            return

        # 3. DCA additional tranches (only relevant for DCA entry modes)
        if (
            effective_entry_regime in ("TRENDING_DOWN", "UNCLEAR", "")
            and tranche_count < MAX_TRANCHES
            and last_buy_price > 0
            and price <= last_buy_price * (1.0 - TRANCHE_DIP_PCT)
            and now_ts > cooldown_until
            and global_sentiment_score > SENTIMENT_GATE
        ):
            unclear_adj = 0.5 if effective_entry_regime == "UNCLEAR" else 1.0
            alloc       = TARGET_ORDER_USD / MAX_TRANCHES
            raw_qty     = int(_sized_quantity(price, alloc) or "0")
            quantity    = str(int(raw_qty * unclear_adj))
            if int(quantity) > 0:
                pending_action       = "BUY"
                pending_action_since = time.time()
                last_buy_price       = price
                await _publish_signal(
                    redis_client, action="BUY", symbol=symbol, quantity=quantity,
                    reason=f"DCA_Tranche{tranche_count + 1}_dip={TRANCHE_DIP_PCT * 100:.1f}pct"
                           f"_entry_regime={entry_regime}",
                    timestamp=timestamp, expected_price=price,
                )
                prev_price = price
                return

        prev_price = price
        return

    # ── ENTRIES (not in position) ─────────────────────────────────────────────
    if now_ts <= cooldown_until or global_sentiment_score <= SENTIMENT_GATE:
        prev_price = price
        return

    # ── RANGING: Bollinger Band mean reversion ────────────────────────────────
    if current_regime == "RANGING":
        breakout_streak = 0
        if price <= bb_lower:
            quantity = _sized_quantity(price, TARGET_ORDER_USD / 2)
            if int(quantity) > 0:
                pending_action       = "BUY"
                pending_action_since = time.time()
                average_cost         = price
                last_buy_price       = price
                atr_at_entry         = atr_value
                entry_regime         = "RANGING"
                await _publish_signal(
                    redis_client, action="BUY", symbol=symbol, quantity=quantity,
                    reason=f"BB_MeanReversion_lower={bb_lower:.4f}_sma={sma20:.4f}",
                    timestamp=timestamp, expected_price=price,
                )
                prev_price = price
                return

    # ── TRENDING_UP: ATR Breakout ─────────────────────────────────────────────
    elif current_regime == "TRENDING_UP":
        if prev_price is not None and atr_value > 0:
            delta = price - prev_price
            if delta > ATR_BREAKOUT_K * atr_value:
                breakout_streak += 1
            else:
                breakout_streak = 0

            if breakout_streak >= max(BREAKOUT_CONFIRM_TICKS, 1):
                quantity = _sized_quantity(price, TARGET_ORDER_USD / 2)
                if int(quantity) > 0:
                    pending_action       = "BUY"
                    pending_action_since = time.time()
                    average_cost         = price
                    last_buy_price       = price
                    atr_at_entry         = atr_value
                    entry_regime         = "TRENDING_UP"
                    await _publish_signal(
                        redis_client, action="BUY", symbol=symbol, quantity=quantity,
                        reason=f"ATR_Breakout_{ATR_BREAKOUT_K}x_delta={delta:.4f}_atr={atr_value:.6f}",
                        timestamp=timestamp, expected_price=price,
                    )
                    breakout_streak = 0
                    prev_price = price
                    return

    # ── TRENDING_DOWN / UNCLEAR: DCA first tranche ────────────────────────────
    else:
        breakout_streak = 0
        if price <= sma20 * (1.0 - INITIAL_DIP_PCT):
            unclear_adj = 0.5 if current_regime == "UNCLEAR" else 1.0
            alloc       = TARGET_ORDER_USD / MAX_TRANCHES
            raw_qty     = int(_sized_quantity(price, alloc) or "0")
            quantity    = str(int(raw_qty * unclear_adj))
            if int(quantity) > 0:
                pending_action       = "BUY"
                pending_action_since = time.time()
                average_cost         = price
                last_buy_price       = price
                atr_at_entry         = atr_value
                entry_regime         = current_regime
                await _publish_signal(
                    redis_client, action="BUY", symbol=symbol, quantity=quantity,
                    reason=f"DCA_Tranche1_dip={INITIAL_DIP_PCT * 100:.1f}pct_regime={current_regime}",
                    timestamp=timestamp, expected_price=price,
                )
                prev_price = price
                return

    prev_price = price


# ── Execution Confirmation Handler ─────────────────────────────────────────────
async def handle_execution_confirmation(raw_payload: str, redis_client: aioredis.Redis) -> None:
    global in_position, open_position_quantity, pending_action, average_cost
    global atr_at_entry, pending_sell_quantity, pending_sell_price, boot_liquidation_pending
    global tranche_count, last_buy_price, entry_regime, ema_fast, ema_slow

    try:
        data = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        log.warning("Malformed execution confirmation: %r | %s", raw_payload, exc)
        return

    confirmed_symbol = str(data.get("symbol", ""))
    if confirmed_symbol and confirmed_symbol != TRADE_SYMBOL:
        return

    side   = str(data.get("action", data.get("side", ""))).upper()
    result = str(data.get("result", "")).upper()
    reason = str(data.get("reason", "")).upper()

    if side not in {"BUY", "SELL"}:
        return

    # ── Startup Liquidation ────────────────────────────────────────────────────
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
        entry_regime             = ""
        ema_fast                 = None
        ema_slow                 = None
        await _save_state(redis_client)
        log.info(
            "Startup liquidation %s for %s.",
            "completed" if result == "SUCCESS" else "skipped",
            TRADE_SYMBOL,
        )
        return

    if pending_action is None:
        return

    if side != pending_action:
        log.info("Out-of-order fill ignored: side=%s pending=%s", side, pending_action)
        return

    if result != "SUCCESS":
        log.warning("%s fill failed (result=%s). Reverting.", side, result)
        if side == "BUY":
            if tranche_count == 0:
                in_position            = False
                open_position_quantity = "0.0000"
                average_cost           = 0.0
                atr_at_entry           = 0.0
                last_buy_price         = 0.0
                entry_regime           = ""
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
            buy_price     = last_buy_price if last_buy_price > 0 else average_cost
            average_cost  = (average_cost * prev_qty + buy_price * net_qty) / new_total_qty
            open_position_quantity = str(new_total_qty)
            in_position    = True
            tranche_count += 1
            last_buy_price = buy_price
        pending_sell_quantity = "0"
        pending_action        = None
        await _save_state(redis_client)
        log.info(
            "BUY confirmed (tranche %d/%d): executed=%.4f net=%d total=%s avg=%.4f entry_regime=%s",
            tranche_count, MAX_TRANCHES,
            executed_qty, net_qty, open_position_quantity, average_cost, entry_regime,
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
            # Record trade return for rolling Sharpe
            if average_cost > 0 and pending_sell_price > 0:
                trade_return = (pending_sell_price - average_cost) / average_cost
                trade_returns.append(trade_return)
                log.info(
                    "Trade return recorded: %.4f%% | Sharpe scalar now %.2f×",
                    trade_return * 100, _sharpe_scalar(),
                )
            average_cost       = 0.0
            atr_at_entry       = 0.0
            tranche_count      = 0
            last_buy_price     = 0.0
            pending_sell_price = 0.0
            entry_regime       = ""

        pending_action = None
        await _save_state(redis_client)
        log.info(
            "SELL confirmed: executed=%.4f remaining=%s reset=%s",
            executed_qty, open_position_quantity, remaining <= 0.0,
        )


# ── Sentiment Update Handler ───────────────────────────────────────────────────
def handle_sentiment_update(raw_payload: str) -> None:
    global global_sentiment_score

    try:
        data  = json.loads(raw_payload)
        score = float(data.get("compound_score", 0.0))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        log.warning("Malformed sentiment payload: %r | %s", raw_payload, exc)
        return

    prev                   = global_sentiment_score
    global_sentiment_score = score
    log.info(
        "Sentiment: %.4f → %.4f | gate=%.2f | senti_scalar=%.2f× | sharpe_scalar=%.2f×",
        prev, global_sentiment_score, SENTIMENT_GATE,
        _sentiment_scalar(), _sharpe_scalar(),
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
            elif channel == EXECUTION_CONFIRMATIONS_CHANNEL:
                await handle_execution_confirmation(payload, redis_client)
            elif channel == MARKET_DATA_CHANNEL:
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
    global in_position, open_position_quantity, pending_action, average_cost
    global atr_at_entry, cooldown_until, pending_sell_quantity, boot_liquidation_pending
    global tranche_count, last_buy_price, entry_regime

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
    open_position_quantity   = "0.0000"
    pending_action           = "SELL"
    average_cost             = 0.0
    atr_at_entry             = 0.0
    cooldown_until           = 0.0
    pending_sell_quantity    = "0.0000"
    tranche_count            = 0
    last_buy_price           = 0.0
    entry_regime             = ""
    log.info("Startup liquidation published for %s.", TRADE_SYMBOL)


# ── Entry Point ────────────────────────────────────────────────────────────────
async def run() -> None:
    global boot_liquidation_done

    log.info(
        "SOL Adaptive Multi-Regime Strategy starting | symbol=%s | "
        "regimes=[RANGING→BB, TRENDING_UP→EMA(%d/%d)DeathCross, TRENDING_DOWN→DCA, UNCLEAR→DCA×0.5] | "
        "dwell_ticks=%d | risk_per_trade=$%.0f | sharpe_window=%d | "
        "bb_profit=%.1f%% | dca_profit=%.1f%% | min_stop=%.1f%% | "
        "target_notional=$%.0f (%.0f%% of $%.0f)",
        TRADE_SYMBOL, EMA_FAST_PERIOD, EMA_SLOW_PERIOD,
        REGIME_DWELL_TICKS, RISK_PER_TRADE_USD, SHARPE_WINDOW,
        BB_PROFIT_MARGIN * 100, PROFIT_MARGIN * 100, MIN_STOP_PCT * 100,
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
