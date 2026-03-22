"""Process B - Strategy Engine (Sentiment-Adjusted Mean Reversion).

Subscribes to:
    - market_data: latest mid_price ticks
    - sentiment_signals: latest NLP compound_score

Publishes trade signals to trade_signals using dynamic lower-band thresholds and
USD-notional position sizing.
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


# Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

MARKET_DATA_CHANNEL = "market_data"
SENTIMENT_CHANNEL = "sentiment_signals"
TRADE_SIGNALS_CHANNEL = "trade_signals"
ORDER_RESULTS_CHANNEL = "order_results"
EXECUTION_CONFIRMATIONS_CHANNEL = "execution_confirmations"
POSITION_CHANNEL = "position_updates"
POSITION_KEY = "position:current"

DEFAULT_SYMBOL = os.getenv("TRADE_SYMBOL", "BTC/USD")
DEFAULT_ORDER_TYPE = "MARKET"

WINDOW_PERIOD = 20

# Portfolio and sizing rules
PORTFOLIO_USD = 1_000_000.0
RISK_PER_TRADE = 0.02
TARGET_ORDER_USD = PORTFOLIO_USD * RISK_PER_TRADE

POLL_TIMEOUT_SEC = float(os.getenv("STRATEGY_POLL_TIMEOUT_SEC", "1.0"))
RETRY_DELAY_SEC = int(os.getenv("STRATEGY_RETRY_DELAY_SEC", "5"))
TRADING_FEE_RATE = float(os.getenv("TRADING_FEE_RATE", "0.001"))
MIN_SELL_MULTIPLIER = float(os.getenv("MIN_SELL_MULTIPLIER", "1.0025"))
STOP_LOSS_MULTIPLIER = float(os.getenv("STOP_LOSS_MULTIPLIER", "0.97"))
COOLDOWN_SECONDS = int(os.getenv("STOP_LOSS_COOLDOWN_SEC", "3600"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Strategy] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


SEVERE_FEAR_PROFILE = {"label": "SEVERE_FEAR", "multiplier": 2.95, "reason": "2.95_SD_Panic_Dip"}
UNEASE_PROFILE = {"label": "UNEASE", "multiplier": 2.45, "reason": "2.45_SD_Unease_Dip"}
NEUTRAL_PROFILE = {"label": "NEUTRAL", "multiplier": 1.95, "reason": "1.95_SD_Neutral_Dip"}
HYPE_PROFILE = {"label": "HYPE", "multiplier": 1.45, "reason": "1.45_SD_Hype_Dip"}


# Stateful strategy variables
prices: deque[float] = deque(maxlen=WINDOW_PERIOD)
latest_sentiment_score = 0.0
in_position = False
open_position_quantity = "0.0000"
pending_action: str | None = None
entry_price = 0.0
stop_loss_price = 0.0
cooldown_until = 0.0
boot_liquidation_done = False
pending_sell_quantity = "0.0000"
boot_liquidation_pending = False


def _handle_shutdown(signum, frame):  # noqa: ANN001
    log.info("Shutdown signal received. Exiting strategy engine.")
    sys.exit(0)


signal.signal(signal.SIGINT, _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


def _mean(values: list[float]) -> float:
    return sum(values) / len(values)


def _std(values: list[float], mean_val: float) -> float:
    variance = sum((v - mean_val) ** 2 for v in values) / len(values)
    return math.sqrt(variance)


def resolve_sd_profile(compound_score: float) -> dict[str, float | str]:
    if compound_score < -0.30:
        return SEVERE_FEAR_PROFILE
    if compound_score < -0.05:
        return UNEASE_PROFILE
    if compound_score <= 0.25:
        return NEUTRAL_PROFILE
    return HYPE_PROFILE


def compute_bollinger_inputs(latest_prices: list[float]) -> tuple[float, float]:
    sma = _mean(latest_prices)
    std = _std(latest_prices, sma)
    return sma, std


def target_quantity_from_price(price: float) -> str:
    if price <= 0:
        return "0.0000"
    qty = math.floor((TARGET_ORDER_USD / price) * 10000) / 10000
    return f"{qty:.4f}"


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
        "action": action,
        "symbol": symbol,
        "type": DEFAULT_ORDER_TYPE,
        "quantity": quantity,
        "reason": reason,
        "timestamp": timestamp,
        "expected_price": expected_price,
    }
    await redis_client.publish(TRADE_SIGNALS_CHANNEL, json.dumps(payload))
    log.info("Signal published → %s", payload)


async def evaluate_market_tick(price: float, symbol: str, timestamp: int, redis_client: aioredis.Redis) -> None:
    global pending_action, in_position, open_position_quantity, entry_price, stop_loss_price, cooldown_until, pending_sell_quantity, boot_liquidation_pending
    prices.append(price)
    if len(prices) < WINDOW_PERIOD:
        log.info(
            "Waiting for window fill: %d/%d prices buffered.",
            len(prices),
            WINDOW_PERIOD,
        )
        return

    sma, std = compute_bollinger_inputs(list(prices))
    profile = resolve_sd_profile(latest_sentiment_score)
    dynamic_lower = sma - (float(profile["multiplier"]) * std)

    quantity = target_quantity_from_price(price)
    state = "IN_POSITION" if in_position else "OUT_OF_POSITION"
    log.info(
        "px=%.6f | SMA=%.6f | SD=%.6f | lower=%.6f | sentiment=%.4f (%s %.2fSD) | state=%s",
        price,
        sma,
        std,
        dynamic_lower,
        latest_sentiment_score,
        profile["label"],
        profile["multiplier"],
        state,
    )

    if pending_action is not None:
        log.info("Signal suppressed while awaiting %s fill confirmation.", pending_action)
        return

    if boot_liquidation_pending:
        log.info("Waiting for startup liquidation result before processing new entries.")
        return

    now_ts = time.time()

    # Cancel cooldown early if market has already recovered above mean.
    if cooldown_until > now_ts and price > sma:
        cooldown_until = 0.0
        log.info("Market recovered above SMA. Cooldown canceled.")

    if not in_position and now_ts > cooldown_until and price < dynamic_lower:
        pending_action = "BUY"
        entry_price = price
        stop_loss_price = entry_price * STOP_LOSS_MULTIPLIER
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

    if in_position and price <= stop_loss_price:
        pending_action = "SELL"
        sell_qty = open_position_quantity
        pending_sell_quantity = sell_qty
        await publish_signal(
            redis_client,
            action="SELL",
            symbol=symbol,
            quantity=sell_qty,
            reason="Stop_Loss_3pct",
            timestamp=timestamp,
            expected_price=price,
        )
        cooldown_until = now_ts + COOLDOWN_SECONDS
        log.warning("Stop-Loss triggered at -3%%.")
        log.info("Stop-Loss hit. Entering %ds cooldown.", COOLDOWN_SECONDS)
        return

    if in_position and price >= sma:
        minimum_sell_price = entry_price * MIN_SELL_MULTIPLIER
        if price >= minimum_sell_price:
            pending_action = "SELL"
            sell_qty = open_position_quantity
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

        log.info("Holding position: SMA reached but 0.25%% fee spread not met")


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
    # Position state is updated from execution_confirmations to avoid Pub/Sub blind spots.
    return


def handle_execution_confirmation(raw_payload: str) -> None:
    global in_position, open_position_quantity, pending_action, entry_price, stop_loss_price, pending_sell_quantity, boot_liquidation_pending
    try:
        data = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        log.warning("Malformed execution confirmation skipped: %r | error: %s", raw_payload, exc)
        return

    side = str(data.get("side", "")).upper()
    if side == "":
        side = str(data.get("action", "")).upper()
    result = str(data.get("result", "")).upper()
    reason = str(data.get("reason", "")).upper()

    if side not in {"BUY", "SELL"}:
        return

    if boot_liquidation_pending and side == "SELL":
        startup_reason = reason in {
            "SYSTEM_REBOOT_LIQUIDATION",
            "STARTUP_BALANCE_SYNC",
            "STARTUP_BALANCE_SYNC_CLEAR",
        }
        if not startup_reason:
            return

        if result != "SUCCESS":
            log.warning("Startup liquidation confirmation not successful: result=%s", result)
            return

        boot_liquidation_pending = False
        pending_action = None
        pending_sell_quantity = "0.0000"
        in_position = False
        open_position_quantity = "0.0000"
        entry_price = 0.0
        stop_loss_price = 0.0
        log.info("Startup liquidation cycle completed with result=%s", result)
        return

    if pending_action is None:
        # Ignore fills that were not initiated by this strategy loop.
        return

    if side != pending_action:
        log.info("Ignoring out-of-order fill event side=%s while pending=%s", side, pending_action)
        return

    if result != "SUCCESS":
        if pending_action == side:
            log.info("%s failed, clearing pending action.", side)
            if side == "BUY":
                in_position = False
                open_position_quantity = "0.0000"
                entry_price = 0.0
                stop_loss_price = 0.0
            if side == "SELL":
                in_position = True
                if open_position_quantity in {"", "0", "0.0", "0.0000"}:
                    open_position_quantity = pending_sell_quantity
            pending_sell_quantity = "0.0000"
            pending_action = None
        return

    executed_qty = _extract_executed_quantity(data)

    if side == "BUY":
        net_qty = max(executed_qty * (1.0 - TRADING_FEE_RATE), 0.0)
        in_position = net_qty > 0
        open_position_quantity = f"{net_qty:.4f}"
        pending_sell_quantity = "0.0000"
        pending_action = None
        log.info(
            "BUY fill confirmed: executed=%.8f, fee_rate=%.4f, net_position=%s",
            executed_qty,
            TRADING_FEE_RATE,
            open_position_quantity,
        )
        return

    if side == "SELL":
        try:
            current_qty = float(open_position_quantity)
        except ValueError:
            current_qty = 0.0

        remaining = max(current_qty - executed_qty, 0.0)
        in_position = remaining > 0
        open_position_quantity = f"{remaining:.4f}"
        pending_sell_quantity = "0.0000"
        if remaining <= 0.0:
            entry_price = 0.0
            stop_loss_price = 0.0
        pending_action = None
        log.info(
            "SELL fill confirmed: executed=%.8f, remaining_position=%s",
            executed_qty,
            open_position_quantity,
        )


def handle_sentiment_update(raw_payload: str) -> None:
    global latest_sentiment_score
    try:
        data = json.loads(raw_payload)
        score = float(data.get("compound_score", 0.0))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        log.warning("Malformed sentiment payload skipped: %r | error: %s", raw_payload, exc)
        return

    latest_sentiment_score = score
    profile = resolve_sd_profile(latest_sentiment_score)
    log.info(
        "Sentiment updated: score=%.4f -> profile=%s (%.2f SD)",
        latest_sentiment_score,
        profile["label"],
        float(profile["multiplier"]),
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


async def consume_stream(redis_client: aioredis.Redis) -> None:
    pubsub = redis_client.pubsub(ignore_subscribe_messages=True)
    await pubsub.subscribe(
        MARKET_DATA_CHANNEL,
        SENTIMENT_CHANNEL,
        ORDER_RESULTS_CHANNEL,
        EXECUTION_CONFIRMATIONS_CHANNEL,
    )

    log.info(
        "Strategy engine listening on channels: %s, %s, %s, %s",
        MARKET_DATA_CHANNEL,
        SENTIMENT_CHANNEL,
        ORDER_RESULTS_CHANNEL,
        EXECUTION_CONFIRMATIONS_CHANNEL,
    )

    try:
        while True:
            message = await pubsub.get_message(timeout=POLL_TIMEOUT_SEC)
            if message is None:
                continue

            if message.get("type") != "message":
                continue

            channel = str(message.get("channel", ""))
            raw_data = message.get("data", "")
            payload = raw_data.decode("utf-8", errors="replace") if isinstance(raw_data, bytes) else str(raw_data)

            if channel == SENTIMENT_CHANNEL:
                handle_sentiment_update(payload)
                continue

            if channel == ORDER_RESULTS_CHANNEL:
                handle_order_result_update(payload)
                continue

            if channel == EXECUTION_CONFIRMATIONS_CHANNEL:
                handle_execution_confirmation(payload)
                await _publish_position_state(redis_client)
                continue

            if channel != MARKET_DATA_CHANNEL:
                continue

            try:
                tick = json.loads(payload)
                price = float(tick["mid_price"])
                timestamp = int(tick.get("timestamp", time.time()))
                symbol = str(tick.get("symbol", DEFAULT_SYMBOL))
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


async def initialize_clean_slate(redis_client: aioredis.Redis) -> None:
    global in_position, open_position_quantity, pending_action, entry_price, stop_loss_price, cooldown_until, pending_sell_quantity, boot_liquidation_pending
    payload = {
        "action": "SELL",
        "symbol": DEFAULT_SYMBOL,
        "type": DEFAULT_ORDER_TYPE,
        "quantity": "ALL",
        "reason": "SYSTEM_REBOOT_LIQUIDATION",
    }
    await redis_client.publish(TRADE_SIGNALS_CHANNEL, json.dumps(payload))
    boot_liquidation_pending = True
    in_position = False
    open_position_quantity = "0.0000"
    pending_action = "SELL"
    entry_price = 0.0
    stop_loss_price = 0.0
    cooldown_until = 0.0
    pending_sell_quantity = "0.0000"
    log.info("Startup clean-slate liquidation signal published.")


async def run() -> None:
    global boot_liquidation_done
    log.info(
        "Strategy engine starting | window=%d | target_notional=$%.2f | default_sentiment=%.2f",
        WINDOW_PERIOD,
        TARGET_ORDER_USD,
        latest_sentiment_score,
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
            # TODO: re-enable once dashboard testing is complete
            # if not boot_liquidation_done:
            #     await initialize_clean_slate(redis_client)
            #     boot_liquidation_done = True
            await consume_stream(redis_client)
        except aioredis.RedisError as exc:
            log.error("Redis error: %s. Retrying in %ds", exc, RETRY_DELAY_SEC)
            await asyncio.sleep(RETRY_DELAY_SEC)
        finally:
            if redis_client is not None:
                await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(run())
