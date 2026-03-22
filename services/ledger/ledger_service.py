"""
Process D - Ledger Service (CSV Audit Log)
===========================================
Subscribes to Redis `order_results` and appends one durable CSV row per message.

CSV headers (exact):
  Timestamp
  Action
  Pair
  Price
  Quantity
    Requested_Qty
    Effective_Qty
  Strategy_Signal
  API_Status
"""

from __future__ import annotations

import csv
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from typing import Any

import redis
from dotenv import load_dotenv

load_dotenv()


REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
ORDER_RESULTS_CHANNEL = "order_results"
RETRY_DELAY_SEC = 5

LOG_DIR = os.getenv("LOG_DIR", "logs")
CSV_FILE = os.getenv("LOG_FILE", os.path.join(LOG_DIR, "execution_log.csv"))

CSV_HEADERS = [
    "Timestamp",
    "Action",
    "Pair",
    "Price",
    "Quantity",
    "Requested_Qty",
    "Effective_Qty",
    "Strategy_Signal",
    "API_Status",
]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Ledger] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


def initialize_log_file(log_dir: str = LOG_DIR, csv_file: str = CSV_FILE) -> None:
    """Create log directory/file and write CSV header if file is missing/empty."""
    os.makedirs(log_dir, exist_ok=True)

    file_exists = os.path.exists(csv_file)
    needs_header = (not file_exists) or os.path.getsize(csv_file) == 0

    if needs_header:
        with open(csv_file, "a", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_HEADERS)
            writer.writeheader()
            fh.flush()
            os.fsync(fh.fileno())


def _format_timestamp(raw_ts: Any) -> str:
    if raw_ts is None:
        ts_seconds = time.time()
    else:
        ts_num = float(raw_ts)
        # Accept either seconds or milliseconds.
        ts_seconds = ts_num / 1000.0 if ts_num > 1_000_000_000_000 else ts_num

    return datetime.fromtimestamp(ts_seconds, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _extract_price(order_result: dict[str, Any]) -> float:
    order_details = order_result.get("order_details")
    if isinstance(order_details, dict):
        for key in ("FilledAverPrice", "Price"):
            if key in order_details and order_details[key] not in (None, ""):
                return float(order_details[key])

    return float(order_result.get("price", 0.0))


def _extract_quantity(order_result: dict[str, Any]) -> float:
    order_details = order_result.get("order_details")
    if isinstance(order_details, dict):
        for key in ("FilledQuantity", "Quantity"):
            if key in order_details and order_details[key] not in (None, ""):
                return float(order_details[key])

    return float(order_result.get("qty", 0.0))


def build_csv_row(order_result: dict[str, Any]) -> dict[str, str]:
    action = str(order_result.get("side") or order_result.get("action") or "UNKNOWN").upper()
    pair = str(order_result.get("symbol") or order_result.get("pair") or "UNKNOWN")
    strategy_signal = str(
        order_result.get("strategy_signal")
        or order_result.get("reason")
        or ""
    )

    status_code = int(order_result.get("status_code", 0))
    result = str(order_result.get("result", "")).upper()
    raw_error = str(order_result.get("raw_error") or "").strip()

    if result == "SUCCESS":
        api_status = f"{status_code} SUCCESS"
    elif raw_error:
        api_status = f"{status_code} {raw_error}"
    else:
        api_status = f"{status_code} {result or 'FAILED'}"

    requested_qty = float(order_result.get("requested_qty", order_result.get("qty", 0.0)))
    effective_qty = float(order_result.get("effective_qty", order_result.get("qty", 0.0)))

    return {
        "Timestamp": _format_timestamp(order_result.get("timestamp")),
        "Action": action,
        "Pair": pair,
        "Price": f"{_extract_price(order_result):.8f}",
        "Quantity": f"{_extract_quantity(order_result):.8f}",
        "Requested_Qty": f"{requested_qty:.8f}",
        "Effective_Qty": f"{effective_qty:.8f}",
        "Strategy_Signal": strategy_signal,
        "API_Status": api_status,
    }


def append_csv_row(row: dict[str, str], csv_file: str = CSV_FILE) -> None:
    """Append one row durably: flush + fsync to reduce abrupt-kill data loss."""
    with open(csv_file, "a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_HEADERS)
        writer.writerow(row)
        fh.flush()
        os.fsync(fh.fileno())


def process_order_result_message(raw_data: str, csv_file: str = CSV_FILE) -> bool:
    try:
        payload = json.loads(raw_data)
    except json.JSONDecodeError:
        log.warning("Malformed order_results payload skipped: %r", raw_data)
        return False

    try:
        row = build_csv_row(payload)
        append_csv_row(row, csv_file=csv_file)
        log.info("Logged order result: %s %s %s", row["Action"], row["Pair"], row["API_Status"])
        return True
    except Exception as exc:
        log.error("Failed to persist order result: %s", exc)
        return False


def _handle_os_signal(signum, frame):  # noqa: ANN001
    log.info("OS signal %s received. Shutting down ledger service.", signum)
    raise SystemExit(0)


signal.signal(signal.SIGINT, _handle_os_signal)
signal.signal(signal.SIGTERM, _handle_os_signal)


def run() -> None:
    initialize_log_file(LOG_DIR, CSV_FILE)
    log.info("Ledger service started. Logging channel '%s' to %s", ORDER_RESULTS_CHANNEL, CSV_FILE)

    while True:
        try:
            r = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True,
                socket_connect_timeout=5,
            )
            r.ping()
            log.info("Connected to Redis at %s:%s", REDIS_HOST, REDIS_PORT)

            pubsub = r.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(ORDER_RESULTS_CHANNEL)
            log.info("Listening on channel: %s", ORDER_RESULTS_CHANNEL)

            for message in pubsub.listen():
                if message.get("type") != "message":
                    continue
                process_order_result_message(str(message.get("data", "")), csv_file=CSV_FILE)

        except redis.exceptions.ConnectionError as exc:
            log.error("Redis connection lost: %s. Retrying in %ds", exc, RETRY_DELAY_SEC)
            time.sleep(RETRY_DELAY_SEC)
        except redis.exceptions.RedisError as exc:
            log.error("Redis error: %s. Retrying in %ds", exc, RETRY_DELAY_SEC)
            time.sleep(RETRY_DELAY_SEC)


if __name__ == "__main__":
    run()