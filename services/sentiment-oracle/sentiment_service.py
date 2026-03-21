"""Process F - Sentiment Oracle.

Fetches crypto headlines from RSS feeds, scores sentiment with VADER,
and publishes market mood to Redis `sentiment_signals`.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Iterable

import feedparser
import nltk
import redis
from dotenv import load_dotenv
from nltk.sentiment.vader import SentimentIntensityAnalyzer

load_dotenv()


REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
SENTIMENT_CHANNEL = "sentiment_signals"

POLL_INTERVAL_SEC = int(os.getenv("SENTIMENT_POLL_INTERVAL_SEC", 300))
MAX_HEADLINES = int(os.getenv("SENTIMENT_MAX_HEADLINES", 20))
REQUEST_TIMEOUT_SEC = int(os.getenv("SENTIMENT_REQUEST_TIMEOUT_SEC", 10))
RETRY_DELAY_SEC = int(os.getenv("SENTIMENT_RETRY_DELAY_SEC", 15))

RSS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
    "https://decrypt.co/feed",
]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Sentiment] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


def ensure_vader_lexicon() -> None:
    """Download VADER lexicon once if missing on host."""
    try:
        nltk.data.find("sentiment/vader_lexicon.zip")
    except LookupError:
        log.info("Downloading NLTK vader_lexicon...")
        nltk.download("vader_lexicon", quiet=True)


def create_analyzer() -> SentimentIntensityAnalyzer:
    ensure_vader_lexicon()
    return SentimentIntensityAnalyzer()


def classify_status(compound_score: float) -> str:
    if compound_score <= -0.50:
        return "SEVERE_FEAR"
    if compound_score <= -0.15:
        return "FEAR"
    if compound_score < 0.15:
        return "NEUTRAL"
    if compound_score < 0.50:
        return "GREED"
    return "EUPHORIA"


def fetch_recent_headlines(
    feed_urls: Iterable[str],
    max_headlines: int = MAX_HEADLINES,
    request_timeout_sec: int = REQUEST_TIMEOUT_SEC,
) -> list[str]:
    """Fetch up to `max_headlines` headline titles across feeds."""
    collected: list[str] = []

    for url in feed_urls:
        if len(collected) >= max_headlines:
            break

        try:
            parsed = feedparser.parse(url, request_headers={"User-Agent": "tradingsystem-sentiment/1.0"})

            # feedparser signals malformed/failed feeds via bozo flag.
            if getattr(parsed, "bozo", False):
                log.warning("RSS parse warning for %s: %s", url, getattr(parsed, "bozo_exception", "unknown"))

            entries = getattr(parsed, "entries", [])
            for entry in entries:
                title = str(getattr(entry, "title", "") or "").strip()
                if title:
                    collected.append(title)
                if len(collected) >= max_headlines:
                    break

        except Exception as exc:
            log.error("Failed fetching RSS %s: %s", url, exc)

        # Respect constrained instance resources and avoid hammering feeds.
        if request_timeout_sec > 0:
            time.sleep(0)

    return collected[:max_headlines]


def average_compound_score(headlines: list[str], analyzer: SentimentIntensityAnalyzer) -> float:
    if not headlines:
        return 0.0

    scores = [analyzer.polarity_scores(text)["compound"] for text in headlines]
    return float(sum(scores) / len(scores))


def build_payload(compound_score: float, latest_headline: str) -> dict[str, object]:
    return {
        "timestamp": int(time.time()),
        "compound_score": float(compound_score),
        "status": classify_status(compound_score),
        "latest_headline": latest_headline,
    }


def publish_sentiment(redis_client: redis.Redis, payload: dict[str, object]) -> None:
    redis_client.publish(SENTIMENT_CHANNEL, json.dumps(payload))


def run_once(
    redis_client: redis.Redis,
    analyzer: SentimentIntensityAnalyzer,
    feed_urls: Iterable[str] = RSS_FEEDS,
    max_headlines: int = MAX_HEADLINES,
) -> dict[str, object]:
    headlines = fetch_recent_headlines(feed_urls=feed_urls, max_headlines=max_headlines)
    compound_score = average_compound_score(headlines, analyzer)
    latest_headline = headlines[0] if headlines else ""

    payload = build_payload(compound_score, latest_headline)
    publish_sentiment(redis_client, payload)

    log.info(
        "Published sentiment: score=%.4f status=%s latest=%r",
        payload["compound_score"],
        payload["status"],
        payload["latest_headline"],
    )
    return payload


def run() -> None:
    analyzer = create_analyzer()

    log.info("Starting Sentiment Oracle. Redis=%s:%s channel=%s", REDIS_HOST, REDIS_PORT, SENTIMENT_CHANNEL)

    while True:
        try:
            redis_client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True,
                socket_connect_timeout=5,
            )
            redis_client.ping()
            run_once(redis_client=redis_client, analyzer=analyzer)

        except redis.exceptions.RedisError as exc:
            log.error("Redis error: %s", exc)
        except Exception as exc:
            log.error("Sentiment loop error: %s", exc)

        time.sleep(POLL_INTERVAL_SEC if POLL_INTERVAL_SEC > 0 else RETRY_DELAY_SEC)


if __name__ == "__main__":
    run()
