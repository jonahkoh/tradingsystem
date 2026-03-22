"""Process F - Sentiment Oracle.

Fetches crypto headlines from RSS feeds, scores sentiment with VADER,
and publishes to two Redis channels:

  sentiment_signals     — global market-wide compound score (all coins)
  sentiment_signals_xrp — XRP/Ripple-specific compound score (filtered headlines)

Coin-specific filtering lets the XRP ATR breakout strategy react to legal/
regulatory news independently of broader market mood.
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


# ── Config ─────────────────────────────────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

SENTIMENT_CHANNEL     = "sentiment_signals"
XRP_SENTIMENT_CHANNEL = "sentiment_signals_xrp"

# Reduced from 300 → 60 to decrease sentiment lag for event-driven assets (XRP).
POLL_INTERVAL_SEC      = int(os.getenv("SENTIMENT_POLL_INTERVAL_SEC", 60))
MAX_HEADLINES          = int(os.getenv("SENTIMENT_MAX_HEADLINES", 20))
REQUEST_TIMEOUT_SEC    = int(os.getenv("SENTIMENT_REQUEST_TIMEOUT_SEC", 10))
RETRY_DELAY_SEC        = int(os.getenv("SENTIMENT_RETRY_DELAY_SEC", 15))

# XRP-specific keyword filter — headlines must contain at least one term.
_XRP_KEYWORDS_RAW = os.getenv(
    "XRP_SENTIMENT_KEYWORDS",
    "xrp,ripple,sec,lawsuit,court,ruling,settlement,judge,ripple labs",
)
XRP_KEYWORDS: list[str] = [kw.strip().lower() for kw in _XRP_KEYWORDS_RAW.split(",") if kw.strip()]

RSS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
    "https://decrypt.co/feed",
]

# Optional XRP-specific RSS feeds (comma-separated env var).
# These are polled in addition to the main feeds when filtering XRP headlines.
_XRP_RSS_RAW = os.getenv("XRP_RSS_FEEDS", "")
XRP_EXTRA_RSS_FEEDS: list[str] = [u.strip() for u in _XRP_RSS_RAW.split(",") if u.strip()]


# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Sentiment] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


# ── VADER Setup ────────────────────────────────────────────────────────────────
def ensure_vader_lexicon() -> None:
    """Download VADER lexicon once if missing on host."""
    try:
        nltk.data.find("sentiment/vader_lexicon.zip")
    except LookupError:
        log.info("Downloading NLTK vader_lexicon...")
        nltk.download("vader_lexicon", quiet=True)


def create_analyzer() -> SentimentIntensityAnalyzer:
    ensure_vader_lexicon()
    analyzer = SentimentIntensityAnalyzer()

    # General crypto lexicon — applied to all coins.
    crypto_lexicon: dict[str, float] = {
        # Extreme bullish
        "bullish":      3.0,
        "moon":         3.5,
        "mooning":      3.5,
        "ath":          3.0,
        "all-time high": 3.0,
        # Positive momentum
        "pump":         2.0,
        "surge":        2.0,
        "rally":        2.0,
        "breakout":     2.0,
        "adoption":     2.5,
        "upgrade":      2.0,
        "partnership":  2.5,
        # Negative momentum
        "dump":        -2.5,
        "bearish":     -2.5,
        "fud":         -2.5,
        "plunge":      -2.5,
        "lawsuit":     -2.5,
        "sec":         -1.5,   # Contextual; net-negative in crypto headlines
        # Extreme fear / catastrophe
        "rekt":        -4.0,
        "rug":         -4.0,
        "rugpull":     -4.0,
        "scam":        -4.0,
        "hack":        -3.5,
        "hacked":      -3.5,
        "exploit":     -3.5,
        "bankrupt":    -4.0,
        "insolvency":  -4.0,
        "subpoena":    -3.0,
        "delist":      -3.0,
        "delisted":    -3.0,
        "crash":       -3.5,
    }

    # XRP / legal-event lexicon — improves accuracy for jump-diffusion assets
    # where price action is driven by binary regulatory outcomes.
    xrp_legal_lexicon: dict[str, float] = {
        # Positive legal outcomes
        "settlement":   3.5,   # SEC settlement = major positive catalyst
        "dismissed":    3.0,   # Case dismissed = bullish relief
        "cleared":      2.5,   # Regulatory clearance
        "acquitted":    3.0,
        "compliant":    2.0,   # Compliance confirmed
        "exemption":    2.5,   # Regulatory exemption granted
        # Negative legal outcomes
        "injunction":  -3.5,   # Court injunction = trade halt risk
        "contempt":    -3.0,   # Court contempt = escalation
        "penalty":     -2.5,
        "seized":      -3.0,   # Asset seizure
        "freeze":      -2.5,   # Account/asset freeze
        "appeal":      -1.5,   # Prolongs uncertainty
        "charges":     -2.0,
        "indicted":    -3.5,
    }

    analyzer.lexicon.update(crypto_lexicon)
    analyzer.lexicon.update(xrp_legal_lexicon)

    log.info(
        "VADER loaded: %d general crypto terms + %d XRP legal terms.",
        len(crypto_lexicon),
        len(xrp_legal_lexicon),
    )
    return analyzer


# ── Headline Fetching ──────────────────────────────────────────────────────────
def fetch_recent_headlines(
    feed_urls: Iterable[str],
    max_headlines: int = MAX_HEADLINES,
    request_timeout_sec: int = REQUEST_TIMEOUT_SEC,
) -> list[str]:
    """Fetch up to `max_headlines` headline titles across the given RSS feeds."""
    collected: list[str] = []

    for url in feed_urls:
        if len(collected) >= max_headlines:
            break
        try:
            parsed = feedparser.parse(
                url,
                request_headers={"User-Agent": "tradingsystem-sentiment/1.0"},
            )
            if getattr(parsed, "bozo", False):
                log.warning(
                    "RSS parse warning for %s: %s",
                    url,
                    getattr(parsed, "bozo_exception", "unknown"),
                )
            for entry in getattr(parsed, "entries", []):
                title = str(getattr(entry, "title", "") or "").strip()
                if title:
                    collected.append(title)
                if len(collected) >= max_headlines:
                    break
        except Exception as exc:
            log.error("Failed fetching RSS %s: %s", url, exc)

    return collected[:max_headlines]


def filter_headlines_by_keywords(headlines: list[str], keywords: list[str]) -> list[str]:
    """Return only headlines containing at least one keyword (case-insensitive)."""
    if not keywords:
        return headlines
    return [h for h in headlines if any(kw in h.lower() for kw in keywords)]


# ── Scoring ────────────────────────────────────────────────────────────────────
def average_compound_score(headlines: list[str], analyzer: SentimentIntensityAnalyzer) -> float:
    if not headlines:
        return 0.0
    scores = [analyzer.polarity_scores(text)["compound"] for text in headlines]
    return float(sum(scores) / len(scores))


def classify_status(compound_score: float) -> str:
    if compound_score < -0.30:
        return "SEVERE_FEAR"
    if compound_score < -0.05:
        return "UNEASE"
    if compound_score <= 0.25:
        return "NEUTRAL"
    return "HYPE"


def build_payload(
    compound_score: float,
    latest_headline: str,
    coin: str = "GLOBAL",
) -> dict[str, object]:
    return {
        "timestamp":       int(time.time()),
        "coin":            coin,
        "compound_score":  float(compound_score),
        "status":          classify_status(compound_score),
        "latest_headline": latest_headline,
    }


# ── Publishing ─────────────────────────────────────────────────────────────────
def publish_sentiment(
    redis_client: redis.Redis,
    channel: str,
    payload: dict[str, object],
) -> None:
    try:
        redis_client.publish(channel, json.dumps(payload))
    except redis.exceptions.RedisError as exc:
        log.error("Failed to publish to channel %s: %s", channel, exc)
        raise


# ── Main cycle ─────────────────────────────────────────────────────────────────
def run_once(
    redis_client: redis.Redis,
    analyzer: SentimentIntensityAnalyzer,
    feed_urls: Iterable[str] = RSS_FEEDS,
    max_headlines: int = MAX_HEADLINES,
) -> None:
    """Fetch headlines, compute global + XRP sentiment, publish both."""
    # ── 1. Fetch all headlines ───────────────────────────────────────────────
    all_headlines = fetch_recent_headlines(
        feed_urls=list(feed_urls),
        max_headlines=max_headlines,
    )

    # ── 2. Global sentiment (all coins) ─────────────────────────────────────
    global_score   = average_compound_score(all_headlines, analyzer)
    global_payload = build_payload(
        global_score,
        latest_headline=all_headlines[0] if all_headlines else "",
        coin="GLOBAL",
    )
    publish_sentiment(redis_client, SENTIMENT_CHANNEL, global_payload)
    log.info(
        "Global sentiment published: score=%.4f status=%s headline=%r",
        global_payload["compound_score"],
        global_payload["status"],
        global_payload["latest_headline"],
    )

    # ── 3. XRP-specific sentiment (filtered headlines) ───────────────────────
    # Also pull from any dedicated XRP RSS feeds if configured.
    xrp_extra_headlines: list[str] = []
    if XRP_EXTRA_RSS_FEEDS:
        xrp_extra_headlines = fetch_recent_headlines(
            feed_urls=XRP_EXTRA_RSS_FEEDS,
            max_headlines=max_headlines,
        )

    combined_headlines = all_headlines + xrp_extra_headlines
    xrp_headlines = filter_headlines_by_keywords(combined_headlines, XRP_KEYWORDS)

    if xrp_headlines:
        xrp_score = average_compound_score(xrp_headlines, analyzer)
        xrp_latest = xrp_headlines[0]
    else:
        # No XRP headlines found — default to neutral (0.0) rather than global score
        # to avoid false signals from unrelated crypto news.
        xrp_score  = 0.0
        xrp_latest = ""
        log.info("No XRP-relevant headlines found this cycle; defaulting XRP sentiment to neutral.")

    xrp_payload = build_payload(xrp_score, xrp_latest, coin="XRP")
    publish_sentiment(redis_client, XRP_SENTIMENT_CHANNEL, xrp_payload)
    log.info(
        "XRP sentiment published: score=%.4f status=%s matched_headlines=%d headline=%r",
        xrp_payload["compound_score"],
        xrp_payload["status"],
        len(xrp_headlines),
        xrp_payload["latest_headline"],
    )


def run() -> None:
    analyzer = create_analyzer()

    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        socket_connect_timeout=5,
    )

    log.info(
        "Sentiment Oracle starting | poll=%ds | xrp_keywords=%s | global_channel=%s | xrp_channel=%s",
        POLL_INTERVAL_SEC,
        XRP_KEYWORDS,
        SENTIMENT_CHANNEL,
        XRP_SENTIMENT_CHANNEL,
    )

    while True:
        try:
            redis_client.ping()
            run_once(redis_client=redis_client, analyzer=analyzer)

        except redis.exceptions.RedisError as exc:
            log.error("Redis error: %s — retrying in %ds.", exc, RETRY_DELAY_SEC)
            time.sleep(RETRY_DELAY_SEC)
            continue
        except Exception as exc:
            log.error("Sentiment loop error: %s", exc)

        effective_interval = POLL_INTERVAL_SEC if POLL_INTERVAL_SEC > 0 else RETRY_DELAY_SEC
        time.sleep(effective_interval)


if __name__ == "__main__":
    run()
