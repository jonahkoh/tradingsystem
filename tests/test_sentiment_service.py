from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tests.test_loader import load_module


sentiment_service = load_module(
    "sentiment_service_mod",
    "services/sentiment-oracle/sentiment_service.py",
    add_sys_path="services/sentiment-oracle",
)


class _RedisStub:
    def __init__(self):
        self.messages = []

    def publish(self, channel, payload):
        self.messages.append((channel, payload))
        return 1


class _AnalyzerStub:
    def __init__(self, scores):
        self._scores = list(scores)

    def polarity_scores(self, text):
        value = self._scores.pop(0)
        return {"compound": value}


class SentimentServiceTests(unittest.TestCase):
    def test_classify_status(self):
        self.assertEqual(sentiment_service.classify_status(-0.60), "SEVERE_FEAR")
        self.assertEqual(sentiment_service.classify_status(-0.20), "UNEASE")
        self.assertEqual(sentiment_service.classify_status(0.00), "NEUTRAL")
        self.assertEqual(sentiment_service.classify_status(0.20), "NEUTRAL")
        self.assertEqual(sentiment_service.classify_status(0.70), "HYPE")

    def test_average_compound_score(self):
        analyzer = _AnalyzerStub([0.5, -0.1, 0.2])
        score = sentiment_service.average_compound_score(["a", "b", "c"], analyzer)
        self.assertAlmostEqual(score, 0.2)

    def test_average_compound_score_empty(self):
        analyzer = _AnalyzerStub([])
        score = sentiment_service.average_compound_score([], analyzer)
        self.assertEqual(score, 0.0)

    @patch.object(sentiment_service.feedparser, "parse")
    def test_fetch_recent_headlines(self, parse_mock):
        parse_mock.side_effect = [
            SimpleNamespace(bozo=False, entries=[SimpleNamespace(title="BTC rallies"), SimpleNamespace(title="ETH up")]),
            SimpleNamespace(bozo=False, entries=[SimpleNamespace(title="Market cools")]),
        ]

        headlines = sentiment_service.fetch_recent_headlines(
            feed_urls=["https://a", "https://b"],
            max_headlines=3,
        )

        self.assertEqual(headlines, ["BTC rallies", "ETH up", "Market cools"])

    @patch.object(sentiment_service, "fetch_recent_headlines")
    def test_run_once_publishes_payload(self, fetch_mock):
        # Headlines have no XRP keywords → XRP channel gets neutral (0.0) default.
        fetch_mock.return_value = ["Bitcoin drops as regulations tighten", "Altcoins slip"]

        redis_stub = _RedisStub()
        analyzer = _AnalyzerStub([-0.7, -0.3])

        # run_once returns None; side effects are the two publish calls.
        result = sentiment_service.run_once(
            redis_client=redis_stub,
            analyzer=analyzer,
            feed_urls=["https://a"],
            max_headlines=2,
        )
        self.assertIsNone(result)

        # Expect two messages: global channel first, XRP channel second.
        self.assertEqual(len(redis_stub.messages), 2)

        # ── Global sentiment ────────────────────────────────────────────────
        channel, raw_payload = redis_stub.messages[0]
        self.assertEqual(channel, sentiment_service.SENTIMENT_CHANNEL)
        data = json.loads(raw_payload)
        self.assertEqual(data["status"], "SEVERE_FEAR")
        self.assertEqual(data["latest_headline"], "Bitcoin drops as regulations tighten")
        self.assertIsInstance(data["compound_score"], float)
        self.assertEqual(data["coin"], "GLOBAL")

        # ── XRP sentiment (no matching headlines → neutral default) ─────────
        xrp_channel, xrp_raw = redis_stub.messages[1]
        self.assertEqual(xrp_channel, sentiment_service.XRP_SENTIMENT_CHANNEL)
        xrp_data = json.loads(xrp_raw)
        self.assertEqual(xrp_data["compound_score"], 0.0)
        self.assertEqual(xrp_data["coin"], "XRP")


if __name__ == "__main__":
    unittest.main()
