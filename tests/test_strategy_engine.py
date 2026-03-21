from __future__ import annotations

import unittest
from unittest.mock import patch

from tests.test_loader import load_module


strategy = load_module("strategy_mod", "services/strategy-engine/strategy_engine.py")


class _RedisStub:
    def __init__(self):
        self.messages = []

    def publish(self, channel, payload):
        self.messages.append((channel, payload))


class StrategyEngineTests(unittest.TestCase):
    def setUp(self):
        strategy.prices.clear()
        strategy.in_position = False
        strategy.entry_price = 0.0
        strategy.last_signal_ts = 0.0

    def test_mean(self):
        self.assertEqual(strategy._mean([1.0, 2.0, 3.0]), 2.0)

    def test_std(self):
        result = strategy._std([1.0, 2.0, 3.0], 2.0)
        self.assertAlmostEqual(result, 0.816496, places=5)

    def test_compute_indicators_keys(self):
        prices = [100.0 + i for i in range(strategy.BB_PERIOD)]
        indicators = strategy._compute_indicators(prices)
        self.assertIn("sma", indicators)
        self.assertIn("upper", indicators)
        self.assertIn("lower", indicators)
        self.assertIn("atr", indicators)
        self.assertIn("vol_ratio", indicators)

    @patch("time.time", return_value=100.0)
    def test_publish_signal_respects_cooldown(self, _mock_time):
        redis_stub = _RedisStub()
        strategy.last_signal_ts = 99.5
        strategy._publish_signal(redis_stub, "BUY", "BTC/USD", "test", 1)
        self.assertEqual(len(redis_stub.messages), 0)

    def test_evaluate_signal_buy_path(self):
        redis_stub = _RedisStub()
        baseline = [100.0] * (strategy.BB_PERIOD - 1)
        for px in baseline:
            strategy.prices.append(px)

        strategy._evaluate_signal(90.0, "BTC/USD", 1, redis_stub)

        self.assertTrue(strategy.in_position)
        self.assertEqual(strategy.entry_price, 90.0)
        self.assertEqual(len(redis_stub.messages), 1)


if __name__ == "__main__":
    unittest.main()
