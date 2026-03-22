from __future__ import annotations

import asyncio
import json
import unittest

from tests.test_loader import load_module


strategy = load_module("strategy_mod", "services/strategy-engine/strategy_engine.py")


class _RedisStub:
    def __init__(self):
        self.messages = []

    async def publish(self, channel, payload):
        self.messages.append((channel, payload))
        return 1


class StrategyEngineTests(unittest.TestCase):
    def setUp(self):
        strategy.prices.clear()
        strategy.latest_sentiment_score = 0.0
        strategy.in_position = False
        strategy.open_position_quantity = "0.0000"
        strategy.pending_action = None
        strategy.entry_price = 0.0
        strategy.stop_loss_price = 0.0
        strategy.cooldown_until = 0.0
        strategy.boot_liquidation_pending = False

    def test_resolve_sd_profile(self):
        self.assertEqual(strategy.resolve_sd_profile(-0.50)["multiplier"], 2.95)
        self.assertEqual(strategy.resolve_sd_profile(-0.10)["multiplier"], 2.45)
        self.assertEqual(strategy.resolve_sd_profile(0.00)["multiplier"], 1.95)
        self.assertEqual(strategy.resolve_sd_profile(0.50)["multiplier"], 1.45)

    def test_mean(self):
        self.assertEqual(strategy._mean([1.0, 2.0, 3.0]), 2.0)

    def test_std(self):
        result = strategy._std([1.0, 2.0, 3.0], 2.0)
        self.assertAlmostEqual(result, 0.816496, places=5)

    def test_target_quantity_from_price(self):
        quantity = strategy.target_quantity_from_price(65000.0)
        self.assertEqual(quantity, "0.3076")

    def test_handle_sentiment_update(self):
        strategy.handle_sentiment_update('{"compound_score": -0.2}')
        self.assertEqual(strategy.latest_sentiment_score, -0.2)
        self.assertEqual(strategy.resolve_sd_profile(strategy.latest_sentiment_score)["label"], "UNEASE")

    def test_evaluate_market_tick_buy_path(self):
        redis_stub = _RedisStub()

        strategy.latest_sentiment_score = -0.6  # 2.95 SD profile
        baseline = [100.0] * (strategy.WINDOW_PERIOD - 1)
        for px in baseline:
            strategy.prices.append(px)

        asyncio.run(strategy.evaluate_market_tick(90.0, "BTC/USD", 1, redis_stub))

        self.assertFalse(strategy.in_position)
        self.assertEqual(strategy.pending_action, "BUY")
        self.assertEqual(strategy.entry_price, 90.0)
        self.assertEqual(strategy.stop_loss_price, 90.0 * strategy.STOP_LOSS_MULTIPLIER)
        self.assertEqual(len(redis_stub.messages), 1)

        channel, raw_payload = redis_stub.messages[0]
        payload = json.loads(raw_payload)
        self.assertEqual(channel, strategy.TRADE_SIGNALS_CHANNEL)
        self.assertEqual(payload["action"], "BUY")
        self.assertEqual(payload["reason"], "2.95_SD_Panic_Dip")

        strategy.handle_execution_confirmation(
            json.dumps(
                {
                    "result": "SUCCESS",
                    "action": "BUY",
                    "qty": float(strategy.target_quantity_from_price(90.0)),
                }
            )
        )
        expected_net = float(strategy.target_quantity_from_price(90.0)) * (1.0 - strategy.TRADING_FEE_RATE)
        self.assertTrue(strategy.in_position)
        self.assertEqual(strategy.open_position_quantity, f"{expected_net:.4f}")
        self.assertIsNone(strategy.pending_action)

    def test_evaluate_market_tick_sell_path(self):
        redis_stub = _RedisStub()

        strategy.in_position = True
        strategy.open_position_quantity = "0.2500"
        strategy.entry_price = 95.0
        strategy.stop_loss_price = 92.15
        strategy.latest_sentiment_score = 0.0

        # Fill window where the current SMA is exactly 100.
        for _ in range(strategy.WINDOW_PERIOD):
            strategy.prices.append(100.0)

        asyncio.run(strategy.evaluate_market_tick(100.0, "BTC/USD", 2, redis_stub))

        self.assertTrue(strategy.in_position)
        self.assertEqual(strategy.pending_action, "SELL")
        self.assertEqual(len(redis_stub.messages), 1)

        _, raw_payload = redis_stub.messages[0]
        payload = json.loads(raw_payload)
        self.assertEqual(payload["action"], "SELL")
        self.assertEqual(payload["reason"], "Mean_Reversion_TP")

        strategy.handle_execution_confirmation(
            json.dumps(
                {
                    "result": "SUCCESS",
                    "action": "SELL",
                    "effective_qty": 0.25,
                }
            )
        )
        self.assertFalse(strategy.in_position)
        self.assertEqual(strategy.open_position_quantity, "0.0000")
        self.assertEqual(strategy.entry_price, 0.0)
        self.assertEqual(strategy.stop_loss_price, 0.0)
        self.assertIsNone(strategy.pending_action)

    def test_evaluate_market_tick_holds_when_sma_reached_but_fee_not_met(self):
        redis_stub = _RedisStub()

        strategy.in_position = True
        strategy.open_position_quantity = "0.2500"
        strategy.entry_price = 100.0
        strategy.stop_loss_price = 97.0
        strategy.latest_sentiment_score = 0.0

        for _ in range(strategy.WINDOW_PERIOD):
            strategy.prices.append(100.0)

        # At SMA (100) but below required 0.25% spread (100.25).
        asyncio.run(strategy.evaluate_market_tick(100.0, "BTC/USD", 3, redis_stub))

        self.assertEqual(len(redis_stub.messages), 0)
        self.assertTrue(strategy.in_position)
        self.assertIsNone(strategy.pending_action)

    def test_evaluate_market_tick_triggers_stop_loss(self):
        redis_stub = _RedisStub()

        strategy.in_position = True
        strategy.open_position_quantity = "0.2500"
        strategy.entry_price = 100.0
        strategy.stop_loss_price = 97.0
        strategy.latest_sentiment_score = 0.0

        for _ in range(strategy.WINDOW_PERIOD):
            strategy.prices.append(100.0)

        asyncio.run(strategy.evaluate_market_tick(96.9, "BTC/USD", 4, redis_stub))

        self.assertEqual(len(redis_stub.messages), 1)
        _, raw_payload = redis_stub.messages[0]
        payload = json.loads(raw_payload)
        self.assertEqual(payload["action"], "SELL")
        self.assertEqual(payload["reason"], "Stop_Loss_3pct")

    def test_cooldown_cancelled_when_price_recovers_above_sma(self):
        redis_stub = _RedisStub()
        strategy.cooldown_until = 9999999999.0
        strategy.latest_sentiment_score = 0.0
        for _ in range(strategy.WINDOW_PERIOD):
            strategy.prices.append(100.0)

        asyncio.run(strategy.evaluate_market_tick(101.0, "BTC/USD", 5, redis_stub))

        self.assertEqual(strategy.cooldown_until, 0.0)


if __name__ == "__main__":
    unittest.main()
