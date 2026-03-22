from __future__ import annotations

import asyncio
import json
import unittest

from tests.test_loader import load_module


strategy = load_module("strategy_mod", "services/strategy-engine/btc_eth_strategy_engine.py")


class _RedisStub:
    def __init__(self):
        self.messages = []
        self.hashes = {}

    async def publish(self, channel, payload):
        self.messages.append((channel, payload))
        return 1

    async def hset(self, key, mapping):
        self.hashes[key] = dict(mapping)
        return 1


class StrategyEngineTests(unittest.TestCase):
    def setUp(self):
        # Pin symbol so tests are independent of the .env TRADE_SYMBOL setting.
        strategy.DEFAULT_SYMBOL = "BTC/USD"

        # Core window state
        strategy.prices.clear()
        strategy.trend_prices.clear()
        strategy.latest_sentiment_score = 0.0

        # ATR state
        strategy.atr_true_ranges.clear()
        strategy.atr_value = 0.0
        strategy.atr_initialized = False
        strategy.last_atr_price = None

        # Position state
        strategy.in_position = False
        strategy.open_position_quantity = "0.0000"
        strategy.pending_action = None
        strategy.entry_price = 0.0
        strategy.stop_loss_price = 0.0
        strategy.half_position_sold = False

        # Timing state
        strategy.cooldown_until = 0.0
        strategy.boot_liquidation_pending = False

    # ── Core helpers ───────────────────────────────────────────────────────────
    def test_resolve_sd_profile(self):
        self.assertEqual(strategy.resolve_sd_profile(-0.50)["multiplier"], 2.95)
        self.assertEqual(strategy.resolve_sd_profile(-0.10)["multiplier"], 2.45)
        self.assertEqual(strategy.resolve_sd_profile(0.00)["multiplier"],  1.95)
        self.assertEqual(strategy.resolve_sd_profile(0.50)["multiplier"],  1.45)

    def test_mean(self):
        self.assertEqual(strategy._mean([1.0, 2.0, 3.0]), 2.0)

    def test_std(self):
        result = strategy._std([1.0, 2.0, 3.0], 2.0)
        self.assertAlmostEqual(result, 0.816496, places=5)

    def test_quantity_from_notional(self):
        # 20000 / 65000 = 0.30769... → floored to 4dp = 0.3076
        qty = strategy._quantity_from_notional(65000.0, strategy.TARGET_ORDER_USD)
        self.assertEqual(qty, "0.3076")

    def test_handle_sentiment_update(self):
        strategy.handle_sentiment_update('{"compound_score": -0.2}')
        self.assertEqual(strategy.latest_sentiment_score, -0.2)
        self.assertEqual(
            strategy.resolve_sd_profile(strategy.latest_sentiment_score)["label"],
            "UNEASE",
        )

    # ── ATR computation ────────────────────────────────────────────────────────
    def test_atr_seeds_after_period_ticks(self):
        # Seed with ATR_PERIOD identical price changes.
        strategy.last_atr_price = 100.0
        for i in range(strategy.ATR_PERIOD):
            result = strategy._compute_atr(100.0 + (i + 1))
        # After ATR_PERIOD true ranges, ATR should be seeded.
        self.assertTrue(strategy.atr_initialized)
        self.assertGreater(strategy.atr_value, 0.0)

    def test_atr_returns_none_before_seed(self):
        # Fresh state — first call seeds last_atr_price only.
        result = strategy._compute_atr(100.0)
        self.assertIsNone(result)
        self.assertFalse(strategy.atr_initialized)

    # ── BUY path ───────────────────────────────────────────────────────────────
    def test_evaluate_market_tick_buy_path(self):
        redis_stub = _RedisStub()

        strategy.latest_sentiment_score = -0.6   # → SEVERE_FEAR (2.95 SD)

        # Fill Bollinger window minus one so the next tick completes it.
        for px in [100.0] * (strategy.WINDOW_PERIOD - 1):
            strategy.prices.append(px)

        # price=90.0 should be well below dynamic lower band.
        asyncio.run(strategy.evaluate_market_tick(90.0, "BTC/USD", 1, redis_stub))

        self.assertFalse(strategy.in_position)
        self.assertEqual(strategy.pending_action, "BUY")
        self.assertEqual(strategy.entry_price, 90.0)
        # ATR not seeded yet → fallback stop used.
        self.assertAlmostEqual(
            strategy.stop_loss_price,
            90.0 * strategy.FALLBACK_STOP_LOSS,
            places=6,
        )
        self.assertEqual(len(redis_stub.messages), 1)

        channel, raw_payload = redis_stub.messages[0]
        payload = json.loads(raw_payload)
        self.assertEqual(channel, strategy.TRADE_SIGNALS_CHANNEL)
        self.assertEqual(payload["action"], "BUY")
        self.assertEqual(payload["reason"], "2.95_SD_Panic_Dip")

        # Confirm BUY fill — uses any sensible executed qty.
        executed_qty = 94.0
        asyncio.run(strategy.handle_execution_confirmation(json.dumps({
            "result":       "SUCCESS",
            "action":       "BUY",
            "symbol":       "BTC/USD",
            "effective_qty": executed_qty,
        }), redis_stub))

        expected_net = executed_qty * (1.0 - strategy.TRADING_FEE_RATE)
        self.assertTrue(strategy.in_position)
        self.assertAlmostEqual(float(strategy.open_position_quantity), expected_net, places=3)
        self.assertIsNone(strategy.pending_action)
        self.assertFalse(strategy.half_position_sold)   # reset on fresh BUY

    # ── Trend filter ───────────────────────────────────────────────────────────
    def test_buy_suppressed_by_trend_filter(self):
        """BUY must be suppressed when price is below the long-term SMA."""
        redis_stub = _RedisStub()

        strategy.latest_sentiment_score = -0.6   # SEVERE_FEAR

        # Fill Bollinger window.
        for px in [100.0] * (strategy.WINDOW_PERIOD - 1):
            strategy.prices.append(px)

        # Fill trend window with prices much higher than current price.
        for px in [200.0] * strategy.TREND_FILTER_PERIOD:
            strategy.trend_prices.append(px)

        # price=90 < trend_sma=200 → BUY must be suppressed.
        asyncio.run(strategy.evaluate_market_tick(90.0, "BTC/USD", 1, redis_stub))

        self.assertEqual(len(redis_stub.messages), 0)
        self.assertIsNone(strategy.pending_action)

    # ── Partial exit path ──────────────────────────────────────────────────────
    def test_partial_exit_first_half_at_sma(self):
        """First exit sells 50 % at SMA when fee spread is met."""
        redis_stub = _RedisStub()

        strategy.PARTIAL_EXITS_ENABLED = True
        strategy.in_position = True
        strategy.open_position_quantity = "0.2500"
        strategy.entry_price = 95.0
        strategy.stop_loss_price = 92.0
        strategy.half_position_sold = False
        strategy.latest_sentiment_score = 0.0

        for _ in range(strategy.WINDOW_PERIOD):
            strategy.prices.append(100.0)

        asyncio.run(strategy.evaluate_market_tick(100.0, "BTC/USD", 2, redis_stub))

        self.assertEqual(len(redis_stub.messages), 1)
        _, raw = redis_stub.messages[0]
        payload = json.loads(raw)
        self.assertEqual(payload["action"], "SELL")
        self.assertEqual(payload["reason"], "Partial_Exit_50pct_SMA")
        self.assertEqual(payload["quantity"], "0.1250")   # half of 0.25

        # Confirm partial fill.
        asyncio.run(strategy.handle_execution_confirmation(json.dumps({
            "result":       "SUCCESS",
            "action":       "SELL",
            "symbol":       "BTC/USD",
            "effective_qty": 0.125,
        }), redis_stub))
        self.assertTrue(strategy.in_position)       # still holding second half
        self.assertEqual(strategy.open_position_quantity, "0.1250")
        self.assertTrue(strategy.half_position_sold)
        self.assertIsNone(strategy.pending_action)

    def test_partial_exit_second_half_at_upper_band(self):
        """Second exit sells remaining 50 % at the upper Bollinger Band."""
        redis_stub = _RedisStub()

        strategy.PARTIAL_EXITS_ENABLED = True
        strategy.in_position = True
        strategy.open_position_quantity = "0.1250"
        strategy.entry_price = 95.0
        strategy.stop_loss_price = 92.0
        strategy.half_position_sold = True   # first half already exited
        strategy.latest_sentiment_score = 0.0

        # All prices at 100 → sma=100, std≈0, upper_band≈100.
        for _ in range(strategy.WINDOW_PERIOD):
            strategy.prices.append(100.0)

        asyncio.run(strategy.evaluate_market_tick(100.0, "BTC/USD", 3, redis_stub))

        self.assertEqual(len(redis_stub.messages), 1)
        _, raw = redis_stub.messages[0]
        payload = json.loads(raw)
        self.assertEqual(payload["action"], "SELL")
        self.assertEqual(payload["reason"], "Partial_Exit_50pct_Upper_Band")
        self.assertEqual(payload["quantity"], "0.1250")

    # ── Stop-loss path ─────────────────────────────────────────────────────────
    def test_evaluate_market_tick_triggers_stop_loss(self):
        redis_stub = _RedisStub()

        strategy.in_position = True
        strategy.open_position_quantity = "0.2500"
        strategy.entry_price = 100.0
        strategy.stop_loss_price = 97.0   # pre-set ATR stop
        strategy.latest_sentiment_score = 0.0

        for _ in range(strategy.WINDOW_PERIOD):
            strategy.prices.append(100.0)

        asyncio.run(strategy.evaluate_market_tick(96.9, "BTC/USD", 4, redis_stub))

        self.assertEqual(len(redis_stub.messages), 1)
        _, raw_payload = redis_stub.messages[0]
        payload = json.loads(raw_payload)
        self.assertEqual(payload["action"], "SELL")
        self.assertEqual(payload["reason"], "ATR_StopLoss")

    # ── Cooldown / hold paths ──────────────────────────────────────────────────
    def test_evaluate_market_tick_holds_when_sma_reached_but_fee_not_met(self):
        redis_stub = _RedisStub()

        strategy.PARTIAL_EXITS_ENABLED = False
        strategy.in_position = True
        strategy.open_position_quantity = "0.2500"
        strategy.entry_price = 100.0
        strategy.stop_loss_price = 97.0
        strategy.latest_sentiment_score = 0.0

        for _ in range(strategy.WINDOW_PERIOD):
            strategy.prices.append(100.0)

        # At SMA (100) but below required 0.25% spread (entry 100 → need 100.25).
        asyncio.run(strategy.evaluate_market_tick(100.0, "BTC/USD", 3, redis_stub))

        self.assertEqual(len(redis_stub.messages), 0)
        self.assertTrue(strategy.in_position)
        self.assertIsNone(strategy.pending_action)

    def test_cooldown_cancelled_when_price_recovers_above_sma(self):
        redis_stub = _RedisStub()
        strategy.cooldown_until = 9_999_999_999.0
        strategy.latest_sentiment_score = 0.0

        for _ in range(strategy.WINDOW_PERIOD):
            strategy.prices.append(100.0)

        asyncio.run(strategy.evaluate_market_tick(101.0, "BTC/USD", 5, redis_stub))

        self.assertEqual(strategy.cooldown_until, 0.0)

    # ── Symbol filter ──────────────────────────────────────────────────────────
    def test_tick_for_wrong_symbol_is_ignored(self):
        redis_stub = _RedisStub()
        for _ in range(strategy.WINDOW_PERIOD):
            strategy.prices.append(100.0)

        asyncio.run(strategy.evaluate_market_tick(50.0, "XRP/USD", 6, redis_stub))

        self.assertEqual(len(redis_stub.messages), 0)
        self.assertIsNone(strategy.pending_action)

    def test_confirmation_for_other_symbol_is_ignored(self):
        redis_stub = _RedisStub()
        strategy.pending_action = "BUY"
        strategy.entry_price = 100.0

        asyncio.run(strategy.handle_execution_confirmation(json.dumps({
            "result": "SUCCESS",
            "action": "BUY",
            "symbol": "ETH/USD",
            "effective_qty": 1.0,
        }), redis_stub))

        self.assertEqual(strategy.pending_action, "BUY")
        self.assertFalse(strategy.in_position)


if __name__ == "__main__":
    unittest.main()
