from __future__ import annotations

import asyncio
import json
import unittest

from tests.test_loader import load_module


sol = load_module("sol_strategy_mod", "services/sol-strategy/sol_strategy_engine.py")


class _RedisStub:
    def __init__(self):
        self.messages = []

    async def publish(self, channel, payload):
        self.messages.append((channel, payload))
        return 1


class SolStrategyTests(unittest.TestCase):
    def setUp(self):
        # Pin symbol so tests are independent of the .env TRADE_SYMBOL setting.
        sol.TRADE_SYMBOL = "SOL/USD"

        # EMA state
        sol.fast_ema       = None
        sol.slow_ema       = None
        sol.prev_fast_ema  = None
        sol.prev_slow_ema  = None
        sol.tick_count     = 0

        # ATR state
        sol.atr_true_ranges.clear()
        sol.atr_value       = 0.0
        sol.atr_initialized = False
        sol.last_atr_price  = None

        # Sentiment
        sol.global_sentiment_score = 0.0

        # Position state
        sol.in_position            = False
        sol.open_position_quantity = "0.0000"
        sol.entry_price            = 0.0
        sol.atr_at_entry           = 0.0

        # Execution handshake
        sol.pending_action         = None
        sol.pending_sell_quantity  = "0.0000"
        sol.cooldown_until         = 0.0

        # Startup guards
        sol.boot_liquidation_pending = False

    # ── EMA helpers ────────────────────────────────────────────────────────────
    def test_update_ema_seeds_from_first_price(self):
        """First call seeds from the price itself (prev=None)."""
        result = sol._update_ema(None, 150.0, 20)
        self.assertEqual(result, 150.0)

    def test_update_ema_formula(self):
        """EMA_t = α × price + (1-α) × prev  where α = 2/(period+1)."""
        alpha  = 2.0 / (20 + 1)
        prev   = 100.0
        price  = 110.0
        expected = alpha * price + (1 - alpha) * prev
        result = sol._update_ema(prev, price, 20)
        self.assertAlmostEqual(result, expected, places=8)

    # ── Warm-up suppression ────────────────────────────────────────────────────
    def test_no_signal_during_warmup(self):
        """Signals must be suppressed until tick_count >= EMA_SLOW_PERIOD."""
        redis_stub = _RedisStub()
        # Feed EMA_SLOW_PERIOD - 1 ticks — all should be silently absorbed.
        for i in range(sol.EMA_SLOW_PERIOD - 1):
            asyncio.run(
                sol.evaluate_market_tick(100.0 + i, "SOL/USD", i, redis_stub)
            )
        self.assertEqual(len(redis_stub.messages), 0)
        self.assertIsNone(sol.pending_action)

    # ── Golden cross → BUY ─────────────────────────────────────────────────────
    def test_golden_cross_triggers_buy(self):
        """A golden cross with neutral sentiment publishes a BUY signal."""
        redis_stub = _RedisStub()

        # Pre-warm tick counter so crossover evaluation is active.
        sol.tick_count = sol.EMA_SLOW_PERIOD

        # Position fast EMA just below slow EMA — next tick flips the cross.
        # With alpha_fast = 2/21 ≈ 0.095 and price=200, the fast EMA will
        # leap above the slow EMA.
        sol.fast_ema = 99.0
        sol.slow_ema = 101.0

        asyncio.run(sol.evaluate_market_tick(200.0, "SOL/USD", 1, redis_stub))

        self.assertEqual(len(redis_stub.messages), 1)
        _, raw = redis_stub.messages[0]
        payload = json.loads(raw)
        self.assertEqual(payload["action"], "BUY")
        self.assertIn("Golden_Cross", payload["reason"])
        self.assertEqual(sol.pending_action, "BUY")
        self.assertEqual(sol.entry_price, 200.0)

    def test_buy_suppressed_by_severe_fear(self):
        """BUY must be suppressed when global sentiment <= SENTIMENT_ENTRY_GATE."""
        redis_stub = _RedisStub()

        sol.tick_count             = sol.EMA_SLOW_PERIOD
        sol.fast_ema               = 99.0
        sol.slow_ema               = 101.0
        sol.global_sentiment_score = sol.SENTIMENT_ENTRY_GATE  # exactly at gate (not above)

        asyncio.run(sol.evaluate_market_tick(200.0, "SOL/USD", 2, redis_stub))

        self.assertEqual(len(redis_stub.messages), 0)
        self.assertIsNone(sol.pending_action)

    # ── BUY confirmation ───────────────────────────────────────────────────────
    def test_buy_confirmation_updates_position(self):
        """handle_execution_confirmation on BUY sets in_position and net qty."""
        sol.pending_action = "BUY"
        sol.entry_price    = 150.0

        executed_qty = 10.0
        sol.handle_execution_confirmation(json.dumps({
            "result":       "SUCCESS",
            "action":       "BUY",
            "effective_qty": executed_qty,
        }))

        expected_net = executed_qty * (1.0 - sol.TRADING_FEE_RATE)
        self.assertTrue(sol.in_position)
        self.assertAlmostEqual(float(sol.open_position_quantity), expected_net, places=3)
        self.assertIsNone(sol.pending_action)

    # ── ATR stop-loss ─────────────────────────────────────────────────────────
    def test_atr_stop_loss_triggers_sell(self):
        """Price below entry - ATR_STOPLOSS_K × atr_at_entry publishes SELL."""
        redis_stub = _RedisStub()

        sol.tick_count             = sol.EMA_SLOW_PERIOD + 1
        sol.in_position            = True
        sol.open_position_quantity = "500.0000"
        sol.entry_price            = 100.0
        sol.atr_at_entry           = 5.0   # stop = 100 - 2×5 = 90
        # EMAs positioned so there is no death cross at the test price.
        sol.fast_ema = 90.0
        sol.slow_ema = 95.0   # fast < slow — death cross guard (no flip)

        # Price drops to 89 (< stop 90).
        asyncio.run(sol.evaluate_market_tick(89.0, "SOL/USD", 3, redis_stub))

        self.assertEqual(len(redis_stub.messages), 1)
        _, raw = redis_stub.messages[0]
        payload = json.loads(raw)
        self.assertEqual(payload["action"], "SELL")
        self.assertIn("ATR_StopLoss", payload["reason"])

    # ── Death cross → SELL ─────────────────────────────────────────────────────
    def test_death_cross_triggers_sell(self):
        """A death cross while in position publishes a SELL signal."""
        redis_stub = _RedisStub()

        sol.tick_count             = sol.EMA_SLOW_PERIOD + 1
        sol.in_position            = True
        sol.open_position_quantity = "500.0000"
        sol.entry_price            = 100.0
        # Make ATR stop unreachably low so only the death cross can exit.
        sol.atr_at_entry           = 1000.0   # stop = 100 - 2000 = -1900

        # Place EMAs just barely crossing: fast 100.01 > slow 100.00.
        # Price = 99.0 tips them: new_fast < new_slow → death cross.
        sol.fast_ema = 100.01
        sol.slow_ema = 100.00

        asyncio.run(sol.evaluate_market_tick(99.0, "SOL/USD", 4, redis_stub))

        self.assertEqual(len(redis_stub.messages), 1)
        _, raw = redis_stub.messages[0]
        payload = json.loads(raw)
        self.assertEqual(payload["action"], "SELL")
        self.assertIn("Death_Cross", payload["reason"])

    # ── Wrong symbol guard ─────────────────────────────────────────────────────
    def test_wrong_symbol_tick_is_ignored(self):
        """Ticks for any symbol other than TRADE_SYMBOL must be silently dropped."""
        redis_stub = _RedisStub()
        sol.tick_count = sol.EMA_SLOW_PERIOD + 1
        sol.fast_ema   = 99.0
        sol.slow_ema   = 101.0

        asyncio.run(sol.evaluate_market_tick(200.0, "BTC/USD", 5, redis_stub))

        self.assertEqual(len(redis_stub.messages), 0)
        self.assertIsNone(sol.pending_action)

    # ── Sentiment update ───────────────────────────────────────────────────────
    def test_handle_sentiment_update(self):
        """handle_sentiment_update stores the compound score."""
        sol.handle_sentiment_update('{"compound_score": -0.15}')
        self.assertAlmostEqual(sol.global_sentiment_score, -0.15)

    # ── SELL confirmation ──────────────────────────────────────────────────────
    def test_sell_confirmation_clears_position(self):
        """A full SELL confirmation resets position state."""
        sol.in_position            = True
        sol.open_position_quantity = "10.0000"
        sol.entry_price            = 150.0
        sol.atr_at_entry           = 5.0
        sol.pending_action         = "SELL"

        sol.handle_execution_confirmation(json.dumps({
            "result":       "SUCCESS",
            "action":       "SELL",
            "effective_qty": 10.0,
        }))

        self.assertFalse(sol.in_position)
        self.assertEqual(sol.open_position_quantity, "0.0000")
        self.assertEqual(sol.entry_price, 0.0)
        self.assertEqual(sol.atr_at_entry, 0.0)
        self.assertIsNone(sol.pending_action)

    # ── Signal suppression while pending ──────────────────────────────────────
    def test_no_signal_while_pending(self):
        """No new signal is published while pending_action is set."""
        redis_stub = _RedisStub()

        sol.pending_action = "BUY"
        sol.tick_count     = sol.EMA_SLOW_PERIOD + 1
        sol.fast_ema       = 99.0
        sol.slow_ema       = 101.0

        asyncio.run(sol.evaluate_market_tick(200.0, "SOL/USD", 6, redis_stub))

        self.assertEqual(len(redis_stub.messages), 0)


if __name__ == "__main__":
    unittest.main()
