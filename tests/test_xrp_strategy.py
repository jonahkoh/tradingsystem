from __future__ import annotations

import asyncio
import json
import unittest
from collections import deque

from tests.test_loader import load_module


xrp = load_module("xrp_strategy_mod", "services/xrp-strategy/xrp_strategy_engine.py")


class _RedisStub:
    def __init__(self):
        self.messages = []

    async def publish(self, channel, payload):
        self.messages.append((channel, payload))
        return 1


class XrpStrategyTests(unittest.TestCase):
    def setUp(self):
        # Pin symbol so tests are independent of the .env TRADE_SYMBOL setting.
        xrp.TRADE_SYMBOL = "XRP/USD"

        # ATR state
        xrp.true_ranges.clear()
        xrp.atr_value       = 0.0
        xrp.atr_initialized = False
        xrp.last_price      = None

        # Sentiment
        xrp.xrp_sentiment_score = 0.0

        # Position state
        xrp.in_position            = False
        xrp.open_position_quantity = "0.0000"
        xrp.entry_price            = 0.0
        xrp.atr_at_entry           = 0.0

        # Execution handshake
        xrp.pending_action         = None
        xrp.pending_sell_quantity  = "0.0000"
        xrp.cooldown_until         = 0.0

        # Startup guards
        xrp.boot_liquidation_pending = False

    # ── ATR helpers ────────────────────────────────────────────────────────────
    def test_atr_seeds_after_period_true_ranges(self):
        """ATR should seed from the simple mean after ATR_PERIOD true ranges."""
        for _ in range(xrp.ATR_PERIOD):
            result = xrp._compute_atr_update(1.0)
        self.assertTrue(xrp.atr_initialized)
        self.assertAlmostEqual(xrp.atr_value, 1.0)
        self.assertEqual(result, 1.0)

    def test_atr_returns_none_before_seeded(self):
        """ATR returns None until ATR_PERIOD true ranges have been collected."""
        result = xrp._compute_atr_update(1.0)
        self.assertIsNone(result)
        self.assertFalse(xrp.atr_initialized)

    def test_atr_wilder_smoothing_after_seed(self):
        """Post-seed ATR follows Wilder's formula: (ATR×(n-1) + TR) / n."""
        for _ in range(xrp.ATR_PERIOD):
            xrp._compute_atr_update(1.0)  # seed at 1.0

        result = xrp._compute_atr_update(3.0)
        expected = (1.0 * (xrp.ATR_PERIOD - 1) + 3.0) / xrp.ATR_PERIOD
        self.assertAlmostEqual(result, expected, places=8)

    def test_target_quantity(self):
        """_target_quantity floors to 4dp: TARGET_ORDER_USD / price."""
        import math
        price    = 0.50
        expected = math.floor((xrp.TARGET_ORDER_USD / price) * 10_000) / 10_000
        self.assertEqual(xrp._target_quantity(price), f"{expected:.4f}")

    # ── ATR warm-up ────────────────────────────────────────────────────────────
    def test_first_tick_sets_last_price_only(self):
        """The very first tick just records last_price and emits no signal."""
        redis_stub = _RedisStub()
        asyncio.run(xrp.evaluate_market_tick(1.00, "XRP/USD", 0, redis_stub))

        self.assertEqual(xrp.last_price, 1.00)
        self.assertEqual(len(redis_stub.messages), 0)

    def test_no_signal_during_atr_warmup(self):
        """No signals during the first ATR_PERIOD ticks while ATR seeds."""
        redis_stub = _RedisStub()
        xrp.last_price = 1.00

        for i in range(xrp.ATR_PERIOD - 1):          # one short of seed
            asyncio.run(
                xrp.evaluate_market_tick(1.00 + i * 0.01, "XRP/USD", i, redis_stub)
            )

        self.assertEqual(len(redis_stub.messages), 0)
        self.assertFalse(xrp.atr_initialized)

    # ── ATR breakout → BUY ────────────────────────────────────────────────────
    def test_atr_breakout_triggers_buy(self):
        """A price spike > ATR_BREAKOUT_K × ATR with good sentiment → BUY."""
        redis_stub = _RedisStub()

        # Pre-seed ATR so the warm-up is bypassed.
        xrp.atr_value       = 0.05
        xrp.atr_initialized = True
        xrp.last_price      = 1.00
        xrp.xrp_sentiment_score = 0.3   # above SENTIMENT_FEAR_GATE (-0.10)

        # price=1.20: delta=0.20, ATR≈0.05+small; 0.20 > 2×0.05 ✓
        asyncio.run(xrp.evaluate_market_tick(1.20, "XRP/USD", 1, redis_stub))

        self.assertEqual(len(redis_stub.messages), 1)
        _, raw = redis_stub.messages[0]
        payload = json.loads(raw)
        self.assertEqual(payload["action"], "BUY")
        self.assertIn("ATR_Breakout", payload["reason"])
        self.assertEqual(xrp.pending_action, "BUY")
        self.assertEqual(xrp.entry_price, 1.20)
        self.assertGreater(xrp.atr_at_entry, 0.0)

    def test_buy_suppressed_by_sentiment_fear_gate(self):
        """BUY must be suppressed when XRP sentiment is at or below the fear gate."""
        redis_stub = _RedisStub()

        xrp.atr_value            = 0.05
        xrp.atr_initialized      = True
        xrp.last_price           = 1.00
        xrp.xrp_sentiment_score  = xrp.SENTIMENT_FEAR_GATE   # exactly at gate (not above)

        asyncio.run(xrp.evaluate_market_tick(1.20, "XRP/USD", 2, redis_stub))

        self.assertEqual(len(redis_stub.messages), 0)
        self.assertIsNone(xrp.pending_action)

    def test_small_price_move_does_not_trigger_buy(self):
        """A price move smaller than ATR_BREAKOUT_K × ATR must not trigger entry."""
        redis_stub = _RedisStub()

        xrp.atr_value            = 0.10
        xrp.atr_initialized      = True
        xrp.last_price           = 1.00
        xrp.xrp_sentiment_score  = 0.5

        # delta=0.05, threshold=2×0.10=0.20 → no breakout
        asyncio.run(xrp.evaluate_market_tick(1.05, "XRP/USD", 3, redis_stub))

        self.assertEqual(len(redis_stub.messages), 0)
        self.assertIsNone(xrp.pending_action)

    # ── BUY confirmation ──────────────────────────────────────────────────────
    def test_buy_confirmation_updates_position(self):
        """Successful BUY confirmation sets in_position and net quantity."""
        xrp.pending_action = "BUY"
        xrp.entry_price    = 1.20
        xrp.atr_at_entry   = 0.05

        executed_qty = 1000.0
        xrp.handle_execution_confirmation(json.dumps({
            "result":       "SUCCESS",
            "action":       "BUY",
            "effective_qty": executed_qty,
        }))

        expected_net = executed_qty * (1.0 - xrp.TRADING_FEE_RATE)
        self.assertTrue(xrp.in_position)
        self.assertAlmostEqual(float(xrp.open_position_quantity), expected_net, places=3)
        self.assertIsNone(xrp.pending_action)

    # ── Exit 1: ATR stop-loss ─────────────────────────────────────────────────
    def test_atr_stop_loss_triggers_sell(self):
        """Price at or below entry - ATR_STOPLOSS_K × atr_at_entry → SELL."""
        redis_stub = _RedisStub()

        xrp.in_position            = True
        xrp.open_position_quantity = "1500.0000"
        xrp.entry_price            = 1.00
        xrp.atr_at_entry           = 0.10   # stop = 1.00 - 1.5×0.10 = 0.85
        xrp.atr_value              = 0.10
        xrp.atr_initialized        = True
        xrp.last_price             = 0.86

        # Feed price at 0.84 (below stop 0.85)
        asyncio.run(xrp.evaluate_market_tick(0.84, "XRP/USD", 4, redis_stub))

        self.assertEqual(len(redis_stub.messages), 1)
        _, raw = redis_stub.messages[0]
        payload = json.loads(raw)
        self.assertEqual(payload["action"], "SELL")
        self.assertIn("ATR_StopLoss", payload["reason"])

    # ── Exit 2: Take-profit ───────────────────────────────────────────────────
    def test_take_profit_triggers_sell(self):
        """Price at or above entry × TAKE_PROFIT_MULT (default 1.08) → SELL."""
        redis_stub = _RedisStub()

        xrp.in_position            = True
        xrp.open_position_quantity = "1500.0000"
        xrp.entry_price            = 1.00
        xrp.atr_at_entry           = 0.10   # stop = 0.85 (well below take-profit)
        xrp.atr_value              = 0.10
        xrp.atr_initialized        = True
        xrp.last_price             = 1.07

        # Feed price at exactly take-profit level.
        take_profit_price = xrp.entry_price * xrp.TAKE_PROFIT_MULT   # 1.08
        asyncio.run(
            xrp.evaluate_market_tick(take_profit_price, "XRP/USD", 5, redis_stub)
        )

        self.assertEqual(len(redis_stub.messages), 1)
        _, raw = redis_stub.messages[0]
        payload = json.loads(raw)
        self.assertEqual(payload["action"], "SELL")
        self.assertIn("TakeProfit", payload["reason"])

    # ── Exit 3: Sentiment reversal ────────────────────────────────────────────
    def test_sentiment_reversal_exit_while_profitable(self):
        """Bad XRP sentiment while price > entry triggers a protective SELL."""
        redis_stub = _RedisStub()

        xrp.in_position            = True
        xrp.open_position_quantity = "1500.0000"
        xrp.entry_price            = 1.00
        xrp.atr_at_entry           = 0.10   # stop = 0.85
        xrp.atr_value              = 0.01
        xrp.atr_initialized        = True
        xrp.last_price             = 1.03
        # Score below exit gate (-0.15) while price is above entry → exit.
        xrp.xrp_sentiment_score    = -0.20

        asyncio.run(xrp.evaluate_market_tick(1.04, "XRP/USD", 6, redis_stub))

        self.assertEqual(len(redis_stub.messages), 1)
        _, raw = redis_stub.messages[0]
        payload = json.loads(raw)
        self.assertEqual(payload["action"], "SELL")
        self.assertIn("Sentiment_Reversal", payload["reason"])

    def test_sentiment_reversal_not_triggered_when_underwater(self):
        """Sentiment exit must NOT fire when price <= entry (already in a loss)."""
        redis_stub = _RedisStub()

        xrp.in_position            = True
        xrp.open_position_quantity = "1500.0000"
        xrp.entry_price            = 1.00
        xrp.atr_at_entry           = 0.10   # stop = 0.85
        xrp.atr_value              = 0.01
        xrp.atr_initialized        = True
        xrp.last_price             = 0.95
        xrp.xrp_sentiment_score    = -0.20   # terrible sentiment, but price at loss

        # Price = 0.96: above stop (0.85), below entry (1.00) → no sentiment exit.
        asyncio.run(xrp.evaluate_market_tick(0.96, "XRP/USD", 7, redis_stub))

        self.assertEqual(len(redis_stub.messages), 0)

    # ── SELL confirmation ─────────────────────────────────────────────────────
    def test_sell_confirmation_clears_position(self):
        """A full SELL confirmation resets all position state."""
        xrp.in_position            = True
        xrp.open_position_quantity = "1500.0000"
        xrp.entry_price            = 1.20
        xrp.atr_at_entry           = 0.05
        xrp.pending_action         = "SELL"

        xrp.handle_execution_confirmation(json.dumps({
            "result":       "SUCCESS",
            "action":       "SELL",
            "effective_qty": 1500.0,
        }))

        self.assertFalse(xrp.in_position)
        self.assertEqual(xrp.open_position_quantity, "0.0000")
        self.assertEqual(xrp.entry_price, 0.0)
        self.assertEqual(xrp.atr_at_entry, 0.0)
        self.assertIsNone(xrp.pending_action)

    # ── Sentiment update handler ───────────────────────────────────────────────
    def test_handle_xrp_sentiment_update(self):
        """handle_xrp_sentiment_update stores the compound_score."""
        xrp.handle_xrp_sentiment_update('{"compound_score": 0.42, "latest_headline": "XRP wins!"}')
        self.assertAlmostEqual(xrp.xrp_sentiment_score, 0.42)

    def test_handle_malformed_sentiment_does_not_crash(self):
        """Malformed sentiment JSON must be silently skipped."""
        original = xrp.xrp_sentiment_score
        xrp.handle_xrp_sentiment_update("not-json")
        self.assertEqual(xrp.xrp_sentiment_score, original)

    # ── Wrong symbol guard ─────────────────────────────────────────────────────
    def test_wrong_symbol_tick_is_ignored(self):
        """Ticks for any symbol other than XRP/USD must be dropped silently."""
        redis_stub = _RedisStub()

        xrp.atr_value       = 0.05
        xrp.atr_initialized = True
        xrp.last_price      = 1.00

        asyncio.run(xrp.evaluate_market_tick(1.20, "BTC/USD", 8, redis_stub))

        self.assertEqual(len(redis_stub.messages), 0)
        self.assertIsNone(xrp.pending_action)

    # ── Signal suppression while pending ──────────────────────────────────────
    def test_no_signal_while_pending(self):
        """No new signal is published while pending_action is already set."""
        redis_stub = _RedisStub()

        xrp.pending_action       = "BUY"
        xrp.atr_value            = 0.05
        xrp.atr_initialized      = True
        xrp.last_price           = 1.00
        xrp.xrp_sentiment_score  = 0.3

        asyncio.run(xrp.evaluate_market_tick(1.20, "XRP/USD", 9, redis_stub))

        self.assertEqual(len(redis_stub.messages), 0)

    # ── Confirmation symbol filter ─────────────────────────────────────────────
    def test_confirmation_for_other_symbol_is_ignored(self):
        """Execution confirmations for other symbols must not touch XRP state."""
        xrp.pending_action = "BUY"
        xrp.entry_price    = 1.20

        xrp.handle_execution_confirmation(json.dumps({
            "result":       "SUCCESS",
            "action":       "BUY",
            "symbol":       "SOL/USD",   # wrong symbol
            "effective_qty": 1000.0,
        }))

        # State must be unchanged.
        self.assertEqual(xrp.pending_action, "BUY")
        self.assertFalse(xrp.in_position)


if __name__ == "__main__":
    unittest.main()
