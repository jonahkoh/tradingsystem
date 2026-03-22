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

    async def hset(self, key, mapping=None, **kwargs):
        return 1

    async def hgetall(self, key):
        return {}


class SolDCAStrategyTests(unittest.TestCase):
    def setUp(self):
        sol.TRADE_SYMBOL = "SOL/USD"

        # SMA window
        sol.sma_prices.clear()

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
        sol.average_cost           = 0.0
        sol.atr_at_entry           = 0.0

        # DCA tranche tracking
        sol.tranche_count  = 0
        sol.last_buy_price = 0.0

        # Execution handshake
        sol.pending_action         = None
        sol.pending_action_since   = 0.0
        sol.pending_sell_quantity  = "0.0000"
        sol.cooldown_until         = 0.0

        # Startup guards
        sol.boot_liquidation_pending = False

    # ── Helpers ────────────────────────────────────────────────────────────────
    def _seed_sma(self, price: float = 100.0):
        """Fill the SMA deque with SMA_PERIOD identical prices."""
        sol.sma_prices.clear()
        for _ in range(sol.SMA_PERIOD):
            sol.sma_prices.append(price)

    # ── SMA warm-up ────────────────────────────────────────────────────────────
    def test_sma_warmup_suppresses_signals(self):
        """No signals until SMA_PERIOD prices have been seen."""
        redis_stub = _RedisStub()
        for i in range(sol.SMA_PERIOD - 1):
            asyncio.run(sol.evaluate_market_tick(90.0, "SOL/USD", i, redis_stub))
        self.assertEqual(len(redis_stub.messages), 0)
        self.assertIsNone(sol.pending_action)

    def test_partial_sma_warmup_suppresses_signal(self):
        """Even a single missing SMA price must suppress all signals."""
        redis_stub = _RedisStub()
        # Pre-seed SMA_PERIOD - 2 prices; evaluate_market_tick appends one more,
        # leaving SMA_PERIOD - 1 total — still below the warm-up threshold.
        for _ in range(sol.SMA_PERIOD - 2):
            sol.sma_prices.append(100.0)
        asyncio.run(sol.evaluate_market_tick(80.0, "SOL/USD", 0, redis_stub))
        self.assertEqual(len(redis_stub.messages), 0)

    # ── First tranche entry ────────────────────────────────────────────────────
    def test_no_signal_when_price_above_sma_dip_threshold(self):
        """No BUY when price is above SMA × (1 - INITIAL_DIP_PCT)."""
        redis_stub = _RedisStub()
        self._seed_sma(100.0)
        # Price at exactly SMA (no dip) — should not trigger.
        asyncio.run(sol.evaluate_market_tick(100.0, "SOL/USD", 0, redis_stub))
        self.assertEqual(len(redis_stub.messages), 0)

    def test_first_tranche_triggers_buy(self):
        """Price well below SMA dip threshold fires first DCA tranche BUY."""
        redis_stub = _RedisStub()
        self._seed_sma(100.0)
        # evaluate_market_tick appends this price to the deque, shifting out one 100.0.
        # SMA becomes (19×100 + 90) / 20 = 99.5; threshold = 99.5 × 0.99 = 98.505.
        # 90 < 98.505 → triggers.
        trigger_price = 90.0
        asyncio.run(sol.evaluate_market_tick(trigger_price, "SOL/USD", 0, redis_stub))
        self.assertEqual(len(redis_stub.messages), 1)
        _, raw = redis_stub.messages[0]
        payload = json.loads(raw)
        self.assertEqual(payload["action"], "BUY")
        self.assertIn("DCA_Tranche1", payload["reason"])
        self.assertEqual(sol.pending_action, "BUY")

    def test_first_tranche_suppressed_by_sentiment_gate(self):
        """BUY suppressed when sentiment is at (not above) the sentiment gate."""
        redis_stub = _RedisStub()
        self._seed_sma(100.0)
        sol.global_sentiment_score = sol.SENTIMENT_GATE  # exactly at gate, not above
        trigger_price = 100.0 * (1.0 - sol.INITIAL_DIP_PCT) - 0.01
        asyncio.run(sol.evaluate_market_tick(trigger_price, "SOL/USD", 0, redis_stub))
        self.assertEqual(len(redis_stub.messages), 0)
        self.assertIsNone(sol.pending_action)

    # ── Additional tranche entry ────────────────────────────────────────────────
    def test_second_tranche_triggers_buy(self):
        """Price 1.5% below last_buy_price while in position fires next tranche."""
        redis_stub = _RedisStub()
        self._seed_sma(100.0)
        sol.in_position            = True
        sol.tranche_count          = 1
        sol.last_buy_price         = 100.0
        sol.open_position_quantity = "100"
        sol.average_cost           = 100.0
        trigger_price = 100.0 * (1.0 - sol.TRANCHE_DIP_PCT) - 0.01
        asyncio.run(sol.evaluate_market_tick(trigger_price, "SOL/USD", 0, redis_stub))
        self.assertEqual(len(redis_stub.messages), 1)
        _, raw = redis_stub.messages[0]
        payload = json.loads(raw)
        self.assertEqual(payload["action"], "BUY")
        self.assertIn("DCA_Tranche2", payload["reason"])

    def test_no_more_tranches_at_max(self):
        """No BUY when tranche_count == MAX_TRANCHES; exits are still evaluated."""
        redis_stub = _RedisStub()
        self._seed_sma(100.0)
        sol.in_position            = True
        sol.tranche_count          = sol.MAX_TRANCHES  # at max
        sol.last_buy_price         = 100.0
        sol.open_position_quantity = "500"
        sol.average_cost           = 100.0
        sol.atr_at_entry           = 1.0   # stop = 100 - 2 = 98
        trigger_price = 100.0 * (1.0 - sol.TRANCHE_DIP_PCT) - 0.01
        asyncio.run(sol.evaluate_market_tick(trigger_price, "SOL/USD", 0, redis_stub))
        # No BUY, and price is above stop and below take-profit → no SELL either.
        self.assertEqual(len(redis_stub.messages), 0)

    # ── BUY confirmation ───────────────────────────────────────────────────────
    def test_buy_confirmation_first_tranche_updates_state(self):
        """First BUY confirmation sets in_position, tranche_count=1, average_cost."""
        redis_stub = _RedisStub()
        sol.pending_action = "BUY"
        sol.tranche_count  = 0
        sol.average_cost   = 150.0
        sol.last_buy_price = 150.0
        sol.in_position    = False

        asyncio.run(sol.handle_execution_confirmation(json.dumps({
            "symbol":        "SOL/USD",
            "result":        "SUCCESS",
            "action":        "BUY",
            "effective_qty": 100.0,
        }), redis_stub))

        self.assertTrue(sol.in_position)
        self.assertEqual(sol.tranche_count, 1)
        expected_net = int(100.0 * (1.0 - sol.TRADING_FEE_RATE))
        self.assertEqual(int(sol.open_position_quantity), expected_net)
        self.assertAlmostEqual(sol.average_cost, 150.0, places=4)
        self.assertIsNone(sol.pending_action)

    def test_buy_confirmation_nth_tranche_updates_weighted_average(self):
        """Nth BUY confirmation correctly updates weighted average_cost."""
        redis_stub = _RedisStub()
        sol.pending_action         = "BUY"
        sol.tranche_count          = 1
        sol.in_position            = True
        sol.open_position_quantity = "99"   # qty after first tranche (fee-adjusted)
        sol.average_cost           = 150.0
        sol.last_buy_price         = 140.0  # second tranche at lower price

        asyncio.run(sol.handle_execution_confirmation(json.dumps({
            "symbol":        "SOL/USD",
            "result":        "SUCCESS",
            "action":        "BUY",
            "effective_qty": 100.0,
        }), redis_stub))

        self.assertEqual(sol.tranche_count, 2)
        net_new = int(100.0 * (1.0 - sol.TRADING_FEE_RATE))
        prev_qty = 99
        new_total = prev_qty + net_new
        expected_avg = (150.0 * prev_qty + 140.0 * net_new) / new_total
        self.assertAlmostEqual(sol.average_cost, expected_avg, places=4)
        self.assertEqual(int(sol.open_position_quantity), new_total)

    # ── Take-profit exit ───────────────────────────────────────────────────────
    def test_take_profit_triggers_sell(self):
        """Price >= average_cost × (1 + PROFIT_MARGIN) fires SELL ALL."""
        redis_stub = _RedisStub()
        self._seed_sma(100.0)
        sol.in_position            = True
        sol.open_position_quantity = "100"
        sol.average_cost           = 100.0
        sol.atr_at_entry           = 1.0   # stop well below
        sol.tranche_count          = 2
        take_profit_price = 100.0 * (1.0 + sol.PROFIT_MARGIN) + 0.01
        asyncio.run(sol.evaluate_market_tick(take_profit_price, "SOL/USD", 0, redis_stub))
        self.assertEqual(len(redis_stub.messages), 1)
        _, raw = redis_stub.messages[0]
        payload = json.loads(raw)
        self.assertEqual(payload["action"], "SELL")
        self.assertIn("DCA_TakeProfit", payload["reason"])

    def test_take_profit_not_triggered_below_threshold(self):
        """Price just below take-profit threshold must not fire SELL."""
        redis_stub = _RedisStub()
        self._seed_sma(100.0)
        sol.in_position            = True
        sol.open_position_quantity = "100"
        sol.average_cost           = 100.0
        sol.atr_at_entry           = 1.0
        below_tp = 100.0 * (1.0 + sol.PROFIT_MARGIN) - 0.01
        asyncio.run(sol.evaluate_market_tick(below_tp, "SOL/USD", 0, redis_stub))
        self.assertEqual(len(redis_stub.messages), 0)

    # ── ATR stop-loss ──────────────────────────────────────────────────────────
    def test_atr_stop_loss_triggers_sell(self):
        """Price below average_cost - ATR_STOPLOSS_K × atr_at_entry fires SELL."""
        redis_stub = _RedisStub()
        self._seed_sma(100.0)
        sol.in_position            = True
        sol.open_position_quantity = "100"
        sol.average_cost           = 100.0
        sol.atr_at_entry           = 5.0   # stop = 100 - 2×5 = 90
        stop_price = 100.0 - sol.ATR_STOPLOSS_K * 5.0   # = 90
        asyncio.run(sol.evaluate_market_tick(stop_price - 0.01, "SOL/USD", 0, redis_stub))
        self.assertEqual(len(redis_stub.messages), 1)
        _, raw = redis_stub.messages[0]
        payload = json.loads(raw)
        self.assertEqual(payload["action"], "SELL")
        self.assertIn("ATR_StopLoss", payload["reason"])

    # ── SELL confirmation ──────────────────────────────────────────────────────
    def test_sell_confirmation_resets_all_dca_state(self):
        """Full SELL confirmation resets average_cost, tranche_count, last_buy_price."""
        redis_stub = _RedisStub()
        sol.in_position            = True
        sol.open_position_quantity = "100.0000"
        sol.average_cost           = 100.0
        sol.atr_at_entry           = 5.0
        sol.tranche_count          = 3
        sol.last_buy_price         = 90.0
        sol.pending_action         = "SELL"

        asyncio.run(sol.handle_execution_confirmation(json.dumps({
            "symbol":        "SOL/USD",
            "result":        "SUCCESS",
            "action":        "SELL",
            "effective_qty": 100.0,
        }), redis_stub))

        self.assertFalse(sol.in_position)
        self.assertEqual(sol.average_cost, 0.0)
        self.assertEqual(sol.tranche_count, 0)
        self.assertEqual(sol.last_buy_price, 0.0)
        self.assertEqual(sol.atr_at_entry, 0.0)
        self.assertIsNone(sol.pending_action)

    # ── Failure reverts ────────────────────────────────────────────────────────
    def test_buy_failure_first_tranche_fully_reverts(self):
        """BUY failure on first tranche resets all position state."""
        redis_stub = _RedisStub()
        sol.pending_action = "BUY"
        sol.tranche_count  = 0
        sol.in_position    = False
        sol.average_cost   = 100.0
        sol.last_buy_price = 100.0

        asyncio.run(sol.handle_execution_confirmation(json.dumps({
            "symbol": "SOL/USD",
            "result": "FAILURE",
            "action": "BUY",
        }), redis_stub))

        self.assertFalse(sol.in_position)
        self.assertEqual(sol.average_cost, 0.0)
        self.assertEqual(sol.last_buy_price, 0.0)
        self.assertEqual(sol.tranche_count, 0)
        self.assertIsNone(sol.pending_action)

    def test_buy_failure_nth_tranche_preserves_existing_position(self):
        """BUY failure on nth tranche preserves the existing position and average_cost."""
        redis_stub = _RedisStub()
        sol.pending_action         = "BUY"
        sol.tranche_count          = 1
        sol.in_position            = True
        sol.open_position_quantity = "99"
        sol.average_cost           = 150.0
        sol.last_buy_price         = 140.0

        asyncio.run(sol.handle_execution_confirmation(json.dumps({
            "symbol": "SOL/USD",
            "result": "FAILURE",
            "action": "BUY",
        }), redis_stub))

        # Position unchanged.
        self.assertTrue(sol.in_position)
        self.assertEqual(sol.tranche_count, 1)
        self.assertAlmostEqual(sol.average_cost, 150.0)
        self.assertIsNone(sol.pending_action)

    # ── Pending suppression ────────────────────────────────────────────────────
    def test_no_signal_while_pending(self):
        """No new signals published while pending_action is set."""
        redis_stub = _RedisStub()
        self._seed_sma(100.0)
        sol.pending_action = "BUY"
        trigger_price = 100.0 * (1.0 - sol.INITIAL_DIP_PCT) - 0.01
        asyncio.run(sol.evaluate_market_tick(trigger_price, "SOL/USD", 0, redis_stub))
        self.assertEqual(len(redis_stub.messages), 0)

    # ── Symbol guard ───────────────────────────────────────────────────────────
    def test_wrong_symbol_tick_is_ignored(self):
        """Ticks for other symbols are silently dropped."""
        redis_stub = _RedisStub()
        self._seed_sma(100.0)
        asyncio.run(sol.evaluate_market_tick(80.0, "BTC/USD", 0, redis_stub))
        self.assertEqual(len(redis_stub.messages), 0)
        self.assertIsNone(sol.pending_action)

    def test_confirmation_for_wrong_symbol_is_ignored(self):
        """Confirmations for other symbols do not change pending_action."""
        redis_stub = _RedisStub()
        sol.pending_action = "BUY"

        asyncio.run(sol.handle_execution_confirmation(json.dumps({
            "symbol": "BTC/USD",
            "result": "SUCCESS",
            "action": "BUY",
            "effective_qty": 100.0,
        }), redis_stub))

        self.assertEqual(sol.pending_action, "BUY")  # unchanged

    # ── Sentiment ─────────────────────────────────────────────────────────────
    def test_handle_sentiment_update(self):
        """handle_sentiment_update stores the compound score."""
        sol.handle_sentiment_update('{"compound_score": -0.15}')
        self.assertAlmostEqual(sol.global_sentiment_score, -0.15)

    # ── Helpers ───────────────────────────────────────────────────────────────
    def test_tranche_quantity_zero_price_returns_zero(self):
        """_tranche_quantity(0) must not divide by zero."""
        self.assertEqual(sol._tranche_quantity(0), "0")

    def test_sentiment_scalar_severe_fear(self):
        """Sentiment below -0.30 returns SEVERE_FEAR scalar."""
        sol.global_sentiment_score = -0.50
        self.assertAlmostEqual(sol._sentiment_scalar(), sol.SENTIMENT_SCALAR_SEVERE_FEAR)

    def test_sentiment_scalar_hype(self):
        """Sentiment above 0.25 returns HYPE scalar."""
        sol.global_sentiment_score = 0.50
        self.assertAlmostEqual(sol._sentiment_scalar(), sol.SENTIMENT_SCALAR_HYPE)


if __name__ == "__main__":
    unittest.main()
