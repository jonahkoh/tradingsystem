from __future__ import annotations

import asyncio
import json
import math
import unittest
from collections import deque

from tests.test_loader import load_module


sol = load_module("sol_strategy_mod", "services/strategy-engine/sol_strategy_engine.py")


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


class SolAdaptiveStrategyTests(unittest.TestCase):
    def setUp(self):
        sol.TRADE_SYMBOL = "SOL/USD"

        # Price windows
        sol.sma_prices.clear()
        sol.sma50_prices.clear()

        # ATR state
        sol.atr_true_ranges.clear()
        sol.atr_value       = 0.0
        sol.atr_initialized = False
        sol.last_atr_price  = None

        # Regime state
        sol.current_regime   = "UNCLEAR"
        sol.candidate_regime = "UNCLEAR"
        sol.candidate_ticks  = 0

        # Sentiment
        sol.global_sentiment_score = 0.0

        # Position state
        sol.in_position            = False
        sol.open_position_quantity = "0.0000"
        sol.average_cost           = 0.0
        sol.atr_at_entry           = 0.0
        sol.entry_regime           = ""

        # DCA tranche tracking
        sol.tranche_count  = 0
        sol.last_buy_price = 0.0

        # Rolling Sharpe
        sol.trade_returns.clear()
        sol.pending_sell_price = 0.0

        # Breakout delta
        sol.prev_price = None

        # Execution handshake
        sol.pending_action         = None
        sol.pending_action_since   = 0.0
        sol.pending_sell_quantity  = "0.0000"
        sol.cooldown_until         = 0.0

        # Startup guards
        sol.boot_liquidation_pending = False

    # ── Helpers ────────────────────────────────────────────────────────────────
    def _seed_warmup(self, price: float = 100.0):
        """Fill both SMA20 and SMA50 windows so warm-up is complete."""
        sol.sma_prices.clear()
        sol.sma50_prices.clear()
        for _ in range(sol.SMA50_PERIOD):
            sol.sma_prices.append(price)
            sol.sma50_prices.append(price)

    def _seed_atr(self, atr: float = 1.0):
        """Pre-seed ATR so regime detection is valid."""
        sol.atr_value       = atr
        sol.atr_initialized = True
        sol.last_atr_price  = 100.0

    # ── Warm-up ────────────────────────────────────────────────────────────────
    def test_warmup_suppresses_signals(self):
        """No signals until SMA50_PERIOD (50) prices have been seen."""
        redis_stub = _RedisStub()
        for i in range(sol.WARMUP_TICKS - 1):
            asyncio.run(sol.evaluate_market_tick(90.0, "SOL/USD", i, redis_stub))
        self.assertEqual(len(redis_stub.messages), 0)
        self.assertIsNone(sol.pending_action)

    def test_partial_warmup_suppresses_signal(self):
        """Partial warm-up (one tick short) suppresses signals."""
        redis_stub = _RedisStub()
        for _ in range(sol.WARMUP_TICKS - 2):
            sol.sma50_prices.append(100.0)
        asyncio.run(sol.evaluate_market_tick(80.0, "SOL/USD", 0, redis_stub))
        self.assertEqual(len(redis_stub.messages), 0)

    # ── _detect_regime ─────────────────────────────────────────────────────────
    def test_detect_regime_ranging(self):
        """Tiny ATR%, narrow BB, price hugging SMA50 → RANGING."""
        self._seed_warmup(100.0)
        self._seed_atr(0.10)   # atr_pct = 0.10/100 = 0.001 < ATR_PCT_RANGING (0.002) ✓
        sma20 = 100.0
        sma50 = 100.0
        # All three indicators vote ranging: tiny atr%, narrow BB (std≈0), price_dev=0
        result = sol._detect_regime(100.0, sma20, sma50)
        self.assertEqual(result, "RANGING")

    def test_detect_regime_trending_up(self):
        """Large ATR%, wide BB, price well above SMA50 → TRENDING_UP."""
        self._seed_warmup(100.0)
        # Set wide BB by making prices vary a lot
        for i in range(sol.SMA_PERIOD):
            sol.sma_prices.append(100.0 + (i % 2) * 10)  # alternating 100/110
        self._seed_atr(1.5)   # atr_pct = 1.5/130 ≈ 1.15% > 0.5% ✓
        sma20 = 105.0
        sma50 = 90.0
        # price (130) >> sma50 (90): price_dev = 40/90 ≈ 44% > 2% ✓
        # At least 2-of-3 should vote trending, price > sma50 → TRENDING_UP
        result = sol._detect_regime(130.0, sma20, sma50)
        self.assertEqual(result, "TRENDING_UP")

    def test_detect_regime_trending_down(self):
        """Large ATR%, price well below SMA50 → TRENDING_DOWN."""
        self._seed_warmup(100.0)
        for i in range(sol.SMA_PERIOD):
            sol.sma_prices.append(100.0 + (i % 2) * 10)
        self._seed_atr(1.5)
        sma20 = 95.0
        sma50 = 130.0
        # price (70) << sma50 (130): price_dev = 60/130 ≈ 46% > 2% ✓; trending, price < sma50
        result = sol._detect_regime(70.0, sma20, sma50)
        self.assertEqual(result, "TRENDING_DOWN")

    def test_detect_regime_unclear_when_atr_zero(self):
        """ATR=0 immediately returns UNCLEAR (no divide-by-zero)."""
        self._seed_warmup(100.0)
        sol.atr_value = 0.0
        result = sol._detect_regime(100.0, 100.0, 100.0)
        self.assertEqual(result, "UNCLEAR")

    # ── _update_regime (dwell filter) ─────────────────────────────────────────
    def test_regime_does_not_commit_before_dwell(self):
        """A new candidate regime must not commit before REGIME_DWELL_TICKS."""
        sol.current_regime   = "UNCLEAR"
        sol.candidate_regime = "UNCLEAR"
        sol.candidate_ticks  = 0
        for _ in range(sol.REGIME_DWELL_TICKS - 1):
            sol._update_regime("RANGING")
        self.assertEqual(sol.current_regime, "UNCLEAR")   # not committed yet

    def test_regime_commits_after_dwell(self):
        """Exactly REGIME_DWELL_TICKS ticks of the same candidate → commit."""
        sol.current_regime   = "UNCLEAR"
        sol.candidate_regime = "UNCLEAR"
        sol.candidate_ticks  = 0
        for _ in range(sol.REGIME_DWELL_TICKS):
            sol._update_regime("RANGING")
        self.assertEqual(sol.current_regime, "RANGING")

    def test_regime_dwell_resets_on_new_candidate(self):
        """A different detected regime resets the dwell counter."""
        sol.current_regime   = "UNCLEAR"
        sol.candidate_regime = "UNCLEAR"
        sol.candidate_ticks  = 0
        # Build up RANGING candidate partway
        for _ in range(sol.REGIME_DWELL_TICKS - 10):
            sol._update_regime("RANGING")
        # Switch to TRENDING_UP — counter resets
        sol._update_regime("TRENDING_UP")
        self.assertEqual(sol.candidate_regime, "TRENDING_UP")
        self.assertEqual(sol.candidate_ticks, 1)
        self.assertEqual(sol.current_regime, "UNCLEAR")

    # ── RANGING: BB mean reversion entry ──────────────────────────────────────
    def test_bb_entry_triggers_buy(self):
        """Price at lower Bollinger Band in RANGING regime → BUY signal."""
        redis_stub = _RedisStub()
        self._seed_warmup(100.0)
        self._seed_atr(0.1)
        sol.current_regime = "RANGING"
        sol.global_sentiment_score = 0.0   # above SENTIMENT_GATE

        # Fill BB window with 100.0 so std≈0 and lower_band≈100
        # Then set a price just at the lower band
        for _ in range(sol.SMA_PERIOD):
            sol.sma_prices.append(100.0)

        # To have a meaningful lower band, use a wider price window
        sol.sma_prices.clear()
        for i in range(sol.SMA_PERIOD):
            sol.sma_prices.append(100.0 + (i % 2) * 4)   # alternating 100/104, std≈2
        # sma≈102, lower_band≈102 - 2*2 = 98 → feed price at 97
        asyncio.run(sol.evaluate_market_tick(97.0, "SOL/USD", 1, redis_stub))

        self.assertEqual(len(redis_stub.messages), 1)
        _, raw = redis_stub.messages[0]
        payload = json.loads(raw)
        self.assertEqual(payload["action"], "BUY")
        self.assertIn("BB_MeanReversion", payload["reason"])
        self.assertEqual(sol.entry_regime, "RANGING")

    def test_bb_entry_suppressed_above_lower_band(self):
        """Price above the lower BB → no signal in RANGING regime."""
        redis_stub = _RedisStub()
        self._seed_warmup(100.0)
        self._seed_atr(0.1)
        sol.current_regime = "RANGING"
        sol.global_sentiment_score = 0.0

        # All prices at 100 → std=0, lower_band=100; price 101 > lower_band
        asyncio.run(sol.evaluate_market_tick(101.0, "SOL/USD", 1, redis_stub))

        self.assertEqual(len(redis_stub.messages), 0)

    # ── TRENDING_UP: ATR breakout entry ───────────────────────────────────────
    def test_breakout_entry_triggers_buy(self):
        """One-tick delta > ATR_BREAKOUT_K × ATR in TRENDING_UP → BUY signal."""
        redis_stub = _RedisStub()
        self._seed_warmup(100.0)
        self._seed_atr(1.0)
        sol.current_regime         = "TRENDING_UP"
        sol.prev_price             = 100.0
        sol.global_sentiment_score = 0.0

        # delta = 103 - 100 = 3 > ATR_BREAKOUT_K (2.0) × ATR (1.0) = 2 ✓
        asyncio.run(sol.evaluate_market_tick(103.0, "SOL/USD", 1, redis_stub))

        self.assertEqual(len(redis_stub.messages), 1)
        _, raw = redis_stub.messages[0]
        payload = json.loads(raw)
        self.assertEqual(payload["action"], "BUY")
        self.assertIn("ATR_Breakout", payload["reason"])
        self.assertEqual(sol.entry_regime, "TRENDING_UP")

    def test_breakout_entry_suppressed_small_delta(self):
        """Delta < ATR_BREAKOUT_K × ATR → no BUY in TRENDING_UP regime."""
        redis_stub = _RedisStub()
        self._seed_warmup(100.0)
        self._seed_atr(2.0)   # threshold = 2×2 = 4; delta=1 is too small
        sol.current_regime         = "TRENDING_UP"
        sol.prev_price             = 100.0
        sol.global_sentiment_score = 0.0

        asyncio.run(sol.evaluate_market_tick(101.0, "SOL/USD", 1, redis_stub))

        self.assertEqual(len(redis_stub.messages), 0)

    # ── DCA first tranche ──────────────────────────────────────────────────────
    def test_dca_first_tranche_triggers_buy(self):
        """Price < SMA20 × (1 - INITIAL_DIP_PCT) in UNCLEAR regime → DCA_Tranche1 BUY."""
        redis_stub = _RedisStub()
        self._seed_warmup(100.0)
        self._seed_atr(1.0)
        sol.current_regime         = "UNCLEAR"
        sol.global_sentiment_score = 0.0   # above SENTIMENT_GATE (-0.30)

        # sma20 ≈ 100 (all prices 100); threshold = 99; feed 80 < 99
        asyncio.run(sol.evaluate_market_tick(80.0, "SOL/USD", 1, redis_stub))

        self.assertEqual(len(redis_stub.messages), 1)
        _, raw = redis_stub.messages[0]
        payload = json.loads(raw)
        self.assertEqual(payload["action"], "BUY")
        self.assertIn("DCA_Tranche1", payload["reason"])
        self.assertIn("UNCLEAR", payload["reason"])
        self.assertEqual(sol.entry_regime, "UNCLEAR")

    def test_dca_first_tranche_suppressed_by_sentiment_gate(self):
        """Sentiment at or below SENTIMENT_GATE suppresses DCA first tranche."""
        redis_stub = _RedisStub()
        self._seed_warmup(100.0)
        self._seed_atr(1.0)
        sol.current_regime         = "UNCLEAR"
        sol.global_sentiment_score = sol.SENTIMENT_GATE   # exactly at gate, not above

        asyncio.run(sol.evaluate_market_tick(80.0, "SOL/USD", 1, redis_stub))

        self.assertEqual(len(redis_stub.messages), 0)
        self.assertIsNone(sol.pending_action)

    def test_dca_first_tranche_not_triggered_above_dip(self):
        """Price above SMA × (1 - INITIAL_DIP_PCT) → no DCA entry."""
        redis_stub = _RedisStub()
        self._seed_warmup(100.0)
        self._seed_atr(1.0)
        sol.current_regime         = "UNCLEAR"
        sol.global_sentiment_score = 0.0

        # sma20≈100; threshold≈99; price=100 is above threshold
        asyncio.run(sol.evaluate_market_tick(100.0, "SOL/USD", 1, redis_stub))

        self.assertEqual(len(redis_stub.messages), 0)

    # ── DCA additional tranches ────────────────────────────────────────────────
    def test_dca_additional_tranche_triggers_buy(self):
        """Price 1.5% below last_buy_price in DCA position → next tranche BUY."""
        redis_stub = _RedisStub()
        self._seed_warmup(100.0)
        self._seed_atr(1.0)
        sol.in_position            = True
        sol.entry_regime           = "TRENDING_DOWN"
        sol.tranche_count          = 1
        sol.last_buy_price         = 100.0
        sol.open_position_quantity = "100"
        sol.average_cost           = 100.0
        sol.global_sentiment_score = 0.0

        trigger_price = 100.0 * (1.0 - sol.TRANCHE_DIP_PCT) - 0.01
        asyncio.run(sol.evaluate_market_tick(trigger_price, "SOL/USD", 1, redis_stub))

        self.assertEqual(len(redis_stub.messages), 1)
        _, raw = redis_stub.messages[0]
        payload = json.loads(raw)
        self.assertEqual(payload["action"], "BUY")
        self.assertIn("DCA_Tranche2", payload["reason"])

    def test_no_more_tranches_at_max(self):
        """No BUY when tranche_count == MAX_TRANCHES."""
        redis_stub = _RedisStub()
        self._seed_warmup(100.0)
        self._seed_atr(1.0)
        sol.in_position            = True
        sol.entry_regime           = "TRENDING_DOWN"
        sol.tranche_count          = sol.MAX_TRANCHES
        sol.last_buy_price         = 100.0
        sol.open_position_quantity = "500"
        sol.average_cost           = 100.0
        sol.atr_at_entry           = 1.0   # stop = 100 - 2 = 98
        sol.global_sentiment_score = 0.0

        # Price dips enough for additional tranche but is at max
        trigger_price = 100.0 * (1.0 - sol.TRANCHE_DIP_PCT) - 0.01
        # Also above stop (98) and below take-profit → no signal at all
        asyncio.run(sol.evaluate_market_tick(trigger_price, "SOL/USD", 1, redis_stub))

        self.assertEqual(len(redis_stub.messages), 0)

    # ── Exit: ATR stop-loss ────────────────────────────────────────────────────
    def test_atr_stop_loss_triggers_sell(self):
        """Price below average_cost - ATR_STOPLOSS_K × atr_at_entry → ATR_StopLoss SELL."""
        redis_stub = _RedisStub()
        self._seed_warmup(100.0)
        sol.in_position            = True
        sol.open_position_quantity = "100"
        sol.average_cost           = 100.0
        sol.atr_at_entry           = 5.0
        sol.entry_regime           = "UNCLEAR"

        # Compute trigger price from the actual module constant (env may override default)
        stop_price = sol.average_cost - sol.ATR_STOPLOSS_K * sol.atr_at_entry
        asyncio.run(sol.evaluate_market_tick(stop_price - 0.01, "SOL/USD", 1, redis_stub))

        self.assertEqual(len(redis_stub.messages), 1)
        _, raw = redis_stub.messages[0]
        payload = json.loads(raw)
        self.assertEqual(payload["action"], "SELL")
        self.assertIn("ATR_StopLoss", payload["reason"])

    def test_atr_stop_loss_regime_agnostic(self):
        """ATR stop fires regardless of the current_regime."""
        for regime in ("RANGING", "TRENDING_UP", "TRENDING_DOWN", "UNCLEAR"):
            self.setUp()
            redis_stub = _RedisStub()
            self._seed_warmup(100.0)
            sol.current_regime         = regime
            sol.in_position            = True
            sol.open_position_quantity = "100"
            sol.average_cost           = 100.0
            sol.atr_at_entry           = 5.0
            sol.entry_regime           = regime

            stop_price = sol.average_cost - sol.ATR_STOPLOSS_K * sol.atr_at_entry
            asyncio.run(sol.evaluate_market_tick(stop_price - 0.01, "SOL/USD", 1, redis_stub))

            self.assertEqual(len(redis_stub.messages), 1, msg=f"regime={regime}")
            payload = json.loads(redis_stub.messages[0][1])
            self.assertIn("ATR_StopLoss", payload["reason"])

    # ── Exit: regime-specific take-profit ─────────────────────────────────────
    def test_ranging_take_profit_at_sma(self):
        """RANGING entry → TP fires when price >= max(sma20, avg × (1+BB_PROFIT_MARGIN))."""
        redis_stub = _RedisStub()
        self._seed_warmup(100.0)
        sol.in_position            = True
        sol.open_position_quantity = "100"
        sol.average_cost           = 97.0   # entry below SMA
        sol.atr_at_entry           = 0.5    # stop = 97 - 2×0.5 = 96 (well below)
        sol.entry_regime           = "RANGING"

        # sma20 ≈ 100; TP = max(100, 97×1.005) = 100
        asyncio.run(sol.evaluate_market_tick(100.5, "SOL/USD", 1, redis_stub))

        self.assertEqual(len(redis_stub.messages), 1)
        payload = json.loads(redis_stub.messages[0][1])
        self.assertEqual(payload["action"], "SELL")
        self.assertIn("BB_MeanReversion", payload["reason"])

    def test_breakout_take_profit(self):
        """TRENDING_UP entry → TP fires at average_cost × BREAKOUT_TP_MULT."""
        redis_stub = _RedisStub()
        self._seed_warmup(100.0)
        sol.in_position            = True
        sol.open_position_quantity = "100"
        sol.average_cost           = 100.0
        sol.atr_at_entry           = 0.5
        sol.entry_regime           = "TRENDING_UP"

        tp = 100.0 * sol.BREAKOUT_TP_MULT   # default 1.05 → 105
        asyncio.run(sol.evaluate_market_tick(tp + 0.01, "SOL/USD", 1, redis_stub))

        self.assertEqual(len(redis_stub.messages), 1)
        payload = json.loads(redis_stub.messages[0][1])
        self.assertIn("Breakout_TakeProfit", payload["reason"])

    def test_dca_take_profit(self):
        """DCA entry → TP fires at average_cost × (1 + PROFIT_MARGIN)."""
        redis_stub = _RedisStub()
        self._seed_warmup(100.0)
        sol.in_position            = True
        sol.open_position_quantity = "100"
        sol.average_cost           = 100.0
        sol.atr_at_entry           = 0.5
        sol.tranche_count          = 2
        sol.entry_regime           = "TRENDING_DOWN"

        tp = 100.0 * (1.0 + sol.PROFIT_MARGIN)
        asyncio.run(sol.evaluate_market_tick(tp + 0.01, "SOL/USD", 1, redis_stub))

        self.assertEqual(len(redis_stub.messages), 1)
        payload = json.loads(redis_stub.messages[0][1])
        self.assertIn("DCA_TakeProfit", payload["reason"])

    def test_wrong_entry_regime_does_not_trigger_tp_early(self):
        """BB TP threshold (SMA) is not used for TRENDING_UP entry; uses BREAKOUT_TP_MULT."""
        redis_stub = _RedisStub()
        self._seed_warmup(100.0)
        sol.in_position            = True
        sol.open_position_quantity = "100"
        sol.average_cost           = 100.0
        sol.atr_at_entry           = 0.5
        sol.entry_regime           = "TRENDING_UP"

        # Price at SMA (100) — would trigger BB TP but NOT breakout TP (needs 105)
        asyncio.run(sol.evaluate_market_tick(100.5, "SOL/USD", 1, redis_stub))

        self.assertEqual(len(redis_stub.messages), 0)

    # ── BUY confirmation ───────────────────────────────────────────────────────
    def test_buy_confirmation_first_tranche(self):
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

    def test_buy_confirmation_nth_tranche_weighted_average(self):
        """Nth BUY confirmation updates weighted average_cost correctly."""
        redis_stub = _RedisStub()
        sol.pending_action         = "BUY"
        sol.tranche_count          = 1
        sol.in_position            = True
        sol.open_position_quantity = "99"
        sol.average_cost           = 150.0
        sol.last_buy_price         = 140.0   # second tranche at lower price

        asyncio.run(sol.handle_execution_confirmation(json.dumps({
            "symbol":        "SOL/USD",
            "result":        "SUCCESS",
            "action":        "BUY",
            "effective_qty": 100.0,
        }), redis_stub))

        self.assertEqual(sol.tranche_count, 2)
        net_new   = int(100.0 * (1.0 - sol.TRADING_FEE_RATE))
        prev_qty  = 99
        new_total = prev_qty + net_new
        expected_avg = (150.0 * prev_qty + 140.0 * net_new) / new_total
        self.assertAlmostEqual(sol.average_cost, expected_avg, places=4)
        self.assertEqual(int(sol.open_position_quantity), new_total)

    def test_buy_failure_first_tranche_fully_reverts(self):
        """BUY failure on first tranche resets all position state."""
        redis_stub = _RedisStub()
        sol.pending_action = "BUY"
        sol.tranche_count  = 0
        sol.in_position    = False
        sol.average_cost   = 100.0
        sol.last_buy_price = 100.0
        sol.entry_regime   = "UNCLEAR"

        asyncio.run(sol.handle_execution_confirmation(json.dumps({
            "symbol": "SOL/USD",
            "result": "FAILURE",
            "action": "BUY",
        }), redis_stub))

        self.assertFalse(sol.in_position)
        self.assertEqual(sol.average_cost, 0.0)
        self.assertEqual(sol.last_buy_price, 0.0)
        self.assertEqual(sol.tranche_count, 0)
        self.assertEqual(sol.entry_regime, "")
        self.assertIsNone(sol.pending_action)

    def test_buy_failure_nth_tranche_preserves_position(self):
        """BUY failure on nth tranche preserves the existing position."""
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

        self.assertTrue(sol.in_position)
        self.assertEqual(sol.tranche_count, 1)
        self.assertAlmostEqual(sol.average_cost, 150.0)
        self.assertIsNone(sol.pending_action)

    # ── SELL confirmation ──────────────────────────────────────────────────────
    def test_sell_confirmation_resets_all_state(self):
        """Full SELL confirmation resets average_cost, tranche_count, entry_regime."""
        redis_stub = _RedisStub()
        sol.in_position            = True
        sol.open_position_quantity = "100.0000"
        sol.average_cost           = 100.0
        sol.atr_at_entry           = 5.0
        sol.tranche_count          = 3
        sol.last_buy_price         = 90.0
        sol.entry_regime           = "TRENDING_DOWN"
        sol.pending_action         = "SELL"
        sol.pending_sell_price     = 105.0

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
        self.assertEqual(sol.entry_regime, "")
        self.assertIsNone(sol.pending_action)

    def test_sell_confirmation_records_trade_return(self):
        """Positive SELL records a positive trade return in trade_returns deque."""
        redis_stub = _RedisStub()
        sol.in_position            = True
        sol.open_position_quantity = "100.0000"
        sol.average_cost           = 100.0
        sol.pending_action         = "SELL"
        sol.pending_sell_price     = 108.0   # 8% gain

        asyncio.run(sol.handle_execution_confirmation(json.dumps({
            "symbol":        "SOL/USD",
            "result":        "SUCCESS",
            "action":        "SELL",
            "effective_qty": 100.0,
        }), redis_stub))

        self.assertEqual(len(sol.trade_returns), 1)
        self.assertAlmostEqual(sol.trade_returns[0], 0.08, places=6)

    # ── Rolling Sharpe scalar ──────────────────────────────────────────────────
    def test_sharpe_scalar_insufficient_data_returns_one(self):
        """Fewer than 3 trades → scalar = 1.0."""
        sol.trade_returns.clear()
        sol.trade_returns.append(0.02)
        sol.trade_returns.append(0.03)
        self.assertAlmostEqual(sol._sharpe_scalar(), 1.0)

    def test_sharpe_scalar_positive_history(self):
        """Consistently positive returns with some variance → scalar >= 1.0."""
        sol.trade_returns.clear()
        # Use varied positive values so std > 0 and Sharpe = mu/std > 0
        for v in [0.03, 0.05, 0.04, 0.06, 0.05]:
            sol.trade_returns.append(v)
        scalar = sol._sharpe_scalar()
        self.assertGreaterEqual(scalar, 1.0)

    def test_sharpe_scalar_negative_history(self):
        """Consistent losses → scalar < 1.0."""
        sol.trade_returns.clear()
        for _ in range(5):
            sol.trade_returns.append(-0.03)
        scalar = sol._sharpe_scalar()
        self.assertLess(scalar, 1.0)

    # ── _sized_quantity ────────────────────────────────────────────────────────
    def test_sized_quantity_zero_price(self):
        """_sized_quantity(0, …) must not divide by zero → returns '0'."""
        self.assertEqual(sol._sized_quantity(0.0, 10000.0), "0")

    def test_sized_quantity_capped_by_allocation(self):
        """Quantity is capped at max_alloc_usd × scalar / price."""
        sol.atr_value = 0.0   # ATR not seeded → falls back to alloc cap
        sol.global_sentiment_score = 0.0   # scalar = 1.0
        result = sol._sized_quantity(100.0, 1000.0)
        # alloc_qty = floor(1000 × 1.0 / 100) = 10
        self.assertEqual(result, "10")

    # ── Pending suppression / symbol guard ───────────────────────────────────
    def test_no_signal_while_pending(self):
        """No new signals while pending_action is already set."""
        import time
        redis_stub = _RedisStub()
        self._seed_warmup(100.0)
        self._seed_atr(1.0)
        sol.pending_action         = "BUY"
        sol.pending_action_since   = time.time()   # fresh — prevents timeout path
        sol.current_regime         = "UNCLEAR"
        sol.global_sentiment_score = 0.0

        asyncio.run(sol.evaluate_market_tick(80.0, "SOL/USD", 1, redis_stub))

        self.assertEqual(len(redis_stub.messages), 0)

    def test_wrong_symbol_tick_is_ignored(self):
        """Ticks for other symbols are silently dropped."""
        redis_stub = _RedisStub()
        self._seed_warmup(100.0)
        self._seed_atr(1.0)
        asyncio.run(sol.evaluate_market_tick(80.0, "BTC/USD", 1, redis_stub))
        self.assertEqual(len(redis_stub.messages), 0)
        self.assertIsNone(sol.pending_action)

    def test_confirmation_wrong_symbol_ignored(self):
        """Confirmations for other symbols do not touch SOL state."""
        redis_stub = _RedisStub()
        sol.pending_action = "BUY"

        asyncio.run(sol.handle_execution_confirmation(json.dumps({
            "symbol":        "BTC/USD",
            "result":        "SUCCESS",
            "action":        "BUY",
            "effective_qty": 100.0,
        }), redis_stub))

        self.assertEqual(sol.pending_action, "BUY")   # unchanged

    # ── Sentiment handler ──────────────────────────────────────────────────────
    def test_handle_sentiment_update(self):
        """handle_sentiment_update stores the compound_score."""
        sol.handle_sentiment_update('{"compound_score": -0.15}')
        self.assertAlmostEqual(sol.global_sentiment_score, -0.15)

    def test_handle_malformed_sentiment_does_not_crash(self):
        """Malformed sentiment JSON is silently skipped."""
        original = sol.global_sentiment_score
        sol.handle_sentiment_update("not-json")
        self.assertEqual(sol.global_sentiment_score, original)

    # ── Sentiment scalar ──────────────────────────────────────────────────────
    def test_sentiment_scalar_severe_fear(self):
        """Score below -0.30 → SEVERE_FEAR scalar."""
        sol.global_sentiment_score = -0.50
        self.assertAlmostEqual(sol._sentiment_scalar(), sol.SENTIMENT_SCALAR_SEVERE_FEAR)

    def test_sentiment_scalar_hype(self):
        """Score above 0.25 → HYPE scalar."""
        sol.global_sentiment_score = 0.50
        self.assertAlmostEqual(sol._sentiment_scalar(), sol.SENTIMENT_SCALAR_HYPE)


if __name__ == "__main__":
    unittest.main()
