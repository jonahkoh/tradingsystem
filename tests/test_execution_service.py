from __future__ import annotations

import hashlib
import hmac
import json
import os
import tempfile
import unittest

from tests.test_loader import load_module


execution = load_module("execution_service_mod", "services/execution-manager/execution_service.py")


class _RedisStub:
    def __init__(self):
        self.messages = []
        self.store = {}

    def publish(self, channel, payload):
        self.messages.append((channel, payload))

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value):
        self.store[key] = value

    def hset(self, key, mapping):
        self.store[key] = dict(mapping)


class _RespStub:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


class _HttpStub:
    def __init__(self, response, balance_response=None):
        self.response = response
        self.balance_response = balance_response or _RespStub(200, {"Data": [{"Asset": "BTC", "Available": "100"}]})
        self.last_url = None
        self.last_headers = None
        self.last_content = None
        self.last_get_url = None
        self.last_get_headers = None
        self.last_get_params = None

    def post(self, url, content, headers):
        self.last_url = url
        self.last_headers = headers
        self.last_content = content
        return self.response

    def get(self, url, params, headers):
        self.last_get_url = url
        self.last_get_headers = headers
        self.last_get_params = params
        return self.balance_response


class _ThrottleNoop:
    def wait(self):
        return None


class ExecutionServiceTests(unittest.TestCase):
    def test_query_orders_no_match_returns_empty_list(self):
        class _HttpNoMatch:
            def post(self, url, content, headers):
                return _RespStub(200, {"Success": False, "ErrMsg": "no order matched"})

        orders = execution._query_orders(_HttpNoMatch(), pair="BTC/USD")
        self.assertEqual(orders, [])

    def test_clear_ledger_csv_resets_last_sync_cursor(self):
        redis_stub = _RedisStub()
        redis_stub.set(execution.HISTORY_LAST_SYNC_ORDER_ID_KEY, "123")

        old_file = execution.LOG_FILE
        old_clear = execution.CLEAR_LEDGER_CSV_ON_STARTUP
        try:
            with tempfile.TemporaryDirectory() as tmp:
                execution.LOG_FILE = os.path.join(tmp, "execution_log.csv")
                execution.CLEAR_LEDGER_CSV_ON_STARTUP = True
                execution._clear_ledger_csv_file(redis_stub)

                self.assertTrue(os.path.exists(execution.LOG_FILE))
                self.assertEqual(redis_stub.get(execution.HISTORY_LAST_SYNC_ORDER_ID_KEY), "0")
        finally:
            execution.LOG_FILE = old_file
            execution.CLEAR_LEDGER_CSV_ON_STARTUP = old_clear

    def test_estimate_entry_price_from_orders_tracks_weighted_average(self):
        orders = [
            {
                "OrderID": 1,
                "CreateTimestamp": 1,
                "Status": "FILLED",
                "Side": "BUY",
                "FilledQuantity": 2,
                "FilledAverPrice": 100,
            },
            {
                "OrderID": 2,
                "CreateTimestamp": 2,
                "Status": "FILLED",
                "Side": "BUY",
                "FilledQuantity": 2,
                "FilledAverPrice": 200,
            },
            {
                "OrderID": 3,
                "CreateTimestamp": 3,
                "Status": "FILLED",
                "Side": "SELL",
                "FilledQuantity": 1,
                "FilledAverPrice": 210,
            },
        ]

        # Remaining qty is 3, avg cost remains weighted from buys: 150.
        self.assertEqual(execution._estimate_entry_price_from_orders(orders), 150.0)

    def test_strategy_state_from_inventory_uses_pair_precision(self):
        btc_state = execution._strategy_state_from_inventory("BTC/USD", 0.123456, 40000.0, 40000.0)
        self.assertEqual(btc_state["open_position_quantity"], "0.1234")
        self.assertEqual(btc_state["in_position"], "True")

        sol_state = execution._strategy_state_from_inventory("SOL/USD", 17.9, 150.0, 150.0)
        self.assertEqual(sol_state["open_position_quantity"], "17")
        self.assertIn("atr_at_entry", sol_state)

        fet_state = execution._strategy_state_from_inventory("FET/USD", 12.34, 1.2, 1.2)
        self.assertEqual(fet_state["open_position_quantity"], "12.3")

    def test_strategy_state_from_inventory_marks_dust_notional_as_flat(self):
        old_threshold = execution.MIN_HOLD_NOTIONAL_USD
        try:
            execution.MIN_HOLD_NOTIONAL_USD = 15.0
            # 0.0002 BTC at $68,000 ~= $13.60, below threshold => treat as flat.
            btc_state = execution._strategy_state_from_inventory("BTC/USD", 0.0002, 68740.02, 68000.0)
            self.assertEqual(btc_state["open_position_quantity"], "0.0002")
            self.assertEqual(btc_state["in_position"], "False")
        finally:
            execution.MIN_HOLD_NOTIONAL_USD = old_threshold

    def test_build_history_result_payload_maps_roostoo_order(self):
        payload = execution._build_history_result_payload(
            {
                "Pair": "BTC/USD",
                "OrderID": 99,
                "Status": "FILLED",
                "Side": "BUY",
                "Price": 50000,
                "Quantity": 1.5,
                "FilledQuantity": 1.25,
                "FilledAverPrice": 49900,
                "CreateTimestamp": 1700000000000,
                "FinishTimestamp": 1700000005000,
            }
        )

        self.assertEqual(payload["result"], "SUCCESS")
        self.assertEqual(payload["symbol"], "BTC/USD")
        self.assertEqual(payload["requested_qty"], 1.5)
        self.assertEqual(payload["effective_qty"], 1.25)
        self.assertEqual(payload["price"], 49900.0)
        self.assertEqual(payload["strategy_signal"], "ROOSTOO_HISTORY_SYNC")

    def test_sync_order_history_to_ledger_publishes_new_orders_only(self):
        redis_stub = _RedisStub()
        redis_stub.set(execution.HISTORY_LAST_SYNC_ORDER_ID_KEY, "10")

        class _HttpHistory:
            def post(self, url, content, headers):
                return _RespStub(
                    200,
                    {
                        "Success": True,
                        "OrderMatched": [
                            {"OrderID": 9, "Pair": "BTC/USD", "Status": "FILLED", "Side": "BUY", "Quantity": 1, "FilledQuantity": 1},
                            {"OrderID": 11, "Pair": "BTC/USD", "Status": "FILLED", "Side": "SELL", "Quantity": 1, "FilledQuantity": 1},
                        ],
                    },
                )

        old_limit = execution.QUERY_ORDER_LIMIT
        try:
            execution.QUERY_ORDER_LIMIT = 100
            execution._sync_order_history_to_ledger(_HttpHistory(), redis_stub)
        finally:
            execution.QUERY_ORDER_LIMIT = old_limit

        self.assertEqual(len(redis_stub.messages), 1)
        self.assertEqual(redis_stub.store[execution.HISTORY_LAST_SYNC_ORDER_ID_KEY], "11")

    def test_extract_available_balance_supports_roostoo_wallet_shape(self):
        payload = {
            "Success": True,
            "ErrMsg": "",
            "Wallet": {
                "BTC": {"Free": 0.454878, "Lock": 0.555},
                "USD": {"Free": 98389854.152001, "Lock": 1601798.197999},
            },
        }
        self.assertEqual(execution._extract_available_balance(payload, "BTC"), 0.454878)

    def test_extract_available_balance_supports_roostoo_spotwallet_shape(self):
        payload = {
            "Success": True,
            "ErrMsg": "",
            "SpotWallet": {
                "BTC": {"Free": 0.5754, "Lock": 0},
                "USD": {"Free": 959734.58, "Lock": 0},
            },
            "MarginWallet": {},
        }
        self.assertEqual(execution._extract_available_balance(payload, "BTC"), 0.5754)

    def test_extract_locked_balance_supports_roostoo_wallet_shape(self):
        payload = {
            "Success": True,
            "ErrMsg": "",
            "Wallet": {
                "BTC": {"Free": 0.0, "Lock": 0.555},
                "USD": {"Free": 100.0, "Lock": 0.0},
            },
        }
        self.assertEqual(execution._extract_locked_balance(payload, "BTC"), 0.555)

    def test_extract_locked_balance_supports_roostoo_spotwallet_shape(self):
        payload = {
            "Success": True,
            "ErrMsg": "",
            "SpotWallet": {
                "BTC": {"Free": 0.0, "Lock": 0.5754},
            },
            "MarginWallet": {},
        }
        self.assertEqual(execution._extract_locked_balance(payload, "BTC"), 0.5754)

    def test_base_asset_from_pair_supports_slashed_and_compact_symbols(self):
        self.assertEqual(execution._base_asset_from_pair("BTC/USD"), "BTC")
        self.assertEqual(execution._base_asset_from_pair("BTCUSDT"), "BTC")
        self.assertEqual(execution._base_asset_from_pair("ETHUSDC"), "ETH")

    def test_get_balance_snapshot_reads_free_and_locked(self):
        class _HttpLocal:
            def get(self, url, params, headers):
                return _RespStub(
                    200,
                    {
                        "Success": True,
                        "Wallet": {
                            "BTC": {"Free": 0.2, "Lock": 0.1},
                        },
                    },
                )

        free_qty, locked_qty = execution._get_balance_snapshot(_HttpLocal(), "BTC/USD")
        self.assertEqual(free_qty, 0.2)
        self.assertEqual(locked_qty, 0.1)

    def test_sorted_params_string_orders_keys(self):
        params = {"side": "BUY", "pair": "BTC/USD", "timestamp": 1, "type": "MARKET", "quantity": "1"}
        result = execution._sorted_params_string(params)
        self.assertEqual(result, "pair=BTC/USD&quantity=1&side=BUY&timestamp=1&type=MARKET")

    def test_sign_message_matches_hashlib(self):
        message = "pair=BTC/USD&quantity=1&side=BUY&timestamp=1580774512000&type=MARKET"
        expected = hmac.new(b"secret", message.encode("utf-8"), hashlib.sha256).hexdigest()
        self.assertEqual(execution._sign_message("secret", message), expected)

    def test_sign_message_matches_roostoo_readme_vector(self):
        message = "pair=BNB/USD&quantity=2000&side=BUY&timestamp=1580774512000&type=MARKET"
        secret = "S1XP1e3UZj6A7H5fATj0jNhqPxxdSJYdInClVN65XAbvqqMKjVHjA7PZj4W12oep"
        expected = "20b7fd5550b67b3bf0c1684ed0f04885261db8fdabd38611e9e6af23c19b7fff"
        self.assertEqual(execution._sign_message(secret, message), expected)

    def test_validate_signal_rejects_bad_action(self):
        with self.assertRaises(ValueError):
            execution._validate_signal({"action": "HOLD", "symbol": "BTC/USD", "quantity": "1", "type": "MARKET"})

    def test_execute_trade_signal_success(self):
        redis_stub = _RedisStub()
        http_stub = _HttpStub(_RespStub(200, {"Success": True, "Data": {"OrderID": "abc-123"}}))
        result = execution.execute_trade_signal(
            {"action": "BUY", "symbol": "BTC/USD", "quantity": "1", "type": "MARKET"},
            http_stub,
            redis_stub,
            _ThrottleNoop(),
        )

        self.assertEqual(result["result"], "SUCCESS")
        self.assertEqual(result["status_code"], 200)
        self.assertEqual(result["order_id"], "abc-123")
        self.assertEqual(result["requested_qty"], 1.0)
        self.assertEqual(result["effective_qty"], 1.0)
        self.assertEqual(len(redis_stub.messages), 1)
        self.assertIn("RST-API-KEY", http_stub.last_headers)
        self.assertIn("MSG-SIGNATURE", http_stub.last_headers)
        self.assertEqual(http_stub.last_headers["Content-Type"], "application/x-www-form-urlencoded")

    def test_execute_trade_signal_success_with_order_detail(self):
        redis_stub = _RedisStub()
        http_stub = _HttpStub(_RespStub(200, {"Success": True, "OrderDetail": {"OrderID": 81}}))
        result = execution.execute_trade_signal(
            {"action": "BUY", "symbol": "BTC/USD", "quantity": "1", "type": "MARKET"},
            http_stub,
            redis_stub,
            _ThrottleNoop(),
        )

        self.assertEqual(result["result"], "SUCCESS")
        self.assertEqual(result["order_id"], 81)
        self.assertIsInstance(result["order_details"], dict)

    def test_execute_trade_signal_failure(self):
        redis_stub = _RedisStub()
        http_stub = _HttpStub(
            _RespStub(400, {"Success": False, "ErrMsg": "bad order"}),
            balance_response=_RespStub(200, {"Data": [{"Asset": "BTC", "Available": "1.0"}]}),
        )
        result = execution.execute_trade_signal(
            {"action": "SELL", "symbol": "BTC/USD", "quantity": "1", "type": "MARKET"},
            http_stub,
            redis_stub,
            _ThrottleNoop(),
        )

        self.assertEqual(result["result"], "FAILED")
        self.assertEqual(result["status_code"], 400)
        self.assertEqual(result["raw_error"], "bad order")
        self.assertEqual(result["requested_qty"], 1.0)
        self.assertEqual(result["effective_qty"], 1.0 - execution.SELL_QTY_EPSILON)
        self.assertEqual(len(redis_stub.messages), 1)

    def test_execute_trade_signal_sell_is_clamped_to_available(self):
        redis_stub = _RedisStub()
        http_stub = _HttpStub(
            _RespStub(200, {"Success": True, "Data": {"OrderID": "sell-1"}}),
            balance_response=_RespStub(200, {"Data": [{"Asset": "BTC", "Available": "0.284990"}]}),
        )

        result = execution.execute_trade_signal(
            {"action": "SELL", "symbol": "BTC/USD", "quantity": "0.28500000", "type": "MARKET"},
            http_stub,
            redis_stub,
            _ThrottleNoop(),
        )

        self.assertEqual(result["result"], "SUCCESS")
        self.assertEqual(result["requested_qty"], 0.285)
        self.assertEqual(result["effective_qty"], 0.28499 - execution.SELL_QTY_EPSILON)
        self.assertIn("quantity=0.28498999", http_stub.last_content)

    def test_execute_trade_signal_sell_skips_when_below_min_qty(self):
        redis_stub = _RedisStub()
        http_stub = _HttpStub(
            _RespStub(200, {"Success": True, "Data": {"OrderID": "unused"}}),
            balance_response=_RespStub(200, {"Data": [{"Asset": "BTC", "Available": "0.00005000"}]}),
        )

        old_min = execution.MIN_TRADABLE_QTY
        try:
            execution.MIN_TRADABLE_QTY = 0.0001
            result = execution.execute_trade_signal(
                {"action": "SELL", "symbol": "BTC/USD", "quantity": "0.00100000", "type": "MARKET"},
                http_stub,
                redis_stub,
                _ThrottleNoop(),
            )
        finally:
            execution.MIN_TRADABLE_QTY = old_min

        self.assertEqual(result["result"], "FAILED")
        self.assertIn("sell skipped", result["raw_error"])
        self.assertIsNone(http_stub.last_content)

    def test_execute_trade_signal_sell_all_uses_available_balance(self):
        redis_stub = _RedisStub()
        http_stub = _HttpStub(
            _RespStub(200, {"Success": True, "Data": {"OrderID": "sell-all"}}),
            balance_response=_RespStub(200, {"Data": [{"Asset": "BTC", "Available": "0.20000000"}]}),
        )

        result = execution.execute_trade_signal(
            {"action": "SELL", "symbol": "BTC/USD", "quantity": "ALL", "type": "MARKET"},
            http_stub,
            redis_stub,
            _ThrottleNoop(),
        )

        self.assertEqual(result["result"], "SUCCESS")
        self.assertEqual(result["requested_qty"], 0.2)
        self.assertEqual(result["effective_qty"], 0.2)

    def test_execute_trade_signal_buy_skips_when_below_min_qty(self):
        redis_stub = _RedisStub()
        http_stub = _HttpStub(_RespStub(200, {"Success": True, "Data": {"OrderID": "unused"}}))

        old_min = execution.MIN_TRADABLE_QTY
        try:
            execution.MIN_TRADABLE_QTY = 0.1
            result = execution.execute_trade_signal(
                {"action": "BUY", "symbol": "BTC/USD", "quantity": "0.00001", "type": "MARKET"},
                http_stub,
                redis_stub,
                _ThrottleNoop(),
            )
        finally:
            execution.MIN_TRADABLE_QTY = old_min

        self.assertEqual(result["result"], "FAILED")
        self.assertIn("order skipped", result["raw_error"])
        self.assertIsNone(http_stub.last_content)


if __name__ == "__main__":
    unittest.main()
