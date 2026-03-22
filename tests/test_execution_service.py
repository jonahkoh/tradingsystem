from __future__ import annotations

import hashlib
import hmac
import json
import unittest

from tests.test_loader import load_module


execution = load_module("execution_service_mod", "services/execution-manager/execution_service.py")


class _RedisStub:
    def __init__(self):
        self.messages = []

    def publish(self, channel, payload):
        self.messages.append((channel, payload))


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


if __name__ == "__main__":
    unittest.main()
