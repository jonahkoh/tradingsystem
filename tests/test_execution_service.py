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
    def __init__(self, response):
        self.response = response
        self.last_url = None
        self.last_headers = None
        self.last_content = None

    def post(self, url, content, headers):
        self.last_url = url
        self.last_headers = headers
        self.last_content = content
        return self.response


class _ThrottleNoop:
    def wait(self):
        return None


class ExecutionServiceTests(unittest.TestCase):
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
        http_stub = _HttpStub(_RespStub(400, {"Success": False, "ErrMsg": "bad order"}))
        result = execution.execute_trade_signal(
            {"action": "SELL", "symbol": "BTC/USD", "quantity": "1", "type": "MARKET"},
            http_stub,
            redis_stub,
            _ThrottleNoop(),
        )

        self.assertEqual(result["result"], "FAILED")
        self.assertEqual(result["status_code"], 400)
        self.assertEqual(result["raw_error"], "bad order")
        self.assertEqual(len(redis_stub.messages), 1)


if __name__ == "__main__":
    unittest.main()
