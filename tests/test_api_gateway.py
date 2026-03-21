from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from tests.test_loader import load_module


api_gateway = load_module("api_gateway_mod", "services/api-gateway/api_gateway.py", add_sys_path="services/api-gateway")


class _PubSubStub:
    def __init__(self, messages=None):
        self.messages = list(messages or [])

    async def subscribe(self, *channels):
        return None

    async def unsubscribe(self, *channels):
        return None

    async def aclose(self):
        return None

    async def get_message(self, timeout=1.0):
        if self.messages:
            return self.messages.pop(0)
        await asyncio.sleep(0)
        return None


class _RedisStub:
    def __init__(self, fail=False, pubsub_messages=None):
        self.fail = fail
        self.messages = []
        self._pubsub = _PubSubStub(pubsub_messages)

    async def publish(self, channel, payload):
        if self.fail:
            raise api_gateway.aioredis.RedisError("redis down")
        self.messages.append((channel, payload))
        return 1

    def pubsub(self, ignore_subscribe_messages=True):
        return self._pubsub

    async def aclose(self):
        return None


class ApiGatewayTests(unittest.TestCase):
    def test_health_endpoint(self):
        with TestClient(api_gateway.app) as client:
            resp = client.get("/health")
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json()["status"], "online")

    def test_kill_endpoint_publishes_halt(self):
        with TestClient(api_gateway.app) as client:
            stub = _RedisStub()
            api_gateway.app.state.redis = stub

            resp = client.post("/api/kill")

            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json()["command"], "HALT")
            self.assertEqual(len(stub.messages), 1)
            self.assertEqual(stub.messages[0][0], api_gateway.CONTROL_CHANNEL)
            self.assertEqual(json.loads(stub.messages[0][1]), {"command": "HALT"})

    def test_resume_endpoint_publishes_start(self):
        with TestClient(api_gateway.app) as client:
            stub = _RedisStub()
            api_gateway.app.state.redis = stub

            resp = client.post("/api/resume")

            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json()["command"], "START")
            self.assertEqual(len(stub.messages), 1)
            self.assertEqual(json.loads(stub.messages[0][1]), {"command": "START"})

    def test_kill_endpoint_redis_error(self):
        with TestClient(api_gateway.app) as client:
            stub = _RedisStub(fail=True)
            api_gateway.app.state.redis = stub

            resp = client.post("/api/kill")

            self.assertEqual(resp.status_code, 503)

    def test_trades_reads_latest_rows(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_file = Path(tmp_dir) / "execution_log.csv"
            csv_file.write_text(
                (
                    "Timestamp,Action,Pair,Price,Quantity,Strategy_Signal,API_Status\n"
                    "2026-03-21 11:00:00,BUY,BTC/USD,1.0,2.0,alpha,200 SUCCESS\n"
                ),
                encoding="utf-8",
            )

            original_csv = api_gateway.CSV_FILE
            api_gateway.CSV_FILE = csv_file
            try:
                with TestClient(api_gateway.app) as client:
                    resp = client.get("/api/trades")
            finally:
                api_gateway.CSV_FILE = original_csv

            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(len(body), 1)
            self.assertEqual(body[0]["Action"], "BUY")
            self.assertIn("Latency_ms", body[0])
            self.assertIn("Raw_Error", body[0])

    def test_ws_stream_forwards_message(self):
        pubsub_messages = [
            {
                "type": "message",
                "channel": "trade_signals",
                "data": '{"action":"BUY"}',
            }
        ]

        with TestClient(api_gateway.app) as client:
            stub = _RedisStub(pubsub_messages=pubsub_messages)
            api_gateway.app.state.redis = stub

            with client.websocket_connect("/ws/stream") as ws:
                payload = ws.receive_json()

            self.assertEqual(payload["channel"], "trade_signals")
            self.assertEqual(payload["data"], {"action": "BUY"})

    @patch.object(api_gateway.httpx, "AsyncClient")
    def test_balance_endpoint_signs_request(self, async_client_cls):
        captured_kwargs = {}

        class _FakeResponse:
            status_code = 200

            @staticmethod
            def json():
                return {"Success": True, "Balance": []}

        class _FakeAsyncClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def get(self, url, **kwargs):
                captured_kwargs.update(kwargs)
                return _FakeResponse()

        async_client_cls.return_value = _FakeAsyncClient()

        with patch.object(api_gateway, "API_KEY", "k"), patch.object(api_gateway, "SECRET_KEY", "s"):
            with TestClient(api_gateway.app) as client:
                resp = client.get("/api/balance")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["Success"], True)
        self.assertIn("headers", captured_kwargs)
        self.assertIn("MSG-SIGNATURE", captured_kwargs["headers"])
        self.assertIn("params", captured_kwargs)
        self.assertIn("timestamp", captured_kwargs["params"])


if __name__ == "__main__":
    unittest.main()
