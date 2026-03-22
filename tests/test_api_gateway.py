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

    # ── PnL tests (unit-test _compute_pnl directly — no HTTP, no Redis) ────────

    def _write_csv(self, tmp_dir: str, rows: list[str]) -> Path:
        csv_file = Path(tmp_dir) / "execution_log.csv"
        header = "Timestamp,Action,Pair,Price,Quantity,Requested_Qty,Effective_Qty,Strategy_Signal,API_Status\n"
        csv_file.write_text(header + "".join(rows), encoding="utf-8")
        return csv_file

    def test_pnl_no_trades_returns_zeros(self):
        with tempfile.TemporaryDirectory() as tmp:
            csv_file = Path(tmp) / "missing.csv"
            result = api_gateway._compute_pnl(csv_file, fee_rate=0.001)
        self.assertEqual(result["realized_pnl_usd"], 0.0)
        self.assertEqual(result["total_closed_trades"], 0)
        self.assertEqual(result["per_symbol"], {})

    def test_pnl_single_winning_round_trip(self):
        """BUY at 100 → SELL at 110: net PnL should be positive."""
        with tempfile.TemporaryDirectory() as tmp:
            csv_file = self._write_csv(tmp, [
                "2026-01-01 00:00:00,BUY,BTC/USD,100.0,1.0,1.0,1.0,alpha,200 SUCCESS\n",
                "2026-01-01 01:00:00,SELL,BTC/USD,110.0,1.0,1.0,1.0,alpha,200 SUCCESS\n",
            ])
            result = api_gateway._compute_pnl(csv_file, fee_rate=0.001)

        # sell_proceeds = 110 × (1 - 0.001) = 109.89
        # buy_cost      = 100 × (1 + 0.001) = 100.10
        # pnl           = 9.79
        self.assertGreater(result["realized_pnl_usd"], 0)
        self.assertEqual(result["winning_trades"], 1)
        self.assertEqual(result["losing_trades"], 0)
        self.assertEqual(result["total_closed_trades"], 1)
        self.assertAlmostEqual(result["realized_pnl_usd"], 9.78, places=1)

    def test_pnl_single_losing_round_trip(self):
        """BUY at 100 → SELL at 90: net PnL should be negative."""
        with tempfile.TemporaryDirectory() as tmp:
            csv_file = self._write_csv(tmp, [
                "2026-01-01 00:00:00,BUY,BTC/USD,100.0,1.0,1.0,1.0,alpha,200 SUCCESS\n",
                "2026-01-01 01:00:00,SELL,BTC/USD,90.0,1.0,1.0,1.0,alpha,200 SUCCESS\n",
            ])
            result = api_gateway._compute_pnl(csv_file, fee_rate=0.001)

        self.assertLess(result["realized_pnl_usd"], 0)
        self.assertEqual(result["winning_trades"], 0)
        self.assertEqual(result["losing_trades"], 1)

    def test_pnl_fifo_lot_matching(self):
        """Two BUYs at different prices: FIFO means the first BUY is matched first."""
        with tempfile.TemporaryDirectory() as tmp:
            csv_file = self._write_csv(tmp, [
                "2026-01-01 00:00:00,BUY,BTC/USD,100.0,1.0,1.0,1.0,alpha,200 SUCCESS\n",
                "2026-01-01 00:01:00,BUY,BTC/USD,200.0,1.0,1.0,1.0,alpha,200 SUCCESS\n",
                "2026-01-01 01:00:00,SELL,BTC/USD,150.0,1.0,1.0,1.0,alpha,200 SUCCESS\n",
            ])
            result = api_gateway._compute_pnl(csv_file, fee_rate=0.001)

        # SELL matches the first BUY (100 → 150): winning trade.
        self.assertEqual(result["winning_trades"], 1)
        # Second BUY at 200 remains as an open lot.
        btc = result["per_symbol"]["BTC/USD"]
        self.assertEqual(len(btc["open_lots"]), 1)
        self.assertAlmostEqual(btc["open_lots"][0]["price"], 200.0, places=4)

    def test_pnl_skips_failed_rows(self):
        """Only SUCCESS rows contribute to PnL."""
        with tempfile.TemporaryDirectory() as tmp:
            csv_file = self._write_csv(tmp, [
                "2026-01-01 00:00:00,BUY,BTC/USD,100.0,1.0,1.0,1.0,alpha,400 FAILED\n",
                "2026-01-01 01:00:00,SELL,BTC/USD,110.0,1.0,1.0,1.0,alpha,400 FAILED\n",
            ])
            result = api_gateway._compute_pnl(csv_file, fee_rate=0.001)

        self.assertEqual(result["realized_pnl_usd"], 0.0)
        self.assertEqual(result["total_closed_trades"], 0)

    def test_pnl_skips_startup_liquidation_rows(self):
        """SYSTEM_REBOOT_LIQUIDATION rows must be excluded from PnL."""
        with tempfile.TemporaryDirectory() as tmp:
            csv_file = self._write_csv(tmp, [
                "2026-01-01 00:00:00,SELL,BTC/USD,100.0,1.0,1.0,1.0,SYSTEM_REBOOT_LIQUIDATION,200 SUCCESS\n",
            ])
            result = api_gateway._compute_pnl(csv_file, fee_rate=0.001)

        self.assertEqual(result["total_closed_trades"], 0)
        self.assertEqual(result["per_symbol"], {})

    def test_pnl_multi_symbol(self):
        """PnL is tracked independently per symbol."""
        with tempfile.TemporaryDirectory() as tmp:
            csv_file = self._write_csv(tmp, [
                "2026-01-01 00:00:00,BUY,BTC/USD,100.0,1.0,1.0,1.0,alpha,200 SUCCESS\n",
                "2026-01-01 00:00:00,BUY,XRP/USD,1.0,100.0,100.0,100.0,beta,200 SUCCESS\n",
                "2026-01-01 01:00:00,SELL,BTC/USD,110.0,1.0,1.0,1.0,alpha,200 SUCCESS\n",
                "2026-01-01 01:00:00,SELL,XRP/USD,0.9,100.0,100.0,100.0,beta,200 SUCCESS\n",
            ])
            result = api_gateway._compute_pnl(csv_file, fee_rate=0.001)

        self.assertIn("BTC/USD", result["per_symbol"])
        self.assertIn("XRP/USD", result["per_symbol"])
        self.assertGreater(result["per_symbol"]["BTC/USD"]["realized_pnl_usd"], 0)
        self.assertLess(result["per_symbol"]["XRP/USD"]["realized_pnl_usd"], 0)

    def test_pnl_endpoint_returns_200(self):
        """GET /api/pnl returns 200 with expected keys even when no CSV exists."""
        original_csv = api_gateway.CSV_FILE
        api_gateway.CSV_FILE = Path("/tmp/nonexistent_log.csv")
        try:
            with TestClient(api_gateway.app) as client:
                resp = client.get("/api/pnl")
        finally:
            api_gateway.CSV_FILE = original_csv

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertIn("realized_pnl_usd", body)
        self.assertIn("per_symbol", body)
        self.assertIn("total_closed_trades", body)

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
