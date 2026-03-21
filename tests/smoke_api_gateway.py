from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
from pathlib import Path

from fastapi.testclient import TestClient

from tests.test_loader import load_module


api_gateway = load_module(
    "api_gateway_smoke_mod",
    "services/api-gateway/api_gateway.py",
    add_sys_path="services/api-gateway",
)


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


class _RedisSmokeStub:
    def __init__(self, pubsub_messages=None):
        self.messages = []
        self._pubsub = _PubSubStub(pubsub_messages)

    async def publish(self, channel, payload):
        self.messages.append((channel, payload))
        return 1

    def pubsub(self, ignore_subscribe_messages=True):
        return self._pubsub

    async def aclose(self):
        return None


def run_smoke_test() -> int:
    pubsub_messages = [
        {
            "type": "message",
            "channel": "market_data",
            "data": '{"symbol":"BTC/USD","mid_price":70000.0}',
        }
    ]

    with tempfile.TemporaryDirectory() as tmp_dir:
        csv_file = Path(tmp_dir) / "execution_log.csv"
        csv_file.write_text(
            (
                "Timestamp,Action,Pair,Price,Quantity,Strategy_Signal,API_Status,Latency_ms,Raw_Error\n"
                "2026-03-21 12:00:00,BUY,BTC/USD,70000.0,1.0,smoke,200 SUCCESS,42,\n"
            ),
            encoding="utf-8",
        )

        original_csv = api_gateway.CSV_FILE
        api_gateway.CSV_FILE = csv_file
        try:
            with TestClient(api_gateway.app) as client:
                stub = _RedisSmokeStub(pubsub_messages=pubsub_messages)
                api_gateway.app.state.redis = stub

                kill_resp = client.post("/api/kill")
                assert kill_resp.status_code == 200
                assert json.loads(stub.messages[-1][1]) == {"command": "HALT"}

                resume_resp = client.post("/api/resume")
                assert resume_resp.status_code == 200
                assert json.loads(stub.messages[-1][1]) == {"command": "START"}

                trades_resp = client.get("/api/trades")
                assert trades_resp.status_code == 200
                assert len(trades_resp.json()) == 1

                with client.websocket_connect("/ws/stream") as ws:
                    stream_payload = ws.receive_json()
                assert stream_payload["channel"] == "market_data"

                print("PASS: kill/resume publishes control commands")
                print("PASS: trades endpoint reads CSV")
                print("PASS: websocket forwards redis messages")
        finally:
            api_gateway.CSV_FILE = original_csv

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="API Gateway quick smoke checks")
    parser.parse_args()
    return run_smoke_test()


if __name__ == "__main__":
    raise SystemExit(main())
