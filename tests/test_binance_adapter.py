"""Unit tests for the Binance adapter modules (feed-handler and execution-manager)."""

import sys
import os
import unittest
from unittest.mock import patch, MagicMock

FEED_ADAPTER_DIR = os.path.join(os.path.dirname(__file__), "..", "services", "feed-handler")
EXEC_ADAPTER_DIR = os.path.join(os.path.dirname(__file__), "..", "services", "execution-manager")

sys.path.insert(0, os.path.abspath(FEED_ADAPTER_DIR))
sys.path.insert(0, os.path.abspath(EXEC_ADAPTER_DIR))

os.environ.setdefault("BINANCE_API_KEY", "test_key")
os.environ.setdefault("BINANCE_API_SECRET", "test_secret")


class TestPairMapping(unittest.TestCase):

    def test_feed_adapter_pair_map(self):
        from binance_adapter import PAIR_MAP
        self.assertEqual(PAIR_MAP["BTC/USD"], "BTCUSDT")
        self.assertEqual(PAIR_MAP["ETH/USD"], "ETHUSDT")
        self.assertEqual(PAIR_MAP["SOL/USD"], "SOLUSDT")
        self.assertEqual(PAIR_MAP["XRP/USD"], "XRPUSDT")
        self.assertEqual(PAIR_MAP["FET/USD"], "FETUSDT")

    def test_to_binance_symbol_valid(self):
        from binance_adapter import _to_binance_symbol
        self.assertEqual(_to_binance_symbol("BTC/USD"), "BTCUSDT")
        self.assertEqual(_to_binance_symbol("sol/usd"), "SOLUSDT")

    def test_to_binance_symbol_invalid(self):
        from binance_adapter import _to_binance_symbol
        with self.assertRaises(ValueError):
            _to_binance_symbol("DOGE/USD")


class TestExecAdapterResponseNormalization(unittest.TestCase):

    @patch("binance_adapter._get_client")
    def test_place_order_normalises_fields(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_client.create_order.return_value = {
            "orderId": 12345,
            "status": "FILLED",
            "side": "BUY",
            "origQty": "0.001",
            "executedQty": "0.001",
            "price": "0",
            "fills": [{"price": "60000.00", "qty": "0.001", "commission": "0.00001", "commissionAsset": "BTC"}],
        }

        import binance_adapter as ba
        result = ba.place_order("BTC/USD", "BUY", "0.001", "MARKET")

        self.assertEqual(result["result"], "SUCCESS")
        self.assertEqual(result["order_id"], 12345)
        self.assertEqual(result["side"], "BUY")
        self.assertEqual(result["symbol"], "BTC/USD")
        self.assertAlmostEqual(result["effective_qty"], 0.001)

        details = result["order_details"]
        self.assertEqual(details["OrderID"], 12345)
        self.assertEqual(details["FilledQuantity"], 0.001)
        self.assertAlmostEqual(details["FilledAverPrice"], 60000.0)
        self.assertEqual(details["Pair"], "BTC/USD")

    @patch("binance_adapter._get_client")
    def test_get_balance_returns_tuple(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_client.get_account.return_value = {
            "balances": [
                {"asset": "BTC", "free": "0.5", "locked": "0.1"},
                {"asset": "USDT", "free": "10000", "locked": "0"},
            ]
        }

        import binance_adapter as ba
        free, locked = ba.get_balance("BTC/USD")
        self.assertAlmostEqual(free, 0.5)
        self.assertAlmostEqual(locked, 0.1)

    @patch("binance_adapter._get_client")
    def test_get_balance_missing_asset(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_client.get_account.return_value = {
            "balances": [{"asset": "USDT", "free": "10000", "locked": "0"}]
        }

        import binance_adapter as ba
        free, locked = ba.get_balance("BTC/USD")
        self.assertEqual(free, 0.0)
        self.assertEqual(locked, 0.0)

    @patch("binance_adapter._get_client")
    def test_query_orders_normalises_fields(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_client.get_all_orders.return_value = [
            {
                "orderId": 99,
                "status": "FILLED",
                "side": "BUY",
                "origQty": "1.0",
                "executedQty": "1.0",
                "price": "150.00",
                "time": 1700000000000,
                "updateTime": 1700000001000,
            }
        ]

        import binance_adapter as ba
        orders = ba.query_orders("SOL/USD", limit=10)
        self.assertEqual(len(orders), 1)

        o = orders[0]
        self.assertEqual(o["OrderID"], 99)
        self.assertEqual(o["Status"], "FILLED")
        self.assertEqual(o["Side"], "BUY")
        self.assertEqual(o["Pair"], "SOL/USD")
        self.assertAlmostEqual(o["FilledQuantity"], 1.0)


class TestExecAdapterPrecision(unittest.TestCase):

    @patch("binance_adapter._get_client")
    def test_get_pair_precision_from_lot_size(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_client.get_symbol_info.return_value = {
            "symbol": "BTCUSDT",
            "filters": [
                {"filterType": "LOT_SIZE", "stepSize": "0.000001"},
            ],
        }

        import binance_adapter as ba
        ba._precision_cache.clear()
        self.assertEqual(ba.get_pair_precision("BTC/USD"), 6)

    @patch("binance_adapter._get_client")
    def test_get_pair_precision_integer_step(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_client.get_symbol_info.return_value = {
            "symbol": "SOLUSDT",
            "filters": [
                {"filterType": "LOT_SIZE", "stepSize": "1.00000000"},
            ],
        }

        import binance_adapter as ba
        ba._precision_cache.clear()
        self.assertEqual(ba.get_pair_precision("SOL/USD"), 0)


if __name__ == "__main__":
    unittest.main()
