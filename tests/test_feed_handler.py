from __future__ import annotations

import unittest

from tests.test_loader import load_module


market_data = load_module("market_data_mod", "services/feed-handler/market_data.py")


class FeedHandlerTests(unittest.TestCase):
    def test_normalize_pair_converts_usdt(self):
        self.assertEqual(market_data._normalize_pair("BTCUSDT"), "BTC/USD")

    def test_normalize_pair_keeps_slash_format(self):
        self.assertEqual(market_data._normalize_pair("ETH/USD"), "ETH/USD")

    def test_sorted_params_string_orders_keys(self):
        params = {"timestamp": 2, "pair": "BTC/USD"}
        self.assertEqual(market_data._sorted_params(params), "pair=BTC/USD&timestamp=2")

    def test_fetch_top_of_book_success(self):
        class _Resp:
            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "Success": True,
                    "Data": {"BTC/USD": {"MaxBid": "60000.5", "MinAsk": "60010.5"}},
                    "ServerTime": 1700000000123,
                }

        class _Client:
            def get(self, _url):
                return _Resp()

        max_bid, min_ask, server_time = market_data._fetch_top_of_book(
            _Client(), "https://mock-api.roostoo.com", "BTC/USD"
        )
        self.assertEqual(max_bid, 60000.5)
        self.assertEqual(min_ask, 60010.5)
        self.assertEqual(server_time, 1700000000123)

    def test_fetch_top_of_book_missing_pair_raises(self):
        class _Resp:
            def raise_for_status(self):
                return None

            def json(self):
                return {"Success": True, "Data": {"ETH/USD": {"MaxBid": "1", "MinAsk": "2"}}}

        class _Client:
            def get(self, _url):
                return _Resp()

        with self.assertRaises(KeyError):
            market_data._fetch_top_of_book(_Client(), "https://mock-api.roostoo.com", "BTC/USD")


if __name__ == "__main__":
    unittest.main()
