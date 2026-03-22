from __future__ import annotations

import csv
import tempfile
import unittest

from tests.test_loader import load_module


ledger = load_module("ledger_mod", "services/ledger/ledger_service.py", add_sys_path="services/ledger")


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.csv_file = f"{self.temp_dir.name}/execution_log.csv"

    def test_initialize_log_file_creates_header(self):
        ledger.initialize_log_file(self.temp_dir.name, self.csv_file)

        with open(self.csv_file, "r", encoding="utf-8") as fh:
            first_line = fh.readline().strip()

        self.assertEqual(first_line, ",".join(ledger.CSV_HEADERS))

    def test_initialize_log_file_is_idempotent(self):
        ledger.initialize_log_file(self.temp_dir.name, self.csv_file)
        ledger.initialize_log_file(self.temp_dir.name, self.csv_file)

        with open(self.csv_file, "r", encoding="utf-8") as fh:
            lines = fh.readlines()

        self.assertEqual(len(lines), 1)

    def test_build_csv_row_success_shape(self):
        row = ledger.build_csv_row(
            {
                "timestamp": 1700000000,
                "side": "BUY",
                "symbol": "BTC/USD",
                "order_details": {
                    "FilledAverPrice": 8100.5,
                    "FilledQuantity": 1.25,
                },
                "strategy_signal": "BB_Lower_Cross",
                "status_code": 200,
                "result": "SUCCESS",
            }
        )

        self.assertEqual(row["Action"], "BUY")
        self.assertEqual(row["Pair"], "BTC/USD")
        self.assertEqual(row["Price"], "8100.50000000")
        self.assertEqual(row["Quantity"], "1.25000000")
        self.assertEqual(row["Requested_Qty"], "0.00000000")
        self.assertEqual(row["Effective_Qty"], "0.00000000")
        self.assertEqual(row["Strategy_Signal"], "BB_Lower_Cross")
        self.assertEqual(row["API_Status"], "200 SUCCESS")

    def test_build_csv_row_failure_contains_error(self):
        row = ledger.build_csv_row(
            {
                "timestamp": 1700000000,
                "side": "SELL",
                "symbol": "BTC/USD",
                "price": 0,
                "qty": 0,
                "status_code": 401,
                "result": "FAILED",
                "raw_error": "signature error",
            }
        )

        self.assertEqual(row["Action"], "SELL")
        self.assertEqual(row["API_Status"], "401 signature error")

    def test_process_order_result_message_appends_one_row(self):
        ledger.initialize_log_file(self.temp_dir.name, self.csv_file)

        ok = ledger.process_order_result_message(
            '{"timestamp":1700000000,"side":"BUY","symbol":"BTC/USD","price":123.45,'
            '"qty":1.0,"reason":"Mean_Reversion_TP","status_code":200,"result":"SUCCESS"}',
            csv_file=self.csv_file,
        )
        self.assertTrue(ok)

        with open(self.csv_file, "r", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["Pair"], "BTC/USD")
        self.assertEqual(rows[0]["Requested_Qty"], "1.00000000")
        self.assertEqual(rows[0]["Effective_Qty"], "1.00000000")
        self.assertEqual(rows[0]["Strategy_Signal"], "Mean_Reversion_TP")


if __name__ == "__main__":
    unittest.main()
