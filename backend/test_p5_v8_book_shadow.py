#!/usr/bin/env python3
import unittest

from p5_v8_book_shadow import enrich_book_features, transform_pools


class BookShadowContractTest(unittest.TestCase):
    def test_enrich_uses_previous_bar_and_keeps_original(self):
        rows = [
            {"symbol": "A", "date": "2026-01-01", "open": 9.0, "high": 10.0, "low": 8.0, "close": 9.5},
            {"symbol": "A", "date": "2026-01-02", "open": 9.2, "high": 10.5, "low": 9.0, "close": 10.2},
        ]
        got = enrich_book_features(rows)
        self.assertEqual(len(got), 2)
        self.assertEqual(got[0]["book"], {})
        self.assertTrue(got[1]["book"]["reclaims_prev_high"])
        self.assertEqual(got[1]["symbol"], "A")

    def test_transform_supports_forward_reverse_and_filter(self):
        pools = {"2026-01-02": [
            {"symbol": "A", "book": {"clv": 0.9}},
            {"symbol": "B", "book": {"clv": 0.4}},
            {"symbol": "C", "book": {}},
        ]}
        forward = transform_pools(pools, "clv", descending=True)
        reverse = transform_pools(pools, "clv", descending=False)
        filtered = transform_pools(pools, "clv", descending=True, threshold=0.75)
        self.assertEqual([x["symbol"] for x in forward["2026-01-02"]], ["A", "B", "C"])
        self.assertEqual([x["symbol"] for x in reverse["2026-01-02"]], ["B", "A", "C"])
        self.assertEqual([x["symbol"] for x in filtered["2026-01-02"]], ["A"])


if __name__ == "__main__":
    unittest.main()
