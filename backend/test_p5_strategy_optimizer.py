#!/usr/bin/env python3
import unittest

from p5_strategy_optimizer import (
    board_limit_pct, entry_is_tradeable, signal_passes, bear_reversal_signal,
    stop_fill_price, take_fill_price, book_reversal_features,
    passes_book_filter, book_quality_score,
)


class OptimizerContractTest(unittest.TestCase):
    def test_board_limit_pct_by_board(self):
        self.assertEqual(board_limit_pct("600000.SH"), 0.10)
        self.assertEqual(board_limit_pct("000001.SZ"), 0.10)
        self.assertEqual(board_limit_pct("300001.SZ"), 0.20)
        self.assertEqual(board_limit_pct("688001.SH"), 0.20)
        self.assertEqual(board_limit_pct("830001.BJ"), 0.30)

    def test_entry_rejects_weak_or_untradeable_open(self):
        self.assertFalse(entry_is_tradeable("600000.SH", 10.0, 9.5, -0.03, 0.04))
        self.assertFalse(entry_is_tradeable("600000.SH", 10.0, 11.0, -0.03, 0.04))
        self.assertFalse(entry_is_tradeable("300001.SZ", 10.0, 12.0, -0.03, 0.04))
        self.assertTrue(entry_is_tradeable("300001.SZ", 10.0, 10.2, -0.03, 0.04))

    def test_signal_modes_are_explicit(self):
        row = {"score": 0.6, "momo": False, "pct": 0.03, "volr": 1.5,
               "pos60": 60, "rsi6": 60, "ret3": 0.04, "trend20": True,
               "close_to_high": 0.99}
        self.assertTrue(signal_passes(row, {"mode": "score", "score_min": 0.5}))
        self.assertFalse(signal_passes(row, {"mode": "momo"}))
        row["momo"] = True
        self.assertTrue(signal_passes(row, {"mode": "momo", "volr_min": 1.3,
                                            "close_to_high_min": 0.97}))
        self.assertFalse(signal_passes(row, {"mode": "momo", "volr_min": 2.0}))
        row["score"] = 0.2
        self.assertFalse(signal_passes(row, {"mode": "intersection", "score_min": 0.35,
                                            "volr_min": 1.3}))
        row["score"] = 0.5
        self.assertTrue(signal_passes(row, {"mode": "intersection", "score_min": 0.35,
                                           "volr_min": 1.3}))

    def test_bear_reversal_requires_all_dimensions(self):
        good = {"ret3": -0.09, "pct": 0.03, "open": 10.0, "close": 10.4,
                "volr": 1.6, "pos60": 0.20, "amount": 80_000_000}
        self.assertTrue(bear_reversal_signal(good, market_mom=-0.01))
        self.assertFalse(bear_reversal_signal({**good, "ret3": -0.07}, market_mom=-0.01))
        self.assertFalse(bear_reversal_signal(good, market_mom=0.01))
        self.assertFalse(bear_reversal_signal({**good, "close": 9.9}, market_mom=-0.01))
        self.assertFalse(bear_reversal_signal({**good, "pos60": None}, market_mom=-0.01))
        self.assertFalse(bear_reversal_signal({**good, "ret3": None}, market_mom=-0.01))

    def test_stop_fill_uses_open_on_gap(self):
        self.assertAlmostEqual(stop_fill_price(10.0, 9.4, 9.2, 0.07) or 0.0, 9.3, places=8)
        self.assertAlmostEqual(stop_fill_price(10.0, 9.0, 8.8, 0.07) or 0.0, 9.0, places=8)
        self.assertIsNone(stop_fill_price(10.0, 9.5, 9.4, 0.07))
        self.assertAlmostEqual(take_fill_price(10.0, 11.6, 11.7, 0.15) or 0.0, 11.6, places=8)
        self.assertAlmostEqual(take_fill_price(10.0, 10.8, 11.6, 0.15) or 0.0, 11.5, places=8)
        self.assertIsNone(take_fill_price(10.0, 10.8, 11.4, 0.15))

    def test_book_reversal_features_use_signal_day_only(self):
        row = {"open": 9.4, "high": 10.2, "low": 9.2, "close": 10.0,
               "volr": 2.0, "pct": 0.04}
        prev = {"high": 9.8, "low": 9.0, "close": 9.6}
        f = book_reversal_features(row, prev)
        self.assertAlmostEqual(f["clv"], 0.8, places=8)
        self.assertAlmostEqual(f["body_ratio"], 0.6, places=8)
        self.assertTrue(f["reclaims_prev_high"])
        self.assertAlmostEqual(f["lower_shadow_ratio"], 0.2, places=8)

    def test_book_filter_is_single_variable_and_missing_safe(self):
        f = {"clv": 0.8, "body_ratio": 0.6, "reclaims_prev_high": True,
             "lower_shadow_ratio": 0.2}
        self.assertTrue(passes_book_filter(f, "clv", 0.75))
        self.assertFalse(passes_book_filter(f, "body_ratio", 0.7))
        self.assertTrue(passes_book_filter(f, "reclaims_prev_high", True))
        self.assertFalse(passes_book_filter({}, "clv", 0.5))

    def test_book_quality_score_is_bounded_and_monotone(self):
        weak = {"clv": 0.55, "body_ratio": 0.20, "reclaims_prev_high": False,
                "lower_shadow_ratio": 0.05}
        strong = {"clv": 0.90, "body_ratio": 0.70, "reclaims_prev_high": True,
                  "lower_shadow_ratio": 0.20}
        self.assertGreater(book_quality_score(strong), book_quality_score(weak))
        self.assertGreaterEqual(book_quality_score(weak), 0.0)
        self.assertLessEqual(book_quality_score(strong), 1.0)


if __name__ == "__main__":
    unittest.main()
