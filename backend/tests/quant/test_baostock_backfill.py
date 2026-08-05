"""baostock_backfill 单元测试（不打真实网络，monkeypatch 假 baostock 模块）。"""
import time

import polars as pl
import pytest

from app.services import baostock_backfill as bb


class _FakeRS:
    """伪造 baostock QueryResult：iter_rows 遍历 rows。"""

    def __init__(self, rows, error_code="0", error_msg="success", fields=None):
        self.error_code = error_code
        self.error_msg = error_msg
        self.fields = fields
        self._rows = rows
        self._i = -1

    def next(self):
        self._i += 1
        return self._i < len(self._rows)

    def get_row_data(self):
        return self._rows[self._i]


class _FakeBS:
    """伪造 baostock 模块（含 fields 的 query_dividend_data）。"""

    def __init__(self):
        self.calls = []

    def query_history_k_data_plus(self, code, fields, start_date, end_date,
                                  frequency, adjustflag):
        self.calls.append(("kline", code, frequency))
        return _FakeRS([["2025-07-01", "20250701093500000",
                         "1.0", "2.0", "1.5", "1.8", "100", "200"]])

    def query_all_stock(self, day=None):
        self.calls.append(("all_stock", day))
        return _FakeRS([["sh.600036", "1", "招商银行"], ["sh.000001", "1", "上证指数"]])

    def query_adjust_factor(self, code, start_date, end_date):
        self.calls.append(("adj", code))
        return _FakeRS([["sh.600036", "2025-07-16", "0.95", "12.76", "12.76"]])

    def query_dividend_data(self, code, year, yearType):
        self.calls.append(("dividend", code, year))
        return _FakeRS([["sh.600036", "2025-07-11", "2025-07-11", "2", "1.8",
                         "0.000000", "10派20元", "0"]],
                       fields=["code", "dividOperateDate", "dividPayDate",
                               "dividCashPsBeforeTax", "dividCashPsAfterTax",
                               "dividStocksPs", "dividCashStock",
                               "dividReserveToStockPs"])


@pytest.fixture
def fake_bs(monkeypatch):
    fb = _FakeBS()
    monkeypatch.setattr(bb, "_bs_module", fb)
    return fb


def test_code_conversion():
    assert bb.to_baostock_code("600036.SH") == "sh.600036"
    assert bb.to_baostock_code("000001.SZ") == "sz.000001"
    assert bb.from_baostock_code("sh.600036") == "600036.SH"
    assert bb.from_baostock_code("sz.000001") == "000001.SZ"


def test_query_kline(fake_bs):
    rows = bb.query_kline("sh.600036", bb.KLINE_5MIN_FIELDS,
                          "2025-07-01", "2025-07-15", "5", "3", timeout=5)
    assert rows == [["2025-07-01", "20250701093500000",
                     "1.0", "2.0", "1.5", "1.8", "100", "200"]]
    assert fake_bs.calls[0] == ("kline", "sh.600036", "5")


def test_query_kline_error_retries(monkeypatch):
    class _ErrBS:
        def query_history_k_data_plus(self, *a, **k):
            return _FakeRS([], error_code="10001003", error_msg="失败")

    monkeypatch.setattr(bb, "_bs_module", _ErrBS())
    monkeypatch.setattr(bb.time, "sleep", lambda s: None)
    with pytest.raises(RuntimeError, match="baostock 查询失败"):
        bb.query_kline("sh.600036", "f", "s", "e", "5", "3",
                       timeout=5, retries=1)


def test_query_all_stock(fake_bs):
    rows = bb.query_all_stock()
    assert len(rows) == 2


def test_query_adjust_factor_rows(fake_bs):
    rows = bb.query_adjust_factor_rows("sh.600036", "2025-01-01", "2025-12-31")
    assert rows == [["sh.600036", "2025-07-16", "0.95", "12.76", "12.76"]]


def test_query_dividend_rows(fake_bs):
    recs = bb.query_dividend_rows("sh.600036", 2025)
    assert recs[0]["dividOperateDate"] == "2025-07-11"
    assert recs[0]["dividCashPsBeforeTax"] == "2"


def test_guarded_timeout():
    def slow():
        time.sleep(0.3)
        return 1

    with pytest.raises(TimeoutError):
        bb._guarded(slow, timeout=0.05)


def test_safe_float():
    assert bb._safe_float("2.5") == 2.5
    assert bb._safe_float("") is None
    assert bb._safe_float("-") is None
    assert bb._safe_float("abc") is None


def test_state_roundtrip(tmp_path):
    p = tmp_path / "state.json"
    st = bb.load_state(p)
    assert st["minute_done"] == []
    bb.mark_done(st, "minute", "600036.SH")
    bb.mark_failed(st, "minute", "000001.SZ", "timeout")
    bb.save_state(st, p)
    st2 = bb.load_state(p)
    assert st2["minute_done"] == ["600036.SH"]
    assert st2["failed"]["minute"]["000001.SZ"] == "timeout"
    assert bb.load_state(tmp_path / "missing.json")["daily_done"] == []


def test_state_atomic_no_tmp_left(tmp_path):
    p = tmp_path / "state.json"
    bb.save_state({"a": 1}, p)
    assert not (tmp_path / "state.json.tmp").exists()


def test_mark_done_and_failed_mutate_inplace(tmp_path):
    st = bb.load_state(tmp_path / "missing.json")
    bb.mark_done(st, "daily", "000001.SH")
    bb.mark_failed(st, "daily", "510300.SH", "empty")
    assert st["daily_done"] == ["000001.SH"]
    assert st["failed"]["daily"] == {"510300.SH": "empty"}
