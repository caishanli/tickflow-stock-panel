"""baostock_backfill 单元测试（不打真实网络，monkeypatch 假 baostock 模块）。"""
import time
from datetime import date as _date
from datetime import datetime as _datetime

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
        return _FakeRS([["sh.600036", "1", "招商银行"], ["sz.000001", "1", "平安银行"]])

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


def _m5(sym, day, hour=10):
    return pl.DataFrame({
        "symbol": [sym, sym],
        "datetime": [_datetime(2025, 7, day, hour, 0), _datetime(2025, 7, day, hour, 5)],
        "open": [1.0, 1.1], "high": [1.2, 1.3], "low": [0.9, 1.0],
        "close": [1.1, 1.2], "volume": [100.0, 200.0], "amount": [110.0, 240.0],
    })


def test_write_minute_partition_idempotent(tmp_path):
    root = tmp_path / "k5"
    bb.write_minute_partition(_m5("600036.SH", 1), root, _date(2025, 7, 1))
    bb.write_minute_partition(_m5("600036.SH", 1), root, _date(2025, 7, 1))
    df = pl.read_parquet(root / "date=2025-07-01" / "part.parquet")
    assert df.height == 2
    assert not (root / "date=2025-07-01" / "part.tmp").exists()


def test_flush_minute_batch_two_days(tmp_path):
    root = tmp_path / "k5"
    bb.flush_minute_batch([_m5("600036.SH", 1), _m5("000001.SZ", 2)], root)
    d1 = pl.read_parquet(root / "date=2025-07-01" / "part.parquet")
    d2 = pl.read_parquet(root / "date=2025-07-02" / "part.parquet")
    assert set(d1["symbol"].to_list()) == {"600036.SH"}
    assert set(d2["symbol"].to_list()) == {"000001.SZ"}


def test_write_daily_partition_merge_with_date_col(tmp_path):
    root = tmp_path / "kd"
    df = pl.DataFrame({
        "symbol": ["000001.SH"], "date": [_date(2025, 7, 1)],
        "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0],
        "volume": [100.0], "amount": [100.0],
    })
    bb.write_daily_partition(df, root)
    bb.write_daily_partition(df, root)
    out = pl.read_parquet(root / "date=2025-07-01" / "part.parquet")
    assert out.height == 1
    assert "date" in out.columns


@pytest.fixture
def tmp_data(tmp_path, monkeypatch):
    """把模块全部路径常量重定向到 tmp 目录。"""
    monkeypatch.setattr(bb, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(bb, "KLINE_5MIN_ROOT", tmp_path / "kline_5min")
    monkeypatch.setattr(bb, "KLINE_INDEX_DAILY_ROOT", tmp_path / "kline_index_daily")
    monkeypatch.setattr(bb, "KLINE_ETF_DAILY_ROOT", tmp_path / "kline_etf_daily")
    monkeypatch.setattr(bb, "ADJ_FACTOR_PATH", tmp_path / "adj_factor" / "all.parquet")
    monkeypatch.setattr(bb, "DIVIDENDS_PATH", tmp_path / "dividends" / "all.parquet")
    monkeypatch.setattr(bb, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(bb, "FAILURE_CSV", tmp_path / "failures.csv")
    return tmp_path


def test_stock_universe_from_instruments(tmp_data, fake_bs):
    inst = tmp_data / "instruments"
    inst.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["600036.SH", "000001.SZ", "920001.BJ"],
        "listing_date": ["2020-01-01", "1991-04-03", "2023-01-01"],
    }).write_parquet(inst / "instruments.parquet")
    assert bb.stock_universe() == ["000001.SZ", "600036.SH"]  # 排除北交所


def test_stock_universe_fallback_all_stock(tmp_data, fake_bs):
    # 无 instruments 文件 → 回退 query_all_stock
    assert bb.stock_universe() == ["000001.SZ", "600036.SH"]


def test_index_universe_from_parquet(tmp_data):
    inst = tmp_data / "instruments_index"
    inst.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["000001.SH", "399001.SZ"],
        "name": ["上证指数", "深证成指"],
    }).write_parquet(inst / "instruments_index.parquet")
    assert bb.index_universe() == ["000001.SH", "399001.SZ"]


def test_listing_date_map(tmp_data):
    inst = tmp_data / "instruments"
    inst.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["600036.SH", "000001.SZ"],
        "listing_date": ["2002-04-09", "1991-04-03"],
    }).write_parquet(inst / "instruments.parquet")
    m = bb.listing_date_map()
    assert m["600036.SH"] == _date(2002, 4, 9)
