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


def test_to_5min_df_parses_baostock_time():
    df = bb._to_5min_df("sh.600036", [
        ["2025-07-01", "20250701093500000", "1.0", "2.0", "1.5", "1.8", "100", "200"],
    ])
    assert df["symbol"].to_list() == ["600036.SH"]
    assert str(df["datetime"][0]) == "2025-07-01 09:35:00"
    assert df["volume"][0] == 100.0
    empty = bb._to_5min_df("sh.600036", [])
    assert empty.is_empty() and "symbol" in empty.columns


def test_sync_minute_writes_partitions_and_state(tmp_data, monkeypatch):
    monkeypatch.setattr(bb, "stock_universe",
                        lambda: ["600036.SH", "000001.SZ", "600519.SH"])
    monkeypatch.setattr(bb, "listing_date_map", lambda: {})

    calls = []

    def fake_query(code, fields, start, end, frequency, adjustflag, timeout, retries=3):
        calls.append(code)
        return [["2025-07-01", "20250701093500000", "1", "2", "1.5", "1.8", "100", "200"]]

    monkeypatch.setattr(bb, "query_kline", fake_query)
    st = bb.load_state(tmp_data / "state.json")
    out = bb.sync_minute(_date(2025, 7, 1), _date(2025, 7, 2), st,
                         timeout=5, flush_batch=2, limit=3)
    assert out["symbols"] == 3
    assert len(calls) == 3
    df = pl.read_parquet(tmp_data / "kline_5min" / "date=2025-07-01" / "part.parquet")
    assert df["symbol"].n_unique() == 3
    assert set(st["minute_done"]) == {"600036.SH", "000001.SZ", "600519.SH"}


def test_sync_minute_resume_skips_done(tmp_data, monkeypatch):
    monkeypatch.setattr(bb, "stock_universe", lambda: ["600036.SH", "000001.SZ"])
    monkeypatch.setattr(bb, "listing_date_map", lambda: {})
    calls = []
    monkeypatch.setattr(bb, "query_kline",
                        lambda *a, **k: (calls.append(a[0]) or
                                         [["2025-07-01", "20250701093500000",
                                           "1", "2", "1.5", "1.8", "100", "200"]]))
    st = bb.load_state(tmp_data / "state.json")
    bb.mark_done(st, "minute", "600036.SH")
    bb.save_state(st, tmp_data / "state.json")
    st2 = bb.load_state(tmp_data / "state.json")
    bb.sync_minute(_date(2025, 7, 1), _date(2025, 7, 2), st2, timeout=5)
    assert calls == ["sz.000001"]  # 已完成的 600036.SH 不重拉


def test_sync_minute_failure_recorded(tmp_data, monkeypatch):
    monkeypatch.setattr(bb, "stock_universe", lambda: ["600036.SH"])
    monkeypatch.setattr(bb, "listing_date_map", lambda: {})
    monkeypatch.setattr(bb, "query_kline",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    st = bb.load_state(tmp_data / "state.json")
    bb.sync_minute(_date(2025, 7, 1), _date(2025, 7, 2), st, timeout=5)
    assert "600036.SH" in st["failed"]["minute"]
    assert st["minute_done"] == []
    assert tmp_data / "failures.csv" in list(tmp_data.iterdir()) or \
        (tmp_data / "failures.csv").exists()


def test_make_progress_printer_prints(capsys):
    p = bb.make_progress_printer()
    p("minute", 10, 100, 1234)
    out = capsys.readouterr().out
    assert "minute" in out and "10/100" in out


def test_to_daily_df_volume_div():
    rows = [["2025-07-01", "3513.25", "3532.11", "3513.25", "3519.65",
             "57208470500", "623102482278"]]
    df = bb._to_daily_df("sh.000001", rows, volume_div=100.0)
    assert df["symbol"].to_list() == ["000001.SH"]
    assert df["volume"][0] == 572084705.0  # 股 ÷100 → 手
    assert str(df["date"][0]) == "2025-07-01"


def test_sync_daily_writes_both_universes(tmp_data, monkeypatch):
    monkeypatch.setattr(bb, "index_universe", lambda: ["000001.SH"])
    monkeypatch.setattr(bb, "etf_universe", lambda: ["510300.SH"])
    monkeypatch.setattr(bb, "query_kline", lambda *a, **k: [
        ["2025-07-01", "1", "2", "1.5", "1.8", "100000000", "200000000"],
    ])
    st = bb.load_state()
    out = bb.sync_daily(_date(2025, 7, 1), _date(2025, 7, 2), st, timeout=5)
    assert out["index"]["rows"] == 1 and out["etf"]["rows"] == 1
    idx = pl.read_parquet(tmp_data / "kline_index_daily" / "date=2025-07-01" / "part.parquet")
    etf = pl.read_parquet(tmp_data / "kline_etf_daily" / "date=2025-07-01" / "part.parquet")
    assert idx["symbol"].to_list() == ["000001.SH"]
    assert etf["symbol"].to_list() == ["510300.SH"]
    assert idx["volume"][0] == 1000000.0  # 指数 ÷100
    assert etf["volume"][0] == 100000000.0  # ETF 不换算
    assert set(st["daily_done"]) == {"000001.SH", "510300.SH"}


def test_write_daily_partition_legacy_no_date_col(tmp_path):
    root = tmp_path / "kd"
    legacy = pl.DataFrame({
        "symbol": ["000001.SH"], "open": [1.0], "high": [1.0], "low": [1.0],
        "close": [1.0], "volume": [100.0], "amount": [100.0],
    })
    (root / "date=2025-07-01").mkdir(parents=True)
    legacy.write_parquet(root / "date=2025-07-01" / "part.parquet")
    new = pl.DataFrame({
        "symbol": ["000001.SH"], "date": [_date(2025, 7, 1)],
        "open": [2.0], "high": [2.0], "low": [2.0], "close": [2.0],
        "volume": [200.0], "amount": [200.0],
    })
    bb.write_daily_partition(new, root)
    out = pl.read_parquet(root / "date=2025-07-01" / "part.parquet")
    assert out.height == 1
    assert out["close"][0] == 2.0
    assert "date" not in out.columns


def test_etf_universe_from_snapshot(tmp_data):
    qk = tmp_data / "quant_kline"
    qk.mkdir(parents=True)
    (qk / "etf_universe_snapshot.json").write_text(
        '{"codes": ["510300.XSHG", "159919.XSHE"]}')
    assert bb.etf_universe() == ["159919.SZ", "510300.SH"]


def test_etf_universe_fallback_partitions(tmp_data):
    bb.write_daily_partition(pl.DataFrame({
        "symbol": ["510300.SH", "159919.SZ"],
        "date": [_date(2025, 7, 1), _date(2025, 7, 1)],
        "open": [1.0, 1.0], "high": [1.0, 1.0], "low": [1.0, 1.0],
        "close": [1.0, 1.0], "volume": [1.0, 1.0], "amount": [1.0, 1.0],
    }), tmp_data / "kline_etf_daily")
    assert bb.etf_universe() == ["159919.SZ", "510300.SH"]


def test_build_ex_factor_table():
    # back 因子在除权日单调累积：1.0 → 2.0 → 4.0（两次 10送10）
    events = {"sh.600036": [
        (_date(2024, 6, 1), 1.0),
        (_date(2025, 6, 1), 2.0),
        (_date(2026, 6, 1), 4.0),
    ]}
    df = bb.build_ex_factor_table(events)
    assert df["symbol"].to_list() == ["600036.SH"] * 3
    # ex_factor = back/latest：1/4, 2/4, 4/4
    assert [round(x, 4) for x in df["ex_factor"].to_list()] == [0.25, 0.5, 1.0]
    # DataManager._adj_events 口径：相邻行 prev/curr = 0.5（10送10 事件因子）
    f1 = df["ex_factor"][0] / df["ex_factor"][1]
    f2 = df["ex_factor"][1] / df["ex_factor"][2]
    assert f1 == 0.5 and f2 == 0.5


def test_write_append_table_idempotent(tmp_path):
    p = tmp_path / "all.parquet"
    df = pl.DataFrame({"symbol": ["600036.SH"], "trade_date": [_date(2025, 7, 16)],
                       "ex_factor": [0.9]})
    bb._write_append_table(df, p, ["symbol", "trade_date"])
    bb._write_append_table(df, p, ["symbol", "trade_date"])
    out = pl.read_parquet(p)
    assert out.height == 1
    assert not (p.with_name(p.name + ".tmp")).exists()


def test_sync_corporate_writes_adj_and_dividends(tmp_data, monkeypatch):
    monkeypatch.setattr(bb, "stock_universe", lambda: ["600036.SH"])
    monkeypatch.setattr(bb, "query_adjust_factor_rows", lambda *a, **k: [
        ["sh.600036", "2025-07-16", "0.954887", "12.763991", "12.763991"],
    ])
    monkeypatch.setattr(bb, "query_dividend_rows", lambda *a, **k: [
        {"code": "sh.600036", "dividOperateDate": "2025-07-11",
         "dividPayDate": "2025-07-11", "dividCashPsBeforeTax": "2",
         "dividCashPsAfterTax": "1.8", "dividStocksPs": "0.000000",
         "dividCashStock": "", "dividReserveToStockPs": ""},
    ])
    st = bb.load_state()
    out = bb.sync_corporate(_date(2025, 1, 1), _date(2025, 12, 31), st, timeout=5)
    assert out["adj"] == 1 and out["dividends"] == 1
    adj = pl.read_parquet(tmp_data / "adj_factor" / "all.parquet")
    assert adj["symbol"].to_list() == ["600036.SH"]
    assert adj["trade_date"][0] == _date(2025, 7, 16)
    assert adj["ex_factor"][0] == pytest.approx(1.0)  # 唯一事件=最新 → 1.0
    div = pl.read_parquet(tmp_data / "dividends" / "all.parquet")
    assert div["cash_ps_before_tax"][0] == 2.0
    assert set(st["adj_done"]) == {"600036.SH"}
    assert set(st["dividends_done"]) == {"600036.SH"}
