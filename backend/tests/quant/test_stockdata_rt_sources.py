# backend/tests/quant/test_stockdata_rt_sources.py
"""rt_sources 单测：解析层纯文本 fixture（2026-08-25 收盘后实测录制）。"""
import datetime as _dt
from contextlib import contextmanager

import polars as pl
import pytest

from app.services.stockdata.rt_sources import (
    RTQuote,
    SinaRTSource,
    TencentRTSource,
    _tf_to_vendor,
    parse_sina_payload,
    parse_tencent_payload,
)
from app.services.stockdata.sources import NetworkPuller

# 2026-08-25 收盘后实测原行(qt.gtimg.cn): v[35]=价/量/额复合串, v[37]=累计额(万元)
_TENCENT_LINE = 'v_sh600000="1~浦发银行~600000~9.08~9.22~9.24~881756~343481~536689~9.07~3089~9.06~15385~9.05~8179~9.04~2976~9.03~3783~9.08~984~9.09~1749~9.10~6749~9.11~634~9.12~1024~~20260825161459~-0.14~-1.52~9.28~9.06~9.08/881756/804478557~881756~80448~0.26~5.90~~9.28~9.06~2.39~3024.17~3024.17~0.40~10.14~8.30~1.32~22272~9.12~4.89~6.05~~~0.01~80447.8557~12.0764~133~   A~GP-A~-24.46~1.23~4.63~6.03~0.49~13.83~8.07~-1.41~-1.20~2.02~33305838300~33305838300~49.99~-18.93~33305838300~~~-33.28~0.00~~CNY~0~___D__F__N~9.00~7435~";'

# 2026-08-25 收盘后实测原行, ETF 同样 v[37]=累计额(万元)
_TENCENT_ETF_LINE = 'v_sh510300="1~沪深300ETF华泰柏瑞~510300~4.616~4.627~4.601~7452568~3503458~3938894~4.615~3707~4.614~5211~4.613~4345~4.612~2174~4.611~1790~4.616~4159~4.617~1072~4.618~1683~4.619~1492~4.620~7078~~20260825161454~-0.011~-0.24~4.639~4.587~4.616/7452568/3435989271~7452568~343599~3.13~~~4.639~4.587~1.12~1099.03~1099.03~0.00~5.090~4.164~0.81~1743~4.610~~~~~~343598.9271~202.6886~4391~   A~ETF~-0.30~-3.57~~~~5.095~4.293~-2.37~-0.24~-5.18~23809087700~23809087700~5.33~2.60~23809087700~0.08~4.6122~3.92~0.02~4.6263~CNY~0~___D__F__N~4.610~6984~";'

_SINA_LINE = (
    'var hq_str_sh600000="浦发银行,9.240,9.220,9.080,9.280,9.060,9.070,9.080,'
    '88175624,804478557.000,308946,9.070,1538500,9.060,817900,9.050,297600,'
    '9.040,378300,9.030,98383,9.080,174900,9.090,674900,9.100,63400,9.110,'
    '102400,9.120,2026-08-25,15:34:59,00";'
)


def test_tf_to_vendor():
    assert _tf_to_vendor("600000.SH") == "sh600000"
    assert _tf_to_vendor("000001.SZ") == "sz000001"
    assert _tf_to_vendor("510300.SH") == "sh510300"
    assert _tf_to_vendor("159915.SZ") == "sz159915"
    assert _tf_to_vendor("BADCODE") is None


def test_parse_tencent_payload_units_and_fields():
    out = parse_tencent_payload(_TENCENT_LINE)
    assert "600000.SH" in out
    q = out["600000.SH"]
    assert q.price == 9.08 and q.prev_close == 9.22 and q.open_ == 9.24
    assert q.high == 9.28 and q.low == 9.06
    assert q.cum_volume == pytest.approx(88_175_600)   # 手→股 ×100
    assert q.cum_amount == pytest.approx(804_480_000)  # 万→元 ×1e4
    assert q.quote_time == _dt.datetime(2026, 8, 25, 16, 14, 59)


def test_parse_tencent_etf_units():
    out = parse_tencent_payload(_TENCENT_ETF_LINE)
    assert "510300.SH" in out
    q = out["510300.SH"]
    assert q.price == 4.616 and q.prev_close == 4.627 and q.open_ == 4.601
    assert q.high == 4.639 and q.low == 4.587
    assert q.cum_volume == pytest.approx(745_256_800)     # 手→股 x100
    assert q.cum_amount == pytest.approx(3_435_990_000)   # 万→元 x1e4
    assert q.quote_time == _dt.datetime(2026, 8, 25, 16, 14, 54)


def test_parse_tencent_skips_zero_price_and_garbage():
    text = _TENCENT_LINE + '\nv_sz000001="1~平安银行~000001~0.00~~~~...";\ngarbage line'
    out = parse_tencent_payload(text)
    assert "000001.SZ" not in out          # 价格 0 → 丢弃（停牌不造 bar）
    assert "600000.SH" in out              # 正常行保留


def test_parse_sina_payload_units_and_fields():
    out = parse_sina_payload(_SINA_LINE)
    q = out["600000.SH"]
    assert q.price == 9.08 and q.prev_close == 9.22 and q.open_ == 9.24
    assert q.high == 9.28 and q.low == 9.06
    assert q.cum_volume == pytest.approx(88_175_624)        # 已是股
    assert q.cum_amount == pytest.approx(804_478_557.0)     # 已是元
    assert q.quote_time == _dt.datetime(2026, 8, 25, 15, 34, 59)


def test_parse_sina_empty_fields_dropped():
    text = 'var hq_str_sz999999=",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,";'
    assert parse_sina_payload(text) == {}


def test_sources_http_roundtrip(monkeypatch):
    """HTTP 层：mock requests.Session.get，验证 URL/头/GBK 解码/批量拼接。"""
    captured = {}

    class FakeResp:
        status_code = 200

        def __init__(self, text):
            self._text = text

        @property
        def text(self):
            return self._text.encode("utf-8").decode("utf-8")

        def raise_for_status(self):
            return None

    class FakeSession:
        def __init__(self):
            self.headers = {}

        def get(self, url, timeout=None):
            captured["url"] = url
            captured["timeout"] = timeout
            body = _TENCENT_LINE if "gtimg" in url else _SINA_LINE
            return FakeResp(body)

    t = TencentRTSource()
    t._session = FakeSession()
    out = t.fetch(["600000.SH"])
    assert "sh600000" in captured["url"]
    assert "600000.SH" in out
    s = SinaRTSource()
    s._session = FakeSession()
    out2 = s.fetch(["600000.SH"])
    assert "hq.sinajs.cn" in captured["url"]
    assert "finance.sina.com.cn" in s._headers.get("Referer", "")
    assert "600000.SH" in out2


def test_source_fetch_network_error_returns_empty(monkeypatch):
    class BoomSession:
        def __init__(self):
            self.headers = {}

        def get(self, url, timeout=None):
            raise OSError("network down")

    t = TencentRTSource()
    t._session = BoomSession()
    assert t.fetch(["600000.SH"]) == {}


def _q(sym, price, cum_vol, cum_amt, ts):
    return RTQuote(symbol=sym, price=price, prev_close=price, open_=price,
                   high=price, low=price, cum_volume=cum_vol,
                   cum_amount=cum_amt, quote_time=ts)


def test_synthesizer_same_minute_updates_hlc():
    from app.services.stockdata.rt_sources import BarSynthesizer
    syn = BarSynthesizer()
    t0 = _dt.datetime(2026, 8, 25, 10, 0, 30)
    frames = syn.update({"600000.SH": _q("600000.SH", 9.0, 100_000, 900_000, t0)})
    assert len(frames) == 1
    df = frames[0]
    assert df["close"].to_list() == [9.0]
    assert df["datetime"].to_list() == [_dt.datetime(2026, 8, 25, 10, 0)]
    # 首拍无基线：量额记 0
    assert df["volume"].to_list() == [0]
    # 同分钟第二拍：high/low/close 更新、量额差分
    t1 = _dt.datetime(2026, 8, 25, 10, 0, 50)
    frames = syn.update({"600000.SH": _q("600000.SH", 9.2, 150_000, 1_380_000, t1)})
    df = frames[0]
    row = df.to_dicts()[0]
    assert row["open"] == 9.0 and row["high"] == 9.2 and row["low"] == 9.0
    assert row["close"] == 9.2
    assert row["volume"] == pytest.approx(50_000)       # 差分
    assert row["amount"] == pytest.approx(480_000)


def test_synthesizer_minute_rollover_opens_new_bar():
    from app.services.stockdata.rt_sources import BarSynthesizer
    syn = BarSynthesizer()
    syn.update({"600000.SH": _q("600000.SH", 9.0, 100_000, 900_000,
                                _dt.datetime(2026, 8, 25, 10, 0, 30))})
    frames = syn.update({"600000.SH": _q("600000.SH", 9.1, 160_000, 1_450_000,
                                         _dt.datetime(2026, 8, 25, 10, 1, 5))})
    df = frames[0]
    # 返回帧 = 封口旧 bar(10:00) + 新 bar(10:01)，调用方按 (symbol,datetime) upsert
    assert sorted(df["datetime"].to_list()) == [
        _dt.datetime(2026, 8, 25, 10, 0), _dt.datetime(2026, 8, 25, 10, 1)]
    new_row = df.filter(pl.col("datetime") == _dt.datetime(2026, 8, 25, 10, 1)
                        ).to_dicts()[0]
    assert new_row["open"] == 9.0                        # 上一拍价格开 bar
    assert new_row["volume"] == pytest.approx(60_000)


def test_synthesizer_out_of_order_tick_folds_into_current_bar():
    from app.services.stockdata.rt_sources import BarSynthesizer
    syn = BarSynthesizer()
    syn.update({"600000.SH": _q("600000.SH", 9.0, 100_000, 900_000,
                                _dt.datetime(2026, 8, 25, 10, 0, 30))})
    syn.update({"600000.SH": _q("600000.SH", 9.1, 160_000, 1_450_000,
                                _dt.datetime(2026, 8, 25, 10, 1, 5))})
    # 迟到 tick(旧分钟时间戳): 不得重新开封已封口的 10:00 bar
    frames = syn.update({"600000.SH": _q("600000.SH", 9.15, 170_000, 1_550_000,
                                         _dt.datetime(2026, 8, 25, 10, 0, 55))})
    df = frames[0]
    assert df["datetime"].to_list() == [_dt.datetime(2026, 8, 25, 10, 1)]
    row = df.to_dicts()[0]
    assert row["close"] == 9.15                      # 价格并入当前 bar
    assert row["volume"] == pytest.approx(70_000)    # 差分并入当前 bar (10k+60k)


def test_synthesizer_negative_delta_clamps_zero():
    from app.services.stockdata.rt_sources import BarSynthesizer

    def latest_volume(df):
        return df.sort("datetime")["volume"].to_list()[-1]

    syn = BarSynthesizer()
    syn.update({"600000.SH": _q("600000.SH", 9.0, 500_000, 4_500_000,
                                _dt.datetime(2026, 8, 25, 10, 0))})
    # 累计量回落（源重置）：clamp 0 并以本次值重建基线
    frames = syn.update({"600000.SH": _q("600000.SH", 9.1, 100_000, 900_000,
                                         _dt.datetime(2026, 8, 25, 10, 1))})
    assert latest_volume(frames[0]) == 0
    # 下一拍基于新基线正常差分
    frames = syn.update({"600000.SH": _q("600000.SH", 9.2, 130_000, 1_170_000,
                                         _dt.datetime(2026, 8, 25, 10, 2))})
    assert latest_volume(frames[0]) == pytest.approx(30_000)


def test_synthesizer_multi_symbol_frames():
    from app.services.stockdata.rt_sources import BarSynthesizer
    syn = BarSynthesizer()
    ts = _dt.datetime(2026, 8, 25, 10, 0)
    frames = syn.update({
        "600000.SH": _q("600000.SH", 9.0, 100_000, 900_000, ts),
        "000001.SZ": _q("000001.SZ", 11.5, 200_000, 2_300_000, ts),
    })
    df = pl.concat(frames)
    assert sorted(df["symbol"].to_list()) == ["000001.SZ", "600000.SH"]


def test_synthesizer_reset_if_new_day():
    from app.services.stockdata.rt_sources import BarSynthesizer
    syn = BarSynthesizer()
    syn.update({"600000.SH": _q("600000.SH", 9.0, 500_000, 4_500_000,
                                _dt.datetime(2026, 8, 25, 15, 0))})
    syn.reset_if_new_day(_dt.date(2026, 8, 26))
    # 新交易日：首拍重新零基线（旧累计量不会产生巨额差分）
    frames = syn.update({"600000.SH": _q("600000.SH", 9.0, 80_000, 720_000,
                                         _dt.datetime(2026, 8, 26, 9, 31))})
    assert frames[0]["volume"].to_list() == [0]
    assert frames[0]["datetime"].to_list() == [_dt.datetime(2026, 8, 26, 9, 31)]


def test_synthesizer_last_quote_time():
    from app.services.stockdata.rt_sources import BarSynthesizer
    syn = BarSynthesizer()
    assert syn.last_quote_time("600000.SH") is None
    syn.update({"600000.SH": _q("600000.SH", 9.0, 100, 900,
                                _dt.datetime(2026, 8, 25, 10, 0, 17))})
    assert syn.last_quote_time("600000.SH") == _dt.datetime(2026, 8, 25, 10, 0, 17)


# ---- NetworkPuller 编排链：TTL 缓存 / mootdx 自举 / 腾讯→新浪降级 ----


@contextmanager
def _mk_puller(monkeypatch, tencent_quotes=None, sina_quotes=None,
               forced=None, mootdx_empty=False):
    """构造注入假源的 NetworkPuller（不触真实网络）。

    FakeSrc.get_minute_recent 返回一根今日 09:31 bar（模拟自举/mootdx 路径出数）；
    mootdx_empty=True 时返回空帧（隔离测试 HTTP 链路，mootdx 视为无数据）。
    """
    from app.services.stockdata.sources import NetworkPuller

    if forced:
        # monkeypatch 自动还原，测试间不泄漏 STOCKDATA_RT_SOURCE
        monkeypatch.setenv("STOCKDATA_RT_SOURCE", forced)
    else:
        # 隔离环境变量：ambient shell 的 STOCKDATA_RT_SOURCE 不得翻转测试行为
        monkeypatch.delenv("STOCKDATA_RT_SOURCE", raising=False)

    class FakeSrc:
        def __init__(self):
            self.calls = []

        def get_minute_recent(self, code, pages=1):
            import pandas as pd
            self.calls.append(code)
            if mootdx_empty:
                return pd.DataFrame()
            day = _dt.date.today().isoformat()
            return pd.DataFrame({
                "datetime": [pd.Timestamp(f"{day} 09:31:00")],
                "open": [1.0], "close": [1.0], "high": [1.0], "low": [1.0],
                "vol": [100.0], "amount": [100.0],
            }).set_index("datetime")

    p = NetworkPuller(factory=lambda: FakeSrc(), workers=1)
    p.tencent = type("T", (), {
        "fetch": staticmethod(lambda syms: tencent_quotes or {}),
        "close": staticmethod(lambda: None)})()
    p.sina = type("S", (), {
        "fetch": staticmethod(lambda syms: sina_quotes or {}),
        "close": staticmethod(lambda: None)})()
    yield p
    p.shutdown()


def test_fetch_many_tencent_primary_sina_fallback(monkeypatch):
    """腾讯只回一只、新浪/mootdx 都拿不到另一只时：只出腾讯那只。"""
    # 固定非交易时段：排除自举路径干扰（自举走 FakeSrc）
    monkeypatch.setattr("app.services.stockdata.sources._in_trading",
                        lambda *a, **k: False)
    ts = _dt.datetime.now().replace(second=0, microsecond=0)
    ok = _q("600000.SH", 9.0, 100_000, 900_000, ts)
    with _mk_puller(monkeypatch, tencent_quotes={"600000.SH": ok},
                    mootdx_empty=True) as p:
        frames = p.fetch_many(["600000.XSHG", "000001.XSHE"])
        syms = {r["symbol"][0] for r in frames}
        assert syms == {"600000.SH"}            # 仅腾讯命中那只


def test_fetch_many_forced_mootdx_skips_http(monkeypatch):
    """STOCKDATA_RT_SOURCE=mootdx：完全不走 HTTP，经 FakeSrc（mootdx 路径）出数。"""
    def boom(syms):
        raise AssertionError("forced=mootdx 不应触 HTTP")

    monkeypatch.setattr("app.services.stockdata.sources._in_trading",
                        lambda *a, **k: True)   # 交易时段才触发自举
    with _mk_puller(monkeypatch, forced="mootdx") as p:
        p.tencent = type("T", (), {"fetch": staticmethod(boom),
                                   "close": staticmethod(lambda: None)})()
        frames = p.fetch_many(["600000.XSHG"])
        assert frames and frames[0]["symbol"][0] == "600000.SH"


def test_fetch_many_forced_sina_no_tencent(monkeypatch):
    """STOCKDATA_RT_SOURCE=sina：不构造腾讯源，走新浪+自举路径。"""
    def boom(syms):
        raise AssertionError("forced=sina 不应触腾讯")

    monkeypatch.setattr("app.services.stockdata.sources._in_trading",
                        lambda *a, **k: True)
    monkeypatch.setenv("STOCKDATA_RT_SOURCE", "sina")
    # 类级雷区：即便未来误构造腾讯源，一旦触网立即爆
    monkeypatch.setattr(TencentRTSource, "fetch", staticmethod(boom))
    p = NetworkPuller(factory=lambda: type("S", (), {
        "get_minute_recent": staticmethod(lambda c, pages=1: __import__("pandas").DataFrame()),
    })(), workers=1)
    try:
        assert p.tencent is None           # 核心回归：sina 模式不构造腾讯源
        assert p.sina is not None          # 新浪仍在
        p.sina = type("Sn", (), {"fetch": staticmethod(lambda syms: {}),
                                 "close": staticmethod(lambda: None)})()
        frames = p.fetch_many(["600000.XSHG"])   # 自举/mootdx 兜底均空帧 → 无数据，全程未触腾讯
        assert frames == []
    finally:
        p.shutdown()


def test_fetch_many_result_ttl_reuses(monkeypatch):
    ts = _dt.datetime.now().replace(second=0, microsecond=0)
    ok = _q("600000.SH", 9.0, 100_000, 900_000, ts)
    calls = []

    def fake_fetch(syms):
        calls.append(list(syms))
        return {"600000.SH": ok}

    # 固定非交易时段 + 禁自举：确保计数只反映 HTTP 批量调用
    monkeypatch.setattr("app.services.stockdata.sources._in_trading",
                        lambda *a, **k: False)
    with _mk_puller(monkeypatch, mootdx_empty=True) as p:
        p.tencent = type("T", (), {"fetch": staticmethod(fake_fetch),
                                   "close": staticmethod(lambda: None)})()
        p.fetch_many(["600000.XSHG"])
        p.fetch_many(["600000.XSHG"])       # TTL 内第二次
        assert len(calls) == 1               # 未再触网


def test_bootstrap_once_per_day(monkeypatch):
    """交易时段冷启动：先 mootdx 自举一次；TTL 内第二轮不再自举。"""
    monkeypatch.setattr("app.services.stockdata.sources._in_trading",
                        lambda *a, **k: True)
    with _mk_puller(monkeypatch, forced="mootdx") as p:
        src_obj = p._source()
        p.fetch_many(["600000.XSHG"])
        n_after_first = len(src_obj.calls)
        assert n_after_first >= 1           # 第一轮自举发生
        p.fetch_many(["600000.XSHG"])       # 结果缓存命中 → 不再自举
        assert len(src_obj.calls) == n_after_first
