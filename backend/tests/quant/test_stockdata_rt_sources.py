# backend/tests/quant/test_stockdata_rt_sources.py
"""rt_sources 单测：解析层纯文本 fixture（2026-08-25 收盘后实测录制）。"""
import datetime as _dt

import pytest

from app.services.stockdata.rt_sources import (
    RTQuote,
    SinaRTSource,
    TencentRTSource,
    parse_sina_payload,
    parse_tencent_payload,
    _tf_to_vendor,
)

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
        headers = {}

        def get(self, url, timeout=None):
            captured["url"] = url
            captured["timeout"] = timeout
            if "gtimg" in url:
                body = _TENCENT_LINE
            else:
                body = _SINA_LINE
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
        headers = {}

        def get(self, url, timeout=None):
            raise OSError("network down")

    t = TencentRTSource()
    t._session = BoomSession()
    assert t.fetch(["600000.SH"]) == {}
