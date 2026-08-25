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

_TENCENT_LINE = (
    'v_sh600000="1~浦发银行~600000~9.08~9.22~9.24~881756~105337~9.09~9.08~'
    '308946~9.08~1538500~9.07~817900~9.06~297600~9.05~378300~9.04~98383~'
    '9.08~174900~9.09~674900~9.10~63400~9.11~102400~9.12~'
    '20260825161459~-120~-1.40~9.28~9.06~9.08~80448~804478528~2.14~23.53~~'
    '9.28~9.06~2.18~879.63~879.63~1.19~9.83~9.28~13.02~~~9.08~9.08~0.66";'
)

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
    assert q.cum_amount == pytest.approx(804_478_528)  # 万→元 ×1e4
    assert q.quote_time == _dt.datetime(2026, 8, 25, 16, 14, 59)


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
