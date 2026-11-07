"""通达信官网盘后包解析探针。

合成包按真实 ``.md1`` 512 字节布局构造：``[0:4]`` 4 字节头，``[4:12]`` 昨收，
``[12:44]`` 开高低收，``[56:64]`` 成交量（股，uint64），``[72:80]`` 成交额（元，double）。
brief 里的示例 ``_build_zip`` 少了开头 4 字节，照抄会让 ``close`` 落在错位的字节上；
断言保持原样。另外 brief 的合成包只有 3 行且只有沪市，因此本模块用 autouse fixture
把「必需市场 / 每市场有价行下限」放宽——这两项在 ``test_default_*`` 里用真实阈值覆盖。
"""

import io
import math
import struct
import zipfile
from datetime import date

import polars as pl
import pytest
import requests

from app.quant.data_verify.probes import tdx_package
from app.quant.data_verify.probes.tdx_package import fetch_daily_package, parse_package

SCHEMA = {
    "symbol": pl.String,
    "date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
    "amount": pl.Float64,
}


@pytest.fixture(autouse=True)
def _relax_completeness_guards(monkeypatch):
    """合成包只有几行、只含单个市场；完整性与下限用专项用例验证。"""
    monkeypatch.setattr(tdx_package, "TDX_REQUIRED_MARKETS", ("sh",))
    monkeypatch.setattr(tdx_package, "TDX_BJ_FIRST_DAY", "99999999")
    monkeypatch.setattr(tdx_package, "TDX_MIN_PRICED", {"sh": 1, "sz": 1, "bj": 1})


def _cod_record(code: str, seq: int, name: str = "测试") -> bytes:
    record = code.encode().ljust(6, b"\x00") + b"\x00" * 26 + struct.pack("<H", seq)
    record += b"\x00" * 6 + name.encode("gbk") + b"\x00" * 30
    return record.ljust(150, b"\x00")


def _md1_block(
    close: float = 10.0,
    *,
    prev_close: float = 9.9,
    open_: float = 10.0,
    high: float = 10.2,
    low: float = 9.8,
    volume: int = 1_000_000,
    amount: float = 1e7,
) -> bytes:
    block = b"\x00" * 4 + struct.pack("<d", prev_close)
    block += struct.pack("<4d", open_, high, low, close)
    block += b"\x00" * 12 + struct.pack("<Q", volume) + b"\x00" * 8
    block += struct.pack("<d", amount)
    return block.ljust(512, b"\x00")


def _zip_bytes(markets: dict[str, tuple[bytes, bytes]], ymd: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for market, (cod, md1) in markets.items():
            z.writestr(f"{market}{ymd[2:]}.cod", cod)
            z.writestr(f"{market}{ymd[2:]}.md1", md1)
    return buf.getvalue()


def _package(
    ymd: str = "20261106",
    markets: tuple[str, ...] = ("sh",),
    n: int = 3,
    codes: tuple[str, ...] | None = None,
    **block_kwargs,
) -> bytes:
    codes = codes or tuple(f"60{i:04d}" for i in range(n))
    payload: dict[str, tuple[bytes, bytes]] = {}
    for market in markets:
        cod = b"".join(_cod_record(code, i) for i, code in enumerate(codes))
        md1 = b"".join(_md1_block(**block_kwargs) for _ in codes)
        payload[market] = (cod, md1)
    return _zip_bytes(payload, ymd)


def _build_zip(ymd="20261106", n=3, close=10.0):
    cod = b""
    md1 = b""
    for i in range(n):
        code = f"60{i:04d}".encode()
        rec = code.ljust(6, b"\x00") + b"\x00" * 26 + struct.pack("<H", i) + b"\x00" * 6
        rec += "测试".encode("gbk") + b"\x00" * 30
        cod += rec.ljust(150, b"\x00")
        block = _md1_block(close=close)
        md1 += block
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(f"sh{ymd[2:]}.cod", cod)
        z.writestr(f"sh{ymd[2:]}.md1", md1)
    return buf.getvalue()


def test_parse_package_rows():
    df = parse_package(_build_zip(), "20261106")
    assert df.height == 3
    assert df["symbol"][0] == "600000.SH"
    assert df["close"][0] == 10.0


def test_missing_market_file_raises():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("sh261106.cod", b"")
    with pytest.raises(RuntimeError):
        parse_package(buf.getvalue(), "20261106")


def test_output_schema_units_and_date():
    df = parse_package(_package(), "20261106")
    assert dict(df.schema) == SCHEMA
    assert df["symbol"].to_list() == ["600000.SH", "600001.SH", "600002.SH"]
    assert df["date"].dtype == pl.Date
    assert df["date"][0] == date(2026, 11, 6)
    assert df["open"][0] == 10.0
    assert df["high"][0] == 10.2
    assert df["low"][0] == 9.8
    assert df["volume"][0] == 1_000_000          # 股，原样不换算
    assert df["amount"][0] == 10_000_000.0       # 元


def test_all_three_markets_use_symbol_suffix():
    content = _zip_bytes(
        {
            "sh": (_cod_record("600000", 0), _md1_block()),
            "sz": (_cod_record("000001", 0), _md1_block()),
            "bj": (_cod_record("920000", 0), _md1_block()),
        },
        "20261106",
    )
    df = parse_package(content, "20261106")
    assert dict(df.schema) == SCHEMA
    assert df["symbol"].to_list() == ["600000.SH", "000001.SZ", "920000.BJ"]


def test_bj_files_missing_before_first_day_is_allowed():
    content = _package(ymd="20220104", markets=("sh", "sz"))
    df = parse_package(content, "20220104")
    assert df.height == 6
    assert set(df["symbol"]) == {"600000.SH", "600001.SH", "600002.SH",
                                 "600000.SZ", "600001.SZ", "600002.SZ"}


def test_bj_files_missing_after_first_day_raises(monkeypatch):
    monkeypatch.setattr(tdx_package, "TDX_BJ_FIRST_DAY", "20220506")
    content = _package(ymd="20220506", markets=("sh", "sz"))
    with pytest.raises(RuntimeError, match="bj"):
        parse_package(content, "20220506")


def test_default_required_market_missing_raises(monkeypatch):
    monkeypatch.setattr(tdx_package, "TDX_REQUIRED_MARKETS", ("sh", "sz"))
    with pytest.raises(RuntimeError, match="sz"):
        parse_package(_build_zip(), "20261106")


def test_truncated_blocks_raise():
    content = _package()
    buf = io.BytesIO(content)
    with zipfile.ZipFile(buf) as z:
        cod = z.read("sh261106.cod")
        md1 = z.read("sh261106.md1")
    with pytest.raises(RuntimeError, match="整块"):
        parse_package(_zip_bytes({"sh": (cod, md1[:-1])}, "20261106"), "20261106")


def test_cod_md1_count_mismatch_raises():
    cod = _cod_record("600000", 0)
    md1 = _md1_block() + _md1_block()
    with pytest.raises(RuntimeError, match="对不上"):
        parse_package(_zip_bytes({"sh": (cod, md1)}, "20261106"), "20261106")


def test_non_six_digit_code_raises():
    with pytest.raises(RuntimeError, match="非 6 位"):
        parse_package(_package(codes=("60001X",)), "20261106")


def test_duplicate_code_raises():
    cod = _cod_record("600000", 0) + _cod_record("600000", 1)
    md1 = _md1_block() + _md1_block()
    with pytest.raises(RuntimeError, match="重复"):
        parse_package(_zip_bytes({"sh": (cod, md1)}, "20261106"), "20261106")


def test_duplicate_sequence_raises():
    cod = _cod_record("600000", 0) + _cod_record("600001", 0)
    md1 = _md1_block() + _md1_block()
    with pytest.raises(RuntimeError, match="重复"):
        parse_package(_zip_bytes({"sh": (cod, md1)}, "20261106"), "20261106")


@pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf])
def test_non_finite_price_raises(bad):
    with pytest.raises(RuntimeError, match="非有限"):
        parse_package(_package(n=1, close=bad), "20261106")


def test_zero_close_rows_are_skipped():
    cod = _cod_record("600000", 0) + _cod_record("880751", 1)
    md1 = _md1_block(close=10.0) + _md1_block(close=0.0)
    df = parse_package(_zip_bytes({"sh": (cod, md1)}, "20261106"), "20261106")
    assert df.height == 1
    assert df["symbol"].to_list() == ["600000.SH"]


def test_priced_row_without_name_raises():
    cod = _cod_record("600000", 0, name="")
    with pytest.raises(RuntimeError, match="名称"):
        parse_package(_zip_bytes({"sh": (cod, _md1_block())}, "20261106"), "20261106")


def test_broken_gbk_name_is_tolerated():
    """真实 2026-11-06 包 sh 588490 的名称在 32 字节处被截断半个汉字。"""
    cod = _cod_record("588490", 0, name="\u79d1\u6280ETF")
    cod = cod[:40] + b"\xbf" + cod[41:]
    df = parse_package(_zip_bytes({"sh": (cod, _md1_block())}, "20261106"), "20261106")
    assert df.height == 1


def test_default_priced_floor_rejects_tiny_market(monkeypatch):
    monkeypatch.setattr(tdx_package, "TDX_MIN_PRICED", {"sh": 10000, "sz": 3000, "bj": 50})
    with pytest.raises(RuntimeError, match="下限"):
        parse_package(_build_zip(), "20261106")


def test_corrupt_zip_raises():
    with pytest.raises(RuntimeError):
        parse_package(b"PK\x03\x04\x00\x00not really a zip", "20261106")


def test_invalid_ymd_raises_value_error():
    with pytest.raises(ValueError):
        parse_package(_build_zip(), "2026-11-06")


class _Response:
    def __init__(self, status_code: int, content: bytes):
        self.status_code = status_code
        self.content = content

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")


def test_fetch_404_raises_value_error(monkeypatch):
    monkeypatch.setattr(tdx_package.requests, "get", lambda *a, **k: _Response(404, b""))
    with pytest.raises(ValueError, match="2026-11-07"):
        fetch_daily_package(date(2026, 11, 7))


def test_fetch_non_zip_content_raises(monkeypatch):
    monkeypatch.setattr(tdx_package.requests, "get", lambda *a, **k: _Response(200, b"<html>"))
    with pytest.raises(RuntimeError, match="zip"):
        fetch_daily_package(date(2026, 11, 6))


def test_fetch_http_error_raises_runtime_error(monkeypatch):
    def _boom(*args, **kwargs):
        raise requests.ConnectionError("boom")

    monkeypatch.setattr(tdx_package.requests, "get", _boom)
    with pytest.raises(RuntimeError, match="boom"):
        fetch_daily_package(date(2026, 11, 6))


def test_fetch_uses_expected_url_and_hard_timeout(monkeypatch):
    seen: dict[str, object] = {}

    def _get(url, **kwargs):
        seen["url"] = url
        seen["timeout"] = kwargs.get("timeout")
        return _Response(200, _build_zip())

    monkeypatch.setattr(tdx_package.requests, "get", _get)
    df = fetch_daily_package(date(2026, 11, 6))
    assert seen["url"] == "https://www.tdx.com.cn/products/data/data/g4day/20261106.zip"
    assert seen["timeout"] == (10.0, 90.0)
    assert df.height == 3
