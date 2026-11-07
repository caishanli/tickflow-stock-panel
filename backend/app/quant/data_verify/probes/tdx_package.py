"""通达信官网盘后包 — 某交易日全市场日线（含成交额）。

官网每个交易日发布一个增量包（HTTP 下载约 2.7MB），解压后每个市场一对文件：

- ``{market}{YYMMDD}.cod``：代码表，150 字节/条；
- ``{market}{YYMMDD}.md1``：行情块，512 字节/块。

布局移植自 a-stock-data §1.3 的 ``_tdx_parse_package``（其二进制布局参考
jing2uo/tdx2db 的 ``tdx/merge.go``，MIT）。相对参考实现只改两处对外口径：

- ``symbol`` 输出为 ``600000.SH`` / ``000001.SZ`` / ``920000.BJ``；
- ``date`` 列为 :class:`polars.Date`，不再携带 ``market`` / ``name`` / ``prev_close``。

``volume`` 单位是「股」、``amount`` 单位是「元」（个股口径；指数等特殊代码为通达信原值）。
非交易日、或当天包尚未发布时官网返回 404，:func:`fetch_daily_package` 抛 ``ValueError``，
不返回空表。
"""

from __future__ import annotations

import io
import math
import re
import struct
import zipfile
import zlib
from datetime import date, datetime

import polars as pl
import requests

#: 盘后包下载地址（``ymd`` 为 ``YYYYMMDD``）。
TDX_PACKAGE_URL = "https://www.tdx.com.cn/products/data/data/g4day/{ymd}.zip"

#: 盘后包从这一天起带北交所文件（2026-09-20 二分实测：20220505 无、20220506 有）。
TDX_BJ_FIRST_DAY = "20220506"

#: 必须存在的市场。北交所按 ``TDX_BJ_FIRST_DAY`` 单独判断。
TDX_REQUIRED_MARKETS: tuple[str, ...] = ("sh", "sz")

#: 每个市场「有收盘价」的最少行数。实测 2021-08-16 ~ 2026-09-18 共 11 个包的最小值：
#: 沪 16202、深 3641（2022 年以前深市大部分代码当日无价）、京 87，
#: 这里取明显低于实测最小值的整数，用来发现残缺 / 格式变更。
TDX_MIN_PRICED: dict[str, int] = {"sh": 10000, "sz": 3000, "bj": 50}

_TDX_MARKETS: tuple[str, ...] = ("sh", "sz", "bj")
_MARKET_SUFFIX = {"sh": "SH", "sz": "SZ", "bj": "BJ"}
_COD_RECORD_SIZE = 150
_MD1_BLOCK_SIZE = 512

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)

_SCHEMA: list[tuple[str, type[pl.DataType] | pl.DataType]] = [
    ("symbol", pl.String),
    ("date", pl.Date),
    ("open", pl.Float64),
    ("high", pl.Float64),
    ("low", pl.Float64),
    ("close", pl.Float64),
    ("volume", pl.Int64),
    ("amount", pl.Float64),
]

__all__ = [
    "TDX_BJ_FIRST_DAY",
    "TDX_MIN_PRICED",
    "TDX_PACKAGE_URL",
    "TDX_REQUIRED_MARKETS",
    "fetch_daily_package",
    "parse_package",
]


def _trade_day(ymd: str) -> date:
    if not re.fullmatch(r"[0-9]{8}", ymd):
        raise ValueError(f"ymd 必须是 YYYYMMDD，收到 {ymd!r}")
    try:
        return datetime.strptime(ymd, "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError(f"ymd 不是合法日期：{ymd!r}") from exc


def _market_missing(market: str, ymd: str) -> bool:
    """该市场整个文件对缺失时是否可接受。

    生产口径下只有「2022-05-06 之前的北交所」可以缺；``TDX_REQUIRED_MARKETS``
    之外的其它市场按调用方放宽处理（测试用小型合成包）。
    """
    if market in TDX_REQUIRED_MARKETS:
        return False
    if market == "bj":
        return ymd < TDX_BJ_FIRST_DAY
    return True


def parse_package(content: bytes, ymd: str) -> pl.DataFrame:
    """解析通达信每日增量包 → ``symbol,date,open,high,low,close,volume,amount``。

    结构不完整（代码表/行情块长度不是整块、条数对不上、代码非 6 位数字或重复、
    行情块序号重复、价格/成交额非有限、某市场有价记录低于实测下限、有价却没名称）
    一律抛 ``RuntimeError``——「源格式变了」与「参数写错」必须分开，不能把
    残缺文件当成全市场截面返回。
    """
    trade_day = _trade_day(ymd)
    rows: list[tuple[str, date, float, float, float, float, int, float]] = []
    try:
        archive = zipfile.ZipFile(io.BytesIO(content))
        names = set(archive.namelist())
        for market in _TDX_MARKETS:
            cod_name = f"{market}{ymd[2:]}.cod"
            md1_name = f"{market}{ymd[2:]}.md1"
            cod_present = cod_name in names
            md1_present = md1_name in names
            if not cod_present and not md1_present:
                if _market_missing(market, ymd):
                    continue
                raise RuntimeError(f"通达信盘后包缺少 {cod_name}/{md1_name}，格式可能已变")
            if not cod_present or not md1_present:
                raise RuntimeError(f"通达信盘后包缺少 {cod_name}/{md1_name}，格式可能已变")

            cod = archive.read(cod_name)
            md1 = archive.read(md1_name)
            if len(cod) % _COD_RECORD_SIZE or len(md1) % _MD1_BLOCK_SIZE:
                raise RuntimeError(f"{market} 代码表或行情块长度不是整块，文件可能被截断")
            if len(cod) // _COD_RECORD_SIZE != len(md1) // _MD1_BLOCK_SIZE:
                raise RuntimeError(
                    f"{market} 代码表 {len(cod) // _COD_RECORD_SIZE} 条、"
                    f"行情块 {len(md1) // _MD1_BLOCK_SIZE} 块，对不上"
                )

            before = len(rows)
            codes: set[str] = set()
            seqs: set[int] = set()
            for offset in range(0, len(cod), _COD_RECORD_SIZE):
                record = cod[offset:offset + _COD_RECORD_SIZE]
                # 11 个真实包（2021-08 ~ 2026-09）的代码全是 6 位 ASCII 数字；
                # 用 replace 解码会把坏字节变成 '\ufffd00000' 放行。
                code = record[0:6].rstrip(b"\x00 ").decode("ascii", "replace")
                seq = struct.unpack("<H", record[32:34])[0]
                if not re.fullmatch(r"[0-9]{6}", code):
                    raise RuntimeError(
                        f"通达信盘后包 {market} 代码表出现非 6 位数字代码 {code!r}，"
                        "格式可能已变"
                    )
                if code in codes or seq in seqs:
                    raise RuntimeError(
                        f"通达信盘后包 {market} 代码表有重复的代码 / 行情块序号"
                        f"（{code!r}, seq={seq}）"
                    )
                codes.add(code)
                seqs.add(seq)

                block = md1[seq * _MD1_BLOCK_SIZE:(seq + 1) * _MD1_BLOCK_SIZE]
                if len(block) != _MD1_BLOCK_SIZE:
                    raise RuntimeError(f"{market}{code} 行情块越界（seq={seq}）")
                prev_close = struct.unpack("<d", block[4:12])[0]
                open_, high, low, close = struct.unpack("<4d", block[12:44])
                amount = struct.unpack("<d", block[72:80])[0]
                # NaN 能绕过 close <= 0 被当成有价记录计入下限；真实包里没有非有限值。
                if not all(math.isfinite(v) for v in (prev_close, open_, high, low, close, amount)):
                    raise RuntimeError(
                        f"通达信盘后包 {market}{code} 行情块出现非有限数值，文件可能已损坏"
                    )
                if close <= 0:
                    # 880/881 等通达信自编板块指数当日无价，整块为 0，不是证券。
                    continue
                volume = struct.unpack("<Q", block[56:64])[0]
                # 真实包 2026-11-06 的 sh 588490 名称在 32 字节处截断半个汉字，
                # 名称只是诊断信息，用 replace 解码，不因此拒绝整包。
                raw_name = record[40:72].split(b"\x00")[0]
                name = raw_name.decode("gbk", "replace").strip()
                if not name:
                    raise RuntimeError(
                        f"通达信盘后包 {market}{code} 有价格却没有名称，文件可能已损坏"
                    )
                rows.append((
                    f"{code}.{_MARKET_SUFFIX[market]}",
                    trade_day,
                    round(open_, 4),
                    round(high, 4),
                    round(low, 4),
                    round(close, 4),
                    volume,
                    round(amount, 2),
                ))

            priced = len(rows) - before
            if priced < TDX_MIN_PRICED[market]:
                raise RuntimeError(
                    f"通达信盘后包 {market} 市场只有 {priced} 条有价记录"
                    f"（实测下限 {TDX_MIN_PRICED[market]}），文件可能残缺或格式已变"
                )
    except (zipfile.BadZipFile, zlib.error, EOFError) as exc:
        raise RuntimeError(
            f"通达信盘后包无法解压: {type(exc).__name__}: {exc}"
        ) from exc
    return pl.DataFrame(rows, orient="row", schema=_SCHEMA)


def fetch_daily_package(day: date, *, timeout: float = 90.0) -> pl.DataFrame:
    """下载并解析某一交易日的通达信盘后包。

    ``timeout`` 是硬墙钟（连接 / 读取分别限时）。非交易日或当日包尚未发布时
    官网返回 404，这里抛 ``ValueError``；其余网络错误、非 zip 响应抛 ``RuntimeError``。
    """
    if timeout <= 0:
        raise ValueError(f"timeout 必须为正数，收到 {timeout!r}")
    ymd = day.strftime("%Y%m%d")
    url = TDX_PACKAGE_URL.format(ymd=ymd)
    try:
        response = requests.get(
            url,
            headers={"User-Agent": _UA},
            timeout=(min(10.0, timeout), timeout),
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"请求 {url} 失败: {type(exc).__name__}: {exc}") from exc
    if response.status_code == 404:
        raise ValueError(
            f"{day} 没有通达信盘后包：非交易日、当日包尚未发布（通常收盘后数小时），"
            "或早于官网保留范围（实测 2021-01-04 已没有）"
        )
    try:
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"请求 {url} 失败: {type(exc).__name__}: {exc}") from exc
    content = response.content
    if not content.startswith(b"PK"):
        raise RuntimeError("通达信盘后包不是 zip 文件，可能是错误页")
    return parse_package(content, ymd)
