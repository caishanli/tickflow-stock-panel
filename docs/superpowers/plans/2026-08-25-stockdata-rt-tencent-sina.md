# stockdata 实时价格源优化 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** stockdata 服务实时路径改为 腾讯(主)→新浪(备)→mootdx(兜底) 批量快照降级链，秒级新鲜度替代 mootdx 分钟级延迟。

**Architecture:** 新模块 `rt_sources.py` 提供批量快照源 + 快照→分钟bar合成器；`sources.py` 的 `NetworkPuller.fetch_many` 改为编排链（结果TTL缓存 → 冷启动mootdx自举 → 腾讯批量 → 新浪批量 → mootdx逐只）；`get_realtime_snapshot` 陈旧门槛参数化（3min→10s）。协议/客户端/前端零变更。

**Tech Stack:** Python 3.12 / requests / polars / pytest（asyncio_mode=auto 不涉及，纯同步线程模型）。

## Global Constraints

- Spec：`docs/superpowers/specs/2026-08-25-stockdata-rt-tencent-sina-design.md`
- 分支：`feat/stockdata-rt-tencent-sina`（已从 custom-main 创建，spec 已提交）
- 后端命令一律 `cd backend && uv run --extra dev ...`（dev 依赖不在基础 venv）
- lint: `uv run --extra dev ruff check app`（line-length 100）；类型: `uv run --extra dev mypy app`
- 测试从 `backend/` 目录运行
- 分钟 bar 列恒为 `_MINUTE_COLS = ["symbol","datetime","open","high","low","close","volume","amount"]`；volume 单位**股**、amount 单位**元**（已实测确认，见下）
- 平台代码只有 `.SH`/`.SZ`（无北交所）；指数不参与实时回源
- 禁止在其它处引入 pandas（唯一边界是现有 mootdx 回源路径已有的 pandas 用法）
- 代码不加注释除非模仿周边既有风格（本仓库中文 docstring 风格，函数级简短说明 OK）

**已实测数据事实（2026-08-25 收盘后验证）：**

| 项 | 值 |
|----|----|
| 目标口径（本地分钟分区） | volume=股，amount=元。600000.SH 全天 88,175,600 股；510300.SH 745,256,800 股 |
| 腾讯 field 6（累计量） | 股票与 ETF 均=手：600000→881756、510300→7452568，**统一 ×100 转股** |
| 腾讯 field 37（累计额） | 万元：600000→80448（≈8.0448e8 元），**×1e4 转元** |
| 腾讯 field 30 | 行情时间戳 `YYYYMMDDHHMMSS` |
| 新浪 idx8/idx9 | 累计量=股、累计额=元，**直接用无需换算**；idx30/31=`YYYY-MM-DD`,`HH:MM:SS` |
| 腾讯响应 | GBK；行格式 `v_sh600000="1~浦发银行~600000~9.08~9.22~9.24~..."`，`~` 分隔；空值可能是空串或 `""` |
| 新浪响应 | GBK；需请求头 `Referer: https://finance.sina.com.cn`；停牌/异常标的返回空串字段 |

---

### Task 1: rt_sources.py — 快照源解析层（Tencent/Sina + 归一化 RTQuote）

**Files:**
- Create: `backend/app/services/stockdata/rt_sources.py`
- Create: `backend/tests/quant/test_stockdata_rt_sources.py`

**Interfaces:**
- Produces（后续任务依赖，签名固定）:
```python
@dataclass
class RTQuote:
    symbol: str            # .SH/.SZ 格式
    price: float           # 现价；无效=0.0
    prev_close: float
    open_: float
    high: float
    low: float
    cum_volume: float      # 累计成交量，单位「股」（两源均已归一）
    cum_amount: float      # 累计成交额，单位「元」（两源均已归一）
    quote_time: datetime   # 行情时间戳（naive 本地时间）

def _tf_to_vendor(symbol: str) -> str | None:
    """'600000.SH' -> 'sh600000'；非 SH/SZ 返回 None"""

class TencentRTSource:
    def fetch(self, symbols: list[str]) -> dict[str, RTQuote]: ...
    def close(self) -> None: ...

class SinaRTSource:
    def fetch(self, symbols: list[str]) -> dict[str, RTQuote]: ...
    def close(self) -> None: ...

def parse_tencent_payload(text: str) -> dict[str, RTQuote]:
def parse_sina_payload(text: str) -> dict[str, RTQuote]:
```

- 解析函数独立于 HTTP（纯文本→dict），便于测试与复用。
- 无效处理规则：price<=0 或缺失 → 该标的不进返回 dict（停牌不造 bar）；字段空串按 0 处理但 price 为 0 即丢弃。
- quote_time 解析失败时用 `datetime.now()` 替代。

- [ ] **Step 1: Write the failing tests**

测试文件 `backend/tests/quant/test_stockdata_rt_sources.py` 开头：

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_stockdata_rt_sources.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.services.stockdata.rt_sources'`

- [ ] **Step 3: Implement rt_sources.py**

```python
"""实时快照源：腾讯/新浪批量 HTTP 拉取 + 统一归一化 RTQuote。

设计 spec：docs/superpowers/specs/2026-08-25-stockdata-rt-tencent-sina-design.md。
单位口径（实测校准 2026-08-25）：RTQuote.cum_volume 一律「股」、cum_amount 一律「元」；
腾讯原始量=手(×100)、额=万元(×1e4)；新浪量=股、额=元直用。
"""
from __future__ import annotations

import datetime as _dt
import logging
import re
from dataclasses import dataclass

import requests

logger = logging.getLogger("app.services.stockdata.rt_sources")

_UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}
_TENCENT_URL = "https://qt.gtimg.cn/q="
_SINA_URL = "https://hq.sinajs.cn/list="
_HTTP_TIMEOUT = 3.0


@dataclass
class RTQuote:
    symbol: str
    price: float
    prev_close: float
    open_: float
    high: float
    low: float
    cum_volume: float
    cum_amount: float
    quote_time: _dt.datetime


def _f(v: str) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _tf_to_vendor(symbol: str) -> str | None:
    pure, _, suf = symbol.rpartition(".")
    if not pure:
        return None
    if suf == "SH":
        return f"sh{pure}"
    if suf == "SZ":
        return f"sz{pure}"
    return None


def _parse_ts(raw: str) -> _dt.datetime:
    for fmt in ("%Y%m%d%H%M%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return _dt.datetime.strptime(raw.strip(), fmt)
        except ValueError:
            continue
    return _dt.datetime.now()


def parse_tencent_payload(text: str) -> dict[str, RTQuote]:
    """腾讯 v_shXXXXXX="1~名称~code~现价~昨收~今开~量(手)~..." ~分隔 payload 解析。"""
    out: dict[str, RTQuote] = {}
    for m in re.finditer(r'v_(?:sh|sz)(\d{6})="([^"]*)"', text):
        code, body = m.group(1), m.group(2)
        v = body.split("~")
        if len(v) < 38:
            continue
        price = _f(v[3])
        if price <= 0:
            continue
        suffix = "SH" if m.group(0).startswith("v_sh") else "SZ"
        out[f"{code}.{suffix}"] = RTQuote(
            symbol=f"{code}.{suffix}",
            price=price,
            prev_close=_f(v[4]),
            open_=_f(v[5]),
            high=_f(v[33]),
            low=_f(v[34]),
            cum_volume=_f(v[6]) * 100.0,
            cum_amount=_f(v[37]) * 1e4,
            quote_time=_parse_ts(v[30]),
        )
    return out


def parse_sina_payload(text: str) -> dict[str, RTQuote]:
    """新浪 var hq_str_shXXXXXX="名,今开,昨收,现价,最高,最低,...,量(股),额(元),...,日期,时间" 解析。"""
    out: dict[str, RTQuote] = {}
    for m in re.finditer(r'hq_str_(?:sh|sz)(\d{6})="([^"]*)"', text):
        code, body = m.group(1), m.group(2)
        v = body.split(",")
        if len(v) < 32:
            continue
        price = _f(v[3])
        if price <= 0:
            continue
        suffix = "SH" if m.group(0).startswith("hq_str_sh") else "SZ"
        out[f"{code}.{suffix}"] = RTQuote(
            symbol=f"{code}.{suffix}",
            price=price,
            prev_close=_f(v[2]),
            open_=_f(v[1]),
            high=_f(v[4]),
            low=_f(v[5]),
            cum_volume=_f(v[8]),
            cum_amount=_f(v[9]),
            quote_time=_parse_ts(f"{v[30]} {v[31]}"),
        )
    return out


class _HttpSource:
    _url: str
    _parser: staticmethod
    _headers: dict[str, str]

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update(self._headers)

    def fetch(self, symbols: list[str]) -> dict[str, RTQuote]:
        vendor = [s for s in (_tf_to_vendor(x) for x in symbols) if s]
        if not vendor:
            return {}
        try:
            resp = self._session.get(
                self._url + ",".join(vendor),
                timeout=float(os.getenv("STOCKDATA_RT_HTTP_TIMEOUT", "") or _HTTP_TIMEOUT))
            resp.raise_for_status()
            resp.encoding = "gbk"
            return self._parser(resp.text)
        except Exception as e:  # noqa: BLE001
            logger.warning("[rt_sources] %s 批量拉取失败(%s 只): %s",
                           type(self).__name__, len(vendor), e)
            return {}

    def close(self) -> None:
        self._session.close()


class TencentRTSource(_HttpSource):
    _url = _TENCENT_URL
    _parser = staticmethod(parse_tencent_payload)
    _headers = dict(_UA)


class SinaRTSource(_HttpSource):
    _url = _SINA_URL
    _parser = staticmethod(parse_sina_payload)
    _headers = {**_UA, "Referer": "https://finance.sina.com.cn"}
```

注意：文件顶部 import 区补 `import os`（`fetch` 里读了 env）。若 mypy 对 `_parser = staticmethod(...)` 子类覆写报类型错，把 `_parser` 声明为 `Callable[[str], dict[str, RTQuote]]` 类注解并在子类用普通函数赋值。

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_stockdata_rt_sources.py -v`
Expected: 全部 PASS

- [ ] **Step 5: Lint + typecheck + commit**

Run: `cd backend && uv run --extra dev ruff check app/services/stockdata/rt_sources.py app && uv run --extra dev mypy app/services/stockdata/rt_sources.py`
Expected: 通过

```bash
git add backend/app/services/stockdata/rt_sources.py backend/tests/quant/test_stockdata_rt_sources.py
git commit -m "feat(stockdata): 腾讯/新浪批量实时快照源+归一化RTQuote"
```

---

### Task 2: rt_sources.py — BarSynthesizer 快照→分钟bar合成器

**Files:**
- Modify: `backend/app/services/stockdata/rt_sources.py`（追加 BarSynthesizer）
- Modify: `backend/tests/quant/test_stockdata_rt_sources.py`（追加合成器测试）

**Interfaces:**
- Consumes: Task 1 的 `RTQuote`
- Produces:
```python
class BarSynthesizer:
    def update(self, quotes: dict[str, RTQuote], now: datetime | None = None
               ) -> list[pl.DataFrame]:
        """并入一批快照，返回本次新产出/更新的 bar 帧（每帧含单/多标的，
        列=_MINUTE_COLS，symbol=.SH/.SZ，datetime=Datetime('us')，volume=股，amount=元）。
        同一分钟多次 update 返回该标的当前分钟 bar 的最新整行（调用方 upsert）。
        无有效产出返回 []。"""

    def last_quote_time(self, symbol: str) -> datetime | None:
        """该标的最近一次快照时间（陈旧判定用）；从未见过返回 None。"""

    def reset_if_new_day(self, day: date) -> None: ...
```

- 合成规则（spec §方案）：同一分钟更新 H/L/C 与差分；跨分钟开新 bar（open 取上一拍 price）；
  差分负值 clamp 0 并重建基线；首拍无基线只建价格 bar、量额记 0；
  bar datetime = quote_time 向下取整分钟（quote_time 异常→now 取整）。
- 线程安全：内部 threading.Lock 保护 per-symbol 状态 dict。
- 内存：状态仅 watchlist 规模；reset_if_new_day 由调用方换日触发清空。

- [ ] **Step 1: Write the failing tests**

追加到 `backend/tests/quant/test_stockdata_rt_sources.py`：

```python
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
```

文件顶部补 `import polars as pl`（multi_symbol 测试用到）。

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_stockdata_rt_sources.py -v -k synthesizer`
Expected: FAIL — `ImportError: cannot import name 'BarSynthesizer'`

- [ ] **Step 3: Implement BarSynthesizer**

追加到 `rt_sources.py`：

```python
_MINUTE_COLS = ["symbol", "datetime", "open", "high", "low", "close",
                "volume", "amount"]


class _SymState:
    __slots__ = ("last_price", "last_vol", "last_amt",
                 "bar_dt", "o", "h", "l", "c", "v", "amt")

    def __init__(self) -> None:
        self.last_price = 0.0
        self.last_vol = 0.0
        self.last_amt = 0.0
        self.bar_dt: _dt.datetime | None = None
        self.o = self.h = self.l = self.c = 0.0
        self.v = self.amt = 0.0


def _floor_minute(ts: _dt.datetime) -> _dt.datetime:
    return ts.replace(second=0, microsecond=0)


class BarSynthesizer:
    """连续快照 → 分钟 bar（线程安全；per-symbol 状态仅 watchlist 规模内存）。

    update 返回：本次封口的历史 bar 行 + 各标的当前分钟 bar 最新整行。
    调用方按 (symbol, datetime) upsert 幂等合并。
    """

    def __init__(self) -> None:
        import threading
        self._lock = threading.Lock()
        self._state: dict[str, _SymState] = {}
        self._quote_ts: dict[str, _dt.datetime] = {}
        self._day: _dt.date = _dt.date.today()

    def reset_if_new_day(self, day: _dt.date) -> None:
        with self._lock:
            if day != self._day:
                self._state.clear()
                self._quote_ts.clear()
                self._day = day

    def last_quote_time(self, symbol: str) -> _dt.datetime | None:
        with self._lock:
            return self._quote_ts.get(symbol)

    def update(self, quotes: dict[str, RTQuote],
               now: _dt.datetime | None = None) -> list[pl.DataFrame]:
        now = now or _dt.datetime.now()
        done_rows: list[tuple] = []      # 跨分钟封口的旧 bar
        cur_rows: list[tuple] = []       # 每标的当前 bar 最新整行
        with self._lock:
            for sym, q in quotes.items():
                if q.price <= 0:
                    continue
                st = self._state.get(sym)
                if st is None:
                    st = _SymState()
                    self._state[sym] = st
                has_bar = st.bar_dt is not None
                dv = max(q.cum_volume - st.last_vol, 0.0) if has_bar else 0.0
                da = max(q.cum_amount - st.last_amt, 0.0) if has_bar else 0.0
                bar_dt = _floor_minute(q.quote_time or now)
                if not has_bar:
                    # 首拍：只建价格 bar，量额记 0（保守不虚增）
                    st.o = st.h = st.l = st.c = q.price
                    st.v = st.amt = 0.0
                    st.bar_dt = bar_dt
                elif bar_dt != st.bar_dt:
                    # 封口旧 bar → done；开新 bar：上一拍价格开盘
                    done_rows.append((sym, st.bar_dt, st.o, st.h, st.l,
                                      st.c, st.v, st.amt))
                    st.o = st.c
                    st.h = max(st.c, q.price)
                    st.l = min(st.c, q.price)
                    st.c = q.price
                    st.v = dv
                    st.amt = da
                    st.bar_dt = bar_dt
                else:
                    st.h = max(st.h, q.price)
                    st.l = min(st.l, q.price)
                    st.c = q.price
                    st.v += dv
                    st.amt += da
                st.last_price = q.price
                st.last_vol = q.cum_volume
                st.last_amt = q.cum_amount
                self._quote_ts[sym] = q.quote_time or now
                cur_rows.append((sym, st.bar_dt, st.o, st.h, st.l,
                                 st.c, st.v, st.amt))
        if not cur_rows and not done_rows:
            return []
        all_rows = done_rows + cur_rows
        cols = list(zip(*all_rows))
        df = pl.DataFrame({
            "symbol": list(cols[0]), "datetime": list(cols[1]),
            "open": list(cols[2]), "high": list(cols[3]), "low": list(cols[4]),
            "close": list(cols[5]), "volume": list(cols[6]),
            "amount": list(cols[7]),
        }, schema_overrides={"datetime": pl.Datetime("us")})
        return [df]
```

注意：同标的同一批内不会重复（quotes 是 dict）；`cur_rows` 与 `done_rows` 可能出现同 symbol 的多行，调用方按 (symbol, datetime) unique keep="last" 合并（minute_store.update 已是此语义）。

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_stockdata_rt_sources.py -v`
Expected: 全部 PASS（含 Task 1 用例）

- [ ] **Step 5: Lint + typecheck + commit**

Run: `cd backend && uv run --extra dev ruff check app && uv run --extra dev mypy app/services/stockdata/rt_sources.py`
Expected: 通过

```bash
git add backend/app/services/stockdata/rt_sources.py backend/tests/quant/test_stockdata_rt_sources.py
git commit -m "feat(stockdata): BarSynthesizer 快照→分钟bar合成器"
```

---

### Task 3: sources.py — NetworkPuller 编排链改造

**Files:**
- Modify: `backend/app/services/stockdata/sources.py`（NetworkPuller 类 ~L233-293）
- Modify: `backend/tests/quant/test_stockdata_rt_sources.py`（追加链路测试）

**Interfaces:**
- Consumes: Task 1 `TencentRTSource/SinaRTSource`、Task 2 `BarSynthesizer`
- Produces:
```python
class NetworkPuller:
    def fetch_many(self, codes: list[str]) -> list[pl.DataFrame]:
        # 语义不变：返回非空分钟 bar 帧列表（_MINUTE_COLS，.SH/.SZ 符号）
        # 内部改为：① 结果 TTL 缓存过滤 ② mootdx 冷启动自举（每标的每日一次）
        #           ③ 腾讯批量 ④ 新浪批量 ⑤ mootdx 逐只兜底
    def shutdown(self) -> None:  # 不变，另关闭 http 会话
```

- env：`STOCKDATA_RT_SOURCE` = auto(默认)/tencent/sina/mootdx；auto=③④⑤ 链，tencent/sina=仅该源+mootdx 兜底？——**否**：强制单源即只走该源，失败不留兜底（便于隔离排障与一键回滚 mootdx）。`mootdx`=完全走旧逐只路径。
- 自举判定：`bootstrap_done: set[symbol]`（每日重置）；交易时段内、结果缓存与合成器均无该标的当日记录时，先走 `_pull_recent_guarded` 拉「今日迄今」一次，成功后标记 done。
- 结果缓存：`{symbol: (monotonic_ts, frame)}`，TTL=`STOCKDATA_RT_RESULT_TTL`(默认3s)；命中直接复用帧不再触网。合成器产出的帧进缓存。
- 线程安全：编排方法整体持锁（batch 级 single-flight——并发 fetch_many 串行化，避免重复批量请求；mootdx 兜底仍在池内并行）。

- [ ] **Step 1: Write the failing tests**

追加到 `backend/tests/quant/test_stockdata_rt_sources.py`：

```python
def _mk_puller(monkeypatch, tencent_quotes=None, sina_quotes=None,
               forced=None, mootdx_empty=False):
    """构造注入假源的 NetworkPuller（不触真实网络）。

    FakeSrc.get_minute_recent 返回一根今日 09:31 bar（模拟自举/mootdx 路径出数）；
    mootdx_empty=True 时返回空帧（隔离测试 HTTP 链路，mootdx 视为无数据）。
    """
    import os

    from app.services.stockdata.sources import NetworkPuller

    os.environ.pop("STOCKDATA_RT_SOURCE", None)
    if forced:
        os.environ["STOCKDATA_RT_SOURCE"] = forced

    class FakeSrc:
        def __init__(self):
            self.calls = []

        def get_minute_recent(self, code, pages=1):
            self.calls.append(code)
            if mootdx_empty:
                import pandas as pd
                return pd.DataFrame()
            import pandas as pd
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
    os.environ["STOCKDATA_RT_SOURCE"] = "mootdx"
    try:
        with _mk_puller(monkeypatch) as p:
            p.tencent = type("T", (), {"fetch": staticmethod(boom),
                                       "close": staticmethod(lambda: None)})()
            frames = p.fetch_many(["600000.XSHG"])
            assert frames and frames[0]["symbol"][0] == "600000.SH"
    finally:
        os.environ.pop("STOCKDATA_RT_SOURCE", None)


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

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_stockdata_rt_sources.py -v -k "fetch_many or bootstrap"`
Expected: FAIL — `NetworkPuller` 无 `tencent` 属性 / 断言失败

- [ ] **Step 3: Implement the chain in NetworkPuller**

`backend/app/services/stockdata/sources.py` 改造：

(a) imports 区加 `from . import rt_sources as _rt`。

(b) **删除**旧方法 `fetch_minute` 与 per-symbol single-flight（`self._single` 字段一并删）；
`_fetch_one` 重命名为 `_pull_mootdx_one`，完整新实现：

```python
    def _pull_mootdx_one(self, code_tf: str) -> pl.DataFrame | None:
        """mootdx 单只实时分钟拉取（超时重置线程源）。返回帧或 None。"""
        try:
            df = _pull_recent_guarded(self._source(), code_tf)
        except TimeoutError:
            self._local.src = None
            return None
        if df is None or df.empty:
            return None
        pdf = df.reset_index()
        pdf["symbol"] = code_tf
        for c in _MINUTE_COLS:
            if c not in pdf.columns:
                pdf[c] = None
        frame = _as_datetime(pl.from_pandas(pdf[_MINUTE_COLS]))
        return frame if not frame.is_empty() else None
```

(c) **替换整个 `fetch_many` 及新增辅助方法**（保留 `_pool/_local/_factory/workers` 原有字段与 `_source()` 不变）：

```python
class NetworkPuller:
    """共享实时拉取编排：结果TTL缓存 → mootdx冷启动自举 → 腾讯批量 → 新浪批量
    → mootdx逐只兜底。batch 级加锁串行化，避免并发客户端重复批量请求；
    forced=mootdx 时完全走旧逐只路径（一键回滚）。"""

    def __init__(self, factory=None, workers=16):
        self._factory = factory
        self._workers = max(1, workers)
        self._local = threading.local()
        self._pool = ThreadPoolExecutor(
            max_workers=self._workers, thread_name_prefix="stockdata-pull")
        self._chain_lock = threading.Lock()
        try:
            self._result_ttl = float(os.getenv("STOCKDATA_RT_RESULT_TTL", "") or 3.0)
        except (TypeError, ValueError):
            self._result_ttl = 3.0
        self._result_cache: dict[str, tuple[float, pl.DataFrame]] = {}
        self._bootstrapped: set[str] = set()
        self._bootstrap_day: _dt.date | None = None
        self.synth = _rt.BarSynthesizer()
        forced = os.getenv("STOCKDATA_RT_SOURCE", "auto") or "auto"
        self.tencent = None if forced == "mootdx" else _rt.TencentRTSource()
        self.sina = None if forced in ("mootdx", "tencent") else _rt.SinaRTSource()

    def _cache_put(self, tf: str, frame: pl.DataFrame) -> None:
        self._result_cache[tf] = (time.monotonic(), frame)

    def _bootstrap_needed(self, code_tf: str) -> bool:
        """交易时段冷启动自举判定：当日既无合成记录也无结果缓存。"""
        if not _in_trading():
            return False
        today = _dt.date.today()
        if self._bootstrap_day != today:
            self._bootstrap_day = today
            self._bootstrapped.clear()
        if code_tf in self._bootstrapped or code_tf in self._result_cache \
                or self.synth.last_quote_time(code_tf) is not None:
            return False
        return True

    def fetch_many(self, codes: list[str]) -> list[pl.DataFrame]:
        with self._chain_lock:
            tf_codes = [_tf_symbol(c) for c in codes]
            out: dict[str, pl.DataFrame] = {}
            todo: list[str] = []
            now_mono = time.monotonic()
            for tf in dict.fromkeys(tf_codes):
                hit = self._result_cache.get(tf)
                if hit is not None and now_mono - hit[0] < self._result_ttl:
                    out[tf] = hit[1]
                else:
                    todo.append(tf)
            # ② 冷启动自举：交易时段、无任何当日记录的标的，mootdx 拉「今日迄今」一次
            for tf in list(todo):
                if self._bootstrap_needed(tf):
                    frame = self._pull_mootdx_one(tf)
                    self._bootstrapped.add(tf)
                    if frame is not None and not frame.is_empty():
                        out[tf] = frame
                        self._cache_put(tf, frame)
                        todo.remove(tf)
            # ③④ 腾讯批量 → 新浪批量 → 合成；forced=mootdx 跳过 HTTP 链
            forced = os.getenv("STOCKDATA_RT_SOURCE", "auto") or "auto"
            if forced != "mootdx" and todo:
                quotes: dict[str, _rt.RTQuote] = {}
                if self.tencent is not None:
                    quotes.update(self.tencent.fetch(todo))
                missing = [s for s in todo if s not in quotes]
                if missing and self.sina is not None:
                    quotes.update(self.sina.fetch(missing))
                still_missing = [s for s in todo if s not in quotes]
                if quotes:
                    self.synth.reset_if_new_day(_dt.date.today())
                    for df in self.synth.update(quotes):
                        for sym in df["symbol"].unique().to_list():
                            sub = df.filter(pl.col("symbol") == sym)
                            out[sym] = sub
                            self._cache_put(sym, sub)
                todo = still_missing
            # ⑤ mootdx 逐只兜底（池内并行）
            if todo:
                futures = {self._pool.submit(self._pull_mootdx_one, c): c
                           for c in todo}
                for f in futures.values():
                    pass
                for f, tf in futures.items():
                    try:
                        frame = f.result()
                    except Exception as e:  # noqa: BLE001
                        logger.warning("[sources] mootdx 兜底失败 %s: %s", tf, e)
                        continue
                    if frame is not None and not frame.is_empty():
                        out[frame["symbol"][0]] = frame
                        self._cache_put(frame["symbol"][0], frame)
            return list(out.values())

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False)
        if getattr(self, "tencent", None) is not None:
            self.tencent.close()
        if getattr(self, "sina", None) is not None:
            self.sina.close()
```

注意：上面 futures 循环里第一段 `for f in futures.values(): pass` 是笔误示例——落地时**删掉**，只保留第二个循环。（实现者若照抄会多一个空循环，ruff 不会报但请勿保留。）

(d) **保留** `_pull_recent_guarded` 模块级函数原样（mootdx 路径仍用它）。

(e) 同步适配既有测试 `tests/quant/test_stockdata_sources.py::test_fetch_one_rebuilds_source_on_timeout`：
把 `p._fetch_one("600000.XSHG")` 两处改为 `p._pull_mootdx_one("600000.XSHG")`，
断言 `.is_empty()` 改为 `is None`（新签名返回 None），其余逻辑不变：

```python
def test_fetch_one_rebuilds_source_on_timeout(monkeypatch):
    """超时后线程本地数据源被重置：下次 fetch 重建（factory 调用次数递增）。"""
    calls = []

    def fake_factory():
        calls.append(object())
        return calls[-1]

    def fake_pull(src, code):
        raise TimeoutError(f"timeout {code}")

    monkeypatch.setattr("app.services.stockdata.sources._pull_recent_guarded", fake_pull)
    p = NetworkPuller(factory=fake_factory, workers=1)
    try:
        assert p._pull_mootdx_one("600000.XSHG") is None
        assert len(calls) == 1
        assert getattr(p._local, "src", None) is None  # 已重置，不复用坏 socket
        assert p._pull_mootdx_one("600000.XSHG") is None
        assert len(calls) == 2  # 第二次 fetch 重建数据源
    finally:
        p.shutdown()
```

另：该文件顶部 import 若引用了已删除符号需同步清理；`test_realtime_snapshot_empty_in_trading_no_crash` 等走 `get_realtime_snapshot` 的用例在交易时段 mock 下会触发自举/HTTP——它们传入的 `mootdx_factory=lambda: StubSrc()` 返回空帧，HTTP 源未 mock 会真实触网。给这两个用例补 `monkeypatch.setattr(s.puller, "tencent", None)` 与 `monkeypatch.setattr(s.puller, "sina", None)` 强制纯 mootdx 离线路径（涉及：`test_realtime_snapshot_empty_in_trading_no_crash`、以及任何 `_in_trading=True` 且无 HTTP mock 的用例）。

- [ ] **Step 4: Run new tests + existing sources tests**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_stockdata_rt_sources.py tests/quant/test_stockdata_sources.py -v`
Expected: PASS（timeout 回归测试适配后绿）

- [ ] **Step 5: Commit**

Run: `cd backend && uv run --extra dev ruff check app && uv run --extra dev mypy app/services/stockdata/sources.py`
Expected: 通过

```bash
git add backend/app/services/stockdata/sources.py backend/tests/quant/test_stockdata_rt_sources.py backend/tests/quant/test_stockdata_sources.py
git commit -m "feat(stockdata): fetch_many 编排链——TTL缓存/mootdx自举/腾讯新浪批量降级"
```

---

### Task 4: sources.py — 陈旧门槛参数化 + get_realtime_snapshot 接线

**Files:**
- Modify: `backend/app/services/stockdata/sources.py`（get_realtime_snapshot ~L515-564）
- Modify: `backend/tests/quant/test_stockdata_sources.py`（新增门槛测试）

**Interfaces:**
- Consumes: Task 3 改造后的 puller（`synth.last_quote_time` 可用于陈旧判定）
- Produces: 陈旧阈值读 `STOCKDATA_RT_STALE_SEC`（默认 10s）；判定用
  `max(bar 最新时刻, synth.last_quote_time(sym))`

- [ ] **Step 1: Write the failing test**

追加到 `backend/tests/quant/test_stockdata_sources.py`：

```python
def test_realtime_stale_gate_configurable(tmp_path, monkeypatch):
    """陈旧门槛 STOCKDATA_RT_STALE_SEC：默认 10s；合成器快照时间可豁免陈旧。"""
    import datetime as dt
    import os

    os.environ["PARTITION_DATA_ROOT"] = str(tmp_path)
    s = DataSources(data_root=str(tmp_path), mootdx_factory=lambda: type(
        "S", (), {"get_minute_recent": staticmethod(
            lambda c, pages=1: __import__("pandas").DataFrame())}),
        fetch_workers=1)
    try:
        monkeypatch.setattr("app.services.stockdata.sources._in_trading",
                            lambda *a, **k: True)
        # 内存库放一根 20s 前的 bar：默认 10s 门槛 → 判陈旧 → todo 含该标的
        now = dt.datetime.now()
        old_bar = pl.DataFrame({
            "symbol": ["600000.SH"],
            "datetime": [now - dt.timedelta(seconds=20)],
            "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0],
            "volume": [0.0], "amount": [0.0]})
        s.minute_store.update(dt.date.today().isoformat(), old_bar)
        pulled = []

        def fake_fetch_many(codes):
            pulled.extend(codes)
            return []

        monkeypatch.setattr(s.puller, "fetch_many", fake_fetch_many)
        s.get_realtime_snapshot(["600000.XSHG"])
        assert pulled == ["600000.XSHG"]        # 20s > 10s → 判陈旧触发回源
        # 但合成器刚见过快照（1s 前）→ 即使 bar 是旧的也不算陈旧
        pulled.clear()
        fake_qt = now - dt.timedelta(seconds=1)
        monkeypatch.setattr(
            s.puller.synth, "last_quote_time",
            lambda sym, _t=fake_qt: _t)
        s.get_realtime_snapshot(["600000.XSHG"])
        assert pulled == []                     # 快照新鲜豁免
    finally:
        s.puller.shutdown()
        os.environ.pop("PARTITION_DATA_ROOT", None)
```

- [ ] **Step 2: Run to verify fail**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_stockdata_sources.py::test_realtime_stale_gate_configurable -v`
Expected: FAIL（pulled 第一次就为空——现硬编码 3min，20s 前的 bar 不判陈旧）

- [ ] **Step 3: Implement**

`get_realtime_snapshot` 中：

```python
stale_sec = float(os.getenv("STOCKDATA_RT_STALE_SEC", "") or 10.0)
...
def _is_stale(sym: str, last_dt) -> bool:
    qt = self.puller.synth.last_quote_time(sym)
    eff = max(last_dt, qt) if qt is not None else last_dt
    return eff < asof_ts - _dt.timedelta(seconds=stale_sec)
todo = [c for c in codes
        if _in_trading(asof_ts) and not _is_index(c)
        and (_tf_symbol(c) not in latest_by_sym
             or _is_stale(_tf_symbol(c), latest_by_sym[_tf_symbol(c)]))]
```

- [ ] **Step 4: Verify pass + full regression**

Run: `cd backend && uv run --extra dev pytest tests/quant/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/services/stockdata/sources.py backend/tests/quant/test_stockdata_sources.py
git commit -m "feat(stockdata): 实时陈旧门槛参数化(STALE_SEC)+快照时间豁免"
```

---

### Task 5: 全量回归 + 实盘验收

**Files:** 无新文件；可能微调前面任务产物

- [ ] **Step 1: 全量后端测试**

Run: `cd backend && uv run --extra dev pytest -q`
Expected: 全绿（对比 main 基线无新增失败；integration 标记用例如环境缺依赖跳过属正常）

- [ ] **Step 2: Lint + mypy 全量**

Run: `cd backend && uv run --extra dev ruff check app && uv run --extra dev mypy app`
Expected: 通过

- [ ] **Step 3: 实盘冒烟（收盘后仍可验：快照返回当日收盘累计数据）**

起服务（后台，遵守 AGENTS setsid 规范）：
```bash
setsid ./dev.sh > /tmp/tickflow-dev.log 2>&1 </dev/null & disown
```
另起命令验证端口存活后，直接对 stockdata 服务发 snapshot 请求（写一次性脚本经 `StockDataClient`）：
```python
# /tmp/opencode/verify_rt.py — 经 StockDataClient.current_snapshot 拉
# ["600000.XSHG","000001.XSHE","510300.XSHG"]，打印各标的最新 bar close/datetime，
# 断言 datetime 为今日且 close 与腾讯现价一致（±0.001）
```
Run: `cd backend && uv run python /tmp/opencode/verify_rt.py`
Expected: 三只标的均有今日 bar；close 与腾讯收盘价一致；日志显示走 tencent 源（`_rt_sources` 无 warning）

- [ ] **Step 4: 回滚开关验证**

Run: `cd backend && STOCKDATA_RT_SOURCE=mootdx uv run python /tmp/opencode/verify_rt.py`
Expected: 仍能取到今日 bar（走 mootdx 路径）

- [ ] **Step 5: 关停 dev 服务 + 提交收尾**

```bash
kill $(ss -tlnp | grep -E ":3018|:3011|:3322" | grep -oP 'pid=\K[0-9]+' | sort -u)
git status --short   # 应干净
```

验收标准（spec）：新旧路径延迟对比记录到 commit message；pytest/ruff/mypy 全绿。
