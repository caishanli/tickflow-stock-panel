"""集合竞价数据采集与缓存。

每日 09:25:30（集合竞价撮合完成后、连续竞价开始前）从腾讯行情抓取
全市场竞价撮合结果（竞价价格 + 竞价成交量），落盘 parquet。
get_call_auction 优先读本缓存（精确竞价数据），缺失时降级分钟 bar 近似。

数据来源：腾讯实时行情 qt.gtimg.cn
- 09:25:30 时 vals[3](最新价) = 竞价撮合价（= 当日开盘价）
- 09:25:30 时 vals[6](累计量/手) = 竞价撮合量（× 100 = 股）
- vals[4] = 昨收（计算竞价量比分母）

文件布局：
    data/auction_cache/date=YYYY-MM-DD/part.parquet
    列: symbol(JQ码), auction_price, auction_volume(股), prev_close

用法：
    # 采集（09:25:30 或任意盘中时刻；盘后返回全天数据非竞价）
    from app.quant.auction_cache import fetch_and_save
    fetch_and_save()  # 全市场

    # 读取（get_call_auction 内调用）
    from app.quant.auction_cache import load_day
    df = load_day(date(2026, 9, 30))  # DataFrame or None
"""
from __future__ import annotations

import datetime as _dt
import logging
import os
import threading
import urllib.request

import numpy as np
import pandas as pd

logger = logging.getLogger("auction_cache")

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CACHE_ROOT = os.path.join(_REPO_ROOT, "data", "auction_cache")

_lock = threading.Lock()
_memory_cache: dict[str, pd.DataFrame] = {}  # date_iso -> DataFrame


def _code_to_jq(bare: str) -> str:
    if bare.startswith(("6", "9")):
        return f"{bare}.XSHG"
    if bare.startswith(("4", "8")):
        return f"{bare}.BJ"
    return f"{bare}.XSHE"


def _jq_to_tencent_prefix(jq_code: str) -> str:
    pure, _, exch = jq_code.partition(".")
    if exch in ("XSHG", "SS"):
        return f"sh{pure}"
    if exch in ("XSHE", "SZ"):
        return f"sz{pure}"
    return f"bj{pure}"


def fetch_tencent_auction(codes: list[str]) -> dict[str, dict]:
    """批量拉取腾讯实时行情，提取竞价数据。

    在 09:25:00~09:30:00 之间调用时，返回的 price/volume 即竞价撮合结果。
    盘后调用时返回全天数据（volume 是全天累计量，非竞价量）。

    codes: JQ 格式代码列表（如 ['000989.XSHE', ...]），也接受 6 位裸码。
    返回: {jq_code: {price, volume_shares, prev_close, open}}
    """
    prefixed = []
    jq_map = {}
    for c in codes:
        bare = c.split(".")[0] if "." in c else c
        prefix = _jq_to_tencent_prefix(c)
        prefixed.append(prefix)
        jq_map[prefix[2:]] = c

    out: dict[str, dict] = {}
    # 腾讯接口每批最多 ~60 只
    batch_size = 55
    for i in range(0, len(prefixed), batch_size):
        batch = prefixed[i:i + batch_size]
        url = "https://qt.gtimg.cn/q=" + ",".join(batch)
        try:
            req = urllib.request.Request(url)
            req.add_header("User-Agent", "Mozilla/5.0")
            resp = urllib.request.urlopen(req, timeout=10)
            data = resp.read().decode("gbk")
        except Exception as e:
            logger.warning("[auction] 腾讯行情批次 %d 失败: %s", i // batch_size, e)
            continue
        for line in data.strip().split(";"):
            if "=" not in line or '"' not in line:
                continue
            key = line.split("=")[0].split("_")[-1]
            vals = line.split('"')[1].split("~")
            if len(vals) < 40:
                continue
            bare = key[2:]
            jq_code = jq_map.get(bare)
            if jq_code is None:
                continue
            try:
                price = float(vals[3]) if vals[3] else 0.0
                prev_close = float(vals[4]) if vals[4] else 0.0
                vol_hands = float(vals[6]) if vals[6] else 0.0
            except (ValueError, IndexError):
                continue
            if price <= 0 or vol_hands <= 0:
                continue
            out[jq_code] = {
                "price": price,
                "volume_shares": vol_hands * 100,  # 手 → 股
                "prev_close": prev_close,
                "open": float(vals[5]) if vals[5] else price,
            }
    return out


def save_auction_day(day: _dt.date, df: pd.DataFrame) -> str:
    """保存某日的竞价数据到 parquet。返回文件路径。"""
    day_dir = os.path.join(CACHE_ROOT, f"date={day.isoformat()}")
    os.makedirs(day_dir, exist_ok=True)
    fpath = os.path.join(day_dir, "part.parquet")
    df.to_parquet(fpath, index=False)
    logger.info("[auction] 已保存 %d 只竞价数据 → %s", len(df), fpath)
    return fpath


def load_day(day: _dt.date) -> pd.DataFrame | None:
    """读取某日的竞价数据缓存；不存在返回 None。"""
    with _lock:
        key = day.isoformat()
        if key in _memory_cache:
            return _memory_cache[key]
    fpath = os.path.join(CACHE_ROOT, f"date={day.isoformat()}", "part.parquet")
    if not os.path.exists(fpath):
        return None
    try:
        df = pd.read_parquet(fpath)
        with _lock:
            _memory_cache[key] = df
        return df
    except Exception:
        return None


def lookup(code: str, day: _dt.date) -> tuple[float, float] | None:
    """查某标的某日的竞价数据 → (竞价量_股, 竞价价格)；无返回 None。"""
    df = load_day(day)
    if df is None or df.empty:
        return None
    hit = df[df["symbol"] == code]
    if hit.empty:
        return None
    row = hit.iloc[-1]
    return float(row["auction_volume"]), float(row["auction_price"])


def fetch_and_save(day: _dt.date | None = None,
                   codes: list[str] | None = None) -> pd.DataFrame:
    """采集并保存竞价数据（每日 09:25:30 调用，或手动补采）。

    day: 采集日期（默认今天）。codes: 标的列表（默认全市场 A 股）。
    """
    if day is None:
        day = _dt.date.today()
    from .stock_meta import instruments_frame

    ins = instruments_frame()
    if codes is None:
        codes = [r["symbol"] for _, r in ins.iter_rows(index=False)]
    else:
        codes = [c if "." in c else c for c in codes]

    raw = fetch_tencent_auction(codes)
    if not raw:
        return pd.DataFrame()

    rows = []
    for jq_code, d in sorted(raw.items()):
        rows.append({
            "symbol": jq_code,
            "auction_price": d["price"],
            "auction_volume": d["volume_shares"],
            "prev_close": d["prev_close"],
        })
    df = pd.DataFrame(rows)
    save_auction_day(day, df)
    with _lock:
        _memory_cache[day.isoformat()] = df
    return df
