"""集合竞价数据采集与缓存。

每日 09:25:05 起（集合竞价撮合完成后、连续竞价开始前）从腾讯行情抓取
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
    # 仅采集当日 09:25:05～09:30 的竞价快照
    from app.quant.auction_cache import fetch_and_save
    fetch_and_save()  # 全市场

    # 读取（get_call_auction 内调用）
    from app.quant.auction_cache import load_day
    df = load_day(date(2026, 9, 30))  # DataFrame or None
"""
from __future__ import annotations

import datetime as _dt
import logging
import math
import os
import tempfile
import threading
import time
import urllib.request
from contextlib import closing, contextmanager
from zoneinfo import ZoneInfo

import pandas as pd

logger = logging.getLogger("auction_cache")

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
CACHE_ROOT = os.path.join(_REPO_ROOT, "data", "auction_cache")

_lock = threading.Lock()
_memory_cache: dict[str, pd.DataFrame] = {}  # date_iso -> DataFrame
_memory_versions: dict[str, tuple] = {}


def _now() -> _dt.datetime:
    return _dt.datetime.now(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None)


@contextmanager
def _day_write_lock(day_dir: str):
    """跨进程串行合并；不持锁联网，sidecar 不删除以保持 inode 稳定。"""
    with open(os.path.join(day_dir, ".write.lock"), "a+b") as stream:
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b"0")
            stream.flush()
        if os.name == "nt":
            import msvcrt

            def lock():
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)

            def unlock():
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            def lock():
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

            def unlock():
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        deadline = time.monotonic() + 5
        while True:
            try:
                lock()
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("auction cache write lock timed out") from None
                time.sleep(0.02)
        try:
            yield
        finally:
            unlock()


def _code_to_jq(bare: str) -> str:
    if bare.startswith("92"):
        return f"{bare}.BJ"
    if bare.startswith(("6", "9")):
        return f"{bare}.XSHG"
    if bare.startswith(("4", "8")):
        return f"{bare}.BJ"
    return f"{bare}.XSHE"


# 交易端口径后缀 → 聚宽后缀（instruments 用 .SH/.SZ/.BJ，策略侧用 .XSHG/.XSHE）
_EXCH_TO_JQ = {"SH": "XSHG", "SS": "XSHG", "SZ": "XSHE",
               "BJ": "BJ", "XSHG": "XSHG", "XSHE": "XSHE"}


def _to_jq_code(code: str) -> str:
    """统一成聚宽码：instruments 的 .SH/.SZ/.BJ、聚宽码、裸 6 位码都归一。

    fetch_and_save 从 instruments_frame() 拿到的 symbol 是短后缀（.SH/.SZ），
    直接喂 fetch_tencent_auction 会因 _jq_to_tencent_prefix 不认 "SH" 落进
    bj 分支而整段抓空沪市，且落盘的 .SZ 与策略 lookup 的 .XSHE 不同口径。
    归一收口在此，缓存 symbol 恒为聚宽码。
    """
    pure, _, exch = str(code).partition(".")
    if exch:
        mapped = _EXCH_TO_JQ.get(exch.upper())
        if mapped:
            return f"{pure}.{mapped}"
    return _code_to_jq(pure)


def _jq_to_tencent_prefix(jq_code: str) -> str:
    pure, _, exch = jq_code.partition(".")
    if exch.upper() in ("XSHG", "SS", "SH"):
        return f"sh{pure}"
    if exch.upper() in ("XSHE", "SZ"):
        return f"sz{pure}"
    return f"bj{pure}"


def fetch_tencent_auction(codes: list[str]) -> dict[str, dict]:
    """批量拉取腾讯实时行情，提取竞价数据。

    仅接受当日 09:25～09:30 的源时间戳；窗口外不联网，跨开盘响应丢弃。

    codes: JQ 格式代码列表（如 ['000989.XSHE', ...]），也接受 .SH/.SZ 短后缀
    与 6 位裸码；返回的 key 恒为聚宽码。
    返回: {jq_code: {price, volume_shares, prev_close, open}}
    """
    prefixed = []
    jq_map = {}
    for c in codes:
        jq = _to_jq_code(c)
        prefix = _jq_to_tencent_prefix(jq)
        prefixed.append(prefix)
        jq_map[prefix[2:]] = jq

    out: dict[str, dict] = {}
    # 腾讯接口每批最多 ~60 只
    batch_size = 55
    for i in range(0, len(prefixed), batch_size):
        now = _now()
        if not _in_auction_window(now):
            break
        batch = prefixed[i:i + batch_size]
        url = "https://qt.gtimg.cn/q=" + ",".join(batch)
        try:
            req = urllib.request.Request(url)
            req.add_header("User-Agent", "Mozilla/5.0")
            remaining = (_dt.datetime.combine(now.date(), _dt.time(9, 30)) - now).total_seconds()
            with closing(urllib.request.urlopen(req, timeout=min(10, remaining))) as resp:
                data = resp.read().decode("gbk")
        except Exception as e:
            logger.warning("[auction] 腾讯行情批次 %d 失败: %s", i // batch_size, e)
            continue
        received = _now()
        if received.date() != now.date() or received.time() >= _dt.time(9, 30):
            break
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
                opening = float(vals[5]) if vals[5] else 0.0
                quote_dt = _dt.datetime.strptime(vals[30], "%Y%m%d%H%M%S")
            except (ValueError, IndexError):
                continue
            if (quote_dt.date() != now.date()
                    or not _dt.time(9, 25) <= quote_dt.time() < _dt.time(9, 30)
                    or quote_dt > received
                    or not all(math.isfinite(v) and v > 0
                               for v in (price, prev_close, vol_hands, opening))):
                continue
            out[jq_code] = {
                "price": price,
                "volume_shares": vol_hands * 100,  # 手 → 股
                "prev_close": prev_close,
                "open": opening,
                "quote_time": quote_dt.isoformat(),
            }
    return out


def save_auction_day(day: _dt.date, df: pd.DataFrame) -> str:
    """锁内重读并合并最新快照，再原子发布，防多个账户互相覆盖。"""
    day_dir = os.path.join(CACHE_ROOT, f"date={day.isoformat()}")
    os.makedirs(day_dir, exist_ok=True)
    fpath = os.path.join(day_dir, "part.parquet")
    with _day_write_lock(day_dir):
        if os.path.exists(fpath):
            # 不吞损坏文件异常：读失败时禁止拿部分新数据覆盖完整快照。
            old = pd.read_parquet(fpath)
            df = pd.concat([old, df], ignore_index=True)
        df = df.drop_duplicates(subset=["symbol"], keep="last")
        fd, tmp = tempfile.mkstemp(dir=day_dir, suffix=".tmp")
        os.close(fd)
        try:
            df.to_parquet(tmp, index=False)
            os.replace(tmp, fpath)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)
    with _lock:
        _memory_cache.pop(day.isoformat(), None)
        _memory_versions.pop(day.isoformat(), None)
    logger.info("[auction] 已保存 %d 只竞价数据 → %s", len(df), fpath)
    return fpath


def load_day(day: _dt.date) -> pd.DataFrame | None:
    """读取某日的竞价数据缓存；不存在返回 None。"""
    key = day.isoformat()
    fpath = os.path.join(CACHE_ROOT, f"date={day.isoformat()}", "part.parquet")
    try:
        stat = os.stat(fpath)
        version = (fpath, stat.st_ino, stat.st_mtime_ns, stat.st_size)
        with _lock:
            if key in _memory_cache and _memory_versions.get(key) == version:
                return _memory_cache[key]
        # fstat 与读取绑定同一 inode，避免原子替换时把旧帧标成新版本。
        with open(fpath, "rb") as stream:
            stat = os.fstat(stream.fileno())
            version = (fpath, stat.st_ino, stat.st_mtime_ns, stat.st_size)
            df = pd.read_parquet(stream)
        with _lock:
            _memory_cache[key] = df
            _memory_versions[key] = version
        return df
    except Exception:
        return None


def lookup(code: str, day: _dt.date) -> tuple[float, float] | None:
    """查某标的某日的竞价数据 → (竞价量_股, 竞价价格)；无返回 None。"""
    df = load_day(day)
    if df is None or df.empty:
        return None
    hit = df[df["symbol"] == _to_jq_code(code)]
    if hit.empty:
        return None
    row = hit.iloc[-1]
    return float(row["auction_volume"]), float(row["auction_price"])


def _in_auction_window(now: _dt.datetime) -> bool:
    """北京时间交易日 09:25:05 ≤ t < 09:30；源时间戳仍须逐条校验。"""
    if now.weekday() >= 5 or not _dt.time(9, 25, 5) <= now.time() < _dt.time(9, 30):
        return False
    from app.services.trade_calendar import load_authoritative_dates

    try:
        dates, _ = load_authoritative_dates()
        return now.date().isoformat() in dates
    except LookupError:
        logger.warning("[auction] 交易日历不可用，跳过竞价采集")
        return False


def ensure_auction(codes: list[str], day: _dt.date,
                   now: _dt.datetime | None = None) -> int:
    """本地无缓存时按需联网抓竞价快照，增量合并落盘。返回写入条数。

    语义＝"本地没有才去网络拿"：已缓存的 symbol 不重复抓；窗口外直接返回
    0 不触网（腾讯现价非竞价撮合价）。day 为缓存归属日（通常=今天）。

    回归背景（2026-10-09）：实盘 09:26 选股时竞价缓存缺失（采集 cron 因
    pandas 崩溃从未成功过），get_call_auction 只能落到分钟回归≈2.02% 的
    假值，候选被 3% 门槛静默丢弃——该账户实盘期零买入即此。
    """
    now = now or _now()
    if not codes or day != now.date() or not _in_auction_window(now):
        return 0
    have = load_day(day)
    known = set()
    if have is not None and not have.empty and "symbol" in have.columns:
        known = set(have["symbol"].tolist())
    uniq = list(dict.fromkeys(_to_jq_code(c) for c in codes))
    missing = [c for c in uniq if c not in known]
    if not missing:
        return 0
    raw = fetch_tencent_auction(missing)
    if not raw:
        return 0
    rows = [{"symbol": jq, "auction_price": d["price"],
             "auction_volume": d["volume_shares"], "prev_close": d["prev_close"],
             "quote_time": d.get("quote_time")}
            for jq, d in sorted(raw.items())]
    save_auction_day(day, pd.DataFrame(rows))
    return len(rows)


def fetch_and_save(day: _dt.date | None = None,
                   codes: list[str] | None = None) -> pd.DataFrame:
    """窗口内分批补齐缺失标的，每批立即发布，供 09:26 策略读取。"""
    now = _now()
    if day is None:
        day = now.date()
    if day != now.date() or not _in_auction_window(now):
        return pd.DataFrame()
    from .stock_meta import instruments_frame

    if codes is None:
        ins = instruments_frame()
        # instruments_frame 返回 pandas 帧（stock_meta.py:57-78），取 symbol 列
        codes = ins["symbol"].tolist() if "symbol" in ins.columns else []
    else:
        codes = list(codes)

    codes = list(dict.fromkeys(_to_jq_code(c) for c in codes))
    for start in range(0, len(codes), 55):
        if not _in_auction_window(_now()):
            break
        ensure_auction(codes[start:start + 55], day)
    df = load_day(day)
    df = df.copy() if df is not None else pd.DataFrame()
    known = set(df["symbol"]) if not df.empty else set()
    df.attrs["complete"] = bool(codes) and set(codes).issubset(known)
    df.attrs["missing"] = len(set(codes) - known)
    return df
