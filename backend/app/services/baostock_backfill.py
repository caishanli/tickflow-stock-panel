"""baostock 全市场近 3 年回源（股票 5min + ETF/指数日线 + 复权因子 + 分红送转明细）。

独立于运行时（不依赖 DataManager/服务层），由 scripts/backfill_baostock_3y.py
CLI 驱动。要点（均实测验证）：
- baostock 无 1min/ETF分钟/指数分钟（frequency="1" 返回 10004012 错误）
- 股票 5min 真实数据 → data/kline_5min/date=YYYY-MM-DD/part.parquet
- ETF 日线 baostock 仅 2026-01-05 起；指数日线 3 年可用
- 断点续传 data/baostock_backfill_state.json；墙钟超时 + 重试 + 失败 CSV
- volume 单位：股票 5min=股；指数日线=股÷100(手，对齐 kline_index_daily)；
  ETF 日线=股；amount 元
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import date as _date
from datetime import datetime as _datetime
from pathlib import Path
from typing import Any, Callable

import polars as pl

logger = logging.getLogger("app.services.baostock_backfill")

_env_root = os.getenv("PARTITION_DATA_ROOT", "").strip()
DATA_ROOT = Path(_env_root) if _env_root else Path(__file__).resolve().parents[3] / "data"

KLINE_5MIN_ROOT = DATA_ROOT / "kline_5min"
KLINE_INDEX_DAILY_ROOT = DATA_ROOT / "kline_index_daily"
KLINE_ETF_DAILY_ROOT = DATA_ROOT / "kline_etf_daily"
ADJ_FACTOR_PATH = DATA_ROOT / "adj_factor" / "all.parquet"
DIVIDENDS_PATH = DATA_ROOT / "dividends" / "all.parquet"
STATE_PATH = DATA_ROOT / "baostock_backfill_state.json"
FAILURE_CSV = DATA_ROOT / "baostock_backfill_failures.csv"

KLINE_5MIN_FIELDS = "date,time,open,high,low,close,volume,amount"
DAILY_FIELDS = "date,open,high,low,close,volume,amount"

_bs_module = None


def _bs():
    """惰性加载 baostock 模块（测试可 monkeypatch _bs_module）。"""
    global _bs_module
    if _bs_module is None:
        import baostock as bs

        _bs_module = bs
    return _bs_module


def _guarded(fn: Callable[[], Any], timeout: float) -> Any:
    """墙钟超时守护：baostock socket 可能永久阻塞，超时弃帧不卡死整批。"""
    box: dict = {}

    def _run() -> None:
        try:
            box["out"] = fn()
        except Exception as e:  # noqa: BLE001
            box["err"] = e

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        raise TimeoutError(f"baostock 调用超时({timeout}s)")
    if "err" in box:
        raise box["err"]
    return box.get("out")


def _rows(rs) -> list[list[str]]:
    out = []
    while rs.error_code == "0" and rs.next():
        out.append(rs.get_row_data())
    return out


def _retry(fn: Callable[[], Any], timeout: float, retries: int) -> Any:
    last: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return _guarded(fn, timeout)
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2.0 * (attempt + 1) ** 2)  # 2/8/18s 递增退避
    raise last  # type: ignore[misc]


def query_kline(code: str, fields: str, start: str, end: str, frequency: str = "5",
                adjustflag: str = "3", timeout: float = 300, retries: int = 3) -> list[list[str]]:
    bs = _bs()

    def _q():
        rs = bs.query_history_k_data_plus(
            code, fields, start_date=start, end_date=end,
            frequency=frequency, adjustflag=adjustflag)
        if rs is None or rs.error_code != "0":
            raise RuntimeError(
                f"baostock 查询失败 {getattr(rs, 'error_code', None)}: "
                f"{getattr(rs, 'error_msg', '')}")
        return _rows(rs)

    return _retry(_q, timeout, retries)


def query_all_stock(day: str | None = None, timeout: float = 120, retries: int = 3) -> list[list[str]]:
    bs = _bs()

    def _q():
        rs = bs.query_all_stock(day=day or _date.today().strftime("%Y-%m-%d"))
        if rs is None or rs.error_code != "0":
            raise RuntimeError(f"query_all_stock 失败: {getattr(rs, 'error_code', None)}")
        return _rows(rs)

    return _retry(_q, timeout, retries)


def query_adjust_factor_rows(code: str, start: str, end: str,
                             timeout: float = 120, retries: int = 3) -> list[list[str]]:
    """复权因子事件行：每行 [code, dividOperateDate, foreAdjustFactor, backAdjustFactor, adjustFactor]。"""
    bs = _bs()

    def _q():
        rs = bs.query_adjust_factor(code, start_date=start, end_date=end)
        if rs is None or rs.error_code != "0":
            raise RuntimeError(f"query_adjust_factor 失败: {getattr(rs, 'error_code', None)}")
        return _rows(rs)

    return _retry(_q, timeout, retries)


def query_dividend_rows(code: str, year: int, timeout: float = 120, retries: int = 3) -> list[dict]:
    """分红送转明细（yearType=operate 除权除息口径），返回 dict 列表（字段名见 baostock）。"""
    bs = _bs()

    def _q():
        rs = bs.query_dividend_data(code, year=year, yearType="operate")
        if rs is None or rs.error_code != "0":
            raise RuntimeError(f"query_dividend_data 失败: {getattr(rs, 'error_code', None)}")
        rows = []
        while rs.error_code == "0" and rs.next():
            rows.append(dict(zip(rs.fields, rs.get_row_data())))
        return rows

    return _retry(_q, timeout, retries)


def to_baostock_code(sym: str) -> str:
    """分区 symbol (.SH/.SZ) -> baostock 码 (sh.600036)。"""
    pure, mkt = sym.split(".")
    return f"{mkt.lower()}.{pure}"


def from_baostock_code(code: str) -> str:
    """baostock 码 (sh.600036) -> 分区 symbol (.SH)。"""
    mkt, pure = code.split(".")
    return f"{pure}.{mkt.upper()}"


def _safe_float(v) -> float | None:
    try:
        if v is None or str(v).strip() in ("", "-"):
            return None
        return float(v)
    except Exception:  # noqa: BLE001
        return None


def load_state(path: Path = STATE_PATH) -> dict:
    """读断点状态；不存在/损坏时返回默认空状态。"""
    default = {
        "start": None, "end": None,
        "minute_done": [], "daily_done": [], "adj_done": [], "dividends_done": [],
        "failed": {},
    }
    if not path.exists():
        return default
    try:
        st = json.loads(path.read_text())
        for k, v in default.items():
            st.setdefault(k, v)
        return st
    except Exception:  # noqa: BLE001
        logger.warning("断点状态损坏，重置: %s", path)
        return default


def save_state(state: dict, path: Path = STATE_PATH) -> None:
    """原子写状态文件（tmp + rename）。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=1))
    tmp.rename(path)


def mark_done(state: dict, stage: str, sym: str) -> None:
    state[f"{stage}_done"].append(sym)


def mark_failed(state: dict, stage: str, sym: str, reason: str) -> None:
    state["failed"].setdefault(stage, {})[sym] = str(reason)[:200]


def append_failure(sym: str, reason: str) -> None:
    """把回源失败标的追加到 failure csv（symbol, 原因, 时间）。"""
    try:
        from datetime import datetime as _dt
        line = f"{sym},{reason},{_dt.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        FAILURE_CSV.parent.mkdir(parents=True, exist_ok=True)
        with open(FAILURE_CSV, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:  # noqa: BLE001
        logger.warning("失败记录写入失败: %s", sym)


def write_minute_partition(df: pl.DataFrame, root: Path, day: _date) -> None:
    """按 date 分区原子写 5min（读旧→concat→unique keep=last→tmp→rename），幂等。"""
    pdir = root / f"date={day}"
    pdir.mkdir(parents=True, exist_ok=True)
    part = pdir / "part.parquet"
    tmp = pdir / "part.tmp"
    if part.exists():
        old = pl.read_parquet(part)
        df = pl.concat([old, df]).unique(
            subset=["symbol", "datetime"], keep="last").sort(["symbol", "datetime"])
    df = df.sort(["symbol", "datetime"])
    df.write_parquet(tmp)
    tmp.rename(part)


def flush_minute_batch(frames: list[pl.DataFrame], root: Path) -> None:
    """一批股票的 5min 按交易日分组一次性写分区（降 IO 一个量级）。"""
    if not frames:
        return
    all_df = pl.concat(frames).unique(
        subset=["symbol", "datetime"], keep="last")
    all_df = all_df.with_columns(pl.col("datetime").dt.date().alias("_day"))
    for d, g in all_df.group_by("_day"):
        write_minute_partition(g.drop("_day"), root, d[0])


def write_daily_partition(df: pl.DataFrame, root: Path) -> None:
    """按 date 分区原子写日线（兼容有无 date 列的既有分区）。"""
    ds = str(df["date"][0])[:10]
    pdir = root / f"date={ds}"
    pdir.mkdir(parents=True, exist_ok=True)
    part = pdir / "part.parquet"
    tmp = pdir / "part.tmp"
    if part.exists():
        old = pl.read_parquet(part)
        if "date" not in old.columns:
            df = df.drop("date")
            merged = pl.concat([old, df]).unique(
                subset=["symbol"], keep="last").sort(["symbol"])
        else:
            merged = pl.concat([old, df]).unique(
                subset=["symbol", "date"], keep="last").sort(["symbol", "date"])
    else:
        merged = df.sort(["symbol", "date"])
    merged.write_parquet(tmp)
    tmp.rename(part)


def stock_universe() -> list[str]:
    """全市场 A 股 symbol 列表（.SH/.SZ，排除北交所；优先 instruments parquet）。"""
    inst = DATA_ROOT / "instruments" / "instruments.parquet"
    if inst.exists():
        try:
            df = pl.read_parquet(inst, columns=["symbol"])
            syms = [s for s in df["symbol"].to_list() if not s.endswith(".BJ")]
            if syms:
                return sorted(syms)
        except Exception as e:  # noqa: BLE001
            logger.warning("instruments 读取失败: %s", e)
    # 兜底：baostock query_all_stock（沪深 A 股）
    out = []
    for r in query_all_stock():
        code = r[0]
        if code.startswith(("sh.6", "sz.0", "sz.3")):
            out.append(from_baostock_code(code))
    return sorted(set(out))


def index_universe() -> list[str]:
    """指数 universe（优先 instruments_index parquet）。"""
    inst_dir = DATA_ROOT / "instruments_index"
    fs = sorted(inst_dir.glob("*.parquet")) if inst_dir.is_dir() else []
    if fs:
        try:
            df = pl.read_parquet(fs[-1], columns=["symbol"])
            return sorted(df["symbol"].to_list())
        except Exception as e:  # noqa: BLE001
            logger.warning("instruments_index 读取失败: %s", e)
    # 兜底：query_all_stock 指数码
    out = []
    for r in query_all_stock():
        code = r[0]
        if code.startswith(("sh.000", "sz.399")):
            out.append(from_baostock_code(code))
    return sorted(set(out))


def etf_universe() -> list[str]:
    """ETF universe（优先 etf_universe_snapshot.json，JQ 码转 .SH/.SZ）。"""
    snap = DATA_ROOT / "quant_kline" / "etf_universe_snapshot.json"
    if snap.exists():
        try:
            codes = json.loads(snap.read_text()).get("codes", [])
            if codes:
                return sorted(
                    c.replace(".XSHG", ".SH").replace(".XSHE", ".SZ") for c in codes)
        except Exception as e:  # noqa: BLE001
            logger.warning("ETF 快照读取失败: %s", e)
    # 兜底：已有 kline_etf_daily 分区里的标的
    if KLINE_ETF_DAILY_ROOT.is_dir():
        try:
            lf = pl.scan_parquet(
                str(KLINE_ETF_DAILY_ROOT / "**" / "*.parquet"), hive_partitioning=True)
            return sorted(lf.select("symbol").unique().collect()["symbol"].to_list())
        except Exception as e:  # noqa: BLE001
            logger.warning("kline_etf_daily 扫描失败: %s", e)
    return []


def listing_date_map() -> dict[str, _date]:
    """{symbol: 上市日期}（instruments parquet；缺失返回空 dict）。"""
    inst = DATA_ROOT / "instruments" / "instruments.parquet"
    out: dict[str, _date] = {}
    if not inst.exists():
        return out
    try:
        df = pl.read_parquet(inst, columns=["symbol", "listing_date"])
        for sym, ld in df.iter_rows():
            if not sym or not ld:
                continue
            s = str(ld).strip()
            try:
                if "-" in s:
                    out[sym] = _date.fromisoformat(s[:10])
                else:
                    out[sym] = _date(int(s[:4]), int(s[4:6]), int(s[6:8]))
            except Exception:  # noqa: BLE001
                continue
    except Exception as e:  # noqa: BLE001
        logger.warning("上市日期读取失败: %s", e)
    return out


_MIN5_SCHEMA = {
    "symbol": pl.Utf8, "datetime": pl.Datetime("us"),
    "open": pl.Float64, "high": pl.Float64, "low": pl.Float64,
    "close": pl.Float64, "volume": pl.Float64, "amount": pl.Float64,
}


def _to_5min_df(code: str, rows: list[list[str]]) -> pl.DataFrame:
    """baostock 5min 行 → polars 帧（symbol .SH/.SZ；volume 股不换算）。"""
    if not rows:
        return pl.DataFrame(schema=_MIN5_SCHEMA)
    ts = [_datetime.strptime(r[1][:14], "%Y%m%d%H%M%S") for r in rows]
    return pl.DataFrame({
        "symbol": [from_baostock_code(code)] * len(rows),
        "datetime": ts,
        "open": [float(r[2]) for r in rows],
        "high": [float(r[3]) for r in rows],
        "low": [float(r[4]) for r in rows],
        "close": [float(r[5]) for r in rows],
        "volume": [float(r[6]) for r in rows],
        "amount": [float(r[7]) for r in rows],
    }).with_columns(pl.col("datetime").cast(pl.Datetime("us")))


def make_progress_printer():
    """返回 progress(stage, i, total, rows) 回调：stdout 打印进度 + 速率 + ETA。"""
    t0 = time.time()

    def _p(stage: str, i: int, total: int, rows: int) -> None:
        elapsed = max(time.time() - t0, 0.001)
        rate = i / elapsed * 60.0
        eta = (total - i) / (i / elapsed) / 3600.0 if i > 0 else float("nan")
        print(f"[{stage}] {i}/{total} 累计{rows}行 "
              f"速率={rate:.1f}只/min ETA={eta:.1f}h", flush=True)

    return _p


def sync_minute(start: _date, end: _date, state: dict, timeout: float = 300,
                flush_batch: int = 100, retry_failed: bool = False,
                limit: int | None = None, progress=None) -> dict:
    """股票 5min 回源主循环：逐只拉 3 年 5min → 批量 flush 分区 → 断点标记。

    - resume：跳过 state['minute_done']；非 --retry-failed 时跳过 failed
    - 上市日期约束：起始 = max(start, listing_date)；1970 占位（退市/异常）跳过
    - 进度：每 flush_batch 只打一次 progress；失败落 failed + CSV
    """
    syms = stock_universe()
    listing = listing_date_map()
    done = set(state["minute_done"])
    failed = state["failed"].get("minute", {})
    todo = [s for s in syms if s not in done]
    if not retry_failed:
        todo = [s for s in todo if s not in failed]
    if limit is not None:
        todo = todo[:limit]
    pre_delisted = [s for s in todo if listing.get(s) == _date(1970, 1, 1)]
    if pre_delisted:
        logger.info("[minute] 跳过 %d 只退市/异常标的（上市日期占位）", len(pre_delisted))
        todo = [s for s in todo if s not in set(pre_delisted)]
    logger.info("[minute] 回源 %d 只（已覆盖 %d）", len(todo), len(done))
    total = 0
    chunk: list[pl.DataFrame] = []
    chunk_syms: list[str] = []
    for i, sym in enumerate(todo, 1):
        sym_start = start
        ld = listing.get(sym)
        if ld is not None and ld > sym_start:
            sym_start = ld
        code = to_baostock_code(sym)
        try:
            rows = query_kline(code, KLINE_5MIN_FIELDS,
                               sym_start.isoformat(), end.isoformat(),
                               "5", "3", timeout)
            df = _to_5min_df(code, rows)
            if df.is_empty():
                raise RuntimeError("empty")
            df = df.filter(pl.col("datetime").dt.date() >= sym_start)
            if df.is_empty():
                raise RuntimeError(f"no_data_since_{sym_start}")
        except Exception as e:  # noqa: BLE001
            mark_failed(state, "minute", sym, str(e)[:120])
            append_failure(sym, f"minute:{str(e)[:120]}")
            save_state(state)
            continue
        chunk.append(df)
        chunk_syms.append(sym)
        total += df.height
        if len(chunk) >= flush_batch:
            flush_minute_batch(chunk, KLINE_5MIN_ROOT)
            for s in chunk_syms:
                mark_done(state, "minute", s)
            save_state(state)
            chunk, chunk_syms = [], []
            if progress:
                progress("minute", i, len(todo), total)
    if chunk:
        flush_minute_batch(chunk, KLINE_5MIN_ROOT)
        for s in chunk_syms:
            mark_done(state, "minute", s)
        save_state(state)
    logger.info("[minute] 完成 %d 只, 累计 %d 行", len(todo), total)
    return {"symbols": len(todo), "rows": total}


_DAILY_SCHEMA = {
    "symbol": pl.Utf8, "date": pl.Date,
    "open": pl.Float64, "high": pl.Float64, "low": pl.Float64,
    "close": pl.Float64, "volume": pl.Float64, "amount": pl.Float64,
}


def _to_daily_df(code: str, rows: list[list[str]], volume_div: float = 1.0) -> pl.DataFrame:
    """baostock 日线行 → polars 帧。volume_div=100 时 baostock 股→手（指数口径）。"""
    if not rows:
        return pl.DataFrame(schema=_DAILY_SCHEMA)
    return pl.DataFrame({
        "symbol": [from_baostock_code(code)] * len(rows),
        "date": [_date.fromisoformat(r[0]) for r in rows],
        "open": [float(r[1]) for r in rows],
        "high": [float(r[2]) for r in rows],
        "low": [float(r[3]) for r in rows],
        "close": [float(r[4]) for r in rows],
        "volume": [float(r[5]) / volume_div for r in rows],
        "amount": [float(r[6]) for r in rows],
    })


def _flush_daily_batch(frames: list[pl.DataFrame], root: Path) -> None:
    if not frames:
        return
    all_df = pl.concat(frames).unique(subset=["symbol", "date"], keep="last")
    for d, g in all_df.group_by("date"):
        write_daily_partition(g, root)


def sync_daily(start: _date, end: _date, state: dict, timeout: float = 300,
               retry_failed: bool = False, limit: int | None = None,
               progress=None) -> dict:
    """ETF + 指数日线回源：逐只拉 3 年日线 → 按 date 批量写分区。

    指数 volume 股÷100 转手（对齐现有 kline_index_daily）；ETF 不换算。
    baostock ETF 日线仅 2026-01-05 起（更早返回空），空帧记失败但不阻塞。
    """
    groups = [
        ("index", index_universe(), KLINE_INDEX_DAILY_ROOT, 100.0),
        ("etf", etf_universe(), KLINE_ETF_DAILY_ROOT, 1.0),
    ]
    stats = {}
    for name, syms, root, vol_div in groups:
        done = set(state["daily_done"])
        failed = state["failed"].get("daily", {})
        todo = [s for s in syms if s not in done]
        if not retry_failed:
            todo = [s for s in todo if s not in failed]
        if limit is not None:
            todo = todo[:limit]
        logger.info("[daily:%s] 回源 %d 只（已覆盖 %d）", name, len(todo), len(done))
        batch: list[tuple[str, pl.DataFrame]] = []
        total = 0
        for i, sym in enumerate(todo, 1):
            code = to_baostock_code(sym)
            try:
                rows = query_kline(code, DAILY_FIELDS,
                                   start.isoformat(), end.isoformat(),
                                   "d", "3", timeout)
                df = _to_daily_df(code, rows, vol_div)
                if df.is_empty():
                    raise RuntimeError("empty")
            except Exception as e:  # noqa: BLE001
                mark_failed(state, "daily", sym, str(e)[:120])
                append_failure(sym, f"daily:{str(e)[:120]}")
                save_state(state)
                continue
            batch.append((sym, df))
            total += df.height
            if len(batch) >= 100:
                _flush_daily_batch([d for _, d in batch], root)
                for s, _ in batch:
                    mark_done(state, "daily", s)
                save_state(state)
                batch = []
                if progress:
                    progress(f"daily:{name}", i, len(todo), total)
        if batch:
            _flush_daily_batch([d for _, d in batch], root)
            for s, _ in batch:
                mark_done(state, "daily", s)
            save_state(state)
        stats[name] = {"symbols": len(todo), "rows": total}
    return stats
