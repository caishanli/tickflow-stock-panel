"""mootdx 数据服务：独立于模拟盘，由盘后管道调度触发。

负责两类每日收盘落盘更新（与「每日分钟线」同机制）：
1. ETF 分钟分区（``data/kline_etf_minute/date=YYYY-MM-DD/part.parquet``）：
   收盘后把当日全部 ETF 的真实 1m 从 mootdx 拉取落盘，供回测/模拟盘复用，
   避免每次回测联网回源。
2. ETF 前复权因子表（``data/adj_factor_etf/all.parquet``）：
   用 mootdx xdxr 除权除息记录重建逐日因子，增量合并进既有表。

回测、模拟盘都只读落盘数据（DataManager._adj_factor_map /
_load_minute_from_partitions），本服务不参与策略执行。
"""
from __future__ import annotations

import datetime as _dt
import logging
import os
import threading
import time
from datetime import date as _date
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl

from app.quant.jqengine.datasource.manager import DataManager
from app.quant.jqengine.datasource.mootdx_src import MootdxSource
from app.services.stockdata.backfill_pool import BackfillPool

logger = logging.getLogger("app.services.mootdx_service")

_env_root = os.getenv("PARTITION_DATA_ROOT", "").strip()
if _env_root:
    DATA_ROOT = Path(_env_root)
else:
    _repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)))))
    DATA_ROOT = Path(_repo_root) / "data"
ETF_MINUTE_ROOT = DATA_ROOT / "kline_etf_minute"
ADJ_FACTOR_PATH = DATA_ROOT / "adj_factor_etf" / "all.parquet"
INDEX_DAILY_ROOT = DATA_ROOT / "kline_index_daily"
# 回源失败标的落盘（symbol, 原因, 时间），供用户手动核查
FAILURE_LOG_PATH = DATA_ROOT / "mootdx_sync_failures.csv"

# 只处理 2020 年以来的除权事件（回测窗口有限，太早的因子无意义）
_SINCE_YEAR = 2020

# 内容完整性校验（symbol 覆盖率 vs 基准宇宙）：
# 覆盖率低于阈值即判残缺重写，防止"目录存在但只剩几只"的残帧永久污染。
# 全量手动校验回看近一年（默认 250 个交易分区）；每日自动只查近 1 周。
_CONTENT_CHECK_RECENT_DAYS = int(os.getenv("CONTENT_CHECK_RECENT_DAYS", "250"))
_DAILY_CHECK_RECENT_PARTITIONS = int(os.getenv("DAILY_CHECK_RECENT_PARTITIONS", "7"))
_CONTENT_CHECK_MIN_COVERAGE = float(os.getenv("CONTENT_CHECK_MIN_COVERAGE", "0.5"))
# 股票分钟 legacy 覆盖（env 可调；默认与共享常量一致）。
_STOCK_MINUTE_MIN_COVERAGE = float(os.getenv("STOCK_MINUTE_MIN_COVERAGE", "0.5"))
_STOCK_MINUTE_RECENT_LIMIT = int(os.getenv("STOCK_MINUTE_RECENT_LIMIT", "250"))
# 权威 ETF 代码段（对齐聚宽 get_all_securities(['etf']) 名单段分布）。
# 深市 159/161/169/180/181；沪市 501/506/510~518/520/526/530/551/560~563/588/589。
# 每个段在完整名单中至少出现 1 只；宇宙缺失整个段 = 快照/回源异常（如 501018
# 南方原油、161226 白银 LOF 曾因 _is_jq_etf_code 误过滤整段消失）。
_ETF_UNIVERSE_EXPECTED_SEGMENTS = (
    "159", "161", "169", "180", "181",
    "501", "506",
    "510", "511", "512", "513", "515", "516", "517", "518",
    "520", "526", "530", "551",
    "560", "561", "562", "563",
    "588", "589",
)


def _append_failure(sym: str, reason: str) -> None:
    """把回源失败标的追加到 failure csv（symbol, 原因, 时间）。"""
    try:
        from datetime import datetime as _dt
        line = f"{sym},{reason},{_dt.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        with open(FAILURE_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:  # noqa: BLE001
        logger.warning("mootdx_service: 失败记录写入失败: %s", sym)


# 回源断点续传 manifest：任务启动写 targets，每批 flush 后追加 done。
# 重启后 todo = targets − done − 最新分区已有 → 精确续跑。
MANIFEST_PATH = DATA_ROOT / "backfill_state.json"

# 15:35 cron / 00:00 巡检 / 启动 backfill 共用的互斥锁。原 scheduler._sync_lock
# 上移至此，scheduler 反向导入同一对象——启动 backfill 此前不持锁，会与
# 00:00 巡检并发轰击同一批被限速的服务器（08-21 深夜案例）。
_SYNC_LOCK = threading.Lock()


def _manifest_load() -> dict:
    import json
    try:
        return json.loads(MANIFEST_PATH.read_text())
    except Exception:  # noqa: BLE001
        return {}


def _manifest_save(data: dict) -> None:
    import json
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = MANIFEST_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False))
    tmp.rename(MANIFEST_PATH)


def _manifest_reset(dataset: str, targets: list[str], mode: str) -> None:
    data = _manifest_load()
    data[dataset] = {"targets": list(targets), "done": [], "mode": mode,
                     "updated_at": _dt.datetime.now().isoformat()}
    _manifest_save(data)


def _manifest_mark_done(dataset: str, symbols: list[str]) -> None:
    data = _manifest_load()
    entry = data.setdefault(dataset, {"targets": [], "done": [], "mode": ""})
    entry["done"] = sorted(set(entry.get("done") or []) | set(symbols))
    entry["updated_at"] = _dt.datetime.now().isoformat()
    _manifest_save(data)


def _manifest_done(dataset: str) -> set[str]:
    return set(_manifest_load().get(dataset, {}).get("done") or [])


def _is_market_open(now: _dt.datetime | None = None) -> bool:
    """A 股交易时段判定（口径同 stockdata.sources._in_trading）。"""
    now = now or _dt.datetime.now()
    t = now.time()
    return (now.weekday() < 5
            and (_dt.time(9, 30) <= t <= _dt.time(11, 30)
                 or _dt.time(13, 0) <= t <= _dt.time(15, 0)))


# 进度日志间隔（秒）：时间驱动替代按只数打点——服务器劣化时单只可达 190s，
# 按 25 只打点会出现 68 分钟零输出黑洞（08-21 Run B），无法区分卡死与正常。
_PROGRESS_LOG_INTERVAL_S = 60.0


def _mk_progress_logger(total: int, label: str):
    """时间驱动的进度日志器：每 60s 打一条（处理数/速率/ETA）。

    返回 ``tick(done_now, current="")``；内部按首次调用计时。速率/ETA 基于
    首次 tick 以来的平均值，供长回源全程可观测。
    """
    state = {"t0": None, "last": None, "n": 0}

    def tick(done_now: int, current: str = "") -> None:
        now = time.time()
        if state["t0"] is None:
            state["t0"] = now
            state["last"] = now
            state["n"] = done_now
            return
        state["n"] = done_now
        if now - state["last"] < _PROGRESS_LOG_INTERVAL_S:
            return
        state["last"] = now
        elapsed = max(1e-9, now - state["t0"])
        rate = done_now / elapsed
        eta = (total - done_now) / rate if rate > 0 else 0.0
        logger.info("%s 进度 %d/%d（当前 %s）速率 %.1f只/s ETA %.0fmin",
                    label, done_now, total, current, rate, eta / 60)

    return tick


def _to_tf_symbol(code: str) -> str:
    """JQ 代码 (.XSHG/.XSHE) -> 分区 symbol (.SH/.SZ)。"""
    pure, mkt = code.split(".")
    return pure + (".SH" if mkt == "XSHG" else ".SZ")


def _etf_universe() -> list[str]:
    """返回 ETF 宇宙 JQ codes 列表（优先快照，回退分区里已有的标的）。"""
    snap_path = DATA_ROOT / "quant_kline" / "etf_universe_snapshot.json"
    if snap_path.exists():
        import json
        try:
            codes = json.loads(snap_path.read_text()).get("codes", [])
            if codes:
                return sorted(codes)
        except Exception as e:
            logger.warning("ETF 快照读取失败，回退分区标的: %s", e)
    lf = pl.scan_parquet(str(ETF_MINUTE_ROOT / "**" / "*.parquet"),
                         hive_partitioning=True)
    syms = lf.select("symbol").unique().collect()["symbol"].to_list()
    out = []
    for s in syms:
        pure, mkt = s.split(".")
        out.append(pure + (".XSHG" if mkt == "SH" else ".XSHE"))
    return sorted(set(out))


def sync_etf_minute(day: _date | None = None) -> int:
    """收盘后同步指定交易日（默认今天）全部 ETF 分钟到按日分区。

    逐标的拉真实 1m：近期日（≤5 天）用 ``get_minute_recent``（含当日盘中），
    历史日（>5 天）用 ``get_minute`` 全量拉再过滤当日（支持 4/1 起缺失日回补）。
    取数经 :class:`BackfillPool` 并发（每 worker 独立连接，坏连接由池自愈重建）。
    以 ``date={day}/part.parquet`` 原子写盘。返回写入行数。
    """
    day = day or _date.today()
    codes = _etf_universe()
    if not codes:
        logger.warning("mootdx_service: ETF 宇宙为空，跳过分钟同步")
        return 0
    historical = (day < _date.today() - _dt.timedelta(days=5))
    keep = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]

    def _fetch_one(src, jq):
        try:
            if historical:
                # since=day 分页：只回看到覆盖目标日，不拉全历史
                df = src.get_minute(jq, max_bars=40000, since=day)
            else:
                df = src.get_minute_recent(jq, pages=2)
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: %s 分钟拉取失败: %s", jq, e)
            return None
        if df is None or df.empty:
            return None
        df = df.copy()
        df["symbol"] = _to_tf_symbol(jq)
        df = df.reset_index()
        for c in keep:
            if c not in df.columns:
                df[c] = None
        df = df[keep]
        df = df[pd.to_datetime(df["datetime"]).dt.date == day]
        if df.empty:
            return None
        out = pl.from_pandas(df)
        return out.with_columns(
            pl.col("datetime").cast(pl.Datetime("us")).alias("datetime"))

    result = BackfillPool().map(_fetch_one, codes)
    if result["failed"]:
        logger.warning("mootdx_service: ETF 分钟回源失败 %d 只: %s",
                       len(result["failed"]), list(result["failed"])[:10])
    if not result["ok"]:
        return 0
    out = pl.concat(result["ok"]).unique(
        subset=["symbol", "datetime"], keep="last").sort(["symbol", "datetime"])
    return _write_minute_partition(out, ETF_MINUTE_ROOT, day)


# mootdx 源错标的 13:00:00 假bar 判定（真实分钟数据下午从 13:01 起，13:00:00 恒为假）
_PHANTOM_NOON_EXPR = (pl.col("datetime").dt.hour() == 13) & (pl.col("datetime").dt.minute() == 0)


def _relabel_phantom_noon_df(df: pl.DataFrame) -> pl.DataFrame:
    """把分钟分区里 mootdx 错标的 13:00:00 假bar 归位为 11:30:00（任意交易日）。

    通达信分钟源在盘中/盘后拉取时会把某日最后一根上午 bar（11:30）错标成
    13:00:00 返回（其 OHLCV 即真实 11:30）。同 symbol 已有真实 11:30 时丢弃
    假bar（unique keep=first + sort 把真实行排前）；11:30 被吞的 symbol 归位
    即补回。
    """
    if not df.filter(_PHANTOM_NOON_EXPR).height:
        return df
    relabeled = df.with_columns(
        _PHANTOM_NOON_EXPR.alias("_is_noon"),
        pl.when(_PHANTOM_NOON_EXPR)
          .then(pl.col("datetime") - pl.duration(hours=1, minutes=30))
          .otherwise(pl.col("datetime")).alias("datetime"))
    return (relabeled
            .sort("_is_noon")  # 真实行(False)在前，unique keep=first 保留真实 11:30
            .unique(subset=["symbol", "datetime"], keep="first")
            .drop("_is_noon")
            .sort(["symbol", "datetime"]))


def clean_phantom_noon_partitions() -> dict:
    """清理既有分钟分区中 mootdx 错标的 13:00:00 假bar（归位 11:30:00）。

    通达信源盘中拉取时把某日 11:30 的最后一根 bar 错标 13:00:00 写入分区：
    ETF 分钟（08-18 案例 1658 只全带）+ 股票分钟（08-05~08-17 十一日，
    180/180 假bar 与真实 11:30 逐列一致，000838.SZ 的 11:30 被吞需补回）。
    真实分钟数据下午从 13:01 起，13:00:00 恒为假bar，其值即真实 11:30，
    归位不丢数据。扫描 ``kline_etf_minute``/``kline_minute`` 全部分区，
    有假bar 的分区原子重写。返回清理过的分区相对路径列表。
    """
    cleaned = []
    for root in (ETF_MINUTE_ROOT, STOCK_MINUTE_ROOT):
        for pdir in sorted(root.glob("date=*")):
            part = pdir / "part.parquet"
            if not part.exists():
                continue
            df = pl.read_parquet(part)
            if not df.filter(_PHANTOM_NOON_EXPR).height:
                continue
            out = _relabel_phantom_noon_df(df)
            tmp = pdir / "part.tmp"
            out.write_parquet(tmp)
            tmp.rename(part)
            rel = f"{root.name}/{pdir.name}"
            cleaned.append(rel)
            logger.info("mootdx_service: %s 清理 13:00:00 假bar（归位 11:30:00）", rel)
    return {"partitions": cleaned}


def _write_minute_partition(df: pl.DataFrame, root: Path, day: _date) -> int:
    """按 date 分区原子写分钟（读旧→concat→unique→归位假bar→tmp→rename）。返回行数。"""
    pdir = root / f"date={day}"
    pdir.mkdir(parents=True, exist_ok=True)
    part = pdir / "part.parquet"
    tmp = pdir / "part.tmp"
    if part.exists():
        old = pl.read_parquet(part)
        df = pl.concat([old, df]).unique(
            subset=["symbol", "datetime"], keep="last").sort(["symbol", "datetime"])
    df = _relabel_phantom_noon_df(df)
    df.write_parquet(tmp)
    tmp.rename(part)
    logger.info("mootdx_service: 分钟落盘 %d 行 → %s", df.height, part)
    return df.height


STOCK_MINUTE_ROOT = DATA_ROOT / "kline_minute"
# 股票分钟回源起点：从该日起补全市场分钟（用户需求 4/1 起）
STOCK_MINUTE_START = _date(2026, 4, 1)
# 每攒满多少只股票一次性批量写分区（写盘 IO 与内存的折中）
_STOCK_MINUTE_BATCH = 100
# 回源进度日志间隔（只数）：100 只才 flush 一次（约 10 分钟），期间无任何输出，
# 无法区分"卡死"与"正常积累"。每间隔这么多只打一条进度日志（已处理/总数 +
# 当前标的 + 耗时），保证长回源全程可见。
_STOCK_MINUTE_PROGRESS_STEP = 25
# 调度任务单次回源的股票分钟只数上限（增量慢跑：启动线程与盘后 cron 各跑
# 一批，resume 跳过已覆盖，多轮后自动补齐全部缺口；None = 一次拉全量）
STOCK_MINUTE_BATCH_LIMIT = 20
# 收盘后最新分钟分区覆盖率的"完整"阈值：低于它视为回源中断残留的残缺日
# （如 08-19 只写了 3600/5209），此时忽略 limit 直接全量补齐。正常完整日
# 覆盖率 >99%，阈值取 0.95 与增量慢跑/内容校验（0.5）的语义区分开。
_STOCK_MINUTE_RESUME_COVERAGE = float(
    os.getenv("STOCK_MINUTE_RESUME_COVERAGE", "0.95"))


def _flush_stock_minute_chunk(chunk: list[pl.DataFrame]) -> None:
    """把一批股票的分钟 bar 按日期分区一次性合并写入。

    先按交易日分组聚合 chunk 内所有股票，再对每个日期分区读旧→concat→
    unique→原子替换。避免逐股票逐分区 IO。
    """
    if not chunk:
        return
    all_df = pl.concat(chunk).unique(
        subset=["symbol", "datetime"], keep="last")
    all_df = all_df.with_columns(pl.col("datetime").dt.date().alias("_day"))
    for d, g in all_df.group_by("_day"):
        day = d[0]
        g = g.drop("_day").sort(["symbol", "datetime"])
        pdir = STOCK_MINUTE_ROOT / f"date={day}"
        pdir.mkdir(parents=True, exist_ok=True)
        pfile = pdir / "part.parquet"
        tmp = pdir / "part.tmp"
        if pfile.exists():
            old = pl.read_parquet(pfile)
            merged = pl.concat([old, g]).unique(
                subset=["symbol", "datetime"], keep="last").sort(["symbol", "datetime"])
        else:
            merged = g
        merged = _relabel_phantom_noon_df(merged)
        merged.write_parquet(tmp)
        tmp.rename(pfile)


def _existing_minute_symbols() -> set[str]:
    """收集**最新**股票分钟分区里已落盘的 symbol 集合（用于 resume）。

    只读最新日期分区：若某标的只存在于老分区而最新分区缺失（如上次回源
    中途中断），不能误判为"已覆盖"而跳过 —— 否则最新交易日数据永久缺失。
    """
    if not STOCK_MINUTE_ROOT.is_dir():
        return set()
    days = sorted(STOCK_MINUTE_ROOT.glob("date=*"))
    if not days:
        return set()
    latest = days[-1] / "part.parquet"
    if not latest.exists():
        return set()
    try:
        df = pl.read_parquet(latest, columns=["symbol"])
        return set(df["symbol"].to_list())
    except Exception:  # noqa: BLE001
        return set()


def _stock_minute_latest_partial(done_syms: set[str], stocks: list[str]) -> bool:
    """收盘后最新分钟分区覆盖率显著不足时返回 True（触发全量补齐而非增量慢跑）。

    15:35 全量回源（limit=None）被重启打断会留下残缺的最新分区（08-19 案例：
    只写了 3600/5209）。resume 只按最新分区判断"已覆盖"，后续每次启动只补
    limit=20 只（需 ~70 轮才追上），残片检测又跳过最新分区 —— 当天分钟线
    永久缺失。此处以完整日覆盖率（正常 >99%）为基线，最新分区覆盖率低于
    ``_STOCK_MINUTE_RESUME_COVERAGE`` 即视为中断残留，应一次全量补齐缺失
    标的，而不是受 limit 限制慢慢爬。
    """
    if not _market_closed():
        return False
    if not done_syms or not stocks:
        return False
    coverage = len(done_syms & set(stocks)) / len(stocks)
    return coverage < _STOCK_MINUTE_RESUME_COVERAGE


# 分区 symbol 数低于基线该数量以上视为残片（回源中断产物）。
# 单日停牌通常 <100 只，阈值取 500 避免误伤停牌日。
_MINUTE_FRAGMENT_THRESHOLD = 500
# 残片检测只看最近 N 个分区：更早的历史分区可能是按需拉取的局部数据
# （symbol 少是正常），全窗口判定会误判为残片。
_MINUTE_FRAGMENT_LOOKBACK_DAYS = 10


def _minute_fragment_days() -> dict[_date, list[str]]:
    """检测最近窗口内的分钟残片分区：某交易日 symbol 数显著低于基线。

    resume 只按最新分区判断"已覆盖"，回源中断留下的中间残片日会被永久
    跳过（如 08-12 凌晨 pytdx 崩溃留下 3640/5208）。返回 {残片日: 缺失
    symbol}，缺失 = 基线分区有、残片日没有；仅补缺失 symbol 而非全市场
    重拉。

    只检查最近 ``_MINUTE_FRAGMENT_LOOKBACK_DAYS`` 个分区：更早的历史
    分区可能是按需拉取的局部数据（1~16 只），symbol 少是正常而非残片；
    全窗口判定会把这些日子误判为残片、触发灾难性重拉。**最新分区跳过
    判定**（新交易日回源未完成由 resume 增量机制负责；它若真是中断残片，
    次日成为中间分区后自愈）。

    基线取窗口内 symbol 数**最多**的分区而非最新分区：最新分区可能是
    新交易日回源未完成（只有 1 只），不能代表全市场。
    """
    if not STOCK_MINUTE_ROOT.is_dir():
        return {}
    days = sorted(STOCK_MINUTE_ROOT.glob("date=*"))
    if len(days) < 2:
        return {}
    window = days[-_MINUTE_FRAGMENT_LOOKBACK_DAYS:]
    best_day: Path | None = None
    best_syms: set[str] | None = None
    for d in window:
        part = d / "part.parquet"
        if not part.exists():
            continue
        try:
            syms = set(pl.read_parquet(part, columns=["symbol"])["symbol"].to_list())
        except Exception:
            continue
        if best_syms is None or len(syms) > len(best_syms):
            best_day, best_syms = d, syms
    if best_day is None or best_syms is None:
        return {}
    fragments: dict[_date, list[str]] = {}
    for d in window[:-1]:
        if d == best_day:
            continue
        part = d / "part.parquet"
        if not part.exists():
            continue
        try:
            syms = set(pl.read_parquet(part, columns=["symbol"])["symbol"].to_list())
        except Exception:
            continue
        missing = best_syms - syms
        if len(missing) >= _MINUTE_FRAGMENT_THRESHOLD:
            fragments[_date.fromisoformat(d.name.removeprefix("date="))] = sorted(missing)
    return fragments


def _listing_date_map() -> dict[str, _date]:
    """返回 {symbol: 上市日期} 映射（instruments parquet，缺失回退 4/1 起点）。

    新股上市日可能晚于回源起点：4/1 后上市的股票，其最早日线/分钟从上市日
    才开始，按上市日作为该标的的回源起点，避免对上市前的空窗期拉取（必然
    获取不到，浪费 8-10s/次服务器轮换超时）。
    """
    inst_path = DATA_ROOT / "instruments" / "instruments.parquet"
    out: dict[str, _date] = {}
    if not inst_path.exists():
        return out
    try:
        df = pl.read_parquet(inst_path, columns=["symbol", "listing_date"])
        for row in df.iter_rows():
            sym, ld = row
            if sym is None or ld is None:
                continue
            try:
                s = str(ld).strip()
                if not s:
                    continue
                if "-" in s:
                    d = _date.fromisoformat(s[:10])
                else:
                    d = _date(int(s[:4]), int(s[4:6]), int(s[6:8]))
                out[sym] = d
            except Exception:  # noqa: BLE001
                continue
    except Exception as e:  # noqa: BLE001
        logger.warning("mootdx_service: 上市日期读取失败: %s", e)
    return out


def sync_stock_minute(limit: int | None = None) -> int:
    """回源 4/1 起全市场 A 股分钟到 ``kline_minute`` 分区（一次拉全量多日）。

    对每只股票用 mootdx ``get_minute`` 拉一次全量历史 1m（约 3 个月，含
    4/1 至今），过滤掉 4/1 前的 bar 后按交易日分组缓存；每攒满
    ``_STOCK_MINUTE_BATCH`` 只即把累积 bar 一次性分散写入各 ``date=`` 分区
    （读旧→concat→unique keep=last）。跳过北交所（mootdx 无数据）。

    ``limit``：最多本次处理的股票数（用于调度任务增量慢跑——每批拉一小
    部分，resume 自动跳过已覆盖，多轮后自然补齐全部缺口）。None = 一次
    拉全量 todo。返回写入行数。

    分块批量写：避免每只股票逐分区读-改-写（5205 只 × 84 分区 IO 巨大），
    批量合并把写盘次数降一个量级。返回写入行数。

    取数经 :class:`BackfillPool` 并发（每线程独立连接，见模块说明），
    ``since`` 按需分页把单只页数从 ~29 页降到覆盖起点所需的最少页数。
    全量初始化（4/1 起全历史）仍属长任务，manifest 断点续传兜底重启。
    """
    stocks = [s for s in _stock_universe() if not s.endswith(".BJ")]
    if not stocks:
        logger.warning("mootdx_service: 股票宇宙为空，跳过分钟同步")
        return 0
    listing = _listing_date_map()
    # 新交易日整日缺失：resume 只按最新分区判断，最新分区是昨天且完整时会
    # 误判"已覆盖"而跳过今天（08-18 案例）——先 range 补缺失交易日。盘中
    # 排除今天，不写半程数据（见 _missing_stock_minute_days）。
    range_rows = 0
    missing_days = _missing_stock_minute_days()
    if missing_days:
        range_rows = sync_stock_minute_range(missing_days)
        logger.info("mootdx_service: 股票分钟补齐缺失交易日 %s 共 %d 行",
                    [d.isoformat() for d in missing_days], range_rows)
    # 残片自愈: 回源中断会留下中间分区 symbol 数严重不足的残片(如 08-12 凌晨
    # pytdx 崩溃留下 3640/5208), resume 只按最新分区判断会永久跳过这些日子。
    # 先补残片日缺失的 symbol, 再做正常增量回源。
    fragments = _minute_fragment_days()
    fragment_rows = 0
    for day, missing in sorted(fragments.items()):
        n = sync_stock_minute_day(day, symbols=missing)
        fragment_rows += n
        logger.info("mootdx_service: 分钟残片日 %s 补齐 %d 行 (%d 只)",
                    day, n, len(missing))
    # resume：收集已落盘的 symbol + manifest 断点，跳过（中断后续跑不重拉）
    done_syms = _existing_minute_symbols() | _manifest_done("stock_minute")
    todo = [s for s in stocks if s not in done_syms]
    if not todo:
        logger.info("mootdx_service: 股票分钟已全部覆盖（%d 只），无需回源", len(done_syms))
        return range_rows + fragment_rows
    if limit is not None:
        if _stock_minute_latest_partial(done_syms, stocks):
            cov_pct = len(done_syms & set(stocks)) / len(stocks) * 100
            # 中断感知：guardian 单实例保证无并发写者——最新分区 mtime 距今
            # <10min 即上个进程刚被杀在中途，措辞用「中断续跑」而非误导性「残缺」。
            days = sorted(STOCK_MINUTE_ROOT.glob("date=*"))
            part = days[-1] / "part.parquet" if days else None
            mt_age = (time.time() - part.stat().st_mtime
                      if part is not None and part.exists() else 1e9)
            if mt_age < 600:
                logger.info("mootdx_service: 上次回源中断于 %s（%.0fmin 前，"
                            "覆盖率 %.1f%%），从断点继续补齐 %d 只",
                            days[-1].name, mt_age / 60, cov_pct, len(todo))
            else:
                logger.info("mootdx_service: 最新分钟分区残缺（覆盖率 %.1f%%），"
                            "忽略 limit=%d 全量补齐 %d 只",
                            cov_pct, limit, len(todo))
        else:
            todo = todo[:limit]
    # 上市日期占位（1970-01-01 = instruments 退市/异常数据）：这些标的 4/1 前
    # 已无交易，回源必然获取不到（每次 8-10s 服务器轮换超时），直接跳过。
    pre_delisted = [s for s in todo if listing.get(s) == _date(1970, 1, 1)]
    if pre_delisted:
        logger.info("mootdx_service: 跳过 %d 只退市/异常标的（上市日期占位）: %s",
                    len(pre_delisted), pre_delisted[:10])
        todo = [s for s in todo if s not in set(pre_delisted)]
    if not todo:
        return range_rows + fragment_rows
    _manifest_reset("stock_minute", todo,
                    mode="full" if limit is None else "recent")
    logger.info("mootdx_service: 股票分钟回源 %d/%d（跳过已覆盖 %d）",
                len(todo), len(stocks), len(done_syms))
    keep = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
    tick = _mk_progress_logger(len(todo), "股票分钟回源")
    counter = {"n": 0}
    total_holder = {"rows": 0}
    pending: list[pl.DataFrame] = []

    def _fetch_one(src, sym):
        counter["n"] += 1
        tick(counter["n"], sym)
        # 回源起点 = max(全局起点, 该股上市日)；新股上市前无数据，不提前拉
        sym_start = STOCK_MINUTE_START
        ld = listing.get(sym)
        if ld is not None and ld > sym_start:
            sym_start = ld
        try:
            df = _guarded_get_minute(src, sym, max_bars=40000, since=sym_start)
        except TimeoutError:
            # 超时：pool 已重建该 worker source（坏 socket 不残留）
            _append_failure(sym, "timeout")
            raise
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: %s 分钟拉取失败: %s", sym, e)
            _append_failure(sym, f"exception:{str(e)[:60]}")
            return None
        if df is None or df.empty:
            _append_failure(sym, "empty")
            return None
        df = df.copy()
        df["symbol"] = sym
        df = df.reset_index()
        for c in keep:
            if c not in df.columns:
                df[c] = None
        df = df[keep]
        df = df[pd.to_datetime(df["datetime"]).dt.date >= sym_start]
        # 盘中排除今天的半程 bar：get_minute 历史含今天盘中数据，写入
        # date=today 分区即污染。与 _missing_stock_minute_days 的盘中约定一致。
        if not _market_closed():
            df = df[pd.to_datetime(df["datetime"]).dt.date < _date.today()]
        if df.empty:
            _append_failure(sym, f"no_data_since_{sym_start}")
            return None
        sub = pl.from_pandas(df)
        sub = sub.with_columns(pl.col("datetime").cast(pl.Datetime("us")).alias("datetime"))
        sub = sub.unique(subset=["symbol", "datetime"], keep="last")
        return sub

    def _on_batch(batch_frames: list[pl.DataFrame]) -> None:
        """主线程批回调：攒满一批统一写分区 + manifest 记账（单线程写盘）。

        行数在落盘点累计——池 keep_frames=False 不驻留帧，峰值内存 O(batch)。
        """
        pending.extend(batch_frames)
        if len(pending) >= _STOCK_MINUTE_BATCH:
            total_holder["rows"] += sum(f.height for f in pending)
            _flush_stock_minute_chunk(pending.copy())
            _manifest_mark_done("stock_minute",
                                [f["symbol"][0] for f in pending])
            pending.clear()

    result = BackfillPool().map(_fetch_one, todo,
                                batch_size=_STOCK_MINUTE_BATCH,
                                on_batch_done=_on_batch, keep_frames=False)
    if pending:
        total_holder["rows"] += sum(f.height for f in pending)
        _flush_stock_minute_chunk(pending)
        _manifest_mark_done("stock_minute", [f["symbol"][0] for f in pending])
        pending.clear()
    total = total_holder["rows"]
    if result["failed"]:
        logger.warning("mootdx_service: 股票分钟回源失败 %d 只: %s",
                       len(result["failed"]),
                       list(result["failed"].items())[:10])
    logger.info("mootdx_service: 股票分钟回源完成 %d 行 (ok=%d failed=%d)",
                total, result["ok_count"], len(result["failed"]))
    return range_rows + total + fragment_rows


def sync_stock_minute_day(day: _date, symbols: list[str] | None = None) -> int:
    """按缺失日补全股票分钟到 ``kline_minute/date={day}``。

    ``symbols``：只处理给定列表（残片日只补缺失标的）；None = 全市场。

    「有数据才回」：上市日晚于目标日的 symbol 跳过（该日尚未上市）；
    当日停牌/无 bar 自然跳过不落盘。逐 symbol 用 ``get_minute`` 全量拉，
    过滤到 ``day`` 后批量写分区（复用 ``_flush_stock_minute_chunk``）。
    返回写入行数。
    """
    stocks = [s for s in _stock_universe() if not s.endswith(".BJ")]
    if not stocks:
        logger.warning("mootdx_service: 股票宇宙为空，跳过分钟同步")
        return 0
    if symbols is not None:
        want = set(symbols)
        stocks = [s for s in stocks if s in want]
        if not stocks:
            return 0
    listing = _listing_date_map()
    # 上市日期占位（1970-01-01 = instruments 退市/异常数据）：这些标的已无交易，
    # 回源必然获取不到（每次 8-10s 服务器轮换超时），直接跳过（与 sync_stock_minute 一致）。
    pre_delisted = [s for s in stocks if listing.get(s) == _date(1970, 1, 1)]
    if pre_delisted:
        logger.info("mootdx_service: 跳过 %d 只退市/异常标的（上市日期占位）: %s",
                    len(pre_delisted), pre_delisted[:10])
        stocks = [s for s in stocks if s not in set(pre_delisted)]
    keep = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
    tick = _mk_progress_logger(len(stocks), f"股票分钟按日回源 {day}")
    counter = {"n": 0}
    pending: list[pl.DataFrame] = []

    def _fetch_one(src, sym):
        counter["n"] += 1
        tick(counter["n"], sym)
        ld = listing.get(sym)
        if ld is not None and ld > day:
            return None  # 上市晚于目标日，该日无数据
        try:
            df = _guarded_get_minute(src, sym, max_bars=40000, since=day)
        except TimeoutError:
            _append_failure(sym, "timeout")
            raise
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: %s 分钟拉取失败: %s", sym, e)
            _append_failure(sym, f"exception:{str(e)[:60]}")
            return None
        if df is None or df.empty:
            _append_failure(sym, "empty")
            return None
        df = df.copy()
        df["symbol"] = sym
        df = df.reset_index()
        for c in keep:
            if c not in df.columns:
                df[c] = None
        df = df[keep]
        df = df[pd.to_datetime(df["datetime"]).dt.date == day]
        if df.empty:
            return None  # 当日停牌/无 bar，跳过
        sub = pl.from_pandas(df)
        sub = sub.with_columns(pl.col("datetime").cast(pl.Datetime("us")).alias("datetime"))
        sub = sub.unique(subset=["symbol", "datetime"], keep="last")
        return sub

    total_holder = {"rows": 0}

    def _on_batch(batch_frames: list[pl.DataFrame]) -> None:
        pending.extend(batch_frames)
        if len(pending) >= _STOCK_MINUTE_BATCH:
            total_holder["rows"] += sum(f.height for f in pending)
            _flush_stock_minute_chunk(pending.copy())
            pending.clear()

    result = BackfillPool().map(_fetch_one, stocks,
                                batch_size=_STOCK_MINUTE_BATCH,
                                on_batch_done=_on_batch, keep_frames=False)
    if pending:
        total_holder["rows"] += sum(f.height for f in pending)
        _flush_stock_minute_chunk(pending)
        pending.clear()
    total = total_holder["rows"]
    logger.info("mootdx_service: 股票分钟按日回源 %s 完成, %d 行 (failed=%d)",
                day, total, len(result["failed"]))
    return total


def sync_stock_minute_range(days: list[_date]) -> int:
    """批量回补多个缺失交易日：每只拉一次全量历史，一次写入所有缺失日分区。

    与 ``sync_stock_minute_day``（逐缺失日全市场重拉）不同，多日缺口时每个
    symbol 只拉一次 ``get_minute`` 全量，再按 ``day_set`` 过滤出落在缺失日的
    bar，由 ``_flush_stock_minute_chunk`` 按各自交易日分组写入 ``date=`` 分区
    （读旧→concat→unique），避免 O(N_days × 全市场) 的重复回源。
    跳过退市/异常（上市日期 1970 占位）与上市日晚于整个缺失窗口末端的标的。
    返回写入行数。
    """
    day_set = set(days)
    window_end = max(days)
    since_day = min(days)  # 只回看到最早缺失日（get_minute since 分页）
    stocks = [s for s in _stock_universe() if not s.endswith(".BJ")]
    if not stocks:
        logger.warning("mootdx_service: 股票宇宙为空，跳过分钟同步")
        return 0
    listing = _listing_date_map()
    # 上市日期占位（1970-01-01 = instruments 退市/异常数据）：这些标的已无交易，
    # 回源必然获取不到（每次 8-10s 服务器轮换超时），直接跳过。
    pre_delisted = [s for s in stocks if listing.get(s) == _date(1970, 1, 1)]
    if pre_delisted:
        logger.info("mootdx_service: 跳过 %d 只退市/异常标的（上市日期占位）: %s",
                    len(pre_delisted), pre_delisted[:10])
        stocks = [s for s in stocks if s not in set(pre_delisted)]
    keep = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
    tick = _mk_progress_logger(len(stocks),
                               f"股票分钟批量回源 {len(days)} 日")
    counter = {"n": 0}
    pending: list[pl.DataFrame] = []

    def _fetch_one(src, sym):
        counter["n"] += 1
        tick(counter["n"], sym)
        ld = listing.get(sym)
        if ld is not None and ld > window_end:
            return None  # 上市晚于整个缺失窗口，窗口内无数据
        try:
            df = _guarded_get_minute(src, sym, max_bars=40000, since=since_day)
        except TimeoutError:
            _append_failure(sym, "timeout")
            raise
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: %s 分钟拉取失败: %s", sym, e)
            _append_failure(sym, f"exception:{str(e)[:60]}")
            return None
        if df is None or df.empty:
            _append_failure(sym, "empty")
            return None
        df = df.copy()
        df["symbol"] = sym
        df = df.reset_index()
        for c in keep:
            if c not in df.columns:
                df[c] = None
        df = df[keep]
        df = df[pd.to_datetime(df["datetime"]).dt.date.isin(day_set)]
        if df.empty:
            return None  # 缺失窗口内无 bar，跳过
        sub = pl.from_pandas(df)
        sub = sub.with_columns(pl.col("datetime").cast(pl.Datetime("us")).alias("datetime"))
        sub = sub.unique(subset=["symbol", "datetime"], keep="last")
        return sub

    total_holder = {"rows": 0}

    def _on_batch(batch_frames: list[pl.DataFrame]) -> None:
        pending.extend(batch_frames)
        if len(pending) >= _STOCK_MINUTE_BATCH:
            total_holder["rows"] += sum(f.height for f in pending)
            _flush_stock_minute_chunk(pending.copy())
            pending.clear()

    result = BackfillPool().map(_fetch_one, stocks,
                                batch_size=_STOCK_MINUTE_BATCH,
                                on_batch_done=_on_batch, keep_frames=False)
    if pending:
        total_holder["rows"] += sum(f.height for f in pending)
        _flush_stock_minute_chunk(pending)
        pending.clear()
    total = total_holder["rows"]
    logger.info("mootdx_service: 股票分钟批量回源 %d 个缺失日, %d 行 (failed=%d)",
                len(days), total, len(result["failed"]))
    return total


def backfill_missing_partitions(missing: dict[str, list[_date]]) -> dict:
    """逐缺失日复用现有 sync 函数补全，单日失败记 errors 不阻断。

    ``missing`` 键名与 ``scan_missing_partitions`` 一致：
    kline_daily / kline_etf_daily → sync_daily；kline_index_daily →
    sync_index_daily；kline_etf_minute → sync_etf_minute；
    kline_minute → sync_stock_minute_range（批量一次补全全部缺失日）。
    """
    result: dict = {
        "daily_days": [], "index_daily_days": [],
        "etf_minute_days": [], "stock_minute_days": [],
        "etf_nav_days": [], "errors": [],
    }

    for day in missing.get("kline_daily", []) + missing.get("kline_etf_daily", []):
        try:
            sync_daily(day)
            result["daily_days"].append(str(day))
        except Exception as e:  # noqa: BLE001
            result["errors"].append(f"daily {day}: {e}")
    for day in missing.get("kline_index_daily", []):
        try:
            _repair_index_day(day)
            result["index_daily_days"].append(str(day))
        except Exception as e:  # noqa: BLE001
            result["errors"].append(f"index_daily {day}: {e}")
    for day in missing.get("kline_etf_minute", []):
        try:
            sync_etf_minute(day)
            result["etf_minute_days"].append(str(day))
        except Exception as e:  # noqa: BLE001
            result["errors"].append(f"etf_minute {day}: {e}")
    min_days = missing.get("kline_minute", [])
    if min_days:
        try:
            sync_stock_minute_range(min_days)
            result["stock_minute_days"].extend(str(d) for d in min_days)
        except Exception as e:  # noqa: BLE001
            result["errors"].append(f"stock_minute {min_days}: {e}")

    for day in missing.get("etf_nav", []):
        try:
            from app.services import etf_nav_service
            etf_nav_service.sync_etf_nav(day)
            result["etf_nav_days"].append(str(day))
        except Exception as e:  # noqa: BLE001
            result["errors"].append(f"etf_nav {day}: {e}")

    return result


def scan_and_backfill_full(content_recent: int | None = None) -> dict:
    """00:00 全量巡检 + 补全入口：扫描缺失 → 逐日补全 → 汇总。

    ``content_recent``：内容校验窗口，默认近 1 周（``_DAILY_CHECK_RECENT_PARTITIONS``）；
    手动全量检验传 250（``_CONTENT_CHECK_RECENT_DAYS``）。
    """
    missing = scan_missing_partitions(content_recent=content_recent)
    backfilled = backfill_missing_partitions(missing)
    total = sum(len(v) for v in missing.values())
    msg = "mootdx_service: 全量扫描 %d 缺失日, 补全 %s, errors=%s"
    args = (total, {k: len(v) for k, v in backfilled.items()},
            len(backfilled["errors"]))
    # 补全后有残留错误 → WARNING（供钉钉通知等消费）
    if backfilled["errors"]:
        logger.warning(msg, *args)
    else:
        logger.info(msg, *args)
    return {"missing": missing, "backfilled": backfilled,
            "errors": backfilled["errors"]}


def _partition_symbols(root: Path, day: _date) -> set[str]:
    """读某日分区所有 parquet 的 symbol 集合（异常/缺失 → 空集）。"""
    pdir = root / f"date={day.isoformat()}"
    syms: set[str] = set()
    for p in sorted(pdir.glob("*.parquet")):
        try:
            syms |= set(pl.read_parquet(p, columns=["symbol"])["symbol"].to_list())
        except Exception:  # noqa: BLE001
            continue
    return syms


def _coverage(root: Path, day: _date, target: set[str]) -> tuple[float, set[str]]:
    """返回 (覆盖率, 缺失 symbol)。分区不存在/宇宙空 → (0.0, target)。"""
    if not target:
        return 0.0, set()
    have = _partition_symbols(root, day)
    if not have:
        return 0.0, target
    inter = have & target
    return len(inter) / len(target), target - have


def check_and_repair_day(day: _date) -> dict:
    """单日检验补齐：对该日 5 类逐类查内容，残缺/缺失则重写。

    返回 {"day": str, "results": {type: {"status": "ok"|"repaired"|"skip"|"failed",
                                          "coverage": float|None, "symbols": int}}}。
    单类失败不阻断其它类。
    """
    today = _date.today()
    if day == today and not _market_closed():
        # 盘中当日半程数据不可作基线（08-18 11:30 误标 13:00 同类污染），
        # 单日补齐同样跳过当日，防把半程残帧写成"完整"分区。
        return {"day": day.isoformat(), "results": {
            key: {"status": "skip", "coverage": None, "symbols": 0}
            for key in ["stock_daily", "etf_daily", "index_daily",
                        "etf_minute", "stock_minute"]}}

    results: dict[str, dict] = {}
    try:
        stocks = _stock_universe()
    except Exception:  # noqa: BLE001
        stocks = []
    try:
        etf_tf = set(_to_tf_symbol(c) for c in _etf_universe())
    except Exception:  # noqa: BLE001
        etf_tf = set()
    try:
        idx = _index_universe()
    except Exception:  # noqa: BLE001
        idx = []

    for key, root, target, repair in [
        ("stock_daily", STOCK_DAILY_ROOT, set(stocks), lambda: sync_daily(day)),
        ("etf_daily", ETF_DAILY_ROOT, etf_tf, lambda: sync_daily(day)),
        ("index_daily", INDEX_DAILY_ROOT, set(idx),
         lambda: _repair_index_day(day)),
        ("etf_minute", ETF_MINUTE_ROOT, etf_tf, lambda: sync_etf_minute(day)),
    ]:
        if not target:
            results[key] = {"status": "skip", "coverage": None, "symbols": 0}
            continue
        cov, _missing = _coverage(root, day, target)
        have = len(_partition_symbols(root, day))
        if cov >= _CONTENT_CHECK_MIN_COVERAGE:
            results[key] = {"status": "ok", "coverage": round(cov, 4), "symbols": have}
            continue
        try:
            repair()
            results[key] = {"status": "repaired", "coverage": round(cov, 4), "symbols": have}
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: 单日补齐 %s %s 失败: %s", key, day, e)
            results[key] = {"status": "failed", "coverage": round(cov, 4), "symbols": have}

    # 股票分钟：只补缺失 symbol（残缺少时快；停牌标的该日无 bar 由 sync 内部跳过）
    stock_target = set(stocks)
    cov, missing = _coverage(STOCK_MINUTE_ROOT, day, stock_target)
    have = len(_partition_symbols(STOCK_MINUTE_ROOT, day))
    if not stock_target:
        results["stock_minute"] = {"status": "skip", "coverage": None, "symbols": 0}
    elif cov >= _CONTENT_CHECK_MIN_COVERAGE:
        results["stock_minute"] = {"status": "ok", "coverage": round(cov, 4), "symbols": have}
    else:
        try:
            sync_stock_minute_day(day, symbols=sorted(missing))
            results["stock_minute"] = {"status": "repaired", "coverage": round(cov, 4),
                                       "symbols": have}
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: 单日补齐 stock_minute %s 失败: %s", day, e)
            results["stock_minute"] = {"status": "failed", "coverage": round(cov, 4),
                                       "symbols": have}

    return {"day": day.isoformat(), "results": results}


def check_and_repair_full(content_recent: int | None = None) -> dict:
    """全量检验补齐：全窗口内容校验 + 全量分区缺失补全。

    content_recent 默认 ``_CONTENT_CHECK_RECENT_DAYS``（250，≈1 年交易日）。
    """
    if content_recent is None:
        content_recent = _CONTENT_CHECK_RECENT_DAYS
    return scan_and_backfill_full(content_recent=content_recent)


def sync_adj_factor() -> dict:
    """增量更新 ETF 前复权因子表（mootdx xdxr 事件重建）。

    对宇宙内每只有除权事件的标的，用 xdxr 记录 + 日线 close 重建逐日
    ex_factor 序列，覆盖该标的在 ``all.parquet`` 中的行（全量重算该标的，
    幂等）。返回 {written_symbols, rows, total_symbols}。
    """
    src = MootdxSource()
    dm = DataManager()
    daily = dm._load_daily_from_partitions(asof=None)
    codes = _etf_universe()
    if not codes:
        return {"written_symbols": 0, "rows": 0, "total_symbols": 0}
    # 只保留宇宙内的标的
    daily = {k: v for k, v in daily.items() if k in set(codes)}
    frames = []
    for jq, pdf in daily.items():
        closes = pdf["close"].dropna()
        if closes.empty:
            continue
        rows = src._xdxr_rows(jq.split(".")[0])
        events = []
        for r in (rows or []):
            cat = r.get("category")
            year = r.get("year")
            if not year or int(year) < _SINCE_YEAR:
                continue
            try:
                ex_dt = pd.Timestamp(int(r["year"]), int(r["month"]), int(r["day"]))
            except Exception:
                continue
            if cat == 11:
                suogu = float(r.get("suogu") or 0)
                if suogu <= 0:
                    continue
                events.append((ex_dt, 1.0 / suogu))
            elif cat == 1:
                # 除权参考价公式与 mootdx_src._to_qfq 同口径：
                # ex_price = (prev_close - fh + pgj*pg) / (1+sg+pg)
                # factor = ex_price / prev_close。现金红利(fh)/配股会摊薄价格，
                # 纯送转比例式 1/(1+sg+pg) 会漏掉这两项（对齐 bug：510880 等
                # 有年度现金分红，漏算导致前复权价偏离聚宽）。
                fh = float(r.get("fenhong") or 0) / 10.0      # 每股现金红利(元)
                sg = float(r.get("songzhuangu") or 0) / 10.0  # 每股送转
                pg = float(r.get("peigu") or 0) / 10.0        # 每股配股
                pgj = float(r.get("peigujia") or 0)           # 配股价
                if fh == 0 and sg == 0 and pg == 0:
                    continue
                prev = closes.loc[closes.index < ex_dt].dropna()
                if prev.empty:
                    continue  # 除权日前一收盘价不在帧内，因子无法计算
                prev_close = float(prev.iloc[-1])
                if prev_close <= 0:
                    continue
                ex_price = (prev_close - fh + pgj * pg) / (1.0 + sg + pg)
                if ex_price <= 0:
                    continue
                events.append((ex_dt, ex_price / prev_close))
        if not events:
            continue
        events = [(e, f) for e, f in events if e < closes.index.max()]
        if not events:
            continue
        adj = pd.Series(1.0, index=closes.index)
        for ex_dt, f in events:
            adj.loc[adj.index < ex_dt] *= f
        frames.append(pl.DataFrame({
            "symbol": jq,
            "trade_date": [d.isoformat() for d in closes.index.date],
            "ex_factor": adj.values,
        }))
    if not frames:
        logger.info("mootdx_service: 无除权事件，因子表无更新")
        return {"written_symbols": 0, "rows": 0, "total_symbols": len(codes)}
    out = pl.concat(frames)
    out = out.with_columns(pl.col("trade_date").cast(pl.Date))
    out = out.unique(subset=["symbol", "trade_date"], keep="last").sort(
        ["symbol", "trade_date"])
    ADJ_FACTOR_PATH.parent.mkdir(parents=True, exist_ok=True)
    # 合并既有表（幂等）：同 symbol+date 覆盖，其余保留
    if ADJ_FACTOR_PATH.exists():
        old = pl.read_parquet(ADJ_FACTOR_PATH)
        out = pl.concat([old, out]).unique(
            subset=["symbol", "trade_date"], keep="last").sort(["symbol", "trade_date"])
    tmp = ADJ_FACTOR_PATH.parent / "all.tmp.parquet"
    out.write_parquet(tmp)
    tmp.rename(ADJ_FACTOR_PATH)
    n_syms = out["symbol"].n_unique()
    logger.info("mootdx_service: 因子表更新 %d 行 / %d 只 → %s",
                out.height, n_syms, ADJ_FACTOR_PATH)
    return {"written_symbols": len(frames), "rows": out.height,
            "total_symbols": len(codes)}


# ---------------------------------------------------------------------------
# 启动回源：系统启动时补齐到当前时间缺失的分钟线和日线
# ---------------------------------------------------------------------------

STOCK_DAILY_ROOT = DATA_ROOT / "kline_daily"
ETF_DAILY_ROOT = DATA_ROOT / "kline_etf_daily"
_DAILY_BACKFILL_LIMIT_DAYS = 90   # 启动回源最多往前补的天数（避免全历史重拉）


def _partition_dates(root: Path) -> list[str]:
    """返回某分区目录下全部 date=YYYY-MM-DD 列表（升序）。"""
    if not root.is_dir():
        return []
    return sorted(d.name[5:] for d in root.iterdir()
                  if d.is_dir() and d.name.startswith("date="))


def _trade_days_up_to(end: _date) -> list[_date]:
    """返回 (end-回看窗口, end] 内 A 股交易日（从沪深300 日线索引推导）。"""
    src = MootdxSource()
    start = end - _date(1970, 1, 1)  # placeholder, overwritten below
    from datetime import timedelta as _td
    start = end - _td(days=_DAILY_BACKFILL_LIMIT_DAYS)
    try:
        df = src.get_daily("000300.XSHG", start.strftime("%Y%m%d"),
                           end.strftime("%Y%m%d"))
        if df is not None and not df.empty:
            days = sorted(d.date() for d in df.index)
            # mootdx get_daily 忽略 start 参数，返回 000300 全历史（自 2023 起）。
            # 必须显式过滤下界，否则空分区 seed 会把全历史交易日列入回源
            # （08-07 空 kline_daily 目录触发 2023-04-20 起连续数日全量回源）。
            return [d for d in days if start <= d <= end]
    except Exception as e:
        logger.warning("mootdx_service: 交易日历获取失败: %s", e)
    # 兜底：工作日近似
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += _td(days=1)
    return days


def _trade_days_in_range(start: _date, end: _date) -> list[_date]:
    """返回 [start, end] 内 A 股交易日（从沪深300 日线推导，全区间不截断）。

    与 ``_trade_days_up_to``（90 天窗口）不同，本函数支持 4/1 至今的全区间
    扫描。取数失败回退工作日近似（不阻断检测）。
    """
    src = MootdxSource()
    try:
        df = src.get_daily("000300.XSHG", start.strftime("%Y%m%d"),
                           end.strftime("%Y%m%d"))
        if df is not None and not df.empty:
            # mootdx get_daily 忽略 start 参数返回全历史，需显式过滤下界
            return sorted(d.date() for d in df.index
                          if start <= d.date() <= end)
    except Exception as e:  # noqa: BLE001
        logger.warning("mootdx_service: 交易日历获取失败: %s", e)
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += _dt.timedelta(days=1)
    return days


def _missing_days_in(calendar: list[_date], root: Path) -> list[_date]:
    """calendar 中不在 root 的 date= 分区里的日期（中间洞也检）。"""
    existing = set(_partition_dates(root))
    return [d for d in calendar if d.isoformat() not in existing]


def scan_missing_partitions(start: _date | None = None,
                            content_recent: int | None = None) -> dict[str, list[_date]]:
    """分区级缺失扫描：4/1（或 start）至今，6 类数据按交易日历逐日比对。

    检测「交易日历上有、但分区目录无 date= 分区」的日期，含中间洞。
    仅分区级（分区存在即视为该日已覆盖），不逐 symbol 校验。

    内容级校验与分区缺口并集：5 类数据（ETF日线/股票日线/指数日线/ETF分钟/
    股票分钟）统一跑 ``_incomplete_*_days()``，把"目录存在但内容残缺"（如只剩
    1 只）的日子也归为缺失，触发重写，防残帧永久污染（见各函数 docstring）。

    额外返回 ``etf_universe_segments``（list[str]）：ETF 宇宙快照缺失的
    权威代码段（如 501/161）。分区覆盖率 vs 残缺宇宙永远测不出"快照缺段"，
    必须用权威段结构做基线（见 ``_etf_universe_segment_missing``）。

    ``content_recent`` 默认 ``_DAILY_CHECK_RECENT_PARTITIONS``（近 1 周），
    全量手动传 250。
    """
    today = _date.today()
    calendar = _trade_days_in_range(start or STOCK_MINUTE_START, today)
    from app.services.etf_nav_service import _missing_etf_nav_days as _missing_nav
    content = (_DAILY_CHECK_RECENT_PARTITIONS
               if content_recent is None else content_recent)
    missing_etf_daily = set(_missing_days_in(calendar, ETF_DAILY_ROOT))
    missing_etf_daily |= set(_incomplete_etf_daily_days(recent=content))
    # 盘中半程快照自愈（08-11 案例）：昨日/历史日分区 mtime 早于自身日期
    # 15:00 即判残缺重写。00:00 巡检跨天也能识别（旧实现只查今天）。
    missing_etf_daily |= set(_stale_daily_days(ETF_DAILY_ROOT))
    # 相对基线检测（08-21 案例：555/599 过绝对阈值但缺 44 只中证系）
    for _d in _shortfall_days(ETF_DAILY_ROOT):
        missing_etf_daily.add(_d)
    missing_stock_daily = set(_missing_days_in(calendar, STOCK_DAILY_ROOT))
    missing_stock_daily |= set(_stale_daily_days(STOCK_DAILY_ROOT))
    missing_stock_daily |= set(_incomplete_stock_daily_days(recent=content))
    for _d in _shortfall_days(STOCK_DAILY_ROOT):
        missing_stock_daily.add(_d)
    missing_index_daily = set(_missing_days_in(calendar, INDEX_DAILY_ROOT))
    missing_index_daily |= set(_incomplete_index_daily_days(recent=content))
    missing_index_daily |= set(_index_shortfall_days())
    missing_etf_minute = set(_missing_days_in(calendar, ETF_MINUTE_ROOT))
    missing_etf_minute |= set(_incomplete_etf_minute_days(recent=content))
    for _d in _shortfall_days(ETF_MINUTE_ROOT):
        missing_etf_minute.add(_d)
    missing_stock_minute = set(_missing_days_in(calendar, STOCK_MINUTE_ROOT))
    missing_stock_minute |= set(_incomplete_stock_minute_days(recent=content))
    seg_missing = _safe_universe_segment_missing()
    if seg_missing:
        logger.warning("mootdx_service: ETF 宇宙快照缺代码段 %s，"
                       "对应标的日线/分钟永不回源，请重建快照",
                       seg_missing)
    return {
        "kline_daily":       sorted(missing_stock_daily),
        "kline_etf_daily":   sorted(missing_etf_daily),
        "kline_index_daily": sorted(missing_index_daily),
        "kline_etf_minute":  sorted(missing_etf_minute),
        "kline_minute":      sorted(missing_stock_minute),
        "etf_nav":           _missing_nav(),
        "etf_universe_segments": sorted(seg_missing),
    }


MARKET_CLOSE_TIME = _dt.time(15, 0)  # 当日日线/分钟视为"可回源"的时间下限


def _market_closed(now: _dt.datetime | None = None) -> bool:
    """当前是否已收盘（≥15:00，含盘后）。盘中视为未收盘。"""
    now = now or _dt.datetime.now()
    return now.time() >= MARKET_CLOSE_TIME


def _missing_minute_days(now: _dt.datetime | None = None) -> list[_date]:
    """找出分区缺失的 ETF 分钟交易日（最新分区日期 → 今天）。

    盘中（<15:00）不把"今天"当作缺失日：当日分钟尚未走完，回源只会拿到
    半日数据，写入会污染当天分区（与日线同一问题）。收盘后才把今天算缺失。
    """
    existing = _partition_dates(ETF_MINUTE_ROOT)
    if not existing:
        return []
    latest = _date.fromisoformat(existing[-1])
    now = now or _dt.datetime.now()
    today = now.date()
    days = [d for d in _trade_days_up_to(today) if latest < d <= today]
    if not _market_closed(now):
        # 盘中不把"今天"当缺失日：当日分钟尚未走完，回源只拿到半日数据，且
        # mootdx 会把 11:30 错标 13:00:00，写入即污染分区（08-18 案例：盘中
        # 重启触发 backfill，latest<today 时旧实现 `latest>=today` 保护失效）。
        # 无论今日分区是否存在都排除今天；latest 之后的真实历史缺失日仍照常补。
        return [d for d in days if d < today]
    return days


def _missing_stock_minute_days(now: _dt.datetime | None = None) -> list[_date]:
    """找出股票分钟分区缺失的交易日（latest < d <= today，盘中排除今天）。

    镜像 ``_missing_minute_days``（ETF 分钟），但读 ``STOCK_MINUTE_ROOT``。
    resume 逻辑只看最新分区，新交易日无分区时会被误判"已覆盖"而跳过——
    本函数供 ``sync_stock_minute`` 在 resume 之前先 range 补整日缺失。
    """
    existing = _partition_dates(STOCK_MINUTE_ROOT)
    if not existing:
        return []
    latest = _dt.date.fromisoformat(existing[-1])
    now = now or _dt.datetime.now()
    today = now.date()
    days = [d for d in _trade_days_up_to(today) if latest < d <= today]
    if not _market_closed(now):
        return [d for d in days if d < today]
    return days


def _missing_daily_days(root: Path, now: _dt.datetime | None = None) -> list[_date]:
    """找出某日线分区缺失的交易日。

    盘中（<15:00）不把"今天"当作缺失日：当日日线尚未收全，回源只能拿到
    盘中半日快照（如 08-05 10:36 写入的坏分区），且写入后 ``latest==today``
    会让后续回源永久跳过修正。收盘后才把今天算缺失（若今日分区为盘中快照
    则一并重写，见 ``_stale_today_daily_days``），由 backfill 补全。

    在交易日历窗口内检测**所有**缺失日，含 ``latest`` 之前的中间洞：路径
    bug / 回源中断曾让某几日永久搁浅，若只查 ``latest < d`` 则 latest 跳过
    后旧洞永远漏检（08-04/05 案例）。

    中间洞检测限定在**最近 ``_DAILY_BACKFILL_LIMIT_DAYS`` 个交易日**内，
    而不是整个交易日历：``_trade_days_up_to`` 可能返回远超 90 天的全量
    历史（mootdx get_daily 忽略 start 参数），而分区在数据源覆盖起点前
    的"合法空缺"（如股票日线 2023-07~2025-07 空缺）不该被当作缺失重拉。
    窗口内的缺失即真实洞，latest 之后的部分照常补。
    """
    existing = _partition_dates(root)
    if not existing:
        return []
    now = now or _dt.datetime.now()
    today = now.date()
    # 盘中（<15:00）绝不把「今天」当缺失日回源：当日日线尚未收全，回源只会
    # 拉到盘中半日数据，写入即污染分区（08-11 12:50 重启时正是如此产生坏帧）。
    # 无论今日分区是否存在，盘中都不回源今天；中间洞（latest 之前）照常检。
    if not _market_closed(now):
        if existing[-1] >= today.isoformat():
            return []
        calendar = _trade_days_up_to(today)
        recent = calendar[-_DAILY_BACKFILL_LIMIT_DAYS:]
        days = _missing_days_in(recent, root)
        return [d for d in days if d < today]
    calendar = _trade_days_up_to(today)
    recent = calendar[-_DAILY_BACKFILL_LIMIT_DAYS:]
    days = _missing_days_in(recent, root)
    # 收盘后：最近分区中「早于其自身日期 15:00 写入」的盘中快照（含今日与
    # 历史日）都要强制重写为收盘完整数据（否则盘中坏分区会永久残留）。
    stale = _stale_daily_days(root, now)
    if stale:
        return sorted(set(days) | set(stale))
    return days


def _etf_universe_segment_missing(codes: list[str]) -> list[str]:
    """返回 ETF 宇宙中缺失的权威代码段（如 ``501``/``161``）。

    背景：旧内容校验只比「分区覆盖率 vs 当前宇宙」，但宇宙快照本身可能残缺
    （如服务器快照缺整个 501/161 段），此时分区覆盖率恒高、永不告警。本函数
    以 ``_ETF_UNIVERSE_EXPECTED_SEGMENTS``（聚宽名单段结构）为权威基线，
    宇宙里完全没有某段的任何代码即判该段缺失。

    返回缺失段列表（空 = 完整）。调用方应在此之前拦截空宇宙。
    """
    if not codes:
        return list(_ETF_UNIVERSE_EXPECTED_SEGMENTS)
    have = {c.split(".", 1)[0][:3] for c in codes if "." in c}
    return [seg for seg in _ETF_UNIVERSE_EXPECTED_SEGMENTS if seg not in have]


def _safe_universe_segment_missing() -> list[str]:
    """读取宇宙并返回缺失段；宇宙读取失败时降级为空（不阻断巡检）。"""
    try:
        codes = _etf_universe()
    except Exception:  # noqa: BLE001
        logger.warning("mootdx_service: ETF 宇宙读取失败，跳过段校验", exc_info=True)
        return []
    if not codes:
        return []
    return _etf_universe_segment_missing(codes)


def _incomplete_partition_days(root: Path, target: set[str], recent: int,
                               min_coverage: float,
                               skip_today_intraday: bool = True) -> list[_date]:
    """最近 recent 个分区 symbol 覆盖率 < min_coverage 即判残缺（目录存在≠完整）。

    root: 分区根目录；target: 已归一化的基准宇宙 symbol 集合。
    分区根 / 宇宙为空 → []（无基线可比）。盘中跳过当日分区（防半程误伤）。
    """
    existing = _partition_dates(root)
    if not existing or not target:
        return []
    today = _date.today()
    out: list[_date] = []
    for ds in existing[-recent:]:
        d = _dt.date.fromisoformat(ds)
        if skip_today_intraday and d == today and not _market_closed():
            logger.info("mootdx_service: 当日 %s 盘中未收盘，跳过内容校验", ds)
            continue
        pdir = root / f"date={ds}"
        parts = sorted(pdir.glob("*.parquet"))
        if not parts:
            continue
        syms: set[str] = set()
        for p in parts:
            try:
                df = pl.read_parquet(p, columns=["symbol"])
                syms |= set(df["symbol"].to_list())
            except Exception:  # noqa: BLE001
                continue
        coverage = len(syms & target) / len(target)
        if coverage < min_coverage:
            out.append(d)
    logger.info("mootdx_service: 内容校验 %s 最近 %d 分区, 残缺 %d: %s",
                root.name, len(existing[-recent:]), len(out),
                [d.isoformat() for d in out])
    return out


def _incomplete_stock_daily_days(recent: int | None = None) -> list[_date]:
    """股票日线内容残缺分区（symbol 覆盖率 << 股票宇宙）。"""
    try:
        codes = _stock_universe()
    except Exception:  # noqa: BLE001
        logger.warning("mootdx_service: 股票宇宙读取失败，跳过股票日线内容校验",
                       exc_info=True)
        return []
    return _incomplete_partition_days(
        STOCK_DAILY_ROOT, set(codes),
        recent or _CONTENT_CHECK_RECENT_DAYS, _CONTENT_CHECK_MIN_COVERAGE)


def _incomplete_etf_daily_days(recent: int | None = None) -> list[_date]:
    """ETF 日线内容残缺分区（symbol 覆盖率 << ETF 宇宙）。"""
    try:
        codes = _etf_universe()
    except Exception:  # noqa: BLE001
        logger.warning("mootdx_service: ETF 宇宙读取失败，跳过 ETF 日线内容校验",
                       exc_info=True)
        return []
    if not codes:
        return []
    target = set(_to_tf_symbol(c) for c in codes)
    return _incomplete_partition_days(
        ETF_DAILY_ROOT, target,
        recent or _CONTENT_CHECK_RECENT_DAYS, _CONTENT_CHECK_MIN_COVERAGE)


def _incomplete_index_daily_days(recent: int | None = None) -> list[_date]:
    """指数日线内容残缺分区（symbol 覆盖率 << 指数宇宙）。

    07-31 案例：instruments_index 缺失时曾用兜底 4 只指数写入，4/600 残帧
    目录存在 → 分区级扫描永不重写；此处内容校验兜住。
    """
    try:
        codes = _index_universe()
    except Exception:  # noqa: BLE001
        logger.warning("mootdx_service: 指数宇宙读取失败，跳过指数日线内容校验",
                       exc_info=True)
        return []
    if not codes:
        return []
    return _incomplete_partition_days(
        INDEX_DAILY_ROOT, set(codes),
        recent or _CONTENT_CHECK_RECENT_DAYS, _CONTENT_CHECK_MIN_COVERAGE)


def _incomplete_etf_minute_days(recent: int | None = None) -> list[_date]:
    """ETF 分钟内容残缺分区（symbol 覆盖率 << ETF 宇宙）。"""
    try:
        codes = _etf_universe()
    except Exception:  # noqa: BLE001
        logger.warning("mootdx_service: ETF 宇宙读取失败，跳过 ETF 分钟内容校验",
                       exc_info=True)
        return []
    if not codes:
        return []
    target = set(_to_tf_symbol(c) for c in codes)
    return _incomplete_partition_days(
        ETF_MINUTE_ROOT, target,
        recent or _CONTENT_CHECK_RECENT_DAYS, _CONTENT_CHECK_MIN_COVERAGE)


def _incomplete_stock_minute_days(recent: int | None = None) -> list[_date]:
    """股票分钟内容残缺分区（symbol 覆盖率 << 股票宇宙）。"""
    try:
        codes = _stock_universe()
    except Exception:  # noqa: BLE001
        logger.warning("mootdx_service: 股票宇宙读取失败，跳过分钟内容校验",
                       exc_info=True)
        return []
    if not codes:
        return []
    recent = _STOCK_MINUTE_RECENT_LIMIT if recent is None else recent
    return _incomplete_partition_days(
        STOCK_MINUTE_ROOT, set(codes),
        recent, _STOCK_MINUTE_MIN_COVERAGE)


def _stock_universe() -> list[str]:
    """返回全市场 A 股 symbol 列表（.SH/.SZ，优先 instruments parquet）。"""
    inst_path = DATA_ROOT / "instruments" / "instruments.parquet"
    if inst_path.exists():
        try:
            df = pl.read_parquet(inst_path, columns=["symbol"])
            syms = df["symbol"].to_list()
            if syms:
                return sorted(syms)
        except Exception as e:
            logger.warning("mootdx_service: instruments 读取失败: %s", e)
    # 兜底：从已有日线分区收集
    lf = pl.scan_parquet(str(STOCK_DAILY_ROOT / "**" / "*.parquet"),
                         hive_partitioning=True)
    return sorted(lf.select("symbol").unique().collect()["symbol"].to_list())


def _jq_to_tf_symbol(sym: str) -> str:
    """分区 symbol (.SH/.SZ) -> mootdx 6位纯代码。"""
    return sym.split(".")[0]


def _guarded_get_daily(src: MootdxSource, sym: str, start: str, end: str,
                       timeout: float = 20.0) -> pd.DataFrame | None:
    """带墙钟超时的 mootdx 日线取数，防长批回源时单只卡死整批。

    mootdx 底层 socket 在服务器劣化时可能永久阻塞（`_with_server_retry` 的
    线程 join 有 10s 超时，但 `_probe`/`_make_client` 等路径仍可能挂起）。
    这里在调用外再包一层线程守护，超时则弃帧不阻断整批。
    """
    import threading as _th
    box: dict = {}

    def _run() -> None:
        try:
            box["df"] = src.get_daily(sym, start, end)
        except Exception as e:  # noqa: BLE001
            box["err"] = e

    t = _th.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        # 卡死：弃帧并强制重建连接，避免后续请求继续用坏 socket
        try:
            src._client = None
            src._server_idx = -1
        except Exception:
            pass
        logger.warning("mootdx_service: %s 日线超时(%ss)，弃帧重建连接", sym, timeout)
        return None
    if "err" in box:
        return None
    return box.get("df")


def _guarded_get_minute(src: MootdxSource, sym: str, max_bars: int = 40000,
                        timeout: float | None = None,
                        since=None) -> pd.DataFrame | None:
    """带墙钟超时的 mootdx 分钟取数，防长批回源时单只卡死整批。

    ``get_minute`` 分页拉取（``since`` 给定时只回看到覆盖 [since, today]，
    见 mootdx_src.get_minute），正常 ~2s、慢标的 ~15s；服务器劣化时底层
    socket 可能永久挂起。这里外包线程守护，超时**抛异常**让调用方
    重建 MootdxSource（彻底刷新连接状态，避免坏 socket/坏 server 索引残留）。

    默认超时须覆盖内层 ``_with_server_retry`` 整轮服务器轮换的最坏耗时
    （``_TDX_FETCH_GUARD_TIMEOUT``）：否则会在轮换中途被掐断、遗弃内层线程
    （线程继续后台换服务器、堆积 socket），长跑回源形成死亡螺旋（08-19 案例）。
    因 socket 已设读超时（见 mootdx_src._patch），单次轮换是有界的，不会无限挂。
    """
    import threading as _th
    box: dict = {}

    def _run() -> None:
        try:
            box["df"] = src.get_minute(sym, max_bars=max_bars, since=since)
        except Exception as e:  # noqa: BLE001
            box["err"] = e

    if timeout is None:
        from app.quant.jqengine.datasource import mootdx_src as _msrc
        timeout = _msrc._TDX_FETCH_GUARD_TIMEOUT
    t = _th.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        raise TimeoutError(f"{sym} 分钟拉取超时({timeout}s)")
    if "err" in box:
        raise box["err"]
    return box.get("df")


def sync_daily(day: _date) -> dict:
    """回源指定交易日全市场日线（股票 kline_daily + ETF kline_etf_daily）。

    取数经 :class:`BackfillPool` 并发（每 worker 独立连接，坏连接由池自愈
    重建），逐标的用 mootdx ``get_daily`` 拉最近日线，取 ``day`` 那根写分区：
    股票 volume 换手（mootdx 股 ÷100），ETF volume 保持股。返回统计。
    """
    # 北交所（920xxx.BJ）mootdx 通达信接口无数据（每只轮换全服务器 ~8-11s 超时），
    # 全量回源时跳过，避免 331 只累积 ~50 分钟纯失败。
    stocks = [s for s in _stock_universe() if not s.endswith(".BJ")]
    etfs = [_to_tf_symbol(c) for c in _etf_universe()]
    day_str = day.strftime("%Y%m%d")
    # 跳过上市日晚于目标日的标的（新股在该日前无数据）；上市日期占位
    # （1970-01-01 = 退市/异常）的标的 4/1 前已无交易，一并跳过免超时。
    # 注意：只对股票做该过滤。ETF 宇宙来自 etf_universe_snapshot（无退市占位），
    # 且 _listing_date_map 读的是股票 instruments 表，对 ETF 过滤会把全部 ETF
    # 判为 1970 占位退市 → daily_written.etf 恒 0，ETF 日线永不落盘。
    listing = _listing_date_map()
    if listing:
        def _active(sym: str) -> bool:
            ld = listing.get(sym, _date(1970, 1, 1))
            return ld > _date(1970, 1, 1) and ld <= day
        stocks = [s for s in stocks if _active(s)]
    written = {"stock": 0, "etf": 0}

    def _fetch_one(src, sym):
        try:
            df = _guarded_get_daily(src, sym, day_str, day_str)
        except Exception:  # noqa: BLE001
            return None
        if df is None or df.empty:
            return None
        # 只保留目标日那根（索引含 15:00 时间戳）
        hit = df[[x.date() == day for x in df.index]]
        if hit.empty:
            return None
        row = hit.iloc[-1]
        return pl.DataFrame({
            "symbol": [sym],
            "date": [day],
            "open": [float(row["open"])],
            "high": [float(row["high"])],
            "low": [float(row["low"])],
            "close": [float(row["close"])],
            "volume": [float(row["volume"])],
            "amount": [float(row["amount"])],
        })

    pool = BackfillPool()
    stock_res = pool.map(_fetch_one, stocks)
    etf_res = pool.map(_fetch_one, etfs)
    sdf = pl.concat(stock_res["ok"]) if stock_res["ok"] else None
    edf = pl.concat(etf_res["ok"]) if etf_res["ok"] else None
    if sdf is not None:
        sdf = sdf.with_columns((pl.col("volume") / 100.0).alias("volume"))
        written["stock"] = sdf.height
    if edf is not None:
        written["etf"] = edf.height
    # 防御：回源结果为空不能静默——否则分区缺口无声累积（曾因过滤 bug 让
    # ETF 日线数月未落盘而不自知）。全市场全失败通常意味着数据源/过滤异常。
    if sdf is not None and edf is None:
        logger.warning("mootdx_service: 日线回源 %s 股票 %d 只但 ETF 全部失败，"
                       "请检查 ETF 宇宙/过滤逻辑", day, written["stock"])
    if sdf is None and edf is None:
        logger.warning("mootdx_service: 日线回源 %s 股票与 ETF 全部失败", day)

    if sdf is not None:
        _write_daily_partition(sdf, STOCK_DAILY_ROOT)
    if edf is not None:
        _write_daily_partition(edf, ETF_DAILY_ROOT)
    logger.info("mootdx_service: 日线回源 %s 完成: %s", day, written)
    return written


def _write_daily_partition(df: pl.DataFrame, root: Path) -> None:
    """按 date 分区原子写日线（读旧→concat→unique→tmp→rename）。

    兼容两类既有格式：
    - 股票日线（kline_daily）：文件内带 ``date`` 列；
    - ETF 日线（kline_etf_daily）：文件内**无** date 列，日期由 hive 分区
      目录名隐含。若既有分区无 date 列，合并前去掉新帧的 date 列，保持
      与既有格式一致（否则 concat 列数不一致报 ShapeError）。
    """
    ds = df["date"][0].isoformat() if hasattr(df["date"][0], "isoformat") else str(df["date"][0])
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


def _index_universe() -> list[str]:
    """返回指数日线回源 symbol 列表（.SH/.SZ，优先 instruments_index 表）。

    指数宇宙：``data/instruments_index/instruments_index.parquet`` 的 symbol 列。
    缺失/读取失败时兜底模拟盘固定 4 只（000300/000510/399006/399101），确保
    弱市判断/基准数据始终可用。
    """
    inst_path = DATA_ROOT / "instruments_index" / "instruments_index.parquet"
    if inst_path.exists():
        try:
            df = pl.read_parquet(inst_path, columns=["symbol"])
            syms = df["symbol"].to_list()
            if syms:
                return sorted(syms)
        except Exception:  # noqa: BLE001
            logger.warning("mootdx_service: instruments_index 读取失败, 用兜底指数")
    return ["000300.SH", "000510.SH", "399006.SZ", "399101.SZ"]


def sync_index_daily(day: _date) -> dict:
    """回源指定交易日全市场指数日线到 ``kline_index_daily`` 分区。

    逐个指数用 mootdx ``get_daily``（指数自动走 index_bars）拉最近日线，
    取 ``day`` 那根写按日分区。跳过北交所（899xxx.BJ mootdx 无数据）。
    返回 {"written": 写入指数数, "symbols": 尝试数}。
    """
    indices = [s for s in _index_universe() if not s.endswith(".BJ")]
    day_str = day.strftime("%Y%m%d")

    def _fetch_one(src, sym):
        try:
            df = _guarded_get_daily(src, sym, day_str, day_str)
            if df is None or df.empty:
                return None
            hit = df[[x.date() == day for x in df.index]]
            if hit.empty:
                return None
            row = hit.iloc[-1]
            return pl.DataFrame({
                "symbol": [sym],
                "date": [day],
                "open": [float(row["open"])],
                "high": [float(row["high"])],
                "low": [float(row["low"])],
                "close": [float(row["close"])],
                "volume": [float(row["volume"])],
                "amount": [float(row["amount"])],
            })
        except Exception:  # noqa: BLE001
            return None

    result = BackfillPool().map(_fetch_one, indices)
    frames = result["ok"]
    if not frames:
        logger.warning("mootdx_service: 指数日线回源 %s 全部失败（%d 只尝试）",
                       day, len(indices))
        return {"written": 0, "symbols": len(indices)}
    _write_daily_partition(pl.concat(frames), INDEX_DAILY_ROOT)
    logger.info("mootdx_service: 指数日线回源 %s 完成: %d 只 (failed=%d)",
                day, len(frames), len(result["failed"]))
    return {"written": len(frames), "symbols": len(indices)}


def _missing_index_daily_days() -> list[_date]:
    """找出 ``kline_index_daily`` 分区缺失的交易日。"""
    return _missing_daily_days(INDEX_DAILY_ROOT)


# ---------------------------------------------------------------------------
# 指数日线相对基线检测 + 跨源补齐
#
# 背景（08-21 案例）：绝对覆盖率阈值(0.5)只能防灾难性残帧；当日分区 555/609
# (91%) 通过全部校验，但比近 5 日基线 599 少 44 只——部分缺失需相对基线检测。
# 且缺的 000xxx.SH 中证/国证系列 mootdx 不提供，须路由 TickFlow 源补齐。
# ---------------------------------------------------------------------------

_INDEX_SHORTFALL_LOOKBACK = 10
_INDEX_SHORTFALL_RATIO = 0.95


_SHORTFILL_PREOPEN_BLOCK_FROM = _dt.time(6, 0)  # 盘前屏蔽起点（06:00 起）


def _shortfall_repair_allowed(now: _dt.datetime | None = None) -> bool:
    """相对基线缺口扫描/修复的时段门控（仅作用于启动回源路径）。

    - 周末：全天允许；
    - 交易日：<06:00 允许（深夜与 00:00 巡检同属安全窗口）；06:00-15:00
      （盘前+盘中）一律跳过——缺口修复密集请求已被限速的公共服务器，
      会挤占盘中实时取数配额，且靠近开盘的修复可能拖进交易时段；
    - ≥15:00 收盘后允许。
    - 此类启动跳过后由当日 00:00 全量巡检或下一次收盘后启动兜底。
    """
    now = now or _dt.datetime.now()
    if now.weekday() >= 5:
        return True
    t = now.time()
    return t >= MARKET_CLOSE_TIME or t < _SHORTFILL_PREOPEN_BLOCK_FROM


def _partition_symbol_sets(root: Path, lookback: int) -> list[tuple[Path, set]]:
    """近 lookback 个分区的 (目录, symbol 集) 列表（按日期升序）。"""
    days = sorted(root.glob("date=*"))[-lookback:]
    out = []
    for d in days:
        p = d / "part.parquet"
        if not p.exists():
            continue
        try:
            syms = set(pl.read_parquet(p, columns=["symbol"])["symbol"].to_list())
        except Exception:  # noqa: BLE001
            continue
        out.append((d, syms))
    return out


def _shortfall_days(
    root: Path,
    lookback: int = _INDEX_SHORTFALL_LOOKBACK,
    ratio: float = _INDEX_SHORTFALL_RATIO,
) -> dict[_date, list[str]]:
    """相对基线检测：近 N 分区以最大 symbol 集为基线，返回显著低于基线的
    {日期: 缺失清单}。适用于任一按日分区数据集。

    - 基线取窗口内最大集（退市/停牌类正常波动 ≤5% 由 ratio 容忍）；
    - 当日盘中不判（半程数据不可作依据，与既有守卫同口径）；
    - 分区 <3 个时无基线可比，返回空。
    """
    if not root.is_dir():
        return {}
    sets_ = _partition_symbol_sets(root, lookback)
    if len(sets_) < 3:
        return {}
    base_syms = max((s for _, s in sets_), key=len)
    if not base_syms:
        return {}
    today = _date.today()
    out: dict[_date, list[str]] = {}
    for d, syms in sets_:
        day = _date.fromisoformat(d.name.removeprefix("date="))
        if day == today and not _market_closed():
            continue
        missing = sorted(base_syms - syms)
        if missing and len(syms) < len(base_syms) * ratio:
            out[day] = missing
    return out


def _index_shortfall_days(
    lookback: int = _INDEX_SHORTFALL_LOOKBACK,
    ratio: float = _INDEX_SHORTFALL_RATIO,
) -> dict[_date, list[str]]:
    """指数日线相对基线检测（泛化入口的 index 包装）。"""
    return _shortfall_days(INDEX_DAILY_ROOT, lookback=lookback, ratio=ratio)


def _missing_vs_baseline(root: Path, day: _date,
                         lookback: int = _INDEX_SHORTFALL_LOOKBACK) -> list[str]:
    """单日相对基线的缺失清单（基线不足时返回空=无法判定）。"""
    sets_ = _partition_symbol_sets(root, lookback)
    base_syms = max((s for _, s in sets_), key=len) if sets_ else set()
    if not base_syms:
        return []
    pdir = root / f"date={day.isoformat()}" / "part.parquet"
    if not pdir.exists():
        return sorted(base_syms)
    try:
        syms = set(pl.read_parquet(pdir, columns=["symbol"])["symbol"].to_list())
    except Exception:  # noqa: BLE001
        return sorted(base_syms)
    return sorted(base_syms - syms)


def _index_missing_vs_baseline(day: _date,
                               lookback: int = _INDEX_SHORTFALL_LOOKBACK) -> list[str]:
    """指数日线单日缺失清单（泛化入口的 index 包装）。"""
    return _missing_vs_baseline(INDEX_DAILY_ROOT, day, lookback)


def _cross_source_index_repair(day: _date, missing: list[str]) -> int:
    """TickFlow 源补齐 mootdx 不提供的指数日K（仅缺口清单，按需拉取）。"""
    from app.services.index_sync import sync_and_persist_index_daily
    from app.tickflow.policy import detect_capabilities
    from app.tickflow.repository import DataStore, KlineRepository

    store = DataStore()
    repo = KlineRepository(store)
    start = _dt.datetime.combine(day, _dt.time.min)
    end = _dt.datetime.combine(day + _dt.timedelta(days=1), _dt.time.min)
    return sync_and_persist_index_daily(
        repo, detect_capabilities(),
        start_date=start, end_date=end,
        symbols_override=sorted(missing))


def _repair_index_day(day: _date,
                      allow_cross_source: bool = True) -> dict:
    """指数日线单日两级修复：先 mootdx（快、覆盖主指），复查基线缺口后
    路由 TickFlow 源补齐 mootdx 不提供的系列。返回 {"mootdx":..., "cross":...}。

    ``allow_cross_source=False``：启动路径的时段门控——盘前/盘中启动只做
    mootdx 一级，跨源拉取留待盘后巡检（避免挤占实时配额）。
    """
    w = sync_index_daily(day)
    n_cross = 0
    missing = _missing_vs_baseline(INDEX_DAILY_ROOT, day)
    if missing and allow_cross_source:
        logger.warning(
            "mootdx_service: 指数日线 %s 相对基线缺 %d 只，路由 TickFlow 源补齐",
            day, len(missing))
        try:
            n_cross = _cross_source_index_repair(day, missing) or 0
            logger.info("mootdx_service: 跨源补齐 %s 完成 +%d 行", day, n_cross)
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: 跨源补齐 %s 失败: %s", day, e)
    elif missing:
        logger.warning(
            "mootdx_service: 指数日线 %s 相对基线缺 %d 只（盘前/盘中不跨源，"
            "留待当日 00:00 巡检或收盘后补齐）", day, len(missing))
    return {"mootdx": w, "cross": n_cross}


def _stale_daily_days(root: Path, now: _dt.datetime | None = None,
                      recent: int | None = None) -> list[_date]:
    """收盘后找出最近日线分区中「早于该分区自身日期收盘」写入的盘中快照。

    场景：盘中某次 backfill 把半日数据当完整日线写入（mtime < 该日 15:00）。
    旧实现 ``_stale_today_daily_days`` 只查"今天"——昨天 12:50 写入的半程
    快照一旦跨天就永远漏检（08-11 案例：15:35 cron 缺席，次日无人重写）。
    本函数按分区**自身的日期**判定：part.parquet 的 mtime 早于该日期 15:00
    即盘中写入，无论"今天"是几号都能识别，跨天也能自愈。
    """
    now = now or _dt.datetime.now()
    existing = _partition_dates(root)
    if not existing:
        return []
    recent = _DAILY_BACKFILL_LIMIT_DAYS if recent is None else recent
    out: list[_date] = []
    for ds in existing[-recent:]:
        d = _dt.date.fromisoformat(ds)
        pdir = root / f"date={ds}"
        part = pdir / "part.parquet"
        if not part.exists():
            continue
        try:
            mt = _dt.datetime.fromtimestamp(part.stat().st_mtime)
        except OSError:
            continue
        # 盘中快照：写入时刻早于该分区日期的 15:00 收盘
        if mt < _dt.datetime.combine(d, MARKET_CLOSE_TIME):
            out.append(d)
    return out


def _stale_today_daily_days(root: Path, now: _dt.datetime | None = None) -> list[_date]:
    """收盘后发现「今天」分区存在但写入时间早于收盘（盘中快照）→ 需重写。

    保留兼容旧测试/调用；通用判定见 ``_stale_daily_days``（覆盖任意历史日）。
    """
    now = now or _dt.datetime.now()
    today = now.date()
    return [today] if today in _stale_daily_days(root, now) else []


def _adj_factor_stale() -> bool:
    """判断 ETF 前复权因子表是否落后于 ETF 日线最新交易日。

    因子表 ``all.parquet`` 的 ``max(trade_date)`` 落后于 ``kline_etf_daily``
    最新分区日期（或有新增交易日）即视为 stale，需重新 ``sync_adj_factor``。
    文件不存在视为 stale（首次部署）。
    """
    if not ADJ_FACTOR_PATH.exists():
        return True
    try:
        df = pl.read_parquet(ADJ_FACTOR_PATH, columns=["trade_date"])
        if df.is_empty():
            return True
        factor_latest = df["trade_date"].max()
        if not isinstance(factor_latest, _date):
            return True
    except Exception:  # noqa: BLE001
        return True
    etf_days = _partition_dates(ETF_DAILY_ROOT)
    if not etf_days:
        # 无 ETF 日线分区：以因子表自身为基准，不误判（有分区时以下逻辑生效）
        return factor_latest < _date.today()
    etf_latest = _date.fromisoformat(etf_days[-1])
    return factor_latest < etf_latest


def _notify_missing(missing: dict) -> None:
    """空分区/缺口/宇宙缺段时打 WARNING 日志并尝试钉钉站内信通知用户。fire-and-forget。"""
    lines = []
    for name, st in missing.items():
        latest = st.get("latest") or "无"
        line = f"- {name}: 最新 {latest}（empty={st.get('empty')}, missing={st.get('missing')}）"
        seg = st.get("segment_missing")
        if seg:
            line += f" [宇宙缺代码段: {seg}]"
        lines.append(line)
    msg = "mootdx 启动回源发现以下数据集缺失/缺口:\n" + "\n".join(lines)
    logger.warning("mootdx_service: %s", msg)
    try:
        from app.quant import db as qdb
        webhook = qdb.get_quant_setting("dingtalk_webhook_url") or ""
        secret = qdb.get_quant_setting("dingtalk_secret") or ""
        if webhook:
            from app.quant.notify import send_dingtalk
            send_dingtalk(webhook, secret, "模拟盘数据回源缺口", msg)
    except Exception:  # noqa: BLE001
        logger.warning("mootdx_service: 钉钉缺口通知失败（忽略）")


def backfill_to_now() -> dict[str, Any]:
    """启动回源：补齐到当前时间缺失的全部数据集（幂等，持 :data:`_SYNC_LOCK`）。

    覆盖：ETF 分钟 + 全市场日线（股票/ETF）+ 指数日线 + 股票分钟一批 +
    ETF 前复权因子表（stale 时）。缺日期分区回溯补窗口；完全空分区也补窗口
    并标记 missing + 钉钉告警。失败标的跳过不阻断。
    返回结果含 index_daily_days/index_daily_written/adj_factor/missing。

    与 15:35 cron、00:00 全量巡检共用 ``_SYNC_LOCK`` 串行——启动 backfill
    此前不持锁，服务重启时会与在途巡检并发轰击同一批服务器。
    """
    with _SYNC_LOCK:
        return _backfill_to_now_locked()


def _backfill_to_now_locked() -> dict[str, Any]:
    """``backfill_to_now`` 的执行体（调用方须已持 :data:`_SYNC_LOCK`）。"""
    # 相对基线缺口检测/修复仅在收盘后或周末（交易日 06:00 前）运行：
    # 盘前 06:00 起与盘中启动一律跳过，由当日 00:00 巡检兜底。
    shortfall_ok = _shortfall_repair_allowed()
    result: dict[str, Any] = {
        "minute_days": [], "minute_rows": 0,
        "daily_days": [], "daily_written": {},
        "index_daily_days": [], "index_daily_written": {},
        "adj_factor": None,
        "stock_minute_rows": 0, "stock_minute_days": [], "etf_nav_days": [], "errors": [],
    }

    from app.services import etf_nav_service
    stocks_daily      = _partition_dates(STOCK_DAILY_ROOT)
    etf_daily_days    = _partition_dates(ETF_DAILY_ROOT)
    index_daily_days  = _partition_dates(INDEX_DAILY_ROOT)
    etf_minute_days   = _partition_dates(ETF_MINUTE_ROOT)
    stock_minute_days = _partition_dates(STOCK_MINUTE_ROOT)
    etf_nav_days      = etf_nav_service._partition_dates()
    missing_nav_days  = etf_nav_service._missing_etf_nav_days()
    # 因子表最新交易日（all.parquet 的 max trade_date；缺失/损坏时 None）
    adj_factor_latest = None
    if ADJ_FACTOR_PATH.exists():
        try:
            _df = pl.read_parquet(ADJ_FACTOR_PATH, columns=["trade_date"])
            if not _df.is_empty():
                adj_factor_latest = str(_df["trade_date"].max())
        except Exception:  # noqa: BLE001
            pass

    content = _DAILY_CHECK_RECENT_PARTITIONS
    incomplete_etf_minute = set(_incomplete_etf_minute_days(recent=content))
    incomplete_stock_daily = set(_incomplete_stock_daily_days(recent=content))
    incomplete_etf_daily = set(_incomplete_etf_daily_days(recent=content))
    # 启动全量校验窗口（默认 250）：missing 字典 / daily 缺口集合共用一次扫描
    # 结果（此前在两处各扫一遍，250 分区 × symbol 列读取 ~12s ×2）。
    incomplete_etf_daily_full = set(_incomplete_etf_daily_days())
    incomplete_index_daily = set(_incomplete_index_daily_days(recent=content))
    incomplete_stock_minute = set(_incomplete_stock_minute_days(recent=content))
    missing_stock_minute_days = set(_missing_stock_minute_days())

    result["missing"] = {
        "kline_etf_minute":   {"latest": etf_minute_days[-1] if etf_minute_days else None,
                               "empty": not etf_minute_days,
                               "missing": bool(_missing_minute_days() or incomplete_etf_minute)},
        "kline_daily":        {"latest": stocks_daily[-1] if stocks_daily else None,
                               "empty": not stocks_daily, "missing": bool(_missing_daily_days(STOCK_DAILY_ROOT))},
        "kline_etf_daily":    {"latest": etf_daily_days[-1] if etf_daily_days else None,
                               "empty": not etf_daily_days,
                               "missing": bool(_missing_daily_days(ETF_DAILY_ROOT)
                                               or incomplete_etf_daily_full),
                               "segment_missing": _safe_universe_segment_missing()},
        "kline_index_daily":  {"latest": index_daily_days[-1] if index_daily_days else None,
                               "empty": not index_daily_days,
                               "missing": bool(_missing_index_daily_days() or incomplete_index_daily)},
        "kline_minute":       {"latest": stock_minute_days[-1] if stock_minute_days else None,
                               "empty": not stock_minute_days,
                               "missing": bool(missing_stock_minute_days or incomplete_stock_minute)},
        "adj_factor_etf":     {"latest": adj_factor_latest, "empty": not ADJ_FACTOR_PATH.exists(),
                               "missing": _adj_factor_stale()},
        "etf_nav":            {"latest": etf_nav_days[-1] if etf_nav_days else None,
                               "empty": not etf_nav_days, "missing": bool(missing_nav_days)},
    }

    # 1. ETF 分钟（含相对基线残缺日）
    etf_minute_shortfall = (set(_shortfall_days(ETF_MINUTE_ROOT))
                            if shortfall_ok else set())
    for day in sorted(set(_missing_minute_days()) | incomplete_etf_minute
                      | etf_minute_shortfall):
        try:
            n = sync_etf_minute(day)
            result["minute_days"].append(str(day))
            result["minute_rows"] += n
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: 分钟回源 %s 失败: %s", day, e)
            result["errors"].append(f"minute {day}: {e}")

    # 2. 日线（股票 + ETF）——统一用一个交易日历；空时补最近窗口
    today = _date.today()
    stock_daily_shortfall = (set(_shortfall_days(STOCK_DAILY_ROOT))
                             if shortfall_ok else set())
    etf_daily_shortfall = (set(_shortfall_days(ETF_DAILY_ROOT))
                           if shortfall_ok else set())
    daily_days = sorted(set(_missing_daily_days(STOCK_DAILY_ROOT))
                        | set(_missing_daily_days(ETF_DAILY_ROOT))
                        | stock_daily_shortfall
                        | etf_daily_shortfall
                        | incomplete_etf_daily_full
                        | set(incomplete_stock_daily)
                        | set(incomplete_etf_daily))
    # 股票日线根为空时的兜底种子窗口；残缺 ETF 日（内容校验）必须保留，
    # 否则会被空窗分支整体覆盖而漏补。
    if _missing_daily_days(STOCK_DAILY_ROOT) == [] and not stocks_daily:
        seed = set(_trade_days_up_to(today)) - set(_partition_dates(STOCK_DAILY_ROOT))
        daily_days = sorted(seed | incomplete_etf_daily_full
                            | set(incomplete_stock_daily) | set(incomplete_etf_daily))
    for day in daily_days:
        try:
            w = sync_daily(day)
            result["daily_days"].append(str(day))
            for k, v in w.items():
                result["daily_written"][k] = result["daily_written"].get(k, 0) + v
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: 日线回源 %s 失败: %s", day, e)
            result["errors"].append(f"daily {day}: {e}")

    # 2b. 指数日线——空时补最近窗口；并入相对基线检测（08-21 案例：555/599
    # 通过绝对阈值校验但缺 44 只中证系，须跨源补齐）
    shortfall_days = set(_index_shortfall_days()) if shortfall_ok else set()
    idx_days = sorted(set(_missing_index_daily_days()) | set(incomplete_index_daily)
                      | shortfall_days)
    if not idx_days and not index_daily_days:
        idx_days = sorted(set(_trade_days_up_to(today))
                          - set(_partition_dates(INDEX_DAILY_ROOT)))
    for day in idx_days:
        try:
            w = _repair_index_day(day, allow_cross_source=shortfall_ok)
            result["index_daily_days"].append(str(day))
            cross = w.pop("cross", 0)
            w["cross"] = cross
            for k, v in w.items():
                result["index_daily_written"][k] = result["index_daily_written"].get(k, 0) + v
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: 指数日线回源 %s 失败: %s", day, e)
            result["errors"].append(f"index_daily {day}: {e}")

    # 2c. ETF 因子表（新增）——stale 才跑
    if result["missing"]["adj_factor_etf"]["missing"] or not ADJ_FACTOR_PATH.exists():
        try:
            result["adj_factor"] = sync_adj_factor()
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: 因子表回源失败: %s", e)
            result["errors"].append(f"adj_factor: {e}")

    # 3. 股票分钟：先修复内容残缺分区（range 全量，走所有缺失日前的分支），
    #    再跑增量慢跑（每次一批，resume 跳过已覆盖，多轮自动补齐）
    incomplete_minute = incomplete_stock_minute | missing_stock_minute_days
    if incomplete_minute:
        try:
            min_days = sorted(incomplete_minute)
            n = sync_stock_minute_range(min_days)
            result["stock_minute_rows"] = result.get("stock_minute_rows", 0) + n
            result["stock_minute_days"] = [d.isoformat() for d in min_days]
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: 股票分钟残缺/缺失分区重写失败 %s: %s",
                           sorted(incomplete_minute), e)
            result["errors"].append(f"stock_minute_range {sorted(incomplete_minute)}: {e}")
    try:
        n = sync_stock_minute(limit=STOCK_MINUTE_BATCH_LIMIT)
        result["stock_minute_rows"] = result.get("stock_minute_rows", 0) + n
    except Exception as e:  # noqa: BLE001
        logger.warning("mootdx_service: 股票分钟回源失败: %s", e)
        result["errors"].append(f"stock_minute: {e}")

    # 3b. ETF 单位净值（akshare，只补最新缺失交易日；盘中未收盘 → 目标前一交易日）
    for day in missing_nav_days:
        try:
            etf_nav_service.sync_etf_nav(day)
            result["etf_nav_days"].append(str(day))
        except Exception as e:  # noqa: BLE001
            logger.warning("mootdx_service: 净值回源 %s 失败: %s", day, e)
            result["errors"].append(f"etf_nav {day}: {e}")

    # 4. 缺口告警（日志 + 钉钉）
    if any((st["missing"] or st["empty"] or st.get("segment_missing"))
           for st in result["missing"].values()):
        _notify_missing(result["missing"])

    logger.info("mootdx_service: 启动回源完成 %s", result)
    return result
