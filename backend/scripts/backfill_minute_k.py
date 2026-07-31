#!/usr/bin/env python3
"""历史分钟K补全脚本 (mootdx, 近3个月)。

支持断点续传: 跳过 DuckDB 中已有分钟数据的标的。
每批 worker 完成后立即写入, 中断不丢失。

用法:
    cd backend
    python -m scripts.backfill_minute_k [--workers N] [--asset stock|etf|all] [--force]

mootdx 单次最多800根, 按start分页回看, 约3个月历史。
脚本跑完即退, 不会无限循环。
"""
from __future__ import annotations

import argparse
import logging
import socket
import sys
import threading
import time

import polars as pl

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

TDX_SERVERS = [
    ("115.238.90.165", 7709), ("115.238.56.198", 7709),
    ("218.75.126.9", 7709), ("124.160.88.183", 7709),
    ("60.191.117.167", 7709), ("60.12.136.250", 7709),
    ("119.97.185.59", 7709), ("124.70.133.119", 7709),
    ("116.205.183.150", 7709), ("123.60.73.44", 7709),
    ("116.205.163.254", 7709), ("121.36.225.169", 7709),
    ("123.60.70.228", 7709), ("124.71.9.153", 7709),
    ("110.41.147.114", 7709), ("124.71.187.122", 7709),
]

PAGE_SIZE = 800
MAX_PAGES = 20  # 20 * 800 = 16000 bars ~ 约3个月


def _probe(ip: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except Exception:
        return False


def discover_servers(max_workers: int) -> list[tuple[str, int]]:
    reachable: list[tuple[str, int]] = []
    for ip, port in TDX_SERVERS:
        if len(reachable) >= max_workers:
            break
        if _probe(ip, port):
            reachable.append((ip, port))
    logger.info("发现 %d 个可达TDX服务器", len(reachable))
    return reachable


def fetch_one_symbol(client, code: str) -> pl.DataFrame | None:
    """单只标的分页拉取全部可用分钟K。"""
    frames: list[pl.DataFrame] = []
    for page in range(MAX_PAGES):
        try:
            df = client.bars(symbol=code, frequency=8, start=page * PAGE_SIZE, offset=PAGE_SIZE)
        except Exception:
            break
        if df is None or df.empty:
            break
        if "vol" in df.columns and "volume" not in df.columns:
            df["volume"] = df["vol"]
        if "amount" in df.columns and "money" not in df.columns:
            df["money"] = df["amount"]
        df.index.name = "datetime"
        if "datetime" in df.columns:
            df = df.drop(columns=["datetime"])
        pdf = df.reset_index()
        frames.append(pl.from_pandas(pdf))
        if len(df) < PAGE_SIZE:
            break
    if not frames:
        return None
    out = pl.concat(frames, how="diagonal_relaxed")
    out = out.unique(subset=["datetime"], keep="last").sort("datetime")
    return out


class BackfillWorker(threading.Thread):
    def __init__(
        self,
        server: tuple[str, int],
        symbols: list[str],
        results: list[pl.DataFrame],
        errors: list[tuple[str, str]],
        skipped: list[str],
        counter: list[int],
        total: int,
    ):
        super().__init__(daemon=True)
        self._server = server
        self._symbols = symbols
        self._results = results
        self._errors = errors
        self._skipped = skipped
        self._counter = counter
        self._total = total

    def run(self) -> None:
        try:
            from mootdx.quotes import Quotes

            c = Quotes.factory(market="std", server=self._server)
        except Exception as e:
            logger.warning("Worker 连接失败 %s: %s", self._server, e)
            return

        for sym in self._symbols:
            code = sym.split(".")[0]
            fetched = False
            for attempt in range(3):
                try:
                    df = fetch_one_symbol(c, code)
                    if df is not None and not df.is_empty():
                        df = df.with_columns(pl.lit(sym).alias("symbol"))
                        keep = [c for c in ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"] if c in df.columns]
                        df = df.select(keep)
                        self._results.append(df)
                        fetched = True
                        break
                except Exception:
                    pass
                if attempt < 2:
                    time.sleep(0.5)
            if not fetched:
                self._skipped.append(sym)
            with threading.Lock():
                self._counter[0] += 1
                if self._counter[0] % 100 == 0:
                    logger.info("进度: %d/%d", self._counter[0], self._total)


def _fetch_symbols_from_tickflow(asset_type: str) -> list[str]:
    """从 TickFlow API 获取标的列表 (instruments 表为空时的 fallback)。"""
    try:
        from app.tickflow.client import get_client
        tf = get_client()
        if tf is None:
            return []
        exchanges = ["SZ", "SH"]
        symbols: list[str] = []
        for ex in exchanges:
            try:
                items = tf.exchanges.get_instruments(ex, instrument_type=asset_type)
                for it in items or []:
                    sym = (it if isinstance(it, dict) else {}).get("symbol")
                    if sym:
                        symbols.append(str(sym))
            except Exception:
                continue
        logger.info("TickFlow API 获取 %s 标的: %d 只", asset_type, len(symbols))
        return symbols
    except Exception:
        return []


def _get_existing_symbols(repo) -> set[str]:
    """查询 kline_minute 中已有数据的标的。"""
    try:
        df = repo.db.execute("SELECT DISTINCT symbol FROM kline_minute").pl()
        if df.is_empty() or "symbol" not in df.columns:
            return set()
        return set(df["symbol"].to_list())
    except Exception:
        return set()


def _write_batch(db, results: list[pl.DataFrame]) -> int:
    """将一批结果写入 DuckDB, 返回写入行数。用 DuckDB 原生 INSERT OR REPLACE。"""
    if not results:
        return 0
    df = pl.concat(results, how="diagonal_relaxed")
    if "volume" in df.columns:
        df = df.with_columns(pl.col("volume").cast(pl.Float64))
    if "amount" in df.columns:
        df = df.with_columns(pl.col("amount").cast(pl.Float64))
    if df.is_empty():
        return 0
    # DuckDB 原生批量写入, 远快于 executemany
    db.execute("INSERT OR REPLACE INTO kline_minute SELECT * FROM df")
    return df.height


BATCH_SIZE = 50  # 每处理50只写入一次


def main() -> None:
    parser = argparse.ArgumentParser(description="历史分钟K补全 (mootdx)")
    parser.add_argument("--workers", type=int, default=8, help="并发线程数 (默认8)")
    parser.add_argument("--asset", choices=["stock", "etf", "all"], default="all", help="补全范围")
    parser.add_argument("--force", action="store_true", help="忽略已有数据, 强制全量补全")
    args = parser.parse_args()

    sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))

    from app.tickflow.repository import DataStore, KlineRepository

    store = DataStore()
    repo = KlineRepository(store)

    symbols: list[str] = []
    if args.asset in ("etf", "all"):
        etf = repo.get_etf_instruments()
        if not etf.is_empty() and "symbol" in etf.columns:
            symbols.extend(etf["symbol"].cast(pl.Utf8).to_list())
        else:
            symbols.extend(_fetch_symbols_from_tickflow("etf"))
    if args.asset in ("stock", "all"):
        inst = repo.get_instruments()
        if not inst.is_empty() and "symbol" in inst.columns:
            symbols.extend(inst["symbol"].cast(pl.Utf8).to_list())
        else:
            symbols.extend(_fetch_symbols_from_tickflow("stock"))
    symbols = sorted(set(symbols))
    if not symbols:
        logger.error("无标的可补全")
        store.db.close()
        return

    # 断点续传: 跳过已有数据的标的
    if not args.force:
        existing = _get_existing_symbols(repo)
        before = len(symbols)
        symbols = [s for s in symbols if s not in existing]
        logger.info("已有 %d 只标的分钟数据, 跳过, 剩余 %d 只待补全", before - len(symbols), len(symbols))
    else:
        logger.info("--force: 全量补全 %d 只", len(symbols))

    if not symbols:
        logger.info("所有标的已有分钟数据, 无需补全")
        store.db.close()
        return

    servers = discover_servers(max_workers=args.workers)
    if not servers:
        logger.error("无可用TDX服务器")
        store.db.close()
        return

    n_workers = min(len(servers), args.workers)

    # 分批处理: 每批 BATCH_SIZE 只, 完成后立即写入
    total_written = 0
    total_errors: list[tuple[str, str]] = []
    all_skipped: list[str] = []
    t0 = time.perf_counter()

    for batch_start in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[batch_start:batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (len(symbols) + BATCH_SIZE - 1) // BATCH_SIZE
        logger.info("批次 %d/%d: %d 只", batch_num, total_batches, len(batch))

        chunks: list[list[str]] = [[] for _ in range(n_workers)]
        for i, sym in enumerate(batch):
            chunks[i % n_workers].append(sym)

        results: list[pl.DataFrame] = []
        errors: list[tuple[str, str]] = []
        skipped: list[str] = []
        counter = [0]
        workers = []

        for i in range(n_workers):
            w = BackfillWorker(servers[i], chunks[i], results, errors, skipped, counter, len(batch))
            workers.append(w)
            w.start()

        for w in workers:
            w.join()

        # 立即写入, 不等全部完成
        written = _write_batch(store.db, results)
        total_written += written
        total_errors.extend(errors)
        logger.info("批次 %d/%d 写入: %d 条 (累计 %d)", batch_num, total_batches, written, total_written)

        # 收集本批次跳过的标的, 用于最终 retry
        if skipped:
            all_skipped.extend(skipped)
            skipped.clear()

    elapsed = time.perf_counter() - t0
    logger.info("全部完成: %d 条写入, %d 错误, %d 跳过, %.1fs", total_written, len(total_errors), len(all_skipped), elapsed)

    # Retry: 对跳过的标的再跑一轮(换服务器)
    if all_skipped:
        logger.info("Retry: 重新尝试 %d 只跳过的标的", len(all_skipped))
        retry_results: list[pl.DataFrame] = []
        retry_skipped: list[str] = []
        retry_errors: list[tuple[str, str]] = []
        retry_counter = [0]
        retry_workers = []
        # 均匀分给所有 worker
        retry_chunks: list[list[str]] = [[] for _ in range(n_workers)]
        for i, sym in enumerate(all_skipped):
            retry_chunks[i % n_workers].append(sym)
        for i in range(n_workers):
            if retry_chunks[i]:
                w = BackfillWorker(servers[i], retry_chunks[i], retry_results, retry_errors, retry_skipped, retry_counter, len(all_skipped))
                retry_workers.append(w)
                w.start()
        for w in retry_workers:
            w.join()
        if retry_results:
            written = _write_batch(store.db, retry_results)
            logger.info("Retry 写入: %d 条, 仍跳过 %d 只", written, len(retry_skipped))
        else:
            logger.info("Retry: 无新数据, 仍跳过 %d 只", len(retry_skipped))

    if total_errors:
        logger.warning("失败标的 (%d): %s", len(total_errors), total_errors[:10])

    store.db.close()


if __name__ == "__main__":
    main()
