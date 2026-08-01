#!/usr/bin/env python3
"""用 mootdx 补全缺失 ETF 日线数据到 DuckDB。

支持断点续传、错误记录、自动重试。
"""
from __future__ import annotations

import logging
import socket
import sys
import threading
import time

import polars as pl

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
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
MAX_PAGES = 20
BATCH_SIZE = 100
MAX_RETRIES = 3

DB_PATH = str(__import__("pathlib").Path(__file__).resolve().parent.parent.parent / "data" / "stock.duckdb")


def probe(ip: str, port: int) -> bool:
    try:
        with socket.create_connection((ip, port), timeout=2):
            return True
    except Exception:
        return False


def fetch_daily(c, code: str) -> pl.DataFrame:
    """拉取单只标的日线，返回 DataFrame（可能为空）。"""
    frames: list[pl.DataFrame] = []
    for page in range(MAX_PAGES):
        try:
            df = c.bars(symbol=code, frequency=9, start=page * PAGE_SIZE, offset=PAGE_SIZE)
        except Exception as e:
            raise RuntimeError(f"mootdx bars error: {e}") from e
        if df is None or df.empty:
            break
        if "vol" in df.columns and "volume" not in df.columns:
            df["volume"] = df["vol"]
        if "amount" in df.columns and "money" not in df.columns:
            df["money"] = df["amount"]
        df.index.name = "date"
        for col in ["date", "datetime"]:
            if col in df.columns:
                df = df.drop(columns=[col])
        frames.append(pl.from_pandas(df.reset_index()))
        if len(df) < PAGE_SIZE:
            break
    if not frames:
        return pl.DataFrame()
    out = pl.concat(frames, how="diagonal_relaxed")
    out = out.unique(subset=["date"], keep="last").sort("date")
    return out


class Worker(threading.Thread):
    lock = threading.Lock()

    def __init__(self, server, codes, results, errors, counter, total):
        super().__init__(daemon=True)
        self.server = server
        self.codes = codes
        self.results = results
        self.errors = errors
        self.counter = counter
        self.total = total

    def run(self):
        try:
            from mootdx.quotes import Quotes
            c = Quotes.factory(market="std", server=self.server)
        except Exception as e:
            logger.warning("连接失败 %s: %s", self.server, e)
            with Worker.lock:
                for sym in self.codes:
                    self.errors.append((sym, f"连接失败: {e}"))
                    self.counter[0] += 1
            return

        for sym in self.codes:
            code = sym.split(".")[0]
            try:
                out = fetch_daily(c, code)
                if not out.is_empty():
                    out = out.with_columns(pl.lit(sym).alias("symbol"))
                    out = out.with_columns(pl.lit(0).cast(pl.Int64).alias("quote_ts"))
                    keep = [c for c in ["symbol", "date", "open", "high", "low", "close", "volume", "amount", "quote_ts"] if c in out.columns]
                    out = out.select(keep)
                    self.results.append(out)
                else:
                    with Worker.lock:
                        self.errors.append((sym, "mootdx 返回空数据"))
            except Exception as e:
                with Worker.lock:
                    self.errors.append((sym, str(e)))
            with Worker.lock:
                self.counter[0] += 1
                if self.counter[0] % 200 == 0:
                    logger.info("进度: %d/%d", self.counter[0], self.total)


def run_batch(servers, n_workers, symbols):
    """执行一批标的的拉取，返回 (results, errors)。"""
    chunks: list[list[str]] = [[] for _ in range(n_workers)]
    for i, sym in enumerate(symbols):
        chunks[i % n_workers].append(sym)

    results: list[pl.DataFrame] = []
    errors: list[tuple[str, str]] = []
    counter = [0]
    workers = [Worker(servers[i], chunks[i], results, errors, counter, len(symbols)) for i in range(n_workers) if chunks[i]]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    return results, errors


def write_batch(results):
    """写入一批结果到 DuckDB，返回写入行数。"""
    if not results:
        return 0
    df = pl.concat(results, how="diagonal_relaxed")
    import duckdb
    con = duckdb.connect(DB_PATH)
    con.execute("INSERT OR REPLACE INTO kline_daily SELECT * FROM df")
    con.close()
    return len(df)


def main():
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
    from app.tickflow.client import get_client

    tf = get_client()
    etfs = []
    for ex in ["SZ", "SH"]:
        items = tf.exchanges.get_instruments(ex, instrument_type="etf")
        for it in items or []:
            sym = (it if isinstance(it, dict) else {}).get("symbol")
            if sym:
                etfs.append(sym)
    logger.info("TickFlow ETF: %d", len(etfs))

    import duckdb
    con = duckdb.connect(DB_PATH, read_only=True)
    existing = set(r[0] for r in con.execute("SELECT DISTINCT symbol FROM kline_daily").fetchall())
    con.close()
    missing = [s for s in etfs if s not in existing]
    logger.info("已有: %d, 待拉取: %d", len(existing), len(missing))

    if not missing:
        logger.info("无需补全")
        return

    servers = [(ip, port) for ip, port in TDX_SERVERS if probe(ip, port)]
    logger.info("可用服务器: %d", len(servers))
    if not servers:
        logger.error("无可用服务器")
        return

    n_workers = min(len(servers), 8)
    total_written = 0
    all_errors: list[tuple[str, str]] = []
    t0 = time.perf_counter()

    # 分批拉取
    for batch_start in range(0, len(missing), BATCH_SIZE):
        batch = missing[batch_start:batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (len(missing) + BATCH_SIZE - 1) // BATCH_SIZE
        logger.info("批次 %d/%d: %d 只", batch_num, total_batches, len(batch))

        results, errors = run_batch(servers, n_workers, batch)
        written = write_batch(results)
        total_written += written
        all_errors.extend(errors)
        logger.info("  写入: %d 条, 错误: %d 只", written, len(errors))

    # 重试失败的标的
    retry_symbols = list(set(sym for sym, _ in all_errors))
    for attempt in range(1, MAX_RETRIES + 1):
        if not retry_symbols:
            break
        logger.info("Retry %d/%d: %d 只失败标的", attempt, MAX_RETRIES, len(retry_symbols))
        # 刷新可用服务器
        servers = [(ip, port) for ip, port in TDX_SERVERS if probe(ip, port)]
        if not servers:
            logger.error("重试: 无可用服务器")
            break
        results, errors = run_batch(servers, n_workers, retry_symbols)
        written = write_batch(results)
        total_written += written
        # 只保留仍然失败的
        retry_symbols = list(set(sym for sym, _ in errors))
        logger.info("  重试写入: %d 条, 仍失败: %d 只", written, len(retry_symbols))

    elapsed = time.perf_counter() - t0

    # 最终统计
    con = duckdb.connect(DB_PATH, read_only=True)
    cnt = con.execute("SELECT count(*) FROM kline_daily").fetchone()[0]
    syms = con.execute("SELECT count(DISTINCT symbol) FROM kline_daily").fetchone()[0]
    con.close()

    logger.info("=" * 60)
    logger.info("完成: kline_daily %d 只, %d 条 (本次写入 %d)", syms, cnt, total_written)
    logger.info("耗时: %.1fs", elapsed)
    if retry_symbols:
        logger.warning("最终失败 %d 只: %s", len(retry_symbols), retry_symbols[:20])
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
