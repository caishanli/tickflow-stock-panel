#!/usr/bin/env python3
"""历史分钟K补全脚本 (mootdx, 近3个月)。

用法:
    cd backend
    python -m scripts.backfill_minute_k [--workers N] [--asset stock|etf|all]

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
        counter: list[int],
        total: int,
    ):
        super().__init__(daemon=True)
        self._server = server
        self._symbols = symbols
        self._results = results
        self._errors = errors
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
            try:
                df = fetch_one_symbol(c, code)
                if df is not None and not df.is_empty():
                    df = df.with_columns(pl.lit(sym).alias("symbol"))
                    keep = [c for c in ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"] if c in df.columns]
                    df = df.select(keep)
                    self._results.append(df)
            except Exception as e:
                self._errors.append((sym, str(e)))
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


def main() -> None:
    parser = argparse.ArgumentParser(description="历史分钟K补全 (mootdx)")
    parser.add_argument("--workers", type=int, default=8, help="并发线程数 (默认8)")
    parser.add_argument("--asset", choices=["stock", "etf", "all"], default="all", help="补全范围")
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

    logger.info("待补全标的: %d 只 (%s)", len(symbols), args.asset)

    servers = discover_servers(max_workers=args.workers)
    if not servers:
        logger.error("无可用TDX服务器")
        store.db.close()
        return

    n_workers = min(len(servers), args.workers)
    chunks: list[list[str]] = [[] for _ in range(n_workers)]
    for i, sym in enumerate(symbols):
        chunks[i % n_workers].append(sym)

    results: list[pl.DataFrame] = []
    errors: list[tuple[str, str]] = []
    counter = [0]
    workers = []
    t0 = time.perf_counter()

    for i in range(n_workers):
        w = BackfillWorker(servers[i], chunks[i], results, errors, counter, len(symbols))
        workers.append(w)
        w.start()

    for w in workers:
        w.join()

    elapsed = time.perf_counter() - t0
    logger.info("拉取完成: %d 条, %d 错误, %.1fs", sum(r.height for r in results), len(errors), elapsed)

    if results:
        df = pl.concat(results, how="diagonal_relaxed")
        if "volume" in df.columns:
            df = df.with_columns(pl.col("volume").cast(pl.Float64))
        if "amount" in df.columns:
            df = df.with_columns(pl.col("amount").cast(pl.Float64))
        repo._upsert_daily(df, "kline_minute")
        logger.info("写入DuckDB: %d 条", df.height)

    store.db.close()

    if errors:
        logger.warning("失败标的 (%d): %s", len(errors), errors[:10])


if __name__ == "__main__":
    main()
