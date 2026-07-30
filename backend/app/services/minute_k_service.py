"""分钟K实时行情服务。

盘中每30秒通过mootdx多线程获取全市场ETF分钟K写入DuckDB,
非交易时段获取全市场股票分钟K。
"""
from __future__ import annotations

import logging
import socket
import threading
import time

import polars as pl

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


def _probe(ip: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except Exception:
        return False


def _discover_servers(max_workers: int = 16) -> list[tuple[str, int]]:
    reachable: list[tuple[str, int]] = []
    for ip, port in TDX_SERVERS:
        if len(reachable) >= max_workers:
            break
        if _probe(ip, port):
            reachable.append((ip, port))
    if not reachable:
        logger.warning("mootdx: 所有TDX服务器均不可达")
    else:
        logger.info("mootdx: 发现 %d 个可达服务器", len(reachable))
    return reachable


class MinuteKWorker(threading.Thread):
    def __init__(
        self,
        server: tuple[str, int],
        symbols: list[str],
        results: list[pl.DataFrame],
        errors: list[tuple[str, str]],
    ):
        super().__init__(daemon=True)
        self._server = server
        self._symbols = symbols
        self._results = results
        self._errors = errors

    def run(self) -> None:
        try:
            import pytdx.hq as _pytdx_hq
            from mootdx.quotes import Quotes

            c = Quotes.factory(market="std", server=self._server)
            px = _pytdx_hq.TdxHq_API()
            px.connect(self._server[0], self._server[1], time_out=10)
            c.client = px
        except Exception as e:
            logger.warning("mootdx Worker 连接失败 %s: %s", self._server, e)
            return

        for sym in self._symbols:
            try:
                code = sym.split(".")[0]
                df = c.bars(symbol=code, frequency=8, start=0, offset=3)
                if df is not None and not df.empty:
                    if "vol" in df.columns and "volume" not in df.columns:
                        df["volume"] = df["vol"]
                    if "amount" in df.columns and "money" not in df.columns:
                        df["money"] = df["amount"]
                    df.index.name = "datetime"
                    if "datetime" in df.columns:
                        df = df.drop(columns=["datetime"])
                    pdf = df.reset_index()
                    pdf["symbol"] = sym
                    self._results.append(
                        pl.from_pandas(pdf[["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]])
                    )
            except Exception as e:
                self._errors.append((sym, str(e)))


class MinuteKService:
    def __init__(self, repo, interval: float = 30.0):
        from app.tickflow.repository import KlineRepository
        self._repo: KlineRepository = repo
        self._interval = interval
        self._running = False
        self._enabled = False
        self._paused = False
        self._thread: threading.Thread | None = None
        self._fetch_lock = threading.Lock()
        self._servers: list[tuple[str, int]] = []
        self._worker_count = 8
        self._app_state = None

    def set_repo(self, repo) -> None:
        self._repo = repo

    def set_app_state(self, app_state) -> None:
        self._app_state = app_state

    def boot_check(self) -> None:
        from app.services import preferences
        if preferences.get_minute_k_enabled():
            self.start()

    def start(self) -> None:
        if self._running:
            return
        from app.services import preferences
        self._interval = preferences.get_minute_k_interval()
        self._worker_count = preferences.get_minute_k_worker_count()
        self._servers = _discover_servers(max_workers=self._worker_count)
        if not self._servers:
            logger.warning("分钟K服务启动失败: 无可用TDX服务器")
            return
        self._running = True
        self._enabled = True
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()
        logger.info("分钟K服务已启动, 间隔 %.1fs, %d 个Worker", self._interval, len(self._servers))

    def stop(self) -> None:
        self._running = False
        self._enabled = False
        if self._thread:
            self._thread.join(timeout=10)
            self._thread = None
        logger.info("分钟K服务已停止")

    def pause(self) -> None:
        self._paused = True

    def resume(self) -> None:
        self._paused = False

    def status(self) -> dict:
        from app.services.market_phase import market_phase
        return {
            "enabled": self._enabled,
            "running": self._running,
            "paused": self._paused,
            "interval_s": self._interval,
            "worker_count": len(self._servers),
            "market_phase": market_phase(),
        }

    def _poll_loop(self) -> None:
        from app.services.market_phase import market_phase
        while self._running and self._enabled:
            try:
                if not self._paused:
                    phase = market_phase()
                    if phase in ("morning", "afternoon"):
                        self._fetch_etf_minute_k()
                    elif phase in ("closed", "close_final"):
                        self._fetch_stock_minute_k()
            except Exception as e:
                logger.warning("分钟K轮询异常: %s", e)

            waited = 0.0
            while self._running and self._enabled and waited < self._interval:
                time.sleep(0.5)
                waited += 0.5

    def _fetch_symbols(self, symbols: list[str], label: str) -> None:
        if not symbols or not self._servers:
            return
        with self._fetch_lock:
            t0 = time.perf_counter()
            n_workers = min(len(self._servers), self._worker_count)
            chunks: list[list[str]] = [[] for _ in range(n_workers)]
            for i, sym in enumerate(symbols):
                chunks[i % n_workers].append(sym)

            results: list[pl.DataFrame] = []
            errors: list[tuple[str, str]] = []
            workers = []
            for i in range(n_workers):
                w = MinuteKWorker(self._servers[i], chunks[i], results, errors)
                workers.append(w)
                w.start()

            for w in workers:
                w.join(timeout=25)

            alive = [w for w in workers if w.is_alive()]
            if alive:
                logger.warning("分钟K %s: %d 个Worker超时", label, len(alive))

            elapsed = time.perf_counter() - t0
            if results:
                df = pl.concat(results, how="diagonal_relaxed")
                self._write_to_duckdb(df)
                logger.info("分钟K %s: %d 只, %d 条, %.1fs", label, len(symbols), df.height, elapsed)
            else:
                logger.warning("分钟K %s: 无数据 (%.1fs, %d 错误)", label, elapsed, len(errors))

    def _fetch_etf_minute_k(self) -> None:
        etf_inst = self._repo.get_etf_instruments()
        if etf_inst.is_empty() or "symbol" not in etf_inst.columns:
            return
        symbols = sorted(set(etf_inst["symbol"].cast(pl.Utf8).to_list()))
        self._fetch_symbols(symbols, "ETF")

    def _fetch_stock_minute_k(self) -> None:
        inst = self._repo.get_instruments()
        if inst.is_empty() or "symbol" not in inst.columns:
            return
        symbols = sorted(set(inst["symbol"].cast(pl.Utf8).to_list()))
        self._fetch_symbols(symbols, "全市场股票")

    def _write_to_duckdb(self, df: pl.DataFrame) -> None:
        if df.is_empty():
            return
        keep = [c for c in ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"] if c in df.columns]
        df = df.select(keep)
        if "volume" in df.columns:
            df = df.with_columns(pl.col("volume").cast(pl.Float64))
        if "amount" in df.columns:
            df = df.with_columns(pl.col("amount").cast(pl.Float64))
        self._repo._upsert_daily(df, "kline_minute")
