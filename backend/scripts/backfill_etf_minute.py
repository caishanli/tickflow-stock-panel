"""回填 ETF 分钟线：从 mootdx 拉取 2026-04-01 ~ 2026-04-22 的数据写入 DuckDB。

用法: cd backend && uv run python scripts/backfill_etf_minute.py
"""
from __future__ import annotations

import os
import sys
import time
import logging
import threading
from datetime import date

import duckdb
import pandas as pd
import pytdx.hq

DUCKDB_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "stock.duckdb")
START_DATE = "2026-04-01"
END_DATE = "2026-04-22"  # inclusive; existing data starts 2026-04-23
TARGET_START = pd.Timestamp(START_DATE)
TARGET_END = pd.Timestamp(END_DATE)

TDX_SERVERS = [
    ('115.238.90.165', 7709), ('115.238.56.198', 7709),
    ('218.75.126.9', 7709), ('124.160.88.183', 7709),
    ('60.191.117.167', 7709), ('60.12.136.250', 7709),
    ('119.97.185.59', 7709), ('124.70.133.119', 7709),
    ('116.205.183.150', 7709),
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill_etf_minute")


def get_etf_symbols(conn):
    rows = conn.execute("SELECT symbol FROM instruments_etf").fetchall()
    return sorted(r[0] for r in rows)


def get_existing_first_dates(conn):
    rows = conn.execute(
        "SELECT symbol, MIN(datetime::DATE) FROM kline_etf_minute GROUP BY symbol"
    ).fetchall()
    return {r[0]: r[1] for r in rows}


class FetchWorker(threading.Thread):
    def __init__(self, server, symbols, results, errors):
        super().__init__(daemon=True)
        self._server = server
        self._symbols = symbols
        self._results = results
        self._errors = errors

    def run(self):
        try:
            px = pytdx.hq.TdxHq_API()
            px.connect(self._server[0], self._server[1], time_out=10)

            from mootdx.quotes import Quotes
            c = Quotes.factory(market="std")
            c.client = px
        except Exception as e:
            log.warning("Worker 连接失败 %s: %s", self._server, e)
            return

        for sym in self._symbols:
            try:
                code = sym.split(".")[0]
                frames = []
                # Page backward from most recent until we pass TARGET_START
                for start in range(0, 60000, 800):
                    df = c.bars(symbol=code, frequency=8, start=start, offset=800)
                    if df is None or df.empty:
                        break
                    # Normalize columns
                    if "vol" in df.columns and "volume" not in df.columns:
                        df["volume"] = df["vol"]
                    if "amount" in df.columns and "money" not in df.columns:
                        df["money"] = df["amount"]
                    df.index.name = "datetime"
                    if "datetime" in df.columns:
                        df = df.drop(columns=["datetime"])
                    first_dt = df.index[0]
                    frames.append(df)
                    # Stop once we've gone past our target start date
                    if first_dt < TARGET_START:
                        break
                    if len(df) < 800:
                        break

                if not frames:
                    continue

                all_df = pd.concat(frames)
                all_df = all_df[~all_df.index.duplicated(keep="last")].sort_index()
                # Filter to target range
                mask = (all_df.index >= TARGET_START) & (all_df.index <= TARGET_END + pd.Timedelta(days=1))
                all_df = all_df[mask]
                if all_df.empty:
                    continue

                pdf = all_df.reset_index()
                pdf["symbol"] = sym
                pdf["datetime"] = pd.to_datetime(pdf["datetime"])
                import polars as pl
                keep_cols = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
                for c_name in ["amount", "money"]:
                    if c_name in pdf.columns and c_name not in keep_cols:
                        pass
                # rename money -> amount if needed
                if "money" in pdf.columns and "amount" not in pdf.columns:
                    pdf = pdf.rename(columns={"money": "amount"})
                pdf_out = pdf[[c for c in keep_cols if c in pdf.columns]]
                self._results.append(pl.from_pandas(pdf_out))

            except Exception as e:
                self._errors.append((sym, str(e)))
        px.disconnect()


def main():
    db_path = os.path.abspath(DUCKDB_PATH)
    log.info("DuckDB: %s", db_path)
    conn = duckdb.connect(db_path, read_only=True)

    symbols = get_etf_symbols(conn)
    existing_first = get_existing_first_dates(conn)
    conn.close()

    # Only need to backfill symbols whose first minute date is >= START_DATE
    need = [s for s in symbols if existing_first.get(s, date(2099, 1, 1)) >= TARGET_START.date()]
    log.info("ETF 总数: %d, 需回填: %d", len(symbols), len(need))

    if not need:
        log.info("无需回填")
        return

    # Split across servers
    n_workers = min(len(TDX_SERVERS), 8)
    chunks = [[] for _ in range(n_workers)]
    for i, sym in enumerate(need):
        chunks[i % n_workers].append(sym)

    results = []
    errors = []
    workers = []
    for i in range(n_workers):
        if not chunks[i]:
            continue
        w = FetchWorker(TDX_SERVERS[i], chunks[i], results, errors)
        workers.append(w)
        w.start()

    log.info("启动 %d 个 worker，开始拉取...", len(workers))
    for w in workers:
        w.join()
    log.info("拉取完成: 成功 %d 只, 错误 %d 只", len(results), len(errors))

    if errors:
        for sym, err in errors[:10]:
            log.warning("  错误 %s: %s", sym, err)

    if not results:
        log.info("无新数据写入")
        return

    import polars as pl
    combined = pl.concat(results, how="diagonal_relaxed")
    log.info("合并数据: %d 行, %d 只", combined.height, combined["symbol"].n_unique())

    # Write to DuckDB
    conn = duckdb.connect(db_path)
    try:
        conn.execute("""
            INSERT OR REPLACE INTO kline_etf_minute
            SELECT * FROM combined
        """)
        log.info("写入 kline_etf_minute 完成: +%d 行", combined.height)
    except Exception as e:
        # Fallback: create temp table and merge
        log.warning("INSERT OR REPLACE 失败: %s, 尝试逐批写入", e)
        written = 0
        for sym, grp in combined.group_by("symbol"):
            try:
                conn.execute("INSERT OR REPLACE INTO kline_etf_minute SELECT * FROM grp")
                written += grp.height
            except Exception as e2:
                log.warning("写入 %s 失败: %s", sym, e2)
        log.info("逐批写入完成: +%d 行", written)
    conn.close()


if __name__ == "__main__":
    main()
