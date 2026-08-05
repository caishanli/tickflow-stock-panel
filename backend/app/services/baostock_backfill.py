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
