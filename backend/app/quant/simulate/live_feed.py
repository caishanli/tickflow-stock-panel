"""实时分钟数据馈送：mootdx 实时 bar 增量合并进 DataManager 内存帧。

盘中只写内存（``_minute_mem`` / ``_minute_cov``），当日真实 1m 于收盘后统一
落盘 ``minute.db``——mootdx 取得的是真实 1m，按 C1 允许且应当落盘，但避免
每分钟全帧重写；内存帧可能含 baostock 插值段，绝不整帧落盘（C2），落盘只
用本模块累积的 mootdx 原始帧。
"""
from __future__ import annotations

import datetime
import logging

import pandas as pd

log = logging.getLogger("app.quant.simulate.live_feed")


def _fetch_recent(dm, code):
    src = dm.sources.get("mootdx")
    if src is None:
        raise RuntimeError("mootdx 源不可用")
    return src.get_minute_recent(code)


def refresh(dm, codes, now=None, fresh_acc=None):
    """刷新 watch 集合的实时分钟帧，返回 ``(prices, bar_dt)``。

    - prices: ``{code: 截至 now 最新 bar 收盘价}``；
    - bar_dt: 全场最新 bar 时刻（``pd.Timestamp``；全部无数据时为 None）；
    - fresh_acc: 可选 dict，收集本轮 mootdx 原始帧（供收盘后 :func:`persist_real`
      落盘；每轮覆盖同 code 旧帧，最新一页已含当日全部 bar）。

    单 code 失败保留内存旧帧并告警，不中断本轮。
    """
    now = pd.Timestamp(now or datetime.datetime.now())
    prices, latest = {}, None
    for code in dict.fromkeys(codes):
        try:
            fresh = _fetch_recent(dm, code)
            if fresh is None or fresh.empty:
                raise RuntimeError("实时分钟为空")
            old = dm._minute_mem.get(code)
            if old is not None and not old.empty:
                merged = pd.concat([old, fresh]).sort_index()
                merged = merged[~merged.index.duplicated(keep="last")]
            else:
                merged = fresh
            dm._minute_mem[code] = merged
            dm._minute_cov[code] = (merged.index.min(), merged.index.max())
            if fresh_acc is not None:
                fresh_acc[code] = fresh
        except Exception as e:  # noqa: BLE001
            log.warning("[live_feed] %s 实时分钟刷新失败，沿用旧帧: %s", code, e)
            merged = dm._minute_mem.get(code)
        if merged is None or (hasattr(merged, "empty") and merged.empty):
            continue
        sub = merged[merged.index <= now]
        if sub.empty:
            continue
        prices[code] = float(sub["close"].iloc[-1])
        bar = sub.index[-1]
        if latest is None or bar > latest:
            latest = bar
    return prices, latest


def persist_real(dm, fresh_frames):
    """收盘后落盘：把当日 mootdx 真实 1m 合并进 ``minute.db`` 的 ``real_<code>``（C1）。

    只写 mootdx 原始帧；内存合并帧可能含 baostock 插值段，绝不整帧落盘（C2）。
    与本地已有 real_ 段按索引去重（keep=last）后整键重写。
    """
    for code, fresh in (fresh_frames or {}).items():
        if fresh is None or fresh.empty:
            continue
        key = f"real_{code}"
        try:
            local = dm.cache.peek("minute", key)
            if local is not None and not local.empty:
                combined = pd.concat([local, fresh]).sort_index()
                combined = combined[~combined.index.duplicated(keep="last")]
            else:
                combined = fresh
            dm.cache.put("minute", key, combined)
        except Exception as e:  # noqa: BLE001
            log.warning("[live_feed] %s 真实分钟落盘失败: %s", code, e)
