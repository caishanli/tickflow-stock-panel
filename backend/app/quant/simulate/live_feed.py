"""实时分钟数据馈送：经 stock data 服务网络客户端取当日真实 1m 并合并进内存帧。

盘中只写内存（``_minute_mem`` / ``_minute_cov``）。落盘已移交 stock data 服务，
本模块不再直连 mootdx 或写本地分区（``persist_real`` 保留为空操作兼容调用方）。
"""
from __future__ import annotations

import datetime
import logging

import pandas as pd

log = logging.getLogger("app.quant.simulate.live_feed")


def _load_recent_via_client(dm, codes, now):
    client = getattr(dm, "client", None)
    if client is None:
        return {}
    return client.current_snapshot(codes, as_of=now)


def refresh(dm, codes, now=None, fresh_acc=None, loader=None, enabled=False):
    """刷新 watch 集合的实时分钟帧，返回 ``(prices, bar_dt)``。

    - prices: ``{code: 截至 now 最新 bar 收盘价}``；
    - bar_dt: 全场最新 bar 时刻（``pd.Timestamp``；全部无数据时为 None）；
    - loader: 取数函数 ``(dm, codes, now) -> {code: df}``，默认走网络客户端
      ``dm.client.current_snapshot``（见 :func:`_load_recent_via_client`）；
    - fresh_acc: 可选 dict，收集本轮原始帧（``persist_real`` 现为空操作，
      仅供兼容调用方传参）；
    - enabled: 向后兼容保留，不再启用任何 mootdx 直连路径。

    单 code 失败保留内存旧帧并告警，不中断本轮。
    """
    now = pd.Timestamp(now or datetime.datetime.now())
    if loader is None:
        loader = _load_recent_via_client
    try:
        fresh_frames = loader(dm, list(dict.fromkeys(codes)), now) or {}
    except Exception as e:  # noqa: BLE001
        log.warning("[live_feed] 实时帧拉取失败，本轮无更新: %s", e)
        fresh_frames = {}
    prices, latest = {}, None
    for code in dict.fromkeys(codes):
        try:
            fresh = fresh_frames.get(code)
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
    """落盘已移交 stock data 服务，本函数保留为空操作（兼容调用方）。"""
    return
