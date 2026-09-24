"""聚宽 ETF 名称加载（模拟盘策略侧用，与回测同源快照）。"""
from __future__ import annotations

import datetime as _dt
import json
import os

from ...config import CONFIG as _JQ_CONFIG

# 与 rqalpha_bridge._ETF_UNIVERSE_SNAPSHOT 同文件
SNAPSHOT_PATH = os.path.join(
    _JQ_CONFIG["DATA_DIR"], "etf_universe_snapshot.json")

# 与 rqalpha_bridge._ETF_SNAPSHOT_MAX_AGE 同口径：快照超过该期限视为过期
MAX_AGE = _dt.timedelta(days=30)

_CACHE: dict[str, str] | None = None


def _snapshot_paths() -> list[str]:
    """快照候选路径：SNAPSHOT_PATH 优先，其次 DATA_DIR 直下与 quant_kline 子目录。

    双路径兼容两种布局（2026-09-16 501018 无名事故）：默认口径嵌套
    quant_kline，.env（./data，经仓库根锚定）与容器（/app/data）口径扁平，
    文件实际落在嵌套侧，扁平侧直读永远命中不了。
    """
    paths = [SNAPSHOT_PATH]
    base = _JQ_CONFIG["DATA_DIR"]
    for cand in (os.path.join(base, "etf_universe_snapshot.json"),
                 os.path.join(base, "quant_kline", "etf_universe_snapshot.json")):
        if cand not in paths:
            paths.append(cand)
    return paths


def load_jq_names() -> dict[str, str]:
    """返回 {JQ码: 聚宽 display_name}，进程内缓存；文件缺失/损坏返回空。

    快照超过 30 天仍降级使用（名称是纯展示元数据，旧名优于落代码兜底；
    2026-08-30 案例：过期返回空 → LOF 名称在 mootdx/ETF 名单兜底里都
    不存在，退化为代码）。过期打一次 WARNING 提示跑量化回测刷新。
    """
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    out: dict[str, str] = {}
    for _path in _snapshot_paths():
        try:
            with open(_path, encoding="utf-8") as f:
                snap = json.load(f)
            if not isinstance(snap, dict):
                continue
            fetched_at = _dt.datetime.fromisoformat(str(snap.get("fetched_at")))
            age = _dt.datetime.now() - fetched_at
            out = {str(k): str(v) for k, v in (snap.get("names") or {}).items()}
            if age > MAX_AGE:
                import logging
                logging.getLogger("app.quant.jqengine.jq_names").warning(
                    "ETF 名称快照已过期 %d 天（fetched_at=%s），降级沿用旧名；"
                    "跑一次量化回测即可刷新", age.days, fetched_at.date())
            break
        except Exception:
            continue
    _CACHE = out
    return out
