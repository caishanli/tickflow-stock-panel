"""聚宽 ETF 名称加载（模拟盘策略侧用，与回测同源快照）。"""
from __future__ import annotations

import json
import os

from ...config import CONFIG as _JQ_CONFIG

# 与 rqalpha_bridge._ETF_UNIVERSE_SNAPSHOT 同文件
SNAPSHOT_PATH = os.path.join(
    _JQ_CONFIG["DATA_DIR"], "etf_universe_snapshot.json")

_CACHE: dict[str, str] | None = None


def load_jq_names() -> dict[str, str]:
    """返回 {JQ码: 聚宽 display_name}，进程内缓存；失败返回空。"""
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    out: dict[str, str] = {}
    try:
        with open(SNAPSHOT_PATH, encoding="utf-8") as f:
            snap = json.load(f)
        out = {str(k): str(v) for k, v in (snap.get("names") or {}).items()}
    except Exception:
        pass
    _CACHE = out
    return out
