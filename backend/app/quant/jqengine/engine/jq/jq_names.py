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


def load_jq_names() -> dict[str, str]:
    """返回 {JQ码: 聚宽 display_name}，进程内缓存；失败返回空。

    快照超过 30 天视为过期：返回空（回退 tdx 名），避免旧名被永久钉住。
    """
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    out: dict[str, str] = {}
    try:
        with open(SNAPSHOT_PATH, encoding="utf-8") as f:
            snap = json.load(f)
        fetched_at = _dt.datetime.fromisoformat(str(snap.get("fetched_at")))
        if _dt.datetime.now() - fetched_at <= MAX_AGE:
            out = {str(k): str(v) for k, v in (snap.get("names") or {}).items()}
    except Exception:
        pass
    _CACHE = out
    return out
