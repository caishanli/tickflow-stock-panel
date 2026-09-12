"""实时取价——当前 bar 成交基准价的唯一解析顺序。

顺序（jq / ptrade 两 API 面相同，差异仅码制转换钩子）：
1. ``snapshot``（回测桥/runner 每 bar 写入的分钟价快照，调用方域代码）；
2. 分钟模式下 ``manager.get_minute_price_at`` 精确取点（拆股 as-of 撤销在内）；
3. 分钟模式无行情返回 0；仅日线模式保留持仓价回退。
"""
from __future__ import annotations


def resolve_live_price(snapshot, code, *, minute_mode=False, manager=None,
                       current_dt=None, fallback_price: float = 0.0,
                       to_engine=None):
    """取当前 bar 的实时价。``to_engine`` 为 PTrade 域→JQ 码转换钩子（jq 侧传 None）。"""
    snap = snapshot or {}
    # 快照价 0/缺失均视为无行情（与旧两引擎 `in snap and snap[code]` 同语义），往下走
    price = snap.get(code)
    if price:
        return price
    if minute_mode and manager is not None and current_dt is not None:
        q = to_engine(code) if to_engine is not None else code
        # 故意不吞异常：get_minute_price_at 缺数返回 None（不抛），真抛出来说明
        # 上游坏了，必须 loud（旧两引擎此处同样不捕获；契约 §7.3 禁止静默回退）。
        p = manager.get_minute_price_at(q, current_dt)
        if p is not None:
            return p
    # 持仓价是估值标记，不能证明当前可成交。连续停牌第二日起可能连占位
    # bar 都没有，此时量能停牌守卫无法确认停牌，也必须按无行情拒绝委托。
    if minute_mode:
        return 0
    return fallback_price or 0
