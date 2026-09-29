"""legacy 止损取整口径（对齐服务器旧实现，opt-in，仅 A/B 对照）。

服务器 `93e6e796` 跑在 `05d2a7d` 之前。实测两机成交价形态：

- 正常委托（``execute_order``）：两机一致且都 tick 对齐
  （07-10 BUY 2.132 / 07-13 SELL 2.193）——旧代码本来就有取整。
- 止损（``Matcher.step``）：服务器 2.1429 保留 4 位小数不取整，本机 2.143
  对齐 tick。``05d2a7d`` 正是"Matcher 止损成交价同步按 tick 取整"那一条。

所以 legacy 必须**只作用于止损路径**；若做成全局开关，连正常委托都变 4 位
（2.1322），反而与服务器不符（实测首笔即分叉）。

该口径已被 ``05d2a7d`` 判定为缺陷（回测 vs 补跑逐笔错位，42 笔 → 0），
且 2.1429 非 ETF 报价档位价，仅用于复现历史数字做 A/B，不用于实盘。
"""
from __future__ import annotations

import pytest

ETF = "159985.XSHE"
STOCK = "600000.XSHG"


def _reload(monkeypatch, legacy: bool):
    for k in ("QUANT_FILL_ROUNDING", "QUANT_LEGACY_MATCHER_ROUNDING"):
        monkeypatch.delenv(k, raising=False)
    if legacy:
        monkeypatch.setenv("QUANT_LEGACY_MATCHER_ROUNDING", "1")
    import importlib

    from app.quant.simulate import matcher
    return importlib.reload(matcher)


def _fresh_state():
    """每次调用都返回全新 state——Matcher.step 会就地改 positions/stop_loss_log，
    浅拷贝会让第二次断言读到第一次的残留。"""
    return {"cash": 0.0, "dt": "2026-07-16 14:52:00", "stop_loss_log": [],
            "positions": {ETF: {"amount": 46400.0, "avg_cost": 2.214, "price": 2.214}}}


def test_default_matcher_rounds_to_tick(monkeypatch):
    """默认：止损成交价对齐 tick，且吃策略声明的 0.0001 滑点。

    2.145 × (1-0.0001) = 2.1448… → tick 对齐回 2.145（与本机
    eb37e928 实测值一致）；不传滑点走 CONFIG 0.001 时 → 2.143。
    """
    m = _reload(monkeypatch, legacy=False)
    out = m.Matcher(0.03).step(_fresh_state(), {ETF: 2.145}, fee=0.0001,
                                stamp_tax=0.0, slippage=0.0001, min_commission=5.0)
    assert out["stop_loss_log"][0]["price"] == pytest.approx(2.145)
    out2 = m.Matcher(0.03).step(_fresh_state(), {ETF: 2.145}, fee=0.0001,
                                stamp_tax=0.0, min_commission=5.0)
    assert out2["stop_loss_log"][0]["price"] == pytest.approx(2.143)


def test_legacy_matcher_keeps_four_decimals(monkeypatch):
    """legacy：止损保留 4 位小数，复现服务器 2.1429。"""
    m = _reload(monkeypatch, legacy=True)
    out = m.Matcher(0.03).step(_fresh_state(), {ETF: 2.145}, fee=0.0001,
                                stamp_tax=0.0, min_commission=5.0)
    assert out["stop_loss_log"][0]["price"] == pytest.approx(2.1429)


def test_legacy_does_not_touch_normal_orders(monkeypatch):
    """关键回归：legacy 只影响止损，正常委托仍须 tick 对齐。

    服务器 07-10 买单记 2.132（对齐 tick）。若开关做成全局，fill_price 会
    返回 2.1322，首笔即与服务器分叉——A/B 就失去意义。
    """
    _reload(monkeypatch, legacy=True)
    from app.quant.core import fees

    import importlib

    fees = importlib.reload(fees)
    assert fees.fill_price(2.132, "buy", 0.0001, ETF) == pytest.approx(2.132)
    assert fees.fill_price(2.145, "sell", 0.001, ETF) == pytest.approx(2.143)
    assert fees.fill_price(9.005, "buy", 0.0001, STOCK) == pytest.approx(9.01)
