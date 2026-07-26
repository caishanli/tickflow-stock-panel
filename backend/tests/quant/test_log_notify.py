"""LogProxy.notify() 单测：notify() 同时触发 info 与 notify 两个 sink level。

不联网、不依赖钉钉，仅校验 LogProxy 行为。
"""
from __future__ import annotations

from app.quant.jqengine.engine.jq import api


class _Mgr:
    """最小 manager stub：取价/下单路径不触碰它。"""
    sources = {}
    _daily_mem = {}
    _minute_mem = {}

    def fetch(self, *a, **k):
        raise RuntimeError("stub")

    def get_minute_price_at(self, code, dt):
        return None


def _setup(cash=100000.0):
    ctx = api._reset(_Mgr(), fee=0.0003, slippage=0.001, cash=cash)
    return ctx


def test_notify_calls_info_and_sink():
    """notify() 应同时触发 info sink（写日志）和 notify sink（推送）。"""
    _setup()
    got = []
    api._state["log_sink"] = lambda level, msg: got.append((level, msg))

    api.log.notify("hello")

    assert got == [("info", "hello"), ("notify", "hello")]
    api._state["log_sink"] = None


def test_notify_without_sink_no_crash():
    """无 log_sink 时 notify() 不应抛异常。"""
    _setup()
    assert api._state.get("log_sink") is None

    api.log.notify("hello")  # 不应抛异常
