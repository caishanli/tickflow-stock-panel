"""wufu-v5.4 策略 is_temporarily_suspended 回归单测。

背景：08-13 模拟盘 637c7aed 两条「分钟数据缺失」异常——159287/561460 盘中
实时分钟回源失败（stockdata 当日分区未落盘、内存库无该标的），策略误判为
临时停牌并踢出动量排名。修复：空分钟数据时用实时行情（last_price / paused）
交叉验证，活跃标的判定为取数异常而非停牌（保留警告），不再静默踢出。

全部离线：swap 策略 namespace 中的 get_price / get_current_data，不触碰数据层。
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

import pandas as pd
import pytest

from app.quant.jqengine.engine.jq import api
from app.quant.jqengine.engine.jq.loader import load_strategy

FIXTURE = (
    Path(__file__).parent.parent / "fixtures" / "wufu_v54" / "wufu-v5.4.py"
)
ETF = "159287.XSHE"


class _Mgr:
    """最小 manager stub：取数路径不触碰它。"""

    def __init__(self):
        self.sources = {}
        self._daily_mem = {}
        self._minute_mem = {}

    def fetch(self, *a, **k):
        raise RuntimeError("stub")

    def get_minute_price_at(self, code, dt):
        return None


class _Live:
    """fake current_data[code]：只暴露策略用到的字段。"""

    def __init__(self, last_price, paused=False):
        self.last_price = last_price
        self.paused = paused


class _CurrentData:
    def __init__(self, snap):
        self.snap = snap

    def __getitem__(self, code):
        return self.snap[code]


def _load():
    # 其它用例（rqalpha/jqcompat）会向 sys.modules 注册带 shim 的假 jqdata，
    # 导致策略 "from jqdata import *" 拿到 jqcompat._Log 而非 api.log，恢复空假模块
    _jq = types.ModuleType("jqdata")
    _jq.__all__ = []
    sys.modules["jqdata"] = _jq
    bundle = load_strategy(
        FIXTURE.read_text(encoding="utf-8"), _Mgr(), 0.0003, 0.001, 100000.0
    )
    # 策略定义了 initialize → init_fn 即策略函数，其 __globals__ 就是注入的命名空间
    ns = bundle.init_fn.__globals__
    # 从策略自身 namespace 取回真身（执行时覆盖了引擎注入的同名 API）
    assert ns["is_temporarily_suspended"] is not api.is_temporarily_suspended
    assert ns["log"] is api.log
    return bundle, ns


def _suspended(bundle_ns, minute_df, last_price=1.0, paused=False):
    """在 swap 后的环境里执行策略的 is_temporarily_suspended。"""
    bundle, ns = bundle_ns
    # LogProxy._modules 是类级状态，会残留其它用例设置的高等级，重置以保证 warn 落 sink
    api.log.set_level("strategy", "info")
    sink = []
    api._state["log_sink"] = lambda level, msg: sink.append((level, msg))
    ns["get_price"] = lambda *a, **k: minute_df
    ns["get_current_data"] = lambda: _CurrentData(
        {ETF: _Live(last_price, paused)}
    )
    try:
        res = ns["is_temporarily_suspended"](ETF, bundle.ctx)
        return res, [m for _, m in sink]
    finally:
        api._state["log_sink"] = None


def _minute_df(volumes):
    return pd.DataFrame(
        {"volume": volumes},
        index=pd.date_range("2026-08-13 13:46", periods=len(volumes), freq="min"),
    )


# ---- 空分钟数据（核心回归）：活跃标的误判防护 ----
def test_empty_minute_data_with_live_price_not_suspended(bundle_ns):
    """空分钟数据 + 实时价正常 → 判定为取数异常而非停牌，不踢出。"""
    res, logs = _suspended(bundle_ns, pd.DataFrame())
    assert res is False
    assert any("分钟数据缺失" in m and "不按停牌处理" in m for m in logs)


def test_empty_minute_data_without_live_price_suspended(bundle_ns):
    """空分钟数据 + 无实时价（last_price=0）→ 仍按临时停牌处理。"""
    res, logs = _suspended(bundle_ns, pd.DataFrame(), last_price=0.0)
    assert res is True
    assert any("分钟数据缺失" in m and "按临时停牌处理" in m for m in logs)


def test_empty_minute_data_when_paused_suspended(bundle_ns):
    """空分钟数据 + 引擎标记全天停牌 → 判停牌。"""
    res, logs = _suspended(bundle_ns, pd.DataFrame(), paused=True)
    assert res is True
    assert any("全天停牌" in m for m in logs)


# ---- 非空分钟数据：维持原有语义 ----
def test_zero_volume_minute_data_is_suspended(bundle_ns):
    """最近10分钟成交量全为0 → 真实盘中临时停牌。"""
    res, _ = _suspended(bundle_ns, _minute_df([0] * 10))
    assert res is True


def test_traded_minute_data_not_suspended(bundle_ns):
    """最近10分钟有成交（且实时价正常）→ 不判停牌。"""
    res, _ = _suspended(bundle_ns, _minute_df([100] * 10))
    assert res is False


@pytest.fixture(autouse=True)
def bundle_ns():
    return _load()