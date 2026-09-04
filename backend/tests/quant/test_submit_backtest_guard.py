"""submit_backtest 去重/幂等 + compile 照常 spawn + tick 口径统一的回归测试。

全部离线：Popen patch 掉，不派生真实 worker；DB 指向 tmp。
"""
from __future__ import annotations

import math
import os
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from app.quant import db
from app.quant import service
from app.quant.config import CONFIG
from app.quant.tick import round_to_tick, tick_size

# popen_factory 会 patch 全局 subprocess.Popen，这里暂存真实现供 _dead_pid 用
_REAL_POPEN = subprocess.Popen


@pytest.fixture
def tmp_quant(tmp_path, monkeypatch):
    monkeypatch.setattr(CONFIG, "db_path", str(tmp_path / "quant.db"))
    monkeypatch.setattr(db, "_COMPILE_DIR", str(tmp_path / "compile"))
    db.init_db(str(tmp_path / "quant.db"))
    return tmp_path


@pytest.fixture
def popen_factory(tmp_quant, monkeypatch):
    """假 Popen：calls 记每次 spawn 的 argv；state['pid'] 控制返回的 pid。

    pid 默认为当前测试进程 pid（存活），测"死亡接管"时改为 _dead_pid()。
    """
    calls: list = []
    state = {"pid": None, "delay": 0.0}

    def _fake(*argv, **kwargs):
        calls.append(list(argv[0]) if argv else [])
        if state["delay"]:
            time.sleep(state["delay"])
        pid = state["pid"] if state["pid"] is not None else os.getpid()
        return SimpleNamespace(pid=pid)

    monkeypatch.setattr(service.subprocess, "Popen", _fake)
    return calls, state


def _dead_pid() -> int:
    """刚退出且已回收的子进程 pid（存活校验必为 False）。"""
    p = _REAL_POPEN([sys.executable, "-c", "pass"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    p.wait()
    return p.pid


# ---- compile 模式照常落库 + spawn（P0 回归：曾被改成 no-op） ----
def test_compile_mode_still_spawns(popen_factory):
    calls, _ = popen_factory
    run_id = service.submit_backtest({"strategy_id": "s", "name": "n"}, compile_mode=True)
    assert run_id.startswith("c_")
    assert len(calls) == 1
    assert calls[0][-2].endswith("run_quant_backtest.py") and calls[0][-1] == run_id
    row = db.get_run(run_id)
    assert row is not None and row["status"] == "queued"
    assert row["pid"] == os.getpid()


# ---- 同 run_id 活任务重复提交 → 拒绝且只 spawn 一次 ----
def test_duplicate_submit_while_alive_rejected(popen_factory):
    calls, _ = popen_factory
    params = {"run_id": "dup1", "strategy_id": "s"}
    service.submit_backtest(params)
    with pytest.raises(RuntimeError, match="已在运行"):
        service.submit_backtest(params)
    assert len(calls) == 1


# ---- 同 run_id 但进程已死 → 接管重跑（复位 queued + 再 spawn） ----
def test_resubmit_after_death_takes_over(popen_factory):
    calls, state = popen_factory
    state["pid"] = _dead_pid()
    params = {"run_id": "dead1", "strategy_id": "s"}
    service.submit_backtest(params)
    service.submit_backtest(params)  # 不抛异常
    assert len(calls) == 2
    assert db.get_run("dead1")["status"] == "queued"


# ---- 遗留 queued 行（无 pid，如 insert 后 crash）→ 接管且不 UNIQUE 冲突 ----
def test_queued_row_without_pid_taken_over(popen_factory):
    calls, _ = popen_factory
    db.insert_run("q1", "s", "n", "{}", "queued")
    service.submit_backtest({"run_id": "q1", "strategy_id": "s"})
    assert len(calls) == 1
    row = db.get_run("q1")
    assert row["status"] == "queued" and row["pid"] == os.getpid()


# ---- 并发 8 连发同 run_id → 恰好一次 spawn（daemon 线程 + 超时断言） ----
def test_concurrent_duplicate_single_spawn(popen_factory):
    calls, state = popen_factory
    state["delay"] = 0.2  # 拉大 spawn 窗口，复现 5 连发竞态
    params = {"run_id": "race1", "strategy_id": "s"}
    outcomes: list = []

    def _submit():
        try:
            service.submit_backtest(dict(params))
            outcomes.append("ok")
        except RuntimeError:
            outcomes.append("rejected")
        except Exception as e:  # noqa: BLE001
            outcomes.append(f"unexpected: {e!r}")

    threads = [threading.Thread(target=_submit, daemon=True) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)
        assert not t.is_alive(), "submit 线程超时未返回（疑似死锁）"
    assert len(calls) == 1, f"重复拉起 worker：{len(calls)} 次"
    assert outcomes.count("ok") == 1
    assert outcomes.count("rejected") == 7, outcomes


# ---- _pid_alive 语义 ----
def test_pid_alive_semantics(tmp_quant, monkeypatch):
    assert service._pid_alive(None) is False
    assert service._pid_alive(0) is False
    assert service._pid_alive(os.getpid()) is True
    assert service._pid_alive(str(os.getpid())) is True
    assert service._pid_alive(_dead_pid()) is False
    # 有进程但无权发信号 → 存活（此前误判死亡会导致活任务被重复拉起）
    monkeypatch.setattr(service.os, "kill",
                        lambda pid, sig: (_ for _ in ()).throw(PermissionError()))
    assert service._pid_alive(12345) is True


# ---- tick 口径统一：三处调用方共享 tick.round_to_tick ----
@pytest.mark.parametrize(("code", "price", "want"), [
    ("510300.XSHG", 2.1322, 2.132),    # ETF 0.001
    ("600000.XSHG", 8.991, 8.99),      # 股票 0.01
    ("159915.XSHE", 1.23456, 1.235),   # 深市 ETF
    ("180012.XSHE", 1.23456, 1.235),   # 18 前缀：旧 bridge 内联判定会误判 0.01
    ("510300", 2.1322, 2.132),         # 无后缀纯数字
    ("688001.XSHG", 10.006, 10.01),    # 科创板按股票 tick
    (None, 8.991, 8.99),               # 未知代码回退股票 tick
])
def test_round_to_tick_cases(code, price, want):
    assert round_to_tick(price, code) == pytest.approx(want)


@pytest.mark.parametrize(("code", "want"), [
    ("510300.XSHG", 0.001),
    ("159915.XSHE", 0.001),
    ("180012.XSHE", 0.001),
    ("600000.XSHG", 0.01),
    ("688001.XSHG", 0.01),
    ("430047.BJ", 0.01),
    ("000001.XSHE", 0.01),
])
def test_tick_size_prefixes(code, want):
    assert tick_size(code) == want


def test_round_to_tick_nonfinite_passthrough():
    assert math.isnan(round_to_tick(float("nan"), "510300.XSHG"))
    assert round_to_tick(float("inf"), "600000.XSHG") == float("inf")


def test_revoke_future_split_factor_asof():
    """拆股复权撤销按 as-of（2026-09-03 教训：562590 07-06 拆股，06-23 补跑
    现价被压到 1.116，动量全毁；    回测侧早有同语义撤销）。"""
    from app.quant.jqengine.datasource.manager import DataManager

    mgr = DataManager.__new__(DataManager)
    mgr._minute_split_events = {"562590.XSHG": [(pd.Timestamp("2026-07-06"), 0.3394793926247288)]}
    # as-of 在除权日前 → 除回去（×1/0.339≈×2.95）：1.118 → 3.29（当日真价）
    f = mgr.revoke_future_split_factor("562590.XSHG", "2026-06-23 13:10:00")
    assert f == pytest.approx(1 / 0.3394793926247288)
    assert 1.118 * f == pytest.approx(3.293, abs=0.01)
    # as-of 在除权日后 → 保留复权视角（系数 1.0）
    assert mgr.revoke_future_split_factor("562590.XSHG", "2026-07-07") == 1.0
    # 无事件 / 未知标的 → 1.0
    assert mgr.revoke_future_split_factor("510300.XSHG", "2026-06-23") == 1.0


def test_batch_daily_refetches_uncovered_mem_frame():
    """批量日线必须校验内存帧覆盖（2026-09-03 教训：窄/陈旧帧被长回看直接用，
    06-23 562590 R²≈0 选票翻转）。覆盖不足 → 走 fetch 补取并 healing；
    覆盖足够 → 不碰网络。"""
    from app.quant.jqengine.engine.jq import api as jq_api

    full_idx = pd.DatetimeIndex([f"2026-05-{d:02d}" for d in range(6, 30)] +
                                [f"2026-06-{d:02d}" for d in range(1, 24)])
    full_idx = full_idx[full_idx.weekday < 5][:40]
    narrow_idx = full_idx[-8:]  # 窄帧：仅尾部 8 根
    stale_idx = full_idx[:30]  # 陈旧帧：末端早于请求末端

    def _df(idx):
        n = len(idx)
        return pd.DataFrame({"close": [1.0] * n, "money": [1e7] * n}, index=idx)

    class _FakeMgr:
        def __init__(self):
            self._daily_mem = {}
            self.fetch_calls = []

        def fetch(self, method, sec, start=None, end=None):
            self.fetch_calls.append(sec)
            df = _df(full_idx)
            self._daily_mem[f"{method}_{sec}"] = df
            return df

    mgr = _FakeMgr()
    jq_api._state["manager"] = mgr
    jq_api._state["ctx"] = None
    try:
        # 覆盖足够：不触发 fetch
        mgr._daily_mem["get_daily_AAA.XSHG"] = _df(full_idx)
        out = jq_api._get_price_batch_daily(
            ["AAA.XSHG"], None, "2026-06-23", 25, ["money"], False)
        assert mgr.fetch_calls == []
        assert len(out) == 25
        # 窄帧：触发补取并 healing，后续不再补取
        mgr._daily_mem["get_daily_BBB.XSHG"] = _df(narrow_idx)
        out = jq_api._get_price_batch_daily(
            ["BBB.XSHG"], None, "2026-06-23", 25, ["money"], False)
        assert mgr.fetch_calls == ["BBB.XSHG"]
        assert len(out) == 25
        out = jq_api._get_price_batch_daily(
            ["BBB.XSHG"], None, "2026-06-23", 25, ["money"], False)
        assert mgr.fetch_calls == ["BBB.XSHG"]  # 已 healing，不再补取
        # 陈旧帧：触发补取
        mgr._daily_mem["get_daily_CCC.XSHG"] = _df(stale_idx)
        out = jq_api._get_price_batch_daily(
            ["CCC.XSHG"], None, "2026-06-23", 25, ["money"], False)
        assert mgr.fetch_calls[-1] == "CCC.XSHG"
        assert len(out) == 25
    finally:
        jq_api._state["manager"] = None
        jq_api._state["ctx"] = None


def test_fund_instrument_type_covers_all_universe_prefixes():
    """_fund_instrument_type 必须覆盖宇宙实证的全部基金前缀（2026-09-03 教训：
    52/53/55 被误判 CS，回测对 ETF 收 0.05% 印花税，520830 一笔多扣 49.76）。
    无 rqalpha 环境跳过（jqcompat 顶层 import rqalpha）。"""
    pytest.importorskip("rqalpha")
    from app.quant import jqcompat

    t = jqcompat._fund_instrument_type
    for code in ["510300.XSHG", "513360.XSHG", "520830.XSHG", "531111.XSHG",
                 "551111.XSHG", "560650.XSHG", "588020.XSHG", "159876.XSHE",
                 "159915.XSHE", "180012.XSHE"]:
        assert t(code) == "ETF", code
    for code in ["501018.XSHG", "161226.XSHE"]:
        assert t(code) == "LOF", code
    for code in ["600000.XSHG", "688001.XSHG", "000001.XSHE", "300750.XSHE"]:
        assert t(code) == "CS", code


def test_mem_recarray_uses_true_money():
    """_mem_daily_to_recarray 必须用 money 列（2026-09-03 教训：close×volume
    与真成交额差 ~1.5%，阈值边缘码分叉致动态池 137 vs 135）。
    money 缺失/NaN/0 才回退 close×volume（与模拟盘读 money 列同口径）。"""
    pytest.importorskip("rqalpha")  # jqcompat 顶层 import rqalpha
    from app.quant import jqcompat

    idx = pd.DatetimeIndex(["2026-08-14", "2026-08-17", "2026-08-18"])
    df = pd.DataFrame({
        "open": [14.9, 15.5, 15.7], "high": [15.0, 15.6, 15.8],
        "low": [14.8, 15.4, 15.6], "close": [14.99, 15.64, 15.80],
        "volume": [35299800.0, 49000000.0, 55000000.0],
        "money": [521545425.0, float("nan"), 0.0],
    }, index=idx)
    arr = jqcompat._mem_daily_to_recarray(df, code="000032.XSHE")
    assert arr["total_turnover"][0] == pytest.approx(521545425.0)  # 真 money
    assert arr["total_turnover"][1] == pytest.approx(15.64 * 49000000.0)  # NaN 回退
    assert arr["total_turnover"][2] == pytest.approx(15.80 * 55000000.0)  # 0 回退
    # 无 money 列 → 全回退
    arr2 = jqcompat._mem_daily_to_recarray(df.drop(columns=["money"]))
    assert arr2["total_turnover"][0] == pytest.approx(14.99 * 35299800.0)


def test_order_cost_recorded_not_dropped():
    """set_order_cost 不得静默丢弃（2026-09-03 教训：回测按万3、补跑按万1，
    19 笔累计差 ~420 元现金，08-06 首个 dust 单即分叉）。
    无 env 时仅断言记录值；运行时覆盖由端到端对齐验证。"""
    pytest.importorskip("rqalpha")  # jqcompat 顶层 import rqalpha
    from app.quant import jqcompat

    jqcompat._ORDER_COST_OVERRIDE = None
    try:
        jqcompat.set_order_cost(jqcompat.OrderCost(
            open_tax=0, close_tax=0, open_commission=0.0001,
            close_commission=0.0001, close_today_commission=0.0001, min_commission=5))
        assert jqcompat._ORDER_COST_OVERRIDE == {
            "open_commission": 0.0001, "close_commission": 0.0001,
            "close_today_commission": 0.0001, "min_commission": 5.0,
        }
    finally:
        jqcompat._ORDER_COST_OVERRIDE = None


def test_tick_patch_hits_installed_rqalpha_layout():
    """tick 取整补丁必须命中当前 venv 的 rqalpha 布局（2026-09-03 教训：
    6.x 重构 matcher 后旧补丁静默 no-op，回测价带滑点尾数）。
    无 rqalpha 的环境（仅 --extra dev）自动跳过，由端到端对齐验证覆盖。"""
    rqalpha = pytest.importorskip("rqalpha")  # noqa: F841
    from app.quant import jqcompat

    assert jqcompat._patch_matcher_tick_rounding() is True
    try:
        from rqalpha.mod.rqalpha_mod_sys_simulation.slippage import SlippageDecider
    except ImportError:
        from rqalpha.mod.rqalpha_mod_sys_simulation.matcher.base import BaseMatcher
        assert getattr(BaseMatcher, "_jq_tick_patched", False) is True
    else:
        assert getattr(SlippageDecider, "_jq_tick_patched", False) is True


def test_matcher_stoploss_uses_shared_tick(tmp_quant):
    """Matcher 止损成交价走 round_to_tick：180012.XSHE 按 0.001 取整得 8.991，
    若回退到旧 _is_etf 二分支会得 8.99（与回测/引擎分叉）。"""
    from app.quant.simulate.matcher import Matcher

    m = Matcher(0.03)
    state = {"cash": 0.0, "positions": {
        "180012.XSHE": {"amount": 100.0, "avg_cost": 10.0, "price": 10.0}},
        "stop_loss_log": [], "dt": "2026-07-17 10:00:00"}
    m.step(state, {"180012.XSHE": 9.0}, fee=0.0, stamp_tax=0.0, slippage=0.001)
    assert state["stop_loss_log"][0]["price"] == pytest.approx(8.991)
