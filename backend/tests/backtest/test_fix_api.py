"""回测 API 层已确认 bug 的回归测试。

覆盖:
- A1  strategy/cancel 漏传 minute_fill → 分钟K任务永远取消不掉
- A5  检查失败路径不置 job.done → 僵尸 job 累积、同参重连 SSE 空转
- A6  FACTOR/STRATEGY_DEFAULT_DAYS 名实互换 (行为不变, 仅命名修正)
- A7  job_key 用原始 start/end (None 固化) → 改为解析后的日期
- A8  optimize 不走并发信号量、max_workers 无上限
- _OPT_BT_FIELDS 缺 entry_fill/exit_fill/asset_type/minute_fill
- 参数校验 (负费用/零下界/枚举)、非法日期/JSON 的 4xx、/status 死代码、/run 防护
"""
from __future__ import annotations

import asyncio
import threading
import time
from datetime import date
from types import SimpleNamespace
from urllib.parse import urlencode

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import backtest as api
from app.api.backtest import (
    BACKTEST_MAX_SYMBOLS,
    FACTOR_DEFAULT_DAYS,
    OPTIMIZE_MAX_WORKERS,
    STRATEGY_DEFAULT_DAYS,
    _OPT_BT_FIELDS,
    _make_job_key,
    _make_opt_job_key,
    _opt_backtest_kwargs,
    _running_jobs,
)


@pytest.fixture(autouse=True)
def _clean_jobs():
    """每个测试前后清空模块级任务表, 避免用例间互相污染。"""
    _running_jobs.clear()
    yield
    _running_jobs.clear()


class _FakeRepo:
    """最小 repo: 只实现端点用到的日期探测方法。"""

    def earliest_daily_date(self):
        return date(2020, 1, 1)

    def earliest_minute_date(self):
        return None


class _FakeCaps:
    def __init__(self, allowed: bool = True):
        self._allowed = allowed

    def has(self, cap):
        return self._allowed


def _make_app(caps: bool = True) -> FastAPI:
    app = FastAPI()
    app.include_router(api.router)
    app.state.repo = _FakeRepo()
    # 预设 dummy engine, 避免 _get_engine 构造真 BacktestEngine
    app.state.backtest_engine = object()
    app.state.strategy_engine = object()
    app.state.capabilities = _FakeCaps(caps)
    return app


class _BlockingSvc:
    """假 StrategyBacktestService: 先发一条进度 (让 SSE 流有首个事件、client.stream 能返回),
    然后阻塞到 cancel_event 置位, 模拟进行中的回测。"""

    def __init__(self, engine, strategy_engine):
        pass

    def run(self, cfg, progress_cb, cancel_event):
        if progress_cb:
            progress_cb({"day": 1, "total": 2, "date": "2025-01-02", "equity": 1.0})
        cancel_event.wait(10)
        return SimpleNamespace(error="cancelled")


def _patch_blocking_svc(monkeypatch):
    import app.backtest.strategy as strat_mod
    monkeypatch.setattr(strat_mod, "StrategyBacktestService", _BlockingSvc)


# strategy_stream 直接调用时的显式参数 (Query(...) 默认值只在 FastAPI 路由层生效,
# 直接调函数必须全部显式传, 否则 Query 对象会混进 job_key)。
_STREAM_DEFAULTS = dict(
    symbols=None, matching="open_t+1", entry_fill=None, exit_fill=None,
    fees_pct=0.0002, commission_pct=None, stamp_tax_pct=None, slippage_bps=5.0,
    max_positions=10, max_exposure_pct=1.0, initial_capital=1_000_000.0,
    position_sizing="equal", params=None, overrides=None, mode="position",
    holding_days=5, asset_type="stock", minute_fill=False,
)


def _stream_request(app):
    class _Req:
        def __init__(self):
            self.app = app

        async def is_disconnected(self):
            return False

    return _Req()


def _cancel_request(app, qs: str):
    class _Req:
        def __init__(self):
            self.app = app

        async def json(self):
            return {"qs": qs}

    return _Req()


def _drain_response(resp, chunks: list) -> threading.Thread:
    """后台线程消费 SSE body_iterator (TestClient 不做增量流式, 直接驱动生成器)。"""
    async def _collect():
        async for ch in resp.body_iterator:
            chunks.append(ch)

    t = threading.Thread(target=lambda: asyncio.run(_collect()), daemon=True)
    t.start()
    return t


# ---------------------------------------------------------------- A6 常量名实相符

def test_default_days_constants_match_actual_usage():
    """A6: 因子默认 3 年、策略默认 180 天 (行为即现状), 常量名与口径一致。"""
    assert FACTOR_DEFAULT_DAYS == 365 * 3
    assert STRATEGY_DEFAULT_DAYS == 180


# ---------------------------------------------------------------- #9 /status 按实际可用性返回

def test_status_reflects_is_available(monkeypatch):
    monkeypatch.setattr(api, "is_available", lambda: False)
    r = TestClient(_make_app()).get("/api/backtest/status")
    assert r.status_code == 200
    assert r.json() == {"available": False}

    monkeypatch.setattr(api, "is_available", lambda: True)
    r = TestClient(_make_app()).get("/api/backtest/status")
    assert r.json() == {"available": True}


# ---------------------------------------------------------------- A1 + A7 cancel 与 stream 的 key 对齐

def test_cancel_minute_fill_job_via_stream_key(monkeypatch):
    """A1: minute_fill=true 启动的任务必须能取消 (修复前 cancel 侧 key 缺 minute_fill)。"""
    _patch_blocking_svc(monkeypatch)
    app = _make_app()
    qs = urlencode({
        "strategy_id": "s1",
        "start": "2025-01-01",
        "end": "2025-02-01",
        "minute_fill": "true",
    })
    resp = asyncio.run(api.strategy_stream(
        _stream_request(app), strategy_id="s1",
        start="2025-01-01", end="2025-02-01",
        **{**_STREAM_DEFAULTS, "minute_fill": True},
    ))
    # 端点体已执行: job 已建 (生成器尚未驱动, 线程未起)
    assert len(_running_jobs) == 1
    job = next(iter(_running_jobs.values()))

    chunks: list = []
    t = _drain_response(resp, chunks)
    # 等回测线程起来 (run 开始会先 append 一条 progress)
    for _ in range(100):
        if job.progress:
            break
        time.sleep(0.05)
    assert job.progress, "回测线程未启动"

    r = asyncio.run(api.strategy_cancel(_cancel_request(app, qs)))
    assert r["ok"] is True
    assert job.cancel_event.is_set()

    t.join(timeout=10)
    assert not t.is_alive()
    assert any("回测已取消" in c for c in chunks)


def test_cancel_resolves_dates_like_stream(monkeypatch):
    """A7: 缺省 start/end 时两侧都先解析再入 key (修复前 key 固化 "None")。"""
    _patch_blocking_svc(monkeypatch)
    app = _make_app()
    resp = asyncio.run(api.strategy_stream(_stream_request(app), strategy_id="s1", **_STREAM_DEFAULTS))
    # key 必须含解析后的日期 (fake repo 最早日K = 2020-01-01, end = 今天)
    expected = _make_job_key(
        "s1", None, "2020-01-01", date.today().isoformat(),
        "open_t+1", None, None, 0.0002, 5.0, 10, 1.0, 1_000_000.0, "equal",
        None, None, "position", 5,
        commission_pct=None, stamp_tax_pct=None, asset_type="stock", minute_fill=False,
    )
    assert expected in _running_jobs
    del resp  # 本用例不需驱动生成器

    r = asyncio.run(api.strategy_cancel(_cancel_request(app, urlencode({"strategy_id": "s1"}))))
    assert r["ok"] is True


def test_cancel_tolerates_empty_numeric_params():
    """#8: cancel 侧 float("") 不得抛 ValueError (修复前 500)。"""
    app = _make_app()

    class _Req:
        def __init__(self, body):
            self._body = body
            self.app = app

        async def json(self):
            return self._body

    res = asyncio.run(api.strategy_cancel(_Req({"qs": "strategy_id=s&fees_pct=&max_positions="})))
    assert res["ok"] is False  # 无此任务, 但不抛异常
    res = asyncio.run(api.strategy_cancel(_Req({"qs": "strategy_id=s&fees_pct=abc"})))
    assert res["ok"] is False
    assert "非法" in res["message"]


# ---------------------------------------------------------------- A5 僵尸 job

def test_guard_failure_marks_job_done_and_reconnect_safe(monkeypatch):
    """A5: guard 失败路径必须置 job.done; 同参重连直接回吐错误而不是空转。"""
    monkeypatch.setattr(api.settings, "backtest_range_guard", True)
    client = TestClient(_make_app())
    params = {"strategy_id": "s1", "start": "2020-01-01", "end": "2026-01-01"}

    r = client.get("/api/backtest/strategy/stream", params=params)
    assert r.status_code == 200
    assert "event: error" in r.text

    assert len(_running_jobs) == 1
    job = next(iter(_running_jobs.values()))
    assert job.done is True
    assert job.error == api.BACKTEST_SERVER_GUARD_MESSAGE
    assert job.finish_ts > 0

    # 同参重连: 仍然立即返回错误事件 (修复前会跳过线程启动并无限空转)
    r2 = client.get("/api/backtest/strategy/stream", params=params)
    assert "event: error" in r2.text
    assert len(_running_jobs) == 1  # 不新增 job

    # done + 过期后能被 _cleanup_stale_jobs 清掉 (修复前僵尸永不清理)
    job.finish_ts -= api._JOB_TTL + 1
    api._cleanup_stale_jobs()
    assert not _running_jobs


def test_minute_fill_cap_failure_marks_job_done():
    """A5: 分钟K Pro+ 门控失败路径同样置 done。"""
    client = TestClient(_make_app(caps=False))
    r = client.get("/api/backtest/strategy/stream", params={
        "strategy_id": "s1", "start": "2025-01-01", "end": "2025-02-01", "minute_fill": "true",
    })
    assert "Pro+" in r.text
    job = next(iter(_running_jobs.values()))
    assert job.done is True
    assert "Pro+" in job.error


def test_optimize_guard_failure_marks_job_done(monkeypatch):
    """A5: optimize_stream 的 guard 失败路径也置 done (grid 非法路径此前已修)。"""
    monkeypatch.setattr(api.settings, "backtest_range_guard", True)
    client = TestClient(_make_app())
    r = client.get("/api/backtest/optimize/stream", params={
        "strategy_id": "s1", "param_grid": '{"p": [1, 2]}',
        "start": "2020-01-01", "end": "2026-01-01",
    })
    assert "event: error" in r.text
    job = next(iter(_running_jobs.values()))
    assert job.done is True
    assert job.error == api.BACKTEST_SERVER_GUARD_MESSAGE


# ---------------------------------------------------------------- #8 非法输入 4xx

def test_invalid_date_returns_400():
    client = TestClient(_make_app())
    r = client.get("/api/backtest/strategy/stream", params={"strategy_id": "s1", "start": "not-a-date"})
    assert r.status_code == 400
    r = client.get("/api/backtest/optimize/stream", params={
        "strategy_id": "s1", "param_grid": '{"p": [1]}', "end": "2026/01/01",
    })
    assert r.status_code == 400


def test_invalid_params_json_returns_400_before_stream():
    """#8: params 非法 JSON 在流开始前 400, 且不残留 job。"""
    client = TestClient(_make_app())
    r = client.get("/api/backtest/strategy/stream", params={
        "strategy_id": "s1", "start": "2025-01-01", "end": "2025-02-01", "params": "{invalid",
    })
    assert r.status_code == 400
    assert not _running_jobs


# ---------------------------------------------------------------- #7 参数校验

def test_stream_rejects_invalid_enum_and_negative_numbers():
    client = TestClient(_make_app())
    base = {"strategy_id": "s1", "start": "2025-01-01", "end": "2025-02-01"}
    for bad in (
        {"matching": "bogus"},
        {"entry_fill": "bogus"},
        {"exit_fill": "bogus"},
        {"mode": "bogus"},
        {"position_sizing": "bogus"},
        {"fees_pct": "-1"},
        {"slippage_bps": "-5"},
        {"max_positions": "0"},
        {"initial_capital": "0"},
        {"holding_days": "0"},
    ):
        r = client.get("/api/backtest/strategy/stream", params={**base, **bad})
        assert r.status_code == 422, bad


def test_post_models_reject_invalid_numbers():
    client = TestClient(_make_app())
    # 信号回测
    r = client.post("/api/backtest/run", json={"symbols": ["600000"], "fees_pct": -0.1})
    assert r.status_code == 422
    r = client.post("/api/backtest/run", json={"symbols": ["600000"], "max_hold_days": 0})
    assert r.status_code == 422
    # 因子回测
    r = client.post("/api/backtest/factor/run", json={"factor_name": "f", "n_groups": 0})
    assert r.status_code == 422
    r = client.post("/api/backtest/factor/run", json={"factor_name": "f", "slippage_bps": -1})
    assert r.status_code == 422
    # 策略回测
    for bad in (
        {"fees_pct": -1}, {"commission_pct": -0.1}, {"max_positions": 0},
        {"initial_capital": 0}, {"max_exposure_pct": 0}, {"holding_days": -1},
    ):
        r = client.post("/api/backtest/strategy/run", json={"strategy_id": "s", **bad})
        assert r.status_code == 422, bad


# ---------------------------------------------------------------- #11 /run 信号回测防护

def test_signal_run_has_range_guard_and_symbols_cap(monkeypatch):
    client = TestClient(_make_app())
    # 标的上限 (guard 关闭时也生效)
    r = client.post("/api/backtest/run", json={"symbols": ["600000"] * (BACKTEST_MAX_SYMBOLS + 1)})
    assert r.status_code == 400
    assert "最多支持" in r.json()["detail"]
    # 服务端范围保护: 默认 3 年区间超上限 → 400 (与因子/策略端点对齐)
    monkeypatch.setattr(api.settings, "backtest_range_guard", True)
    r = client.post("/api/backtest/run", json={"symbols": ["600000"]})
    assert r.status_code == 400
    assert r.json()["detail"] == api.BACKTEST_SERVER_GUARD_MESSAGE


# ---------------------------------------------------------------- #6 _OPT_BT_FIELDS 口径完整

def test_opt_bt_fields_cover_fill_and_asset_type():
    """#6: 优化必须透传成交口径/资产类型/分钟K, 否则优化的是另一套配置。"""
    for f in ("entry_fill", "exit_fill", "asset_type", "minute_fill"):
        assert f in _OPT_BT_FIELDS
    bt = _opt_backtest_kwargs("open_t+1", 0.0002, None, None, 5.0, 10, 1.0, 1e6, "equal", "position", 5)
    for f in _OPT_BT_FIELDS:
        assert f in bt
    # 口径不同 → bt_sig 不同 → job_key 不同
    sig = lambda b: "|".join(f"{k}={b[k]}" for k in _OPT_BT_FIELDS)
    bt_m = _opt_backtest_kwargs("open_t+1", 0.0002, None, None, 5.0, 10, 1.0, 1e6, "equal", "position", 5,
                                minute_fill=True, asset_type="etf")
    assert _make_opt_job_key("s", None, None, None, "g", "sortino", None, sig(bt)) != \
        _make_opt_job_key("s", None, None, None, "g", "sortino", None, sig(bt_m))


# ---------------------------------------------------------------- A8 optimize 并发防护

class _RecSem:
    """记录 acquire 次数的假信号量。"""

    def __init__(self):
        self.acquired = 0

    def acquire(self):
        self.acquired += 1

    def release(self):
        pass


class _RecOptimizer:
    """记录 OptimizeConfig 的假优化器, 立即返回结果让 SSE 收尾。"""
    seen: dict = {}

    def __init__(self, svc, strategy_engine):
        pass

    def optimize(self, ocfg, progress_cb, cancel_event):
        _RecOptimizer.seen = {
            "max_workers": ocfg.max_workers,
            "backtest_kwargs": dict(ocfg.backtest_kwargs),
        }
        return {"best_params": {"p": 1}, "results": []}


def test_optimize_uses_semaphore_and_clamps_max_workers(monkeypatch):
    import app.backtest.optimizer as opt_mod
    monkeypatch.setattr(opt_mod, "StrategyOptimizer", _RecOptimizer)
    sem = _RecSem()
    monkeypatch.setattr(api, "_backtest_semaphore", sem)

    client = TestClient(_make_app())
    r = client.get("/api/backtest/optimize/stream", params={
        "strategy_id": "s1", "param_grid": '{"p": [1, 2]}',
        "start": "2025-01-01", "end": "2025-02-01",
        "max_workers": "99", "entry_fill": "close_t", "asset_type": "etf", "minute_fill": "true",
    })
    assert r.status_code == 200
    assert "best_params" in r.text  # done 事件

    # A8: _run_opt 走信号量; max_workers 钳到绝对上限
    assert sem.acquired == 1
    assert _RecOptimizer.seen["max_workers"] == OPTIMIZE_MAX_WORKERS
    # #6: 用户口径完整透传给优化器
    bt = _RecOptimizer.seen["backtest_kwargs"]
    assert bt["entry_fill"] == "close_t"
    assert bt["asset_type"] == "etf"
    assert bt["minute_fill"] is True
