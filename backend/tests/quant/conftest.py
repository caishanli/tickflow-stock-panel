"""quant 测试共享 fixture。"""
from __future__ import annotations

import types

import pytest


@pytest.fixture
def mem_ok(monkeypatch):
    """内存守卫隔离: 无活模拟盘进程 + 内存充裕(守卫恒放行)。

    service.account_start / account_ensure_running 在 spawn 前经
    sim_memory.memory_check 做 psutil 全机扫描(活模拟盘进程数 + 空闲内存),
    不隔离的话测试结果取决于宿主机状态。沿用 test_sim_memory.py 的 patch 惯例。
    """
    from app.quant.simulate import memory as sim_memory
    monkeypatch.setattr(sim_memory.psutil, "process_iter", lambda attrs=(): iter([]))
    monkeypatch.setattr(sim_memory.psutil, "virtual_memory",
                        lambda: types.SimpleNamespace(available=8 * 1024 ** 3))
