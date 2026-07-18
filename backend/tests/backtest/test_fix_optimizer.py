"""优化器已确认 bug 的回归测试。

覆盖:
- S4  full 模式 stats 无 annual_return/calmar/avg_pnl/median_pnl/avg_holding_days,
      objective_value 全返 -inf → best_params=None 无报错 (静默全灭)
      → 跑网格前校验, 缺失时 ValueError 并列出该模式可用目标。
- cfg.direction 未校验: 非 max/min 以前静默按 max → 现在 ValueError。
- 网格 {min,max,step} 展开 step 不整除时丢 max 端点 → 补上端点 (且不超界)。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pytest

from app.backtest.optimizer import OptimizeConfig, StrategyOptimizer, expand_param_grid


@dataclass
class _FakeDef:
    meta: dict


class _FakeEngine:
    def __init__(self, params_meta):
        self._def = _FakeDef(meta={"params": params_meta})

    def get(self, strategy_id):
        return self._def


@dataclass
class _FakeResult:
    stats: dict
    error: str | None = None


class _FakeService:
    def run(self, config, progress_cb=None, cancel_event=None):
        return _FakeResult(stats={
            "sharpe": 1.0, "total_return": 0.1, "win_rate": 0.5,
            "max_drawdown": -0.1, "avg_holding_days": 3.0,
        })


PARAMS_META = [
    {"id": "ma_proximity", "type": "float", "default": 0.02, "min": 0.01, "max": 0.05, "step": 0.005},
]


def _cfg(**kw) -> OptimizeConfig:
    base = dict(
        strategy_id="s", symbols=None, start=date(2024, 1, 1), end=date(2024, 6, 1),
        param_grid={"ma_proximity": [0.01, 0.02]}, objective="sharpe", max_workers=2,
    )
    base.update(kw)
    return OptimizeConfig(**base)


def _optimizer() -> StrategyOptimizer:
    return StrategyOptimizer(_FakeService(), _FakeEngine(PARAMS_META))


# ---------------------------------------------------------------- S4 目标校验

@pytest.mark.parametrize("objective", ["annual_return", "calmar", "avg_pnl", "median_pnl", "avg_holding_days"])
def test_full_mode_missing_objective_rejected(objective):
    """full 模式不存在的目标 → ValueError, 报可用目标列表 (不再静默全灭)。"""
    with pytest.raises(ValueError, match="full 模式"):
        _optimizer().optimize(_cfg(objective=objective, backtest_kwargs={"mode": "full"}))


@pytest.mark.parametrize("objective", ["sharpe", "total_return", "win_rate", "max_drawdown"])
def test_full_mode_available_objective_accepted(objective):
    """full 模式存在的目标正常跑通。"""
    out = _optimizer().optimize(_cfg(objective=objective, backtest_kwargs={"mode": "full"}))
    assert out["best_params"] is not None


def test_position_mode_objectives_unaffected():
    """position (默认) 模式所有目标不受影响。"""
    out = _optimizer().optimize(_cfg(objective="avg_holding_days"))
    assert out["best_params"] is not None


# ---------------------------------------------------------------- direction 校验

def test_invalid_direction_rejected():
    """direction 非 max/min → ValueError (修复前静默按 max 处理)。"""
    with pytest.raises(ValueError, match="direction"):
        _optimizer().optimize(_cfg(direction="up"))


def test_valid_directions_accepted():
    assert _optimizer().optimize(_cfg(direction="min"))["direction"] == "min"
    assert _optimizer().optimize(_cfg(direction="max"))["direction"] == "max"


# ---------------------------------------------------------------- 网格端点

def test_range_spec_includes_max_when_step_not_divisible():
    """step 不整除 [min,max] 时补上 max 端点 (修复前 1.0 被丢)。"""
    meta = [{"id": "p", "type": "float", "default": 0.0, "min": 0.0, "max": 1.0, "step": 0.3}]
    vals = sorted(c["p"] for c in expand_param_grid(meta, {"p": {"min": 0.0, "max": 1.0, "step": 0.3}}))
    assert vals == [0.0, 0.3, 0.6, 0.9, 1.0]


def test_range_spec_never_exceeds_max():
    """不整除且 round 会越界时, 不得产出 > max 的候选 (如 1.05)。"""
    meta = [{"id": "p", "type": "float", "default": 0.0, "min": 0.0, "max": 1.0, "step": 0.35}]
    vals = sorted(c["p"] for c in expand_param_grid(meta, {"p": {"min": 0.0, "max": 1.0, "step": 0.35}}))
    assert vals == [0.0, 0.35, 0.7, 1.0]
    assert all(v <= 1.0 for v in vals)


def test_range_spec_int_includes_max_when_step_not_divisible():
    meta = [{"id": "n", "type": "int", "default": 1, "min": 1, "max": 10, "step": 4}]
    vals = sorted(c["n"] for c in expand_param_grid(meta, {"n": {"min": 1, "max": 10, "step": 4}}))
    assert vals == [1, 5, 9, 10]
    assert all(isinstance(v, int) for v in vals)
