"""backtest/strategy.py 修复回归测试。

覆盖:
- A3: 风控 override 统一语义 (0/""/None → None 关闭; 非零才钳制)
- 死代码 _run_full_simulation 已删除
- 未知信号名 logger.warning + 结果带 warnings 字段
- order_by 原始因子排序与 [0,100] score 过滤不兼容 → 忽略 score 过滤并告警
"""
from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from app.backtest.engine import SimResult
from app.backtest.strategy import StrategyBacktestConfig, StrategyBacktestService
from app.strategy.engine import StrategyDef


def _strategy(**kwargs) -> StrategyDef:
    defaults = dict(
        meta={"id": "test", "name": "test", "scoring": {}, "params": [], "limit": 100},
        basic_filter={"enabled": False},
        entry_signals=["foo"],
        exit_signals=[],
        stop_loss=None,
        trailing_stop=None,
        trailing_take_profit_activate=None,
        trailing_take_profit_drawdown=None,
        max_hold_days=None,
        alerts=[],
        filter_fn=lambda df, params: pl.lit(True),
        filter_history_fn=None,
        lookback_days=1,
        source="custom",
        file_path=None,
    )
    defaults.update(kwargs)
    return StrategyDef(**defaults)


class _StrategyEngineStub:
    def __init__(self, strategy: StrategyDef) -> None:
        self.strategy = strategy

    def get(self, strategy_id: str) -> StrategyDef:
        return self.strategy


class _RepoStub:
    def get_index_daily(self, *args, **kwargs) -> pl.DataFrame:
        return pl.DataFrame()


class _EngineStub:
    """捕获传给撮合层的 MatcherConfig。"""

    def __init__(self, panel: pl.DataFrame) -> None:
        self.panel = panel
        self.repo = _RepoStub()
        self.matcher_config = None

    def load_panel(self, symbols, start, end, columns=None, asset_type: str = "stock") -> pl.DataFrame:
        return self.panel

    def simulate_portfolio(self, panel, entries, exits, config, progress_cb=None, cancel_event=None,
                           entry_signal_ids=None, exit_signal_ids=None) -> SimResult:
        self.matcher_config = config
        return SimResult(equity_curve=[], drawdown_curve=[], trades=[], per_symbol_stats=[], stats={})


def _panel() -> pl.DataFrame:
    start = date(2024, 1, 1)
    return pl.DataFrame([
        {"symbol": "A", "name": "A", "date": start + timedelta(days=i),
         "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0,
         "volume": 100_000, "amount": 1e8,
         "signal_foo": i == 0, "signal_bar": False}
        for i in range(3)
    ]).sort(["symbol", "date"])


def _run(overrides: dict | None, strategy: StrategyDef | None = None):
    engine = _EngineStub(_panel())
    svc = StrategyBacktestService(engine=engine, strategy_engine=_StrategyEngineStub(strategy or _strategy()))
    start = date(2024, 1, 1)
    result = svc.run(StrategyBacktestConfig(
        strategy_id="test", symbols=None, start=start, end=start + timedelta(days=2),
        matching="close_t", mode="position", overrides=overrides,
    ))
    return result, engine


# ── A3: 风控 override 统一语义 ──────────────────────────────────

def test_normalize_risk_pct_zero_means_disabled():
    n = StrategyBacktestService._normalize_risk_pct
    assert n(0, 0.005, 0.5) is None
    assert n(0.0, 0.005, 0.5) is None
    assert n("0", 0.005, 0.5) is None
    assert n("", 0.005, 0.5) is None
    assert n(None, 0.005, 0.5) is None
    assert n("abc", 0.005, 0.5) is None


def test_normalize_risk_pct_nonzero_clamps():
    n = StrategyBacktestService._normalize_risk_pct
    assert n(0.05, 0.005, 0.5) == 0.05
    assert n(-0.08, 0.005, 0.5) == 0.08  # 负数取 abs (策略定义惯例 stop_loss=-0.05)
    assert n(0.001, 0.005, 0.5) == 0.005
    assert n(999, 0.005, 0.5) == 0.5


def test_override_stop_loss_zero_disables_matcher_stop():
    """override stop_loss=0 → MatcherConfig.stop_loss_pct is None。

    旧行为: 0 直接透传 → 止损线=成本价, 次日几乎必触发, 每笔白亏双边费用。"""
    result, engine = _run({"stop_loss": 0})
    assert result.error is None
    assert engine.matcher_config.stop_loss_pct is None


def test_override_zero_disables_all_risk_params():
    """take_profit/trailing_stop 等为 0 时不再被钳成最紧档, 统一为关闭。"""
    result, engine = _run({
        "take_profit": 0,
        "trailing_stop": 0,
        "trailing_take_profit_activate": 0,
        "trailing_take_profit_drawdown": 0,
    })
    assert result.error is None
    cfg = engine.matcher_config
    assert cfg.take_profit_pct is None
    assert cfg.trailing_stop_pct is None
    assert cfg.trailing_take_profit_activate_pct is None
    assert cfg.trailing_take_profit_drawdown_pct is None


def test_override_empty_string_disables_stop_loss():
    _, engine = _run({"stop_loss": ""})
    assert engine.matcher_config.stop_loss_pct is None


def test_override_stop_loss_nonzero_normalized():
    _, engine = _run({"stop_loss": 0.05})
    assert engine.matcher_config.stop_loss_pct == 0.05


def test_strategy_default_negative_stop_loss_abs_normalized():
    """策略定义惯例 stop_loss=-0.08 → 取 abs 后生效 (engine 侧本就按 abs 解释, 行为不变)。"""
    _, engine = _run(None, _strategy(stop_loss=-0.08))
    assert engine.matcher_config.stop_loss_pct == 0.08


# ── 死代码删除 ─────────────────────────────────────────────────

def test_run_full_simulation_removed():
    """_run_full_simulation 无任何调用方, 已删除 (防回潮)。"""
    assert not hasattr(StrategyBacktestService, "_run_full_simulation")


# ── 未知信号名 warning + warnings 字段 ──────────────────────────

def test_unknown_exit_signal_warns_and_surfaces_in_result():
    """exit_signals 拼错名字 → logger.warning + result.warnings, 不再静默忽略。"""
    result, _ = _run({"exit_signals": ["bar", "macd_dead_typo"]})
    assert result.error is None
    assert any("macd_dead_typo" in w for w in result.warnings)


def test_unknown_entry_signal_falls_back_to_error():
    """entry 侧信号名全错 → 沿用既有兜底报错 (区间内未产生买入信号)。"""
    result, _ = _run({"entry_signals": ["no_such_entry"]})
    assert result.error is not None
    assert "买入信号" in result.error


# ── order_by 与 score 钳制不兼容 ────────────────────────────────

def test_order_by_ignores_score_range_filter():
    """order_by 原始因子值 (如 amount ~1e8) 与 [0,100] score 过滤不兼容 → 忽略并告警。

    旧行为: score_max=100 时所有候选被 score 过滤筛掉, 回测零成交。"""
    strategy = _strategy(meta={"id": "test", "name": "test", "scoring": {},
                               "order_by": "amount", "descending": True, "params": []})
    result, engine = _run({"score_min": 10, "score_max": 100}, strategy)
    assert result.error is None
    assert engine.matcher_config.score_min is None
    assert engine.matcher_config.score_max is None
    assert any("score" in w for w in result.warnings)


def test_scoring_strategy_keeps_score_range_clamp():
    """归一化评分策略保持 [0,100] 钳制语义不变。"""
    strategy = _strategy(meta={"id": "test", "name": "test", "scoring": {"amount": 1.0},
                               "order_by": "score", "descending": True, "params": []})
    result, engine = _run({"score_min": 10, "score_max": 150}, strategy)
    assert result.error is None
    assert engine.matcher_config.score_min == 10.0
    assert engine.matcher_config.score_max == 100.0
    assert result.warnings == []
