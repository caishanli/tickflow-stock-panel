"""引擎/API 审查收尾修复的回归测试。

覆盖:
- E4  minute_fill asset_type 传递: MatcherConfig 显式 > 同线程 load_panel 记录 >
      启发式兜底(告警); 深市 ETF (159915.SZ) 不再因启发式猜错 asset_type 而
      读错分钟K目录、静默降级日K。
- E5  全量模式 sharpe/sortino: 日收益序列补齐 [首次退出, 末次退出] 内全部
      交易日 (无退出日记 0), 稀疏退出不再因 ×sqrt(252) 年化虚高。
- Task6 optimize/stream 补 minute_fill 门控 (Pro+ 权限 + 分钟K数据覆盖),
      与 strategy/stream 共用 _check_minute_fill_guard, 失败走 _fail_job。
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

import numpy as np
import polars as pl
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import backtest as api
from app.api.backtest import _running_jobs
from app.backtest.engine import BacktestEngine, MatcherConfig, TradeRecord


def _panel(symbols: list[str], days: int, price: float = 10.0) -> pl.DataFrame:
    """构造最小日K面板 (与 test_fix_engine 同款)。"""
    start = date(2024, 1, 1)
    rows = []
    for sym in symbols:
        for i in range(days):
            rows.append({
                "symbol": sym, "name": sym,
                "date": start + timedelta(days=i),
                "open": price, "high": price, "low": price, "close": price,
                "volume": 100_000.0,
            })
    return pl.DataFrame(rows).sort(["symbol", "date"])


def _mask(panel: pl.DataFrame, marks: set[tuple[str, int]]) -> pl.Series:
    base = date(2024, 1, 1)
    values = []
    for row in panel.select(["symbol", "date"]).iter_rows(named=True):
        values.append((row["symbol"], (row["date"] - base).days) in marks)
    return pl.Series(values, dtype=pl.Boolean)


# ---------------------------------------------------------------- E4 minute_fill asset_type

class _RecMinuteRepo:
    """分钟K桩: 记录 get_minute_by_dates 收到的 asset_type。"""

    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df
        self.seen_asset_types: list[str] = []

    def get_minute_by_dates(self, symbols, dates, asset_type="stock"):  # noqa: ANN001
        self.seen_asset_types.append(asset_type)
        return self._df


def _minute_df(symbol: str, day: date) -> pl.DataFrame:
    return pl.DataFrame({
        "symbol": [symbol] * 2,
        "datetime": [datetime(day.year, day.month, day.day, 9, 31),
                     datetime(day.year, day.month, day.day, 14, 57)],
        "open": [10.0, 10.1], "high": [10.2, 10.2],
        "low": [9.9, 10.0], "close": [10.1, 10.1],
        "volume": [100.0, 100.0], "amount": [1000.0, 1010.0],
    })


_SZ_ETF = "159915.SZ"  # 深市 ETF: 旧启发式 (".SH"+5开头) 必猜成 stock


def _open_t1_config(**kw) -> MatcherConfig:
    return MatcherConfig(entry_fill="open_t+1", exit_fill="close_t",
                         fees_pct=0, slippage_bps=0,
                         max_positions=1, initial_capital=100_000,
                         minute_fill=True, **kw)


def test_e4_sz_etf_minute_fill_uses_config_asset_type():
    """组合模式: 显式 asset_type="etf" 直达 get_minute_by_dates, 不再静默降级日K。"""
    panel = _panel([_SZ_ETF], days=3)
    repo = _RecMinuteRepo(_minute_df(_SZ_ETF, date(2024, 1, 2)))
    entries = _mask(panel, {(_SZ_ETF, 0)})
    exits = _mask(panel, set())

    BacktestEngine(repo=repo).simulate_portfolio(
        panel, entries, exits, _open_t1_config(asset_type="etf"))

    assert repo.seen_asset_types == ["etf"]


def test_e4_full_mode_uses_config_asset_type():
    """全量模式此前硬编码 "stock": 显式 etf 后 get_minute_by_dates 必须收到 etf。"""
    panel = _panel([_SZ_ETF], days=3)
    repo = _RecMinuteRepo(_minute_df(_SZ_ETF, date(2024, 1, 2)))
    entries = _mask(panel, {(_SZ_ETF, 0)})
    exits = _mask(panel, set())

    BacktestEngine(repo=repo).simulate_independent_candidates(
        panel, entries, exits, _open_t1_config(asset_type="etf"))

    assert repo.seen_asset_types == ["etf"]


def test_e4_asset_type_falls_back_to_load_panel_record():
    """配置未显式传 asset_type 时, 用同线程 load_panel 的记录兜底。

    生产链路: strategy.py 以 config.asset_type 调 load_panel 后再撮合,
    panel 与分钟K分区同源, 不需要启发式猜测。
    """
    panel = _panel([_SZ_ETF], days=3)
    repo = _RecMinuteRepo(_minute_df(_SZ_ETF, date(2024, 1, 2)))
    engine = BacktestEngine(repo=repo)
    engine._panel_ctx.asset_type = "etf"  # 模拟 load_panel(asset_type="etf") 的记录
    entries = _mask(panel, {(_SZ_ETF, 0)})
    exits = _mask(panel, set())

    engine.simulate_portfolio(panel, entries, exits, _open_t1_config())

    assert repo.seen_asset_types == ["etf"]


def test_e4_heuristic_fallback_warns(caplog):
    """配置与 load_panel 记录都缺失时才走启发式, 且必须 warning (不再静默)。"""
    engine = BacktestEngine(repo=None)
    with caplog.at_level(logging.WARNING, logger="app.backtest.engine"):
        at = engine._resolve_minute_asset_type(MatcherConfig(), [_SZ_ETF])
    assert at == "stock"  # 深市 ETF 按旧启发式必猜错 → 所以必须显式告警
    warns = [r for r in caplog.records if "asset_type" in r.message]
    assert len(warns) == 1


def test_e4_explicit_config_wins_over_panel_record():
    """优先级: MatcherConfig 显式值 > load_panel 记录。"""
    engine = BacktestEngine(repo=None)
    engine._panel_ctx.asset_type = "etf"
    at = engine._resolve_minute_asset_type(MatcherConfig(asset_type="stock"), [_SZ_ETF])
    assert at == "stock"


# ---------------------------------------------------------------- E5 全量模式 sharpe 补零

def _trade(sym: str, exit_date: date, pnl: float) -> TradeRecord:
    return TradeRecord(
        symbol=sym, entry_date=date(2024, 1, 1), exit_date=exit_date,
        entry_price=10.0, exit_price=10.0 * (1 + pnl), pnl_pct=pnl,
        duration=3, exit_reason="signal",
    )


def test_e5_sparse_exits_sharpe_not_inflated():
    """稀疏退出: 15 个交易日仅 3 天有退出, sharpe 按补零序列计算不再虚高。

    修复前 daily_avg 只含 3 个退出日再 ×sqrt(252) 年化 → 虚高约 sqrt(15/3) 倍。
    """
    trading_dates = [(date(2024, 1, 1) + timedelta(days=i)).isoformat() for i in range(15)]
    trades = [
        _trade("A", date(2024, 1, 1), 0.01),
        _trade("B", date(2024, 1, 8), 0.02),
        _trade("C", date(2024, 1, 15), -0.005),
    ]
    res = BacktestEngine._calc_independent_candidate_result(
        trades, n_candidates=3, execution_stats={}, trading_dates=trading_dates)

    padded = np.array([0.01, 0, 0, 0, 0, 0, 0, 0.02, 0, 0, 0, 0, 0, 0, -0.005])
    expected = float(np.mean(padded) / np.std(padded) * np.sqrt(252))
    legacy = np.array([0.01, 0.02, -0.005])  # 旧口径: 仅有退出的日子
    legacy_sharpe = float(np.mean(legacy) / np.std(legacy) * np.sqrt(252))

    assert res.stats["sharpe"] == round(expected, 2)
    assert res.stats["sharpe"] < round(legacy_sharpe, 2)  # 不再虚高
    # sortino 同一补零序列
    assert res.stats["sortino"] == round(
        BacktestEngine._sortino_ratio(padded), 2)


def test_e5_without_trading_dates_keeps_legacy_caliber():
    """未传 trading_dates 时保持旧口径 (向后兼容直接调用方)。"""
    trades = [
        _trade("A", date(2024, 1, 1), 0.01),
        _trade("B", date(2024, 1, 8), 0.02),
        _trade("C", date(2024, 1, 15), -0.005),
    ]
    res = BacktestEngine._calc_independent_candidate_result(
        trades, n_candidates=3, execution_stats={})
    legacy = np.array([0.01, 0.02, -0.005])
    assert res.stats["sharpe"] == round(
        float(np.mean(legacy) / np.std(legacy) * np.sqrt(252)), 2)


# ---------------------------------------------------------------- Task6 optimize minute_fill 门控

@pytest.fixture(autouse=True)
def _clean_jobs():
    """每个测试前后清空模块级任务表, 避免用例间互相污染。"""
    _running_jobs.clear()
    yield
    _running_jobs.clear()


class _FakeRepo:
    """最小 repo: 只实现端点用到的日期探测方法。"""

    def __init__(self, earliest_minute=None):
        self._earliest_minute = earliest_minute

    def earliest_daily_date(self):
        return date(2020, 1, 1)

    def earliest_minute_date(self):
        return self._earliest_minute


class _FakeCaps:
    def __init__(self, allowed: bool = True):
        self._allowed = allowed

    def has(self, cap):
        return self._allowed


def _make_app(caps: bool = True, earliest_minute=None) -> FastAPI:
    app = FastAPI()
    app.include_router(api.router)
    app.state.repo = _FakeRepo(earliest_minute)
    # 预设 dummy engine, 避免 _get_engine 构造真 BacktestEngine
    app.state.backtest_engine = object()
    app.state.strategy_engine = object()
    app.state.capabilities = _FakeCaps(caps)
    return app


def test_optimize_minute_fill_cap_denied_marks_job_done():
    """Task6: optimize/stream 无 Pro+ 权限时与 strategy 侧同口径拒绝, 且 job 收尾。"""
    client = TestClient(_make_app(caps=False))
    r = client.get("/api/backtest/optimize/stream", params={
        "strategy_id": "s1", "param_grid": '{"p": [1, 2]}',
        "start": "2025-01-01", "end": "2025-02-01", "minute_fill": "true",
    })
    assert "Pro+" in r.text
    job = next(iter(_running_jobs.values()))
    assert job.done is True
    assert "Pro+" in job.error


def test_optimize_minute_fill_data_not_covering_marks_job_done():
    """Task6: 本地分钟K历史覆盖不足时同样走 _fail_job (此前 optimize 侧无检查)。"""
    client = TestClient(_make_app(caps=True, earliest_minute=date(2025, 6, 1)))
    r = client.get("/api/backtest/optimize/stream", params={
        "strategy_id": "s1", "param_grid": '{"p": [1, 2]}',
        "start": "2025-01-01", "end": "2025-02-01", "minute_fill": "true",
    })
    assert "分钟K历史" in r.text
    job = next(iter(_running_jobs.values()))
    assert job.done is True
    assert "分钟K历史" in job.error


def test_optimize_minute_fill_guard_passes_when_allowed():
    """Task6: 权限具备且数据覆盖时门控放行 (guard 返回 None, 不误伤正常路径)。"""
    req = type("Req", (), {"app": _make_app(caps=True, earliest_minute=date(2020, 1, 1))})()
    assert api._check_minute_fill_guard(req, date(2025, 1, 1)) is None
