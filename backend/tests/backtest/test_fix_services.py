"""services/backtest.py 修复回归测试。

覆盖:
- A2: max_hold_days 强制退出矩阵 (旧实现同 bar entry+exit 双丢 → 零成交)
- A4: _load_panel 指标预热 (MA60 类信号在区间开头静默缺失)
- SignalKind 未实现 kind 抛 ValueError (旧行为静默跳过)
- _persist: json.dumps 落盘 + 写盘失败不影响结果返回
- NaN Return: 期末未平仓交易 NaN/inf 清洗为 None
"""
from __future__ import annotations

import json
import types
from dataclasses import asdict
from datetime import date, timedelta

import numpy as np
import pandas as pd
import polars as pl
import pytest

pytest.importorskip("vectorbt")

from app.services.backtest import BacktestConfig, BacktestService


def _panel(n: int, entry_bars: set[int], closes: list[float] | None = None,
           exit_bars: set[int] | None = None, start: date = date(2024, 1, 1)) -> pd.DataFrame:
    """合成单日标的面板: 平票价 + 指定 bar 的 macd 金/死叉信号。"""
    closes = closes or [10.0] * n
    return pd.DataFrame({
        "date": [start + timedelta(days=i) for i in range(n)],
        "symbol": ["A"] * n,
        "open": closes,
        "high": closes,
        "low": closes,
        "close": closes,
        "volume": [1000.0] * n,
        "rsi_14": [50.0] * n,
        "signal_macd_golden": [i in entry_bars for i in range(n)],
        "signal_macd_dead": [i in (exit_bars or set()) for i in range(n)],
    })


def _svc(panel: pd.DataFrame) -> BacktestService:
    """_load_panel 走合成面板, _persist 不落盘。"""
    svc = BacktestService(types.SimpleNamespace())
    svc._load_panel = lambda *a, **k: panel  # type: ignore[method-assign]
    svc._persist = lambda r: None  # type: ignore[method-assign]
    return svc


def _config(n: int, **kwargs) -> BacktestConfig:
    d0 = date(2024, 1, 1)
    defaults = dict(
        symbols=["A"], start=d0, end=d0 + timedelta(days=n - 1),
        entries=["macd_golden"], exits=[], fees_pct=0, slippage_bps=0,
    )
    defaults.update(kwargs)
    return BacktestConfig(**defaults)


def _hold_days(trade: dict) -> int:
    entry = date.fromisoformat(trade["entry_date"][:10])
    exit_ = date.fromisoformat(trade["exit_date"][:10])
    return (exit_ - entry).days


# ── A2: max_hold_days 强制退出矩阵 ──────────────────────────────

def test_max_hold_produces_trade_with_expected_hold_days():
    """信号连续 3 天为 True + max_hold=10 → 有成交且持仓约 10 天。

    旧实现 exits_idx = entries.copy() 使每个信号 bar 同 bar entry+exit 双丢 → 零成交。"""
    panel = _panel(40, entry_bars={10, 11, 12})
    result = _svc(panel).run(_config(40, max_hold_days=10))

    assert result.stats.get("error") is None
    assert len(result.trades) == 1
    assert _hold_days(result.trades[0]) == 10


def test_max_hold_chains_entries_after_forced_exit():
    """信号连续 35 天: 迭代逼近应对每笔实际建仓补退出点 → 0→10, 11→21, 22→32, 33→34。"""
    panel = _panel(35, entry_bars=set(range(35)))
    result = _svc(panel).run(_config(35, max_hold_days=10))

    assert result.stats.get("error") is None
    assert [_hold_days(t) for t in result.trades] == [10, 10, 10, 1]


def test_max_hold_signal_exit_takes_priority():
    """持仓期内出现卖出信号 → 信号优先, 不等 max_hold 到期。"""
    panel = _panel(40, entry_bars={10, 11, 12}, exit_bars={15})
    result = _svc(panel).run(_config(40, exits=["macd_dead"], max_hold_days=10))

    assert len(result.trades) == 1
    assert _hold_days(result.trades[0]) == 5


def test_max_hold_zero_disables():
    """max_hold_days=0 视为关闭 (旧实现 0 也会触发 exits_idx=entries 零成交陷阱)。"""
    panel = _panel(40, entry_bars={10}, exit_bars={15})
    result = _svc(panel).run(_config(40, exits=["macd_dead"], max_hold_days=0))

    assert len(result.trades) == 1
    assert _hold_days(result.trades[0]) == 5


# ── A4: _load_panel 指标预热 ────────────────────────────────────

def test_load_panel_warmup_keeps_indicators_valid_at_range_start(monkeypatch, tmp_path):
    """加载窗口前扩 120 日历日预热 → 正式区间开头 MA60 类信号/RSI 不为 null。"""
    n, warmup_gap = 200, 140
    d0 = date(2024, 1, 1)
    lf = pl.LazyFrame({
        "symbol": ["A"] * n,
        "date": [d0 + timedelta(days=i) for i in range(n)],
        "open": [100.0 + i for i in range(n)],
        "high": [100.0 + i for i in range(n)],
        "low": [100.0 + i for i in range(n)],
        "close": [100.0 + i for i in range(n)],
        "volume": [1000.0] * n,
        "amount": [10000.0] * n,
    })
    repo = types.SimpleNamespace(store=types.SimpleNamespace(data_dir=tmp_path))
    svc = BacktestService(repo)

    # 正式区间只取最后 60 根: 无预热时 ma60 整段为 null, rsi_14 前 14 根为 null
    start = d0 + timedelta(days=warmup_gap)
    end = d0 + timedelta(days=n - 1)
    panel = svc._load_panel(["A"], start, end)

    assert not panel.empty
    assert panel["date"].min().date() == start  # 预热段已裁掉, 不回漏到结果
    assert panel["rsi_14"].notna().all()
    assert panel["signal_ma_golden_20_60"].notna().all()
    assert panel["signal_ma20_breakout"].notna().all()


# ── SignalKind 未实现 kind 抛 ValueError ────────────────────────

@pytest.mark.parametrize("kind, hint", [
    ("stop_loss", "stop_loss_pct"),
    ("max_hold", "max_hold_days"),
    ("trailing_stop", "暂未实现"),
])
def test_risk_kinds_raise_with_param_hint(kind, hint):
    svc = BacktestService(types.SimpleNamespace())
    with pytest.raises(ValueError, match=hint):
        svc.run(_config(10, entries=[kind]))


def test_unknown_kind_raises():
    svc = BacktestService(types.SimpleNamespace())
    with pytest.raises(ValueError, match="未知信号"):
        svc.run(_config(10, exits=["no_such_signal"]))


# ── _persist 健壮性 ────────────────────────────────────────────

def test_persist_writes_valid_json_stats(monkeypatch, tmp_path):
    """stats 用 json.dumps 落盘 (旧实现 str(repr) 不是合法 JSON)。"""
    monkeypatch.setattr("app.services.backtest.settings", types.SimpleNamespace(data_dir=tmp_path))
    panel = _panel(20, entry_bars={5}, exit_bars={10})
    svc = BacktestService(types.SimpleNamespace())
    svc._load_panel = lambda *a, **k: panel  # type: ignore[method-assign]

    result = svc.run(_config(20, exits=["macd_dead"]))

    files = list((tmp_path / "backtest_results").glob("*.parquet"))
    assert len(files) == 1
    row = pl.read_parquet(files[0]).row(0, named=True)
    assert row["run_id"] == result.run_id
    parsed = json.loads(row["stats_json"])  # 必须是合法 JSON
    assert isinstance(parsed, dict)
    assert row["n_trades"] == 1


def test_persist_failure_does_not_break_run(monkeypatch, tmp_path):
    """写盘失败只记日志, 已算完的回测正常返回 (不 500)。"""
    blocker = tmp_path / "blocker"
    blocker.write_text("not a dir")  # mkdir(parents=True) 必失败
    monkeypatch.setattr("app.services.backtest.settings", types.SimpleNamespace(data_dir=blocker))
    panel = _panel(20, entry_bars={5}, exit_bars={10})
    svc = BacktestService(types.SimpleNamespace())
    svc._load_panel = lambda *a, **k: panel  # type: ignore[method-assign]

    result = svc.run(_config(20, exits=["macd_dead"]))

    assert result.stats.get("error") is None
    assert len(result.trades) == 1


# ── NaN Return 序列化 ──────────────────────────────────────────

def test_open_trade_nan_return_sanitized_to_none():
    """期末未平仓 + 尾部 NaN 价 → Return 为 NaN, 应清洗为 None 且整体可严格 JSON 序列化。"""
    n = 20
    closes = [10.0] * (n - 1) + [np.nan]
    panel = _panel(n, entry_bars={0}, closes=closes)
    result = _svc(panel).run(_config(n))

    assert len(result.trades) == 1
    assert result.trades[0]["pnl_pct"] is None
    assert result.trades[0]["exit_price"] is None
    # allow_nan=False: 有任何 NaN/inf token 都会抛错
    json.dumps(asdict(result), allow_nan=False, default=str)
