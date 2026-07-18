"""主引擎已确认 bug 的回归测试。

覆盖:
- E1  信号归因: open_t+1 口径用成交行解析信号 ID → 归到信号日行 (idx-1);
      full 模式 entry_idx 为 numpy int64 被 polars 拒绝 → 归因恒 None。
- E3  close_t 建仓日盘中高点计入移动止损峰值 → 峰值从 entry_price 起算。
- E2  minute_fill 未来函数: 参考线取前一交易日均线值; close_t + minute_fill
      组合本质前视, 降级为日K收盘价并 warning 一次。
- NaN 成交量停牌判定: float(nan or 0)<=0 为 False 漏判 → NaN 视为停牌。
- cross_section_rank: method="random" 不可复现 → "average"。
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

import numpy as np
import polars as pl

from app.backtest.engine import BacktestEngine, MatcherConfig


def _panel(symbols: list[str], days: int, price: float = 10.0,
           overrides: dict[tuple[str, int], dict] | None = None,
           extra_cols: dict[tuple[str, int], dict] | None = None) -> pl.DataFrame:
    """构造最小日K面板; overrides 覆盖 OHLC/volume, extra_cols 覆盖信号/均线等附加列。"""
    overrides = overrides or {}
    extra_cols = extra_cols or {}
    start = date(2024, 1, 1)
    rows = []
    for sym in symbols:
        for i in range(days):
            patch = overrides.get((sym, i), {})
            row = {
                "symbol": sym,
                "name": sym,
                "date": start + timedelta(days=i),
                "open": patch.get("open", price),
                "high": patch.get("high", price),
                "low": patch.get("low", price),
                "close": patch.get("close", price),
                "volume": patch.get("volume", 100_000.0),
            }
            row.update(extra_cols.get((sym, i), {}))
            rows.append(row)
    return pl.DataFrame(rows).sort(["symbol", "date"])


def _mask(panel: pl.DataFrame, marks: set[tuple[str, int]]) -> pl.Series:
    base = date(2024, 1, 1)
    values = []
    for row in panel.select(["symbol", "date"]).iter_rows(named=True):
        values.append((row["symbol"], (row["date"] - base).days) in marks)
    return pl.Series(values, dtype=pl.Boolean)


def _engine() -> BacktestEngine:
    return BacktestEngine(repo=None)


# ---------------------------------------------------------------- E1 信号归因

def test_entry_signal_id_resolved_on_signal_day_open_t1():
    """open_t+1: 信号日 signal_x=True, 成交日为 False → 归因仍应归到 signal_x。

    修复前用成交行解析, 成交行信号列为 False → entry_signal_id 恒 None。
    """
    panel = _panel(
        ["A"], days=3,
        extra_cols={
            ("A", 0): {"signal_x": True},
            ("A", 1): {"signal_x": False},
            ("A", 2): {"signal_x": False},
        },
    )
    entries = _mask(panel, {("A", 0)})
    exits = _mask(panel, set())

    result = _engine().simulate_portfolio(
        panel, entries, exits,
        MatcherConfig(matching="open_t+1", fees_pct=0, slippage_bps=0,
                      max_positions=1, initial_capital=100_000),
        entry_signal_ids=["x"],
    )

    assert len(result.trades) == 1
    assert result.trades[0].entry_signal_id == "signal_x"


def test_entry_signal_id_not_misattributed_to_fill_day_signal():
    """open_t+1 张冠李戴场景: 信号日 signal_x=True, 成交日恰好 signal_y=True。

    修复前在成交行解析 → 错误归到 signal_y; 修复后必须归到信号日的 signal_x。
    """
    panel = _panel(
        ["A"], days=3,
        extra_cols={
            ("A", 0): {"signal_x": True, "signal_y": False},
            ("A", 1): {"signal_x": False, "signal_y": True},
            ("A", 2): {"signal_x": False, "signal_y": False},
        },
    )
    entries = _mask(panel, {("A", 0)})
    exits = _mask(panel, set())

    result = _engine().simulate_portfolio(
        panel, entries, exits,
        MatcherConfig(matching="open_t+1", fees_pct=0, slippage_bps=0,
                      max_positions=1, initial_capital=100_000),
        entry_signal_ids=["x", "y"],
    )

    assert len(result.trades) == 1
    assert result.trades[0].entry_signal_id == "signal_x"


def test_exit_signal_id_resolved_on_signal_day_open_t1():
    """open_t+1 卖出: 信号日 signal_z=True, 成交日为 False → 归因 signal_z。"""
    panel = _panel(
        ["A"], days=4,
        extra_cols={
            ("A", 2): {"signal_z": True},
        },
    )
    entries = _mask(panel, {("A", 0)})
    exits = _mask(panel, {("A", 2)})

    result = _engine().simulate_portfolio(
        panel, entries, exits,
        MatcherConfig(entry_fill="close_t", exit_fill="open_t+1",
                      fees_pct=0, slippage_bps=0,
                      max_positions=1, initial_capital=100_000),
        exit_signal_ids=["z"],
    )

    assert len(result.trades) == 1
    trade = result.trades[0]
    assert trade.exit_reason == "signal"
    assert trade.exit_signal_id == "signal_z"


def test_full_mode_entry_signal_id_close_t():
    """full 模式 close_t 建仓归因必须有值。

    修复前 entry_idx 来自 np.flatnonzero (numpy int64), polars 拒绝 numpy 整型
    索引抛 TypeError, 被 _resolve_signal_id 的 except 吞掉 → 恒 None。
    """
    panel = _panel(
        ["A"], days=3,
        extra_cols={("A", 0): {"signal_x": True}},
    )
    entries = _mask(panel, {("A", 0)})
    exits = _mask(panel, set())

    result = _engine().simulate_independent_candidates(
        panel, entries, exits,
        MatcherConfig(matching="close_t", fees_pct=0, slippage_bps=0),
        entry_signal_ids=["x"],
    )

    assert len(result.trades) == 1
    assert result.trades[0].entry_signal_id == "signal_x"


def test_full_mode_entry_signal_id_open_t1_uses_signal_day():
    """full 模式 open_t+1: 归因归到信号日 (修复前成交行解析 + numpy 索引双重 bug)。"""
    panel = _panel(
        ["A"], days=3,
        extra_cols={
            ("A", 0): {"signal_x": True},
            ("A", 1): {"signal_x": False},
        },
    )
    entries = _mask(panel, {("A", 0)})
    exits = _mask(panel, set())

    result = _engine().simulate_independent_candidates(
        panel, entries, exits,
        MatcherConfig(matching="open_t+1", fees_pct=0, slippage_bps=0),
        entry_signal_ids=["x"],
    )

    assert len(result.trades) == 1
    assert result.trades[0].entry_signal_id == "signal_x"


# ---------------------------------------------------------------- E3 移动止损峰值

def test_close_t_entry_day_high_not_counted_in_trailing_peak():
    """close_t 收盘 10.0 买入, 当日 high 11.0 发生在买入之前, 不得计入峰值。

    复现原 bug: trailing_stop_pct=0.08 → 旧峰值 11.0 给出止损线 10.12 > 成本,
    次日 open 10.05 以 trailing_stop 在成本上方离场 (盈利却叫止损)。
    修复后峰值从 10.0 起算 (止损线 9.2), 不再触发。
    """
    panel = _panel(
        ["A"], days=3,
        overrides={
            ("A", 0): {"open": 10.0, "high": 11.0, "low": 9.9, "close": 10.0},
            ("A", 1): {"open": 10.05, "high": 10.1, "low": 10.0, "close": 10.05},
            ("A", 2): {"open": 10.05, "high": 10.1, "low": 10.0, "close": 10.08},
        },
    )
    entries = _mask(panel, {("A", 0)})
    exits = _mask(panel, set())

    result = _engine().simulate_portfolio(
        panel, entries, exits,
        MatcherConfig(matching="close_t", fees_pct=0, slippage_bps=0,
                      max_positions=1, initial_capital=100_000,
                      trailing_stop_pct=0.08),
    )

    assert len(result.trades) == 1
    trade = result.trades[0]
    assert trade.exit_reason == "end"  # 不再是 trailing_stop
    assert trade.pnl_pct > 0


def test_open_t1_entry_day_high_still_counted():
    """open_t+1 当日 open 成交, 当日 high 在成交之后 → 峰值合法计入 (口径保持)。"""
    panel = _panel(
        ["A"], days=4,
        overrides={
            ("A", 1): {"open": 10.0, "high": 11.0, "low": 9.9, "close": 10.8},
            ("A", 2): {"open": 10.05, "high": 10.1, "low": 10.0, "close": 10.05},
        },
    )
    entries = _mask(panel, {("A", 0)})
    exits = _mask(panel, set())

    result = _engine().simulate_portfolio(
        panel, entries, exits,
        MatcherConfig(matching="open_t+1", fees_pct=0, slippage_bps=0,
                      max_positions=1, initial_capital=100_000,
                      trailing_stop_pct=0.08),
    )

    assert len(result.trades) == 1
    trade = result.trades[0]
    # 峰值 11.0 → 止损线 10.12, day2 open 10.05 触发 trailing_stop
    assert trade.exit_reason == "trailing_stop"
    assert trade.exit_price == 10.05


def test_full_mode_close_t_entry_day_high_not_counted():
    """full 模式同样口径: close_t 建仓当日 high 不并入峰值。"""
    panel = _panel(
        ["A"], days=3,
        overrides={
            ("A", 0): {"open": 10.0, "high": 11.0, "low": 9.9, "close": 10.0},
            ("A", 1): {"open": 10.05, "high": 10.1, "low": 10.0, "close": 10.05},
            ("A", 2): {"open": 10.05, "high": 10.1, "low": 10.0, "close": 10.08},
        },
    )
    entries = _mask(panel, {("A", 0)})
    exits = _mask(panel, set())

    result = _engine().simulate_independent_candidates(
        panel, entries, exits,
        MatcherConfig(matching="close_t", fees_pct=0, slippage_bps=0,
                      trailing_stop_pct=0.08),
    )

    assert len(result.trades) == 1
    assert result.trades[0].exit_reason == "end"


# ---------------------------------------------------------------- E2 minute_fill

class _MinuteRepo:
    """分钟K桩: get_minute_by_dates 返回预构造 DataFrame, 并记录调用次数。"""

    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df
        self.calls = 0

    def get_minute_by_dates(self, symbols, dates, asset_type="stock"):  # noqa: ANN001
        self.calls += 1
        return self._df


def _minute_df(symbol: str, day: date, o: float, h: float, l: float, c: float) -> pl.DataFrame:
    return pl.DataFrame({
        "symbol": [symbol] * 2,
        "datetime": [datetime(day.year, day.month, day.day, 9, 31),
                     datetime(day.year, day.month, day.day, 14, 57)],
        "open": [o, c],
        "high": [h, h],
        "low": [l, l],
        "close": [c, c],
        "volume": [100.0, 100.0],
        "amount": [o * 100, c * 100],
    })


def test_close_t_minute_fill_degrades_to_daily_close_with_warning(caplog):
    """close_t + minute_fill 本质前视 (信号 15:00 才确认却 9:30 成交):
    降级为日K收盘价, warning 一次, 且不加载分钟数据。"""
    day0, day1 = date(2024, 1, 1), date(2024, 1, 2)
    panel = _panel(
        ["A"], days=2,
        overrides={
            ("A", 0): {"open": 10.5, "high": 12.1, "low": 10.4, "close": 12.0},
            ("A", 1): {"open": 12.0, "high": 12.2, "low": 11.9, "close": 12.1},
        },
        extra_cols={("A", 0): {"ma5": 10.0}, ("A", 1): {"ma5": 11.0}},
    )
    repo = _MinuteRepo(_minute_df("A", day0, 10.5, 12.1, 10.4, 12.0))
    entries = _mask(panel, {("A", 0)})
    exits = _mask(panel, set())

    with caplog.at_level(logging.WARNING, logger="app.backtest.engine"):
        result = BacktestEngine(repo=repo).simulate_portfolio(
            panel, entries, exits,
            MatcherConfig(matching="close_t", fees_pct=0, slippage_bps=0,
                          max_positions=1, initial_capital=100_000,
                          minute_fill=True),
        )

    assert len(result.trades) == 1
    # 修复前成交价 = 当日分钟开盘价 10.5 (前视); 修复后 = 日K收盘 12.0
    assert result.trades[0].entry_price == 12.0
    assert repo.calls == 0  # close_t 腿不读分钟K
    warns = [r for r in caplog.records if "minute_fill" in r.message]
    assert len(warns) == 1  # 每次回测只 warning 一次


def test_open_t1_minute_fill_uses_prev_day_ref_line():
    """open_t+1 + minute_fill: 参考线取前一交易日 (信号日) 均线值。

    信号日 ma5=10.0, 成交日 ma5=11.0; 分钟K 高点 10.05 穿越 10.0 但不触及 11.0。
    修复前用成交行 ma5=11.0 → 不穿越 → 按分钟收盘 9.95 成交;
    修复后用前一日 ma5=10.0 → 穿越 → 按参考线 10.0 成交。
    """
    day1 = date(2024, 1, 2)
    panel = _panel(
        ["A"], days=3,
        overrides={
            ("A", 0): {"open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0},
            ("A", 1): {"open": 9.9, "high": 10.05, "low": 9.8, "close": 9.95},
            ("A", 2): {"open": 9.95, "high": 10.0, "low": 9.9, "close": 9.98},
        },
        extra_cols={
            ("A", 0): {"ma5": 10.0},   # 信号日参考线 (T 日已知, 无前视)
            ("A", 1): {"ma5": 11.0},   # 成交日均线含当日收盘, 不得使用
            ("A", 2): {"ma5": 11.0},
        },
    )
    repo = _MinuteRepo(_minute_df("A", day1, 9.9, 10.05, 9.8, 9.95))
    entries = _mask(panel, {("A", 0)})
    exits = _mask(panel, set())

    result = BacktestEngine(repo=repo).simulate_portfolio(
        panel, entries, exits,
        MatcherConfig(entry_fill="open_t+1", exit_fill="close_t",
                      fees_pct=0, slippage_bps=0,
                      max_positions=1, initial_capital=100_000,
                      minute_fill=True),
    )

    assert len(result.trades) == 1
    assert repo.calls == 1  # open_t+1 腿正常加载分钟K
    assert result.trades[0].entry_price == 10.0


# ---------------------------------------------------------------- NaN 成交量停牌

def test_nan_volume_same_price_bar_counts_as_suspended():
    """volume=NaN 且 OHLC 同价 = 停牌, 不可成交。

    修复前 float(nan or 0)<=0 为 False → 漏判停牌, 信号可成交。
    """
    panel = _panel(
        ["A"], days=2,
        overrides={
            ("A", 0): {"open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0,
                       "volume": float("nan")},
        },
    )
    entries = _mask(panel, {("A", 0)})
    exits = _mask(panel, set())

    result = _engine().simulate_portfolio(
        panel, entries, exits,
        MatcherConfig(matching="close_t", fees_pct=0, slippage_bps=0,
                      max_positions=1, initial_capital=100_000),
    )

    assert result.trades == []
    assert result.stats["execution"]["buy_suspended"] == 1


def test_nan_volume_suspended_full_mode():
    """full 模式同一判定: NaN 成交量 + 同价 bar → buy_suspended。"""
    panel = _panel(
        ["A"], days=2,
        overrides={
            ("A", 0): {"open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0,
                       "volume": float("nan")},
        },
    )
    entries = _mask(panel, {("A", 0)})
    exits = _mask(panel, set())

    result = _engine().simulate_independent_candidates(
        panel, entries, exits,
        MatcherConfig(matching="close_t", fees_pct=0, slippage_bps=0),
    )

    assert result.trades == []
    assert result.stats["execution"]["buy_suspended"] == 1


# ---------------------------------------------------------------- cross_section_rank

def test_cross_section_rank_is_deterministic_and_ties_averaged():
    """method="average": 并列取平均名次, 两次调用结果一致 (原 "random" 不可复现)。"""
    panel = pl.DataFrame({
        "symbol": ["A", "B", "C"],
        "date": [date(2024, 1, 1)] * 3,
        "f": [5.0, 5.0, 1.0],
    })
    r1 = BacktestEngine.cross_section_rank(panel, "f")["f_rank"].to_list()
    r2 = BacktestEngine.cross_section_rank(panel, "f")["f_rank"].to_list()
    assert r1 == r2
    assert r1 == [2.5, 2.5, 1.0]  # 两个 5.0 并列第 2/3 名, 各取 2.5
