"""首板滚动窗口对重复 yesterday 幂等（国庆假日重跑回归）。

纯逻辑测试：从 fixture 策略文件提取 `l0_prepare_stock_list`（及批量切分
helper `_hl_from_frame`）源码并 exec，fake g/context + stub
`get_trade_days`/`l0_get_hl_stock`/`get_price`，不起引擎、不碰 DB。
`pandas` 仅用于构造 canned 日线帧（被测函数做 frame 切分），不连网络。
"""
from __future__ import annotations

import ast
import datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

FIXTURE = (
    Path(__file__).resolve().parent.parent
    / "fixtures"
    / "shoubangaokai"
    / "strategy.py"
)

SEP28 = datetime.date(2026, 9, 28)
SEP29 = datetime.date(2026, 9, 29)
SEP30 = datetime.date(2026, 9, 30)

HL28 = ["STOCK_Z"]
HL29 = ["STOCK_A", "STOCK_B"]
HL30 = ["STOCK_B", "STOCK_C"]
EXPECTED_POOL = ["STOCK_C"]  # hl30 - hl29


def _load_prepare_fn(ns):
    src = FIXTURE.read_text(encoding="utf-8")
    mod = ast.parse(src)
    for name in ("l0_prepare_stock_list", "_hl_from_frame"):
        node = next(
            n
            for n in mod.body
            if isinstance(n, ast.FunctionDef) and n.name == name
        )
        fn_src = ast.get_source_segment(src, node)
        assert fn_src, f"fixture 缺少 {name}"
        exec(compile(fn_src, str(FIXTURE), "exec"), ns)
    return ns["l0_prepare_stock_list"]


def _hl_frame():
    """3 天 canned 日线（列仿真实 batch 输出：code/time/close/high_limit/paused）。"""
    rows = []

    def bar(code, day, close, high_limit, paused=0):
        rows.append({"code": code, "time": pd.Timestamp(day),
                     "close": close, "high_limit": high_limit, "paused": paused})

    bar("STOCK_Z", SEP28, 10.0, 10.0)
    bar("STOCK_A", SEP29, 10.0, 10.0)
    bar("STOCK_B", SEP29, 10.0, 10.0)
    bar("STOCK_B", SEP30, 11.0, 11.0)
    bar("STOCK_C", SEP30, 11.0, 11.0)
    return pd.DataFrame(rows)


def _make_ns():
    hl_by_day = {SEP28: list(HL28), SEP29: list(HL29), SEP30: list(HL30)}
    g = SimpleNamespace(l0=SimpleNamespace(n_days_limit_up_list=[]))
    log_lines: list = []
    log = SimpleNamespace(
        info=lambda msg, *a, **k: log_lines.append(("info", msg)),
        warning=lambda msg, *a, **k: log_lines.append(("warning", msg)),
    )
    frame = _hl_frame()
    ns = {
        "g": g,
        "log": log,
        "pd": pd,
        "get_trade_days": lambda end_date=None, count=None: [SEP28, SEP29, SEP30],
        "get_price": lambda *a, **k: frame,
        "l0_get_hl_stock": lambda stock_list, day, n=1: list(hl_by_day[day]),
        "l0_set_stockpool": lambda ctx: ["STOCK_A", "STOCK_B", "STOCK_C", "STOCK_Z"],
        "l0_filter_kcbj_stock": lambda lst: lst,
        "l0_filter_st_paused_stock": lambda lst, today: lst,
        "l0_filter_new_stock": lambda lst, today: lst,
    }
    return g, ns, log_lines


def _make_context():
    return SimpleNamespace(
        current_dt=datetime.datetime(2026, 10, 8, 9, 30),
        previous_date=SEP30,
    )


def test_repeat_yesterday_does_not_evict_window():
    # seed window [hl29, hl30] 后连续调 yesterday=Sep30 三次，
    # pool 应始终 == hl30-hl29 且窗口不变
    g, ns, log_lines = _make_ns()
    prepare = _load_prepare_fn(ns)
    ctx = _make_context()

    pool1 = prepare(ctx)
    assert sorted(pool1) == sorted(EXPECTED_POOL)
    window_after_first = [sorted(w) for w in g.l0.n_days_limit_up_list]
    assert window_after_first == [sorted(HL29), sorted(HL30)]

    pool2 = prepare(ctx)
    pool3 = prepare(ctx)
    assert sorted(pool2) == sorted(EXPECTED_POOL)
    assert sorted(pool3) == sorted(EXPECTED_POOL)
    window_after_repeats = [sorted(w) for w in g.l0.n_days_limit_up_list]
    assert window_after_repeats == window_after_first
    assert len(g.l0.n_days_limit_up_list) == 2
    # 基础股票池分阶段统计走 log 通道（页面日志可见），而非 print（文件缓冲、
    # SIGTERM 可丢——2026-10-12 sb_gaokai_sim 盘前无输出误判的根因之一）。
    # 四阶段合并为一行，避免每天刷 4 条。
    stages = [m for lv, m in log_lines if lv == "info" and m.startswith("【基础股票池】")]
    assert len(stages) == 3, stages
    assert stages[0] == "【基础股票池】全市场4只 → 板块4只 → ST停牌4只 → 新股4只"
