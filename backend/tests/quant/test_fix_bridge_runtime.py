"""quant 桥接层修复的运行时测试（合成数据跑微型 rqalpha 回测）。

覆盖：
- H3 ETF 注册为 ETF 类型后卖出印花税为 0、佣金 = fee×成交额（对照 CS 股票收印花税）。
- M14 卖出成交 pnl/pnl_pct 按移动平均成本法计算。
- H4 UI 路径 context.universe 可写（_patch_rqalpha_objects 已补齐）。
- H5 日线频率下 run_daily('09:00'/'15:10') 归并后每交易日触发，且盘前回调
  先于 handle_bar、盘后回调晚于 handle_bar。
- 状态保护：run 已被外部置为 failed（terminate）时不被回写 done。
- M13 UI 路径无基准时 equity 第 3 列（benchmark 净值）统一占位 1.0。
"""
import os

import pandas as pd
import pytest

from app.quant import db
from app.quant import jqcompat as jq
from app.quant.rqalpha_bridge import run_backtest_on_bundle


def _write_bundle(tmp_path, bars: dict) -> str:
    """构造临时 bundle：{code: [(date, close), ...]}，open/high/low 由 close 派生。"""
    bdir = tmp_path / "bundle"
    os.makedirs(bdir / "bars", exist_ok=True)
    for code, rows in bars.items():
        df = pd.DataFrame({
            "date": [r[0] for r in rows],
            "open": [r[1] for r in rows],
            "high": [r[1] * 1.01 for r in rows],
            "low": [r[1] * 0.99 for r in rows],
            "close": [r[1] for r in rows],
            "volume": [100000.0] * len(rows),
        })
        df.to_csv(bdir / "bars" / f"{code}.csv", index=False)
    return str(bdir)


_DAYS = [("2024-01-02", 10.0), ("2024-01-03", 11.0), ("2024-01-04", 12.0)]

# 基准校验需要起点前一交易日的数据（rqalpha analyser 的 returns 种子日）
_PREV_DAY = ("2023-12-29", 10.0)

# 首日买 100 股、次日全卖：验证费用与 pnl
_TRADE_STRATEGY_TMPL = (
    "from rqalpha import api\n"
    "def init(context):\n"
    "    context.stock = '{code}'\n"
    "    context.i = 0\n"
    "def handle_bar(context, bar):\n"
    "    context.i += 1\n"
    "    if context.i == 1:\n"
    "        api.order(context.stock, 100)\n"
    "    elif context.i == 2:\n"
    "        api.order(context.stock, -100)\n"
)


def _run_trade(tmp_path, code, run_id):
    bundle = _write_bundle(tmp_path, {code: _DAYS})
    db_path = str(tmp_path / f"{run_id}.db")
    db.init_db(db_path)
    res = run_backtest_on_bundle(
        bundle_dir=bundle,
        strategy_code=_TRADE_STRATEGY_TMPL.format(code=code),
        params={"run_id": run_id, "symbols": [code], "start": "2024-01-02",
                "end": "2024-01-04", "frequency": "1d", "capital": 100000.0,
                "fee": 0.001, "min_commission": 0, "slippage": 0.0},
        db_path=db_path,
    )
    run = db.get_run(run_id)
    assert run["status"] == "done", f"status={run['status']}, error={run.get('error')}"
    assert "error" not in res
    return db.get_trades(run_id)


def test_h3_etf_sell_tax_zero_and_commission_proportional(tmp_path):
    trades = _run_trade(tmp_path, "510300.XSHG", "etf_tax")
    assert len(trades) == 2, f"成交数={len(trades)}: {trades}"
    buy = next(t for t in trades if "BUY" in t["action"])
    sell = next(t for t in trades if "SELL" in t["action"])
    # 佣金 = fee × 成交额 = 0.001 × (100 × 10.0) = 1.0
    assert buy["commission"] == pytest.approx(1.0, abs=1e-6)
    # ETF 卖出免印花税：费用 = 佣金 0.001 × (100 × 11.0) = 1.1（CS 会多 0.55 印花税）
    assert sell["commission"] == pytest.approx(1.1, abs=1e-6)
    # M14：移动平均成本 pnl = (11 − 10) × 100 = 100，pnl_pct = 10%
    assert sell["pnl"] == pytest.approx(100.0, abs=1e-6)
    assert sell["pnl_pct"] == pytest.approx(0.1, abs=1e-6)
    assert buy["pnl"] == 0.0


def test_h3_cs_stock_sell_still_charged_tax(tmp_path):
    trades = _run_trade(tmp_path, "600000.XSHG", "cs_tax")
    sell = next(t for t in trades if "SELL" in t["action"])
    # CS 卖出：佣金 1.1 + 印花税 0.0005×1100 = 0.55 → 1.65
    assert sell["commission"] == pytest.approx(1.65, abs=1e-6)


# H4/H5：context.universe 可写 + 日线频率 run_daily 归并触发
_H4_STRATEGY = (
    "from jqdata import *\n"
    "def init(context):\n"
    "    context.universe = ['600000.XSHG']\n"
    "    run_daily(_morning, time='09:00')\n"
    "    run_daily(_after_close, time='15:10')\n"
    "def _morning(context):\n"
    "    log.info('CB_MORNING')\n"
    "def _after_close(context):\n"
    "    log.info('CB_AFTER_CLOSE')\n"
    "def handle_bar(context, bar):\n"
    "    log.info('CB_HANDLE_BAR')\n"
)


def test_h4_h5_universe_writable_and_run_daily_fires_in_daily_mode(tmp_path):
    jq.install_jqcompat(["600000.XSHG"])  # 同时清空跨测试残留的调度回调
    bundle = _write_bundle(tmp_path, {"600000.XSHG": _DAYS})
    db_path = str(tmp_path / "h4.db")
    db.init_db(db_path)
    res = run_backtest_on_bundle(
        bundle_dir=bundle,
        strategy_code=_H4_STRATEGY,
        params={"run_id": "h4h5", "symbols": ["600000.XSHG"], "start": "2024-01-02",
                "end": "2024-01-04", "frequency": "1d", "capital": 100000.0,
                "fee": 0.0003, "slippage": 0.0},
        db_path=db_path,
    )
    run = db.get_run("h4h5")
    assert run["status"] == "done", f"status={run['status']}, error={run.get('error')}"
    logs = [r["message"] for r in db.get_logs("h4h5")]
    # 实时日志为双通道（LIVE_SINK + stdlib handler 各写一条），故每个交易日 2 条
    for marker in ("CB_MORNING", "CB_HANDLE_BAR", "CB_AFTER_CLOSE"):
        assert logs.count(marker) == 6, f"{marker} 触发次数={logs.count(marker)}: {logs}"
    # 每个交易日：盘前回调 → handle_bar → 盘后回调
    seq = [(m, i) for i, m in enumerate(logs) if m.startswith("CB_")]
    days = {"CB_MORNING": [], "CB_HANDLE_BAR": [], "CB_AFTER_CLOSE": []}
    for m, i in seq:
        days[m].append(i)
    for k in range(3):
        assert days["CB_MORNING"][k] < days["CB_HANDLE_BAR"][k] < days["CB_AFTER_CLOSE"][k]
    # M13：UI 路径无基准 → benchmark 净值列统一占位 1.0
    equity = db.get_equity("h4h5")
    assert equity and all(r["benchmark"] == 1.0 for r in equity)


# 状态保护：外部 terminate（置 failed）后子进程跑完不得回写 done
_GUARD_STRATEGY = (
    "from app.quant import db as _db\n"
    "def init(context):\n"
    "    context.done = False\n"
    "def handle_bar(context, bar):\n"
    "    if not context.done:\n"
    "        context.done = True\n"
    "        _db.update_run('guard1', 'failed', error='terminated')\n"
)


def test_status_terminal_not_overwritten_by_done(tmp_path):
    bundle = _write_bundle(tmp_path, {"600000.XSHG": _DAYS})
    db_path = str(tmp_path / "guard.db")
    db.init_db(db_path)
    res = run_backtest_on_bundle(
        bundle_dir=bundle,
        strategy_code=_GUARD_STRATEGY,
        params={"run_id": "guard1", "symbols": ["600000.XSHG"], "start": "2024-01-02",
                "end": "2024-01-04", "frequency": "1d", "capital": 100000.0,
                "fee": 0.0003, "slippage": 0.0},
        db_path=db_path,
    )
    assert res["run_id"] == "guard1"
    run = db.get_run("guard1")
    assert run["status"] == "failed", f"终态被覆盖: {run['status']}"
    assert run["error"] == "terminated"


def test_benchmark_nav_live_hook_with_configured_benchmark(tmp_path):
    """配置基准后 live 钩子写真实基准净值（回归：此前传 Instrument 对象给
    data_proxy.get_bar 抛 TypeError → 基准恒 1.0，收益曲线无基线）。"""
    # bundle 同时含策略标的与基准标的（close 10/11/12，另含起点前一交易日）
    bundle = _write_bundle(tmp_path, {
        "600000.XSHG": [_PREV_DAY] + _DAYS,
        "510300.XSHG": [_PREV_DAY] + _DAYS,
    })
    db_path = str(tmp_path / "bench.db")
    db.init_db(db_path)
    res = run_backtest_on_bundle(
        bundle_dir=bundle,
        strategy_code=_TRADE_STRATEGY_TMPL.format(code="600000.XSHG"),
        params={"run_id": "bench1", "symbols": ["600000.XSHG"], "start": "2024-01-02",
                "end": "2024-01-04", "frequency": "1d", "capital": 100000.0,
                "fee": 0.001, "min_commission": 0, "slippage": 0.0,
                "benchmark": "510300.XSHG"},
        db_path=db_path,
    )
    run = db.get_run("bench1")
    assert run["status"] == "done", f"status={run['status']}, error={run.get('error')}"
    equity = db.get_equity("bench1")
    # 基准净值 = 当日收盘 / 首日收盘 → 1.0 / 1.1 / 1.2
    assert [round(r["benchmark"], 6) for r in equity] == [1.0, 1.1, 1.2]


def test_backtest_logs_use_engine_advancing_time(tmp_path):
    """回测策略日志时间戳为引擎推进时刻（trading_dt），非真实墙钟。"""
    import datetime as _dt

    jq.install_jqcompat(["600000.XSHG"])  # 注册 jqdata.log（_Log → LIVE_SINK）
    bundle = _write_bundle(tmp_path, {"600000.XSHG": _DAYS})
    db_path = str(tmp_path / "logts.db")
    db.init_db(db_path)
    res = run_backtest_on_bundle(
        bundle_dir=bundle,
        strategy_code=_H4_STRATEGY,
        params={"run_id": "logts1", "symbols": ["600000.XSHG"], "start": "2024-01-02",
                "end": "2024-01-04", "frequency": "1d", "capital": 100000.0,
                "fee": 0.0003, "slippage": 0.0},
        db_path=db_path,
    )
    run = db.get_run("logts1")
    assert run["status"] == "done", f"status={run['status']}, error={run.get('error')}"
    logs = db.get_logs("logts1")
    engine_msgs = [l for l in logs if l["message"].startswith("CB_")]
    assert engine_msgs, "应捕获到策略日志"
    for l in engine_msgs:
        ts = _dt.datetime.strptime(l["ts"], "%Y-%m-%d %H:%M:%S")
        # 引擎推进时间落在回测窗口内（2024-01-02~04），而非提交时刻的真实时钟
        assert _dt.datetime(2024, 1, 2) <= ts <= _dt.datetime(2024, 1, 4, 23, 59, 59), (
            f"策略日志应为引擎推进时间, got {l['ts']} msg={l['message']}")
