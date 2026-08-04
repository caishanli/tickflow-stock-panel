import os

from app.quant import db
from app.quant.rqalpha_bridge import (
    QuantRQAlphaDataSource,
    _compute_trade_metrics,
    run_backtest_on_bundle,
)


def test_datasource_instantiable():
    # 仅验证抽象方法已全部实现（否则实例化抛 TypeError）
    missing = getattr(QuantRQAlphaDataSource, "__abstractmethods__", set())
    assert not missing, f"未实现的抽象方法: {missing}"


# 合法的 rqalpha 策略：首日买入并持有，用于验证数据源与回测闭环
_STRATEGY = (
    "from rqalpha import api\n"
    "def init(context):\n"
    "    context.stock = '600000.XSHG'\n"
    "\n"
    "def handle_bar(context, bar):\n"
    "    api.order_target_percent(context.stock, 1.0)\n"
)


def test_run_on_mini_bundle(tmp_path):
    db_path = str(tmp_path / "q.db")
    db.init_db(db_path)

    res = run_backtest_on_bundle(
        bundle_dir="tests/quant/fixtures/mini_bundle",
        strategy_code=_STRATEGY,
        params={"run_id": "t1", "symbols": ["600000.XSHG"], "start": "2024-01-02",
                "end": "2024-01-04", "frequency": "daily", "capital": 100000.0,
                "fee": 0.0003, "slippage": 0.001},
        db_path=db_path,
    )
    assert res["run_id"] == "t1"
    run = db.get_run("t1")
    assert run is not None, "run 未写入 db"
    assert run["status"] == "done", f"status={run['status']}, error={run.get('error')}"
    equity = db.get_equity("t1")
    assert len(equity) >= 1, "未回收净值曲线"


def test_compute_trade_metrics_tuple_trades():
    """_extract_trades 输出元组，指标必须正确统计（回归：曾因误当 dict 全为 0）。"""
    trades = [
        ("2026-07-10 13:10", "159985.XSHE", "BUY", 2.1322, 46800.0, 0.0, 0.0, 9.98),
        ("2026-07-13 13:10", "159985.XSHE", "SELL", 2.1928, 46800.0, 2836.08, 0.0284, 10.26),
        ("2026-07-14 13:10", "511880.XSHG", "SELL", 100.6019, 1000.0, -33.2, -0.0003, 10.06),
    ]
    m = _compute_trade_metrics(trades)
    assert m["trade_count"] == 2
    assert m["win_rate"] == 0.5
    assert m["profit_loss_ratio"] is not None and m["profit_loss_ratio"] > 1


def test_compute_trade_metrics_dict_trades():
    """dict 结构（side/pnl 键）仍兼容。"""
    trades = [
        {"side": "SELL", "pnl": 100.0},
        {"side": "SELL", "pnl": -50.0},
        {"side": "BUY", "pnl": 0.0},
    ]
    m = _compute_trade_metrics(trades)
    assert m["trade_count"] == 2
    assert m["win_rate"] == 0.5


def test_run_on_existing_run_row_upserts(tmp_path):
    """API 已建 'queued' 行时，脚本调用 run_backtest 不应触发 UNIQUE 冲突。"""
    db_path = str(tmp_path / "q.db")
    db.init_db(db_path)
    # 模拟 service.submit_backtest 已先插入一行
    db.insert_run("t2", "", "", "{}", "queued")

    res = run_backtest_on_bundle(
        bundle_dir="tests/quant/fixtures/mini_bundle",
        strategy_code=_STRATEGY,
        params={"run_id": "t2", "symbols": ["600000.XSHG"], "start": "2024-01-02",
                "end": "2024-01-04", "frequency": "daily", "capital": 100000.0,
                "fee": 0.0003, "slippage": 0.001},
        db_path=db_path,
    )
    assert res["run_id"] == "t2"
    run = db.get_run("t2")
    assert run["status"] == "done", f"status={run['status']}, error={run.get('error')}"
