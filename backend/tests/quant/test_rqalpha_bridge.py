import os

from app.quant import db
from app.quant.rqalpha_bridge import QuantRQAlphaDataSource, run_backtest_on_bundle


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
