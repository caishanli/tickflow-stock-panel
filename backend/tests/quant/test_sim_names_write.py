"""模拟盘落库写名称测试：成交带 name、持仓带 name。"""
from __future__ import annotations

import pytest

from app.quant import db
from app.quant.config import CONFIG
from app.quant.simulate import names, runner


@pytest.fixture
def tmp_quant(tmp_path, monkeypatch):
    db_path = tmp_path / "quant.db"
    monkeypatch.setattr(CONFIG, "db_path", str(db_path))
    monkeypatch.setattr(CONFIG, "runtime_dir", str(tmp_path / "quant_sim"))
    db.init_db(str(db_path))
    return tmp_path


def test_persist_trade_row_carries_name(tmp_quant, monkeypatch):
    """_persist 构造的 trade_row 第 4 位（name）来自 names.resolve_name。"""
    monkeypatch.setattr(names, "get_name_map",
                        lambda: {"159985": "豆粕ETF华夏"})
    class _Api:
        _state = {"trades": [
            {"dt": "2024-01-02 09:31", "code": "159985.XSHE",
             "amount": 100, "price": 2.139, "fee": 9.99},
        ]}
    aux = {"trades_drained": 0, "replay_mode": False}
    state = {"start_cash": 100000.0}
    ctx = _FakeCtx()
    runner._persist("a1", ctx, state, "2024-01-02 09:31", _Api(), aux)
    trades = db.get_sim_trades("a1")
    assert len(trades) == 1
    assert trades[0]["code"] == "159985.XSHE"
    assert trades[0]["name"] == "豆粕ETF华夏"


def test_state_from_portfolio_positions_carry_name(tmp_quant, monkeypatch):
    monkeypatch.setattr(names, "get_name_map",
                        lambda: {"511880": "银华日利ETF"})
    class _P:
        def __init__(self, amount, avg_cost, price):
            self.amount = amount; self.avg_cost = avg_cost
            self.price = price; self.today_amount = 0.0
    class _Ctx:
        portfolio = type("PF", (), {
            "positions": {"511880.XSHG": _P(1000, 1.0, 1.1)},
            "cash": 100000.0,
        })()
    state = {}
    runner._state_from_portfolio(_Ctx(), state)
    assert state["positions"]["511880.XSHG"]["name"] == "银华日利ETF"


class _FakeCtx:
    """最小 ctx：_persist 只读 portfolio.cash/positions。"""
    portfolio = type("PF", (), {"cash": 100000.0, "positions": {}})()
