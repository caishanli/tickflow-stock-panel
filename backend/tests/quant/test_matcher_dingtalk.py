"""Matcher on_stop_loss 回调测试。"""
from app.quant.simulate.matcher import Matcher


def _state(price=9.6, avg_cost=10.0, amount=100.0):
    return {
        "cash": 0.0, "start_cash": 1000.0, "net_value": 1000.0, "pnl": 0.0,
        "positions": {"600000.XSHG": {"amount": amount, "avg_cost": avg_cost, "price": price}},
        "stop_loss_log": [],
    }


def test_on_stop_loss_fires_with_full_info():
    records = []
    m = Matcher(0.03, on_stop_loss=records.append)
    m.step(_state(), {"600000.XSHG": 9.6})
    assert len(records) == 1
    rec = records[0]
    assert rec["code"] == "600000.XSHG"
    assert rec["action"] == "STOP_LOSS"
    assert rec["amount"] == 100.0
    assert rec["price"] > 0 and rec["price"] < 9.6          # 含滑点
    assert rec["pnl"] < 0
    assert rec["pnl_pct"] == -0.04                          # 9.6/10-1
    assert rec["commission"] >= 0
    assert isinstance(rec["name"], str) and rec["name"]


def test_no_callback_when_above_stop():
    records = []
    m = Matcher(0.03, on_stop_loss=records.append)
    m.step(_state(price=11.0), {"600000.XSHG": 11.0})
    assert records == []


def test_no_callback_when_no_sell():
    records = []
    m = Matcher(0.03, on_stop_loss=records.append)
    m.step(_state(price=9.6), {"600000.XSHG": 9.6}, no_sell={"600000.XSHG"})
    assert records == []


def test_callback_optional():
    m = Matcher(0.03)
    out = m.step(_state(), {"600000.XSHG": 9.6})
    assert "600000.XSHG" not in out["positions"]
