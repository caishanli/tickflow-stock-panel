from app.quant.simulate.matcher import Matcher


def test_stop_loss_triggers():
    m = Matcher(0.03)
    state = {
        "cash": 0.0, "start_cash": 1000.0, "net_value": 1000.0, "pnl": 0.0,
        "positions": {"600000.XSHG": {"amount": 100.0, "avg_cost": 10.0, "price": 9.6}},
        "stop_loss_log": [],
    }
    out = m.step(state, {"600000.XSHG": 9.6}, fee=0.0003)
    assert "600000.XSHG" not in out["positions"]          # 已平仓
    assert len(out["stop_loss_log"]) == 1                  # 记一条止损
    assert out["cash"] > 0                                # 回收现金
    assert out["net_value"] == out["cash"]                # 无持仓时净值=现金


def test_no_trigger_when_above_stop():
    m = Matcher(0.03)
    state = {
        "cash": 0.0, "start_cash": 1000.0, "net_value": 1100.0, "pnl": 100.0,
        "positions": {"600000.XSHG": {"amount": 100.0, "avg_cost": 10.0, "price": 11.0}},
        "stop_loss_log": [],
    }
    out = m.step(state, {"600000.XSHG": 11.0})
    assert "600000.XSHG" in out["positions"]
    assert out["pnl"] == 100.0
