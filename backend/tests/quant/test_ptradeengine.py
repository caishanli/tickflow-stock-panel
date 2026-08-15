"""ptradeengine 本地引擎：context/portfolio 别名、代码转换、ptrade_api 状态与订单。"""
from app.quant.ptradeengine.context import (
    PtradeContext,
    PtradePortfolio,
    PtradePosition,
    ptrade_code_conv,
)


def test_position_ptrade_aliases():
    p = PtradePosition(amount=100, avg_cost=3.0, price=3.1)
    assert p.enable_amount == 100
    assert p.cost_basis == 3.0
    assert p.last_sale_price == 3.1
    assert p.total_amount == 100


def test_portfolio_ptrade_alias():
    pf = PtradePortfolio(cash=10000.0)
    pos = PtradePosition(amount=100, avg_cost=3.0, price=3.1)
    pf.positions["510300.SS"] = pos
    assert pf.portfolio_value == pf.total_value == 10000.0 + 310.0


def test_context_blotter():
    import pandas as pd
    ctx = PtradeContext()
    ctx.current_dt = pd.Timestamp("2026-07-10 13:10")
    assert ctx.blotter.current_dt == ctx.current_dt


def test_code_conv():
    to_engine, to_pt = ptrade_code_conv()
    assert to_engine("510300.SS") == "510300.XSHG"
    assert to_pt("510300.XSHG") == "510300.SS"
    assert to_engine("510300.XSHG") == "510300.XSHG"  # jq 码幂等


# ---- ptrade_api ----

class _StubDm:
    """最小 DataManager 桩：get_minute_price_at/fetch 返回 None/空。"""

    def get_minute_price_at(self, code, dt):
        return None

    def fetch(self, *a, **k):
        import pandas as pd
        return pd.DataFrame()


def _fresh_api():
    from app.quant.ptradeengine import ptrade_api as api
    api._reset(_StubDm(), 0.0001, 0.0001, 100000.0)
    return api


def test_api_state_shape_and_code_domain():
    api = _fresh_api()
    for key in ("ctx", "manager", "fee", "slippage", "fee_config", "daily", "minute",
                "trades", "minute_prices", "minute_mode", "no_buy", "no_sell", "log_sink"):
        assert key in api._state, key
    assert callable(api.on_new_day)


def test_api_run_daily_registers():
    api = _fresh_api()
    calls = []

    def cb(context):
        calls.append(1)

    api.run_daily(None, cb, time="13:10")
    assert (cb, "13:10") in api._state["daily"]


def test_api_order_records_ptrade_code():
    """order 用 PTrade 码，成交 trades 记 PTrade 码，portfolio positions 键 PTrade 码。"""
    api = _fresh_api()
    api._state["minute_prices"] = {"510300.SS": 3.0}
    api._state["minute_mode"] = True
    ok = api.order("510300.SS", 1000)
    assert ok
    assert "510300.SS" in api._state["ctx"].portfolio.positions
    assert api._state["trades"][-1]["code"] == "510300.SS"


def test_api_get_positions_ptrade_keys():
    api = _fresh_api()
    api._state["minute_prices"] = {"510300.SS": 3.0}
    api._state["minute_mode"] = True
    api.order("510300.SS", 1000)
    assert "510300.SS" in api.get_positions()
