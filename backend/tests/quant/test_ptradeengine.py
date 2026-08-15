"""ptradeengine 本地引擎：context/portfolio 别名、代码转换。"""
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
