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


# ---- ptrade_loader ----

def test_loader_bundle_hooks_and_conv():
    from app.quant.ptradeengine import ptrade_loader
    code = (
        "def initialize(context):\n"
        "    g.holdings_num = 2\n"
        "    run_daily(context, after, time='13:10')\n"
        "def after(context):\n"
        "    pass\n"
        "def handle_data(context, data):\n"
        "    pass\n"
        "def before_trading_start(context, data):\n"
        "    pass\n"
        "def after_trading_end(context):\n"
        "    pass\n"
    )
    b = ptrade_loader.load_strategy(code, None, 0.0001, 0.0001, 100000.0)
    assert b.before_trading_start is not None
    assert b.after_trading_end is not None
    assert b.handle_data is not None
    to_engine, to_pt = b.conv
    assert to_engine("510300.SS") == "510300.XSHG"
    assert to_pt("510300.XSHG") == "510300.SS"
    b.init_fn(b.ctx)  # initialize 内注册 run_daily
    assert b.ctx.g.holdings_num == 2
    assert len(b.daily) == 1 and b.daily[0][1] == "13:10"


def test_loader_hook_signature_adaptive():
    """after_trading_end(context) 1 参不被注入 data；handle_data 2 参正常注入。

    回归：wufu-v5.4.ptrade.py 用官方 1 参签名，引擎按签名自适应，不得抛
    TypeError: after_trading_end() takes 1 positional argument but 2 were given。
    """
    from app.quant.ptradeengine import ptrade_loader

    calls = {"ate": None, "hd": None}

    def _ate(context):
        calls["ate"] = context

    def _hd(context, data):
        calls["hd"] = (context, data)

    ns = {
        "initialize": lambda context: None,
        "handle_data": _hd,
        "before_trading_start": lambda context, data: None,
        "after_trading_end": _ate,
    }

    class _Ctx:
        universe = []
        portfolio = type("P", (), {"positions": {}})()

    b = ptrade_loader.StrategyBundle(ns["initialize"], _Ctx(), ns)
    ctx = _Ctx()
    b.after_trading_end(ctx)   # 1 参 → 只传 context
    b.handle_data(ctx)         # 2 参 → 注入快照
    assert calls["ate"] is ctx
    assert calls["hd"][0] is ctx
    assert calls["hd"][1] is not None


def test_get_history_single_code_field_column_local():
    """官方格式：本地引擎 ptrade_api.get_history 单标的列=行情字段（df['close']）。"""
    from datetime import datetime

    import pandas as pd

    from app.quant.ptradeengine import ptrade_api

    class _Fake:
        def __init__(self):
            self._daily_mem = {}
            self._minute_mem = {}
            self.sources = {}

        def fetch(self, name, *a, **kw):  # noqa: N802
            idx = pd.date_range("2026-07-01", periods=6, freq="D")
            return pd.DataFrame({"close": [1.0, 1.1, 1.2, 1.3, 1.4, 1.5]}, index=idx)

    ptrade_api._reset(_Fake(), 0.0001, 0.0001, 100000.0)
    ptrade_api._state["ctx"].current_dt = datetime(2026, 7, 4, 10, 0)
    df = ptrade_api.get_history(3, "1d", "close", security_list="510300.SS")
    assert isinstance(df, pd.DataFrame)
    assert "close" in df.columns, "单标的列名必须是行情字段（官方 get_history）"


def test_base_position_has_ptrade_aliases():
    """恢复持仓路径（_restore_portfolio）构造的是基础 Position，
    也必须带 PTrade 别名——否则模拟盘重启续跑后 minute_level_stop_loss
    等策略代码访问 position.enable_amount 直接 AttributeError
    （960366ab 08-21 14:37 起 117 条错误日志的根因）。"""
    from app.quant.jqengine.engine.jq.context import Position
    p = Position(amount=100, avg_cost=3.0, price=3.1)
    assert p.enable_amount == 100
    assert p.cost_basis == 3.0
    assert p.last_sale_price == 3.1


def test_get_stock_status_never_halted_on_snapshot_absence():
    """快照缺价不得推断为停牌（2026-08-25 960366ab 159502 全天停牌误判回归）。

    实时回源瞬态失败/服务重启清内存 → minute_prices 缺该码，旧实现据此返回
    True，叠加策略 _HALT_CACHE 按日黏住导致当天全部交易被跳过；且 jq 引擎
    paused 恒 False，两侧行为不对称。现本地恒 False（真机仍由交易所应答）。
    """
    api = _fresh_api()
    api._state["minute_prices"] = {}  # 快照全空：旧实现会把所有码判成停牌
    out = api.get_stock_status(["159502.SZ", "511880.SS"], query_type="HALT")
    assert out == {"159502.SZ": False, "511880.SS": False}
    assert api.get_stock_status("510300.SS") == {"510300.SS": False}
    # 其余 query_type 同样恒 False（本地无权威停牌源）
    assert api.get_stock_status(["510300.SS"], query_type="ST") == {"510300.SS": False}
