"""simulate/runner ptrade flavor 路由: 策略代码含 .SS/.SZ 则用 ptradeengine。"""

from app.quant.simulate.runner import _is_ptrade_strategy, _load_engine


def test_is_ptrade_strategy_detects_ptrade_code():
    assert _is_ptrade_strategy("def initialize(context):\n    g.x = ['510300.SS']")
    assert _is_ptrade_strategy("511880.SS")
    assert not _is_ptrade_strategy("def initialize(context):\n    g.x = ['510300.XSHG']")
    assert not _is_ptrade_strategy("")


def test_load_engine_routes_ptrade():
    pt_api, pt_loader = _load_engine("g.x = ['510300.SS']")
    assert pt_loader.__name__ == "app.quant.ptradeengine.ptrade_loader"
    assert hasattr(pt_api, "build_data_snapshot")


def test_load_engine_routes_jq():
    _jq_api, jq_loader = _load_engine("def initialize(context): pass")
    assert jq_loader.__name__ == "app.quant.jqengine.engine.jq.loader"


def test_load_engine_default_jq():
    _jq_api, jq_loader = _load_engine()
    assert jq_loader.__name__ == "app.quant.jqengine.engine.jq.loader"


def test_ptrade_bundle_runs_real_fixture_init():
    """本地 ptradeengine 能编译并初始化真实 ptrade 双持仓 fixture。"""
    from pathlib import Path

    from app.quant.jqengine.datasource.manager import get_data_manager
    from app.quant.ptradeengine import ptrade_loader

    src = Path(__file__).parent.parent / "fixtures" / "dual_v54" / "wufu-v5.4-dual-adapt.ptrade.py"
    dm = get_data_manager()
    bundle = ptrade_loader.load_strategy(src.read_text(encoding="utf-8"), dm, 0.0001, 0.0001, 100000.0)
    bundle.init_fn(bundle.ctx)
    assert bundle.ctx.g.holdings_num == 2
    assert len(bundle.daily) == 4
    times = {t for _, t in bundle.daily}
    assert {"09:40", "13:10"} <= times
