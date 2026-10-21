"""get_security_name 整表缓存：逐只解析不得每次全量拉取名称表。

回归（2026-10-13 实测）：首板高开 09:26 选股 173s，其中新股过滤
4415×get_security_info（每只调 2 次 get_security_name，每次全量传输 ~5k
名称表）占 36s，ST 过滤的名称部分同理。名称整表必须日级缓存。
"""
import datetime
from types import SimpleNamespace

import pytest

from app.quant.jqengine.engine.jq import api


@pytest.fixture
def _api_state():
    saved_state = dict(api._state)
    saved_names = dict(api._STOCK_NAMES)
    saved_etfs = dict(api._ETF_LIST)
    saved_failed = dict(api._STOCK_NAMES_FAILED_AT)
    api._state["sec_names"] = {}
    api._STOCK_NAMES.clear()
    api._ETF_LIST.clear()
    api._STOCK_NAMES_FAILED_AT.clear()
    api._RATE_CACHE.clear()
    try:
        yield
    finally:
        api._state.clear()
        api._state.update(saved_state)
        api._STOCK_NAMES.clear()
        api._STOCK_NAMES.update(saved_names)
        api._ETF_LIST.clear()
        api._ETF_LIST.update(saved_etfs)
        api._STOCK_NAMES_FAILED_AT.clear()
        api._STOCK_NAMES_FAILED_AT.update(saved_failed)
        api._RATE_CACHE.clear()


class _CountingNetwork:
    def __init__(self, table, fail=False):
        self.table = table
        self.calls = 0
        self.fail = fail

    def get_stock_names(self):
        self.calls += 1
        if self.fail:
            raise TimeoutError("net down")
        return dict(self.table)


def _stub_manager(monkeypatch, net):
    monkeypatch.setattr(api, "_name_source", lambda: "service")
    api._state["manager"] = SimpleNamespace(sources={"network": net})


TABLE = {"600000": "浦发银行", "000001": "平安银行", "603906": "龙蟠科技"}


def test_bulk_name_table_fetched_once_for_many_codes(monkeypatch, _api_state):
    """多只逐只解析只拉一次整表（RED：现实现每次调用都全量传输）。"""
    net = _CountingNetwork(TABLE)
    _stub_manager(monkeypatch, net)
    got = [api.get_security_name(c) for c in
           ("600000.XSHG", "000001.XSHE", "603906.XSHG")]
    assert got == ["浦发银行", "平安银行", "龙蟠科技"]
    assert net.calls == 1
    # 第二轮全部命中进程缓存，不再触网
    got2 = [api.get_security_name(c) for c in
            ("600000.XSHG", "000001.XSHE", "603906.XSHG")]
    assert got2 == got
    assert net.calls == 1


def test_stale_day_key_misses_and_today_key_hits(monkeypatch, _api_state):
    """跨天失效：旧 key 命中不得冒充今日整表；今日 key 命中零触网。"""
    net = _CountingNetwork(TABLE)
    _stub_manager(monkeypatch, net)
    api._STOCK_NAMES["2000-01-01"] = {"600000": "旧名"}
    assert api.get_security_name("600000.XSHG") == "浦发银行"
    assert net.calls == 1
    today_key = datetime.date.today().isoformat()
    assert set(api._STOCK_NAMES) == {today_key}
    # 同 key 下再次解析零触网
    api._state["sec_names"] = {}
    assert api.get_security_name("000001.XSHE") == "平安银行"
    assert net.calls == 1


def test_bulk_failure_does_not_poison_cache(monkeypatch, _api_state):
    """整表拉取失败不得缓存空表（否则当日全部名称解析静默走降级）。"""
    net = _CountingNetwork(TABLE, fail=True)
    _stub_manager(monkeypatch, net)
    out = api.get_security_name("600000.XSHG")
    assert isinstance(out, str) and out
    assert api._STOCK_NAMES == {}
    assert net.calls == 1


def test_bulk_name_failure_is_negatively_cached(monkeypatch, _api_state):
    """整表失败后短时间内不得逐只重试（RED：全市场扫描把 1 次失败放大成数千次）。

    2026-10-21 sb_gaokai_sim：09:26 选股期间名称整表拉取失败，ST 过滤
    4853 只 + 新股过滤 4650 只每一只都重拉整表（65ms/次），选股从 ~1 分钟
    拖到 18 分钟、拖过 09:30 竞价窗口。失败负缓存后同一窗口只付 1 次。
    """
    net = _CountingNetwork(TABLE, fail=True)
    _stub_manager(monkeypatch, net)
    out = [api.get_security_name(c) for c in
           ("600000.XSHG", "000001.XSHE", "603906.XSHG", "999999.XSHG")]
    assert out == ["600000.XSHG", "000001.XSHE", "603906.XSHG", "999999.XSHG"]
    assert net.calls == 1
    # TTL 过期后允许重试（恢复不被永久封印）
    key = datetime.date.today().isoformat()
    api._STOCK_NAMES_FAILED_AT[key] = (
        datetime.datetime.now() - datetime.timedelta(seconds=api._NAMES_FAIL_TTL + 1))
    net.fail = False
    assert api.get_security_name("600000.XSHG") == "浦发银行"
    assert net.calls == 2


def test_etf_fallback_list_fetched_once_per_day(monkeypatch, _api_state):
    """名称表未收录的标的不得每次 get_security_name 都全量拉 ETF 名录。

    RED（2026-10-21）：``mgr.fetch("get_etf_list")`` 服务端全表扫描 0.5s/次
    且无缓存；334 只退市/停牌标的在 ST 过滤 + 新股过滤里各付 1~2 次，
    合计 ~12 分钟。ETF 名录是纯展示元数据，日级缓存足够。
    """
    net = _CountingNetwork(TABLE)
    api._state["manager"] = _FakeMgrWithFetch(net)
    monkeypatch.setattr(api, "_name_source", lambda: "service")
    for _ in range(3):
        assert api.get_security_name("888888.XSHG") == "888888.XSHG"
    assert api._state["manager"].etf_calls == 1
    # 名称表里已有的标的不该触发任何名录回源
    assert api.get_security_name("600000.XSHG") == "浦发银行"
    assert api._state["manager"].etf_calls == 1


class _FakeMgrWithFetch:
    def __init__(self, net):
        self.sources = {"network": net}
        self.etf_calls = 0

    def fetch(self, method, *args, **kwargs):
        assert method == "get_etf_list"
        self.etf_calls += 1
        return [{"ts_code": "510300.SH", "name": "沪深300ETF"}]
