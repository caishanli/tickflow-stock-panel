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
    api._state["sec_names"] = {}
    api._STOCK_NAMES.clear()
    try:
        yield
    finally:
        api._state.clear()
        api._state.update(saved_state)
        api._STOCK_NAMES.clear()
        api._STOCK_NAMES.update(saved_names)


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
