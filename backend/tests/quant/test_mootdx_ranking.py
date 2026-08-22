"""rank_servers 延迟排名测试。"""
from __future__ import annotations

from app.quant.jqengine.datasource import mootdx_src as msrc


def test_rank_servers_orders_by_latency(monkeypatch):
    monkeypatch.setattr(msrc, "_TDX_SERVERS", [("a", 1), ("b", 2), ("c", 3)])
    monkeypatch.setattr(msrc, "probe_servers",
                        lambda timeout=1.5: [
                            {"ip": "a", "port": 1, "ok": True, "latency_ms": 50},
                            {"ip": "b", "port": 2, "ok": True, "latency_ms": 10},
                            {"ip": "c", "port": 3, "ok": False, "latency_ms": None}])
    lat = {"a": 500.0, "b": 60.0}  # 实测请求延迟：a 慢 b 快

    def fake_req(ip, port):
        import time
        t0 = time.perf_counter()
        rows = 0 if ip == "a" else 10  # a 还返回空 → 双重降权
        return rows

    monkeypatch.setattr(msrc, "_measure_server", lambda ip, port, timeout=5.0: (
        lat.get(ip, 9e9), fake_req(ip, port)))
    msrc._RANK_CACHE["ts"] = 0.0
    msrc._RANK_CACHE["data"] = None
    ranked = msrc.rank_servers()
    assert ranked[0] == ("b", 2)
    assert ("c", 3) not in ranked      # TCP 不可达剔除
    assert ranked[-1] == ("a", 1)      # 空+慢沉底


def test_rank_servers_cache_ttl(monkeypatch):
    calls = {"n": 0}
    monkeypatch.setattr(msrc, "_TDX_SERVERS", [("a", 1)])
    monkeypatch.setattr(msrc, "probe_servers",
                        lambda timeout=1.5: [{"ip": "a", "port": 1, "ok": True,
                                              "latency_ms": 5}])
    monkeypatch.setattr(msrc, "_measure_server", lambda ip, port, timeout=5.0: (5.0, 10))

    original_uncached = msrc._rank_servers_uncached

    def counting(*a, **k):
        calls["n"] += 1
        return original_uncached()

    monkeypatch.setattr(msrc, "_rank_servers_uncached", counting)
    msrc._RANK_CACHE["ts"] = 0.0
    msrc._RANK_CACHE["data"] = None
    msrc.rank_servers()
    msrc.rank_servers()  # TTL 内第二次走缓存
    assert calls["n"] == 1
