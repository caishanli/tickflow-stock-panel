"""probe_servers 并发探测全部 mootdx 显式服务器。"""
from app.quant.jqengine.datasource import mootdx_src as msrc


def test_probe_servers_returns_all_with_status(monkeypatch):
    results = {}
    def fake_probe(ip, port, timeout=2.0):
        results[ip] = timeout
        return ip.startswith("115.")
    monkeypatch.setattr(msrc, "_probe", fake_probe)
    out = msrc.probe_servers(timeout=1.5)
    assert len(out) == len(msrc._TDX_SERVERS)
    assert all({k} <= {"ip", "port", "ok", "latency_ms"} for r in out for k in r)
    ok = [r for r in out if r["ok"]]
    assert ok and all(r["latency_ms"] is not None for r in ok)
    fail = [r for r in out if not r["ok"]]
    assert fail and all(r["latency_ms"] is None for r in fail)
    assert results and set(results.values()) == {1.5}