"""sync_daily_batch 429 退避重试回归测试。

背景：2026-09-11 15:30 管道撞免费 60/min 限流，5 个 chunk（462 只）整块
丢弃无重试。修复：按服务端 message 里“请 Xms 后重试”等待后重试，最多 3 次。
"""
import pandas as pd

from app.services import kline_sync as ks


class _FakeKlines:
    def __init__(self, effects):
        self._effects = list(effects)
        self.calls = 0

    def batch(self, *a, **k):
        self.calls += 1
        eff = self._effects.pop(0) if self._effects else {}
        if isinstance(eff, Exception):
            raise eff
        return eff


class _FakeClient:
    def __init__(self, effects):
        self.klines = _FakeKlines(effects)


def _df():
    return pd.DataFrame({
        "date": [pd.Timestamp("2026-09-11").date()],
        "open": [9.35], "high": [9.35], "low": [9.22], "close": [9.26],
        "volume": [100.0], "amount": [926.0],
    })


def test_parse_retry_wait_ms():
    assert ks._parse_retry_wait_ms("请求频率超限 (60/min)，请 15036ms 后重试。") == 15036
    assert ks._parse_retry_wait_ms("请 182180ms 后重试") == 60000  # 上限 60s
    assert ks._parse_retry_wait_ms(" plain error", 0) == 5000
    assert ks._parse_retry_wait_ms(" plain error", 5) == 15000


def test_retry_then_success(monkeypatch):
    sleeps = []
    monkeypatch.setattr(ks.time, "sleep", sleeps.append)
    monkeypatch.setattr(ks, "sleep_between_batches", lambda i, rpm: None)
    cli = _FakeClient([RuntimeError("请求频率超限，请 15036ms 后重试。"),
                       RuntimeError("请 309ms 后重试"),
                       {"AAA": _df()}])
    monkeypatch.setattr(ks, "get_client", lambda: cli)
    out = ks.sync_daily_batch(["AAA"], batch_size=100, rpm=None)
    assert cli.klines.calls == 3
    assert sleeps == [15.036, 0.309]  # 实现按秒 sleep（wait_ms/1000）
    assert len(out) == 1


def test_exhaust_marks_failed(monkeypatch):
    sleeps = []
    monkeypatch.setattr(ks.time, "sleep", sleeps.append)
    monkeypatch.setattr(ks, "sleep_between_batches", lambda i, rpm: None)
    cli = _FakeClient([RuntimeError("请求频率超限，请 100ms 后重试。")] * 5)
    monkeypatch.setattr(ks, "get_client", lambda: cli)
    out = ks.sync_daily_batch(["AAA", "BBB"], batch_size=100, rpm=None)
    assert cli.klines.calls == 3
    assert len(sleeps) == 2
    assert out.is_empty()
