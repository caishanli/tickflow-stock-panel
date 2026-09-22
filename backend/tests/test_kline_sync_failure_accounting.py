"""sync_daily_batch 失败统计：要了但零贡献的标的必须进 failed_out。

回归 2026-09-22：北交所 344 只返回空帧，dict 分支 continue 吞掉，
管线报 done。成功路径行为必须不变（failed 为空时无 WARNING、无返回值变化）。
"""
from __future__ import annotations

from datetime import date

import polars as pl

from app.services import kline_sync


def _frame(sym: str) -> pl.DataFrame:
    return pl.DataFrame({
        "symbol": [sym],
        "date": [date(2026, 9, 22)],
        "open": [10.0],
        "high": [10.1],
        "low": [9.9],
        "close": [10.0],
        "volume": [100.0],
        "amount": [1000.0],
    })


class _FakeKlines:
    def __init__(self, payload):
        self._payload = payload

    def batch(self, *args, **kwargs):
        return self._payload


class _FakeClient:
    def __init__(self, payload):
        self.klines = _FakeKlines(payload)


def _patch(monkeypatch, payload):
    monkeypatch.setattr(kline_sync, "get_client", lambda: _FakeClient(payload))


def test_dict_empty_and_missing_keys_recorded_as_failed(monkeypatch):
    # B 返回空帧（当日未发布），C 连 key 都没有（provider 省略）
    _patch(monkeypatch, {"A": _frame("A"), "B": _frame("B").clear()})
    failed: list[str] = []

    out = kline_sync.sync_daily_batch(
        ["A", "B", "C"], start_time=None, end_time=None, failed_out=failed,
    )

    assert sorted(failed) == ["B", "C"]
    assert out["symbol"].to_list() == ["A"]


def test_dict_all_present_records_nothing(monkeypatch):
    _patch(monkeypatch, {"A": _frame("A"), "B": _frame("B")})
    failed: list[str] = []

    out = kline_sync.sync_daily_batch(
        ["A", "B"], start_time=None, end_time=None, failed_out=failed,
    )

    assert failed == []
    assert sorted(out["symbol"].to_list()) == ["A", "B"]


def test_flat_df_shortfall_records_missing_symbols(monkeypatch):
    _patch(monkeypatch, _frame("A"))  # 整 chunk 只回 A，B 无声消失
    failed: list[str] = []

    out = kline_sync.sync_daily_batch(
        ["A", "B"], start_time=None, end_time=None, failed_out=failed,
    )

    assert failed == ["B"]
    assert out["symbol"].to_list() == ["A"]


def test_dict_halt_only_rows_not_recorded_as_failed(monkeypatch):
    # 停牌日(open/high=0)被过滤后无行可写 ≠ 拉取失败：不能记失败，否则日常停牌天天红灯
    halted = pl.DataFrame({
        "symbol": ["H"],
        "date": [date(2026, 9, 22)],
        "open": [0.0],
        "high": [0.0],
        "low": [9.9],
        "close": [10.0],
        "volume": [0.0],
        "amount": [0.0],
    })
    _patch(monkeypatch, {"A": _frame("A"), "H": halted})
    failed: list[str] = []

    out = kline_sync.sync_daily_batch(
        ["A", "H"], start_time=None, end_time=None, failed_out=failed,
    )

    assert failed == []
    assert out["symbol"].to_list() == ["A"]


def test_flat_without_symbol_col_keeps_old_behavior(monkeypatch):
    # 扁平帧无 symbol 列时无法观测谁缺席：保持旧行为（落盘、不统计）
    nosym = pl.DataFrame({
        "trade_date": ["2026-09-22"],
        "open": [10.0],
        "high": [10.1],
        "low": [9.9],
        "close": [10.0],
        "volume": [100.0],
        "amount": [1000.0],
    })
    _patch(monkeypatch, nosym)
    failed: list[str] = []

    out = kline_sync.sync_daily_batch(
        ["A"], start_time=None, end_time=None, failed_out=failed,
    )

    assert failed == []
    assert out.height == 1


def test_failed_out_none_default_path(monkeypatch):
    # 不传 failed_out（指数/ETF 等旧调用）：行为与从前一致，不抛错
    _patch(monkeypatch, {"A": _frame("A")})

    out = kline_sync.sync_daily_batch(["A"], start_time=None, end_time=None)

    assert out["symbol"].to_list() == ["A"]


def test_persist_forwards_failed_out(monkeypatch, tmp_path):
    from app.tickflow.capabilities import Cap, CapabilityLimits, CapabilitySet
    from app.tickflow.repository import DataStore, KlineRepository

    captured: dict = {}

    def _fake_batch(symbols, **kwargs):
        captured["failed_out"] = kwargs.get("failed_out")
        failed = kwargs.get("failed_out")
        if failed is not None:
            failed.append("B")
        return _frame("A")

    monkeypatch.setattr(kline_sync, "sync_daily_batch", _fake_batch)
    repo = KlineRepository(DataStore(tmp_path))
    capset = CapabilitySet({Cap.KLINE_DAILY_BATCH: CapabilityLimits(batch=100, rpm=60)})
    failed: list[str] = []

    n = kline_sync.sync_and_persist_daily_batch(
        ["A", "B"], repo, capset, failed_out=failed,
    )

    assert n == 1
    assert failed == ["B"]
    assert captured["failed_out"] is failed


def test_pipeline_partial_daily_failure_fails_loudly(tmp_path, monkeypatch):
    """batch 零贡献 → stage_errors → PipelineStageError，不再绿灯。"""
    from datetime import datetime, time, timedelta
    from types import SimpleNamespace

    import pytest

    from app.config import settings as app_settings
    from app.jobs import daily_pipeline
    from app.market_time import CN_TZ
    from app.services import instrument_sync, kline_sync
    from app.tickflow.repository import DataStore, KlineRepository

    today = datetime.now(CN_TZ).date()
    yesterday = today - timedelta(days=1)
    while yesterday.weekday() >= 5:
        yesterday -= timedelta(days=1)

    def _ts_ms(day, t):
        return int(datetime.combine(day, t, tzinfo=CN_TZ).timestamp() * 1000)

    for day, ts in ((yesterday, _ts_ms(yesterday, time(11, 58))), (today, _ts_ms(today, time(10, 0)))):
        part = tmp_path / "kline_daily" / f"date={day.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({
            "symbol": ["600001.SH", "600002.SH"],
            "date": [day, day],
            "open": [10.0, 20.0], "high": [10.1, 20.1],
            "low": [9.9, 19.9], "close": [10.0, 20.0],
            "volume": [100.0, 200.0], "amount": [1000.0, 4000.0],
            "quote_ts": [ts, ts],
        }).write_parquet(part / "part.parquet")

    monkeypatch.setattr(instrument_sync, "sync_instruments", lambda data_dir: 0)

    def _fake_batch(universe, repo, capset, start_date=None, end_date=None,
                    on_chunk_done=None, failed_out=None):
        if failed_out is not None:
            failed_out.append("600002.SH")
        return 0

    monkeypatch.setattr(kline_sync, "sync_and_persist_daily_batch", _fake_batch)
    monkeypatch.setattr(app_settings, "data_dir", tmp_path)
    repo = KlineRepository(DataStore(tmp_path))
    capset = SimpleNamespace(has=lambda key: key == "QUOTE_POOL")

    with pytest.raises(daily_pipeline.PipelineStageError, match="sync_daily"):
        daily_pipeline.run_now(repo, capset)  # type: ignore[arg-type]
