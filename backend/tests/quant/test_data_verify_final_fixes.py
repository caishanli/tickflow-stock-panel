"""最终整支评审回归：自动修复安全阀、游标、审计与新增 stage。"""

from __future__ import annotations

import json
from datetime import date, datetime

import polars as pl
import pytest

from app.quant.data_verify.findings import Finding
from app.quant.data_verify.runner import run_verify

DAY = date(2026, 11, 6)
MINUTE_SCHEMA = {
    "symbol": pl.String,
    "datetime": pl.Datetime("us"),
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "amount": pl.Float64,
}
DAILY_SCHEMA = {
    "symbol": pl.String,
    "date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "amount": pl.Float64,
}


def _write_daily(root, day=DAY, *, symbol="600000.SH", close=10.0, volume=1000.0):
    part = root / "kline_daily" / f"date={day}" / "part.parquet"
    part.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [(symbol, day, close, close, close, close, volume, close * volume * 100)],
        schema=DAILY_SCHEMA,
        orient="row",
    ).write_parquet(part)
    return part


def _write_minute_day(root, day=DAY, *, symbol="688001.SH", bad=True):
    minute_part = root / "kline_minute" / f"date={day}" / "part.parquet"
    minute_part.parent.mkdir(parents=True, exist_ok=True)
    correct_shares = 1000.0
    minute_volume = correct_shares * (100 if bad else 1)
    pl.DataFrame(
        [(symbol, datetime.combine(day, datetime.min.time()).replace(hour=15),
          10.0, 10.0, 10.0, 10.0, minute_volume, minute_volume * 10)],
        schema=MINUTE_SCHEMA,
        orient="row",
    ).write_parquet(minute_part)
    _write_daily(root, day, symbol=symbol, volume=10.0)
    return minute_part


def _external_daily(day=DAY, *, close=11.0):
    return pl.DataFrame(
        [("600000.SH", day, close, close, close, close, 100_000.0, close * 100_000.0)],
        schema=DAILY_SCHEMA,
        orient="row",
    )


def test_scoped_apply_records_completed_days_without_moving_full_cursor(tmp_path):
    earlier = date(2026, 11, 5)
    _write_minute_day(tmp_path, earlier, bad=False)
    _write_minute_day(tmp_path, DAY, bad=True)

    scoped = run_verify(
        "internal", apply=True, days=[DAY], data_root=tmp_path, force=True
    )
    assert scoped["repaired"] == 1
    state = json.loads((tmp_path / "data_verify_state.json").read_text())
    assert state["completed_days"] == [DAY.isoformat()]
    assert "last_day_done" not in state

    resumed = run_verify(
        "internal", apply=True, days=None, data_root=tmp_path, resume=True, force=True
    )
    assert resumed["days"] == 2                 # 较早的一天没有被 scoped 游标跳过


def test_scheduled_subset_does_not_poison_existing_full_resume_cursor(tmp_path):
    day1, day2, day3 = date(2026, 11, 4), date(2026, 11, 5), DAY
    for day in (day1, day2, day3):
        _write_minute_day(tmp_path, day, bad=False)
    state_path = tmp_path / "data_verify_state.json"
    state_path.write_text(json.dumps({
        "stage": "internal", "last_day_done": day1.isoformat(), "applied": 0,
    }))

    run_verify("internal", apply=True, days=[day3], data_root=tmp_path)
    state = json.loads(state_path.read_text())
    assert state["last_day_done"] == day1.isoformat()
    assert state["completed_days"] == [day3.isoformat()]

    resumed = run_verify(
        "internal", apply=True, days=None, data_root=tmp_path, resume=True
    )
    assert resumed["days"] == 2                 # day2/day3 均重扫


def test_scoped_other_stage_preserves_full_cursor_stage(tmp_path):
    day1, day2, day3 = date(2026, 11, 4), date(2026, 11, 5), DAY
    for day in (day1, day2, day3):
        _write_minute_day(tmp_path, day, bad=False)
    state_path = tmp_path / "data_verify_state.json"
    state_path.write_text(json.dumps({
        "stage": "internal", "last_day_done": day1.isoformat(), "applied": 0,
    }))

    run_verify("structure", apply=True, days=[day3], data_root=tmp_path)
    state = json.loads(state_path.read_text())
    assert state["stage"] == "internal"
    assert state["last_day_done"] == day1.isoformat()
    assert state["completed_days"] == [day3.isoformat()]

    resumed = run_verify(
        "internal", apply=True, days=None, data_root=tmp_path, resume=True
    )
    assert resumed["days"] == 2


def test_non_star_x100_is_reported_but_never_deterministically_repaired(tmp_path):
    part = _write_minute_day(tmp_path, symbol="600000.SH", bad=True)
    before = part.read_bytes()
    report = run_verify(
        "internal", apply=True, days=[DAY], data_root=tmp_path, force=True
    )
    assert report["findings"] >= 1
    assert report["repaired"] == 0
    assert part.read_bytes() == before


def test_x100_postcondition_failure_is_reported_with_backup(tmp_path, monkeypatch):
    from app.quant.data_verify import checks_internal, runner

    part = _write_minute_day(tmp_path, bad=True)
    real_compare = checks_internal.compare_minute_to_daily
    calls = 0

    def fail_postcondition(minute, daily, *, table, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return real_compare(minute, daily, table=table, **kwargs)
        return [Finding(
            table=table,
            day=DAY,
            symbol="688001.SH",
            code="MINUTE_VOLUME_X100",
            severity="hard",
            detail={"ratio": 100.0},
        )]

    monkeypatch.setattr(runner, "compare_minute_to_daily", fail_postcondition)
    report = run_verify(
        "internal", apply=True, days=[DAY], data_root=tmp_path, force=True
    )

    assert report["repaired"] == 0
    assert report["repair_failures"][0]["code"] == "POSTCONDITION_FAILED"
    assert report["receipts"][0]["status"] == "postcondition_failed"
    backup = report["repair_failures"][0]["backup"]
    assert backup and (tmp_path / "data_verify_backup").exists()
    assert pl.read_parquet(part)["volume"][0] == 1000.0


def _write_blast_partition(root, affected: int):
    symbols = [f"688{i:03d}.SH" for i in range(affected)]
    symbols += [f"60{i:04d}.SH" for i in range(1000 - affected)]
    minute_rows = []
    daily_rows = []
    for symbol in symbols:
        bad = symbol.startswith("688")
        volume = 100_000.0 if bad else 1000.0
        minute_rows.append((symbol, datetime(2026, 11, 6, 15), 10.0, 10.0, 10.0,
                            10.0, volume, volume * 10))
        daily_rows.append((symbol, DAY, 10.0, 10.0, 10.0, 10.0, 10.0, 10_000.0))
    minute_part = root / "kline_minute" / f"date={DAY}" / "part.parquet"
    minute_part.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(minute_rows, schema=MINUTE_SCHEMA, orient="row").write_parquet(minute_part)
    daily_part = root / "kline_daily" / f"date={DAY}" / "part.parquet"
    daily_part.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(daily_rows, schema=DAILY_SCHEMA, orient="row").write_parquet(daily_part)
    return minute_part


@pytest.mark.parametrize("affected,force,expected", [(49, False, 49), (51, False, 0),
                                                      (51, True, 51)])
def test_partition_blast_radius_guard(tmp_path, affected, force, expected):
    part = _write_blast_partition(tmp_path, affected)
    report = run_verify(
        "internal", apply=True, days=[DAY], data_root=tmp_path, force=force
    )
    assert report["repaired"] == expected
    scaled = pl.read_parquet(part).filter(pl.col("symbol").str.starts_with("688"))
    assert scaled["volume"].to_list() == ([1000.0] * affected if expected
                                          else [100_000.0] * affected)
    if not expected:
        assert any(item["code"] == "PARTITION_BLAST_RADIUS"
                   for item in report["finding_details"])


def test_daily_stage_uses_injected_fetcher_and_writes_receipt(tmp_path, monkeypatch):
    part = _write_daily(tmp_path)
    calls = []
    invalidations = []

    def fetch(day):
        calls.append(day)
        return _external_daily(day)

    from app.api import data as data_api
    monkeypatch.setattr(data_api, "invalidate_storage_cache", lambda: invalidations.append(1))

    skipped = run_verify("daily", days=[DAY], data_root=tmp_path, daily_fetcher=fetch)
    assert calls == [DAY]
    assert skipped["repaired"] == 0
    assert skipped["single_source"] == 1
    assert pl.read_parquet(part)["close"][0] == 10.0

    repaired = run_verify(
        "daily",
        apply=True,
        days=[DAY],
        data_root=tmp_path,
        daily_fetcher=fetch,
        allow_single_source=True,
        force=True,
    )
    assert repaired["repaired"] == 1
    assert pl.read_parquet(part)["close"][0] == 11.0
    assert invalidations
    receipt = repaired["receipts"][0]
    assert {"table", "day", "symbol", "field", "old", "new", "source", "run_id"} <= set(receipt)
    receipt_path = tmp_path / "data_verify_receipts" / f"{repaired['run_id']}.json"
    assert json.loads(receipt_path.read_text())[0]["symbol"] == "600000.SH"


def test_factors_stage_uses_baostock_factors_and_injected_tencent_fetcher(tmp_path):
    days = [date(2026, 11, 5), DAY]
    for day in days:
        _write_daily(tmp_path, day)
    factor_dir = tmp_path / "adj_factor_baostock"
    factor_dir.mkdir()
    pl.DataFrame(
        [("600000.SH", days[0], 0.5)],
        schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64},
        orient="row",
    ).write_parquet(factor_dir / "all.parquet")
    calls = []

    def fetch(symbol):
        calls.append(symbol)
        return pl.DataFrame(
            [(symbol, day, 4.0) for day in days],
            schema={"symbol": pl.String, "date": pl.Date, "qfq_close": pl.Float64},
            orient="row",
        )

    report = run_verify(
        "factors", days=days, data_root=tmp_path, factor_fetcher=fetch, sample=1
    )
    assert calls == ["600000.SH"]
    assert report["findings"] >= 1
    assert report["sources"] == ["tencent"]


def test_cli_exposes_force_and_single_source_opt_in():
    from app.quant.data_verify.__main__ import _build_parser

    args = _build_parser().parse_args([
        "--stage", "internal", "--apply", "--force", "--allow-single-source",
    ])
    assert args.force is True
    assert args.allow_single_source is True
