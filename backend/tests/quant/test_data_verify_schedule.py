"""每日增量数据核验调度 + 编排测试。

覆盖 Task 11 的两块：

1. ``build_data_verify_trigger``：盘后管道时间 +60 分钟、工作日、Asia/Shanghai，
   跨日（23:xx → 次日 00:xx）小时回绕；
2. ``run_daily_incremental``：最近 N 个交易日 → 日线外部覆盖（通达信盘后包，
   volume 股→手 ×0.01）→ ``run_verify("all", apply=True)`` → 仅当日线真的被写过
   才 ``recompute_enriched`` → ``write_status`` + ``write_quarantine``。

编排用 fakes 注入，不触网、不跑真实 pipeline：``fetch_daily_package`` /
``run_verify`` / ``recompute_enriched`` 打桩，``compare_daily_external`` /
``decide`` / ``replace_symbols`` / ``write_status`` / ``write_quarantine`` 走真实
实现（Polars 纯计算 + 写临时分区）。
"""

from datetime import date, datetime, timedelta

import polars as pl

from app.jobs.daily_pipeline import (
    build_data_verify_trigger,
    run_daily_incremental,
)

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


def _field(trig, name: str) -> str:
    """CronTrigger 3.x 不暴露 ``.hour``/``.minute``，从 ``fields`` 读。"""
    return str(next(field for field in trig.fields if field.name == name))


def _write_daily(
    root, day: str, *, close: float = 10.0, volume: float = 1000.0,
    symbol: str = "600000.SH",
):
    part = root / "kline_daily" / f"date={day}" / "part.parquet"
    part.parent.mkdir(parents=True, exist_ok=True)
    # kline_daily：volume 单位「手」、amount 单位「元」（= close × volume手 × 100）。
    pl.DataFrame(
        [(symbol, date.fromisoformat(day), 10.0, 10.2, 9.8, close, volume,
          close * volume * 100)],
        schema=DAILY_SCHEMA,
        orient="row",
    ).write_parquet(part)
    return part


def _external(day: str, *, close: float = 10.5, volume: float = 100_000.0):
    """通达信盘后包口径：volume 单位「股」、amount 单位「元」（= close × volume股）。"""
    return pl.DataFrame(
        [("600000.SH", date.fromisoformat(day), close, close, close, close, volume,
          close * volume)],
        schema={**DAILY_SCHEMA, "volume": pl.Float64},
        orient="row",
    )


# ── 触发点 ────────────────────────────────────────────────────────


def test_trigger_is_after_pipeline():
    trig = build_data_verify_trigger({"hour": 15, "minute": 30})
    assert _field(trig, "hour") == "16"
    assert _field(trig, "minute") == "30"
    assert _field(trig, "day_of_week") == "mon-fri"
    assert str(trig.timezone) == "Asia/Shanghai"


def test_trigger_wraps_past_midnight():
    trig = build_data_verify_trigger({"hour": 23, "minute": 30})
    assert _field(trig, "hour") == "0"
    assert _field(trig, "minute") == "30"


# ── 编排 ──────────────────────────────────────────────────────────


def test_run_daily_incremental_repairs_daily_and_recomputes(tmp_path, monkeypatch):
    from app.quant.data_verify import recompute as recompute_module
    from app.quant.data_verify import runner as runner_module
    from app.quant.data_verify.probes import tdx_package

    _write_daily(tmp_path, "2026-11-05", close=10.0)
    _write_daily(tmp_path, "2026-11-06", close=10.0)

    verify_calls: list = []
    recompute_calls: list = []

    def _fake_fetch(day, *, timeout=None):
        return _external(day.isoformat(), close=10.5, volume=100_000.0)

    def _fake_run_verify(stage, *, apply=False, days=None, data_root=None):
        verify_calls.append((stage, apply, [str(d) for d in days], str(data_root)))
        return {"findings": 0, "repaired": 0, "days": len(days),
                "sources": [], "state_path": "x"}

    def _fake_recompute(days, *, tables, prune, runner, data_dir=None,
                        repo=None, capset=None):
        recompute_calls.append((sorted(str(d) for d in days), list(tables), str(data_dir)))
        return {"ok": True, "days": len(days), "error": None}

    monkeypatch.setattr(tdx_package, "fetch_daily_package", _fake_fetch)
    monkeypatch.setattr(runner_module, "run_verify", _fake_run_verify)
    monkeypatch.setattr(recompute_module, "recompute_enriched", _fake_recompute)

    result = run_daily_incremental(days=7, data_root=tmp_path)

    # 日线被外部覆盖修复（close 10.0 → 10.5），volume 保持「手」（100_000 股 ×0.01）。
    for day in ("2026-11-05", "2026-11-06"):
        out = pl.read_parquet(tmp_path / "kline_daily" / f"date={day}" / "part.parquet")
        assert out["close"][0] == 10.5
        assert out["volume"][0] == 1000.0

    assert verify_calls == [("all", True, ["2026-11-05", "2026-11-06"], str(tmp_path))]
    assert recompute_calls == [(["2026-11-05", "2026-11-06"], ["kline_daily_enriched"],
                                str(tmp_path))]
    assert result["repaired"] == 2
    assert result["daily_repaired_days"] == ["2026-11-05", "2026-11-06"]
    assert result["recompute"]["days"] == 2
    assert result["sources"] == ["tdx"]
    # 状态与隔离清单已落盘
    assert (tmp_path / "data_verify_status.json").is_file()
    assert (tmp_path / "data_verify_quarantine.json").is_file()


def test_run_daily_incremental_skips_recompute_without_daily_repair(tmp_path, monkeypatch):
    from app.quant.data_verify import recompute as recompute_module
    from app.quant.data_verify import runner as runner_module
    from app.quant.data_verify.probes import tdx_package

    _write_daily(tmp_path, "2026-11-05", close=10.0)

    recompute_calls: list = []

    def _fake_fetch(day, *, timeout=None):
        # 外部与本地一致（10.0），不产出 finding → 不触发日线修复。
        return _external(day.isoformat(), close=10.0, volume=100_000.0)

    def _fake_run_verify(stage, *, apply=False, days=None, data_root=None):
        return {"findings": 0, "repaired": 0, "days": len(days),
                "sources": [], "state_path": "x"}

    def _fake_recompute(days, *, tables, prune, runner, data_dir=None,
                        repo=None, capset=None):
        recompute_calls.append(days)
        return {"ok": True, "days": len(days), "error": None}

    monkeypatch.setattr(tdx_package, "fetch_daily_package", _fake_fetch)
    monkeypatch.setattr(runner_module, "run_verify", _fake_run_verify)
    monkeypatch.setattr(recompute_module, "recompute_enriched", _fake_recompute)

    result = run_daily_incremental(days=7, data_root=tmp_path)

    assert recompute_calls == []
    assert result["repaired"] == 0
    assert result["daily_repaired_days"] == []


def test_run_daily_incremental_writes_quarantine_for_conflicts(tmp_path, monkeypatch):
    from app.quant.data_verify import policy as policy_module
    from app.quant.data_verify import recompute as recompute_module
    from app.quant.data_verify import runner as runner_module
    from app.quant.data_verify.probes import tdx_package

    _write_daily(tmp_path, "2026-11-05", close=10.0)

    def _fake_fetch(day, *, timeout=None):
        return _external(day.isoformat(), close=10.5, volume=100_000.0)

    def _fake_run_verify(stage, *, apply=False, days=None, data_root=None):
        return {"findings": 0, "repaired": 0, "days": len(days),
                "sources": [], "state_path": "x"}

    def _fake_recompute(days, *, tables, prune, runner, data_dir=None,
                        repo=None, capset=None):
        return {"ok": True, "days": len(days), "error": None}

    # 把 decide 打成「无多数 → 隔离」，验证冲突进隔离清单而非被修。
    def _fake_decide(findings, verdicts):
        return [
            policy_module.Decision(
                policy_module.ACTION_QUARANTINE, finding, "tdx",
            )
            for finding in findings
        ]

    monkeypatch.setattr(tdx_package, "fetch_daily_package", _fake_fetch)
    monkeypatch.setattr(runner_module, "run_verify", _fake_run_verify)
    monkeypatch.setattr(recompute_module, "recompute_enriched", _fake_recompute)
    monkeypatch.setattr(policy_module, "decide", _fake_decide)

    result = run_daily_incremental(days=7, data_root=tmp_path)

    # 未被外部覆盖：close 保持本地原值。
    out = pl.read_parquet(tmp_path / "kline_daily" / "date=2026-11-05" / "part.parquet")
    assert out["close"][0] == 10.0
    assert result["repaired"] == 0
    assert result["quarantined"] == 1
    assert result["daily_repaired_days"] == []


def test_run_daily_incremental_excludes_today_before_selecting_last_n(tmp_path, monkeypatch):
    """当天分区不能占用最近 N 个已收盘交易日的名额。"""
    from app.quant.data_verify import runner as runner_module
    from app.quant.data_verify.probes import tdx_package

    today = date.today()
    previous = sorted(today - timedelta(days=i) for i in (1, 2))
    for day in [*previous, today]:
        _write_daily(tmp_path, day.isoformat())

    verify_days: list = []

    def _fake_fetch(day, *, timeout=None):
        return _external(day.isoformat(), close=10.0)

    def _fake_run_verify(stage, *, apply=False, days=None, data_root=None):
        verify_days.extend(days)
        return {"findings": 0, "repaired": 0, "days": len(days), "sources": []}

    monkeypatch.setattr(tdx_package, "fetch_daily_package", _fake_fetch)
    monkeypatch.setattr(runner_module, "run_verify", _fake_run_verify)

    result = run_daily_incremental(days=2, data_root=tmp_path)

    assert verify_days == previous
    assert result["daily_days"] == 2


def test_acceptance_star_dry_run_counts_repairable_without_writing(tmp_path):
    """验收 dry-run 的「修复数」是可修复数，且不得改分钟分区。"""
    import importlib.util
    from pathlib import Path

    day = "2026-11-06"
    symbol = "688001.SH"
    _write_daily(tmp_path, day, volume=100.0, symbol=symbol)
    minute_part = tmp_path / "kline_minute" / f"date={day}" / "part.parquet"
    minute_part.parent.mkdir(parents=True)
    pl.DataFrame(
        [(symbol, datetime(2026, 11, 6, 15, 0), 10.0, 10.0, 10.0, 10.0,
          1_000_000.0, 10_000_000.0)],
        schema={
            "symbol": pl.String, "datetime": pl.Datetime("us"),
            "open": pl.Float64, "high": pl.Float64, "low": pl.Float64,
            "close": pl.Float64, "volume": pl.Float64, "amount": pl.Float64,
        },
        orient="row",
    ).write_parquet(minute_part)
    before = minute_part.read_bytes()

    script = Path(__file__).parents[2] / "scripts" / "verify_acceptance.py"
    spec = importlib.util.spec_from_file_location("verify_acceptance", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    result = module.check_star_x100(tmp_path, [day], min_per_day=1, apply=False)

    assert result["status"] == "PASS"
    assert result["detected"] == 1
    assert result["repaired"] == 1
    assert minute_part.read_bytes() == before


def test_scheduled_data_verify_skips_when_job_store_is_active(monkeypatch):
    """活跃 JobStore 任务存在时不得启动每日核验。"""
    import app.jobs.daily_pipeline as pipeline
    from app.services import pipeline_jobs

    class ActiveStore:
        def create(self, *, long_running=False):
            assert long_running
            return "active-job", False

    monkeypatch.setattr(pipeline_jobs, "job_store", ActiveStore())
    monkeypatch.setattr(
        pipeline, "run_daily_incremental",
        lambda: (_ for _ in ()).throw(AssertionError("活跃任务时不应跑核验")),
    )

    pipeline._scheduled_data_verify()


def test_start_scheduler_registers_replaceable_data_verify_job(monkeypatch):
    """每日核验以稳定 id 注册，replace_existing 防止重启后重复 job。"""
    import app.jobs.daily_pipeline as pipeline
    from app.services import preferences

    class FakeScheduler:
        def __init__(self):
            self.jobs = []
            self.started = False

        def add_job(self, fn, **kwargs):
            self.jobs.append({"fn": fn, **kwargs})

        def start(self):
            self.started = True

    fake = FakeScheduler()
    monkeypatch.setattr(pipeline, "AsyncIOScheduler", lambda **kwargs: fake)
    monkeypatch.setattr(preferences, "get_pipeline_schedule", lambda: {"hour": 15, "minute": 30})
    monkeypatch.setattr(preferences, "get_instruments_schedule", lambda: {"hour": 9, "minute": 5})
    monkeypatch.setattr(preferences, "get_depth_finalize_time", lambda: {"hour": 15, "minute": 2})
    monkeypatch.setattr(preferences, "get_review_schedule", lambda: {"enabled": False})

    result = pipeline.start_scheduler(object(), object())

    job = next(job for job in fake.jobs if job["id"] == "data_verify_daily")
    assert job["replace_existing"] is True
    assert _field(job["trigger"], "hour") == "16"
    assert _field(job["trigger"], "minute") == "30"
    assert result is fake and fake.started
