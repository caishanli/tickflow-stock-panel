"""Runner：分区枚举 / L0+L1 编排 / 科创板 ×100 修复 / 断点状态 / dry-run 零写入。

约束（M1 可交付）：

- ``days=None`` = 扫描 ``data_root`` 下全部已有分钟分区（``kline_minute`` /
  ``kline_etf_minute``），逐日先 L0 后 L1；
- **dry-run 是默认**：只读不写，连状态文件都不落盘（否则 ``--resume --apply``
  会跳过只干跑过的日子，静默漏修）；
- ``apply=True`` 只对 ``MINUTE_VOLUME_X100`` 做 ``factor=100`` 的整分区缩放，
  写盘前备份到 ``data_verify_backup/<run_id>/``（与分区目录同级，不会被
  分区枚举误当成待检分区）；
- 每完成一天原子写 ``data_verify_state.json``，``resume=True`` 从
  ``last_day_done`` 之后继续。
"""

import json
from datetime import date, datetime

import polars as pl
import pytest

from app.quant.data_verify.runner import run_verify

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


def _write_day(root, day, stars_bad=True):
    mdir = root / "kline_minute" / f"date={day}" / "part.parquet"
    mdir.parent.mkdir(parents=True, exist_ok=True)
    vol = 8_630_559.0 * (100 if stars_bad else 1)
    pl.DataFrame(
        [("688001.SH", datetime.fromisoformat(f"{day}T09:30:00"), 1.0, 1.0, 1.0, 1.0, vol, vol)],
        schema=MINUTE_SCHEMA,
        orient="row",
    ).write_parquet(mdir)
    ddir = root / "kline_daily" / f"date={day}" / "part.parquet"
    ddir.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [
            (
                "688001.SH",
                datetime.fromisoformat(f"{day}T00:00:00").date(),
                1.0,
                1.0,
                1.0,
                1.0,
                86_305.59,
                8.63e8,
            )
        ],
        schema=DAILY_SCHEMA,
        orient="row",
    ).write_parquet(ddir)


def _minute_volume(root, day="2026-11-06"):
    return pl.read_parquet(root / "kline_minute" / f"date={day}" / "part.parquet")["volume"][0]


def test_runner_detects_and_repairs_star_x100(tmp_path):
    _write_day(tmp_path, "2026-11-06")
    rep = run_verify("internal", apply=True, days=None, data_root=tmp_path)
    assert rep["findings"] >= 1 and rep["repaired"] == 1
    assert _minute_volume(tmp_path) == 8_630_559.0
    state = json.loads((tmp_path / "data_verify_state.json").read_text())
    assert state["last_day_done"] == "2026-11-06"


def test_runner_dry_run_by_default(tmp_path):
    _write_day(tmp_path, "2026-11-06")
    rep = run_verify("internal", days=None, data_root=tmp_path)
    assert rep["repaired"] == 0
    assert _minute_volume(tmp_path) == 863_055_900.0


def test_runner_is_idempotent(tmp_path):
    _write_day(tmp_path, "2026-11-06")
    run_verify("internal", apply=True, days=None, data_root=tmp_path)
    rep = run_verify("internal", apply=True, days=[date(2026, 11, 6)], data_root=tmp_path)
    assert rep["repaired"] == 0
    assert _minute_volume(tmp_path) == 8_630_559.0


def test_runner_resume_skips_done_days(tmp_path):
    _write_day(tmp_path, "2026-11-05", stars_bad=False)
    _write_day(tmp_path, "2026-11-06", stars_bad=True)
    (tmp_path / "data_verify_state.json").write_text(
        json.dumps({"last_run_at": "2026-11-07T00:00:00", "stage": "internal",
                    "last_day_done": "2026-11-05", "applied": 0}))
    rep = run_verify("internal", apply=True, days=None, data_root=tmp_path, resume=True)
    assert rep["days"] == 1                      # 只处理 2026-11-06
    assert _minute_volume(tmp_path) == 8_630_559.0


def test_runner_dry_run_writes_nothing(tmp_path):
    """干跑不得落盘：分区、备份、状态文件都不许出现。"""
    _write_day(tmp_path, "2026-11-06")
    run_verify("internal", days=None, data_root=tmp_path)
    assert not (tmp_path / "data_verify_state.json").exists()
    assert not (tmp_path / "data_verify_backup").exists()
    assert sorted(p.name for p in (tmp_path / "kline_minute").iterdir()) == ["date=2026-11-06"]
    assert not list(tmp_path.rglob("*.tmp"))


def test_runner_structure_stage_reports_missing_bars(tmp_path):
    _write_day(tmp_path, "2026-11-06", stars_bad=False)
    rep = run_verify("structure", days=None, data_root=tmp_path)
    assert rep["findings"] == 1                  # 单 bar 分区 → MISSING_BARS (soft)
    assert rep["repaired"] == 0                  # L0 不驱动修复
    assert _minute_volume(tmp_path) == 8_630_559.0


def test_runner_repairs_only_x100_symbols_in_mixed_partition(tmp_path):
    """真实分区是混合的：616 只科创板 ×100 与 4602 只正常标的同时存在。

    整分区缩放会把正常标的打成 1/100，所以修复必须按 symbol 圈定范围。
    """
    day = "2026-11-06"
    part = tmp_path / "kline_minute" / f"date={day}" / "part.parquet"
    part.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            ("688001.SH", datetime(2026, 11, 6, 9, 30), 1.0, 1.0, 1.0, 1.0, 863_055_900.0, 8.63e10),
            ("600000.SH", datetime(2026, 11, 6, 9, 30), 1.0, 1.0, 1.0, 1.0, 1_000_000.0, 1.0e7),
        ],
        schema=MINUTE_SCHEMA,
        orient="row",
    ).write_parquet(part)
    daily = tmp_path / "kline_daily" / f"date={day}" / "part.parquet"
    daily.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            ("688001.SH", date(2026, 11, 6), 1.0, 1.0, 1.0, 1.0, 86_305.59, 8.63e8),
            ("600000.SH", date(2026, 11, 6), 1.0, 1.0, 1.0, 1.0, 10_000.0, 1.0e7),
        ],
        schema=DAILY_SCHEMA,
        orient="row",
    ).write_parquet(daily)
    rep = run_verify("internal", apply=True, days=None, data_root=tmp_path)
    assert rep["repaired"] == 1
    df = pl.read_parquet(part)
    star = df.filter(pl.col("symbol") == "688001.SH")
    clean = df.filter(pl.col("symbol") == "600000.SH")
    assert star["volume"][0] == 8_630_559.0 and star["amount"][0] == 8.63e8
    assert clean["volume"][0] == 1_000_000.0 and clean["amount"][0] == 1.0e7   # 未被误伤


def test_runner_apply_backs_up_beside_partitions(tmp_path):
    """备份落在 ``data_verify_backup`` 下，且不会被下一轮分区枚举当成待检分区。"""
    _write_day(tmp_path, "2026-11-06")
    run_verify("internal", apply=True, days=None, data_root=tmp_path)
    backups = list((tmp_path / "data_verify_backup").rglob("part.parquet"))
    assert len(backups) == 1
    assert pl.read_parquet(backups[0])["volume"][0] == 863_055_900.0
    rep = run_verify("internal", days=None, data_root=tmp_path)
    assert rep["days"] == 1                      # 备份分区没被算成一天


def test_runner_apply_skips_unclosed_day(tmp_path, monkeypatch):
    """安全阀：当日分区还在被收盘链路写，apply 也不能动它（design §6.4）。"""
    from app.quant.data_verify import runner

    monkeypatch.setattr(runner, "_today", lambda: date(2026, 11, 6))
    _write_day(tmp_path, "2026-11-06")
    rep = run_verify("internal", apply=True, days=None, data_root=tmp_path)
    assert rep["findings"] >= 1 and rep["repaired"] == 0
    assert _minute_volume(tmp_path) == 863_055_900.0
    assert not (tmp_path / "data_verify_backup").exists()


def test_runner_rejects_unknown_stage(tmp_path):
    with pytest.raises(ValueError):
        run_verify("external", days=None, data_root=tmp_path)


def test_runner_resume_ignores_state_from_other_stage(tmp_path, caplog):
    """断点绑 stage：structure 跑出来的 last_day_done 不能拿来跳过 internal。"""
    _write_day(tmp_path, "2026-11-05", stars_bad=False)
    _write_day(tmp_path, "2026-11-06", stars_bad=True)
    (tmp_path / "data_verify_state.json").write_text(
        json.dumps({"last_run_at": "2026-11-07T00:00:00", "stage": "structure",
                    "last_day_done": "2026-11-06", "applied": 0}))
    with caplog.at_level("WARNING"):
        rep = run_verify("internal", apply=True, days=None, data_root=tmp_path, resume=True)
    assert rep["days"] == 2                      # 断点作废，两天都要跑
    assert rep["repaired"] == 1
    assert _minute_volume(tmp_path) == 8_630_559.0
    assert "structure" in caplog.text


def test_runner_accepts_datetime_in_days(tmp_path):
    """``datetime`` 是 ``date`` 的子类，必须按「日期」而不是带时间戳比对分区。"""
    _write_day(tmp_path, "2026-11-06")
    rep = run_verify("internal", days=[datetime(2026, 11, 6, 15, 0)], data_root=tmp_path)
    assert rep["days"] == 1


def test_runner_progress_callback_reaches_100(tmp_path):
    _write_day(tmp_path, "2026-11-05", stars_bad=False)
    _write_day(tmp_path, "2026-11-06", stars_bad=True)
    seen = []
    run_verify("internal", days=None, data_root=tmp_path,
               progress=lambda stage, pct, msg: seen.append((stage, pct, msg)))
    assert {stage for stage, _, _ in seen} == {"internal"}
    pcts = [pct for _, pct, _ in seen]
    assert pcts == sorted(pcts) and pcts[-1] == 100
