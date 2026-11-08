"""产物保留策略：备份/receipt 按轮次滚动清理。"""

from __future__ import annotations

from pathlib import Path

import pytest

from app.quant.data_verify.retention import (
    BACKUP_DIRNAME,
    DEFAULT_KEEP,
    RECEIPTS_DIRNAME,
    prune_old_runs,
)

RUN_IDS = [
    "20261108T000934210225",
    "20261107T153000000001",
    "20261106T153000000002",
    "20261105T153000000003",
]


def _make_run(root: Path, run_id: str, table: str = "kline_minute",
              day: str = "date=2026-11-06") -> Path:
    part_dir = root / BACKUP_DIRNAME / run_id / table / day
    part_dir.mkdir(parents=True, exist_ok=True)
    part = part_dir / "part.parquet"
    part.write_bytes(b"x" * 1024)
    return part


def _make_receipt(root: Path, run_id: str) -> Path:
    d = root / RECEIPTS_DIRNAME
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{run_id}.json"
    p.write_text('[]\n', encoding="utf-8")
    return p


def test_keeps_most_recent_runs(tmp_path):
    for run_id in RUN_IDS:
        _make_run(tmp_path, run_id)
        _make_receipt(tmp_path, run_id)

    result = prune_old_runs(tmp_path, keep=2)

    kept_dirs = sorted(p.name for p in (tmp_path / BACKUP_DIRNAME).iterdir())
    assert kept_dirs == sorted(RUN_IDS[:2])   # 最新的两轮
    kept_files = sorted(p.stem for p in (tmp_path / RECEIPTS_DIRNAME).glob("*.json"))
    assert kept_files == sorted(RUN_IDS[:2])
    assert result["removed"] == 4             # 2 备份 + 2 receipt
    assert result["freed_bytes"] > 0


def test_unknown_directories_are_never_deleted(tmp_path):
    for run_id in RUN_IDS:
        _make_run(tmp_path, run_id)
    manual = tmp_path / BACKUP_DIRNAME / "manual-before-incident"
    manual.mkdir(parents=True, exist_ok=True)
    (manual / "note.txt").write_text("keep me", encoding="utf-8")

    prune_old_runs(tmp_path, keep=1)

    assert manual.is_dir()                     # 非 run_id 形态：一律不动
    assert (manual / "note.txt").is_file()


def test_missing_directories_is_a_noop(tmp_path):
    result = prune_old_runs(tmp_path, keep=DEFAULT_KEEP)
    assert result == {"kept": 0, "removed": 0, "freed_bytes": 0}


def test_keep_env_override(monkeypatch, tmp_path):
    for run_id in RUN_IDS:
        _make_run(tmp_path, run_id)
    monkeypatch.setenv("DATA_VERIFY_BACKUP_KEEP", "1")

    prune_old_runs(tmp_path)

    assert [p.name for p in (tmp_path / BACKUP_DIRNAME).iterdir()] == [RUN_IDS[0]]


@pytest.mark.parametrize("bad", ["0", "-3", "abc", ""])
def test_invalid_keep_falls_back_to_default(monkeypatch, tmp_path, bad):
    from app.quant.data_verify import retention

    for run_id in RUN_IDS:
        _make_run(tmp_path, run_id)
    monkeypatch.setenv("DATA_VERIFY_BACKUP_KEEP", bad)

    prune_old_runs(tmp_path)

    # 回退默认值 10 > 4 轮，什么都不删
    assert len(list((tmp_path / BACKUP_DIRNAME).iterdir())) == len(RUN_IDS)
    assert retention.DEFAULT_KEEP == 10
