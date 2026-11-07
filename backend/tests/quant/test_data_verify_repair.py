"""确定性修复：备份 / 原子写 / 分钟量额 ×100 还原。

修复必须可回滚：写盘前先把原分区复制到 ``bak/<run_id>/<table>/date=.../``，
写入走 `.tmp` + `os.replace` 并持有 ``daily_partition_lock``（与收盘回源等
写入方互斥，进程中断不留半截 parquet）。

只改 ``volume`` / ``amount`` 两列 —— 列结构、其余数值、行序都不许变，
否则下游 enriched / 回测口径会被顺手改掉。
"""

from datetime import datetime
from pathlib import Path

import polars as pl

from app.quant.data_verify.repair import (
    backup_partition,
    scale_minute_volume_amount,
    write_partition_atomic,
)

SCHEMA = {
    "symbol": pl.String,
    "datetime": pl.Datetime("us"),
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "amount": pl.Float64,
}


def _write(part: Path) -> None:
    part.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [("688001.SH", datetime(2026, 11, 6, 9, 30), 1.0, 1.0, 1.0, 1.0, 863055900.0, 3.13e10)],
        schema=SCHEMA,
        orient="row",
    ).write_parquet(part)


def test_dry_run_does_not_write(tmp_path):
    part = tmp_path / "kline_minute" / "date=2026-11-06" / "part.parquet"
    _write(part)
    res = scale_minute_volume_amount(part, table="kline_minute", factor=100.0,
                                     backup_root=tmp_path / "bk", run_id="r1", dry_run=True)
    assert res["changed_rows"] == 1
    assert pl.read_parquet(part)["volume"][0] == 863055900.0
    assert not (tmp_path / "bk").exists()


def test_apply_scales_and_backs_up(tmp_path):
    part = tmp_path / "kline_minute" / "date=2026-11-06" / "part.parquet"
    _write(part)
    res = scale_minute_volume_amount(part, table="kline_minute", factor=100.0,
                                     backup_root=tmp_path / "bk", run_id="r1", dry_run=False)
    df = pl.read_parquet(part)
    assert df["volume"][0] == 8630559.0 and df["amount"][0] == 3.13e8
    assert Path(res["backup"]).exists()
    assert pl.read_parquet(res["backup"])["volume"][0] == 863055900.0


def test_backup_layout_follows_run_table_and_date(tmp_path):
    part = tmp_path / "kline_minute" / "date=2026-11-06" / "part.parquet"
    _write(part)
    out = backup_partition(part, tmp_path / "bk", "run-42")
    assert out == tmp_path / "bk" / "run-42" / "kline_minute" / "date=2026-11-06" / "part.parquet"
    assert pl.read_parquet(out).equals(pl.read_parquet(part))


def test_repeated_apply_keeps_first_pristine_backup(tmp_path):
    part = tmp_path / "kline_minute" / "date=2026-11-06" / "part.parquet"
    _write(part)
    res = scale_minute_volume_amount(part, table="kline_minute", factor=100.0,
                                     backup_root=tmp_path / "bk", run_id="r1", dry_run=False)
    scale_minute_volume_amount(part, table="kline_minute", factor=100.0,
                               backup_root=tmp_path / "bk", run_id="r1", dry_run=False)
    # 备份仍是被缩放前的原始值，重复执行不会把备份覆盖成已修的二手数据
    assert pl.read_parquet(res["backup"])["volume"][0] == 863055900.0


def test_only_volume_and_amount_change_and_schema_is_preserved(tmp_path):
    part = tmp_path / "kline_minute" / "date=2026-11-06" / "part.parquet"
    part.parent.mkdir(parents=True, exist_ok=True)
    schema = dict(SCHEMA, volume=pl.Int64, amount=pl.Int64)
    pl.DataFrame(
        [("688001.SH", datetime(2026, 11, 6, 9, 30), 1.5, 1.6, 1.4, 1.55, 863055900, 31300000000)],
        schema=schema,
        orient="row",
    ).write_parquet(part)
    before = pl.read_parquet(part)
    scale_minute_volume_amount(part, table="kline_minute", factor=100.0,
                               backup_root=tmp_path / "bk", run_id="r1", dry_run=False)
    after = pl.read_parquet(part)
    assert after.schema == before.schema              # 列名/顺序/dtype 均不变
    assert after["volume"][0] == 8630559 and after["amount"][0] == 313000000
    assert after.drop(["volume", "amount"]).equals(before.drop(["volume", "amount"]))


def test_all_zero_partition_is_noop_without_backup(tmp_path):
    part = tmp_path / "kline_minute" / "date=2026-11-06" / "part.parquet"
    part.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [("688001.SH", datetime(2026, 11, 6, 9, 30), 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)],
        schema=SCHEMA,
        orient="row",
    ).write_parquet(part)
    res = scale_minute_volume_amount(part, table="kline_minute", factor=100.0,
                                     backup_root=tmp_path / "bk", run_id="r1", dry_run=False)
    assert res == {"changed_rows": 0, "backup": None}
    assert not (tmp_path / "bk").exists()


def test_write_partition_atomic_leaves_no_tmp_behind(tmp_path):
    part = tmp_path / "kline_minute" / "date=2026-11-06" / "part.parquet"
    _write(part)
    df = pl.read_parquet(part).with_columns(pl.lit(2.0).alias("close"))
    write_partition_atomic(df, part)
    assert pl.read_parquet(part)["close"][0] == 2.0
    assert not list(part.parent.glob("*.tmp"))
    assert sorted(p.name for p in part.parent.iterdir()) == ["part.parquet"]
