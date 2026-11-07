"""外部覆盖修复 + 下游 enriched 重算。

外部源判定本地某几只标的是错的时，修复动作是「按 symbol 整行替换」：

- 只动 ``replacement`` 里出现的 symbol，其余标的一行都不许变；
- 分区列结构原样保留（``kline_etf_daily`` 某些历史分区没有 ``date`` 列，
  不能因为外部帧带了 ``date`` 就凭空加一列）；
- 写盘前必须备份、写入走原子替换（复用 Task 4 的 ``backup_partition`` /
  ``write_partition_atomic``）。

日线族一改，下游 ``*_enriched`` 不会自动失效（增量只算不存在的日期），
所以修复日必须 prune 后重跑 pipeline。``recompute_enriched`` 只做编排，
真正的 prune / pipeline 由调用方注入，失败只报告不抛异常（不回滚已修数据）。
"""

from datetime import date

import polars as pl

from app.quant.data_verify.recompute import recompute_enriched
from app.quant.data_verify.repair import replace_symbols


def test_replace_symbols_preserves_etf_daily_without_date_column(tmp_path):
    part = tmp_path / "kline_etf_daily" / "date=2026-11-06" / "part.parquet"
    part.parent.mkdir(parents=True)
    schema = {"symbol": pl.String, "open": pl.Float64, "high": pl.Float64,
              "low": pl.Float64, "close": pl.Float64,
              "volume": pl.Float64, "amount": pl.Float64}
    pl.DataFrame([("510300.SH", 4.0, 4.1, 3.9, 4.05, 1000.0, 4000.0)],
                 schema=schema, orient="row").write_parquet(part)
    repl = pl.DataFrame([("510300.SH", 4.0, 4.1, 3.9, 4.12, 1000.0, 4000.0)],
                        schema=schema, orient="row")
    replace_symbols(part, repl, backup_root=tmp_path / "bk", run_id="r1", dry_run=False)
    out = pl.read_parquet(part)
    assert out["close"][0] == 4.12 and "date" not in out.columns


def test_replace_symbols_leaves_other_symbols_and_backs_up_original(tmp_path):
    part = tmp_path / "kline_daily" / "date=2026-11-06" / "part.parquet"
    part.parent.mkdir(parents=True)
    schema = {"symbol": pl.String, "date": pl.Date, "open": pl.Float64,
              "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
              "volume": pl.Float64, "amount": pl.Float64}
    pl.DataFrame(
        [("600000.SH", date(2026, 11, 6), 1.0, 1.1, 0.9, 1.0, 10.0, 10.0),
         ("600519.SH", date(2026, 11, 6), 2.0, 2.1, 1.9, 2.0, 20.0, 40.0)],
        schema=schema, orient="row",
    ).write_parquet(part)
    repl = pl.DataFrame(
        [("600519.SH", date(2026, 11, 6), 2.0, 2.1, 1.9, 2.5, 21.0, 52.5)],
        schema=schema, orient="row",
    )
    res = replace_symbols(part, repl, backup_root=tmp_path / "bk", run_id="r1",
                          dry_run=False)
    out = pl.read_parquet(part)
    assert out.filter(pl.col("symbol") == "600000.SH").equals(
        pl.read_parquet(res["backup"]).filter(pl.col("symbol") == "600000.SH")
    )
    assert out.filter(pl.col("symbol") == "600519.SH")["close"][0] == 2.5
    assert out.schema == pl.read_parquet(res["backup"]).schema
    assert res["changed_rows"] == 1


def test_replace_symbols_dry_run_does_not_write_or_backup(tmp_path):
    part = tmp_path / "kline_daily" / "date=2026-11-06" / "part.parquet"
    part.parent.mkdir(parents=True)
    schema = {"symbol": pl.String, "close": pl.Float64}
    pl.DataFrame([("600519.SH", 2.0)], schema=schema, orient="row").write_parquet(part)
    repl = pl.DataFrame([("600519.SH", 2.5)], schema=schema, orient="row")
    res = replace_symbols(part, repl, backup_root=tmp_path / "bk", run_id="r1",
                          dry_run=True)
    assert res == {"changed_rows": 1, "backup": None}
    assert pl.read_parquet(part)["close"][0] == 2.0
    assert not (tmp_path / "bk").exists()


def test_recompute_prunes_and_calls_runner(tmp_path):
    called = []

    def fake_prune(data_dir, start, table):
        called.append(("prune", str(start), table))

    def fake_runner(repo, capset, days):
        called.append(("run", tuple(str(d) for d in days)))

    res = recompute_enriched([date(2026, 11, 5), date(2026, 11, 6)],
                             tables=["kline_daily_enriched"],
                             prune=fake_prune, runner=fake_runner)
    assert res["days"] == 2 and res["ok"] is True
    assert called[0] == ("prune", "2026-11-05", "kline_daily_enriched")
    assert called[-1] == ("run", ("2026-11-05", "2026-11-06"))


def test_recompute_prunes_every_table_from_earliest_day():
    called = []

    def fake_prune(data_dir, start, table):
        called.append(("prune", str(start), table))

    def fake_runner(repo, capset, days):
        called.append(("run", tuple(str(d) for d in days)))

    res = recompute_enriched([date(2026, 11, 6), date(2026, 11, 5)],
                             tables=["kline_daily_enriched", "kline_etf_enriched"],
                             prune=fake_prune, runner=fake_runner)
    assert res["ok"] is True
    assert called[:2] == [("prune", "2026-11-05", "kline_daily_enriched"),
                          ("prune", "2026-11-05", "kline_etf_enriched")]
    assert called[-1] == ("run", ("2026-11-05", "2026-11-06"))


def test_recompute_empty_days_is_noop():
    called = []

    def fake_prune(data_dir, start, table):
        called.append(("prune", table))

    def fake_runner(repo, capset, days):
        called.append(("run", days))

    res = recompute_enriched([], tables=["kline_daily_enriched"],
                             prune=fake_prune, runner=fake_runner)
    assert res == {"ok": True, "days": 0, "error": None}
    assert called == []


def test_recompute_failure_reported_not_raised():
    def boom(*a, **k):
        raise RuntimeError("pipeline down")
    res = recompute_enriched([date(2026, 11, 6)], tables=["kline_daily_enriched"],
                             prune=lambda *a, **k: None, runner=boom)
    assert res["ok"] is False and "pipeline down" in res["error"]
