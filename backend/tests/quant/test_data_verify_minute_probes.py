"""分钟外部源探针 + ``external_minute`` stage（离线 fixture，不触网）。

覆盖：

- 腾讯 / 新浪 / eltdx 三个 probe 的解析口径（volume 一律折成「股」）；
- ``available_sources`` 不得为探测 eltdx 而急切 import；
- runner 的 ``external_minute`` stage：抽样、逐 bar 比对、多源仲裁修复、
  冲突不修、无源跳过、dry-run 零写入。
"""

from __future__ import annotations

import importlib.util
import os
from dataclasses import dataclass
from datetime import date, datetime

import polars as pl
import pytest

from app.quant.data_verify.probes import eltdx_m1, sina_m1, tencent_m1
from app.quant.data_verify.probes.eltdx_m1 import parse_eltdx_bars
from app.quant.data_verify.probes.sina_m1 import fetch_sina_m1, parse_sina_m1
from app.quant.data_verify.probes.tencent_m1 import (
    fetch_tencent_m1,
    parse_tencent_m1,
)

LIVE = os.environ.get("ASTOCK_LIVE") == "1"

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


# ---------------------------------------------------------------------------
# Step 1（brief 指定的两条）
# ---------------------------------------------------------------------------


def test_parse_tencent_m1_star_keeps_shares():
    rows = [["202611061500", "35.69", "36.68", "36.68", "35.69", "95429.00", {}, "2.02"]]
    df = parse_tencent_m1("688001.SH", rows)
    assert df["volume"][0] == 95429.0 and df.schema["datetime"] == pl.Datetime("us")


def test_parse_sina_m1_volume_is_shares():
    payload = [{"day": "2026-11-06 15:00:00", "open": "10.03", "high": "10.03",
                "low": "10.03", "close": "10.03", "volume": "955500", "amount": "9583665"}]
    df = parse_sina_m1("600000.SH", payload)
    assert df["volume"][0] == 955500.0


# ---------------------------------------------------------------------------
# probe 解析口径补充
# ---------------------------------------------------------------------------


def test_parse_tencent_m1_mainboard_lots_to_shares():
    """主板原始量是「手」：9555 手 → 955,500 股（与新浪同 bar 一致）。"""
    rows = [["202611061500", "10.03", "10.03", "10.03", "10.03", "9555.00", {}, "0.29"]]
    df = parse_tencent_m1("600000.SH", rows)
    assert df["volume"][0] == 955500.0
    assert df["symbol"][0] == "600000.SH"
    assert df["datetime"][0] == datetime(2026, 11, 6, 15, 0)


def test_parse_tencent_m1_days_filter_and_empty_schema():
    rows = [["202611061500", "10.03", "10.03", "10.03", "10.03", "9555.00", {}, "0.29"]]
    assert parse_tencent_m1("600000.SH", rows, days={date(2026, 11, 5)}).is_empty()
    empty = parse_tencent_m1("600000.SH", [])
    assert empty.is_empty()
    assert empty.columns == list(MINUTE_SCHEMA)


def test_parse_sina_m1_estimates_amount_with_attr():
    payload = [{"day": "2026-11-06 09:31:00", "open": "10.00", "high": "10.10",
                "low": "10.00", "close": "10.05", "volume": "1000"}]
    df = parse_sina_m1("600000.SH", payload)
    assert getattr(df, "attrs", {}).get("amount") == "estimated"
    assert df["amount"][0] == pytest.approx(1000 * (10.10 + 10.00 + 10.05) / 3)


def test_parse_sina_m1_uses_provided_amount():
    payload = [{"day": "2026-11-06 09:31:00", "open": "10.00", "high": "10.10",
                "low": "10.00", "close": "10.05", "volume": "1000", "amount": "10040"}]
    df = parse_sina_m1("600000.SH", payload)
    assert df["amount"][0] == 10040.0
    assert getattr(df, "attrs", {}).get("amount") != "estimated"


@dataclass
class _FakeBar:
    time: str
    open: float
    high: float
    low: float
    close: float
    volume_lots: float
    amount: float


def test_eltdx_vendor_rejects_convertible_bonds():
    """可转债一手 10 张，×100 换股数是错的——直接不核验。"""
    assert eltdx_m1._vendor("113001.SH") is None
    assert eltdx_m1._vendor("123001.SZ") is None
    assert eltdx_m1._vendor("600000.SH") == "sh600000"


def test_parse_eltdx_bars_lots_to_shares_naive_beijing():
    bar = _FakeBar("2026-11-06T15:00:00+08:00", 10.0, 10.1, 9.9, 10.05, 9555.0, 9_600_000.0)
    df = parse_eltdx_bars("600000.SH", [bar])
    assert df["volume"][0] == 955500.0
    assert df["datetime"][0] == datetime(2026, 11, 6, 15, 0)
    assert df.schema["datetime"] == pl.Datetime("us")


def test_eltdx_available_sources_does_not_import_eltdx(monkeypatch):
    seen: list[str] = []

    def fake_find_spec(name: str):
        seen.append(name)
        return None

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)
    assert eltdx_m1.available_sources() == []
    assert seen == ["eltdx"]


def test_http_probe_modules_report_sources():
    assert tencent_m1.available_sources() == ["tencent"]
    assert sina_m1.available_sources() == ["sina"]


@pytest.mark.skipif(not LIVE, reason="设置 ASTOCK_LIVE=1 才跑真实抓取")
def test_tencent_and_sina_agree_on_last_star_bar():
    """联网冒烟：科创板 15:00 bar 的 volume 都是「股」，两源应一致。"""
    tencent = fetch_tencent_m1("688001.SH")
    assert tencent.height >= 240
    last = tencent["datetime"].max()
    sina = fetch_sina_m1("688001.SH").filter(pl.col("datetime") == last)
    assert sina.height == 1
    assert tencent.filter(pl.col("datetime") == last)["volume"][0] == pytest.approx(
        sina["volume"][0], rel=1e-6
    )


# ---------------------------------------------------------------------------
# runner external_minute stage
# ---------------------------------------------------------------------------


def _bars(symbol: str, day: date, *, close: float = 10.0, volume: float = 1000.0) -> pl.DataFrame:
    times = ("09:30", "09:31", "15:00")
    rows = [
        (symbol, datetime.fromisoformat(f"{day.isoformat()}T{t}:00"),
         close, close, close, close, volume, volume * close)
        for t in times
    ]
    return pl.DataFrame(rows, schema=MINUTE_SCHEMA, orient="row")


def _write_minute_partition(root, day: date, df: pl.DataFrame) -> None:
    part = root / "kline_minute" / f"date={day.isoformat()}" / "part.parquet"
    part.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(part)


def _source(name: str, frame: pl.DataFrame, priority: int = 3):
    from app.quant.data_verify.runner import _MinuteSource

    return _MinuteSource(name=name, fetch=lambda _symbol: frame, priority=priority)


def _run_external(root, monkeypatch, sources, *, apply=True, **kwargs):
    from app.quant.data_verify import runner

    monkeypatch.setattr(runner, "_configured_sources", lambda: sources)
    return runner.run_verify(
        "external_minute",
        apply=apply,
        days=[date(2026, 11, 5)],
        data_root=root,
        **kwargs,
    )


DAY = date(2026, 11, 5)
SYMBOL = "600000.SH"


def test_external_minute_repairs_when_two_sources_agree(tmp_path, monkeypatch):
    _write_minute_partition(tmp_path, DAY, _bars(SYMBOL, DAY, close=10.0, volume=1000.0))
    corrected = _bars(SYMBOL, DAY, close=10.5, volume=2000.0)

    rep = _run_external(
        tmp_path,
        monkeypatch,
        [_source("sina", corrected, 2), _source("tencent", corrected, 3)],
    )

    assert rep["repaired"] == 1
    assert rep["findings"] > 0
    local = pl.read_parquet(
        tmp_path / "kline_minute" / f"date={DAY.isoformat()}" / "part.parquet"
    )
    assert local["close"].to_list() == [10.5, 10.5, 10.5]
    assert local["volume"].to_list() == [2000.0, 2000.0, 2000.0]
    assert len(list((tmp_path / "data_verify_backup").rglob("part.parquet"))) == 1


def test_external_minute_conflict_keeps_local(tmp_path, monkeypatch):
    """一源说本地错、一源说本地对 → 无多数，不修（design §5.4）。"""
    local_df = _bars(SYMBOL, DAY, close=10.0, volume=1000.0)
    _write_minute_partition(tmp_path, DAY, local_df)
    corrected = _bars(SYMBOL, DAY, close=10.5, volume=2000.0)

    rep = _run_external(
        tmp_path,
        monkeypatch,
        [_source("sina", local_df, 2), _source("tencent", corrected, 3)],
    )

    assert rep["repaired"] == 0
    assert rep["findings"] > 0
    local = pl.read_parquet(
        tmp_path / "kline_minute" / f"date={DAY.isoformat()}" / "part.parquet"
    )
    assert local["close"].unique().sort().to_list() == [10.0]
    assert not (tmp_path / "data_verify_backup").exists()


def test_external_minute_single_source_repairs_with_flag(tmp_path, monkeypatch):
    _write_minute_partition(tmp_path, DAY, _bars(SYMBOL, DAY, close=10.0))
    corrected = _bars(SYMBOL, DAY, close=10.5)
    rep = _run_external(tmp_path, monkeypatch, [_source("tencent", corrected, 3)])
    assert rep["repaired"] == 1


def test_external_minute_dry_run_writes_nothing(tmp_path, monkeypatch):
    _write_minute_partition(tmp_path, DAY, _bars(SYMBOL, DAY, close=10.0))
    corrected = _bars(SYMBOL, DAY, close=10.5)
    rep = _run_external(
        tmp_path,
        monkeypatch,
        [_source("sina", corrected, 2), _source("tencent", corrected, 3)],
        apply=False,
    )
    assert rep["findings"] > 0 and rep["repaired"] == 0
    local = pl.read_parquet(
        tmp_path / "kline_minute" / f"date={DAY.isoformat()}" / "part.parquet"
    )
    assert local["close"].unique().sort().to_list() == [10.0]
    assert not (tmp_path / "data_verify_backup").exists()
    assert not (tmp_path / "data_verify_state.json").exists()


def test_external_minute_no_source_skips_without_raising(tmp_path, monkeypatch):
    _write_minute_partition(tmp_path, DAY, _bars(SYMBOL, DAY))
    rep = _run_external(tmp_path, monkeypatch, [])
    assert rep["skipped"] == "no_source"
    assert rep["findings"] == 0 and rep["repaired"] == 0


def test_external_minute_unreachable_sources_skip(tmp_path, monkeypatch):
    from app.quant.data_verify.runner import _MinuteSource

    def boom(_symbol):
        raise RuntimeError("offline")

    _write_minute_partition(tmp_path, DAY, _bars(SYMBOL, DAY))
    from app.quant.data_verify import runner

    monkeypatch.setattr(
        runner, "_configured_sources", lambda: [_MinuteSource("tencent", boom, 3)]
    )
    rep = runner.run_verify(
        "external_minute", days=[DAY], data_root=tmp_path
    )
    assert rep["skipped"] == "no_source"
    assert rep["days"] == 0 and rep["findings"] == 0


def test_all_stage_still_runs_internal_when_external_is_skipped(tmp_path, monkeypatch):
    from app.quant.data_verify import runner

    _write_minute_partition(tmp_path, DAY, _bars(SYMBOL, DAY))
    monkeypatch.setattr(runner, "_configured_sources", lambda: [])
    rep = runner.run_verify("all", days=[DAY], data_root=tmp_path)
    assert rep["days"] == 1                      # structure/internal 照常跑
    assert rep["skipped"] == "no_source"


def test_external_minute_missing_bar_is_detected(tmp_path, monkeypatch):
    _write_minute_partition(tmp_path, DAY, _bars(SYMBOL, DAY))
    extra = _bars(SYMBOL, DAY).vstack(
        pl.DataFrame(
            [(SYMBOL, datetime(2026, 11, 5, 10, 0), 10.0, 10.0, 10.0, 10.0, 1.0, 10.0)],
            schema=MINUTE_SCHEMA,
            orient="row",
        )
    )
    rep = _run_external(
        tmp_path,
        monkeypatch,
        [_source("sina", extra, 2), _source("tencent", extra, 3)],
    )
    assert rep["repaired"] == 1
    local = pl.read_parquet(
        tmp_path / "kline_minute" / f"date={DAY.isoformat()}" / "part.parquet"
    )
    assert local.height == 4                       # 外部独有的 bar 被补进来


def test_configured_sources_excludes_missing_eltdx():
    from app.quant.data_verify import runner

    names = [source.name for source in runner._configured_sources()]
    assert names[:2] == ["tencent", "sina"] or names[0] == "eltdx"
    if importlib.util.find_spec("eltdx") is None:
        assert "eltdx" not in names


def test_external_repair_keeps_local_only_bars(tmp_path, monkeypatch):
    """外部源覆盖不全时不能删本地 bar（design §6.3）。"""
    local = _bars(SYMBOL, DAY, close=10.0).vstack(
        pl.DataFrame(
            [(SYMBOL, datetime(2026, 11, 5, 10, 0), 10.0, 10.0, 10.0, 10.0, 1.0, 10.0)],
            schema=MINUTE_SCHEMA,
            orient="row",
        )
    )
    _write_minute_partition(tmp_path, DAY, local)
    corrected = _bars(SYMBOL, DAY, close=10.5)
    _run_external(
        tmp_path,
        monkeypatch,
        [_source("sina", corrected, 2), _source("tencent", corrected, 3)],
    )
    got = pl.read_parquet(
        tmp_path / "kline_minute" / f"date={DAY.isoformat()}" / "part.parquet"
    )
    assert got.height == 4
    assert got.filter(pl.col("datetime") == datetime(2026, 11, 5, 10, 0)).height == 1


def test_diff_stats_skips_0930_for_all_and_0931_only_for_sina():
    """09:30 对所有源跳过；09:31 只有新浪跳过（腾讯 09:31 的真实错误必须报）。"""
    from app.quant.data_verify.runner import _diff_stats

    local = _bars(SYMBOL, DAY, close=10.0, volume=1000.0)
    external = local.with_columns(
        pl.when(pl.col("datetime") == datetime(2026, 11, 5, 9, 31))
        .then(pl.lit(20.0)).otherwise(pl.col("close")).alias("close")
    )
    assert _diff_stats(local, external)["close_mismatch"] == 1          # tencent
    assert _diff_stats(local, external, skip_0931=True)["close_mismatch"] == 0  # sina


def test_diff_stats_detects_redistribution_with_total_check():
    """逐 bar 都在容差内、但总量不守恒（重新分配）必须报 aggregate。"""
    from app.quant.data_verify.runner import _diff_stats

    local = _bars(SYMBOL, DAY, close=10.0, volume=1000.0)
    external = local.with_columns((pl.col("volume") + 6).alias("volume"))
    stats = _diff_stats(local, external)
    # 逐 bar：6 <= 当日总量 3000 × 0.5% = 15，全部过
    assert stats["volume_mismatch"] == 0
    assert stats["volume_tolerance"] == pytest.approx(15.0)
    # 总量：非竞价 bar（09:31+15:00）Σ差 12 > 2000 × 0.5% = 10
    assert stats["volume_total_mismatch"] is True
    assert stats["volume_total_tolerance"] == pytest.approx(10.0)


def test_resolve_replacement_conflicting_singletons_is_quarantine():
    """两个外源互相矛盾（各成单元素簇）→ 不得靠优先级挑一个 single_source 修复。"""
    from app.quant.data_verify.runner import _resolve_replacement

    local = _bars(SYMBOL, DAY, close=10.0)
    tencent = _bars(SYMBOL, DAY, close=10.5)
    sina = _bars(SYMBOL, DAY, close=11.0)
    replacement, decision = _resolve_replacement(
        local, {"tencent": tencent, "sina": sina}
    )
    assert replacement is None
    assert decision["action"] == "quarantine"
    assert decision["conflict"] is True
    assert decision["single_source"] is False


def test_external_minute_conflicting_sources_do_not_repair(tmp_path, monkeypatch):
    _write_minute_partition(tmp_path, DAY, _bars(SYMBOL, DAY, close=10.0))
    rep = _run_external(
        tmp_path,
        monkeypatch,
        [
            _source("sina", _bars(SYMBOL, DAY, close=11.0), 2),
            _source("tencent", _bars(SYMBOL, DAY, close=10.5), 3),
        ],
    )
    assert rep["repaired"] == 0
    local = pl.read_parquet(
        tmp_path / "kline_minute" / f"date={DAY.isoformat()}" / "part.parquet"
    )
    assert local["close"].unique().to_list() == [10.0]


def test_sample_symbols_is_deterministic_and_evenly_strided():
    from app.quant.data_verify.runner import _sample_symbols

    symbols = [f"6{i:05d}.SH" for i in range(10)]
    picked = _sample_symbols(symbols, 3)
    assert picked == ["600000.SH", "600003.SH", "600006.SH"]
    assert _sample_symbols(symbols, 100) == sorted(symbols)
    assert _sample_symbols(symbols, 0) == sorted(symbols)


def test_resolve_replacement_marks_single_source():
    from app.quant.data_verify.runner import _resolve_replacement

    local = _bars(SYMBOL, DAY, close=10.0)
    corrected = _bars(SYMBOL, DAY, close=10.5)
    replacement, decision = _resolve_replacement(local, {"tencent": corrected})
    assert replacement is not None
    assert replacement.height == 3
    assert decision["single_source"] is True and decision["confirmed"] is False


def test_external_fetch_is_memoized_per_source_symbol(tmp_path, monkeypatch):
    """逐日循环不能把同一只标的的完整历史重复拉 94 次。"""
    from app.quant.data_verify import runner
    from app.quant.data_verify.runner import _MinuteSource

    day1, day2 = date(2026, 11, 4), date(2026, 11, 5)
    frame1, frame2 = _bars(SYMBOL, day1), _bars(SYMBOL, day2)
    _write_minute_partition(tmp_path, day1, frame1)
    _write_minute_partition(tmp_path, day2, frame2)
    full = pl.concat([frame1, frame2])
    calls: list[str] = []

    def fetch(symbol):
        calls.append(symbol)
        return full

    monkeypatch.setattr(
        runner, "_configured_sources",
        lambda: [_MinuteSource("tencent", fetch, 3)],
    )
    rep = runner.run_verify("external_minute", days=[day1, day2], data_root=tmp_path)
    assert rep["days"] == 2
    assert calls == [SYMBOL]              # probe + 2 天共用同一次全量拉取


def test_merge_bars_only_rewrites_mismatching_keys(tmp_path):
    """确认修复只覆盖真正不一致的 bar，容差内的 tick 差异不动。"""
    from app.quant.data_verify.runner import _merge_bars

    df = _bars(SYMBOL, DAY, close=10.0)
    replacement = df.with_columns([
        pl.when(pl.col("datetime").dt.hour() == 15)
        .then(pl.lit(10.5)).otherwise(pl.col("close")).alias("close"),
        # 09:31 的 0.001 差异在容差内，不应触发整行重写
        pl.when(pl.col("datetime") == datetime(2026, 11, 5, 9, 31))
        .then(pl.col("open") + 0.001).otherwise(pl.col("open")).alias("open"),
    ])
    merged, changed = _merge_bars(tmp_path / "part.parquet", df, replacement)
    assert changed == 1
    early = merged.filter(pl.col("datetime") == datetime(2026, 11, 5, 9, 31))
    assert early["open"][0] == 10.0
    late = merged.filter(pl.col("datetime").dt.hour() == 15)
    assert late["close"][0] == 10.5


def test_external_minute_lookback_limits_days(tmp_path, monkeypatch):
    _write_minute_partition(tmp_path, date(2026, 11, 4), _bars(SYMBOL, date(2026, 11, 4)))
    _write_minute_partition(tmp_path, date(2026, 11, 5), _bars(SYMBOL, DAY))
    same = _bars(SYMBOL, DAY)
    from app.quant.data_verify import runner

    monkeypatch.setattr(
        runner, "_configured_sources",
        lambda: [_source("sina", same, 2), _source("tencent", same, 3)],
    )
    rep = runner.run_verify(
        "external_minute", apply=False, days=None, data_root=tmp_path, lookback_days=1
    )
    assert rep["days"] == 1
