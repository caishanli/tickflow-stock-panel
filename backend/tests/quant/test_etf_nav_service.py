"""etf_nav_service：akshare 全市场净值 → 按日分区落盘 + 缺失日巡检。"""
import datetime as _dt
from datetime import date

import polars as pl

from app.services import etf_nav_service as svc
from app.services import mootdx_service


def test_sync_writes_partition_and_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setenv("PARTITION_DATA_ROOT", str(tmp_path))
    import importlib
    importlib.reload(svc)

    def fake_fund_etf_fund_daily_em():
        return pl.DataFrame({
            "基金代码": ["510300", "159915", "518880"],
            "2026-08-06-单位净值": [4.7111, 3.5800, 5.0100],
            "2026-08-06-累计净值": [4.7111, 3.5800, 5.0100],
            "2026-08-07-单位净值": [4.7556, 3.5860, 5.0123],
            "2026-08-07-累计净值": [4.7556, 3.5860, 5.0123],
        })

    monkeypatch.setattr(svc, "_fund_etf_fund_daily_em", fake_fund_etf_fund_daily_em)
    day = date(2026, 8, 6)  # 传入日仅作兜底；实际分区日 = 列名解析出的披露日 08-07
    n1 = svc.sync_etf_nav(day)
    n2 = svc.sync_etf_nav(day)  # 幂等：分区已存在 → 跳过
    assert n1 == 3
    assert n2 == 0
    part = tmp_path / "etf_nav" / "date=2026-08-07" / "part.parquet"
    assert part.exists()
    df = pl.read_parquet(part)
    assert sorted(df["symbol"].to_list()) == ["159915.XSHE", "510300.XSHG", "518880.XSHG"]
    assert df["unit_nav"].dtype == pl.Float64
    assert set(df["date"].to_list()) == {"2026-08-07"}


def test_sync_drops_dash_values(tmp_path, monkeypatch):
    """akshare 当日无净值/停牌的 ETF 返回 "---"，应丢弃而非拖垮整个分区写入。"""
    monkeypatch.setenv("PARTITION_DATA_ROOT", str(tmp_path))
    import importlib
    importlib.reload(svc)

    def fake_fund_etf_fund_daily_em():
        return pl.DataFrame({
            "基金代码": ["510300", "159915", "518880"],
            "2026-08-07-单位净值": ["4.7556", "---", "5.0123"],
            "2026-08-07-累计净值": ["4.7556", "---", "5.0123"],
        })

    monkeypatch.setattr(svc, "_fund_etf_fund_daily_em", fake_fund_etf_fund_daily_em)
    n = svc.sync_etf_nav()
    assert n == 2  # "---" 行被丢弃，只写入 2 只
    part = tmp_path / "etf_nav" / "date=2026-08-07" / "part.parquet"
    assert part.exists()
    df = pl.read_parquet(part)
    assert df["symbol"].to_list() == ["510300.XSHG", "518880.XSHG"]
    assert df["unit_nav"].dtype == pl.Float64


def test_nav_date_picks_latest_column_with_data():
    """当日未披露（全 --- 占位）时选前一披露日，而非最新列名（避免写空分区）。"""
    raw = pl.DataFrame({
        "基金代码": ["510300", "159915"],
        "2026-08-13-单位净值": [4.7, 3.5],
        "2026-08-14-单位净值": ["---", "---"],
    })
    assert svc._nav_date_from_columns(raw) == date(2026, 8, 13)
    # 披露齐全后（08-14 有值）→ 推进到 08-14
    raw2 = pl.DataFrame({
        "基金代码": ["510300", "159915"],
        "2026-08-13-单位净值": [4.7, 3.5],
        "2026-08-14-单位净值": [4.8, 3.6],
    })
    assert svc._nav_date_from_columns(raw2) == date(2026, 8, 14)
    # 当日披露过半（08-14 6 行 >= 最大 10 行的 50%）→ 推进到当日
    raw_partial = pl.DataFrame({
        "基金代码": [str(i) for i in range(10)],
        "2026-08-13-单位净值": ["1.0"] * 10,
        "2026-08-14-单位净值": ["1.0"] * 6 + ["---"] * 4,
    })
    assert svc._nav_date_from_columns(raw_partial) == date(2026, 8, 14)
    # 当日披露稀疏（08-14 2 行 < 50%）→ 回落前一日，避免过早写稀疏分区
    raw_sparse = pl.DataFrame({
        "基金代码": [str(i) for i in range(10)],
        "2026-08-13-单位净值": ["1.0"] * 10,
        "2026-08-14-单位净值": ["1.0"] * 2 + ["---"] * 8,
    })
    assert svc._nav_date_from_columns(raw_sparse) == date(2026, 8, 13)
    # 全部无有效值 → None（调用方跳过落盘）
    raw3 = pl.DataFrame({
        "基金代码": ["510300", "159915"],
        "2026-08-14-单位净值": ["---", "---"],
    })
    assert svc._nav_date_from_columns(raw3) is None


def test_sync_rewrites_sparse_partition(tmp_path, monkeypatch):
    """已存在但稀疏的分区（过早写入的占位）→ 覆盖重写为全量快照。"""
    monkeypatch.setenv("PARTITION_DATA_ROOT", str(tmp_path))
    import importlib
    importlib.reload(svc)

    def fake_fund_etf_fund_daily_em():
        return pl.DataFrame({
            "基金代码": ["510300", "159915", "518880"],
            "2026-08-13-单位净值": ["4.7", "3.5", "5.0"],
        })

    monkeypatch.setattr(svc, "_fund_etf_fund_daily_em", fake_fund_etf_fund_daily_em)
    part = tmp_path / "etf_nav" / "date=2026-08-13" / "part.parquet"
    part.parent.mkdir(parents=True)
    pl.DataFrame({"symbol": ["510300.XSHG"], "unit_nav": [4.7],
                  "date": ["2026-08-13"]}).write_parquet(part)  # 旧版稀疏占位
    n = svc.sync_etf_nav()
    assert n == 3
    assert pl.read_parquet(part).height == 3


def test_missing_nav_days_sparse_partition_not_covered(tmp_path, monkeypatch):
    """空/稀疏分区不掩盖缺失：_latest_valid_nav_date 只看有数据的分区。"""
    monkeypatch.setenv("PARTITION_DATA_ROOT", str(tmp_path))
    import importlib
    importlib.reload(svc)
    _patch_trade_days(monkeypatch)
    # 08-06 有数据，08-07 只有空目录 → 最新有效 = 08-06 < 候选 08-07 → 仍缺失
    (tmp_path / "etf_nav" / "date=2026-08-06").mkdir(parents=True)
    pl.DataFrame({"symbol": ["510300.XSHG"], "unit_nav": [4.7],
                  "date": ["2026-08-06"]}).write_parquet(
        tmp_path / "etf_nav" / "date=2026-08-06" / "part.parquet")
    (tmp_path / "etf_nav" / "date=2026-08-07").mkdir(parents=True)
    assert svc._latest_valid_nav_date() == date(2026, 8, 6)
    assert svc._missing_etf_nav_days(now=_dt.datetime(2026, 8, 7, 15, 30)) == [_dt.date(2026, 8, 7)]


def test_symbol_market_mapping():
    # 5/6/9 开头 → 沪市 XSHG；0/1/2/3 开头 → 深市 XSHE
    assert svc._jq_symbol("510300") == "510300.XSHG"
    assert svc._jq_symbol("159915") == "159915.XSHE"
    assert svc._jq_symbol("518880") == "518880.XSHG"


def _patch_trade_days(monkeypatch):
    """把 _missing_etf_nav_days 依赖的交易日历 pin 死：8/6(周四)、8/7(周五)。"""
    from app.services import mootdx_service
    monkeypatch.setattr(mootdx_service, "_trade_days_up_to",
                        lambda end: [_dt.date(2026, 8, 6), _dt.date(2026, 8, 7)])


def test_missing_nav_days_respects_market_close(tmp_path, monkeypatch):
    monkeypatch.setenv("PARTITION_DATA_ROOT", str(tmp_path))
    import importlib
    importlib.reload(svc)
    _patch_trade_days(monkeypatch)
    # 盘中（<15:00）当日净值未披露 → 候选 = 前一交易日
    assert svc._missing_etf_nav_days(now=_dt.datetime(2026, 8, 7, 14, 0)) == [_dt.date(2026, 8, 6)]
    # 收盘后当日净值已披露 → 候选 = 最新交易日
    missing = svc._missing_etf_nav_days(now=_dt.datetime(2026, 8, 7, 15, 30))
    assert missing == [_dt.date(2026, 8, 7)]
    # 已有空分区（无有效数据，如过早写入的占位）→ 仍视为缺失
    (tmp_path / "etf_nav" / "date=2026-08-07").mkdir(parents=True)
    assert svc._missing_etf_nav_days(now=_dt.datetime(2026, 8, 7, 15, 30)) == [_dt.date(2026, 8, 7)]
    # 已有含数据的当日分区 → 不再缺失
    part = tmp_path / "etf_nav" / "date=2026-08-07" / "part.parquet"
    pl.DataFrame({"symbol": ["510300.XSHG"], "unit_nav": [4.7], "date": ["2026-08-07"]}).write_parquet(part)
    assert svc._missing_etf_nav_days(now=_dt.datetime(2026, 8, 7, 15, 30)) == []


def test_missing_nav_days_midnight_targets_previous_trading_day(tmp_path, monkeypatch):
    """00:00 巡检：今日为交易日但净值未披露 → 目标前一交易日（修复 00:00 no-op）。"""
    monkeypatch.setenv("PARTITION_DATA_ROOT", str(tmp_path))
    import importlib
    importlib.reload(svc)
    _patch_trade_days(monkeypatch)
    missing = svc._missing_etf_nav_days(now=_dt.datetime(2026, 8, 7, 0, 0))
    assert missing == [_dt.date(2026, 8, 6)]
    assert len(missing) <= 1


def test_missing_nav_days_weekend_targets_latest_trading_day(tmp_path, monkeypatch):
    """周末巡检（非交易日）：候选 = 最近一个交易日（周五净值已披露）。"""
    monkeypatch.setenv("PARTITION_DATA_ROOT", str(tmp_path))
    import importlib
    importlib.reload(svc)
    _patch_trade_days(monkeypatch)
    # 8/8 是周六，8/7 是周五交易日；周六净值无新披露 → 候选 = 8/7
    missing = svc._missing_etf_nav_days(now=_dt.datetime(2026, 8, 8, 10, 0))
    assert missing == [_dt.date(2026, 8, 7)]
    assert len(missing) <= 1


def test_missing_nav_days_only_syncs_latest_not_history(tmp_path, monkeypatch):
    """历史不补：只有最新缺失交易日会被回源，不会写假历史分区。

    akshare fund_etf_fund_daily_em 只有当前快照（无逐日历史），回补历史只会
    把今日净值写成过去每一天（假数据）。因此 _missing_etf_nav_days 最多返回
    **一个**候选日（最新分区之后最近一个净值应已披露的交易日）。
    """
    monkeypatch.setenv("PARTITION_DATA_ROOT", str(tmp_path))
    import importlib
    importlib.reload(svc)
    _patch_trade_days(monkeypatch)
    # 已有 08-05 分区，今天是 08-07 收盘后 → 只缺 08-07，不返回 08-06 之前的历史
    (tmp_path / "etf_nav" / "date=2026-08-05").mkdir(parents=True)
    missing = svc._missing_etf_nav_days(now=_dt.datetime(2026, 8, 7, 15, 30))
    assert missing == [_dt.date(2026, 8, 7)]
    assert len(missing) <= 1  # 历史不补的核心断言


def test_missing_nav_days_empty_root_returns_single_latest(tmp_path, monkeypatch):
    """冷启动空分区：只返回最近一个交易日，不铺开 4/1 以来的全部历史。"""
    monkeypatch.setenv("PARTITION_DATA_ROOT", str(tmp_path))
    import importlib
    importlib.reload(svc)
    _patch_trade_days(monkeypatch)
    missing = svc._missing_etf_nav_days(now=_dt.datetime(2026, 8, 7, 15, 30))
    assert missing == [_dt.date(2026, 8, 7)]
    assert len(missing) <= 1


def test_scan_missing_partitions_has_etf_nav(tmp_path, monkeypatch):
    monkeypatch.setenv("PARTITION_DATA_ROOT", str(tmp_path))
    import importlib
    importlib.reload(svc)
    importlib.reload(mootdx_service)
    missing = mootdx_service.scan_missing_partitions()
    assert "etf_nav" in missing
    assert isinstance(missing["etf_nav"], list)
    assert len(missing["etf_nav"]) <= 1


def test_backfill_to_now_includes_etf_nav(tmp_path, monkeypatch):
    monkeypatch.setenv("PARTITION_DATA_ROOT", str(tmp_path))
    import importlib
    importlib.reload(svc)
    importlib.reload(mootdx_service)

    def fake_sync(day=None):
        return 3

    def fake_missing(now=None):
        return [_dt.date(2026, 8, 7)]

    monkeypatch.setattr(svc, "sync_etf_nav", fake_sync)
    monkeypatch.setattr(svc, "_missing_etf_nav_days", fake_missing)
    # 避免触发真实网络回源（mootdx 可达时 backfill_to_now 会真的回源几十个
    # 交易日）。对齐 test_mootdx_backfill_coverage 的既有 backfill 测试口径，
    # 把所有网络 sync 都 mock 成 no-op，只验证 etf_nav 接线。
    monkeypatch.setattr(mootdx_service, "_missing_minute_days", lambda now=None: [])
    monkeypatch.setattr(mootdx_service, "_missing_daily_days", lambda root, now=None: [])
    monkeypatch.setattr(mootdx_service, "_missing_index_daily_days", lambda: [])
    monkeypatch.setattr(mootdx_service, "_trade_days_up_to", lambda end: [])
    monkeypatch.setattr(mootdx_service, "_adj_factor_stale", lambda: False)
    monkeypatch.setattr(mootdx_service, "sync_etf_minute", lambda day=None: 0)
    monkeypatch.setattr(mootdx_service, "sync_daily", lambda day: {"stock": 0, "etf": 0})
    monkeypatch.setattr(mootdx_service, "sync_index_daily", lambda day: {"written": 0})
    monkeypatch.setattr(mootdx_service, "sync_adj_factor", lambda: None)
    monkeypatch.setattr(mootdx_service, "sync_stock_minute", lambda limit=None: 0)
    monkeypatch.setattr(mootdx_service, "_notify_missing", lambda m: None)

    res = mootdx_service.backfill_to_now()
    assert "etf_nav_days" in res
    assert "etf_nav" in res.get("missing", {})


def test_sync_retries_empty_once(tmp_path, monkeypatch):
    """NAV 链路：akshare 偶发空结果重试一次，第二次成功即正常落盘。"""
    monkeypatch.setenv("PARTITION_DATA_ROOT", str(tmp_path))
    import importlib
    importlib.reload(svc)
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            return pl.DataFrame()
        return pl.DataFrame({
            "基金代码": ["510300"],
            "2026-08-06-单位净值": [4.7111],
            "2026-08-06-累计净值": [4.7111],
        })

    monkeypatch.setattr(svc, "_fund_etf_fund_daily_em", flaky)
    n = svc.sync_etf_nav(date(2026, 8, 6))
    assert calls["n"] == 2 and n == 1
