"""交易日历 fallback 回归测试。

2026-09-11 事故：mootdx 全局不可用时 `_trade_days_up_to` 退回工作日近似，
把端午休市日 2026-06-19（周五）误判为交易日，导致启动 backfill 与备用链
为空跑全市场。修复后应优先用本地 kline_index_daily 的 000300 bar 推导。

2026-09-14 事故：mootdx 全天故障，而"本地 index 分区"这条 fallback 是自指的
——分区缺失恰是待补的缺口，于是日历末端停在上一交易日、今天被判为非交易日、
缺口检测为空，备用链被自己的前置条件挡死（0/3），当日日线全线缺失。修复后
权威日历（新浪）插在 mootdx 与本地分区之间，破环。
"""
from datetime import date

import polars as pl

import app.services.mootdx_service as ms

D618 = date(2026, 6, 18)  # 周四，交易日
D619 = date(2026, 6, 19)  # 周五，端午休市
D910 = date(2026, 9, 10)
D911 = date(2026, 9, 11)


class _BoomSource:
    """一律爆炸的 mootdx 源：模拟出口 IP 被限 / 全局无响应。"""

    def __init__(self, *a, **k):
        pass

    def get_daily(self, *a, **k):
        raise TimeoutError("mootdx down")


def _write_index_day(root, day):
    d = root / f"date={day.isoformat()}"
    d.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": ["000300.SH", "000001.SH"],
        "date": [day, day],
        "open": [1.0, 1.0], "high": [1.0, 1.0], "low": [1.0, 1.0],
        "close": [1.0, 1.0], "volume": [1.0, 1.0], "amount": [1.0, 1.0],
    }).write_parquet(d / "part.parquet")


def _local_cal(monkeypatch, tmp_path):
    idx = tmp_path / "kline_index_daily"
    for d in (D618, D910, D911):
        _write_index_day(idx, d)
    monkeypatch.setattr(ms, "MootdxSource", _BoomSource)
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", idx)
    return idx


def test_up_to_excludes_holiday_without_mootdx(monkeypatch, tmp_path):
    _local_cal(monkeypatch, tmp_path)
    days = ms._trade_days_up_to(D911)
    assert D910 in days and D618 in days
    assert D619 not in days  # 端午休市：本地 000300 无 bar


def test_in_range_excludes_holiday_without_mootdx(monkeypatch, tmp_path):
    _local_cal(monkeypatch, tmp_path)
    days = ms._trade_days_in_range(D618, D911)
    assert D910 in days
    assert D619 not in days


def test_empty_local_uses_authoritative_calendar(monkeypatch, tmp_path):
    """本地 index 分区为空（= 待修缺口）时，改用权威日历，不再退回工作日近似。

    旧行为退回工作日近似会把端午休市 06-19 误判为交易日（09-11 事故），且
    index 分区缺失时自指死锁让备用链整条失效（09-14 事故，见
    test_authoritative_calendar_unblocks_fill_recent_gaps）。
    """
    monkeypatch.setattr(ms, "MootdxSource", _BoomSource)
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "nope")
    days = ms._trade_days_up_to(D911)
    assert D910 in days                       # 真实交易日
    assert D619 not in days                   # 端午休市：权威日历不含
    assert len(days) > 60                     # 未退化成空/极小窗口


def test_weekday_fallback_only_when_no_calendar(monkeypatch, tmp_path):
    """权威日历与本地分区都不可用 → 退回 2026 法定节假日感知的日历（不断链）。

    8ceacc2 起 fallback 不再是朴素工作日：端午 6-19 不得再被误判为交易日
    （09-11 事故根因），但正常交易日仍在、列表不退化为空。
    """
    monkeypatch.setattr(ms, "MootdxSource", _BoomSource)
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "nope")
    monkeypatch.setattr(ms, "_authoritative_trade_days", lambda s, e: [])
    days = ms._trade_days_up_to(D911)
    assert len(days) > 50
    assert D618 in days
    assert D619 not in days


def test_outage_unblocks_fill_recent_gaps(monkeypatch, tmp_path):
    """端到端回归（09-14 事故）：mootdx 全天故障 + 当日分区全缺 → 备用链必须触发。

    故障态复刻：mootdx 全挂、熔断开路、三个日线根目录都已存在但**均缺当日**。
    旧行为下 index 分区缺失使自指日历判"今天非交易日"，missing 恒空，
    fill_recent_gaps_daily 三次全部 return None（当日日线永不补齐）。
    """
    from app.services import alt_daily as ad

    # 用窗口内最近已收盘交易日（相对今天，不写死）：fill_recent_gaps_daily
    # 只看近 lookback 天，写死历史日期会随时间滑出窗口导致零触发。
    day = ad._recent_closed_days()[-1]
    # 三根根目录各写一个受控的"已有历史"分区集合：只缺当日。
    # 不能用真实近期日期枚举——_missing_daily_days 扫的是 90 天窗口，任何未列举
    # 的交易日都会算缺口。这里让窗口内只有 D911 存在，因此下面只断言**当日
    # 出现在触发列表里**（核心契约），不苛求唯一。
    for name in ("idx", "stk", "etf"):
        (tmp_path / name).mkdir(parents=True, exist_ok=True)
        _write_index_day(tmp_path / name, D911)

    monkeypatch.setattr(ms, "MootdxSource", _BoomSource)
    monkeypatch.setattr(ms, "INDEX_DAILY_ROOT", tmp_path / "idx")
    monkeypatch.setattr(ms, "STOCK_DAILY_ROOT", tmp_path / "stk")
    monkeypatch.setattr(ms, "ETF_DAILY_ROOT", tmp_path / "etf")
    monkeypatch.setattr("app.quant.jqengine.datasource.mootdx_breaker.kline_allowed",
                        lambda: False)

    fired: list[tuple[str, list]] = []
    for kind in ("stock", "etf", "index"):
        monkeypatch.setattr(ad, f"sync_{kind}_daily_alt",
                            lambda days, _k=kind, **kw: fired.append((_k, list(days)))
                            or {"total": 0})
        ad.fill_recent_gaps_daily(kind)

    assert [k for k, _ in fired] == ["stock", "etf", "index"], \
        f"备用链未全部触发（实际 {fired}）"
    # 核心契约：当日缺失必须被识别并交给备用链（旧行为下 missing 恒空、0 触发）
    assert all(day in days for _, days in fired), fired
