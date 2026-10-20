"""回测桥复权（manager._adj_events / _apply_qfq）与服务端口径一致性。

背景（2026-10-21 修复）：`_adj_events` 原本用 10% 阈值过滤"除权事件"，
而股票分红常在 1%~5%（实测 baostock 全市场因子表：59197 个事件里 13153 个
是 1%~10% 的小额分红）→ 这些事件被当噪声滤掉，回测桥与服务端
（`apply_qfq_daily` 直查因子表）的复权价最大差 5.32%（000001.SZ）。

修复：阈值降到 0.01%（1e-4）。该值高于两张因子表的噪声底
（baostock 股票表 ~1e-6、mootdx ETF 表 ~1e-5 的震荡噪声），
又能保留全部真实小额分红事件。
"""

import pandas as pd
import pytest

from app.quant.jqengine.datasource.manager import DataManager


def _dm_with_factor_map(monkeypatch, fmap: dict) -> DataManager:
    """构造一个只带因子表的 DataManager（不触网、不读盘）。"""
    dm = DataManager.__new__(DataManager)
    dm._adj_factor = {k: dict(v) for k, v in fmap.items()}
    dm._adj_events_cache = None
    dm._adj_fresh = lambda: True
    dm._adj_built_day = pd.Timestamp.now().normalize().date().isoformat()
    return dm


def _daily(jq: str, rows: list[tuple[str, float]]) -> pd.DataFrame:
    idx = pd.DatetimeIndex([pd.Timestamp(d) for d, _ in rows])
    return pd.DataFrame({"close": [c for _, c in rows]}, index=idx)


def test_small_dividend_event_is_not_dropped(monkeypatch):
    """1%~5% 的现金分红必须被识别为除权事件（原来 10% 阈值会滤掉）。"""
    # 因子从 0.98 跳到 1.0 → 事件乘数 0.98（2% 分红）
    fmap = {"000001.XSHE": {
        pd.Timestamp("2025-06-11"): 0.979507,
        pd.Timestamp("2025-06-12"): 0.979257,
        pd.Timestamp("2025-10-14"): 0.971821,
        pd.Timestamp("2025-10-15"): 1.0,
    }}
    dm = _dm_with_factor_map(monkeypatch, fmap)
    events = dm._adj_events()["000001.XSHE"]
    dates = [e[0].strftime("%Y-%m-%d") for e in events]
    assert "2025-06-12" in dates, f"2% 的分红事件被漏掉: {events}"
    assert "2025-10-15" in dates, f"2.8% 的分红事件被漏掉: {events}"


def test_tiny_rounding_noise_is_still_filtered(monkeypatch):
    """~1e-6 量级的因子震荡（舍入噪声）不应被当成事件。"""
    fmap = {"159357.XSHE": {
        pd.Timestamp("2025-07-09"): 0.99276349,
        pd.Timestamp("2025-07-10"): 0.99275527,
        pd.Timestamp("2025-07-11"): 0.99276349,
        pd.Timestamp("2025-07-14"): 0.99276450,
        pd.Timestamp("2025-07-16"): 0.99276349,
    }}
    dm = _dm_with_factor_map(monkeypatch, fmap)
    assert dm._adj_events().get("159357.XSHE", []) == []


def test_real_split_event_is_kept(monkeypatch):
    """3:1 拆分（乘数 0.333）必须保留。"""
    fmap = {"159667.XSHE": {
        pd.Timestamp("2026-06-09"): 0.333333,
        pd.Timestamp("2026-06-10"): 1.0,
    }}
    dm = _dm_with_factor_map(monkeypatch, fmap)
    events = dm._adj_events()["159667.XSHE"]
    assert len(events) == 1
    assert events[0][0].strftime("%Y-%m-%d") == "2026-06-10"
    assert abs(events[0][1] - 0.333333) < 1e-6


def test_apply_qfq_matches_server_side_factor_table(monkeypatch):
    """回测桥复权价必须与「因子表直乘」的服务端口径一致。

    服务端 apply_qfq_daily = price × ex_factor(date)（最新锚定，无阈值）。
    回测桥 = price × prod(事件乘数, 仅 t < 事件日 <= cutoff)。
    两者在事件完整时应逐点相等。
    """
    days = pd.date_range("2025-06-05", periods=20, freq="B")
    raw = [11.0 + i * 0.1 for i in range(20)]
    # 因子：6/12 起从 0.95 跳到 1.0（5% 分红），10/15 再跳到 1.03（相对另一标的）
    factor = {}
    for d in days:
        factor[pd.Timestamp(d)] = 0.95 if d < pd.Timestamp("2025-06-12") else 1.0
    fmap = {"000001.XSHE": factor}
    dm = _dm_with_factor_map(monkeypatch, fmap)

    pdf = _daily("000001.XSHE", [(str(d.date()), c) for d, c in zip(days, raw, strict=True)])
    got = dm._apply_qfq(pdf.copy(), "000001.XSHE")

    # 期望：事件日之前的价格乘以事件乘数（0.95），之后不变
    ex = pd.Timestamp("2025-06-12")
    mult = 0.95 / 1.0
    for d, c in zip(days, raw, strict=True):
        exp = c * mult if d < ex else c
        assert got.loc[pd.Timestamp(d), "close"] == pytest.approx(exp), d


def test_apply_qfq_cutoff_ignores_future_events(monkeypatch):
    """cutoff 之后的事件不参与折算（动态前复权）。"""
    days = pd.date_range("2025-06-05", periods=20, freq="B")
    raw = [11.0 + i * 0.1 for i in range(20)]
    factor = {}
    for d in days:
        factor[pd.Timestamp(d)] = 0.95 if d < pd.Timestamp("2025-06-12") else 1.0
    dm = _dm_with_factor_map(monkeypatch, {"000001.XSHE": factor})
    pdf = _daily("000001.XSHE", [(str(d.date()), c) for d, c in zip(days, raw, strict=True)])

    got = dm._apply_qfq(pdf.copy(), "000001.XSHE", cutoff=pd.Timestamp("2025-06-10"))
    # cutoff 早于事件日 → 不折算
    assert (got["close"] == pdf["close"]).all()
