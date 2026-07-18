"""minute_synth 日线合成分钟源的回归测试（合成数据，不联网）。

覆盖修复点：索引归一化移到窗口切片之前——原实现先 ``daily.index >= lo``
切片、后转换索引，tushare 日线帧（RangeIndex + trade_date 字符串列）在
切片处直接抛 TypeError，合成兜底对非 DatetimeIndex 帧全部失效。同时防止
RangeIndex 被 pd.to_datetime 当纳秒时间戳转成 1970 垃圾日期。
"""
import pandas as pd

from app.quant.jqengine.datasource.minute_synth import SyntheticMinuteSource


def _tushare_style_daily():
    # tushare 日线 schema：RangeIndex + trade_date 字符串列（YYYYmmdd）
    return pd.DataFrame({
        "trade_date": ["20260105", "20260106"],
        "open": [4.8, 4.85], "high": [4.9, 4.9], "low": [4.75, 4.8],
        "close": [4.85, 4.83], "volume": [1e8, 1.1e8],
    })


def _mootdx_style_daily():
    idx = pd.to_datetime(["2026-01-05", "2026-01-06"])
    return pd.DataFrame({
        "open": [4.8, 4.85], "high": [4.9, 4.9], "low": [4.75, 4.8],
        "close": [4.85, 4.83], "volume": [1e8, 1.1e8],
    }, index=idx)


def test_range_index_tushare_frame_synthesizes_minutes():
    """RangeIndex + trade_date 列的日线帧：正常合成，日期正确（非 1970）。"""
    src = SyntheticMinuteSource(lambda code, start, end: _tushare_style_daily())
    out = src.get_minute("510300.XSHG", "2026-01-06", "2026-01-05")
    assert not out.empty
    assert len(out) == 480  # 2 个交易日 × 240 根
    assert out.index.min() == pd.Timestamp("2026-01-05 09:30")
    assert out.index.max() == pd.Timestamp("2026-01-06 15:00")
    # 价格约束在当日 [low, high] 内
    assert out["close"].min() >= 4.75
    assert out["close"].max() <= 4.9
    assert (out["volume"] > 0).all()


def test_datetime_index_frame_still_works():
    """mootdx 风格（已是 DatetimeIndex）行为不变。"""
    src = SyntheticMinuteSource(lambda code, start, end: _mootdx_style_daily())
    out = src.get_minute("510300.XSHG", "2026-01-06", "2026-01-05")
    assert len(out) == 480
    assert out.index.min() == pd.Timestamp("2026-01-05 09:30")


def test_range_index_without_date_column_returns_empty():
    """RangeIndex 且无日期列：返回空帧，绝不产出 1970 垃圾日期。"""
    df = pd.DataFrame({
        "open": [4.8], "high": [4.9], "low": [4.75],
        "close": [4.85], "volume": [1e8],
    })
    src = SyntheticMinuteSource(lambda code, start, end: df)
    out = src.get_minute("X", "2026-01-06", "2026-01-05")
    assert out.empty


def test_bad_trade_date_rows_dropped():
    """trade_date 含无法解析的行：该行丢弃，其余正常合成。"""
    df = _tushare_style_daily()
    df.loc[1, "trade_date"] = "bad"
    src = SyntheticMinuteSource(lambda code, start, end: df)
    out = src.get_minute("X", "2026-01-06", "2026-01-05")
    assert len(out) == 240  # 只剩 2026-01-05 一天
    assert out.index.max() == pd.Timestamp("2026-01-05 15:00")


def test_window_slice_excludes_out_of_range_days():
    """窗口切片在归一化之后生效：窗口外的交易日不展开。"""
    df = _tushare_style_daily()
    src = SyntheticMinuteSource(lambda code, start, end: df)
    out = src.get_minute("X", "2026-01-05", "2026-01-05")
    assert len(out) == 240  # 只含 01-05 一天
    assert out.index.max() == pd.Timestamp("2026-01-05 15:00")
