"""ETF 日线成交量单位（股 / 手）判定与归一的回归测试。

背景：2026-10 事故——`kline_etf_daily` 的 volume 契约是「股」且读侧不换算，
但归档导入按股票口径 ÷100、跨源补齐原样落盘，导致 09-14～10-28 共 27 个
分区 ~4.5 万行被写成「手」，策略的 volume_ratio 放大 100 倍、被成交量过滤器
静默踢掉（10-08 动量第一名 159509「成交量比值 108.85 ❌」被否）。
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from app.quant.archive_fill import pick_converter
from app.quant.archive_import import convert_daily, convert_daily_etf
from app.quant.volume_units import infer_etf_volume_unit, normalize_etf_daily_volume


def _frame(volume: float, amount: float, close: float = 1.0) -> pl.DataFrame:
    return pl.DataFrame({
        "symbol": ["159529.SZ"],
        "close": [close],
        "volume": [volume],
        "amount": [amount],
    })


def _archive_frame() -> pl.DataFrame:
    return pl.DataFrame({
        "code": ["159529.SZ"],
        "date": ["20261022"],
        "open": [1.35],
        "high": [1.36],
        "low": [1.34],
        "close": [1.351],
        "vol": [863_900_000.0],   # 归档为「股」
        "amount": [1.167e9],
    })


def test_infer_shares_unit():
    # volume 股：amount ≈ volume × close
    assert infer_etf_volume_unit(_frame(1_000_000.0, 1_000_000.0)) == "shares"


def test_infer_lots_unit():
    # volume 手：amount ≈ volume × 100 × close
    assert infer_etf_volume_unit(_frame(10_000.0, 1_000_000.0)) == "lots"


def test_infer_unknown_without_amount_column():
    df = pl.DataFrame({"symbol": ["159529.SZ"], "close": [1.0], "volume": [1.0]})
    assert infer_etf_volume_unit(df) == "unknown"


def test_infer_unknown_when_dimension_is_not_1_or_100():
    # 万元/千股之类的其它量纲不应被猜着改
    assert infer_etf_volume_unit(_frame(1_000.0, 5_000.0)) == "unknown"


def test_normalize_lots_multiplies_by_100():
    out, unit = normalize_etf_daily_volume(_frame(10_000.0, 1_000_000.0), source="t")
    assert unit == "lots"
    assert out["volume"][0] == 1_000_000.0
    assert infer_etf_volume_unit(out) == "shares"


def test_normalize_shares_is_noop():
    out, unit = normalize_etf_daily_volume(_frame(1_000_000.0, 1_000_000.0), source="t")
    assert unit == "shares"
    assert out["volume"][0] == 1_000_000.0


def test_normalize_unknown_keeps_values():
    df = pl.DataFrame({"symbol": ["159529.SZ"], "close": [1.0], "volume": [123.0]})
    out, unit = normalize_etf_daily_volume(df, source="t")
    assert unit == "unknown"
    assert out["volume"][0] == 123.0


def test_convert_daily_etf_does_not_divide_volume():
    etf = convert_daily_etf(_archive_frame())
    stock = convert_daily(_archive_frame())
    # ETF 存「股」：不换算
    assert etf["volume"][0] == 863_900_000.0
    # 股票存「手」：÷100
    assert stock["volume"][0] == 8_639_000.0


def test_pick_converter_routes_etf_target_to_etf_converter():
    etf_conv = pick_converter("daily", Path("/x/data/kline_etf_daily"))
    stock_conv = pick_converter("daily", Path("/x/data/kline_daily"))
    assert etf_conv is convert_daily_etf
    assert stock_conv is convert_daily
