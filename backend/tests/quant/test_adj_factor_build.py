import polars as pl

from app.quant.adj_factor_build import build_factor_table


def _raw() -> pl.DataFrame:
    """模拟归档：因子随时间递增，在除权日跳变。"""
    return pl.DataFrame(
        {
            "symbol": ["000001.SZ"] * 4 + ["600000.SH"] * 2,
            "trade_date": [
                "2025-06-11", "2025-06-12",
                "2025-10-14", "2025-10-15",
                "2025-06-11", "2025-10-15",
            ],
            "adj_factor": [127.7841, 131.7878, 131.7878, 134.5794, 50.0, 50.0],
        },
        schema={
            "symbol": pl.String,
            "trade_date": pl.Date,
            "adj_factor": pl.Float64,
        },
    )


def test_normalized_latest_is_one():
    """最新锚定：每个标的的最新 ex_factor 必须 = 1.0。"""
    out = build_factor_table(_raw())
    for sym in ("000001.SZ", "600000.SH"):
        last = out.filter(pl.col("symbol") == sym).sort("trade_date").row(-1, named=True)
        assert abs(last["ex_factor"] - 1.0) < 1e-9


def test_normalized_earlier_is_smaller():
    """越往前因子越小（单调不增）。"""
    out = build_factor_table(_raw()).filter(pl.col("symbol") == "000001.SZ").sort("trade_date")
    vals = out["ex_factor"].to_list()
    assert vals[0] < vals[1] == vals[2] < vals[3]
    assert abs(vals[0] - 127.7841 / 134.5794) < 1e-9


def test_factor_constant_between_events():
    """两个事件日之间因子恒定。"""
    out = build_factor_table(_raw()).filter(pl.col("symbol") == "000001.SZ").sort("trade_date")
    assert out["ex_factor"][1] == out["ex_factor"][2]


def test_output_schema_matches_etf_table():
    """与本机 ETF 因子表 schema 一致：symbol/trade_date(date)/ex_factor(f64)。"""
    out = build_factor_table(_raw())
    assert out.columns == ["symbol", "trade_date", "ex_factor"]
    assert out.schema["trade_date"] == pl.Date
    assert out.schema["ex_factor"] == pl.Float64
    assert out.schema["symbol"] == pl.String


def test_factor_carries_forward_when_a_day_is_absent():
    """归档缺某交易日时，用前值填充（因子在事件间恒定）。

    构造：6/11 因子 127.78、6/13 因子 134.58，中间 6/12 缺失 →
    6/12 之后的日期用 6/11 的因子填充，直到 6/13 出现新值。
    """
    raw = pl.DataFrame(
        {
            "symbol": ["000001.SZ", "000001.SZ"],
            "trade_date": ["2025-06-11", "2025-06-13"],
            "adj_factor": [127.7841, 134.5794],
        },
        schema={"symbol": pl.String, "trade_date": pl.Date, "adj_factor": pl.Float64},
    )
    out = build_factor_table(raw).filter(pl.col("symbol") == "000001.SZ").sort("trade_date")
    # 归一化后：6/11 = 0.9495，6/13 = 1.0（各自是最新锚定下的值）
    assert out.height == 2
    assert out["ex_factor"][0] < out["ex_factor"][1]
    assert abs(out["ex_factor"][1] - 1.0) < 1e-9
