"""``DataSources.apply_qfq_daily`` 的回归测试（reviewer Important #5）。

分支把服务端的因子 join 从**精确 join** 换成 **as-of backward join**
（为了同时支持逐日表与事件行表），并在因子加载里引入**按标的来源分离**
（不同来源的归一化锚点不同，混用会产生 ±20%~134% 的复权伪跳变）。

本文件锁住三条契约：
1. 逐日表下，as-of 展开与原精确 join 逐点等价。
2. 事件行表下，as-of 正确展开 step function。
3. 两表同标的时按来源优先级取值（baostock 优先于 mootdx），
   且**因子序列必须是单调不减且 ≤ 1.0**（最新锚定的不变量）。
"""

import polars as pl
import pytest

from app.services.stockdata.sources import DataSources


def _src(tmp_path, monkeypatch) -> DataSources:
    monkeypatch.setenv("PARTITION_DATA_ROOT", str(tmp_path))
    return DataSources(data_root=str(tmp_path), mootdx_factory=None, fetch_workers=1)


def _write_factors(root, subdir: str, df: pl.DataFrame) -> None:
    d = root / subdir
    d.mkdir(parents=True, exist_ok=True)
    df.write_parquet(d / "all.parquet")


def _d(s: str):
    """'YYYY-MM-DD' → datetime.date（polars DataFrame 构造里不能用 pl.date()）。"""
    from datetime import date as _date
    y, m, d = (int(x) for x in s.split("-"))
    return _date(y, m, d)


def _daily_frame(rows: list[tuple[str, str, float]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": [r[0] for r in rows],
            "date": [r[1] for r in rows],
            "open": [r[2] for r in rows],
            "high": [r[2] for r in rows],
            "low": [r[2] for r in rows],
            "close": [r[2] for r in rows],
            "volume": [1000.0] * len(rows),
            "amount": [1000.0] * len(rows),
        },
        schema={
            "symbol": pl.String, "date": pl.Date, "open": pl.Float64, "high": pl.Float64,
            "low": pl.Float64, "close": pl.Float64, "volume": pl.Float64, "amount": pl.Float64,
        },
    )


def test_daily_factor_table_asof_equals_exact_join(tmp_path, monkeypatch):
    """逐日表：as-of backward 展开 == 精确 join（逐点）。"""
    days = [f"2025-01-{d:02d}" for d in range(2, 10)]
    factors = pl.DataFrame({
        "symbol": ["600000.SH"] * 8,
        "trade_date": [d for d in days],
        "ex_factor": [0.95] * 4 + [1.0] * 4,
    }, schema={"symbol": pl.String, "trade_date": pl.String, "ex_factor": pl.Float64})
    factors = factors.with_columns(pl.col("trade_date").str.to_date())
    _write_factors(tmp_path, "adj_factor_etf", factors)
    src = _src(tmp_path, monkeypatch)
    df = _daily_frame([("600000.SH", d, 10.0) for d in days])
    out = src.apply_qfq_daily(df)
    # 精确 join 的期望：前 4 天 ×0.95，后 4 天 ×1.0
    assert out["close"].to_list() == pytest.approx([9.5] * 4 + [10.0] * 4)


def test_event_factor_table_expands_step_function(tmp_path, monkeypatch):
    """事件行表：as-of backward 正确展开 step function。"""
    events = pl.DataFrame({
        "symbol": ["600000.SH", "600000.SH"],
        "trade_date": [_d("2025-01-02"), _d("2025-01-06")],
        "ex_factor": [0.95, 1.0],
    }, schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64})
    _write_factors(tmp_path, "adj_factor_baostock", events)
    src = _src(tmp_path, monkeypatch)
    df = _daily_frame([("600000.SH", d, 10.0) for d in
                       ["2025-01-02", "2025-01-03", "2025-01-06", "2025-01-07"]])
    out = src.apply_qfq_daily(df)
    assert out["close"].to_list() == pytest.approx([9.5, 9.5, 10.0, 10.0])


def test_row_order_is_preserved(tmp_path, monkeypatch):
    """as-of join 会按 date 排序；输出必须恢复原行序。"""
    days = ["2025-01-06", "2025-01-02", "2025-01-07", "2025-01-03"]
    events = pl.DataFrame({
        "symbol": ["600000.SH"], "trade_date": [_d("2025-01-02")], "ex_factor": [0.9],
    }, schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64})
    _write_factors(tmp_path, "adj_factor_baostock", events)
    src = _src(tmp_path, monkeypatch)
    df = _daily_frame([("600000.SH", d, 10.0) for d in days])
    out = src.apply_qfq_daily(df)
    assert out["date"].to_list() == [_d(d) for d in days]


def test_source_priority_baostock_wins_over_mootdx(tmp_path, monkeypatch):
    """同一标的两表都有 → 取 baostock（权威源）；mootdx 锚点不同，混用会出伪跳变。"""
    # mootdx 源（聚宽码）：值 0.5 —— 尺度不同
    mootdx = pl.DataFrame({
        "symbol": ["600000.XSHG"] * 2,
        "trade_date": [_d("2025-01-02"), _d("2025-01-03")],
        "ex_factor": [0.5, 0.5],
    }, schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64})
    # baostock 源（tushare 码）：值 0.9
    baostock = pl.DataFrame({
        "symbol": ["600000.SH"] * 2,
        "trade_date": [_d("2025-01-02"), _d("2025-01-03")],
        "ex_factor": [0.9, 0.9],
    }, schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64})
    _write_factors(tmp_path, "adj_factor_etf", mootdx)
    _write_factors(tmp_path, "adj_factor_baostock", baostock)
    src = _src(tmp_path, monkeypatch)
    fac = src.get_adj_factors()
    # 600000.SH 只应来自 baostock（0.9），不能出现 mootdx 的 0.5
    vals = fac.filter(pl.col("symbol") == "600000.SH")["ex_factor"].unique().to_list()
    assert vals == [0.9], f"来源优先级失效：{vals}"


def test_same_symbol_string_baostock_wins_over_legacy(tmp_path, monkeypatch):
    """**同一个 symbol 字符串**时，权威源（baostock）仍须胜出。

    回归：旧实现按 concat 顺序 + ``unique(keep="last")`` 取值 → 低优先级的旧表
    （adj_factor）会覆盖权威源，实测服务端复权价直接取到旧表的 0.5（应为 0.9）。
    """
    baostock = pl.DataFrame({
        "symbol": ["600000.SH"] * 2,
        "trade_date": [_d("2025-01-02"), _d("2025-01-03")],
        "ex_factor": [0.9, 0.9],
    }, schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64})
    legacy = pl.DataFrame({
        "symbol": ["600000.SH"] * 2,
        "trade_date": [_d("2025-01-02"), _d("2025-01-03")],
        "ex_factor": [0.5, 0.5],
    }, schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64})
    _write_factors(tmp_path, "adj_factor_baostock", baostock)
    _write_factors(tmp_path, "adj_factor", legacy)
    src = _src(tmp_path, monkeypatch)
    fac = src.get_adj_factors()
    vals = fac.filter(pl.col("symbol") == "600000.SH")["ex_factor"].unique().to_list()
    assert vals == [0.9], f"旧表覆盖了权威源：{vals}"
    df = _daily_frame([("600000.SH", d, 10.0) for d in ("2025-01-02", "2025-01-03")])
    out = src.apply_qfq_daily(df)
    assert out["close"].to_list() == pytest.approx([9.0, 9.0])


def test_dates_before_factor_start_use_first_factor_not_one(tmp_path, monkeypatch):
    """因子表起始日**之前**的 bar 回填首个因子值（不是 1.0）。

    因子是累计量，首行值已含全部历史事件；填 1.0 会在因子表覆盖首日凭空出现
    一次跳变（实测 ETF 159971.SZ 2025-06-30 单日 −49.2%）。
    """
    factors = pl.DataFrame({
        "symbol": ["159971.SZ"],
        "trade_date": [_d("2025-06-30")],
        "ex_factor": [0.5],
    }, schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64})
    _write_factors(tmp_path, "adj_factor_etf", factors)
    src = _src(tmp_path, monkeypatch)
    days = ["2025-06-26", "2025-06-27", "2025-06-30", "2025-07-01"]
    df = _daily_frame([("159971.SZ", d, 10.0) for d in days])
    out = src.apply_qfq_daily(df)
    assert out["close"].to_list() == pytest.approx([5.0, 5.0, 5.0, 5.0])


def test_no_fake_jump_at_factor_table_start(tmp_path, monkeypatch):
    """回归：因子表覆盖首日不得出现复权价跳变（收益序列必须连续）。"""
    factors = pl.DataFrame({
        "symbol": ["159971.SZ", "159971.SZ"],
        "trade_date": [_d("2025-06-30"), _d("2025-08-04")],
        "ex_factor": [0.5, 0.6],
    }, schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64})
    _write_factors(tmp_path, "adj_factor_etf", factors)
    src = _src(tmp_path, monkeypatch)
    days = ["2025-06-26", "2025-06-27", "2025-06-30", "2025-07-01"]
    df = _daily_frame([("159971.SZ", d, 10.0) for d in days])
    out = src.apply_qfq_daily(df)
    closes = out["close"].to_list()
    rets = [closes[i] / closes[i - 1] - 1 for i in range(1, len(closes))]
    assert all(abs(r) < 1e-9 for r in rets), f"覆盖首日出现假跳变：{rets}"


def test_gross_factor_drop_is_quarantined(tmp_path, monkeypatch):
    """因子序列出现 >10% 下降（源数据伪跳变）→ 整段隔离，按原始价计算。"""
    factors = pl.DataFrame({
        "symbol": ["159971.SZ", "159971.SZ"],
        "trade_date": [_d("2025-06-30"), _d("2026-06-15")],
        "ex_factor": [1.2, 1.0],
    }, schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64})
    _write_factors(tmp_path, "adj_factor_etf", factors)
    src = _src(tmp_path, monkeypatch)
    fac = src.get_adj_factors()
    assert fac.is_empty() or "159971.SZ" not in fac["symbol"].unique().to_list()
    df = _daily_frame([("159971.SZ", d, 10.0)
                       for d in ("2025-06-30", "2026-06-15")])
    out = src.apply_qfq_daily(df)
    assert out["close"].to_list() == pytest.approx([10.0, 10.0])


def test_small_factor_wiggle_is_kept(tmp_path, monkeypatch):
    """小幅震荡（<10%）不隔离（只是噪声，不是伪跳变）。"""
    factors = pl.DataFrame({
        "symbol": ["159207.SZ", "159207.SZ"],
        "trade_date": [_d("2025-07-01"), _d("2025-07-21")],
        "ex_factor": [0.953807, 0.951639],
    }, schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64})
    _write_factors(tmp_path, "adj_factor_etf", factors)
    src = _src(tmp_path, monkeypatch)
    fac = src.get_adj_factors()
    assert "159207.SZ" in fac["symbol"].unique().to_list()


def test_loaded_factor_series_is_monotone_and_bounded(tmp_path, monkeypatch):
    """不变量：每个标的的因子序列单调不减且 ≤ 1.0（最新锚定）。"""
    events = pl.DataFrame({
        "symbol": ["600000.SH"] * 3,
        "trade_date": [_d("2025-01-02"), _d("2025-01-03"), _d("2025-01-06")],
        "ex_factor": [0.9, 0.95, 1.0],
    }, schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64})
    _write_factors(tmp_path, "adj_factor_baostock", events)
    src = _src(tmp_path, monkeypatch)
    fac = src.get_adj_factors().sort(["symbol", "trade_date"])
    vals = fac["ex_factor"].to_list()
    assert vals == sorted(vals), f"因子序列非单调不减：{vals}"
    assert max(vals) <= 1.0 + 1e-9, f"因子 > 1.0（最新锚定不应出现）：{vals}"
