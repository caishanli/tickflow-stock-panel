"""真实回测路径（``jqcompat.JqDataSource._apply_qfq_bars``）下的复权口径一致性实测。

回测实际走的是 ``jqcompat._apply_qfq_bars``（numpy recarray，datetime 为
YYYYMMDDHHMMSS int64），它经 ``DataManager._adj_events()`` 从因子表重建事件。

**本脚本直接调用生产方法**（``JqDataSource._apply_qfq_bars``），不再复刻一份
逻辑——复刻版会与生产代码悄悄漂移，校验的就不是同一条路径了。

对比口径：
  服务端 : apply_qfq_daily = close × ex_factor(date)   （直查因子表，最新锚定）
  回测桥 : _apply_qfq_bars = close × prod(事件乘数, bar日 < 事件日 <= 决策日 dt)

回测桥是**动态前复权**（锚在决策日 dt），服务端是**静态**（锚在因子表末行），
两者相差一个常数比例（不影响收益率），故这里比较**日收益率序列**（形状）；
静态口径的逐点相等由 ``qfq_consistency_check.py`` 负责。

用法: uv run python -m app.quant.qfq_bars_consistency_check
"""

from __future__ import annotations

import argparse
import json
import types
from typing import cast

import numpy as np

from app.quant.jqcompat import JqDataSource
from app.quant.jqengine.datasource.manager import DataManager
from app.services.stockdata.sources import DataSources


def bars_from_daily(rows: list[tuple[str, float]]) -> np.ndarray:
    """构造 jqcompat 风格的 recarray：datetime=YYYYMMDDHHMMSS int64 + OHLC。"""
    dt = np.array([int(d.replace("-", "") + "150000") for d, _ in rows], dtype="int64")
    close = np.array([c for _, c in rows], dtype="float64")
    return np.rec.fromarrays(
        [dt, close, close, close, close],   # type: ignore[call-overload]
        names="datetime,open,high,low,close",
    )


def _returns(closes: np.ndarray) -> np.ndarray:
    """日收益率序列（去掉首根）。"""
    if len(closes) < 2:
        return np.array([])
    with np.errstate(divide="ignore", invalid="ignore"):
        rets = closes[1:] / closes[:-1] - 1.0
    return rets[np.isfinite(rets)]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="+", default=[
        # 股票（权威源 baostock）
        "000001.SZ", "600000.SH", "000002.SZ", "300750.SZ", "601318.SH", "000651.SZ",
        # ETF（mootdx 源，历史伪跳变高发区——不能只抽样股票）
        "159934.SZ", "159971.SZ", "159207.SZ", "510300.SH", "588000.SH",
    ])
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default="2026-10-20")
    args = ap.parse_args()

    ds = DataSources()
    dm = DataManager()
    out = []
    worst = 0.0
    for sym in args.symbols:
        df = ds.get_daily([sym], args.start, args.end)
        if df.is_empty():
            out.append({"symbol": sym, "error": "无数据"})
            continue
        srv = ds.apply_qfq_daily(df).sort("date")
        s = np.array([r["close"] for r in srv.iter_rows(named=True)], dtype="float64")

        rows = [(str(r["date"]), r["close"]) for r in df.sort("date").iter_rows(named=True)]
        bars = bars_from_daily(rows)
        jq = dm._to_jq_code(sym)
        dt = df["date"].max()
        # **真实生产方法**（_apply_qfq_bars 只用到 self._dm）
        fake_self = cast(JqDataSource, types.SimpleNamespace(_dm=dm))
        adj_bars = JqDataSource._apply_qfq_bars(fake_self, bars, jq, dt)
        b = np.array([x["close"] for x in adj_bars], dtype="float64")

        rs, rb = _returns(s), _returns(b)
        n = min(len(rs), len(rb))
        if n == 0:
            out.append({"symbol": sym, "error": "无重叠"})
            continue
        diff = np.abs(rs[:n] - rb[:n])
        rel = float(diff.max()) if diff.size else 0.0
        worst = max(worst, rel)
        out.append({
            "symbol": sym, "n": int(n),
            "max_ret_diff_bp": round(rel * 1e4, 6),
        })
    print(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"\n最大日收益率差: {worst:.8%}（{worst * 1e4:.4f} bp）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
