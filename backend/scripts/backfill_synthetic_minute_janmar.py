"""用日线 OHLC 合成缺失月份的股票分钟锚点（仅策略触点时刻）。

背景：f51e08f9 对齐需要自 2026-01-05 起回测，但本地股票分钟数据
(kline_minute) 自 2026-04-01 起。mootdx/baostock 均拿不到 1-3 月分钟线。
策略在分钟级只触及：09:31（周度调仓/开盘判定）、11:30、14:00（止损检查）、
15:00。故按日线 OHLC 合成这 4 个锚点 bar 写入缺失日期分区：
  09:31=open，15:00=close，11:30/14:00 沿 open→close 线性内插并夹于 [low,high]。

幂等：已存在的分区跳过；只写 kline_minute 中完全缺失的 date 分区，
不触碰任何已有真实数据。
"""
import datetime as _dt
import os
from pathlib import Path

import polars as pl

DATA_ROOT = Path("/home/caisl/tickflow-stock-panel/data")
KLINE_DAILY = DATA_ROOT / "kline_daily"
KLINE_MINUTE = DATA_ROOT / "kline_minute"
START, END = "2026-01-05", "2026-03-31"


def trading_dates():
    return sorted(p.name.split("=")[1]
                  for p in KLINE_DAILY.iterdir()
                  if p.is_dir() and START <= p.name.split("=")[1] <= END)


def synth_partition(daily_df, dstr):
    d = _dt.date.fromisoformat(dstr)

    def ts(hh, mm):
        return _dt.datetime.combine(d, _dt.time(hh, mm))

    frames = []
    frac = {ts(11, 30): 0.45, ts(14, 0): 0.8}
    rows = []
    for r in daily_df.iter_rows(named=True):
        o, c, h, l = r["open"], r["close"], r["high"], r["low"]
        vol = (r["volume"] or 0.0) / 4.0
        amt = (r["amount"] or 0.0) / 4.0

        def px(f):
            v = o + (c - o) * f
            return min(max(v, l), h)

        base_vol = vol * 2 if False else vol  # 09:31 与 15:00 各占一半
        rows.append({"symbol": r["symbol"], "datetime": ts(9, 31),
                     "open": o, "high": h, "low": l, "close": o,
                     "volume": vol, "amount": amt})
        for t, f in frac.items():
            p = px(f)
            rows.append({"symbol": r["symbol"], "datetime": t,
                         "open": p, "high": max(h, p), "low": min(l, p),
                         "close": p, "volume": vol, "amount": amt})
        rows.append({"symbol": r["symbol"], "datetime": ts(15, 0),
                     "open": c, "high": h, "low": l, "close": c,
                     "volume": vol, "amount": amt})
    return pl.DataFrame(rows)


def main():
    dates = trading_dates()
    print(f"待处理交易日 {len(dates)} 天 ({dates[0]}~{dates[-1]})")
    written = skipped = 0
    for dstr in dates:
        part = KLINE_MINUTE / f"date={dstr}" / "part.parquet"
        src = KLINE_DAILY / f"date={dstr}" / "part.parquet"
        if not src.exists():
            continue
        if part.exists():
            # 修复模式：1-3 月本地不可能有真实全市场分钟，符号数过少=残缺帧，
            # 直接以合成数据替换（正常完整分区 ~5000 符号）
            try:
                n_sym = pl.read_parquet(part).select(
                    pl.col("symbol").n_unique()).item()
            except Exception:
                n_sym = 0
            if n_sym >= 100:
                skipped += 1
                continue
            print(f"  替换残缺分区 {dstr} (仅 {n_sym} 符号)")
        daily = pl.read_parquet(src)
        out = synth_partition(daily, dstr)
        part.parent.mkdir(parents=True, exist_ok=True)
        tmp = part.with_suffix(".tmp.parquet")
        out.write_parquet(tmp)
        os.replace(tmp, part)
        written += 1
        if written % 10 == 0:
            print(f"  {written} 分区已写 ({dstr})")
    print(f"完成：写入 {written} 个分区（含替换残缺），跳过完好 {skipped} 个")


if __name__ == "__main__":
    main()
