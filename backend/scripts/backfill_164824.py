"""补齐 164824.XSHE（南方原油 LOF）：腾讯日线 → 合成分钟锚点 + 日线分区。

mootdx/baostock 均无此 LOF 数据，腾讯 fqkline 有日线。策略在 ETF 分支仅
于 09:31（周度调仓）、14:00（止损检查）、15:00 触及分钟价，故按日线 OHLC
合成每日 4 个锚点分钟 bar（09:31=open / 11:30 / 14:00 线性插值 / 15:00=close），
写入 kline_etf_minute 与 kline_etf_daily 分区。合成数据量小、只影响该标的，
幂等（重复运行先清除本标的旧行再写）。
"""
import datetime as _dt
import tempfile
import os
import shutil
from pathlib import Path

import polars as pl
import requests

DATA_ROOT = Path("/home/caisl/tickflow-stock-panel/data")
ETF_MINUTE = DATA_ROOT / "kline_etf_minute"
ETF_DAILY = DATA_ROOT / "kline_etf_daily"
SYMBOL = "164824.SZ"
TENCENT_SYM = "sz164824"
START, END = "2026-03-01", "2026-08-21"


def fetch_daily():
    r = requests.get(
        "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get",
        params={"param": f"{TENCENT_SYM},day,{START},{END},320,qfq"},
        timeout=20)
    data = r.json().get("data", {}).get(TENCENT_SYM, {})
    rows = data.get("qfqday") or data.get("day") or []
    # [date, open, close, high, low, volume, ...]
    out = []
    for k in rows:
        out.append({"date": k[0], "open": float(k[1]), "close": float(k[2]),
                    "high": float(k[3]), "low": float(k[4]),
                    "volume": float(k[5]) if len(k) > 5 else 0.0})
    return out


def synth_minutes(day):
    """日线 OHLC → 4 个锚点分钟：09:31/11:30/14:00/15:00，线性内插价格。"""
    o, c, h, l = day["open"], day["close"], day["high"], day["low"]
    d = _dt.date.fromisoformat(day["date"])

    def ts(hh, mm):
        return _dt.datetime.combine(d, _dt.time(hh, mm))

    def px(frac):  # open→close 线性，夹在 [low, high]
        v = o + (c - o) * frac
        return min(max(v, l), h)

    anchors = [(9, 31, o), (11, 30, px(0.45)), (14, 0, px(0.8)), (15, 0, c)]
    vol = (day["volume"] or 0.0) / 4.0
    rows = []
    for hh, mm, p in anchors:
        rows.append({"symbol": SYMBOL, "datetime": ts(hh, mm),
                     "open": p, "high": max(o, p), "low": min(o, p),
                     "close": p, "volume": vol,
                     "amount": vol * p})
    return rows


def main():
    days = fetch_daily()
    print(f"腾讯日线 {len(days)} 天")
    assert days, "无日线数据"
    ddf = pl.DataFrame([{
        "symbol": SYMBOL, "date": d["date"],
        "open": d["open"], "high": d["high"], "low": d["low"],
        "close": d["close"], "volume": d["volume"],
        "amount": (d["volume"] or 0) * d["close"],
    } for d in days])
    ddf = ddf.with_columns(pl.col("date").str.to_date().alias("date"))

    touched = 0
    for day in days:
        dstr = day["date"]
        part = ETF_MINUTE / f"date={dstr}" / "part.parquet"
        if not part.exists():
            continue
        df = pl.read_parquet(part)
        base = df.filter(pl.col("symbol") != SYMBOL)
        new = pl.DataFrame(synth_minutes(day)).with_columns(
            pl.col("datetime").cast(df.schema["datetime"]))
        merged = pl.concat([base, new]).sort(["symbol", "datetime"])
        tmp = part.with_suffix(".tmp.parquet")
        merged.write_parquet(tmp)
        os.replace(tmp, part)
        touched += 1
    print(f"分钟锚点写入 {touched} 个交易日分区")

    # 日线分区：按 date= 目录逐个合并
    daily_touched = 0
    for day in days:
        dstr = day["date"]
        part = ETF_DAILY / f"date={dstr}" / "part.parquet"
        if not part.exists():
            continue
        df = pl.read_parquet(part)
        base = df.filter(pl.col("symbol") != SYMBOL)
        row = ddf.filter(pl.col("date") == _dt.date.fromisoformat(dstr))
        # 日线分区不含 date 列（hive 分区目录承载日期），按既有 schema 对齐
        row = row.select([pl.col(c).cast(df.schema[c]) for c in df.columns])
        merged = pl.concat([base, row]).sort(["symbol"])
        tmp = part.with_suffix(".tmp.parquet")
        merged.write_parquet(tmp)
        os.replace(tmp, part)
        daily_touched += 1
    print(f"日线写入 {daily_touched} 个交易日分区")


if __name__ == "__main__":
    main()
