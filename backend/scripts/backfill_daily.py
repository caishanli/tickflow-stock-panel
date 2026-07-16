"""补齐 daily.db 中冻结/不完整的 ETF 日线。

问题背景：daily.db 分批构建，1328 只 ETF 的 tushare 日线缓存冻结在 2026-01-30，
回测中 fetch("get_daily") 只要缓存 key 命中就直接返回，从不检查数据是否覆盖到回测
区间末端，导致 2 月起全用 1/30 的旧数据，动量/排名全面错乱。

本脚本按交易日批量拉取 fund_daily（单次调用返回全市场 ~1900 只基金，约 0.25s），
遍历 2026-01-31 ~ 回测末端的所有交易日，与现有缓存合并后写回 daily.db。
"""
import os
import sys
import time
import sqlite3
import pandas as pd
import tushare as ts

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault(
    "TUSHARE_TOKEN", "9bee25a983fb21afec0556cead53b1bd782a97f35d0662d4ffffeccf"
)

DB = "app/quant/jqengine/data/daily.db"
TOKEN = os.environ["TUSHARE_TOKEN"]
START = "20260131"
END = "20260715"  # 须严格覆盖回测末端(7/8)之后的若干交易日，否则基准/数据范围校验失败
# 现有缓存里有多少只就按多少只补齐（其余全市场基金忽略，避免污染 ETF 池）
PREFIX = "tushare_"


def to_jq_code(ts_code):
    pure, exch = ts_code.split(".")
    return f"{pure}.XSHE" if exch == "SZ" else f"{pure}.XSHG"


def main():
    pro = ts.pro_api(TOKEN)
    # 交易日历
    cal = pro.trade_cal(
        exchange="SSE", start_date=START, end_date=END, is_open="1"
    )
    trade_dates = sorted(cal["cal_date"].tolist())
    print(f"需补齐的交易日: {len(trade_dates)} 个 ({START} ~ {END})")

    # 读现有缓存的末端日期，只补齐不完整者
    conn = sqlite3.connect(DB)
    exist = {
        r[0]: r[1]
        for r in conn.execute(
            "SELECT key, data FROM cache WHERE key LIKE ?", (PREFIX + "%",)
        )
    }
    from app.quant.jqengine.datasource.cache import DataCache

    n_total = len(exist)
    n_full = 0
    last_dates = {}
    for k, blob in exist.items():
        try:
            df = pd.read_pickle(__import__("io").BytesIO(blob))
        except Exception:
            continue
        if df is None or df.empty or "trade_date" not in df.columns:
            continue
        ld = str(df["trade_date"].max())
        last_dates[k] = ld
        if ld >= END:
            n_full += 1
    print(f"现有 tushare 日线缓存 {n_total} 只, 已完整到 {END}: {n_full} 只, 需补齐: {n_total - n_full} 只")

    cache = DataCache()

    t0 = time.time()
    fetched = {}  # jq_code -> list of day-rows DataFrame
    for i, d in enumerate(trade_dates):
        try:
            df = pro.fund_daily(trade_date=d)
        except Exception as e:
            print(f"  {d} 拉取失败: {e}")
            time.sleep(1)
            continue
        if df is None or df.empty:
            continue
        for ts_code, g in df.groupby("ts_code"):
            jq = to_jq_code(ts_code)
            key = PREFIX + jq
            if key not in exist:
                continue  # 只补齐 ETF 池内已有的缓存
            fetched.setdefault(key, []).append(g)
        if (i + 1) % 20 == 0:
            print(f"  已拉取 {i + 1}/{len(trade_dates)} 个交易日 ...")

    print(f"拉取完成, 耗时 {time.time() - t0:.1f}s")

    updated = 0
    for key, parts in fetched.items():
        if key not in exist:
            continue
        try:
            old = pd.read_pickle(__import__("io").BytesIO(exist[key]))
        except Exception:
            old = None
        if old is None or old.empty:
            merged = pd.concat(parts, ignore_index=True)
        else:
            new = pd.concat(parts, ignore_index=True)
            merged = pd.concat([old, new], ignore_index=True)
        merged = merged.drop_duplicates(subset=["trade_date"]).sort_values("trade_date")
        cache.put("daily", key, merged)
        updated += 1

    conn.commit()
    conn.close()
    print(f"补齐完成: {updated} 只 ETF 日线已更新到 {END}")


if __name__ == "__main__":
    main()
