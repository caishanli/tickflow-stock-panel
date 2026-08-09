#!/usr/bin/env python3
"""把本地 rqalpha 回测产物转换为聚宽同款三份 CSV（收益/交易/日志）。

用法:
  python scripts/align_jq_output.py \
      --local data/quant_sim/jqwufu \
      --out data/quant_sim/jqwufu/aligned \
      --benchmark tushare_510300.XSHG
"""
import argparse
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def load_local(local_dir):
    eq = pd.read_csv(os.path.join(local_dir, "equity.csv"))
    eq["date"] = pd.to_datetime(eq["date"])
    eq = eq.sort_values("date").reset_index(drop=True)
    tr = pd.read_csv(os.path.join(local_dir, "trades.csv"))
    tr["dt"] = pd.to_datetime(tr["dt"])
    return eq, tr


def load_benchmark(dm, bench_key):
    df = dm.cache.peek("daily", bench_key)
    if df is None or df.empty:
        return None
    df = df.copy()
    if "trade_date" in df.columns:
        df["date"] = pd.to_datetime(df["trade_date"].astype(str), format="%Y%m%d")
    elif "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    df["bench_close"] = df["close"].astype(float)
    return df[["date", "bench_close"]]


def build_return_csv(eq, bench, tr):
    cap = 100000.0
    eq = eq.copy()
    eq["策略收益%"] = (eq["value"] / cap - 1) * 100
    # 当日盈亏（value 增量）
    eq["delta"] = eq["value"].diff().fillna(eq["value"].iloc[0] - cap)
    # 当日买卖额：从 trades 按日聚合
    if tr is not None and len(tr):
        tr = tr.copy()
        tr["day"] = tr["dt"].dt.date
        tr["amt"] = tr["qty"] * tr["price"]
        buy = tr[tr["side"] == "BUY"].groupby("day")["amt"].sum()
        sell = tr[tr["side"] == "SELL"].groupby("day")["amt"].sum()
        eq["day"] = eq["date"].dt.date
        eq["当日买入"] = eq["day"].map(buy).fillna(0.0).round(2)
        eq["当日卖出"] = eq["day"].map(sell).fillna(0.0).round(2)
    else:
        eq["当日买入"] = 0.0
        eq["当日卖出"] = 0.0
    # 合并基准，基准收益以回测首日为基准（对齐聚宽）
    if bench is not None:
        merged = pd.merge(
            eq, bench, left_on="date", right_on="date", how="left")
        bclose = merged["bench_close"].ffill()
        bbase = float(bclose.iloc[0])
        bench_ret = (bclose / bbase - 1) * 100
    else:
        bench_ret = pd.Series(0.0, index=eq.index)
    out = pd.DataFrame({
        "时间": eq["date"].dt.strftime("%Y-%m-%d 16:00:00"),
        "基准收益": bench_ret.round(4),
        "策略收益": eq["策略收益%"].round(4),
        "当日盈利": eq["delta"].clip(lower=0).round(2),
        "当日亏损": (-eq["delta"].clip(upper=0)).round(2),
        "当日买入": eq["当日买入"],
        "当日卖出": eq["当日卖出"],
        "超额收益(%)": (eq["策略收益%"] - bench_ret).round(4),
        "走弱期状态": 0,
    })
    return out


def build_trade_csv(tr):
    tr = tr.copy()
    tr["日期"] = tr["dt"].dt.strftime("%Y/%m/%d")
    tr["委托时间"] = tr["dt"].dt.strftime("%H:%M:%S")
    tr["标的"] = tr["code"]
    tr["交易类型"] = tr["side"].map({"BUY": "买", "SELL": "卖"})
    tr["下单类型"] = "市价单"
    tr["成交数量"] = (tr["qty"].astype(int).astype(str) + "股")
    tr["成交价"] = tr["price"].round(4)
    tr["成交额"] = tr["qty"] * tr["price"]
    tr["成交额"] = tr["成交额"].map(lambda v: f'"{v:,.2f}"')
    tr["平仓盈亏"] = 0.0
    tr["手续费"] = tr["cost"].round(2)
    cols = ["日期", "委托时间", "标的", "交易类型", "下单类型",
            "成交数量", "成交价", "成交额", "平仓盈亏", "手续费"]
    tr = tr[cols]
    tr["_dt"] = tr["dt"] if "dt" in tr.columns else tr.index
    return tr


def main():
    ap = argparse.ArgumentParser()
    _runtime = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data", "quant_sim")
    ap.add_argument("--local", default=os.path.join(_runtime, "jqwufu"))
    ap.add_argument("--out", default=os.path.join(_runtime, "jqwufu", "aligned"))
    ap.add_argument("--benchmark", default="tushare_510300.XSHG")
    ap.add_argument("--log", default=None, help="策略日志文件(可选)")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    eq, tr = load_local(args.local)

    from app.quant.jqengine.datasource.manager import get_data_manager
    dm = get_data_manager()
    bench = load_benchmark(dm, args.benchmark)

    ret = build_return_csv(eq, bench, tr)
    ret.to_csv(os.path.join(args.out, "收益.csv"), index=False, encoding="utf-8-sig")
    print(f"[align] 收益.csv 行数={len(ret)} 本地终值策略收益={ret['策略收益'].iloc[-1]:.2f}%")

    tcsv = build_trade_csv(tr)
    # 聚宽格式每笔后跟空行分隔同秒多笔
    tcsv = tcsv.reset_index(drop=True)
    with open(os.path.join(args.out, "交易记录.csv"), "w", encoding="utf-8-sig", newline="") as f:
        f.write(",".join(tcsv.columns) + "\n")
        last_dt = None
        for _, row in tcsv.iterrows():
            if last_dt is not None and row["_dt"] != last_dt:
                f.write("\n")
            f.write(",".join(str(row[c]) for c in tcsv.columns) + "\n")
            last_dt = row["_dt"]
    print(f"[align] 交易记录.csv 笔数={len(tcsv)}")

    if args.log:
        import shutil
        shutil.copy(args.log, os.path.join(args.out, "日志.txt"))
        print(f"[align] 日志.txt 已复制")


if __name__ == "__main__":
    main()
