#!/usr/bin/env python3
"""对比 我们的回测输出 与 聚宽参考（收益/交易记录），用于对齐调试。

用法:
  uv run python scripts/compare_wufu.py \
      --ours data/quant_sim/jqwufu \
      --jq /home/caisl/五福闹新春-v5.2 \
      [--start 2026-06-08] [--end 2026-07-08]
"""
import argparse
import os
import re

import pandas as pd

JQ_DIR = "/home/caisl/五福闹新春-v5.2"
JQ_TRADES = "20260101-20260708交易记录.csv"
JQ_EQUITY = "20260101-20260708收益.csv"
INIT_CASH = 100000.0


def _norm_date(s):
    s = str(s).strip()
    try:
        return pd.Timestamp(s).strftime("%Y-%m-%d")
    except Exception:
        pass
    m = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return s


def _code_from_name(name):
    m = re.search(r"(\d{6}\.(?:XSHG|XSHE))", str(name))
    return m.group(1) if m else str(name)


def load_jq_trades(path):
    df = pd.read_csv(path, encoding="gbk").dropna(how="all")
    df = df[df["日期"].notna()]
    qty = pd.to_numeric(
        df["成交数量"].astype(str).str.replace("股", "").str.replace(",", ""),
        errors="coerce")
    price = pd.to_numeric(df["成交价"].astype(str).str.replace(",", ""), errors="coerce")
    out = pd.DataFrame({
        "date": [_norm_date(d) for d in df["日期"]],
        "time": df["委托时间"].astype(str).str[:5].values,
        "code": [_code_from_name(n) for n in df["标的"]],
        "side": df["交易类型"].astype(str).map(
            lambda s: "BUY" if "买" in s else "SELL").values,
        "qty": qty.abs().values,
        "price": price.values,
    })
    return out.reset_index(drop=True)


def load_our_trades(path):
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["dt"]).dt.strftime("%Y-%m-%d")
    df["time"] = pd.to_datetime(df["dt"]).dt.strftime("%H:%M")
    df["side"] = df["side"].str.upper()
    df["qty"] = df["qty"].abs()
    return df[["date", "time", "code", "side", "qty", "price"]]


def load_jq_equity(path):
    df = pd.read_csv(path, encoding="gbk")
    date = pd.to_datetime(df["时间"]).dt.strftime("%Y-%m-%d")
    ret = pd.to_numeric(df["策略收益"], errors="coerce")
    val = INIT_CASH * (1 + ret / 100.0)
    return pd.DataFrame({"date": date, "value": val, "weak": df["走弱期状态"]})


def load_our_equity(path):
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    return df[["date", "value"]]


def _clip(df, col, start, end):
    if start:
        df = df[df[col] >= start]
    if end:
        df = df[df[col] <= end]
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ours", required=True)
    ap.add_argument("--jq", default=JQ_DIR)
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--show", type=int, default=30, help="打印明细行数上限")
    args = ap.parse_args()

    jq_t = load_jq_trades(os.path.join(args.jq, JQ_TRADES))
    our_t = load_our_trades(os.path.join(args.ours, "trades.csv"))
    jq_e = load_jq_equity(os.path.join(args.jq, JQ_EQUITY))
    our_e = load_our_equity(os.path.join(args.ours, "equity.csv"))

    for d in (jq_t, our_t):
        pass
    jq_t = _clip(jq_t, "date", args.start, args.end)
    our_t = _clip(our_t, "date", args.start, args.end)
    jq_e = _clip(jq_e, "date", args.start, args.end)
    our_e = _clip(our_e, "date", args.start, args.end)

    # ---- 交易对比（同日同标同向按序号配对，避免笛卡尔积）----
    print("\n===== 交易记录对比 =====")
    print(f"jq trades={len(jq_t)}  ours={len(our_t)}")
    for d in (our_t, jq_t):
        d["occ"] = d.groupby(["date", "code", "side"]).cumcount()
    merged = our_t.merge(jq_t, on=["date", "code", "side", "occ"], how="outer",
                         suffixes=("_ours", "_jq"), indicator=True)
    merged = merged.sort_values(["date", "code", "side", "occ"])
    matched = merged[merged["_merge"] == "both"]
    print(f"匹配到(同日/标/向): {len(matched)}")
    if len(matched):
        mp = (matched["price_ours"] - matched["price_jq"]).abs()
        mq = (matched["qty_ours"] - matched["qty_jq"]).abs()
        print(f"  价格最大偏差: {mp.max():.4f}  平均: {mp.mean():.4f}")
        print(f"  数量最大偏差(股): {int(mq.max())}  平均: {mq.mean():.1f}")
    only_jq = merged[merged["_merge"] == "right_only"]
    only_ours = merged[merged["_merge"] == "left_only"]
    print(f"\n仅聚宽有: {len(only_jq)}")
    if len(only_jq):
        print(only_jq[["date", "time_jq", "code", "side", "qty_jq", "price_jq"]]
              .head(args.show).to_string(index=False))
    print(f"\n仅我们有: {len(only_ours)}")
    if len(only_ours):
        print(only_ours[["date", "time_ours", "code", "side", "qty_ours", "price_ours"]]
              .head(args.show).to_string(index=False))

    # ---- 净值对比 ----
    print("\n===== 收益曲线对比 =====")
    print(f"jq equity rows={len(jq_e)}  ours={len(our_e)}")
    em = our_e.merge(jq_e, on="date", how="outer", suffixes=("_ours", "_jq"))
    em = em.sort_values("date")
    first_jq = em["value_jq"].dropna().iloc[0] if em["value_jq"].notna().any() else INIT_CASH
    first_ours = em["value_ours"].dropna().iloc[0] if em["value_ours"].notna().any() else INIT_CASH
    last_jq = em["value_jq"].dropna().iloc[-1]
    last_ours = em["value_ours"].dropna().iloc[-1]
    print(f"  期初: jq={first_jq:.2f} ours={first_ours:.2f}")
    print(f"  期末: jq={last_jq:.2f} ours={last_ours:.2f}")
    print(f"  聚宽区间收益: {(last_jq/INIT_CASH-1)*100:.2f}%  "
          f"我们区间收益: {(last_ours/INIT_CASH-1)*100:.2f}%")
    em["diff"] = em["value_ours"] - em["value_jq"]
    worst = em.dropna(subset=["diff"]).reindex(
        em["diff"].abs().sort_values(ascending=False).index).head(10)
    print("  偏差最大的交易日:")
    cols = [c for c in ["date", "value_jq", "value_ours", "diff", "weak"] if c in worst.columns]
    print(worst[cols].to_string(index=False))


if __name__ == "__main__":
    main()
