#!/usr/bin/env python3
"""对比本地 rqalpha 回测产物与聚宽 fixtures 的收益/交易。

用法:
  python scripts/diff_jq_vs_local.py \
      --local data/quant_sim/jqwufu \
      --fixture tests/fixtures/wufu_v52 \
      --out data/quant_sim/jqwufu/diff
"""
import argparse
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

FIXTURE_RET = "20260101-20260708收益.csv"
FIXTURE_TRD = "20260101-20260708交易记录.csv"


def load_local_equity(local_dir):
    eq = pd.read_csv(os.path.join(local_dir, "equity.csv"))
    eq["date"] = pd.to_datetime(eq["date"])
    eq = eq.sort_values("date")
    # 策略累计收益%
    cap = 100000.0
    eq["策略收益"] = (eq["value"] / cap - 1) * 100
    return eq


def load_fixture_return(fix_dir, ret_name=FIXTURE_RET):
    df = pd.read_csv(os.path.join(fix_dir, ret_name))
    df["时间"] = pd.to_datetime(df["时间"])
    df = df.sort_values("时间")
    return df


def diff_return(local_eq, fix_ret, out_dir):
    le = local_eq.copy()
    le["d"] = le["date"].dt.date
    fr = fix_ret.copy()
    fr["d"] = fr["时间"].dt.date
    le2 = le[["d", "策略收益"]].rename(columns={"策略收益": "本地策略收益%"})
    fr2 = fr[["d", "基准收益", "策略收益", "超额收益(%)"]].rename(
        columns={"策略收益": "聚宽策略收益%"})
    merged = pd.merge(fr2, le2, on="d", how="outer").sort_values("d")
    merged = merged.rename(columns={"d": "时间"})
    merged["策略收益差%"] = merged["本地策略收益%"] - merged["聚宽策略收益%"]
    merged.to_csv(os.path.join(out_dir, "return_diff.csv"), index=False)
    # 汇总
    n = len(merged.dropna(subset=["本地策略收益%"]))
    max_abs = merged["策略收益差%"].abs().max()
    final_local = merged["本地策略收益%"].dropna().iloc[-1]
    final_jq = merged["聚宽策略收益%"].dropna().iloc[-1]
    print(f"[收益] 对齐交易日={n} 最大|差|={max_abs:.3f}% "
          f"本地终值={final_local:.2f}% 聚宽终值={final_jq:.2f}%")
    # 逐日差>0.05% 的交易日
    bad = merged[merged["策略收益差%"].abs() > 0.05]
    print(f"[收益] 偏离>0.05% 的交易日: {len(bad)} 个")
    if len(bad):
        print(bad[["时间", "聚宽策略收益%", "本地策略收益%", "策略收益差%"]].head(20).to_string(index=False))
    return merged


def load_local_trades(local_dir):
    tr = pd.read_csv(os.path.join(local_dir, "trades.csv"))
    tr["dt"] = pd.to_datetime(tr["dt"])
    tr["date"] = tr["dt"].dt.date
    return tr


def load_fixture_trades(fix_dir, trd_name=FIXTURE_TRD):
    df = pd.read_csv(os.path.join(fix_dir, trd_name))
    # 聚宽格式含空行分隔同秒多笔；去掉全空行
    df = df.dropna(how="all")
    # 标的形如 "国防ETF(512670.XSHG)" -> 提取代码
    def code_of(s):
        if not isinstance(s, str):
            return None
        i = s.find("(")
        j = s.find(")")
        if i >= 0 and j > i:
            return s[i + 1:j]
        return s
    df["code"] = df["标的"].map(code_of)
    df["date"] = pd.to_datetime(df["日期"]).dt.date
    df["side"] = df["交易类型"].map({"买": "BUY", "卖": "SELL"})
    df["qty"] = df["成交数量"].astype(str).str.replace("股", "").str.replace(",", "").astype(float)
    return df


def diff_trades(local_tr, fix_tr, out_dir):
    # 按 date+code+side 聚合数量（聚宽每笔一行，本地每笔一行）
    lg = local_tr.groupby(["date", "code", "side"])["qty"].sum().reset_index()
    fg = fix_tr.groupby(["date", "code", "side"])["qty"].sum().reset_index()
    merged = pd.merge(
        fg, lg, on=["date", "code", "side"], how="outer",
        suffixes=("_jq", "_local"),
    )
    merged["qty_diff"] = merged["qty_local"].fillna(0) - merged["qty_jq"].fillna(0)
    merged.to_csv(os.path.join(out_dir, "trade_diff.csv"), index=False)
    n_jq = len(fg)
    n_local = len(lg)
    matched = merged["qty_local"].notna() & merged["qty_jq"].notna()
    print(f"[交易] 聚宽笔-组={n_jq} 本地笔-组={n_local} 两边都有的组={matched.sum()}")
    # 仅在聚宽出现（本地缺失）
    only_jq = merged[merged["qty_local"].isna()]
    only_local = merged[merged["qty_jq"].isna()]
    print(f"[交易] 仅聚宽有(本地缺失) 组={len(only_jq)} 仅本地有 组={len(only_local)}")
    # 按 date 统计聚宽 vs 本地 交易组数
    jq_by_date = fg.groupby("date").size()
    lo_by_date = lg.groupby("date").size()
    both = pd.DataFrame({"jq": jq_by_date, "local": lo_by_date}).fillna(0).astype(int)
    both["diff"] = both["local"] - both["jq"]
    both.to_csv(os.path.join(out_dir, "trade_by_date.csv"))
    print("[交易] 每日交易组数差异(本地-聚宽):")
    print(both.reset_index().to_string(index=False))
    return merged


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--local", default="data/quant_sim/jqwufu")
    ap.add_argument("--fixture", default="tests/fixtures/wufu_v52")
    ap.add_argument("--ret", default=None, help="收益CSV文件名(覆盖默认)")
    ap.add_argument("--trd", default=None, help="交易记录CSV文件名(覆盖默认)")
    ap.add_argument("--out", default="data/quant_sim/jqwufu/diff")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    local_eq = load_local_equity(args.local)
    fix_ret = load_fixture_return(args.fixture, args.ret or FIXTURE_RET)
    diff_return(local_eq, fix_ret, args.out)

    local_tr = load_local_trades(args.local)
    fix_tr = load_fixture_trades(args.fixture, args.trd or FIXTURE_TRD)
    diff_trades(local_tr, fix_tr, args.out)


if __name__ == "__main__":
    main()
