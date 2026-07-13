#!/usr/bin/env python3
"""对齐比较（适配本回测 trades.csv 列: dt,code,side,price,qty）。

用法: python compare_adapted.py <our_dir> [ref_dir]
参考收益基准: 沪深300(510300.XSHG)
"""
import csv
import os
import re
import sys

REPO = "/home/ubuntu/quant-daydayup"


def _code_in(s):
    m = re.search(r"\((\d+\.\w+)\)", s)
    return m.group(1) if m else s.strip()


def _num(s):
    if s is None:
        return None
    s = str(s).replace("股", "").replace("元", "").replace(",", "").replace('"', "").strip()
    try:
        return float(s)
    except Exception:
        return None


def _norm_date(s):
    parts = s.strip().replace("/", "-").split("-")
    if len(parts) == 3:
        return f"{int(parts[0]):04d}-{int(parts[1]):02d}-{int(parts[2]):02d}"
    return s.strip().replace("/", "-")


def _norm_time(s):
    s = (s or "").strip()
    if not s:
        return "13:10"
    parts = s.split(":")
    if len(parts) >= 2:
        try:
            return f"{int(parts[0]):02d}:{int(parts[1]):02d}"
        except Exception:
            return s[:5]
    return s[:5]


def load_ref_trades(path):
    rows = []
    with open(path, encoding="gbk", errors="ignore") as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if len(row) < 7:
                continue
            if not row[0].strip():
                continue
            date = _norm_date(row[0])
            t = _norm_time(row[1])
            code = _code_in(row[2])
            side_raw = row[3].strip()
            if not side_raw:
                continue
            qty = _num(row[5])
            price = _num(row[6])
            amt = _num(row[7])
            rows.append({
                "date": date, "time": t, "code": code,
                "side": "BUY" if side_raw.startswith("买") else "SELL",
                "qty": abs(qty) if qty is not None else None,
                "price": price, "amount": amt,
            })
    return rows


def load_our_trades(path):
    rows = []
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            dt = row.get("dt", "")
            date = dt[:10]
            t = _norm_time(dt[11:]) if len(dt) > 15 else "13:10"
            code = row.get("code", "")
            price = _num(row.get("price"))
            qty = _num(row.get("qty"))
            side = row.get("side", "").strip().upper()
            rows.append({
                "date": date, "time": t, "code": code,
                "side": side, "qty": abs(qty) if qty is not None else None,
                "price": price, "amount": None,
            })
    return rows


def load_ref_returns(path):
    out = {}
    with open(path, encoding="gbk", errors="ignore") as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if len(row) < 8:
                continue
            if not row[0].strip():
                continue
            date = row[0][:10]
            bench_cum = _num(row[1])
            strat_cum = _num(row[2])
            weak = row[8] if len(row) > 8 else None
            out[date] = (bench_cum, strat_cum, weak)
    return out


def load_our_equity(path):
    out = {}
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            d = row.get("date", "")
            v = _num(row.get("value"))
            if d and v is not None:
                out[d] = v
    return out


def main():
    our_dir = sys.argv[1] if len(sys.argv) > 1 else os.path.join(REPO, "data", "quant_sim", "jqwufu")
    ref_dir = sys.argv[2] if len(sys.argv) > 2 else os.path.join(REPO, "strategy", "五福闹新春-v5.2")

    ref_trades = load_ref_trades(os.path.join(ref_dir, "20260101-20260708交易记录.csv"))
    our_trades = load_our_trades(os.path.join(our_dir, "trades.csv"))
    ref_ret = load_ref_returns(os.path.join(ref_dir, "20260101-20260708收益.csv"))
    our_eq = load_our_equity(os.path.join(our_dir, "equity.csv"))

    print(f"参考成交 {len(ref_trades)} 笔, 本回测成交 {len(our_trades)} 笔")
    print(f"参考收益交易日 {len(ref_ret)} 天, 本回测净值交易日 {len(our_eq)} 天")

    def key(r):
        return (r["date"], r["time"], r["code"], r["side"])

    ref_map, our_map = {}, {}
    for r in ref_trades:
        ref_map.setdefault(key(r), []).append(r)
    for r in our_trades:
        our_map.setdefault(key(r), []).append(r)
    ref_keys, our_keys = set(ref_map), set(our_map)
    matched = ref_keys & our_keys
    only_ref = ref_keys - our_keys
    only_ours = our_keys - ref_keys
    print(f"\n[交易] 签名匹配 {len(matched)} 笔")
    print(f"[交易] 仅参考有(本回测缺失) {len(only_ref)} 笔")
    print(f"[交易] 仅本回测有(参考无) {len(only_ours)} 笔")

    print("\n[交易] 逐月签名匹配率(按 date[:7])")
    months = sorted({k[0][:7] for k in (ref_keys | our_keys)})
    for m in months:
        rv = {k for k in ref_keys if k[0][:7] == m}
        ov = {k for k in our_keys if k[0][:7] == m}
        inter = rv & ov
        union = rv | ov
        rate = len(inter) / len(union) * 100 if union else 0
        print(f"  {m}: 参考{len(rv)} 我们{len(ov)} 匹配{len(inter)} 匹配率={rate:.0f}%")

    qty_diff = price_diff = 0
    for k in sorted(matched):
        for a, b in zip(ref_map[k], our_map[k]):
            dq = abs((a["qty"] or 0) - (b["qty"] or 0)) if a["qty"] and b["qty"] else 0
            dp = (abs(a["price"] - b["price"]) / (a["price"] or 1)
                  if a["price"] and b["price"] else 0)
            ok = "OK" if dq <= 1 and dp < 0.01 else "DIFF"
            if dq > 1:
                qty_diff += 1
            if dp >= 0.01:
                price_diff += 1
            if ok == "DIFF":
                print(f"  [{ok}] {a['date']} {a['time']} {a['side']} {a['code']} "
                      f"qty:{a['qty']}/{b['qty']} price:{a['price']}/{b['price']}")
    print(f"[交易] 数量差异(>1股) {qty_diff} 笔, 价格差异(>=1%) {price_diff} 笔")

    print("\n--- 仅参考有(前30) ---")
    for k in sorted(only_ref)[:30]:
        r = ref_map[k][0]
        print(f"  {r['date']} {r['time']} {r['side']} {r['code']} qty={r['qty']} price={r['price']}")
    print("--- 仅本回测有(前30) ---")
    for k in sorted(only_ours)[:30]:
        r = our_map[k][0]
        print(f"  {r['date']} {r['time']} {r['side']} {r['code']} qty={r['qty']} price={r['price']}")

    print("\n[收益] 按交易日对比累计收益率(%)")
    dates = sorted(set(ref_ret) & set(our_eq))
    print(f"  共同交易日 {len(dates)} 天")
    if dates:
        start_v = our_eq[dates[0]]
        max_abs = 0.0
        worst = None
        for d in dates:
            bench_cum, strat_cum, weak = ref_ret[d]
            our_cum = (our_eq[d] / 100000.0 - 1) * 100
            diff = (our_cum or 0) - (strat_cum or 0)
            if strat_cum is not None and abs(diff) > max_abs:
                max_abs = abs(diff)
                worst = (d, our_cum, strat_cum, diff)
            if d.endswith("01") or d == dates[-1] or len(dates) <= 30:
                print(f"  {d} 参考累计={strat_cum:.2f}% 本回测累计={our_cum:.2f}% 差={diff:+.2f}% 基准={bench_cum}")
        print(f"  最大累计收益偏差: {max_abs:.2f}% @ {worst[0] if worst else '-'}")
        d = dates[-1]
        bench_cum, strat_cum, weak = ref_ret[d]
        our_cum = (our_eq[d] / 100000.0 - 1) * 100
        print(f"  末日 {d}: 参考累计收益={strat_cum:.2f}%, 本回测={our_cum:.2f}%, 基准={bench_cum}%")


if __name__ == "__main__":
    main()
