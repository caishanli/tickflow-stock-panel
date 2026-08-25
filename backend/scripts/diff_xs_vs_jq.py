"""f51e08f9 小市值策略 本地回测 vs 聚宽 fixture 对齐比对。

口径参照 wufu 对齐（docs/2026-08-06-wufu-v52-alignment-diagnosis.md）：
- 共同交易组覆盖率、组内成交价比中位数
- 周度目标清单重合度
- 月度信号一致性

用法: uv run python scripts/diff_xs_vs_jq.py <run_id>
"""
import csv
import datetime as dt
import json
import sqlite3
import sys
from collections import defaultdict

RUN_ID = sys.argv[1] if len(sys.argv) > 1 else None
FIXTURE_DIR = "tests/fixtures/xiaoshizhi_v30/backtest_260101-260821"
WIN = (dt.date(2026, 4, 1), dt.date(2026, 8, 21))


def load_local_trades(run_id):
    con = sqlite3.connect("/home/caisl/tickflow-stock-panel/data/quant.db")
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT * FROM backtest_trades WHERE run_id=?", (run_id,)).fetchall()
    out = []
    for r in rows:
        d = dt.datetime.strptime(r["ts"][:10], "%Y-%m-%d").date()
        side = "买" if r["action"].upper() in ("BUY", "B") else "卖"
        out.append({"date": d, "code": r["code"], "side": side,
                    "price": float(r["price"]), "qty": float(r["amount"])})
    return out


def load_jq_trades():
    rows = list(csv.reader(open(f"{FIXTURE_DIR}/交易记录.csv", encoding="gbk")))[1:]
    out = []
    for r in rows:
        if not r[0]:
            continue
        d = dt.datetime.strptime(r[0], "%Y/%m/%d").date()
        if not (WIN[0] <= d <= WIN[1]):
            continue
        code = r[2].split("(")[1].rstrip(")")
        praw = r[6].replace(",", "")
        price = float(praw) if praw not in ("--", "") else 0.0
        qty = float(r[5].replace("股", "").replace(",", "") or 0)
        out.append({"date": d, "code": code, "side": r[3],
                    "price": price, "qty": qty})
    return out


def main():
    global RUN_ID
    if RUN_ID is None:
        con = sqlite3.connect("/home/caisl/tickflow-stock-panel/data/quant.db")
        RUN_ID = con.execute("SELECT id FROM backtest_runs ORDER BY created_at DESC LIMIT 1").fetchone()[0]
    print(f"run_id={RUN_ID} 窗口={WIN[0]}~{WIN[1]}")
    lt = sorted(load_local_trades(RUN_ID), key=lambda t: (t["date"], t["code"], t["side"]))
    jt = sorted(load_jq_trades(), key=lambda t: (t["date"], t["code"], t["side"]))
    print(f"本地成交 {len(lt)} 笔 | 聚宽(窗口内) {len(jt)} 笔")

    # ---- 按日+代码 分组（同日同标的的买卖归一组）----
    def group(ts):
        g = defaultdict(list)
        for t in ts:
            g[(t["date"], t["code"])].append(t)
        return g
    lg, jg = group(lt), group(jt)
    common_days = set(lg) & set(jg)
    union_days = set(lg) | set(jg)
    cover = len(common_days) / len(union_days) * 100 if union_days else 0
    print(f"交易组(日,标的): 本地 {len(lg)} | 聚宽 {len(jg)} | 共同 {len(common_days)} "
          f"| 覆盖率 {cover:.1f}%")

    # 价格比（共同组、首笔对首笔）
    ratios = []
    flips = 0
    for k in sorted(common_days):
        l, j = lg[k], jg[k]
        ls = {t["side"]: t for t in l}
        js = {t["side"]: t for t in j}
        for side in ("买", "卖"):
            if side in ls and side in js:
                if ls[side]["price"] and js[side]["price"]:
                    ratios.append(ls[side]["price"] / js[side]["price"])
        # 方向翻转：本地只有买而聚宽只有卖（或反之）
        if set(ls) ^ set(js):
            flips += 1
    if ratios:
        ratios.sort()
        med = ratios[len(ratios)//2]
        print(f"可比较价格对 {len(ratios)} | 成交价比中位 {med:.4f} | "
              f"min {ratios[0]:.4f} / max {ratios[-1]:.4f}")
    print(f"方向不一致组: {flips}")

    only_local = sorted(set(lg) - set(jg))
    only_jq = sorted(set(jg) - set(lg))
    print(f"\n仅本地有 ({len(only_local)}):",
          [(str(d), c) for d, c in only_local[:8]])
    print(f"仅聚宽有 ({len(only_jq)}):",
          [(str(d), c) for d, c in only_jq[:8]])

    # ---- 净值对比（窗口末日累计收益）----
    con = sqlite3.connect("/home/caisl/tickflow-stock-panel/data/quant.db")
    eq = con.execute(
        "SELECT dt, value FROM backtest_equity WHERE run_id=? ORDER BY dt",
        (RUN_ID,)).fetchall()
    if eq:
        cap0 = 1000000.0
        print(f"\n本地期末净值: {eq[-1][1]:,.0f} "
          f"(区间收益 {(eq[-1][1]/cap0-1)*100:.2f}%)")
    jr = list(csv.reader(open(f"{FIXTURE_DIR}/收益.csv", encoding="gbk")))[1:]
    jq_in_win = [r for r in jr if WIN[0].isoformat() <= r[0][:10] <= WIN[1].isoformat()]
    if jq_in_win:
        first, last = jq_in_win[0], jq_in_win[-1]
        v0 = 1 + float(first[2])/100 if False else None
        # 聚宽「策略收益」为自起点累计%；窗口内起止差即区间收益
        ret_jq = float(last[2]) - float(first[2])
        print(f"聚宽窗口内区间收益: {ret_jq:+.2f}% (期初 {first[2]}% → 期末 {last[2]}%)")


if __name__ == "__main__":
    main()
