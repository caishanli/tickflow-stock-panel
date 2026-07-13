#!/usr/bin/env python3
"""回测 五福闹新春-v5.2 策略（jqengine 移植版），导出成交/净值用于与聚宽参考对齐。

用法:
  python scripts/run_jq_wufu.py [--start 2026-01-01] [--end 2026-07-08] \
        [--strategy .../wufu-v5.2.py] [--out runtime/jqwufu]

输出:
  <out>/trades.csv        成交明细
  <out>/equity.csv        每日净值
"""
import argparse
import csv as _csv
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.quant.jqengine.engine.jq.loader import load_strategy
from app.quant.jqengine.engine.backtrader_bridge import run_backtest
from app.quant.jqengine.datasource.manager import DataManager
from app.quant.jqengine.config import CONFIG

REPO = "/home/ubuntu/quant-daydayup"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-01-01")
    ap.add_argument("--end", default="2026-07-08")
    ap.add_argument(
        "--strategy",
        default=os.path.join(REPO, "strategy", "五福闹新春-v5.2", "wufu-v5.2.py"),
    )
    ap.add_argument("--out", default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "runtime", "jqwufu"))
    ap.add_argument("--cash", type=float, default=100000.0)
    ap.add_argument("--fee", type=float, default=0.0001)
    ap.add_argument("--slippage", type=float, default=0.0001)
    args = ap.parse_args()

    out = os.path.abspath(args.out)
    os.makedirs(out, exist_ok=True)
    with open(args.strategy, encoding="utf-8") as f:
        code = f.read()

    dm = DataManager()
    dm.preload_daily()
    bundle = load_strategy(code, dm, args.fee, args.slippage, args.cash)
    dm.set_minute_window(args.start, args.end)

    g = bundle.ctx.g
    prewarm = list(set(
        ["000300.XSHG"] + list(getattr(g, "fixed_etf_pool", []) or [])
        + ["399101.XSHE", "399006.XSHE", "000510.XSHG"]
        + ["511880.XSHG"]
    ))
    print(f"[driver] 预热 baostock 5min 缓存 ({len(prewarm)} 只)...")
    for c in prewarm:
        try:
            dm._load_minute_merged(c, full=True)
        except Exception:
            pass
    print("[driver] 预热完成")

    from app.quant.jqengine.engine.jq.api import _reset  # noqa
    _reset(dm, args.fee, args.slippage, args.cash)
    bundle = load_strategy(code, dm, args.fee, args.slippage, args.cash)
    benchmark = "000300.XSHG"
    feeds = [benchmark]
    print(f"[driver] feeds({len(feeds)}): clock={benchmark} (其余惰性加载)")

    metrics, csv_path, equity, daily_equity = run_backtest(
        bundle, feeds, args.start, args.end, "1min",
        args.fee, args.slippage, cash=args.cash,
    )

    out_trades = os.path.join(out, "trades.csv")
    if csv_path and os.path.exists(csv_path):
        shutil.copy(csv_path, out_trades)

    out_eq = os.path.join(out, "equity.csv")
    with open(out_eq, "w", newline="") as f:
        w = _csv.writer(f)
        w.writerow(["date", "value", "ret_pct"])
        prev = None
        for d, v in daily_equity:
            ret = "" if prev is None else f"{(v / prev - 1) * 100:.4f}"
            w.writerow([d.isoformat() if hasattr(d, "isoformat") else d,
                        f"{v:.4f}", ret])
            prev = v

    print("[driver] metrics:", metrics)
    print(f"[driver] trades -> {out_trades}")
    print(f"[driver] equity -> {out_eq}")


if __name__ == "__main__":
    main()
