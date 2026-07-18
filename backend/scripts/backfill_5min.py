"""补齐历史 5 分钟线（baostock）到 5min.db，供早期回测区间的当日成交量计算。

用法:
  python scripts/backfill_5min.py --start 20260315 --end 20260716 [--sleep 0.05]

仅补齐 5min.db 中缺失的区间；已覆盖的不重拉。带线程超时（baostock 源内部已处理）。
"""
import os
import sys
import time
import argparse
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.quant.jqengine.datasource.manager import get_data_manager


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="20260315")
    ap.add_argument("--end", default="20260716")
    ap.add_argument("--sleep", type=float, default=0.05)
    ap.add_argument("--timeout", type=int, default=120)
    args = ap.parse_args()

    dm = get_data_manager()
    # universe 取自已缓存的日线标的（去掉源前缀），与回测覆盖一致
    all_daily = dm.cache.get_all("daily")
    codes = []
    for k in all_daily:
        if k.startswith("tushare_"):
            codes.append(k[len("tushare_"):])
        elif k.startswith("astock_"):
            codes.append(k[len("astock_"):])
        elif k.startswith("baostock_"):
            codes.append(k[len("baostock_"):])
        else:
            codes.append(k)
    # 去重保序
    seen = set()
    codes = [c for c in codes if not (c in seen or seen.add(c))]
    print(f"[backfill_5min] universe={len(codes)} 目标 {args.start}~{args.end}")

    bs = dm.sources["baostock"]
    all_5 = dm.cache.get_all("5min")

    def _fmt(d):
        d = str(d).strip()
        if "-" in d:
            return d
        return f"{d[:4]}-{d[4:6]}-{d[6:8]}"

    s = _fmt(args.start)
    e = _fmt(args.end)

    ok = fail = skip = 0
    for i, code in enumerate(codes):
        key5 = f"baostock_5min_{code}"
        cached = all_5.get(key5)
        need = True
        if cached is not None and not getattr(cached, "empty", True):
            cmin = cached.index.min()
            cmax = cached.index.max()
            if cmin <= pd.Timestamp(args.start) and cmax >= pd.Timestamp(args.end):
                need = False
        if not need:
            skip += 1
            continue
        _first_err = None
        for attempt in range(3):
            try:
                df = bs.get_5min(code, s, e, timeout=args.timeout)
                if df is not None and not df.empty:
                    dm.cache.put("5min", key5, df)
                    ok += 1
                    break
            except Exception as e:
                if _first_err is None:
                    _first_err = f"{type(e).__name__}: {str(e)[:160]}"
                time.sleep(args.sleep * (attempt + 2))
        else:
            fail += 1
            if fail <= 3:
                print(f"[backfill_5min] FAIL {code}: {_first_err}", flush=True)
        if args.sleep:
            time.sleep(args.sleep)
        if (i + 1) % 10 == 0:
            print(f"[backfill_5min] 进度 {i+1}/{len(codes)} ok={ok} fail={fail} skip={skip}", flush=True)
    print(f"[backfill_5min] 完成 ok={ok} fail={fail} skip={skip}")


if __name__ == "__main__":
    main()
