"""解析聚宽 wufu-v5.2-probe.log 的 WFMISS| 行，仅补全 db 中『当日完全无数据』的缺口。

策略：
  - 若 real_<code> 在 date 当天已有数据 -> 跳过(不污染已有真实分钟)；
  - 若当天为空 -> 用聚宽 240 根 bar 整体写入该日(combine_first 到全序列)。
日志: WFMISS|code|YYYY-MM-DD|o,h,l,c,v,m;...
"""
import os
import re
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.quant.jqengine.datasource.cache import DataCache

FIELDS = ["open", "high", "low", "close", "volume", "money"]


def parse_log(path):
    out = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if "WFMISS|" not in line:
                continue
            payload = line.split("WFMISS|", 1)[1].rstrip("\n")
            parts = payload.split("|")
            if len(parts) != 3:
                continue
            code, date_str, body = [p.strip() for p in parts]
            if body.startswith("EMPTY") or body.startswith("ERR"):
                continue
            bars = []
            for b in body.split(";"):
                b = b.strip()
                if not b:
                    continue
                fs = b.split(",")
                if len(fs) != 6:
                    continue
                try:
                    bars.append(tuple(float(x) for x in fs))
                except ValueError:
                    continue
            if bars:
                out.append((code, date_str, bars))
    return out


def main():
    if len(sys.argv) < 2:
        print("用法: python scripts/parse_jq_dump.py <log>")
        sys.exit(1)
    path = sys.argv[1]
    rows = parse_log(path)
    print(f"解析到 {len(rows)} 条 (code,date) 分钟数据")

    cache = DataCache()
    written = skipped = 0
    for code, date_str, bars in rows:
        key = f"real_{code}"
        day = pd.Timestamp(date_str)
        local = cache.peek("minute", key)
        if local is not None and not local.empty:
            same_day = local[(local.index >= day) &
                             (local.index < day + pd.Timedelta(days=1))]
            if len(same_day) > 0:
                skipped += 1
                continue
        idx = pd.date_range(day + pd.Timedelta(hours=9, minutes=31),
                            periods=240, freq="1min")
        if len(idx) != len(bars):
            n = min(len(idx), len(bars))
            idx = idx[:n]
            bars = bars[:n]
        df_new = pd.DataFrame(
            {f: [b[i] for b in bars] for i, f in enumerate(FIELDS)},
            index=idx,
        )
        df_new.index.name = "datetime"
        if local is not None and not local.empty:
            merged = df_new.combine_first(local).sort_index()
        else:
            merged = df_new
        cache.put("minute", key, merged)
        written += 1
    print(f"写入 {written} 条, 跳过(当日已有数据) {skipped} 条")


if __name__ == "__main__":
    main()
