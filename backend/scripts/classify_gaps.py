"""分类 real_ 仍断裂点：可补(mootdx 新鲜含该点) vs 源端真缺(mootdx 也缺)。

输出 /tmp/opencode/gap_classify.json：
  fixable: [(code, gap_ts), ...]   # mootdx 新鲜数据含断裂点 -> 可重新补齐
  genuine: [(code, gap_ts), ...]  # mootdx 也缺 -> 只能 5min 插值兜底
  per_code_gaps: {code: [gap_ts,...]}  # 每个标的全部断裂点
"""
import json
import time
import pandas as pd

from app.quant.jqengine.datasource.cache import DataCache
from app.quant.jqengine.datasource.mootdx_src import MootdxSource

GAP_DAYS = 4


def gaps_of(df):
    if df is None or df.empty:
        return []
    d = df.index.to_series().diff().dt.total_seconds() / 86400
    return list(d.iloc[1:][d.iloc[1:] > GAP_DAYS].index)


def main():
    c = DataCache()
    s = MootdxSource()
    keys = [k for k in c.get_all("minute").keys() if k.startswith("real_")]

    fixable, genuine, per_code = [], [], {}
    t0 = time.time()
    for i, k in enumerate(keys):
        code = k[len("real_"):]
        local = c.peek("minute", k)
        gs = gaps_of(local)
        if not gs:
            continue
        per_code[code] = [str(g) for g in gs]
        # 取该标的新鲜 mootdx 数据一次，批量判断各断裂点是否含
        try:
            fresh = s.get_minute(code)
        except Exception:
            fresh = None
        for g in gs:
            present = fresh is not None and g in fresh.index
            if present:
                fixable.append((code, str(g)))
            else:
                genuine.append((code, str(g)))
        if (i + 1) % 50 == 0:
            print(f"  [{i+1}/{len(keys)}] fixable={len(fixable)} genuine={len(genuine)} "
                  f"{time.time()-t0:.0f}s", flush=True)

    out = {
        "fixable": fixable,
        "genuine": genuine,
        "per_code_gaps": per_code,
    }
    with open("/tmp/opencode/gap_classify.json", "w") as f:
        json.dump(out, f, indent=0)
    print(f"完成: 仍断裂标的={len(per_code)}, 可补点={len(fixable)}, "
          f"源端真缺点={len(genuine)}, 耗时{time.time()-t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
