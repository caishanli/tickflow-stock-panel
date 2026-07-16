"""B 阶段：分类 real_ 全部断裂点。

对每只仍断裂的 real_ 标的，取 mootdx 新鲜全量分钟数据，
判断每个断裂点（与本地间隔>4天）是否“在新鲜数据里也存在”，
从而区分：
  - 可补(fixable): 本地缺该 index，但 mootdx 新鲜有 -> 重抓 combine_first 可补
  - 源端真缺(genuine): mootdx 新鲜也带同样的>4天间隔 -> 重抓无效，需 5min 插值

本地“含该断裂点”本身不代表已补好（combine_first 只填 index，长假间隔仍在），
所以判据是：mootdx 新鲜数据在该断裂点“是否真的有中间交易日分钟”。
简化判据：比较 本地 vs 新鲜 在同一断裂点处的“间隔天数”，若新鲜间隔明显更小(<4天)则视为可补；否则真缺。

输出 /tmp/opencode/gap_classify.json
"""
import json
import time

import pandas as pd

from app.quant.jqengine.datasource.cache import DataCache
from app.quant.jqengine.datasource.mootdx_src import MootdxSource

GAP_DAYS = 4


def gaps_with_days(df):
    if df is None or df.empty:
        return []
    d = df.index.to_series().diff().dt.total_seconds() / 86400
    sub = d.iloc[1:][d.iloc[1:] > GAP_DAYS]
    return [(ts, float(d[ts])) for ts in sub.index]


def main():
    c = DataCache()
    s = MootdxSource()
    keys = [k for k in c.get_all("minute").keys() if k.startswith("real_")]

    # 断点续跑：跳过已完成的 code
    import os
    done = set()
    if os.path.exists("/tmp/opencode/gap_classify.json"):
        try:
            prev = json.load(open("/tmp/opencode/gap_classify.json"))
            done = set(prev.get("per_code", {}).keys())
            fixable = list(prev.get("fixable", []))
            genuine = list(prev.get("genuine", []))
            per_code = dict(prev.get("per_code", {}))
            print(f"续跑: 已完成 {len(done)} 只, fixable={len(fixable)} genuine={len(genuine)}",
                  flush=True)
        except Exception:
            pass

    fixable, genuine, per_code = fixable, genuine, per_code
    t0 = time.time()
    for i, k in enumerate(keys):
        code = k[len("real_"):]
        if code in done:
            continue
        local = c.peek("minute", k)
        lgaps = gaps_with_days(local)
        if not lgaps:
            done.add(code)
            continue
        per_code[code] = {str(ts): round(dd, 2) for ts, dd in lgaps}
        try:
            fresh = s.get_minute(code)
            fgaps = gaps_with_days(fresh)
        except Exception:
            fgaps = []
        fresh_gap_days = {str(ts): dd for ts, dd in fgaps}
        for ts, ldd in lgaps:
            ts_s = str(ts)
            # mootdx 新鲜在该点间隔<=4天 => 新鲜能补出中间交易日 => 可补
            fdd = fresh_gap_days.get(ts_s)
            if fdd is not None and fdd <= GAP_DAYS:
                fixable.append((code, ts_s, round(ldd, 2), round(fdd, 2)))
            else:
                genuine.append((code, ts_s, round(ldd, 2), None if fdd is None else round(fdd, 2)))
        if (i + 1) % 10 == 0:
            with open("/tmp/opencode/gap_classify.json", "w") as _f:
                json.dump(
                    {"fixable": fixable, "genuine": genuine, "per_code": per_code,
                     "progress": [i + 1, len(keys)]},
                    _f,
                )
                _f.flush()
                import os as _os
                _os.fsync(_f.fileno())
            print(f"[{i+1}/{len(keys)}] fixable={len(fixable)} genuine={len(genuine)} "
                  f"{time.time()-t0:.0f}s", flush=True)

    with open("/tmp/opencode/gap_classify.json", "w") as _f:
        json.dump({"fixable": fixable, "genuine": genuine, "per_code": per_code}, _f)
        _f.flush()
        import os as _os
        _os.fsync(_f.fileno())
    print(f"完成: 仍断裂{len(per_code)} 可补点{len(fixable)} 真缺点{len(genuine)} "
          f"{time.time()-t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
