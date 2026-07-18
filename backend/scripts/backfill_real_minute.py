"""一次性修补本地分钟缓存中 real_<code> 真实段的段内空洞。

对每个已存在的 real_<code> 键：
  - 重新从 mootdx 拉全量 1 分钟线（mootdx 全量本身连续，含此前缺失的
    假期/停牌段，除非标的真长期停牌）；
  - 与本地 real_<code> 合并（后者优先，避免用旧数据覆盖本地已有正确段），
    去重后落盘本地分钟缓存。

效果：消除"段内断裂"（此前抓取时漏抓的交易日），使近 3 个月真实
分钟线连续，满足「近 3 个月真实」的数据要求，且结果可复现。

用法：
  PYTHONPATH=. python scripts/backfill_real_minute.py
"""
import os
import sys
import time
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.quant.jqengine.datasource.cache import DataCache
from app.quant.jqengine.datasource.manager import get_data_manager

GAP_DAYS = 4  # 间隔 > 4 天视为段内断裂（排除正常周末 2~3 天）
FETCH_TIMEOUT = 15  # 单只 mootdx 拉取超时（秒），避免个别标的卡死整轮


def find_gaps(df):
    if df is None or df.empty:
        return []
    diffs = df.index.to_series().diff().dt.total_seconds() / 86400
    interior = diffs.iloc[1:][diffs.iloc[1:] > GAP_DAYS]
    return list(interior.index)


def fetch_with_timeout(dm, code, timeout=FETCH_TIMEOUT):
    """带超时的 mootdx 拉取：超时返回 None（跳过），不阻塞整轮。"""
    box = {}
    def _run():
        try:
            box["df"] = dm.sources["mootdx"].get_minute(code)
        except Exception as e:
            box["err"] = e
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        return None, "timeout"
    if "err" in box:
        return None, str(box["err"])[:80]
    return box.get("df"), None


def main():
    dm = get_data_manager()
    cache = DataCache()
    keys = [k for k in cache.get_all("minute").keys() if k.startswith("real_")]

    total = len(keys)
    patched = 0
    no_gap = 0
    fetch_fail = 0
    t0 = time.time()

    for i, key in enumerate(keys):
        code = key[len("real_"):]
        local = cache.peek("minute", key)
        gaps_before = find_gaps(local)
        if not gaps_before:
            no_gap += 1
            continue
        try:
            fresh, err = fetch_with_timeout(dm, code)
        except Exception as e:
            fetch_fail += 1
            print(f"  [skip] {code} 异常: {e}")
            continue
        if fresh is None or fresh.empty:
            fetch_fail += 1
            if err:
                print(f"  [skip] {code} mootdx 失败/超时: {err}")
            continue
        # 本地优先（避免用可能更旧的全量覆盖本地已有正确段），补缺即可
        merged = local.combine_first(fresh)
        merged = merged[~merged.index.duplicated(keep="last")]
        merged = merged.sort_index()
        cache.put("minute", key, merged)
        patched += 1
        if (i + 1) % 50 == 0:
            print(f"  [{i+1}/{total}] 已修补 {patched} 只，耗时 {time.time()-t0:.0f}s")

    print(f"完成: 总 {total} 只, 无空洞 {no_gap}, 已修补 {patched}, "
          f"拉取失败/空 {fetch_fail}, 耗时 {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
