"""导出聚宽补全所需的"标的 -> 缺失日期区间"清单。

对每个仍断裂的 real_ 标的，把它的断裂点前后各扩 PAD 天，
合并重叠区间，输出 [{code, ranges:[[start,end],...]}, ...]。
聚宽脚本按此区间用 get_price(frequency='1m') 取数并回传。

输出 /tmp/opencode/jq_missing_ranges.json
"""
import json
import datetime as dt

PAD = 5  # 断裂点前后各扩 5 天，确保覆盖整段缺口

RAW = json.load(open("/tmp/opencode/gap_classify.json"))
per_code = RAW["per_code"]  # code -> {gap_ts: gap_days, ...}


def parse(ts):
    return dt.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")


def merge_ranges(ranges):
    ranges = sorted(ranges)
    out = []
    for s, e in ranges:
        if out and s <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], e))
        else:
            out.append((s, e))
    return out


tasks = []
for code, gaps in per_code.items():
    day_points = set()
    for ts in gaps:
        d = parse(ts).date()
        day_points.add(d)
    ranges = []
    for d in sorted(day_points):
        s = (d - dt.timedelta(days=PAD)).strftime("%Y-%m-%d")
        e = (d + dt.timedelta(days=PAD)).strftime("%Y-%m-%d")
        ranges.append((s, e))
    merged = merge_ranges(ranges)
    tasks.append({"code": code, "ranges": merged})

json.dump(tasks, open("/tmp/opencode/jq_missing_ranges.json", "w"),
          ensure_ascii=False, indent=0)
print(f"导出 {len(tasks)} 只标的的补全区间")
tot = sum(len(t["ranges"]) for t in tasks)
print(f"总区间数 {tot}")
print("样例:", tasks[0])
