#!/usr/bin/env python3
"""用聚宽探针结果(result.txt)更新本地 ETF 快照名称，对齐聚宽 display_name。

输入：backend/scripts/result.txt（聚宽全量 ETF `code,display_name,start,end`）
输出：data/quant_kline/etf_universe_snapshot.json 的 names 字段（重叠部分替换为聚宽名）
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

SNAPSHOT = Path("/home/caisl/tickflow-stock-panel/data/quant_kline/etf_universe_snapshot.json")
PROBE = Path("/home/caisl/tickflow-stock-panel/backend/scripts/result.txt")


def parse_jq_names(path: Path) -> dict[str, str]:
    """解析聚宽探针输出，返回 {code: display_name}。取最后出现（日期较新）。"""
    out: dict[str, str] = {}
    pat = re.compile(r"(\d{6}\.(?:XSHG|XSHE)),([^,]+),\d{4}-\d{2}-\d{2}")
    with open(path, encoding="utf-8") as f:
        for line in f:
            m = pat.search(line)
            if m:
                out[m.group(1)] = m.group(2).strip()
    return out


def main() -> int:
    jq_names = parse_jq_names(PROBE)
    if not jq_names:
        print("未解析到聚宽名称，退出")
        return 1
    with open(SNAPSHOT, encoding="utf-8") as f:
        snap = json.load(f)
    names = snap.get("names") or {}
    replaced = 0
    changed = []
    for code, jq_name in jq_names.items():
        if code in names and names.get(code) != jq_name:
            changed.append((code, names[code], jq_name))
            names[code] = jq_name
            replaced += 1
        elif code not in names:
            names[code] = jq_name
            replaced += 1
    snap["names"] = names
    # 原子写回
    tmp = str(SNAPSHOT) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(snap, f, ensure_ascii=False)
    import os
    os.replace(tmp, SNAPSHOT)
    print(f"聚宽名称 {len(jq_names)} 只，替换/新增 {replaced} 只")
    print("=== 变更示例（前 15）===")
    for code, old, new in changed[:15]:
        print(f"  {code}: {old} -> {new}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
