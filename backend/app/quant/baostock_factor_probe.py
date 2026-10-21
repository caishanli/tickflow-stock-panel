"""探测 baostock 复权因子的最新覆盖范围（是否比归档 2026-04-17 更新）。

baostock 的 query_adjust_factor 返回事件行：
  [code, dividOperateDate, foreAdjustFactor, backAdjustFactor, adjustFactor]
其中 backAdjustFactor 是"后复权因子"（累计、随时间递增），
foreAdjustFactor 是前复权因子。归档 `4.21更新` 日线里的 `adj_factor`
列与 backAdjustFactor 同构（已验证：000001.SZ 2019=108.031 → 2026-04=134.5794）。

用法: uv run python -m app.quant.baostock_factor_probe --symbols 000001.SZ 600000.SH
"""

from __future__ import annotations

import argparse
import json

# 代码转换统一走 baostock_factor_sync.to_bs_code（避免三份实现漂移）
from app.quant.baostock_factor_sync import to_bs_code
from app.services.baostock_backfill import _guarded, query_adjust_factor_rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="+", default=["000001.SZ", "600000.SH"])
    ap.add_argument("--start", default="2016-01-01")
    ap.add_argument("--end", default="2026-12-31")
    args = ap.parse_args()

    # 墙钟守护：baostock 的 socket 挂起会**永久阻塞**这个 CLI（仓库其它入口
    # 都走 _guarded）
    try:
        _guarded(lambda: _probe(args.symbols, args.start, args.end), timeout=300)
    except Exception as exc:
        print(json.dumps({"error": type(exc).__name__, "msg": str(exc)[:300]},
                         ensure_ascii=False))
        return 1
    return 0


def _probe(symbols, start, end) -> None:
    import baostock as bs

    lg = _guarded(bs.login, timeout=30)
    if lg.error_code != "0":
        print(json.dumps({"error": "login failed", "msg": lg.error_msg}, ensure_ascii=False))
        return
    try:
        out = []
        for sym in symbols:
            code = to_bs_code(sym)
            rows = _guarded(
                lambda c=code: query_adjust_factor_rows(c, start, end, timeout=60, retries=1),
                timeout=90)
            out.append({
                "symbol": sym,
                "code": code,
                "event_count": len(rows),
                "latest_event_date": max((r[1] for r in rows), default=None),
                "events": rows[-6:],
            })
        print(json.dumps(out, ensure_ascii=False, indent=2))
    finally:
        _guarded(bs.logout, timeout=30)


if __name__ == "__main__":
    raise SystemExit(main())
