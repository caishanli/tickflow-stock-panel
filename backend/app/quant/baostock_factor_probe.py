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

import baostock as bs

BAOSTOCK_CODES = {
    ".SZ": "sz.",
    ".SH": "sh.",
    ".BJ": "bj.",
}


def to_bs_code(symbol: str) -> str:
    pure, suffix = symbol.split(".", 1)
    return BAOSTOCK_CODES.get("." + suffix.upper(), "sh.") + pure


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="+", default=["000001.SZ", "600000.SH"])
    ap.add_argument("--start", default="2016-01-01")
    ap.add_argument("--end", default="2026-12-31")
    args = ap.parse_args()

    lg = bs.login()
    if lg.error_code != "0":
        print(json.dumps({"error": "login failed", "msg": lg.error_msg}, ensure_ascii=False))
        return 1
    try:
        out = []
        for sym in args.symbols:
            code = to_bs_code(sym)
            rs = bs.query_adjust_factor(code=code, start_date=args.start, end_date=args.end)
            if rs is None or rs.error_code != "0":
                out.append({"symbol": sym, "error": getattr(rs, "error_code", None),
                            "msg": getattr(rs, "error_msg", "")})
                continue
            rows = []
            while rs.error_code == "0" and rs.next():
                rows.append(rs.get_row_data())
            out.append({
                "symbol": sym,
                "code": code,
                "event_count": len(rows),
                "latest_event_date": max((r[1] for r in rows), default=None),
                "events": rows[-6:],
            })
        print(json.dumps(out, ensure_ascii=False, indent=2))
    finally:
        bs.logout()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
