#!/usr/bin/env python3
"""一次性回填模拟盘历史数据：sim_trades.name + 持仓 name 键。

幂等，可重复执行：只补 name 为空的行/持仓。
用法：cd backend && uv run --extra dev python scripts/backfill_sim_names.py
"""
from __future__ import annotations

import json

from app.quant import db
from app.quant.simulate import names


def main() -> None:
    db.init_db()
    name_map = names.get_name_map()
    if not name_map:
        print("名称映射为空（stockdata 服务不可达？），跳过回填")
        return
    total_trades = 0
    total_pos = 0
    for acct in db.list_sim_accounts():
        aid = acct["id"]
        # 1) 成交记录
        with db.get_conn() as c:
            rows = c.execute(
                "SELECT rowid, code, name FROM sim_trades "
                "WHERE account_id=? AND (name IS NULL OR name='')",
                (aid,),
            ).fetchall()
            for r in rows:
                n = name_map.get(r["code"].split(".")[0]) or ""
                c.execute("UPDATE sim_trades SET name=? WHERE rowid=?",
                          (n, r["rowid"]))
            total_trades += len(rows)
        # 2) 持仓
        st = db.read_sim_state(aid)
        pos = st.get("positions") or {}
        changed = False
        for code, p in pos.items():
            if not p.get("name"):
                p["name"] = name_map.get(code.split(".")[0]) or ""
                changed = True
        if changed:
            st["positions_json"] = json.dumps(pos, ensure_ascii=False)
            db.upsert_sim_state(
                aid, float(st.get("cash", 0.0)),
                st["positions_json"], float(st.get("net_value", 0.0)),
                float(st.get("pnl", 0.0)), float(st.get("start_cash", 0.0)),
                json.dumps(st.get("stop_loss_log", []), ensure_ascii=False),
                st.get("dt"))
            total_pos += 1
    print(f"backfill done: trades={total_trades}, accounts_with_positions={total_pos}")


if __name__ == "__main__":
    main()
