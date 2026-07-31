#!/usr/bin/env python3
"""一次性迁移：把 kline_daily/kline_minute 里的 ETF 数据搬到独立表。

用法: cd backend && uv run python scripts/migrate_etf_to_separate_table.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb

DB_PATH = os.getenv("TICKFLOW_DB_PATH", os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data", "stock.duckdb",
))

def main():
    conn = duckdb.connect(DB_PATH, read_only=False)

    # 检查 instruments_etf 是否有数据
    etf_count = conn.execute("SELECT COUNT(*) FROM instruments_etf").fetchone()[0]
    if etf_count == 0:
        print("ERROR: instruments_etf 为空，请先填充 ETF 列表")
        sys.exit(1)
    print(f"instruments_etf: {etf_count} 只 ETF")

    migrations = [
        ("kline_daily", "kline_etf_daily"),
        ("kline_daily_enriched", "kline_etf_enriched"),
        ("kline_minute", "kline_etf_minute"),
    ]

    for src, dst in migrations:
        # 检查源表是否有数据
        try:
            src_total = conn.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]
        except Exception:
            print(f"SKIP: {src} 表不存在")
            continue

        # 检查源表里有多少 ETF 数据
        etf_rows = conn.execute(f"""
            SELECT COUNT(*) FROM {src}
            WHERE symbol IN (SELECT symbol FROM instruments_etf)
        """).fetchone()[0]

        if etf_rows == 0:
            print(f"{src}: 无 ETF 数据，跳过")
            continue

        # 迁移前快照
        dst_before = conn.execute(f"SELECT COUNT(*) FROM {dst}").fetchone()[0]

        # INSERT OR REPLACE 到目标表
        conn.execute(f"""
            INSERT OR REPLACE INTO {dst}
            SELECT s.* FROM {src} s
            JOIN instruments_etf e ON s.symbol = e.symbol
        """)

        # 从源表删除
        conn.execute(f"""
            DELETE FROM {src}
            WHERE symbol IN (SELECT symbol FROM instruments_etf)
        """)

        # 验证
        src_after = conn.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]
        dst_after = conn.execute(f"SELECT COUNT(*) FROM {dst}").fetchone()[0]

        print(f"{src} → {dst}: 迁移 {etf_rows} 行, "
              f"源表 {src_total}→{src_after}, 目标表 {dst_before}→{dst_after}")

    conn.close()
    print("迁移完成")

if __name__ == "__main__":
    main()
