"""一次性迁移：行情缓存 SQLite（pickle BLOB）→ 每键 parquet（zstd）。

用法:
  python scripts/migrate_cache_to_parquet.py [--data-dir DIR]
      [--freq daily|minute|5min] [--keep-db]

对 {daily,minute,5min}.db 逐键读出 pickle 帧 → 走 DataCache.put 写 parquet
（含 __attrs__ 元数据列）→ 读回校验行数/列数一致。全部键通过后删除对应
.db（及 -wal/-shm），除非 --keep-db。幂等：库不存在/已迁移自动跳过，可重跑。
"""
import argparse
import io
import os
import sqlite3
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.quant.jqengine.config import CONFIG
from app.quant.jqengine.datasource.cache import DataCache

FREQS = ("daily", "minute", "5min")


def _sizeof(path):
    total = 0
    for suffix in ("", "-wal", "-shm"):
        p = path + suffix
        if os.path.exists(p):
            total += os.path.getsize(p)
    return total


def _migrate_freq(freq, data_dir, cache, keep_db):
    db_path = os.path.join(data_dir, f"{freq}.db")
    if not os.path.exists(db_path):
        print(f"[{freq}] {db_path} 不存在，跳过")
        return True
    old_size = _sizeof(db_path)
    conn = sqlite3.connect(db_path)
    total = conn.execute("SELECT COUNT(*) FROM cache").fetchone()[0]
    print(f"[{freq}] {total} 个键，旧库 {old_size / 1e6:.1f} MB")

    ok, failed = 0, []
    for key, blob in conn.execute("SELECT key, data FROM cache"):
        try:
            df = pd.read_pickle(io.BytesIO(blob))
            cache.put(freq, key, df)
            back = cache.peek(freq, key)
            if back is None or back.shape != df.shape:
                raise ValueError(
                    f"读回校验失败: 原 {df.shape} vs "
                    f"{None if back is None else back.shape}")
            ok += 1
        except Exception as e:
            failed.append((key, str(e)))
            print(f"[{freq}] 键 {key} 迁移失败: {e}")
    conn.close()
    print(f"[{freq}] 成功 {ok}/{total}")

    if failed:
        print(f"[{freq}] 有 {len(failed)} 个键失败，保留旧库 {db_path}")
        return False
    if keep_db:
        print(f"[{freq}] --keep-db：保留旧库 {db_path}")
        return True
    for suffix in ("", "-wal", "-shm"):
        p = db_path + suffix
        if os.path.exists(p):
            os.remove(p)
    new_size = sum(
        os.path.getsize(os.path.join(data_dir, freq, f))
        for f in os.listdir(os.path.join(data_dir, freq))
        if f.endswith(".parquet")
    ) if os.path.isdir(os.path.join(data_dir, freq)) else 0
    print(f"[{freq}] 旧库已删除；parquet 合计 {new_size / 1e6:.1f} MB "
          f"（原 {old_size / 1e6:.1f} MB）")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=CONFIG["DATA_DIR"])
    ap.add_argument("--freq", choices=FREQS, default=None, help="只迁移指定频率")
    ap.add_argument("--keep-db", action="store_true", help="迁移后保留旧 SQLite 库")
    args = ap.parse_args()

    cache = DataCache(root=args.data_dir)
    freqs = (args.freq,) if args.freq else FREQS
    results = [_migrate_freq(f, args.data_dir, cache, args.keep_db) for f in freqs]
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
