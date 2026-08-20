"""聚宽式 .py 策略 CRUD（文件落 data/quant_strategies/，元数据落 quant.db）。"""
from __future__ import annotations

import os
import uuid

from .. import db
from ..config import CONFIG


def _path(sid):
    return os.path.join(CONFIG.strategies_dir, f"{sid}.py")


def _ensure():
    os.makedirs(CONFIG.strategies_dir, exist_ok=True)


def list_strategies():
    with db.get_conn() as c:
        rows = c.execute(
            "SELECT id,name,updated_at FROM strategies ORDER BY updated_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def get_strategy(sid):
    with db.get_conn() as c:
        row = c.execute(
            "SELECT id,name,file FROM strategies WHERE id=?", (sid,)
        ).fetchone()
    if not row:
        return None
    row = dict(row)
    p = _path(sid)
    row["code"] = open(p, encoding="utf-8").read() if os.path.exists(p) else ""
    return row


def save_strategy(sid, name, code):
    _ensure()
    with open(_path(sid), "w", encoding="utf-8") as f:
        f.write(code)
    with db.get_conn() as c:
        c.execute(
            "INSERT INTO strategies(id,name,file,updated_at) VALUES(?,?,?,datetime('now')) "
            "ON CONFLICT(id) DO UPDATE SET name=excluded.name,file=excluded.file,"
            "updated_at=datetime('now')",
            (sid, name, f"{sid}.py"),
        )
    return get_strategy(sid)


def delete_strategy(sid):
    if os.path.exists(_path(sid)):
        os.remove(_path(sid))
    with db.get_conn() as c:
        c.execute("DELETE FROM strategies WHERE id=?", (sid,))


def export_strategy(sid):
    s = get_strategy(sid)
    return s["code"] if s else ""


def import_strategy(name, code):
    sid = uuid.uuid4().hex[:8]
    save_strategy(sid, name, code)
    return sid
