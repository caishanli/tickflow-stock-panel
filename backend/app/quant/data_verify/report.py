"""核验结果的落盘产物：状态报告 + 隔离清单。

两个文件都放在数据根（``settings.data_dir``）下、与分区表同级，供面板
（``GET /api/settings/data-verify/status``）和人直接查看：

- ``data_verify_status.json``：**只保留固定键** ``last_run_at`` / ``stage`` /
  ``findings`` / ``repaired`` / ``quarantined`` / ``sources``。runner 的报告
  还带 ``days`` / ``state_path`` / ``skipped`` 之类的运行细节，那些属于 job
  结果（``job_store.succeed``），不进状态文件——状态文件是「面板一眼看
  上一轮核验结果」的稳定契约，键随 runner 演化而漂移会让前端跟着碎。
- ``data_verify_quarantine.json``：跨轮**合并**的隔离清单（design §6.3：外源
  与本地对峙、无多数时不修，只记账）。去重键是
  ``table/day/symbol/reason``；已有条目优先（重复写入不覆盖 detail，避免
  同一问题每轮被后来的空 detail 刷掉）。

两者一律 ``.tmp`` + :func:`os.replace` 原子写：面板可能在任意时刻读，
半截 JSON 会让状态页直接 500。写失败时 ``.tmp`` 会被清掉，不留垃圾。
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Final

logger = logging.getLogger(__name__)

#: 状态文件（数据根下）。
STATUS_FILENAME: Final = "data_verify_status.json"

#: 隔离清单文件（数据根下）。
QUARANTINE_FILENAME: Final = "data_verify_quarantine.json"

#: 状态文件的固定键（顺序即序列化顺序，便于人读 diff）。
STATUS_KEYS: Final[tuple[str, ...]] = (
    "last_run_at",
    "stage",
    "findings",
    "repaired",
    "quarantined",
    "sources",
)

#: 隔离清单的去重键；缺字段按 ``None`` 归一（分区级结论可以没有 symbol）。
#: 注意 ``symbol=None`` 与 ``symbol=""`` 视为同一分区级结论。
QUARANTINE_KEY_FIELDS: Final[tuple[str, ...]] = ("table", "day", "symbol", "reason")


def _now_iso() -> str:
    """状态文件的时间戳（秒级，可被 ``datetime.fromisoformat`` 解析）。"""
    return datetime.now().replace(microsecond=0).isoformat()


def _atomic_write_json(path: Path, payload: Any) -> Path:
    """原子写 JSON：先写 ``.tmp`` 再 :func:`os.replace`，失败清理临时文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    try:
        tmp.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()
    return path


def _as_count(value: Any) -> int:
    """计数归一：``None`` → 0；列表/元组（明细）→ 条数；其余强制 int。"""
    if value is None:
        return 0
    if isinstance(value, (list, tuple, set)):
        return len(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def write_status(report: dict, data_root: Path) -> Path:
    """把一轮核验摘要写成 ``<data_root>/data_verify_status.json``。

    ``report`` 是 :func:`~app.quant.data_verify.runner.run_verify` 的返回值
    （也可只给部分键）；缺省值：``last_run_at`` 取当前时间、``stage`` 为
    ``"all"``、计数为 0、``sources`` 为空列表。多余键一律丢弃，保证
    状态文件对外是固定 schema。
    """
    report = report or {}
    payload = {
        "last_run_at": str(report.get("last_run_at") or _now_iso()),
        "stage": str(report.get("stage") or "all"),
        "findings": _as_count(report.get("findings")),
        "repaired": _as_count(report.get("repaired")),
        "quarantined": _as_count(report.get("quarantined")),
        "sources": [str(source) for source in (report.get("sources") or [])],
    }
    path = Path(data_root) / STATUS_FILENAME
    _atomic_write_json(path, payload)
    logger.info("data_verify 状态已写入 %s（stage=%s findings=%d repaired=%d）",
                path, payload["stage"], payload["findings"], payload["repaired"])
    return path


def read_status(data_root: Path) -> dict[str, Any] | None:
    """读状态文件；缺失/损坏/非 JSON 对象一律返回 ``None``（= 从未跑过）。

    只返回 :data:`STATUS_KEYS` 里的键（缺失的给 ``None``），状态文件被手工
    加了别的字段也不会漏给面板。
    """
    path = Path(data_root) / STATUS_FILENAME
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(raw, dict):
        return None
    return {key: raw.get(key) for key in STATUS_KEYS}


def _quarantine_key(entry: dict) -> tuple[str, ...]:
    """去重键：缺失字段归一为空串（``None`` 与 ``""`` 同一条）。"""
    return tuple(
        "" if entry.get(field) is None else str(entry.get(field))
        for field in QUARANTINE_KEY_FIELDS
    )


def _read_quarantine_entries(path: Path) -> list[dict]:
    """读已有清单；缺失/损坏/不是列表一律当空清单（坏文件整体重写）。"""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    if not isinstance(raw, list):
        logger.warning("隔离清单不是列表，将被重写: %s", path)
        return []
    return [item for item in raw if isinstance(item, dict)]


def write_quarantine(entries: list[dict], data_root: Path) -> Path:
    """把 ``entries`` 合并进 ``<data_root>/data_verify_quarantine.json``。

    合并语义：先保留已有条目（顺序不变），再按给定顺序追加去重键
    （``table/day/symbol/reason``）尚未出现的新条目；**已有条目不覆盖**
    ——同一问题每轮重复出现时保留第一次的 detail。空 ``entries`` 也会
    写一次文件（内容不变），便于调用方统一走这条路径。
    """
    path = Path(data_root) / QUARANTINE_FILENAME
    merged = _read_quarantine_entries(path)
    seen = {_quarantine_key(item) for item in merged}
    added = 0
    for entry in entries or []:
        if not isinstance(entry, dict):
            logger.warning("跳过非 dict 隔离条目: %r", entry)
            continue
        key = _quarantine_key(entry)
        if key in seen:
            continue
        seen.add(key)
        merged.append(dict(entry))
        added += 1
    _atomic_write_json(path, merged)
    if added:
        logger.info("隔离清单 %s 新增 %d 条（共 %d 条）", path, added, len(merged))
    return path


__all__ = [
    "QUARANTINE_FILENAME",
    "QUARANTINE_KEY_FIELDS",
    "STATUS_FILENAME",
    "STATUS_KEYS",
    "read_status",
    "write_quarantine",
    "write_status",
]
