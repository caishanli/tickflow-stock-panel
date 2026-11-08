"""核验产物的保留策略：备份与 receipt 按 run 轮次滚动清理。

问题：``data_verify_backup/<run_id>/`` 每轮把「即将被改写的分区」整份复制一份
（2026-11-08 全历史修 22194 个 symbol-day → 520MB），``data_verify_receipts/<run_id>.json``
每轮也会新增。定时任务每天跑一次，不清理就是每天几十 MB 的净增长。

策略：
- **保留最近 N 轮**（默认 10，``DATA_VERIFY_BACKUP_KEEP`` 可调），按 run_id 排序
  （``YYYYMMDDTHHMMSSffffff`` 定长可字典序比较）删除更早的轮次；
- **只删认得的 run 目录**：名字必须匹配 ``^\\d{8}T\\d{12}$``，其它目录（手工放的、
  历史的、别的工具的）一律不碰——这是「清理」而不是「清空」；
- 清理失败不影响核验结果，只告警（备份是保险，清理是卫生）。

注意 ``data_verify_backup`` 本身不进 gdrive 备份白名单（它是备份的备份），
而 ``data_verify_receipts`` 进——后者是审计证据，保留 N 轮同样适用。
"""

from __future__ import annotations

import logging
import os
import re
import shutil
from pathlib import Path
from typing import Final

logger = logging.getLogger(__name__)

#: 备份目录名（与分区表同级，不被任何 ``date=*`` glob 命中）。
BACKUP_DIRNAME: Final = "data_verify_backup"

#: receipt 目录名（与 ``report.RECEIPTS_DIRNAME`` 同值，避免循环导入重复声明）。
RECEIPTS_DIRNAME: Final = "data_verify_receipts"

#: 默认保留轮数。
DEFAULT_KEEP: Final = 10

#: run_id 形态：``20261108T000934210225``（8 位日期 + T + 12 位时间）。
_RUN_ID_RE: Final = re.compile(r"^\d{8}T\d{12}$")


def _keep_count() -> int:
    """保留轮数：env ``DATA_VERIFY_BACKUP_KEEP``，非法/非正回退默认值。"""
    raw = (os.getenv("DATA_VERIFY_BACKUP_KEEP", "") or "").strip()
    if not raw:
        return DEFAULT_KEEP
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_KEEP
    return value if value > 0 else DEFAULT_KEEP


def _dir_size(path: Path) -> int:
    """目录字节数（清理日志用；失败按 0 计，不阻塞）。"""
    total = 0
    try:
        for item in path.rglob("*"):
            try:
                if item.is_file():
                    total += item.stat().st_size
            except OSError:
                continue
    except OSError:
        return 0
    return total


def prune_old_runs(
    data_root: Path,
    *,
    keep: int | None = None,
    backup_dirname: str = BACKUP_DIRNAME,
    receipts_dirname: str = RECEIPTS_DIRNAME,
) -> dict:
    """只保留最近 ``keep`` 轮备份/receipt，删除更早的轮次。

    返回 ``{"kept": int, "removed": int, "freed_bytes": int}``；
    无目录、无旧轮、清理失败都返回可读摘要而不抛错。
    """
    root = Path(data_root)
    limit = keep if keep is not None else _keep_count()
    limit = max(1, limit)

    removed = 0
    freed = 0
    kept = 0

    backup_dir = root / backup_dirname
    if backup_dir.is_dir():
        runs = sorted(
            (p for p in backup_dir.iterdir() if p.is_dir() and _RUN_ID_RE.match(p.name)),
            key=lambda p: p.name,
            reverse=True,
        )
        kept = min(len(runs), limit)
        for stale in runs[limit:]:
            size = _dir_size(stale)
            try:
                shutil.rmtree(stale)
            except OSError as exc:
                logger.warning("data_verify 清理旧备份失败 %s: %s", stale, exc)
                continue
            removed += 1
            freed += size

    receipts_dir = root / receipts_dirname
    if receipts_dir.is_dir():
        files = sorted(
            (p for p in receipts_dir.iterdir()
             if p.is_file() and p.suffix == ".json" and _RUN_ID_RE.match(p.stem)),
            key=lambda p: p.stem,
            reverse=True,
        )
        for stale in files[limit:]:
            try:
                size = stale.stat().st_size
                stale.unlink()
            except OSError as exc:
                logger.warning("data_verify 清理旧 receipt 失败 %s: %s", stale, exc)
                continue
            removed += 1
            freed += size

    if removed:
        logger.info("data_verify 清理 %d 轮旧产物，释放 %.1f MB（保留最近 %d 轮）",
                    removed, freed / 1e6, limit)
    return {"kept": kept, "removed": removed, "freed_bytes": freed}


__all__ = [
    "BACKUP_DIRNAME",
    "DEFAULT_KEEP",
    "RECEIPTS_DIRNAME",
    "prune_old_runs",
]
