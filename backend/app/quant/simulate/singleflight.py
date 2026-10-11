"""模拟盘分账户单实例文件锁（跨进程互斥）。

背景：同一账户曾出现 6 个 sb_gaokai_sim / 3 个 sb_gaokai_perf 进程并发
（重复下单/重复快照）。进程内 `_spawn_lock` 与 DB-pid 检查覆盖不到手动
拉起与跨进程竞态，此处加一层 per-account 文件锁兜底。

用法：`run_loop` 入口 `acquire(aid)`，全程持有，退出即 `release`。
抢不到抛 `SimLockError`，调用方直接 warn 落日志后返回（不改 status、
不抛错——daemon 见记录 pid 存活不会重拉）。

实现风格同 `app/services/mining_process_lock.py`：fcntl `LOCK_EX|LOCK_NB`
+ msvcrt `LK_NBLCK` 双分支。
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import BinaryIO

from ..config import CONFIG


class SimLockError(RuntimeError):
    """同账户已有实例持有锁（后到重复拉起时抛出）。"""


def _lock_path(account_id: str) -> Path:
    return Path(CONFIG.runtime_dir) / f"{account_id}.lock"


def acquire(account_id: str) -> BinaryIO:
    """非阻塞独占 `<runtime_dir>/<aid>.lock`；被占时抛 `SimLockError`。"""
    path = _lock_path(account_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    stream = path.open("a+b")
    try:
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b"0")
            stream.flush()
        os.set_inheritable(stream.fileno(), False)
        _try_lock_file(stream)
    except BaseException:
        stream.close()
        raise
    return stream


def release(handle: BinaryIO | None) -> None:
    """解锁并关闭句柄（`None` 直接返回，供 `finally` 使用）。"""
    if handle is None:
        return
    try:
        _unlock_file(handle)
    finally:
        handle.close()


def _try_lock_file(stream: BinaryIO) -> None:
    if os.name == "nt":
        import msvcrt

        stream.seek(0)
        try:
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            raise SimLockError(
                "same sim account instance is already running"
            ) from exc
        return

    import fcntl

    try:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        raise SimLockError(
            "same sim account instance is already running"
        ) from exc


def _unlock_file(stream: BinaryIO) -> None:
    if os.name == "nt":
        import msvcrt

        stream.seek(0)
        msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
        return

    import fcntl

    fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
