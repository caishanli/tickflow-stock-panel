"""Contract v2: cooperative canonical partition resource mutex.

All enrolled daily/enriched repository and direct sinks participate regardless
of table label. The legacy .raw_partition_locks name and daily_partition_lock
API denote the shared resource namespace, NOT a nonraw exemption. No additional
raw business lock is introduced. Table validation remains a separate concern.
Stop old writers and exclude cleanup/normalize/migration; never unlink sidecars.
POSIX flock is loaded on use: all enrolled writes now require its support.
Static symlink topology only; no hardlink/bind-mount identity claim.
"""
from contextlib import contextmanager
import hashlib
from pathlib import Path
import time


@contextmanager
def daily_partition_lock(part: Path, timeout: float = 30.0):
    """Non-reentrant; one monotonic acquisition budget, kernel exit release.

    Sidecar lives outside date partitions AND kline_daily so date/table cleanup
    cannot remove its inode. Whole-data-root deletion remains uncooperative.
    Advisory only; atomic rename is not a power-loss durability guarantee.
    """
    import fcntl

    if timeout < 0:
        raise ValueError("negative lock timeout")
    part = Path(part).resolve()
    root = part.parent.parent.parent
    key = hashlib.sha256(str(part.relative_to(root)).encode()).hexdigest()
    sidecar = root / ".raw_partition_locks" / (key + ".lock")
    sidecar.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout
    with sidecar.open("a+b") as stream:
        while True:
            try:
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"RAW_PARTITION_LOCK_TIMEOUT: {part}") from None
                time.sleep(min(0.02, remaining))
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
