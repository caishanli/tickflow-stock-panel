# backend/scripts/run_backend_reloader.py
"""uvicorn --reload 的防挂死包装（dev.sh 专用，生产 Docker 不走此路径）。

根治「改代码触发 reload 后端口永久无响应」问题：

机制（2026-09-07 复盘，每次必现非竞态）：
- uvicorn ``BaseReload.restart()`` = ``process.terminate()`` + ``process.join()``
  （join **无超时**，supervisors/basereload.py:87-101）；
- reload child 收 SIGTERM 后走优雅停机（lifespan shutdown + 线程池 join），
  但 app 内存在无法及时退出的非 daemon 线程（polars 线程池 / APScheduler /
  扩展表轮询等）或 shutdown 阻塞点 → child 进程 8s+ 不退出（实测）；
- watcher ``join()`` 死等老 child → 新 child 永不 spawn → 监听 socket 虽在
  watcher 手里但无 worker accept → 端口挂死。

修法：patch ``BaseReload.restart/shutdown``，``join(timeout)`` 超时后对老
child ``SIGKILL``（进程级强杀必然成功）再继续 spawn 新 child。优雅停机正常
完成时行为与原版完全一致；卡死时最多延迟 ``BACKEND_RELOAD_KILL_AFTER`` 秒。

另注册 faulthandler SIGUSR1：卡死时 ``kill -USR1 <child_pid>`` 转储全线程栈
到 data/backend_fault_dump.txt，定位新的阻塞点。
"""
from __future__ import annotations

import contextlib
import faulthandler
import logging
import os
import signal
import sys
from pathlib import Path

# 与 dev.sh 同目录约定：backend/ 根加入 sys.path（脚本被直接执行时）
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("backend.reloader")

_KILL_AFTER = float(os.getenv("BACKEND_RELOAD_KILL_AFTER", "") or 8.0)


def _join_with_kill(proc) -> None:
    """join 带 SIGKILL 兜底：优雅停机超过 _KILL_AFTER 秒即强杀。"""
    proc.join(timeout=_KILL_AFTER)
    if proc.is_alive():
        logger.warning(
            "reload child pid=%s 优雅停机 %ss 未退出，SIGKILL 强杀（端口挂死兜底）",
            proc.pid, _KILL_AFTER)
        with contextlib.suppress(ProcessLookupError):
            os.kill(proc.pid, signal.SIGKILL)
        proc.join(timeout=5.0)


def _patch_reload_join_timeout() -> None:
    """给 BaseReload.restart/shutdown 的 process.join() 加超时 + SIGKILL 兜底。

    与原版逐行对齐（含 win32 分支），只替换 terminate 后的 join。若 uvicorn
    升级改变了方法结构，防御性 try/except 保证不阻塞启动（退化为原版行为）。
    """
    try:
        from uvicorn.supervisors.basereload import BaseReload, get_subprocess

        def restart(self) -> None:
            if sys.platform == "win32":  # pragma: py-win32
                self.is_restarting = True
                assert self.process.pid is not None
                os.kill(self.process.pid, signal.CTRL_C_EVENT)
                sys.stdout.write(" ")
                sys.stdout.flush()
            else:
                self.process.terminate()
                _join_with_kill(self.process)
            self.process = get_subprocess(
                config=self.config, target=self.target, sockets=self.sockets)
            self.process.start()

        def shutdown(self) -> None:
            if sys.platform == "win32":
                self.should_exit.set()
            else:
                self.process.terminate()
                _join_with_kill(self.process)
            for sock in self.sockets:
                sock.close()

        BaseReload.restart = restart
        BaseReload.shutdown = shutdown
    except Exception as e:
        logger.warning("reload join 超时 patch 失败（退化为 uvicorn 原版行为）: %s", e)


def _enable_faulthandler() -> None:
    try:
        from app.config import settings
        dump_path = settings.data_dir / "backend_fault_dump.txt"
        dump_path.parent.mkdir(parents=True, exist_ok=True)
        fh_file = open(dump_path, "w")  # noqa: SIM115  # 需跨信号存活的句柄
        faulthandler.enable(file=fh_file)
        faulthandler.register(signal.SIGUSR1, file=fh_file, all_threads=True)
    except Exception as e:
        logger.warning("faulthandler 注册失败: %s", e)


def main() -> None:
    _patch_reload_join_timeout()
    _enable_faulthandler()
    # 解析 dev.sh 透传的完整 CLI 参数（--env-file/--reload/--host/--port 等），
    # 复用 uvicorn 自己的 parser，避免参数定义漂移
    from uvicorn.main import main as uvicorn_cli

    sys.argv[0] = "uvicorn"
    uvicorn_cli()


if __name__ == "__main__":
    main()
