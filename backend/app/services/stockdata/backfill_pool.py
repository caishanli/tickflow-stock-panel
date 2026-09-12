"""回源并发池：N worker 线程、线程独立 MootdxSource、主线程批量 flush。

与实时路径 NetworkPuller 物理隔离（独立连接/独立限速策略）；场景天然错开：
回源任务盘后 ~20 分钟内完成，盘中只剩罕见的历史缺口小任务。
"""
from __future__ import annotations

import datetime as _dt
import logging
import os
import threading
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

logger = logging.getLogger("app.services.stockdata.backfill_pool")

BACKFILL_WORKERS_DEFAULT = 6
_INTRADAY_HALVE_THRESHOLD = 500


def _is_market_open(now: _dt.datetime | None = None) -> bool:
    """A 股交易时段判定（与 mootdx_service._is_market_open 口径一致；本地副本避免循环导入）。"""
    now = now or _dt.datetime.now()
    t = now.time()
    return (now.weekday() < 5
            and (_dt.time(9, 30) <= t <= _dt.time(11, 30)
                 or _dt.time(13, 0) <= t <= _dt.time(15, 0)))


class BackfillPool:
    def __init__(self, workers: int | None = None, source_factory=None):
        if workers is None:
            try:
                workers = int(os.getenv("BACKFILL_WORKERS", "") or BACKFILL_WORKERS_DEFAULT)
            except ValueError:
                workers = BACKFILL_WORKERS_DEFAULT
        self._configured_workers = max(1, workers)
        self._factory = source_factory
        self._local = threading.local()

    def effective_workers(self, task_size: int) -> int:
        """交易时段的大任务减半（轻量保险；正常盘后任务不受影响）。"""
        w = self._configured_workers
        if _is_market_open() and task_size > _INTRADAY_HALVE_THRESHOLD:
            w = max(1, w // 2)
        return w

    def _source(self):
        src = getattr(self._local, "src", None)
        if src is None:
            if self._factory is not None:
                src = self._factory()
            else:
                from app.quant.jqengine.datasource.mootdx_src import MootdxSource
                src = MootdxSource()
            self._local.src = src
        return src

    def _reset_source(self):
        self._local.src = None

    def map(self, fn, symbols, batch_size=100, on_batch_done=None,
            keep_frames: bool = True) -> dict:
        """逐 symbol 执行 fn(src, symbol)；批满主线程回调 on_batch_done。

        失败语义：单只异常记 failed 不阻断；异常时重建该 worker 的 source
        （坏 socket 不残留）。返回 {"ok":[...], "ok_count":int, "failed":{}}。

        ``keep_frames=False``：池不驻留结果帧（分钟级大帧 × 全市场会推高
        RSS 至 OOM——旧串行实现每批 flush 即弃，本参数保持该峰值形态），
        "ok" 恒为空列表，成功数看 "ok_count"；调用方经 on_batch_done 消费。
        最多保留 2×workers 个在途任务；回调按完成顺序消费，返回 ok 保持输入顺序。
        """
        symbols = list(symbols)
        # mootdx 真源路径（默认 None 或 MootdxSource；测试假源不拦截）：
        # 熔断开路期整批跳过（不触网），缺口由下轮定时任务补回——避免
        # 全市场 5000+ 只逐只空转 + 日志风暴。
        from app.quant.jqengine.datasource.mootdx_src import MootdxSource
        if self._factory is None or self._factory is MootdxSource:
            from app.quant.jqengine.datasource.mootdx_breaker import (
                BREAKER_OPEN_MSG,
                kline_allowed,
            )
            if not kline_allowed():
                logger.warning("backfill pool 跳过：%s（%d 只，缺口下轮补回）",
                               BREAKER_OPEN_MSG, len(symbols))
                return {"ok": [], "ok_count": 0,
                        "failed": {s: BREAKER_OPEN_MSG for s in symbols}}
        workers = self.effective_workers(len(symbols))
        results: dict = {}
        ok_count = 0
        failed: dict[str, str] = {}
        batch: list = []

        def _one(sym):
            try:
                out = fn(self._source(), sym)
                err = None
            except Exception as e:  # noqa: BLE001
                out, err = None, e
                self._reset_source()
            return sym, out, err

        with ThreadPoolExecutor(max_workers=workers,
                                thread_name_prefix="backfill") as ex:
            # Future 也持有结果帧：全量 submit + 保存 futures 会使已落盘数据
            # 仍驻留至整批结束。限制待完成窗口，并在消费后释放 Future/帧引用。
            remaining = iter(enumerate(symbols))
            pending = {}
            for index, sym in remaining:
                pending[ex.submit(_one, sym)] = index
                if len(pending) >= workers * 2:
                    break
            while pending:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                while done:
                    fut = done.pop()
                    index = pending.pop(fut)
                    sym, out, err = fut.result()
                    if err is not None:
                        failed[sym] = str(err)[:120]
                    elif out is not None:
                        if keep_frames:
                            results[index] = out
                        ok_count += 1
                        if on_batch_done is not None:
                            batch.append(out)
                    if on_batch_done is not None and len(batch) >= batch_size:
                        on_batch_done(batch)
                        batch = []
                    del fut, out, err
                    item = next(remaining, None)
                    if item is not None:
                        index, sym = item
                        pending[ex.submit(_one, sym)] = index
        if on_batch_done is not None and batch:
            on_batch_done(batch)
        logger.info("backfill pool done: ok=%d failed=%d workers=%d",
                    ok_count, len(failed), workers)
        return {"ok": [results[i] for i in sorted(results)],
                "ok_count": ok_count, "failed": failed}
