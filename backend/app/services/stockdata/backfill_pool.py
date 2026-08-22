"""回源并发池：N worker 线程、线程独立 MootdxSource、主线程批量 flush。

与实时路径 NetworkPuller 物理隔离（独立连接/独立限速策略）；场景天然错开：
回源任务盘后 ~20 分钟内完成，盘中只剩罕见的历史缺口小任务。
"""
from __future__ import annotations

import datetime as _dt
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor

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
        """
        symbols = list(symbols)
        workers = self.effective_workers(len(symbols))
        results: list = []
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
            futures = [ex.submit(_one, s) for s in symbols]
            for fut in futures:
                sym, out, err = fut.result()
                if err is not None:
                    failed[sym] = str(err)[:120]
                    continue
                if out is not None:
                    if keep_frames:
                        results.append(out)
                    ok_count += 1
                    # 仅在确有回调时攒批：否则 keep_frames=False 下 batch
                    # 会全程驻留帧（潜在 OOM 脚枪，评审复审指出）
                    if on_batch_done is not None:
                        batch.append(out)
                if on_batch_done is not None and len(batch) >= batch_size:
                    on_batch_done(batch)
                    batch = []
        if on_batch_done is not None and batch:
            on_batch_done(batch)
        logger.info("backfill pool done: ok=%d failed=%d workers=%d",
                    ok_count, len(failed), workers)
        return {"ok": results, "ok_count": ok_count, "failed": failed}
