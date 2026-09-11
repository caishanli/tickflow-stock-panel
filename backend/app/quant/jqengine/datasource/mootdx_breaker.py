"""mootdx K 线接口熔断器（进程级，跨线程共享）。

背景：TDX K 线（bars）可能全局返回空/超时（如 2026-09-10 起全市场 K 线
返回空而名录接口正常）。无熔断时调用方逐只烧满整轮服务器轮换（16 台显式
+ bestip 兜底 × ~10s）：实时链每只卡 30s 墙钟、批量回源 0.2 只/s 蠕动、
日志风暴——且全部注定失败。

语义：连续 ``threshold`` 次 K 线调用失败（整轮轮换耗尽）即开路；开路期
``cooldown`` 秒内所有 K 线调用快速失败（不触网），调用方直走腾讯/新浪；
冷却到期后放行单个半开探测，成功则关闭、失败则续开。

env（每次调用时读取，测试可用 monkeypatch 覆盖）：
- ``MOOTDX_BREAKER_THRESHOLD``：连续失败开路阈值（默认 20）
- ``MOOTDX_BREAKER_COOLDOWN``：开路冷却秒数（默认 600）
- ``MOOTDX_BREAKER_DISABLED=1``：关闭熔断（全部放行，仅供排查）

锁纪律：模块唯一 ``_lock``（threading.Lock），各公开函数只取一次、
函数间不嵌套调用（防 08-31 式自死锁；见 AGENTS.md 并发纪律）。
"""
from __future__ import annotations

import logging
import os
import threading
import time

logger = logging.getLogger(__name__)

#: 开路期快速失败的错误标记：调用方据此降噪（逐只不再刷屏），
#: 不应作为成功处理。
BREAKER_OPEN_MSG = "mootdx 熔断开路中"

_lock = threading.Lock()
_fail_streak = 0
_opened_at: float | None = None
_probe_in_flight = False


def _cfg() -> tuple[int, float, bool]:
    try:
        threshold = int(os.getenv("MOOTDX_BREAKER_THRESHOLD", "") or 20)
    except ValueError:
        threshold = 20
    try:
        cooldown = float(os.getenv("MOOTDX_BREAKER_COOLDOWN", "") or 600)
    except ValueError:
        cooldown = 600.0
    disabled = (os.getenv("MOOTDX_BREAKER_DISABLED", "") or "").strip().lower() in (
        "1", "true", "yes",
    )
    return max(1, threshold), max(1.0, cooldown), disabled


def kline_allowed() -> bool:
    """K 线调用是否放行：关闭态放行；开路冷却期内拒绝；冷却到期后仅放行
    单个半开探测（其余调用继续快速失败，直到探测落定）。"""
    _, cooldown, disabled = _cfg()
    if disabled:
        return True
    global _probe_in_flight
    with _lock:
        if _opened_at is None:
            return True
        if time.monotonic() - _opened_at < cooldown:
            return False
        if _probe_in_flight:
            return False
        _probe_in_flight = True
        return True


def kline_record_ok() -> None:
    """一次 K 线调用成功：清零并关闭熔断（恢复只记一条日志）。"""
    _, _, disabled = _cfg()
    if disabled:
        return
    global _fail_streak, _opened_at, _probe_in_flight
    with _lock:
        was_open = _opened_at is not None
        _fail_streak = 0
        _opened_at = None
        _probe_in_flight = False
    if was_open:
        logger.warning("mootdx K线熔断恢复：半开探测成功，关闭熔断")


def kline_record_fail() -> None:
    """一次 K 线调用失败（整轮轮换耗尽）：连击达阈值即开路并记一条日志；
    开路期探测失败则续开（冷却重新计时）。"""
    threshold, _, disabled = _cfg()
    if disabled:
        return
    global _fail_streak, _opened_at, _probe_in_flight
    just_opened = False
    with _lock:
        _probe_in_flight = False
        if _opened_at is not None:
            _opened_at = time.monotonic()
            reopen = True
            streak = _fail_streak
        else:
            _fail_streak += 1
            streak = _fail_streak
            reopen = False
            if _fail_streak >= threshold:
                _opened_at = time.monotonic()
                just_opened = True
    if reopen:
        logger.warning("mootdx K线半开探测失败，续开熔断（连击%d）", streak)
    elif just_opened:
        logger.warning(
            "mootdx K线熔断开路：连续%d次失败，冷却%ss（期间K线调用快速失败，直走腾讯/新浪）",
            streak, os.getenv("MOOTDX_BREAKER_COOLDOWN", "") or 600,
        )


def breaker_state() -> dict:
    """当前熔断状态（供日志/排查/测试）：{"open": bool, "fail_streak": int}。"""
    with _lock:
        return {"open": _opened_at is not None, "fail_streak": _fail_streak}


def _reset_for_tests() -> None:
    """重置熔断状态（仅测试用）。"""
    global _fail_streak, _opened_at, _probe_in_flight
    with _lock:
        _fail_streak = 0
        _opened_at = None
        _probe_in_flight = False
