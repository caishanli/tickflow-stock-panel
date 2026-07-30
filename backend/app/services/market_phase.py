"""市场阶段判断 — 共用函数。

从 QuoteService 提取, 供 MinuteKService 等模块复用。
"""
from __future__ import annotations

from datetime import time as dt_time

from app.market_time import cn_now


def market_phase() -> str:
    """A股行情轮询阶段 (北京时间)。

    返回值:
      - preopen:      9:15-9:30 (集合竞价)
      - morning:      9:30-11:30 (上午连续竞价)
      - morning_final: 11:30-12:55 (午休缓冲, 定版)
      - pre_afternoon: 12:55-13:00 (午后预热)
      - afternoon:    13:00-15:00 (下午连续竞价)
      - close_final:  15:00+ (收盘缓冲, 定版)
      - closed:       周末/其他时段
    """
    now = cn_now()
    if now.weekday() >= 5:
        return "closed"
    t = now.time()
    if dt_time(9, 15) <= t < dt_time(9, 30):
        return "preopen"
    if dt_time(9, 30) <= t < dt_time(11, 30):
        return "morning"
    if dt_time(11, 30) <= t < dt_time(12, 55):
        return "morning_final"
    if dt_time(12, 55) <= t < dt_time(13, 0):
        return "pre_afternoon"
    if dt_time(13, 0) <= t < dt_time(15, 0):
        return "afternoon"
    if t >= dt_time(15, 0):
        return "close_final"
    return "closed"


def is_continuous_trading() -> bool:
    """A股连续竞价时段 (北京时间): 9:30-11:30 / 13:00-15:00, 仅工作日。"""
    now = cn_now()
    t = now.time()
    morning = dt_time(9, 30) <= t <= dt_time(11, 30)
    afternoon = dt_time(13, 0) <= t <= dt_time(15, 0)
    return now.weekday() < 5 and (morning or afternoon)


def is_trading_hours() -> bool:
    """是否处于行情轮询窗口: 包含盘前预热和未完成的午休/收盘定版。"""
    phase = market_phase()
    return phase in {"preopen", "morning", "pre_afternoon", "afternoon"}
