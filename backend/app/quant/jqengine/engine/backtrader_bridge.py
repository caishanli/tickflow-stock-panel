"""Backtrader 桥接：把聚宽兼容策略适配进 ``bt.Cerebro`` 回测。

设计：Backtrader 作为行情回放与定时调度引擎；持仓/资金核算由聚宽
``Portfolio`` 完成（``order_target_percent`` 等直接改写 ``context.portfolio``），
``bt`` broker 不参与下单。这样聚宽策略语义与原平台一致。
"""

import logging
from datetime import datetime

import backtrader as bt
import pandas as pd

from .jq import api as jq
from .jq.loader import StrategyBundle
from .metrics import compute_metrics, to_csv

logger = logging.getLogger(__name__)


def _normalize_df(df, frequency="daily"):
    """把任意数据源返回的 DataFrame 归一化为 datetime 索引 + OHLCV。"""
    if df is None or df.empty:
        return df
    df = df.copy()
    dt_col = None
    for c in ("datetime", "trade_time", "date", "time", "trade_date"):
        if c in df.columns:
            dt_col = c
            break
    if dt_col:
        df["datetime"] = pd.to_datetime(df[dt_col], format="mixed", errors="coerce")
    if "datetime" not in df.columns and not isinstance(df.index, pd.DatetimeIndex):
        df["datetime"] = pd.to_datetime(df.index, errors="coerce")
    if not isinstance(df.index, pd.DatetimeIndex):
        df = df.dropna(subset=["datetime"]).set_index("datetime")
    df = df.rename(columns={"vol": "volume", "Vol": "volume"})
    for col in ("open", "high", "low", "close", "volume"):
        if col not in df.columns:
            df[col] = 0.0
    df = df[["open", "high", "low", "close", "volume"]].astype(float)
    df = df.sort_index()
    df = df[~df.index.duplicated(keep="last")]
    return df


class _JqAdapter(bt.Strategy):
    params = dict(bundle=None, frequency="daily", progress=None, window_start=None)

    def __init__(self):
        self.bundle = self.p.bundle
        self.equity = []
        self.daily_equity = []   # [(date, value), ...] 每日收盘净值
        self._last_eq_day = None
        self._prev_eq_val = None
        self._last_day = None
        self._prev_day = None
        self._done_daily = set()
        self.errors = []
        self.progress = self.p.progress
        self._flushed_trades = 0
        self.window_start = self.p.window_start
        self._win_day = self.window_start.date() if self.window_start else None

    def start(self):
        try:
            self.bundle.init_fn(self.bundle.ctx)
        except Exception as e:
            logger.exception("策略 init() 异常")
            self.errors.append(f"init: {e}")

    def _dispatch_daily(self, ctx, dt, is_last_bar):
        """按当前时间分派 run_daily 定时任务（日线/分钟线通用）。"""
        cur = dt.strftime("%H:%M")
        # 把特殊时间点映射到可比较的调度时刻
        for i, (fn, t) in enumerate(self.bundle.daily):
            if t == "every_bar":
                continue
            key = (i, t)
            if key in self._done_daily:
                continue
            sched = t
            if t == "open":
                sched = "09:30"
            elif t == "close":
                sched = "15:00"
            elif t == "09:00":
                sched = "09:30"          # 盘前任务在首个 bar 执行
            elif t >= "15:01":
                sched = "15:00"          # 盘后任务在最后一根 bar 执行
            if cur >= sched or (is_last_bar and sched > cur):
                try:
                    fn(ctx)
                except Exception as e:
                    logger.exception("策略 daily 回调异常 dt=%s task=%s", dt, t)
                    self.errors.append(f"daily@{dt}:{t}: {e}")
                self._done_daily.add(key)

    def next(self):
        ctx = self.bundle.ctx
        self._bar = getattr(self, "_bar", 0) + 1
        if self._bar % 2000 == 0:
            print(f"[bridge] bar {self._bar} dt={self.datas[0].datetime.datetime(0)}", flush=True)
        dt = self.datas[0].datetime.datetime(0)
        if not isinstance(dt, datetime):
            dt = datetime.combine(dt, datetime.min.time())
        ctx.current_dt = dt
        day = dt.date()
        # 仅回测窗口内的日期才记录净值/成交；窗口前为指标预热（lookback）
        active = (self._win_day is None) or (day >= self._win_day)
        # 日期切换时，把上一交易日的末值写入每日净值序列（仅窗口内）
        if self._last_eq_day is not None and day != self._last_eq_day:
            if self._win_day is None or self._last_eq_day >= self._win_day:
                self.daily_equity.append((self._last_eq_day, self._prev_eq_val))
                if self.progress is not None:
                    self.progress.flush_equity(self._last_eq_day, self._prev_eq_val)
        self._prev_eq_val = ctx.portfolio.value
        self._last_eq_day = day
        is_min = self.p.frequency == "1min"
        is_last_bar = False
        if is_min:
            # 判断是否为当日最后一根分钟 bar
            try:
                nxt = self.datas[0].datetime.datetime(1)
                is_last_bar = (nxt.date() != day) if isinstance(nxt, datetime) else True
            except Exception:
                is_last_bar = True

        if day != self._last_day:
            self._last_day = day
            self._done_daily = set()
            jq.clear_current_data_cache()
            if self._prev_day is None:
                # 回测首个交易日：previous_date 应为「起始日前一个交易日」，
                # 与聚宽一致（否则会回退成当前日，导致 3 日窗口少一天）。
                prev = jq.get_trade_days(end_date=day, count=2)
                ctx.previous_date = (prev[-2].date() if len(prev) >= 2 else day)
            else:
                ctx.previous_date = self._prev_day
            self._prev_day = day

        # 维护实时分钟价快照（供 get_current_data / order 使用）
        prices = jq._state.setdefault("minute_prices", {})
        for d in self.datas:
            prices[d._name] = d.close[0]
            ctx.portfolio.update_price(d._name, d.close[0])

        if active:
            if is_min:
                # every_bar 任务（如分钟级止损）每个 bar 执行
                for fn, t in self.bundle.daily:
                    if t == "every_bar":
                        try:
                            fn(ctx)
                        except Exception as e:
                            logger.exception("策略 every_bar 回调异常 dt=%s", dt)
                            self.errors.append(f"every_bar@{dt}: {e}")
                # 分派定时任务
                self._dispatch_daily(ctx, dt, is_last_bar)
            else:
                for fn, _t in self.bundle.daily:
                    try:
                        fn(ctx)
                    except Exception as e:
                        logger.exception("策略 daily 回调异常 dt=%s", dt)
                        self.errors.append(f"daily@{dt}: {e}")
            self.equity.append(ctx.portfolio.value)
            if self.progress is not None:
                trades = jq._state.get("trades", [])
                while self._flushed_trades < len(trades):
                    t = trades[self._flushed_trades]
                    self.progress.flush_trade(
                        t.get("dt"), t.get("code"), t.get("amount"),
                        t.get("price"), t.get("fee"))
                    self._flushed_trades += 1

    def stop(self):
        if self._last_eq_day is not None and self._prev_eq_val is not None:
            if self._win_day is None or self._last_eq_day >= self._win_day:
                self.daily_equity.append((self._last_eq_day, self._prev_eq_val))
                if self.progress is not None:
                    self.progress.flush_equity(self._last_eq_day, self._prev_eq_val)


def run_backtest(bundle, securities, start, end, frequency, fee, slippage,
                 feeds=None, cash=1000000.0, progress=None):
    """执行回测，返回 ``(metrics, trades_csv_path, equity_curve)``。

    ``feeds`` 可传入 ``{code: DataFrame}`` 用于测试或预取数据；否则用
    策略绑定的 manager 取数并归一化。
    """
    # 重置组合与成交记录
    bundle.ctx.portfolio.cash = cash
    bundle.ctx.portfolio.start_cash = cash
    bundle.ctx.portfolio.positions = {}
    jq._state["trades"] = []
    jq._state["minute_prices"] = {}
    jq._state["minute_mode"] = (frequency == "1min")
    # 预热窗口：指标（动量/均线等）需要 start 之前的历史，但回测只从 start 执行
    lb_days = 30 if frequency == "1min" else 250
    lb_start = pd.Timestamp(start) - pd.Timedelta(days=lb_days)
    lb_start_str = lb_start.strftime("%Y-%m-%d")
    if frequency == "1min":
        manager = jq._state.get("manager")
        if manager is not None:
            cur_win = getattr(manager.minute_source, "window", None)
            new_win = (lb_start, pd.Timestamp(end))
            if cur_win != new_win:
                manager.set_minute_window(lb_start_str, end)

    cerebro = bt.Cerebro()
    cerebro.broker.setcash(cash)

    manager = jq._state.get("manager")
    n_sec = len(securities)
    for i, code in enumerate(securities):
        if feeds and code in feeds:
            df = feeds[code]
        else:
            if manager is None:
                raise RuntimeError("无 manager 且未提供 feeds，无法取数")
            if frequency == "1min":
                raw = manager.get_minute_feed(code, lb_start_str, end)
            else:
                raw = manager.fetch("get_daily", code, lb_start_str, end)
            df = _normalize_df(raw, frequency)
        if df is not None and not df.empty:
            # 数据源可能返回全量历史；裁剪到 [预热起点, end]，窗口前仅预热
            mask = (df.index >= lb_start) & (df.index <= pd.Timestamp(end))
            df = df[mask]
        if df is None or df.empty:
            logger.warning("回测跳过 %s：无数据", code)
            continue
        data = bt.feeds.PandasData(dataname=df)
        cerebro.adddata(data, name=code)
        if i % 20 == 0:
            print(f"[bridge] 已加载 {i}/{n_sec} 标的 feeds", flush=True)

    print(f"[bridge] 全部 {n_sec} 标的 feeds 就绪，启动 cerebro...", flush=True)
    cerebro.addstrategy(_JqAdapter, bundle=bundle, frequency=frequency,
                         progress=progress, window_start=pd.Timestamp(start))
    results = cerebro.run()
    print("[bridge] cerebro 运行结束", flush=True)
    strat = None
    if results:
        first = results[0]
        if isinstance(first, (list, tuple)):
            strat = first[0] if first else None
        else:
            strat = first
    trades = list(jq._state["trades"])
    equity = strat.equity if strat is not None and hasattr(strat, 'equity') and strat.equity else [cash]
    daily_equity = (strat.daily_equity if strat is not None
                    and hasattr(strat, 'daily_equity') and strat.daily_equity
                    else [(d, v) for d, v in enumerate(equity)])
    metrics = compute_metrics(equity, trades)
    if strat is not None and hasattr(strat, 'errors') and strat.errors:
        metrics["strategy_errors"] = strat.errors
    csv_path = to_csv(trades, None)
    return metrics, csv_path, equity, daily_equity
