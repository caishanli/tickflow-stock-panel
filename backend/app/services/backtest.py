"""回测服务(§6.7)。

包 vectorbt — 全项目唯一一处出现 pandas。
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Literal

import numpy as np
import pandas as pd
import polars as pl

from app.config import settings
from app.tickflow.repository import KlineRepository

logger = logging.getLogger(__name__)

# 指标预热窗口 (日历日): 与 factor.FACTOR_WARMUP_DAYS / strategy.py warmup 对齐,
# 保证 MA60 类信号在正式区间开头就有足够历史 (60 交易日 ≈ 84 日历日, 120 留足余量)。
_INDICATOR_WARMUP_DAYS = 120

# vectorbt 是 optional extras(见 pyproject.toml).未装时只有 backtest 不可用,其他功能正常.
_vbt = None
_vbt_unavailable_reason: str | None = None


class VectorbtUnavailable(RuntimeError):
    """vectorbt 未安装 — 提示用户 `uv sync --extra backtest`."""


def _get_vbt():
    global _vbt, _vbt_unavailable_reason
    if _vbt is not None:
        return _vbt
    if _vbt_unavailable_reason is not None:
        raise VectorbtUnavailable(_vbt_unavailable_reason)
    try:
        import vectorbt as vbt
        _vbt = vbt
        return _vbt
    except ImportError as e:
        _vbt_unavailable_reason = (
            "vectorbt 未安装 — 它是回测的可选依赖.macOS Intel 用户先 `brew install cmake` "
            "然后 `uv sync --extra backtest`"
        )
        logger.warning("vectorbt unavailable: %s", e)
        raise VectorbtUnavailable(_vbt_unavailable_reason) from e


def is_available() -> bool:
    """供 API 层快速检测."""
    try:
        _get_vbt()
        return True
    except VectorbtUnavailable:
        return False


SignalKind = Literal[
    "macd_golden", "macd_dead",
    "ma_golden_5_20", "ma_dead_5_20",
    "ma_golden_20_60",
    "ma20_breakout", "ma20_breakdown",
    "n_day_high", "n_day_low",
    "boll_breakout_upper", "boll_breakdown_lower",
    "volume_surge",
    "rsi_oversold", "rsi_overbought",
    "stop_loss", "trailing_stop", "max_hold",
]


@dataclass
class BacktestConfig:
    symbols: list[str]
    start: date
    end: date
    # 买入信号(任一触发即买)
    entries: list[str] = field(default_factory=list)
    # 卖出信号(任一触发即卖)
    exits: list[str] = field(default_factory=list)
    # 其他参数
    stop_loss_pct: float | None = None       # 例 -0.05 = -5%
    max_hold_days: int | None = None
    fees_pct: float = 0.0002                 # 万二佣金
    slippage_bps: float = 5                  # 5 bps
    # 撮合
    matching: Literal["close_t", "open_t+1"] = "close_t"
    rsi_oversold_threshold: float = 30
    rsi_overbought_threshold: float = 70
    asset_type: str = "stock"


@dataclass
class BacktestResult:
    run_id: str
    config: dict
    stats: dict
    equity_curve: list[dict]      # [{date, value}]
    trades: list[dict]            # [{symbol, entry_date, exit_date, pnl_pct, ...}]
    per_symbol_stats: list[dict]  # 每只股票的统计


# enriched 表里的信号列名映射
_SIGNAL_COLS: dict[SignalKind, str] = {
    "macd_golden": "signal_macd_golden",
    "macd_dead": "signal_macd_dead",
    "ma_golden_5_20": "signal_ma_golden_5_20",
    "ma_dead_5_20": "signal_ma_dead_5_20",
    "ma_golden_20_60": "signal_ma_golden_20_60",
    "ma20_breakout": "signal_ma20_breakout",
    "ma20_breakdown": "signal_ma20_breakdown",
    "n_day_high": "signal_n_day_high",
    "n_day_low": "signal_n_day_low",
    "boll_breakout_upper": "signal_boll_breakout_upper",
    "boll_breakdown_lower": "signal_boll_breakdown_lower",
    "volume_surge": "signal_volume_surge",
}

# 风控类 SignalKind 不是面板信号列, 混进 entries/exits 时给出明确报错提示 (旧行为静默跳过)
_KIND_PARAM_HINTS: dict[str, str] = {
    "stop_loss": "请改用 stop_loss_pct 参数",
    "max_hold": "请改用 max_hold_days 参数",
    "trailing_stop": "trailing_stop 回测暂未实现",
}


class BacktestService:
    def __init__(self, repo: KlineRepository) -> None:
        self.repo = repo

    def _load_panel(
        self,
        symbols: list[str],
        start: date,
        end: date,
        asset_type: str = "stock",
    ) -> pd.DataFrame:
        """加载 [date × symbol] 价格面板 — Polars scan_parquet + 即时计算指标。

        **全项目唯一从 Polars 转 pandas 的边界**(§7.4 / ADR-19)。
        asset_type='etf' 时读 ETF enriched。

        加载窗口向前扩 _INDICATOR_WARMUP_DAYS 个日历日做指标预热, compute_all 之后
        再过滤回 [start,end] — 否则 MA60 类指标/信号在区间开头约 3 个月静默缺失
        (且对过滤后数据重算指标也不等于全历史口径)。
        """
        warmup_start = start - timedelta(days=_INDICATOR_WARMUP_DAYS)
        try:
            from app.tickflow.repository import enriched_dirname
            table = enriched_dirname(asset_type)
            sym_placeholders = ", ".join(["?"] * len(symbols))
            df = self.repo.db.execute(
                f"SELECT * FROM {table} "
                f"WHERE symbol IN ({sym_placeholders}) AND date >= ? AND date <= ? "
                f"ORDER BY date, symbol",
                [*symbols, warmup_start, end],
            ).pl()
        except Exception as e:  # noqa: BLE001
            logger.warning("backtest load failed: %s", e)
            return pd.DataFrame()

        if df.is_empty():
            return pd.DataFrame()

        # 即时计算指标 + 信号 (在含预热窗口的数据上计算)
        from app.indicators.pipeline import compute_all
        df = compute_all(df)

        # 预热段只参与指标计算, 裁回正式区间
        df = df.filter((pl.col("date") >= start) & (pl.col("date") <= end))
        if df.is_empty():
            return pd.DataFrame()

        # 选择需要的列
        needed_cols = [
            "date", "symbol", "open", "high", "low", "close", "volume",
            "rsi_14", "signal_macd_golden", "signal_macd_dead",
            "signal_ma_golden_5_20", "signal_ma_dead_5_20",
            "signal_ma_golden_20_60",
            "signal_ma20_breakout", "signal_ma20_breakdown",
            "signal_n_day_high", "signal_n_day_low",
            "signal_boll_breakout_upper", "signal_boll_breakdown_lower",
            "signal_volume_surge",
        ]
        existing = [c for c in needed_cols if c in df.columns]
        df = df.select(existing)

        # to_pandas 边界
        return df.to_pandas(use_pyarrow_extension_array=False)

    def _build_signal_matrix(
        self,
        panel: pd.DataFrame,
        kinds: list[str],
        config: BacktestConfig,
    ) -> pd.DataFrame:
        """从面板构造 [date × symbol] 的布尔信号矩阵。"""
        if not kinds or panel.empty:
            return pd.DataFrame()
        # 未实现/非信号列的 kind 直接报错, 不再静默跳过
        self._validate_signal_kinds(kinds)

        # pivot 成 [date × symbol] 形式
        result = None
        for kind in kinds:
            mat = None
            if kind in _SIGNAL_COLS:
                col = _SIGNAL_COLS[kind]
                mat = panel.pivot(index="date", columns="symbol", values=col).fillna(False).astype(bool)
            elif kind == "rsi_oversold":
                mat = (panel.pivot(index="date", columns="symbol", values="rsi_14")
                       < config.rsi_oversold_threshold)
            elif kind == "rsi_overbought":
                mat = (panel.pivot(index="date", columns="symbol", values="rsi_14")
                       > config.rsi_overbought_threshold)

            if mat is not None:
                result = mat if result is None else (result | mat)
        return result if result is not None else pd.DataFrame()

    @classmethod
    def _validate_signal_kinds(cls, kinds: list[str]) -> None:
        """entries/exits 中出现未实现/非信号列的 kind 时抛 ValueError 说明。

        stop_loss / max_hold 有对应参数路径, trailing_stop 暂未实现;
        旧行为是静默跳过, 用户以为生效了风控实际没有。"""
        for kind in kinds:
            if kind in _SIGNAL_COLS or kind in ("rsi_oversold", "rsi_overbought"):
                continue
            hint = _KIND_PARAM_HINTS.get(kind)
            if hint is not None:
                raise ValueError(f"信号 '{kind}' 不支持作为买卖信号: {hint}")
            raise ValueError(f"未知信号: '{kind}'")

    # vectorbt OSS 没有按笔计时止损 (td_stop 是 vectorbtpro 功能), 用迭代退出矩阵逼近:
    # 先跑一遍拿实际成交的入场点, 只对实际建仓的入场在 entry+max_hold_days 处补强制退出点。
    # (旧实现对所有信号行补退出点, 含持仓期被忽略的重复信号 — 同 bar entry+exit 会被
    #  vectorbt 双双丢弃, 设了 max_hold_days 后零成交; 且 iloc 链式赋值在 CoW 下静默失效。)
    # 强制退出 bar 上的入场信号要一并抹掉 (持仓期 entry 本就无效, 不抹会把强制退出也吞掉)。
    # 强制退出可能解锁新的入场, 迭代到退出矩阵稳定为止 (防御性上限 _MAX_HOLD_ITERATIONS)。
    _MAX_HOLD_ITERATIONS = 8

    def _run_with_max_hold(self, vbt, pf_kwargs: dict, max_hold_days: int):
        """带 max_hold_days 时间退出的 from_signals, 返回最终 Portfolio。"""
        entries: pd.DataFrame = pf_kwargs["entries"]
        base_exits: pd.DataFrame = pf_kwargs["exits"]
        n_bars = len(entries.index)
        forced = np.zeros((n_bars, len(entries.columns)), dtype=bool)
        for _ in range(self._MAX_HOLD_ITERATIONS):
            forced_df = pd.DataFrame(forced, index=entries.index, columns=entries.columns)
            pf_kwargs["entries"] = entries & ~forced_df
            pf_kwargs["exits"] = base_exits | forced_df
            pf = vbt.Portfolio.from_signals(**pf_kwargs)
            new_forced = np.zeros_like(forced)
            for col_i, bar_i in self._actual_entry_positions(pf, entries):
                end_i = min(bar_i + max_hold_days, n_bars - 1)
                if end_i > bar_i:
                    new_forced[end_i, col_i] = True
            if np.array_equal(new_forced, forced):
                return pf  # 退出矩阵稳定, 当前 pf 即最终结果
            forced = new_forced
        # 未收敛兜底 (理论上单调扩张必收敛, 这里只是防御迭代上限)
        logger.warning("max_hold_days 退出矩阵 %d 轮未收敛, 使用最后一轮结果", self._MAX_HOLD_ITERATIONS)
        forced_df = pd.DataFrame(forced, index=entries.index, columns=entries.columns)
        pf_kwargs["entries"] = entries & ~forced_df
        pf_kwargs["exits"] = base_exits | forced_df
        return vbt.Portfolio.from_signals(**pf_kwargs)

    @staticmethod
    def _actual_entry_positions(pf, entries: pd.DataFrame) -> list[tuple[int, int]]:
        """从 Portfolio 的成交订单提取实际建仓位置: [(col_idx, bar_idx)]。"""
        records = pf.orders.records_readable
        if records.empty:
            return []
        buys = records[records["Side"] == "Buy"]
        col_pos = {c: i for i, c in enumerate(entries.columns)}
        bar_pos = {t: i for i, t in enumerate(entries.index)}
        positions: list[tuple[int, int]] = []
        for col, ts in zip(buys["Column"], buys["Timestamp"]):
            c = col_pos.get(col)
            i = bar_pos.get(ts)
            if c is not None and i is not None:
                positions.append((c, i))
        return positions

    def run(self, config: BacktestConfig) -> BacktestResult:
        vbt = _get_vbt()
        run_id = uuid.uuid4().hex[:10]

        # 快速校验: 未实现/非信号列的 kind 直接报错 (旧行为静默跳过)
        self._validate_signal_kinds(config.entries + config.exits)

        panel = self._load_panel(config.symbols, config.start, config.end, config.asset_type)
        if panel.empty:
            return BacktestResult(
                run_id=run_id,
                config=_config_to_dict(config),
                stats={"error": "no data"},
                equity_curve=[],
                trades=[],
                per_symbol_stats=[],
            )

        # 价格面板
        close = panel.pivot(index="date", columns="symbol", values="close")

        # 信号矩阵
        entries = self._build_signal_matrix(panel, config.entries, config)
        exits = self._build_signal_matrix(panel, config.exits, config)

        # 对齐 index/columns
        if not entries.empty:
            entries = entries.reindex_like(close).fillna(False).astype(bool)
        else:
            entries = pd.DataFrame(False, index=close.index, columns=close.columns)
        if not exits.empty:
            exits = exits.reindex_like(close).fillna(False).astype(bool)
        else:
            exits = pd.DataFrame(False, index=close.index, columns=close.columns)

        if not entries.any().any():
            return BacktestResult(
                run_id=run_id,
                config=_config_to_dict(config),
                stats={"error": "no buy signals"},
                equity_curve=[],
                trades=[],
                per_symbol_stats=[],
            )

        # T+1 适配:vectorbt 默认信号当根 K 撮合
        # close_t 撮合:维持默认
        # open_t+1 撮合:shift 信号 1 根 + 用 open 作为价
        if config.matching == "open_t+1":
            entries = entries.shift(1).fillna(False).astype(bool)
            exits = exits.shift(1).fillna(False).astype(bool)
            price = panel.pivot(index="date", columns="symbol", values="open")
        else:
            price = close

        # 跑回测
        try:
            pf_kwargs = dict(
                close=close,
                entries=entries,
                exits=exits,
                price=price,
                fees=config.fees_pct,
                slippage=config.slippage_bps / 10000.0,
                freq="1D",
            )
            if config.stop_loss_pct is not None:
                pf_kwargs["sl_stop"] = abs(config.stop_loss_pct)
            if config.max_hold_days:
                pf = self._run_with_max_hold(vbt, pf_kwargs, config.max_hold_days)
            else:
                pf = vbt.Portfolio.from_signals(**pf_kwargs)
        except Exception as e:  # noqa: BLE001
            logger.exception("vectorbt backtest failed")
            return BacktestResult(
                run_id=run_id,
                config=_config_to_dict(config),
                stats={"error": str(e)},
                equity_curve=[],
                trades=[],
                per_symbol_stats=[],
            )

        # 提取结果
        try:
            stats_series = pf.stats(silence_warnings=True)
            if isinstance(stats_series, pd.DataFrame):
                # 多列时取 agg
                stats_dict = stats_series.mean(numeric_only=True).to_dict()
            else:
                stats_dict = stats_series.to_dict()
        except Exception:  # noqa: BLE001
            stats_dict = {}

        # 净值曲线(组合平均)
        equity = pf.value().mean(axis=1) if isinstance(pf.value(), pd.DataFrame) else pf.value()
        equity_curve = [
            {"date": str(idx.date() if hasattr(idx, "date") else idx), "value": float(v)}
            for idx, v in equity.items() if pd.notna(v)
        ]

        # 交易记录
        try:
            trades_df = pf.trades.records_readable
            trades = trades_df.to_dict(orient="records") if not trades_df.empty else []
            # 字段名美化
            trades = [
                {
                    "symbol": t.get("Column", t.get("Symbol", "")),
                    "entry_date": str(t.get("Entry Timestamp", t.get("Entry Date", ""))),
                    "exit_date": str(t.get("Exit Timestamp", t.get("Exit Date", ""))),
                    # NaN/inf 清洗为 None: 期末未平仓交易 Return/均价可能是 NaN,
                    # 否则 asdict 后会产出非法 JSON token
                    "entry_price": _finite_or_none(t.get("Avg Entry Price", t.get("Avg. Entry Price", 0))),
                    "exit_price": _finite_or_none(t.get("Avg Exit Price", t.get("Avg. Exit Price", 0))),
                    "pnl_pct": _finite_or_none(t.get("Return", t.get("PnL %", 0))),
                    "duration": str(t.get("Duration", "")),
                }
                for t in trades
            ]
        except Exception:  # noqa: BLE001
            trades = []

        # 每标的统计
        per_symbol = []
        try:
            total_ret = pf.total_return()
            if isinstance(total_ret, pd.Series):
                for sym, ret in total_ret.items():
                    if pd.notna(ret):
                        per_symbol.append({"symbol": sym, "total_return": float(ret)})
        except Exception:  # noqa: BLE001
            pass

        result = BacktestResult(
            run_id=run_id,
            config=_config_to_dict(config),
            stats={k: _json_safe(v) for k, v in stats_dict.items()},
            equity_curve=equity_curve,
            trades=trades,
            per_symbol_stats=per_symbol,
        )

        # 落盘
        self._persist(result)
        return result

    def _persist(self, result: BacktestResult) -> None:
        # 落盘只是快照: 失败记日志, 不让已算完的回测 500
        try:
            out_dir = settings.data_dir / "backtest_results"
            out_dir.mkdir(parents=True, exist_ok=True)
            # 用 polars 写一份汇总 (stats 走 json.dumps 保证可 JSON 序列化)
            summary = pl.DataFrame({
                "run_id": [result.run_id],
                "stats_json": [json.dumps(result.stats, ensure_ascii=False, default=str)],
                "n_trades": [len(result.trades)],
            })
            summary.write_parquet(out_dir / f"run_id={result.run_id}.parquet")
        except Exception:  # noqa: BLE001
            logger.exception("backtest persist failed (run_id=%s)", result.run_id)

    def get_result(self, run_id: str) -> BacktestResult | None:
        # Phase 1:只保留近似落盘,完整结果保存在内存的近期 cache 中
        # 简化:重新 run 比缓存复杂结果代价小,暂不实现 get_result
        return None


def _config_to_dict(c: BacktestConfig) -> dict:
    return {
        "symbols": c.symbols,
        "start": str(c.start),
        "end": str(c.end),
        "entries": c.entries,
        "exits": c.exits,
        "stop_loss_pct": c.stop_loss_pct,
        "max_hold_days": c.max_hold_days,
        "fees_pct": c.fees_pct,
        "slippage_bps": c.slippage_bps,
        "matching": c.matching,
    }


def _json_safe(v):
    # NaN/inf 统一清洗为 None, 避免序列化出非法 JSON token
    if isinstance(v, float) and not np.isfinite(v):
        return None
    if isinstance(v, (int, float, str, bool)) or v is None:
        return v
    if isinstance(v, (np.floating, np.integer)):
        return float(v) if np.isfinite(float(v)) else None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def _finite_or_none(v) -> float | None:
    """转 float; NaN/inf/不可转 → None (避免 asdict 后产出非法 JSON token)。"""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if np.isfinite(f) else None
