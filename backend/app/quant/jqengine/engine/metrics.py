"""回测绩效指标计算 + 成交明细导出。

指标：总收益、年化、最大回撤、夏普比率、盈亏比、胜率、交易次数。
胜率/盈亏比用 FIFO 配对买卖单实现。
"""

import csv
import os
from collections import defaultdict, deque

from ..config import CONFIG


def _match_trades(trades):
    """FIFO 配对买卖单，返回 (wins, losses) 盈亏列表。"""
    queues = defaultdict(deque)
    wins, losses = [], []
    for t in trades:
        code = t.get("code")
        amt = t.get("amount", 0)
        price = t.get("price", 0.0)
        q = queues[code]
        if amt > 0:
            q.append([amt, price])
        elif amt < 0:
            remain = -amt
            while remain > 0 and q:
                lot_amt, lot_price = q[0]
                take = min(remain, lot_amt)
                pnl = (price - lot_price) * take
                (wins if pnl >= 0 else losses).append(pnl)
                lot_amt -= take
                remain -= take
                if lot_amt <= 0:
                    q.popleft()
                else:
                    q[0][0] = lot_amt
    return wins, losses


def compute_metrics(equity, trades):
    """由净值序列与成交记录计算绩效指标。"""
    eq = list(equity)
    if not eq:
        return {
            "total_return": 0.0, "annual_return": 0.0, "max_drawdown": 0.0,
            "sharpe": 0.0, "profit_loss_ratio": 0.0, "win_rate": 0.0,
            "trade_count": len(trades),
        }
    total = eq[-1] / eq[0] - 1 if eq[0] else 0.0
    n = len(eq)
    years = max(n / 252.0, 1e-9)
    annual = (eq[-1] / eq[0]) ** (1 / years) - 1 if eq[0] else 0.0

    peak = eq[0]
    mdd = 0.0
    for v in eq:
        peak = max(peak, v)
        mdd = min(mdd, v / peak - 1)

    rets = [eq[i] / eq[i - 1] - 1 for i in range(1, n)] if n > 1 else []
    sharpe = 0.0
    if len(rets) > 1:
        mean = sum(rets) / len(rets)
        var = sum((r - mean) ** 2 for r in rets) / len(rets)
        std = var ** 0.5
        sharpe = (mean / std) * (252 ** 0.5) if std > 0 else 0.0

    wins, losses = _match_trades(trades)
    total_closed = len(wins) + len(losses)
    win_rate = (len(wins) / total_closed) if total_closed else 0.0
    avg_win = (sum(wins) / len(wins)) if wins else 0.0
    avg_loss = (abs(sum(losses) / len(losses))) if losses else 0.0
    pl_ratio = (avg_win / avg_loss) if (avg_win and avg_loss) else 0.0

    return {
        "total_return": round(total, 4),
        "annual_return": round(annual, 4),
        "max_drawdown": round(mdd, 4),
        "sharpe": round(sharpe, 2),
        "profit_loss_ratio": round(pl_ratio, 2),
        "win_rate": round(win_rate, 2),
        "trade_count": len(trades),
    }


def to_csv(trades, path=None):
    """导出成交明细到 CSV，返回路径。"""
    os.makedirs(CONFIG["RUNTIME_DIR"], exist_ok=True)
    if path is None:
        path = os.path.join(CONFIG["RUNTIME_DIR"], "trades.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["dt", "code", "amount", "price"])
        w.writeheader()
        for t in trades:
            w.writerow({k: t.get(k, "") for k in ["dt", "code", "amount", "price"]})
    return path
