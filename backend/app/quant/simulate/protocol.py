"""模拟盘账户 live state 读写（落 quant.db 的 sim_state）。"""
from __future__ import annotations

from .. import db


def read_state(account_id: str) -> dict:
    return db.read_sim_state(account_id)


def save_state(account_id: str, state: dict) -> None:
    db.upsert_sim_state(
        account_id,
        cash=float(state.get("cash", 0.0)),
        positions_json=__import__("json").dumps(state.get("positions", {}), ensure_ascii=False),
        net_value=float(state.get("net_value", 0.0)),
        pnl=float(state.get("pnl", 0.0)),
        start_cash=float(state.get("start_cash", 0.0)),
        stop_loss_log_json=__import__("json").dumps(state.get("stop_loss_log", []), ensure_ascii=False),
        dt=state.get("dt"),
    )


def is_paused(account_id: str) -> bool:
    import os
    from ..config import CONFIG
    return os.path.exists(os.path.join(CONFIG.runtime_dir, f"{account_id}.pause"))
