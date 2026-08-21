"""量化模块配置（读 .env）。

所有数据路径统一指向项目根 ``data/`` 目录（与 tickflow 主数据同根），
不依赖运行时 CWD。环境变量 ``QUANT_*`` 可覆盖。
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

# config.py 位于 backend/app/quant/config.py，向上 3 级到 backend/，再上 1 级到项目根
_PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
_DATA_DIR = os.path.join(_PROJECT_ROOT, "data")


def _csv_list(value: str | None, default: list[str]) -> list[str]:
    if not value:
        return list(default)
    return [p.strip() for p in value.split(",") if p.strip()]


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def _data_path(sub: str) -> str:
    return os.path.join(_DATA_DIR, sub)


@dataclass
class QuantConfig:
    data_priority: list[str] = field(
        default_factory=lambda: ["tickflow", "mootdx", "astock"]
    )
    fee_rate: float = 0.0003
    slippage: float = 0.001
    default_stop_loss: float = 0.03
    sim_account_mem_mb: float = 400.0
    sim_account_mem_min_mb: float = 300.0
    db_path: str = ""
    bundle_dir: str = ""
    strategies_dir: str = ""
    runtime_dir: str = ""


def load_config() -> QuantConfig:
    return QuantConfig(
        data_priority=_csv_list(
            _env("QUANT_DATA_PRIORITY"),
            ["tickflow", "mootdx", "astock"],
        ),
        fee_rate=float(_env("QUANT_FEE_RATE", "0.0003") or "0.0003"),
        slippage=float(_env("QUANT_SLIPPAGE", "0.001") or "0.001"),
        default_stop_loss=float(_env("QUANT_DEFAULT_STOP_LOSS", "0.03") or "0.03"),
        sim_account_mem_mb=float(_env("SIM_ACCOUNT_MEM_MB", "400.0") or "400.0"),
        sim_account_mem_min_mb=float(_env("SIM_ACCOUNT_MEM_MIN_MB", "300.0") or "300.0"),
        db_path=_env("QUANT_DB_PATH", _data_path("quant.db")),
        bundle_dir=_env("QUANT_BUNDLE_DIR", _data_path("quant_bundle")),
        strategies_dir=_env("QUANT_STRATEGIES_DIR", _data_path("quant_strategies")),
        runtime_dir=_env("QUANT_RUNTIME_DIR", _data_path("quant_sim")),
    )


CONFIG = load_config()
