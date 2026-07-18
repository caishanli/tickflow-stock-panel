"""量化模块配置（读 .env）。"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _csv_list(value: str | None, default: list[str]) -> list[str]:
    if not value:
        return list(default)
    return [p.strip() for p in value.split(",") if p.strip()]


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


@dataclass
class QuantConfig:
    data_priority: list[str] = field(
        default_factory=lambda: ["tickflow", "tushare", "mootdx", "astock"]
    )
    tushare_token: str = ""
    fee_rate: float = 0.0003
    slippage: float = 0.001
    default_stop_loss: float = 0.03
    db_path: str = "data/quant.db"
    bundle_dir: str = "data/quant_bundle"
    strategies_dir: str = "data/quant_strategies"
    runtime_dir: str = "data/quant_sim"


def load_config() -> QuantConfig:
    return QuantConfig(
        data_priority=_csv_list(
            _env("QUANT_DATA_PRIORITY"),
            ["tickflow", "tushare", "mootdx", "astock"],
        ),
        tushare_token=_env("QUANT_TUSHARE_TOKEN"),
        fee_rate=float(_env("QUANT_FEE_RATE", "0.0003") or "0.0003"),
        slippage=float(_env("QUANT_SLIPPAGE", "0.001") or "0.001"),
        default_stop_loss=float(_env("QUANT_DEFAULT_STOP_LOSS", "0.03") or "0.03"),
        db_path=_env("QUANT_DB_PATH", "data/quant.db"),
        bundle_dir=_env("QUANT_BUNDLE_DIR", "data/quant_bundle"),
        strategies_dir=_env("QUANT_STRATEGIES_DIR", "data/quant_strategies"),
        runtime_dir=_env("QUANT_RUNTIME_DIR", "data/quant_sim"),
    )


CONFIG = load_config()
