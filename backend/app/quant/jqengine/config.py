import os

# backend/ directory: config.py lives at <backend>/app/quant/jqengine/config.py
# so backend is 4 levels up from this file.
BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
# repo root: point at the tickflow backend root (per port spec).
REPO_ROOT = BASE_DIR


def _default_data_dir():
    """行情缓存默认目录：仓库根 ``data/quant_kline``（引擎中立，与 tickflow
    数据层同根； jqengine 只是其中一个消费方，模拟盘/后续其他平台桥接共用）。

    兼容旧默认 ``backend/app/quant/jqengine/data``：新目录不存在而旧目录存在时
    沿用旧目录，老机器升级后缓存不丢。``DATA_DIR`` 环境变量始终优先（Docker
    部署强制 ``DATA_DIR=/app/data``，不走此默认）。
    """
    new = os.path.join(os.path.dirname(BASE_DIR), "data", "quant_kline")
    old = os.path.join(BASE_DIR, "app", "quant", "jqengine", "data")
    if not os.path.isdir(new) and os.path.isdir(old):
        return old
    return new


def load_config():
    return {
        "DATASOURCE_PRIORITY": [
            s.strip()
            for s in os.getenv("DATASOURCE_PRIORITY", "mootdx,astock").split(",")
            if s.strip()
        ],
        "FEE_RATE": float(os.getenv("FEE_RATE", "0.0003")),
        "SLIPPAGE": float(os.getenv("SLIPPAGE", "0.001")),
        "DEFAULT_STOP_LOSS": float(os.getenv("DEFAULT_STOP_LOSS", "0.03")),
        "DATA_DIR": os.getenv("DATA_DIR", _default_data_dir()),
        "RUNTIME_DIR": os.getenv("RUNTIME_DIR", os.path.join(BASE_DIR, "app", "quant", "jqengine", "runtime")),
        "STRATEGY_DIR": os.getenv(
            "STRATEGY_DIR", os.path.join(REPO_ROOT, "strategy")
        ),
        "DB_PATH": os.getenv("DB_PATH", os.path.join(BASE_DIR, "app.db")),
    }


CONFIG = load_config()
