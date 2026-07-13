import os

# backend/ directory: config.py lives at <backend>/app/quant/jqengine/config.py
# so backend is 4 levels up from this file.
BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
# repo root: point at the tickflow backend root (per port spec).
REPO_ROOT = BASE_DIR


def load_config():
    return {
        "TUSHARE_TOKEN": os.getenv("TUSHARE_TOKEN", ""),
        "DATASOURCE_PRIORITY": [
            s.strip()
            for s in os.getenv("DATASOURCE_PRIORITY", "tushare,mootdx,astock").split(",")
            if s.strip()
        ],
        "FEE_RATE": float(os.getenv("FEE_RATE", "0.0003")),
        "SLIPPAGE": float(os.getenv("SLIPPAGE", "0.001")),
        "DEFAULT_STOP_LOSS": float(os.getenv("DEFAULT_STOP_LOSS", "0.03")),
        "DATA_DIR": os.getenv("DATA_DIR", os.path.join(BASE_DIR, "app", "quant", "jqengine", "data")),
        "RUNTIME_DIR": os.getenv("RUNTIME_DIR", os.path.join(BASE_DIR, "app", "quant", "jqengine", "runtime")),
        "STRATEGY_DIR": os.getenv(
            "STRATEGY_DIR", os.path.join(REPO_ROOT, "strategy")
        ),
        "DB_PATH": os.getenv("DB_PATH", os.path.join(BASE_DIR, "app.db")),
    }


CONFIG = load_config()
