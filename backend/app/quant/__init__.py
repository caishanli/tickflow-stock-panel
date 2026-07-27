"""量化回测 / 模拟盘子系统（独立目录，不改动原工程其它模块）。"""

from __future__ import annotations

import os
from pathlib import Path

# ── 确保 backend/.env 在任何子模块导入前加载 ──────────────
# jqengine.config 和 app.quant.config 都在模块级读 os.environ，
# 若 .env 未加载，环境变量为空 → 数据源可能被降级。
# __file__ = backend/app/quant/__init__.py → .parent.parent.parent = backend/
_ENV_PATH = Path(__file__).resolve().parent.parent.parent / ".env"
if _ENV_PATH.is_file():
    try:
        from dotenv import load_dotenv as _ld

        _ld(_ENV_PATH, override=False)
    except ImportError:
        pass

# 导入即确保 quant.db 表结构存在（任何入口都会 import 本包）。
from . import db

db.init_db()
