"""量化回测 / 模拟盘子系统（独立目录，不改动原工程其它模块）。"""

# 导入即确保 quant.db 表结构存在（任何入口都会 import 本包）。
from . import db

db.init_db()
