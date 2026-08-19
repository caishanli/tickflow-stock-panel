"""70978ed5 ptrade 版注册进策略库：import_strategy 落盘 + 可被回测路由识别。"""
from pathlib import Path

from app.quant.config import CONFIG
from app.quant.strategies import store

PT_FIXTURE = Path(__file__).parent.parent / "fixtures" / "70978ed5_ptrade" / "70978ed5.ptrade.py"


def test_register_ptrade_strategy():
    code = PT_FIXTURE.read_text(encoding="utf-8")
    assert ".SS" in code
    sid = store.import_strategy("五福v5.4-ptrade", code)
    assert len(sid) == 8
    saved = store.get_strategy(sid)
    assert saved is not None
    assert saved["name"] == "五福v5.4-ptrade"
    assert ".SS" in saved["code"]
    p = Path(CONFIG.strategies_dir) / f"{sid}.py"
    assert p.exists()
    print(f"registered sid={sid} file={p}")