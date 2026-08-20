"""70978ed5 ptrade 版注册进策略库：import_strategy 落盘 + 可被回测路由识别。"""
import os
import tempfile
from pathlib import Path

import pytest

from app.quant import db
from app.quant.config import CONFIG
from app.quant.strategies import store

PT_FIXTURE = Path(__file__).parent.parent / "fixtures" / "70978ed5_ptrade" / "70978ed5.ptrade.py"


@pytest.fixture
def env(tmp_path, monkeypatch):
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.unlink(path)
    db.init_db(path)
    monkeypatch.setattr(CONFIG, "strategies_dir", str(tmp_path))
    return path


def test_register_ptrade_strategy(env):
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
