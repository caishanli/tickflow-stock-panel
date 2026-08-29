"""--clone-from 克隆账户：配置镜像 + sim_state 整行镜像。"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

from app.quant import db
from app.quant.config import CONFIG

_RUN_SIM_PATH = Path(__file__).parent.parent.parent / "scripts" / "run_quant_sim.py"


def _load_run_quant_sim():
    """scripts/ 不是包（无 __init__.py），按路径加载 run_quant_sim 模块。"""
    spec = importlib.util.spec_from_file_location("run_quant_sim", str(_RUN_SIM_PATH))
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def tmp_quant(tmp_path, monkeypatch):
    db_path = tmp_path / "quant.db"
    monkeypatch.setattr(CONFIG, "db_path", str(db_path))
    db.init_db(str(db_path))
    return tmp_path


@pytest.fixture
def source_account(tmp_quant):
    db.insert_sim_account("src12345", "五福v5.4-钉钉版", 100000.0, 0.03,
                          "paused", "wufu-src-strategy", "2026-07-10", "minute")
    db.update_sim_account("src12345", dingtalk_enabled=1)
    db.upsert_sim_state("src12345", 139.7262,
                        json.dumps({"159502.XSHE": {"amount": 70800.0, "avg_cost": 1.673}}),
                        116605.7262, 16605.7262, 100000.0, "[]", "2026-08-25 11:19:00")
    return "src12345"


def _run_create(argv):
    import sys as _sys
    mod = _load_run_quant_sim()
    old = _sys.argv
    _sys.argv = ["run_quant_sim.py", *argv]
    try:
        mod.main()
    finally:
        _sys.argv = old


def test_clone_mirrors_config_and_state(source_account, capsys):
    _run_create(["--create", "--clone-from", source_account,
                 "--strategy-id", "wufu-v5.4-ding-report"])
    accounts = {a["id"]: a for a in db.list_sim_accounts()}
    clone_id = next(aid for aid in accounts if aid != source_account)
    clone = accounts[clone_id]
    assert clone["strategy_id"] == "wufu-v5.4-ding-report"
    assert clone["name"] == "五福v5.4-钉钉版-预报告"           # 缺省名 = 源名-预报告
    assert clone["capital"] == 100000.0 and clone["stop_loss"] == 0.03
    assert clone["start_date"] == "2026-07-10" and clone["frequency"] == "minute"
    assert clone["dingtalk_enabled"] == 1
    st = db.read_sim_state(clone_id)
    assert st["cash"] == 139.7262 and st["dt"] == "2026-08-25 11:19:00"
    assert "159502.XSHE" in st["positions"]                    # 持仓镜像
    out = capsys.readouterr().out
    assert f"cloned account {source_account} -> {clone_id}" in out


def test_clone_requires_strategy_id(source_account, capsys):
    with pytest.raises(SystemExit):
        _run_create(["--create", "--clone-from", source_account])
    assert any("--strategy-id" in s for s in capsys.readouterr().err.splitlines())


def test_clone_missing_source_exits(source_account):
    with pytest.raises(SystemExit):
        _run_create(["--create", "--clone-from", "nonexist", "--strategy-id", "s1"])


def test_clone_overrides_apply(source_account):
    _run_create(["--create", "--clone-from", source_account, "--strategy-id", "sid-x",
                 "--name", "自定义名", "--account-id", "cln00001",
                 "--capital", "200000", "--start-date", "2026-08-01"])
    acc = db.get_sim_account("cln00001")
    assert acc["name"] == "自定义名" and acc["capital"] == 200000.0
    assert acc["start_date"] == "2026-08-01"
    st = db.read_sim_state("cln00001")
    assert st["positions"]                                     # state 仍克隆
