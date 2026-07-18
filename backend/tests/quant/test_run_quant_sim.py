import importlib.util
import sys

import pytest


def _load_script():
    path = "scripts/run_quant_sim.py"
    spec = importlib.util.spec_from_file_location("run_quant_sim", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_missing_arg_exits_nonzero(capsys):
    rs = _load_script()
    old = sys.argv
    sys.argv = ["run_quant_sim.py"]
    try:
        with pytest.raises(SystemExit) as exc:
            rs.main()
        assert exc.value.code == 1
    finally:
        sys.argv = old
    err = capsys.readouterr().err
    assert "usage" in err.lower()


def test_valid_account_id_calls_run_loop(monkeypatch):
    rs = _load_script()

    captured = {}

    def fake_run_loop(account_id):
        captured["account_id"] = account_id

    monkeypatch.setattr(rs, "run_loop", fake_run_loop)

    old = sys.argv
    sys.argv = ["run_quant_sim.py", "acct42"]
    try:
        rs.main()  # should not raise
    finally:
        sys.argv = old

    assert captured["account_id"] == "acct42"
