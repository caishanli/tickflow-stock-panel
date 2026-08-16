import importlib.util
import json
import sys

import pytest


def _load_script():
    path = "scripts/run_quant_backtest.py"
    spec = importlib.util.spec_from_file_location("run_quant_backtest", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_missing_arg_exits_nonzero(capsys):
    rb = _load_script()
    old = sys.argv
    sys.argv = ["run_quant_backtest.py"]
    try:
        with pytest.raises(SystemExit) as exc:
            rb.main()
        assert exc.value.code == 1
    finally:
        sys.argv = old
    err = capsys.readouterr().err
    assert "usage" in err.lower()


def test_unknown_run_id_exits_nonzero(monkeypatch):
    rb = _load_script()
    monkeypatch.setattr(rb.db, "get_run", lambda run_id: None)
    old = sys.argv
    sys.argv = ["run_quant_backtest.py", "nope"]
    try:
        with pytest.raises(SystemExit) as exc:
            rb.main()
        assert exc.value.code == 1
    finally:
        sys.argv = old


def test_valid_run_resolves_strategy_and_calls_run_backtest(monkeypatch):
    rb = _load_script()

    params = {
        "strategy_id": "strat1",
        "run_id": "run42",
        "start": "2020-01-01",
        "end": "2020-02-01",
        "symbols": ["600000.XSHG"],
    }
    run_row = {"params_json": json.dumps(params)}

    captured = {}

    def fake_get_run(run_id):
        assert run_id == "run42"
        return run_row

    def fake_get_strategy(sid):
        assert sid == "strat1"
        return {"code": "STRATEGY_CODE_BODY"}

    def fake_run_backtest(code, params, provider=None, db_path=None):
        captured["code"] = code
        captured["params"] = params
        captured["provider"] = provider
        captured["db_path"] = db_path
        return {"run_id": "run42"}

    monkeypatch.setattr(rb.db, "get_run", fake_get_run)
    monkeypatch.setattr(rb, "get_strategy", fake_get_strategy)
    monkeypatch.setattr(rb, "run_backtest", fake_run_backtest)

    old = sys.argv
    sys.argv = ["run_quant_backtest.py", "run42"]
    try:
        rb.main()  # should not raise
    finally:
        sys.argv = old

    assert captured["code"] == "STRATEGY_CODE_BODY"
    assert captured["params"]["run_id"] == "run42"
    assert captured["params"]["strategy_id"] == "strat1"
    assert captured["provider"] is not None
    assert captured["db_path"] is not None


def test_bridge_import_failure_marks_run_failed(monkeypatch):
    rb = _load_script()

    params = {
        "strategy_id": "strat1",
        "run_id": "runX",
        "start": "2020-01-01",
        "end": "2020-02-01",
        "symbols": ["600000.XSHG"],
    }
    run_row = {"params_json": json.dumps(params)}

    monkeypatch.setattr(rb.db, "get_run", lambda run_id: run_row)
    monkeypatch.setattr(rb, "get_strategy", lambda sid: {"code": "STRATEGY_CODE_BODY"})
    monkeypatch.setattr(rb, "run_backtest", None)
    monkeypatch.setattr(rb, "run_jq_backtest", None)
    monkeypatch.setattr(rb, "_BRIDGE_IMPORT_ERROR", RuntimeError("no rqalpha"))

    updates = []
    monkeypatch.setattr(
        rb.db, "update_run",
        lambda run_id, status, error=None, **kw: updates.append((status, error)),
    )

    old = sys.argv
    sys.argv = ["run_quant_backtest.py", "runX"]
    try:
        with pytest.raises(SystemExit) as exc:
            try:
                rb.main()
            except Exception as e:  # 模拟 __main__ 兜底
                rb.db.update_run("runX", "failed", error=str(e)[:500])
                raise SystemExit(1) from None
        assert exc.value.code == 1
    finally:
        sys.argv = old

    assert updates, "db.update_run was not called"
    assert updates[0][0] == "failed"
    assert "rqalpha" in updates[0][1]
