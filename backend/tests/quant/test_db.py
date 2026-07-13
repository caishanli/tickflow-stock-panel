import os, tempfile, sqlite3
from app.quant import db

def _fresh():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd); os.unlink(path)
    db.init_db(path)
    return path

def test_backtest_run_lifecycle():
    p = _fresh()
    db.init_db(p)
    db.insert_run("r1", "s1", '{"a":1}', "queued")
    assert db.get_run("r1")["status"] == "queued"
    db.bulk_insert_equity("r1", [("2024-01-02", 1.0, 1.0, 0.9, 0.1)])
    db.insert_trade("r1", "2024-01-02 09:30", "600000.XSHG", "BUY", 10.0, 100, 0.0, 0.0, 0.0)
    db.insert_log("r1", "2024-01-02 09:30", "INFO", "start")
    db.update_run("r1", "done", metrics_json='{"sharpe":1.2}')
    r = db.get_run("r1")
    assert r["status"] == "done" and "sharpe" in r["metrics_json"]
    assert len(db.get_equity("r1")) == 1
    assert len(db.get_trades("r1")) == 1
    assert len(db.get_logs("r1")) == 1
    db.delete_run("r1")
    assert db.get_run("r1") is None
    os.unlink(p)

def test_sim_account_and_state():
    p = _fresh()
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "created")
    assert db.get_sim_account("a1")["capital"] == 100000.0
    db.upsert_sim_state("a1", 99000.0, '{"600000.XSHG":{}}', 99000.0, -1000.0, 100000.0, "[]", "2024-01-02 09:30")
    st = db.read_sim_state("a1")
    assert st["cash"] == 99000.0 and st["pnl"] == -1000.0
    db.insert_sim_snapshot("a1", "2024-01-02 09:30", 99000.0, 99000.0, 0.0, -1000.0, -0.01)
    db.insert_sim_trade("a1", "2024-01-02 09:31", "600000.XSHG", "SELL", 10.0, 100, -50.0, -0.005, 0.0)
    db.insert_sim_stoploss("a1", "2024-01-02 09:31", "600000.XSHG", "STOP_LOSS", 9.9, -0.01)
    assert len(db.get_sim_snapshots("a1")) == 1
    assert len(db.get_sim_trades("a1")) == 1
    assert len(db.get_sim_stoploss("a1")) == 1
    db.delete_sim_account("a1")
    assert db.get_sim_account("a1") is None
    os.unlink(p)
