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
    db.insert_run("r1", "s1", "", '{"a":1}', "queued")
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
    db.insert_sim_stoploss("a1", "2024-01-02 09:31", "600000.XSHG", "浦发银行", "STOP_LOSS", 9.9, 100, -50.0, -0.01, 0.0)
    assert len(db.get_sim_snapshots("a1")) == 1
    assert len(db.get_sim_trades("a1")) == 1
    assert len(db.get_sim_stoploss("a1")) == 1
    db.delete_sim_account("a1")
    assert db.get_sim_account("a1") is None
    os.unlink(p)

def test_quant_settings_kv():
    p = _fresh()
    db.set_quant_setting("dingtalk_webhook_url", "https://oapi.dingtalk.com/robot/send?access_token=xxx")
    db.set_quant_setting("dingtalk_secret", "SECxxx")
    assert db.get_quant_setting("dingtalk_webhook_url") == "https://oapi.dingtalk.com/robot/send?access_token=xxx"
    assert db.get_quant_setting("dingtalk_secret") == "SECxxx"
    assert db.get_quant_setting("nonexistent") is None
    os.unlink(p)

def test_sim_account_dingtalk_enabled():
    p = _fresh()
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "created")
    acct = db.get_sim_account("a1")
    assert acct["dingtalk_enabled"] == 0
    db.update_sim_account("a1", dingtalk_enabled=1)
    acct = db.get_sim_account("a1")
    assert acct["dingtalk_enabled"] == 1
    os.unlink(p)

def test_sim_trade_name_column():
    p = _fresh()
    db.insert_sim_trade("a1", "2024-01-02 09:31", "159985.XSHE", "BUY",
                        2.139, 100, 0.0, 0.0, 9.99, "豆粕ETF华夏")
    trades = db.get_sim_trades("a1")
    assert len(trades) == 1
    assert trades[0]["code"] == "159985.XSHE"
    assert trades[0]["name"] == "豆粕ETF华夏"
    db.delete_sim_account("a1")
    os.unlink(p)


def test_sim_trade_name_optional_old_signature():
    """不带 name 的旧调用仍可用（name 默认空串）。"""
    p = _fresh()
    db.insert_sim_trade("a1", "2024-01-02 09:31", "600000.XSHG", "BUY", 10.0, 100, 0.0, 0.0, 0.0)
    trades = db.get_sim_trades("a1")
    assert trades[0]["name"] == ""
    db.delete_sim_account("a1")
    os.unlink(p)


def test_sim_trade_batch_with_name():
    p = _fresh()
    db.batch_insert_trades([
        ("a1", "2024-01-02 09:31", "159985.XSHE", "BUY", 2.139, 100, 0.0, 0.0, 9.99, "豆粕ETF华夏"),
        ("a1", "2024-01-02 09:32", "511880.XSHG", "SELL", 100.0, 1000, 0.0, 0.0, 1.0, "银华日利ETF"),
    ])
    trades = db.get_sim_trades("a1")
    assert len(trades) == 2
    assert {t["name"] for t in trades} == {"豆粕ETF华夏", "银华日利ETF"}
    db.delete_sim_account("a1")
    os.unlink(p)


def test_sim_trade_name_column_migration_on_old_db():
    """旧库（无 name 列）init_db 自动补列，历史行 name 为 NULL。"""
    p = _fresh()
    # 手动建无 name 列的旧表结构（先删新表）
    conn = sqlite3.connect(p)
    conn.execute("DROP TABLE sim_trades")
    conn.execute(
        "CREATE TABLE sim_trades (account_id TEXT, ts TEXT, code TEXT, action TEXT, "
        "price REAL, amount REAL, pnl REAL, pnl_pct REAL, commission REAL)")
    conn.execute(
        "INSERT INTO sim_trades VALUES('a1','2024-01-02 09:31','600000.XSHG','BUY',"
        "10.0,100,0.0,0.0,0.0)")
    conn.commit(); conn.close()
    # init_db 补列
    db.init_db(p)
    trades = db.get_sim_trades("a1")
    assert len(trades) == 1
    assert "name" in trades[0]
    assert trades[0]["name"] is None
    os.unlink(p)


def test_compile_run_routing(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "_COMPILE_DIR", str(tmp_path / "compile"))
    p = _fresh()
    db.insert_run("c_12345678", "s1", "", '{"a":1}', "queued")
    db.insert_run("main1234", "s1", "", '{"a":1}', "queued")
    assert db.get_run("c_12345678")["status"] == "queued"
    assert db.get_run("main1234")["status"] == "queued"
    with db.get_conn() as c:
        ids = [x["id"] for x in c.execute("SELECT id FROM backtest_runs").fetchall()]
    assert ids == ["main1234"]
    assert db.routed_db_path("c_12345678").endswith("compile/c_12345678.db")
    assert not db.is_compile_run("main1234")
    assert db.is_compile_run("c_12345678")


def test_compile_run_lifecycle(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "_COMPILE_DIR", str(tmp_path / "compile"))
    p = _fresh()
    rid = "c_12345678"
    db.insert_run(rid, "s1", "", "{}", "queued")
    db.bulk_insert_equity(rid, [("2024-01-02", 1.0, 1.0, 0.9, 0.1)])
    db.insert_trade(rid, "2024-01-02 09:30", "600000.XSHG", "BUY", 10.0, 100, 0.0, 0.0, 0.0)
    db.insert_log(rid, "2024-01-02 09:30", "INFO", "start")
    db.update_run(rid, "done", metrics_json='{"sharpe":1.2}')
    assert len(db.get_equity(rid)) == 1
    assert len(db.get_trades(rid)) == 1
    assert len(db.get_logs(rid)) == 1
    assert db.get_max_log_id(rid) == 1
    assert len(db.get_logs_after(rid, 0)) == 1
    with db.get_conn() as c:
        for t in ("backtest_runs", "backtest_equity", "backtest_trades", "backtest_logs"):
            n = c.execute(f"SELECT COUNT(*) AS n FROM {t}").fetchone()["n"]
            assert n == 0, f"主库 {t} 应零残留"
    db.delete_run(rid)
    assert db.get_run(rid) is None
