"""data_verify 状态报告 / 隔离清单 / 设置 API 测试。

核心（``write_status`` / ``write_quarantine``）用纯函数直测，不依赖鉴权；
HTTP 端点只做冒烟（本机未设密码 → 200，已设密码 → 401，两者都符合鉴权
约定），再用**直接调用端点函数**覆盖「从未跑过 / 有状态文件 / 单飞」分支。
后台任务体 ``_run_verify_job`` 用假 job_store 同步驱动，不真跑
``run_verify``（那是 runner 自己的测试范围）。
"""
from __future__ import annotations

import json
from datetime import datetime

from fastapi.testclient import TestClient

from app.main import app
from app.quant.data_verify.report import (
    QUARANTINE_FILENAME,
    STATUS_FILENAME,
    read_status,
    write_quarantine,
    write_status,
)

STATUS_KEYS = {
    "last_run_at",
    "stage",
    "findings",
    "repaired",
    "quarantined",
    "sources",
}


# ── 隔离清单 ────────────────────────────────────────────────────────


def test_quarantine_merges_and_dedupes(tmp_path):
    entry = {"table": "kline_minute", "day": "2026-09-01",
             "symbol": "600004.SH", "reason": "conflict"}
    write_quarantine([entry], tmp_path)
    write_quarantine([entry, {**entry, "symbol": "600005.SH"}], tmp_path)
    data = json.loads((tmp_path / "data_verify_quarantine.json").read_text())
    assert len(data) == 2


def test_quarantine_dedupes_across_runs_and_keeps_details(tmp_path):
    entry = {"table": "kline_daily", "day": "2026-09-02", "symbol": "600004.SH",
             "reason": "conflict", "detail": {"source": "sina"}}
    write_quarantine([entry], tmp_path)
    # 同一键、字段顺序不同、detail 内容不同 → 仍算同一条（不去重会被刷爆）
    write_quarantine([
        {"reason": "conflict", "symbol": "600004.SH", "day": "2026-09-02",
         "table": "kline_daily", "detail": {"source": "tencent"}},
    ], tmp_path)
    data = json.loads((tmp_path / QUARANTINE_FILENAME).read_text())
    assert len(data) == 1
    assert data[0]["detail"] == {"source": "sina"}   # 已有记录不被覆盖


def test_quarantine_key_includes_reason(tmp_path):
    base = {"table": "kline_minute", "day": "2026-09-01", "symbol": "600004.SH"}
    write_quarantine([{**base, "reason": "conflict"},
                      {**base, "reason": "single_source"}], tmp_path)
    data = json.loads((tmp_path / QUARANTINE_FILENAME).read_text())
    assert sorted(item["reason"] for item in data) == ["conflict", "single_source"]


def test_quarantine_partition_level_entry_without_symbol(tmp_path):
    entry = {"table": "kline_daily", "day": "2026-09-01", "reason": "unreadable"}
    write_quarantine([entry, dict(entry)], tmp_path)
    data = json.loads((tmp_path / QUARANTINE_FILENAME).read_text())
    assert len(data) == 1


def test_quarantine_write_is_atomic_and_leaves_no_tmp(tmp_path):
    path = write_quarantine(
        [{"table": "kline_minute", "day": "2026-09-01", "symbol": "600004.SH",
          "reason": "conflict"}],
        tmp_path,
    )
    assert path == tmp_path / QUARANTINE_FILENAME
    assert sorted(p.name for p in tmp_path.iterdir()) == [QUARANTINE_FILENAME]


def test_quarantine_corrupt_existing_file_is_replaced(tmp_path):
    (tmp_path / QUARANTINE_FILENAME).write_text("{not json")
    write_quarantine(
        [{"table": "kline_minute", "day": "2026-09-01", "symbol": "600004.SH",
          "reason": "conflict"}],
        tmp_path,
    )
    data = json.loads((tmp_path / QUARANTINE_FILENAME).read_text())
    assert len(data) == 1


# ── 状态报告 ────────────────────────────────────────────────────────


def test_write_status_atomic_fixed_keys(tmp_path):
    report = {
        "findings": 12,
        "repaired": 3,
        "sources": ["tencent", "sina"],
        "days": 5,                       # 非固定键必须被丢掉
        "state_path": "/tmp/whatever",
    }
    path = write_status(report, tmp_path)
    assert path == tmp_path / STATUS_FILENAME
    data = json.loads(path.read_text())
    assert set(data) == STATUS_KEYS
    assert data["findings"] == 12
    assert data["repaired"] == 3
    assert data["stage"] == "all"       # 未给 stage 时默认整轮
    assert data["quarantined"] == 0
    assert data["sources"] == ["tencent", "sina"]
    datetime.fromisoformat(data["last_run_at"])      # ISO 可被解析
    assert not (tmp_path / (STATUS_FILENAME + ".tmp")).exists()


def test_write_status_overwrites_previous_run(tmp_path):
    write_status({"stage": "structure", "findings": 9, "quarantined": 2}, tmp_path)
    write_status({"stage": "external_minute", "findings": 1, "sources": ["sina"]},
                 tmp_path)
    data = read_status(tmp_path)
    assert data["stage"] == "external_minute"
    assert data["findings"] == 1
    assert data["quarantined"] == 0    # 不残留上一轮的值
    assert data["sources"] == ["sina"]


def test_write_status_accepts_explicit_last_run_at(tmp_path):
    write_status({"last_run_at": "2026-11-07T15:35:00", "stage": "all"}, tmp_path)
    assert read_status(tmp_path)["last_run_at"] == "2026-11-07T15:35:00"


def test_write_status_counts_quarantine_entries(tmp_path):
    write_status({"quarantined": [{"table": "kline_daily"}, {"table": "kline_minute"}]},
                 tmp_path)
    assert read_status(tmp_path)["quarantined"] == 2


def test_read_status_missing_or_corrupt_returns_none(tmp_path):
    assert read_status(tmp_path) is None
    (tmp_path / STATUS_FILENAME).write_text("[]")
    assert read_status(tmp_path) is None


# ── 端点 ────────────────────────────────────────────────────────────


def test_status_endpoint_shape(monkeypatch):
    # 不进 lifespan：startup 会拉起 mining/stockdata 等后台服务，本机已有 dev
    # 后端在跑时会撞 data/.mining_process.lock（与本端点无关）。路由与鉴权
    # 中间件不依赖 lifespan，`with TestClient(...)` 只额外跑 startup/shutdown。
    #
    # 客户端 IP 固定成回环：TestClient 默认 peer 是 "testclient"，在「未设密码
    # 且非内网」时全局中间件直接 403（与本端点无关）。回环 + 未设密码 → 200，
    # 已设密码且无 session → 401，两者都符合鉴权约定。
    from app.api import auth as auth_api

    monkeypatch.setattr(auth_api, "_client_ip", lambda _request: "127.0.0.1")
    client = TestClient(app)
    try:
        r = client.get("/api/settings/data-verify/status")
    finally:
        client.close()
    assert r.status_code in (200, 401)      # 未登录时 401 也算通过鉴权约定


def test_status_endpoint_never_ran(tmp_path, monkeypatch):
    from app.api import settings as settings_api
    from app.config import settings

    monkeypatch.setattr(settings, "data_dir", tmp_path)
    assert settings_api.get_data_verify_status() == {"status": "never"}


def test_status_endpoint_returns_last_run(tmp_path, monkeypatch):
    from app.api import settings as settings_api
    from app.config import settings

    monkeypatch.setattr(settings, "data_dir", tmp_path)
    write_status({"stage": "external_minute", "findings": 2, "repaired": 1,
                  "quarantined": 1, "sources": ["tencent"]}, tmp_path)

    data = settings_api.get_data_verify_status()
    assert data["status"] == "ok"
    assert data["stage"] == "external_minute"
    assert data["findings"] == 2
    assert data["repaired"] == 1
    assert data["quarantined"] == 1
    assert data["sources"] == ["tencent"]
    assert data["last_run"] == data["last_run_at"]


class _FakeJobStore:
    """只记录调用的 job_store 替身（端点/后台体不落真任务）。"""

    def __init__(self, created=("job-fake", True)):
        self._created = created
        self.created: list[dict] = []
        self.started: list[str] = []
        self.progress_calls: list[tuple[str, int, str]] = []
        self.succeeded: tuple[str, object] | None = None
        self.failed: tuple[str, str] | None = None

    def create(self, **_kwargs):
        self.created.append(dict(_kwargs))
        return self._created

    def start(self, job_id):
        self.started.append(job_id)

    def progress(self, job_id, stage, pct, msg):
        self.progress_calls.append((stage, pct, msg))

    def succeed(self, job_id, result):
        self.succeeded = (job_id, result)

    def fail(self, job_id, error):
        self.failed = (job_id, error)


def _install_fake_store(monkeypatch, fake):
    from app.services import pipeline_jobs

    monkeypatch.setattr(pipeline_jobs, "job_store", fake)
    return fake


def test_run_endpoint_starts_background_job(monkeypatch):
    from app.api import settings as settings_api

    fake = _install_fake_store(monkeypatch, _FakeJobStore(("job-abc", True)))
    spawned: list[str] = []
    monkeypatch.setattr(settings_api, "_spawn_verify_job", spawned.append)

    assert settings_api.run_data_verify() == {"job_id": "job-abc", "reused": False}
    assert fake.created == [{"long_running": True}]
    assert spawned == ["job-abc"]


def test_run_endpoint_reuses_active_job(monkeypatch):
    from app.api import settings as settings_api

    _install_fake_store(monkeypatch, _FakeJobStore(("job-running", False)))
    spawned: list[str] = []
    monkeypatch.setattr(settings_api, "_spawn_verify_job", spawned.append)

    assert settings_api.run_data_verify() == {"job_id": "job-running", "reused": True}
    assert spawned == []                 # 复用活跃任务时绝不能再起一条


def test_run_verify_job_writes_status_and_succeeds(tmp_path, monkeypatch):
    from app.api import settings as settings_api
    from app.config import settings
    from app.quant.data_verify import runner

    monkeypatch.setattr(settings, "data_dir", tmp_path)
    fake = _install_fake_store(monkeypatch, _FakeJobStore())
    seen: dict = {}

    def _fake_run(stage, *, apply=False, progress=None):
        seen["stage"] = stage
        seen["apply"] = apply
        progress("structure", 50, "half way")
        return {"findings": 4, "repaired": 2, "days": 3,
                "sources": ["tencent"], "state_path": "x"}

    monkeypatch.setattr(runner, "run_verify", _fake_run)

    settings_api._run_verify_job("job-1")

    assert seen == {"stage": "all", "apply": True}
    assert fake.started == ["job-1"]
    assert ("structure", 50, "half way") in fake.progress_calls
    assert fake.failed is None
    assert fake.succeeded is not None and fake.succeeded[0] == "job-1"

    status = read_status(tmp_path)
    assert status["stage"] == "all"
    assert status["findings"] == 4
    assert status["repaired"] == 2
    assert status["sources"] == ["tencent"]


def test_run_verify_job_marks_failed_on_error(tmp_path, monkeypatch):
    from app.api import settings as settings_api
    from app.config import settings
    from app.quant.data_verify import runner

    monkeypatch.setattr(settings, "data_dir", tmp_path)
    fake = _install_fake_store(monkeypatch, _FakeJobStore())

    def _boom(*_args, **_kwargs):
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(runner, "run_verify", _boom)

    settings_api._run_verify_job("job-2")

    assert fake.failed == ("job-2", "disk on fire")
    assert fake.succeeded is None
    assert read_status(tmp_path) is None      # 失败不写半截状态


def test_run_verify_job_swallows_cancel(tmp_path, monkeypatch):
    from app.api import settings as settings_api
    from app.config import settings
    from app.quant.data_verify import runner
    from app.services.pipeline_jobs import JobCancelledError

    monkeypatch.setattr(settings, "data_dir", tmp_path)
    fake = _install_fake_store(monkeypatch, _FakeJobStore())

    def _cancelled(*_args, **_kwargs):
        raise JobCancelledError("job-3")

    monkeypatch.setattr(runner, "run_verify", _cancelled)

    settings_api._run_verify_job("job-3")     # 不得抛出

    assert fake.failed is None                # 状态已被 terminate() 写成 failed
    assert fake.succeeded is None
