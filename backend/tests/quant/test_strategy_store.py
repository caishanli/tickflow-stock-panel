import os
import tempfile

import pytest

from app.quant import db
from app.quant.config import CONFIG
from app.quant.strategies import (
    delete_strategy,
    export_strategy,
    get_strategy,
    import_strategy,
    list_strategies,
    save_strategy,
)


@pytest.fixture
def env(tmp_path, monkeypatch):
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.unlink(path)
    db.init_db(path)
    monkeypatch.setattr(CONFIG, "strategies_dir", str(tmp_path))
    return path


SAMPLE_CODE = "def init(context):\n    pass\n"


def test_import_and_list(env):
    rec = import_strategy("demo", SAMPLE_CODE)
    assert rec["name"] == "demo" and rec["code"] == SAMPLE_CODE
    names = [s["name"] for s in list_strategies()]
    assert "demo" in names


def test_get_and_export(env):
    rec = import_strategy("demo", SAMPLE_CODE)
    sid = rec["id"]
    got = get_strategy(sid)
    assert got["code"] == SAMPLE_CODE
    assert export_strategy(sid) == SAMPLE_CODE
    assert get_strategy("missing") is None
    assert export_strategy("missing") == ""


def test_save_update_and_delete(env):
    rec = save_strategy("abc", "first", SAMPLE_CODE)
    assert rec["name"] == "first"
    save_strategy("abc", "second", "def handle(c):\n    pass\n")
    assert get_strategy("abc")["name"] == "second"
    assert "handle" in get_strategy("abc")["code"]
    delete_strategy("abc")
    assert get_strategy("abc") is None
