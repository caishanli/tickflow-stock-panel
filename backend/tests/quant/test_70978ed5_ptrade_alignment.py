"""70978ed5 ptrade vs jq rqalpha 回测对齐：成交一致 + 逐日净值零差。"""
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

BACKEND = Path(__file__).parent.parent.parent
JQ_RUNNER = BACKEND / "scripts" / "run_jq_rqalpha.py"
PT_RUNNER = BACKEND / "scripts" / "run_ptrade_rqalpha.py"
FIXTURE = BACKEND / "tests" / "fixtures" / "70978ed5_ptrade"
JQ = FIXTURE / "70978ed5.py"
PT = FIXTURE / "70978ed5.ptrade.py"
START, END, CASH, FEE, SLIP = "2026-04-01", "2026-07-16", "100000", "0.0001", "0.0001"


@pytest.mark.integration
@pytest.mark.skipif(not JQ.exists() or not PT.exists(), reason="fixture 缺失")
def test_full_window_alignment(tmp_path):
    jq_out = tmp_path / "jq"
    pt_out = tmp_path / "pt"
    env = dict(os.environ, PYTHONPATH=str(BACKEND))
    subprocess.run([sys.executable, str(JQ_RUNNER), "--start", START, "--end", END,
                    "--strategy", str(JQ), "--out", str(jq_out),
                    "--cash", CASH, "--fee", FEE, "--slippage", SLIP],
                   env=env, check=True, capture_output=True, timeout=3000)
    subprocess.run([sys.executable, str(PT_RUNNER), "--start", START, "--end", END,
                    "--strategy", str(PT), "--out", str(pt_out),
                    "--cash", CASH, "--fee", FEE, "--slippage", SLIP],
                   env=env, check=True, capture_output=True, timeout=3000)

    jq_tr = pd.read_csv(jq_out / "trades.csv")
    pt_tr = pd.read_csv(pt_out / "trades.csv")
    pd.testing.assert_frame_equal(jq_tr, pt_tr)

    jq_eq = pd.read_csv(jq_out / "equity.csv")
    pt_eq = pd.read_csv(pt_out / "equity.csv")
    m = pd.merge(jq_eq, pt_eq, on="date", suffixes=("_jq", "_pt"))
    assert len(m) > 20
    diff = (m["value_pt"] - m["value_jq"]).abs()
    assert diff.max() == 0.0, f"逐日净值存在差异: max={diff.max():.6f}"