"""模拟盘独立进程：由 FastAPI 派生子进程或 pm2/nohup 守护。"""
from __future__ import annotations

import sys

from app.quant.simulate.runner import run_loop


def main():
    if len(sys.argv) < 2:
        print("usage: run_quant_sim.py <account_id>", file=sys.stderr)
        sys.exit(1)
    run_loop(sys.argv[1])


if __name__ == "__main__":
    main()
